package filestore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/txsaga/txsaga"
)

// fileTx 是一次 Commit 的事务视图：操作对象是当前内存根的深拷贝，
// 回调成功且快照落盘后整根才会替换，因此回调失败或落盘失败都不留痕迹。
type fileTx struct {
	business string
	idem     string
	binding  *txsaga.Binding
	state    *txsaga.ExecutionState
	created  bool
	staged   []txsaga.Event
}

func (t *fileTx) Binding() (txsaga.Binding, bool) {
	if t.binding != nil {
		return *t.binding, true
	}
	return txsaga.Binding{}, false
}

func (t *fileTx) State() *txsaga.ExecutionState { return t.state }

func (t *fileTx) Create(state *txsaga.ExecutionState) {
	state.BusinessKey = t.business
	state.IdempotencyKey = t.idem
	t.state = state
	t.created = true
}

func (t *fileTx) AppendEvent(eventType string, p txsaga.EventPayload) {
	t.staged = append(t.staged, txsaga.Event{
		BusinessKey: t.business,
		Type:        eventType,
		Payload:     p,
	})
}

// cloneData 生成内存根的深拷贝：新根中的任何修改（含 fsEvent 与步骤切片）
// 都不会影响旧根，保证落盘失败时对外可见状态不变。
func cloneData(d *fsData) *fsData {
	n := newData()
	n.seq = d.seq
	n.claimSeq = d.claimSeq
	for k, b := range d.bindings {
		bc := b
		n.bindings[k] = bc
	}
	for id, st := range d.execs {
		n.execs[id] = cloneState(st)
	}
	// 事件逐个复制结构（含其负载中的切片），活动索引与审计链共享新指针。
	ptr := make(map[string]*fsEvent, len(d.events))
	for _, old := range d.history {
		cp := *old
		ev := old.event
		if ep, ok := ev.Payload.(txsaga.EventPayload); ok {
			if ep.SucceededSteps != nil {
				ep.SucceededSteps = append([]string(nil), ep.SucceededSteps...)
			}
			ev.Payload = ep
		}
		cp.event = ev
		ptr[cp.event.ID] = &cp
	}
	n.events = ptr
	n.pending = append([]string(nil), d.pending...)
	for _, old := range d.history {
		n.history = append(n.history, ptr[old.event.ID])
	}
	return n
}

func cloneState(st *txsaga.ExecutionState) *txsaga.ExecutionState {
	c := *st // Payload 为不透明负载，按根包内存实现的口径只做接口值复制
	if st.Steps != nil {
		c.Steps = append([]txsaga.StepState(nil), st.Steps...)
	}
	if st.SucceededOrder != nil {
		c.SucceededOrder = append([]string(nil), st.SucceededOrder...)
	}
	return &c
}

// mutate 在锁内克隆当前根、应用变更、原子落盘，成功后替换内存根。
// 回调作用于克隆根；返回错误或落盘失败时内存根保持不变。
func (s *FileStore) mutate(ctx context.Context, fn func(d *fsData, now time.Time) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ready(ctx); err != nil {
		return err
	}
	next := cloneData(s.d)
	now := s.now()
	if err := fn(next, now); err != nil {
		return err
	}
	if err := s.persist(next); err != nil {
		// 落盘失败（含负载不可 JSON 表示）：不替换内存根，状态与事件都不变。
		return err
	}
	s.d = next
	return nil
}

// LoadSnapshot 实现 txsaga.Store：读取执行身份快照与业务键定义绑定。
func (s *FileStore) LoadSnapshot(ctx context.Context, businessKey, idempotencyKey string) (txsaga.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ready(ctx); err != nil {
		return txsaga.Snapshot{}, err
	}
	snap := txsaga.Snapshot{}
	if b, ok := s.d.bindings[businessKey]; ok {
		snap.Binding, snap.Bound = b, true
	}
	if st, ok := s.d.execs[joinExecID(businessKey, idempotencyKey)]; ok {
		snap.State = cloneState(st)
	}
	return snap, nil
}

// Commit 实现 txsaga.Store：回调内的状态变更与事件追加在同一份快照中
// 原子落盘（临时文件 + fsync + 改名），要么整体可见，要么整体不可见。
func (s *FileStore) Commit(ctx context.Context, businessKey, idempotencyKey string, fn func(tx txsaga.Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ready(ctx); err != nil {
		return err
	}

	next := cloneData(s.d)
	t := &fileTx{business: businessKey, idem: idempotencyKey}
	if b, ok := next.bindings[businessKey]; ok {
		bc := b
		t.binding = &bc
	}
	if st, ok := next.execs[joinExecID(businessKey, idempotencyKey)]; ok {
		t.state = st
	}

	if err := fn(t); err != nil {
		return err // 回调失败：状态副本与暂存事件一并丢弃，文件不动
	}

	now := s.now()
	id := joinExecID(businessKey, idempotencyKey)
	if t.state != nil {
		if t.created {
			if _, exists := next.execs[id]; exists {
				return errors.New("txsaga: execution already exists")
			}
			t.state.CreatedAt = now
			if _, ok := next.bindings[businessKey]; !ok {
				next.bindings[businessKey] = txsaga.Binding{
					SagaName:    t.state.SagaName,
					Fingerprint: t.state.Fingerprint,
				}
			}
		}
		t.state.UpdatedAt = now
		next.execs[id] = t.state
	}

	for i := range t.staged {
		ev := t.staged[i]
		next.seq++
		ev.ID = fmt.Sprintf("evt-%020d", next.seq)
		ev.OccurredAt = now
		me := &fsEvent{event: ev, idempotencyKey: idempotencyKey, alive: true}
		next.events[ev.ID] = me
		next.pending = append(next.pending, ev.ID)
		next.history = append(next.history, me)
	}

	// 先在内存中编码成功才触碰文件：负载不被 JSON 稳定支持时整体失败，
	// 不会留下状态或事件的半次提交。
	if err := s.persist(next); err != nil {
		return err
	}
	s.d = next
	return nil
}

// ClaimPendingEvents 实现 txsaga.Store，语义与 MemoryStore 一致：
// 领取即锁定（直到 Ack/Nack），有效租约内的事件不被无租约领取取得。
func (s *FileStore) ClaimPendingEvents(ctx context.Context, max int) ([]*txsaga.Event, error) {
	var out []*txsaga.Event
	err := s.mutate(ctx, func(d *fsData, now time.Time) error {
		if max <= 0 {
			max = 16
		}
		out = make([]*txsaga.Event, 0, max)
		remaining := d.pending[:0]
		for _, eid := range d.pending {
			me := d.events[eid]
			if me == nil || !me.alive {
				continue // 已 Ack，丢弃墓碑 ID
			}
			if me.claimID != "" && now.Before(me.claimedUntil) {
				remaining = append(remaining, eid)
				continue
			}
			if len(out) >= max {
				remaining = append(remaining, eid)
				continue
			}
			me.claimed = true
			me.claimID = ""
			me.claimedUntil = time.Time{}
			me.event = me.event.WithDeliveryMeta(me.event.Deliveries()+1, now)
			cp := me.event
			out = append(out, &cp)
		}
		d.pending = remaining
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ClaimPendingEventsLeased 实现 txsaga.ClaimLeaseStore：从未领取或租约已
// 到期的事件可领取，每次生成新 ClaimID；事件在队列中原位保留，故到期
// 恢复与 Nack 都不改变追加顺序。
func (s *FileStore) ClaimPendingEventsLeased(ctx context.Context, ttl time.Duration, max int) ([]txsaga.ClaimedEvent, error) {
	var out []txsaga.ClaimedEvent
	err := s.mutate(ctx, func(d *fsData, now time.Time) error {
		if max <= 0 {
			max = 16
		}
		out = make([]txsaga.ClaimedEvent, 0, max)
		live := d.pending[:0]
		for _, eid := range d.pending {
			me := d.events[eid]
			if me == nil || !me.alive {
				continue
			}
			if me.claimID != "" && now.Before(me.claimedUntil) {
				live = append(live, eid)
				continue
			}
			if len(out) >= max {
				live = append(live, eid)
				continue
			}
			d.claimSeq++
			cid := fmt.Sprintf("claim-%020d", d.claimSeq)
			me.claimID = cid
			me.claimedUntil = now.Add(ttl)
			me.event = me.event.WithDeliveryMeta(me.event.Deliveries()+1, now)
			cp := me.event
			out = append(out, txsaga.ClaimedEvent{
				Event:        &cp,
				ClaimID:      cid,
				ClaimedUntil: me.claimedUntil,
			})
			live = append(live, eid)
		}
		d.pending = live
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// AckEvent 实现 txsaga.Store：标记投递成功。活动副本退出领取与待领取队列，
// 审计链中的副本永久保留；事件不存在时返回错误。
func (s *FileStore) AckEvent(ctx context.Context, eventID string) error {
	return s.mutate(ctx, func(d *fsData, _ time.Time) error {
		me, ok := d.events[eventID]
		if !ok || !me.alive {
			return fmt.Errorf("txsaga: event %q not found", eventID)
		}
		me.alive = false
		me.claimed = false
		me.claimID = ""
		me.claimedUntil = time.Time{}
		return nil
	})
}

// AckLeasedEvent 实现 txsaga.ClaimLeaseStore：仅当前 claimID 可确认；
// 过期或他人领取返回包装了 txsaga.ErrStaleClaim 的错误且不改动事件。
func (s *FileStore) AckLeasedEvent(ctx context.Context, eventID, claimID string) error {
	return s.mutate(ctx, func(d *fsData, _ time.Time) error {
		me, ok := d.events[eventID]
		if !ok || !me.alive {
			return fmt.Errorf("txsaga: event %q not found", eventID)
		}
		if me.claimID != claimID {
			return fmt.Errorf("txsaga: event %q claim %q is not current: %w", eventID, claimID, txsaga.ErrStaleClaim)
		}
		me.alive = false
		return nil
	})
}

// NackEvent 实现 txsaga.Store：解除普通领取锁定并退回待领取队列；
// 未在领取中的事件幂等处理。
func (s *FileStore) NackEvent(ctx context.Context, eventID string) error {
	return s.mutate(ctx, func(d *fsData, _ time.Time) error {
		me, ok := d.events[eventID]
		if !ok || !me.alive {
			return fmt.Errorf("txsaga: event %q not found", eventID)
		}
		if !me.claimed {
			return nil
		}
		me.claimed = false
		d.pending = append(d.pending, eventID)
		return nil
	})
}

// NackLeasedEvent 实现 txsaga.ClaimLeaseStore：仅当前 claimID 可退回，
// 退回后租约立即解除、事件可被领取，追加顺序不变。
func (s *FileStore) NackLeasedEvent(ctx context.Context, eventID, claimID string) error {
	return s.mutate(ctx, func(d *fsData, _ time.Time) error {
		me, ok := d.events[eventID]
		if !ok || !me.alive {
			return fmt.Errorf("txsaga: event %q not found", eventID)
		}
		if me.claimID != claimID {
			return fmt.Errorf("txsaga: event %q claim %q is not current: %w", eventID, claimID, txsaga.ErrStaleClaim)
		}
		me.claimID = ""
		me.claimedUntil = time.Time{}
		return nil
	})
}

// PendingCount 返回当前立即可领取的活动事件数（不含有效租约内事件），
// 与 MemoryStore.PendingCount 的观测口径一致，便于观测与测试。
func (s *FileStore) PendingCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	n := 0
	for _, eid := range s.d.pending {
		me := s.d.events[eid]
		if me == nil || !me.alive {
			continue
		}
		if me.claimID != "" && now.Before(me.claimedUntil) {
			continue
		}
		n++
	}
	return n
}

// TotalEvents 返回尚未 Ack 的活动事件数（含领取中、待投递），
// 与 MemoryStore.TotalEvents 的观测口径一致；已 Ack 事件只在历史中保留。
func (s *FileStore) TotalEvents() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, me := range s.d.events {
		if me.alive {
			n++
		}
	}
	return n
}
