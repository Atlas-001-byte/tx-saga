package txsaga

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// 死信相关的哨兵错误，调用方可用 errors.Is 判定。
var (
	// ErrDeadLetterUnsupported Relay 配置了 WithMaxDeliveries，或调用方请求
	// 死信重新入队，但 Store 未实现 DeadLetterStore。未配置投递上限时，
	// 任何 Store 的领取、Ack、Nack 与至少一次重投语义都不需要本接口。
	ErrDeadLetterUnsupported = errors.New("txsaga: store does not support dead letters")
	// ErrEventNotFound 重新入队的事件 ID 不存在。
	ErrEventNotFound = errors.New("txsaga: event not found")
	// ErrEventNotDeadLettered 重新入队的事件不在死信状态：尚可投递、
	// 领取中或已投递的事件都返回该错误，事件不发生任何变化。
	ErrEventNotDeadLettered = errors.New("txsaga: event is not dead-lettered")
)

// DeadLetterStore 是在 Store 之上可选实现的有限投递与死信接口。
//
// Relay 通过 WithMaxDeliveries 配置单轮投递次数上限后，要求 Store 实现
// 本接口，否则 DeliverOnce 与 Run 返回 ErrDeadLetterUnsupported。语义：
//
//   - 每次领取使事件的累计 Deliveries 加一；Publisher 返回 nil 仍走 Ack；
//   - Publisher 返回错误时，本轮投递次数（累计 Deliveries 减去最近一次
//     重新入队时的基数）未达上限则 Nack 保留，达到上限则在同一存储操作中
//     原子转为死信并退出普通领取；
//   - 领取时（如租约到期恢复）发现本轮次数已用尽的事件不再交给
//     Publisher，而是在该次领取操作中直接隔离为死信；
//   - 死信保留事件 ID、类型、发生时间、负载、累计 Deliveries、最近领取
//     时间、失败原因与进入死信时间；
//   - RequeueDeadLetterEvent 只作用于死信事件：清除领取锁并重新入队，
//     开启新一轮有限计数；累计 Deliveries 与历史失败信息不回退。
type DeadLetterStore interface {
	Store

	// ClaimPendingEventsBounded 与 ClaimPendingEvents 相同，但按
	// maxDeliveries 上限领取：本轮投递次数已达上限的事件不再返回，
	// 而是在本次领取操作中原子转为死信。返回本次领取的事件与本次
	// 操作中被隔离为死信的事件数。max<=0 表示实现自选批量。
	ClaimPendingEventsBounded(ctx context.Context, maxDeliveries, max int) (claimed []*Event, deadLettered int, err error)

	// NackEventBounded 发送失败后退回事件：本轮投递次数未达
	// maxDeliveries 时与 NackEvent 相同（解除领取锁定、保留事件）；
	// 达到上限时在同一操作中原子转为死信（记录失败原因 reason），
	// 事件退出普通领取。返回是否转为死信。
	NackEventBounded(ctx context.Context, eventID, reason string, maxDeliveries int) (deadLettered bool, err error)

	// RequeueDeadLetterEvent 将死信事件重新入队：清除领取锁、恢复为
	// 待投递，开启新一轮有限计数；累计 Deliveries、最近失败原因与进入
	// 死信时间作为历史信息保留。事件不存在返回 ErrEventNotFound；
	// 事件不在死信状态（尚可投递、领取中或已投递）返回
	// ErrEventNotDeadLettered；ctx 取消时返回该错误且不改变事件。
	// 本操作与并发领取、Ack、Nack 原子互斥；重复调用不追加事件、
	// 不改变事件顺序，也不制造第二个副本。
	RequeueDeadLetterEvent(ctx context.Context, eventID string) error
}

// DeadLetterLeaseStore 是同时支持带租约领取与有限投递死信的接口。
// Relay 同时配置 WithClaimLease 与 WithMaxDeliveries 时要求 Store 实现
// 本接口，否则 DeliverOnce 与 Run 返回 ErrDeadLetterUnsupported。
type DeadLetterLeaseStore interface {
	DeadLetterStore
	ClaimLeaseStore

	// ClaimPendingEventsLeasedBounded 与 ClaimPendingEventsLeased 相同，
	// 但按 maxDeliveries 上限领取：租约到期恢复时发现本轮投递次数已
	// 用尽的事件不再返回，而是在本次领取操作中原子隔离为死信。
	// 返回本次领取的租约与本次操作中被隔离为死信的事件数。
	ClaimPendingEventsLeasedBounded(ctx context.Context, ttl time.Duration, maxDeliveries, max int) (claimed []ClaimedEvent, deadLettered int, err error)

	// NackLeasedEventBounded 按当前 claimID 退回事件：本轮投递次数未达
	// maxDeliveries 时与 NackLeasedEvent 相同；达到上限时在同一操作中
	// 原子转为死信（记录失败原因 reason），事件退出普通领取。
	// claimID 不是该事件当前有效领取时返回 ErrStaleClaim，且不改动事件。
	// 返回是否转为死信。
	NackLeasedEventBounded(ctx context.Context, eventID, claimID, reason string, maxDeliveries int) (deadLettered bool, err error)
}

// RequeueDeadLetterEvent 将处于死信状态的 Outbox 事件重新入队，使其按
// 当前 Relay 的投递上限开始新一轮有限投递。
//
// 只作用于死信事件：事件不存在返回 ErrEventNotFound；尚可投递、领取中
// 或已投递返回 ErrEventNotDeadLettered；Store 未实现 DeadLetterStore
// 返回 ErrDeadLetterUnsupported；ctx 取消时返回该错误且不改变事件。
// 成功调用清除领取锁并恢复待投递：累计 Deliveries 与历史失败信息不回退，
// 新一轮重新计数，再次耗尽仍进入死信。重复调用不追加事件、不改变顺序，
// 也不制造第二个副本。
func (e *Engine) RequeueDeadLetterEvent(ctx context.Context, eventID string) error {
	ds, ok := e.store.(DeadLetterStore)
	if !ok {
		return ErrDeadLetterUnsupported
	}
	return ds.RequeueDeadLetterEvent(ctx, eventID)
}

// ---- MemoryStore 的有限投递与死信实现 ----

// removePendingLocked 从待领取队列移除事件 ID（死信隔离时使用）。
// 调用方须持有 s.mu。
func (s *MemoryStore) removePendingLocked(eventID string) {
	kept := s.pending[:0]
	for _, id := range s.pending {
		if id != eventID {
			kept = append(kept, id)
		}
	}
	s.pending = kept
}

// deadLetterLocked 把事件转为死信：清除全部领取标记并记录进入死信时间。
// 调用方须持有 s.mu，并负责把事件移出待领取队列（领取扫描路径通过
// 不保留该 ID 实现，退回路径显式调用 removePendingLocked）。
func (s *MemoryStore) deadLetterLocked(me *memEvent, now time.Time) {
	me.dead = true
	me.claimed = false
	me.claimID = ""
	me.claimedUntil = time.Time{}
	me.deadLetteredAt = now
}

// roundExhaustedLocked 报告本轮投递次数是否已达上限。调用方须持有 s.mu。
func roundExhaustedLocked(me *memEvent, maxDeliveries int) bool {
	return me.event.deliveries-me.roundBase >= maxDeliveries
}

// ClaimPendingEventsBounded 实现 DeadLetterStore。
func (s *MemoryStore) ClaimPendingEventsBounded(_ context.Context, maxDeliveries, max int) ([]*Event, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if max <= 0 {
		max = 16
	}
	now := s.now()
	out := make([]*Event, 0, max)
	dead := 0
	remaining := s.pending[:0]
	for _, id := range s.pending {
		me := s.events[id]
		if me == nil || me.dead {
			continue // 已 Ack 移除或已死信隔离，丢弃墓碑 ID
		}
		if me.claimID != "" && now.Before(me.claimedUntil) {
			remaining = append(remaining, id)
			continue
		}
		if roundExhaustedLocked(me, maxDeliveries) {
			// 本轮次数已用尽（如租约到期恢复）：不再交给 Publisher，
			// 在本次领取操作中直接隔离为死信。
			s.deadLetterLocked(me, now)
			dead++
			continue
		}
		if len(out) >= max {
			remaining = append(remaining, id)
			continue
		}
		me.claimed = true
		me.claimID = ""
		me.claimedUntil = time.Time{}
		me.event.deliveries++
		me.event.lastAttemptAt = now
		cp := me.event
		out = append(out, &cp)
	}
	s.pending = remaining
	return out, dead, nil
}

// NackEventBounded 实现 DeadLetterStore：未达上限退回保留，达到上限在
// 同一操作中原子转为死信。
func (s *MemoryStore) NackEventBounded(_ context.Context, eventID, reason string, maxDeliveries int) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	me, ok := s.events[eventID]
	if !ok {
		return false, fmt.Errorf("txsaga: event %q not found", eventID)
	}
	if me.dead {
		return true, nil // 已被隔离：幂等
	}
	if !me.claimed {
		return false, nil // 幂等退回：未在领取中的事件无需处理
	}
	me.lastError = reason
	if roundExhaustedLocked(me, maxDeliveries) {
		s.deadLetterLocked(me, s.now())
		return true, nil
	}
	me.claimed = false
	s.pending = append(s.pending, eventID)
	return false, nil
}

// ClaimPendingEventsLeasedBounded 实现 DeadLetterLeaseStore。
func (s *MemoryStore) ClaimPendingEventsLeasedBounded(_ context.Context, ttl time.Duration, maxDeliveries, max int) ([]ClaimedEvent, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if max <= 0 {
		max = 16
	}
	now := s.now()
	out := make([]ClaimedEvent, 0, max)
	dead := 0
	live := s.pending[:0]
	for _, id := range s.pending {
		me := s.events[id]
		if me == nil || me.dead {
			continue // 已 Ack 移除或已死信隔离，丢弃墓碑 ID
		}
		if me.claimID != "" && now.Before(me.claimedUntil) {
			live = append(live, id) // 有效租约内：原位保留，本次跳过
			continue
		}
		if roundExhaustedLocked(me, maxDeliveries) {
			// 租约到期恢复时发现本轮次数已用尽：直接隔离为死信，
			// 不再生成新租约、不再交给 Publisher。
			s.deadLetterLocked(me, now)
			dead++
			continue
		}
		if len(out) >= max {
			live = append(live, id)
			continue
		}
		s.claimSeq++
		cid := fmt.Sprintf("claim-%020d", s.claimSeq)
		me.claimID = cid
		me.claimedUntil = now.Add(ttl)
		me.event.deliveries++
		me.event.lastAttemptAt = now
		cp := me.event
		out = append(out, ClaimedEvent{
			Event:        &cp,
			ClaimID:      cid,
			ClaimedUntil: me.claimedUntil,
		})
		live = append(live, id)
	}
	s.pending = live
	return out, dead, nil
}

// NackLeasedEventBounded 实现 DeadLetterLeaseStore：仅当前 claimID 可
// 退回；达到上限时在同一操作中原子转为死信。
func (s *MemoryStore) NackLeasedEventBounded(_ context.Context, eventID, claimID, reason string, maxDeliveries int) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	me, ok := s.events[eventID]
	if !ok {
		return false, fmt.Errorf("txsaga: event %q not found", eventID)
	}
	if me.claimID != claimID {
		return false, fmt.Errorf("txsaga: event %q claim %q is not current: %w", eventID, claimID, ErrStaleClaim)
	}
	me.lastError = reason
	if roundExhaustedLocked(me, maxDeliveries) {
		s.deadLetterLocked(me, s.now())
		s.removePendingLocked(eventID) // 租约事件原位于待领取队列中，移出
		return true, nil
	}
	me.claimID = ""
	me.claimedUntil = time.Time{}
	return false, nil
}

// RequeueDeadLetterEvent 实现 DeadLetterStore：只作用于死信事件，
// 与并发领取、Ack、Nack 在同一把锁内原子互斥。
func (s *MemoryStore) RequeueDeadLetterEvent(ctx context.Context, eventID string) error {
	if err := ctx.Err(); err != nil {
		return err // ctx 取消：不改变事件
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	me, ok := s.events[eventID]
	if !ok {
		// 已 Ack 的事件活动副本已移除，但审计链仍持有指针：据此把
		// “已投递”与“不存在”区分开，二者错误口径不同。
		for _, h := range s.history {
			if h.event.ID == eventID && h.acked {
				return fmt.Errorf("txsaga: event %q already delivered: %w", eventID, ErrEventNotDeadLettered)
			}
		}
		return fmt.Errorf("txsaga: event %q: %w", eventID, ErrEventNotFound)
	}
	if !me.dead {
		return fmt.Errorf("txsaga: event %q is deliverable: %w", eventID, ErrEventNotDeadLettered)
	}
	// 清除领取锁、恢复待投递；累计 Deliveries、lastError 与
	// deadLetteredAt 作为历史信息保留，不回退。
	me.dead = false
	me.claimed = false
	me.claimID = ""
	me.claimedUntil = time.Time{}
	me.roundBase = me.event.deliveries // 新一轮有限计数从零开始
	s.pending = append(s.pending, eventID)
	return nil
}
