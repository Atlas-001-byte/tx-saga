// Package filestore 提供 txsaga 的本地目录持久化存储。
//
// FileStore 实现 txsaga.Store、txsaga.ClaimLeaseStore 与
// txsaga.EventHistoryStore，语义与 txsaga.MemoryStore 的可观察结果一致：
// 每次 Commit 把执行状态变更与当次 Outbox 事件整体写入目录下的单个
// JSON 快照（临时文件 + fsync + 原子重命名）。写入返回成功后，重新打开
// 目录即可继续执行、查询终态并读取完整事件历史；写入被中断时旧快照保持
// 完整，不会只留下状态或只留下事件。
//
// FileStore 支持单进程内多个 goroutine 共享同一目录；同一进程重复打开
// 同一目录得到 ErrStoreLocked。不保证多个进程同时写同一目录。
// 本包只依赖 Go 标准库与 txsaga 包。
package filestore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/txsaga/txsaga"
)

// 哨兵错误，调用方可用 errors.Is 判定。
var (
	// ErrStoreOpen 目录不可创建、不可读或不可写，构造失败。
	ErrStoreOpen = errors.New("filestore: cannot open store directory")
	// ErrStoreLocked 同一进程已有另一 FileStore 占用该目录。
	ErrStoreLocked = errors.New("filestore: directory already opened by another FileStore")
	// ErrCorruptStore 已有数据损坏、被截断或无法组成一致快照。
	// 返回该错误时不覆盖、不删除原文件。
	ErrCorruptStore = errors.New("filestore: stored data is corrupt")
	// ErrUnsupportedPayload Payload 或 EventPayload 无法用 JSON 稳定表示，
	// 写入被拒绝且不留下半次提交。
	ErrUnsupportedPayload = errors.New("filestore: payload is not JSON-representable")

	// errStoreClosed 在 Close 之后调用存储方法。
	errStoreClosed = errors.New("filestore: store is closed")
)

const (
	// storeFileName 是目录内快照文件名；写入经同目录临时文件原子重命名。
	storeFileName = "store.json"
	// formatVersion 是磁盘格式版本，不认识的版本一律按损坏处理。
	formatVersion = 1
	// defaultEventHistoryLimit 与 txsaga 包内 EventHistoryQuery 的默认页大小一致。
	defaultEventHistoryLimit = 100
)

// 编译期断言：FileStore 同时满足 Store、ClaimLeaseStore 与 EventHistoryStore。
var (
	_ txsaga.Store             = (*FileStore)(nil)
	_ txsaga.ClaimLeaseStore   = (*FileStore)(nil)
	_ txsaga.EventHistoryStore = (*FileStore)(nil)
)

// registry 是进程内目录占用登记表：同一目录同时只允许一个 FileStore。
// 多进程互斥不在本包范围内。
var registry = struct {
	sync.Mutex
	dirs map[string]struct{}
}{dirs: make(map[string]struct{})}

// fileEvent 对应 MemoryStore 的 memEvent：事件本体、所属执行的外部幂等键、
// 领取/租约状态与投递元数据；acked 表示已确认投递，事件仅从活动 Outbox
// 移除，审计副本永久保留在历史链中。
type fileEvent struct {
	id             string
	businessKey    string
	idempotencyKey string
	typ            string
	occurredAt     time.Time
	payload        txsaga.EventPayload

	// claimed 是普通领取（ClaimPendingEvents）的锁定标记。锁随进程存亡：
	// 落盘时记录该标记，但加载时不恢复锁定——进程退出即视为 worker 失联，
	// 重开后未 Ack 的事件重新入队、恢复可领取（至少一次语义）。
	claimed bool

	// 带租约领取的状态，按墙钟时间持久化：租约到期后（含重开之后）
	// 事件可被重新领取并获得新的 claimID。
	claimID      string
	claimedUntil time.Time

	deliveries    int
	lastAttemptAt time.Time
	acked         bool
}

// storeData 是 FileStore 的全部可持久状态，与磁盘快照一一对应。
type storeData struct {
	seq      uint64
	claimSeq uint64
	execs    map[string]*txsaga.ExecutionState
	bindings map[string]txsaga.Binding
	events   map[string]*fileEvent
	pending  []string // 未领取（或租约期内）事件 ID，保持追加顺序
	history  []string // 只增审计链：全部事件 ID（含已 Ack），按追加顺序
}

func newStoreData() *storeData {
	return &storeData{
		execs:    make(map[string]*txsaga.ExecutionState),
		bindings: make(map[string]txsaga.Binding),
		events:   make(map[string]*fileEvent),
	}
}

// clone 复制 map 与切片头；元素指针仍共享，修改元素前必须先复制元素本身。
func (d *storeData) clone() *storeData {
	return &storeData{
		seq:      d.seq,
		claimSeq: d.claimSeq,
		execs:    maps.Clone(d.execs),
		bindings: maps.Clone(d.bindings),
		events:   maps.Clone(d.events),
		pending:  slices.Clone(d.pending),
		history:  slices.Clone(d.history),
	}
}

// FileStore 是基于本地目录的持久化 Store。零值不可用，请用 Open 创建；
// 不再使用时调用 Close 释放目录占用。
type FileStore struct {
	mu     sync.Mutex
	dir    string
	key    string // registry 中的规范化目录键
	data   *storeData
	closed bool
	now    func() time.Time
}

// Open 打开（必要时创建）dir 下的持久化存储。
//
// 目录不可创建或不可读写时返回 ErrStoreOpen；同一进程已有另一 FileStore
// 占用该目录时返回 ErrStoreLocked；已有快照损坏、被截断或无法组成一致
// 快照时返回 ErrCorruptStore，且不改动目录内任何文件。
func Open(dir string) (*FileStore, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("filestore: resolve %q: %v: %w", dir, err, ErrStoreOpen)
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return nil, fmt.Errorf("filestore: create directory %q: %v: %w", abs, err, ErrStoreOpen)
	}
	key := abs
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		key = resolved // 符号链接与不同写法归一到同一目录键
	}

	registry.Lock()
	if _, ok := registry.dirs[key]; ok {
		registry.Unlock()
		return nil, fmt.Errorf("filestore: directory %q: %w", abs, ErrStoreLocked)
	}
	registry.dirs[key] = struct{}{}
	registry.Unlock()

	s := &FileStore{dir: abs, key: key, now: time.Now}
	if err := s.open(); err != nil {
		registry.Lock()
		delete(registry.dirs, key)
		registry.Unlock()
		return nil, err
	}
	return s, nil
}

// open 校验目录可写并加载既有快照；加载失败时不触碰目录内任何文件。
func (s *FileStore) open() error {
	probe, err := os.CreateTemp(s.dir, ".probe-*")
	if err != nil {
		return fmt.Errorf("filestore: directory %q not writable: %v: %w", s.dir, err, ErrStoreOpen)
	}
	_ = probe.Close()
	_ = os.Remove(probe.Name())

	raw, err := os.ReadFile(filepath.Join(s.dir, storeFileName))
	switch {
	case errors.Is(err, os.ErrNotExist):
		s.data = newStoreData()
	case err != nil:
		return fmt.Errorf("filestore: read store: %v: %w", err, ErrStoreOpen)
	default:
		data, err := decodeStore(raw)
		if err != nil {
			return err // 已包装 ErrCorruptStore；原文件保持不动
		}
		s.data = data
	}
	return nil
}

// Close 释放目录占用。已持久化的数据不受影响，可再次 Open 同一目录。
// 重复调用安全。
func (s *FileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	registry.Lock()
	delete(registry.dirs, s.key)
	registry.Unlock()
	return nil
}

// persistLocked 把 d 编码为 JSON 并原子替换快照文件，成功后切换内存状态。
// 编码失败（ErrUnsupportedPayload）或写盘失败时内存与磁盘都保持原状，
// 不留下半次提交。调用方必须持有 s.mu。
func (s *FileStore) persistLocked(d *storeData) error {
	raw, err := encodeStore(d)
	if err != nil {
		return err
	}
	if err := writeFileAtomic(s.dir, storeFileName, raw); err != nil {
		return fmt.Errorf("filestore: persist store: %w", err)
	}
	s.data = d
	return nil
}

func execID(businessKey, idempotencyKey string) string {
	return businessKey + "\x00" + idempotencyKey
}

func cloneState(s *txsaga.ExecutionState) *txsaga.ExecutionState {
	if s == nil {
		return nil
	}
	c := *s
	if s.Steps != nil {
		c.Steps = append([]txsaga.StepState(nil), s.Steps...)
	}
	if s.SucceededOrder != nil {
		c.SucceededOrder = append([]string(nil), s.SucceededOrder...)
	}
	return &c
}

// fileTx 是 Commit 回调中的事务视图，语义与 MemoryStore 的 memTx 一致。
type fileTx struct {
	business string
	idem     string
	binding  *txsaga.Binding
	state    *txsaga.ExecutionState
	created  bool
	staged   []stagedEvent
}

type stagedEvent struct {
	typ     string
	payload txsaga.EventPayload
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
	if p.SucceededSteps != nil {
		p.SucceededSteps = append([]string(nil), p.SucceededSteps...)
	}
	t.staged = append(t.staged, stagedEvent{typ: eventType, payload: p})
}

// LoadSnapshot 实现 txsaga.Store 接口。
func (s *FileStore) LoadSnapshot(ctx context.Context, businessKey, idempotencyKey string) (txsaga.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return txsaga.Snapshot{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return txsaga.Snapshot{}, errStoreClosed
	}

	snap := txsaga.Snapshot{}
	if b, ok := s.data.bindings[businessKey]; ok {
		snap.Binding, snap.Bound = b, true
	}
	if st, ok := s.data.execs[execID(businessKey, idempotencyKey)]; ok {
		snap.State = cloneState(st)
	}
	return snap, nil
}

// Commit 实现 txsaga.Store 接口：在副本上运行回调并暂存修改，
// 回调成功后把状态与事件整体编码、原子落盘，再切换内存状态；
// 回调失败、ctx 取消或编码/写盘失败时不留下任何半次提交。
func (s *FileStore) Commit(ctx context.Context, businessKey, idempotencyKey string, fn func(txsaga.Tx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errStoreClosed
	}

	nd := s.data.clone()
	t := &fileTx{business: businessKey, idem: idempotencyKey}
	if b, ok := nd.bindings[businessKey]; ok {
		bc := b
		t.binding = &bc
	}
	id := execID(businessKey, idempotencyKey)
	if st, ok := nd.execs[id]; ok {
		t.state = cloneState(st)
	}

	if err := fn(t); err != nil {
		return err // 回调失败：staged 事件与状态副本一并丢弃
	}
	if err := ctx.Err(); err != nil {
		return err // 提交前取消：已提交数据不受影响
	}

	now := s.now()
	if t.state != nil {
		if t.created {
			if _, exists := nd.execs[id]; exists {
				return errors.New("txsaga: execution already exists")
			}
			t.state.CreatedAt = now
			// 首次执行建立业务键与定义的绑定；同键的后续执行定义一致，覆盖无害。
			if _, ok := nd.bindings[businessKey]; !ok {
				nd.bindings[businessKey] = txsaga.Binding{SagaName: t.state.SagaName, Fingerprint: t.state.Fingerprint}
			}
		}
		t.state.UpdatedAt = now
		nd.execs[id] = cloneState(t.state)
	}

	for _, ev := range t.staged {
		nd.seq++
		fe := &fileEvent{
			id:             fmt.Sprintf("evt-%020d", nd.seq),
			businessKey:    businessKey,
			idempotencyKey: idempotencyKey,
			typ:            ev.typ,
			occurredAt:     now,
			payload:        ev.payload,
		}
		nd.events[fe.id] = fe
		nd.pending = append(nd.pending, fe.id)
		// 与状态、活动事件在同一次原子落盘中追加，历史读取只见完整提交。
		nd.history = append(nd.history, fe.id)
	}
	return s.persistLocked(nd)
}

// eventFrom 把内部事件复制为脱离存储的领取结果。
func eventFrom(me *fileEvent) *txsaga.Event {
	return txsaga.NewEvent(me.id, me.businessKey, me.typ, me.occurredAt,
		me.payload, me.deliveries, me.lastAttemptAt)
}

// ClaimPendingEvents 实现 txsaga.Store 接口，语义与 MemoryStore 一致：
// 领取即锁定并计数一次投递；已被领取或租约仍有效的事件不再被领取。
func (s *FileStore) ClaimPendingEvents(ctx context.Context, max int) ([]*txsaga.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errStoreClosed
	}

	if max <= 0 {
		max = 16
	}
	now := s.now()
	nd := s.data.clone()
	out := make([]*txsaga.Event, 0, max)
	dirty := false
	remaining := nd.pending[:0]
	for _, id := range nd.pending {
		me := nd.events[id]
		if me == nil || me.acked {
			dirty = true // 清除墓碑 ID
			continue
		}
		// 已被带租约领取且租约仍有效的事件不得被无租约领取再次取得。
		if me.claimID != "" && now.Before(me.claimedUntil) {
			remaining = append(remaining, id)
			continue
		}
		if len(out) >= max {
			remaining = append(remaining, id)
			continue
		}
		cp := *me
		cp.claimed = true
		// 接管已到期的租约事件时清除旧租约，避免遗留失效的 ClaimID。
		cp.claimID = ""
		cp.claimedUntil = time.Time{}
		cp.deliveries++
		cp.lastAttemptAt = now
		nd.events[id] = &cp
		out = append(out, eventFrom(&cp))
		dirty = true
	}
	nd.pending = remaining
	if dirty {
		if err := s.persistLocked(nd); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// ClaimPendingEventsLeased 实现 txsaga.ClaimLeaseStore 接口。
// 从未领取或当前租约已到期的事件可被领取；每次领取生成新的 ClaimID，
// 租约期内事件留在追加队列中原位跳过，到期后原位恢复，顺序不变。
func (s *FileStore) ClaimPendingEventsLeased(ctx context.Context, ttl time.Duration, max int) ([]txsaga.ClaimedEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errStoreClosed
	}

	if max <= 0 {
		max = 16
	}
	now := s.now()
	nd := s.data.clone()
	out := make([]txsaga.ClaimedEvent, 0, max)
	dirty := false
	live := nd.pending[:0]
	for _, id := range nd.pending {
		me := nd.events[id]
		if me == nil || me.acked {
			dirty = true // 清除墓碑 ID
			continue
		}
		if me.claimID != "" && now.Before(me.claimedUntil) {
			live = append(live, id) // 有效租约内：原位保留，本次跳过
			continue
		}
		if len(out) >= max {
			live = append(live, id) // 批量已满：保留顺序，下轮再领
			continue
		}
		nd.claimSeq++
		cid := fmt.Sprintf("claim-%020d", nd.claimSeq)
		cp := *me
		cp.claimID = cid
		cp.claimedUntil = now.Add(ttl)
		cp.deliveries++
		cp.lastAttemptAt = now
		nd.events[id] = &cp
		out = append(out, txsaga.ClaimedEvent{
			Event:        eventFrom(&cp),
			ClaimID:      cid,
			ClaimedUntil: cp.claimedUntil,
		})
		live = append(live, id) // 租约期内留在队列，到期后原位可恢复
		dirty = true
	}
	nd.pending = live
	if dirty {
		if err := s.persistLocked(nd); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// removeID 从 ids（调用方私有的副本）中移除 id，保持其余元素顺序。
func removeID(ids []string, id string) []string {
	out := ids[:0]
	for _, x := range ids {
		if x != id {
			out = append(out, x)
		}
	}
	return out
}

// AckEvent 实现 txsaga.Store 接口：事件从活动 Outbox 移除，
// 审计副本保留在历史链中，可继续通过 ListEvents 读取。
func (s *FileStore) AckEvent(ctx context.Context, eventID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errStoreClosed
	}
	me, ok := s.data.events[eventID]
	if !ok || me.acked {
		return fmt.Errorf("txsaga: event %q not found", eventID)
	}

	nd := s.data.clone()
	cp := *me
	cp.acked = true
	cp.claimed = false
	cp.claimID = ""
	cp.claimedUntil = time.Time{}
	nd.events[eventID] = &cp
	// 立即从领取队列移除（MemoryStore 留待下次领取时惰性清除墓碑，
	// 二者可观察行为一致）。
	nd.pending = removeID(nd.pending, eventID)
	return s.persistLocked(nd)
}

// AckLeasedEvent 实现 txsaga.ClaimLeaseStore 接口：仅当前 claimID 可确认。
func (s *FileStore) AckLeasedEvent(ctx context.Context, eventID, claimID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errStoreClosed
	}
	me, ok := s.data.events[eventID]
	if !ok || me.acked {
		return fmt.Errorf("txsaga: event %q not found", eventID)
	}
	if me.claimID != claimID {
		// 含从未租约领取、旧 ClaimID 已到期、已被新领取取代三种情形，
		// 均不得改动当前事件或新租约。
		return fmt.Errorf("txsaga: event %q claim %q is not current: %w", eventID, claimID, txsaga.ErrStaleClaim)
	}

	nd := s.data.clone()
	cp := *me
	cp.acked = true
	cp.claimID = ""
	cp.claimedUntil = time.Time{}
	nd.events[eventID] = &cp
	nd.pending = removeID(nd.pending, eventID)
	return s.persistLocked(nd)
}

// NackEvent 实现 txsaga.Store 接口：解除领取锁定，事件重新入队等待领取。
func (s *FileStore) NackEvent(ctx context.Context, eventID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errStoreClosed
	}
	me, ok := s.data.events[eventID]
	if !ok || me.acked {
		return fmt.Errorf("txsaga: event %q not found", eventID)
	}
	if !me.claimed {
		return nil // 幂等退回：未在领取中的事件无需处理
	}

	nd := s.data.clone()
	cp := *me
	cp.claimed = false
	nd.events[eventID] = &cp
	nd.pending = append(nd.pending, eventID)
	return s.persistLocked(nd)
}

// NackLeasedEvent 实现 txsaga.ClaimLeaseStore 接口：仅当前 claimID 可退回，
// 退回后租约解除、事件立即可领取，且保持原追加顺序。
func (s *FileStore) NackLeasedEvent(ctx context.Context, eventID, claimID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errStoreClosed
	}
	me, ok := s.data.events[eventID]
	if !ok || me.acked {
		return fmt.Errorf("txsaga: event %q not found", eventID)
	}
	if me.claimID != claimID {
		return fmt.Errorf("txsaga: event %q claim %q is not current: %w", eventID, claimID, txsaga.ErrStaleClaim)
	}

	nd := s.data.clone()
	cp := *me
	cp.claimID = ""
	cp.claimedUntil = time.Time{}
	nd.events[eventID] = &cp
	// 租约事件始终留在追加队列中，退回只解除租约：事件立即可领取，
	// 且相对其它事件的顺序不变。
	return s.persistLocked(nd)
}

// PendingCount 返回当前立即可领取的事件数（不含有效租约内的事件），
// 便于观测与测试。
func (s *FileStore) PendingCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	n := 0
	for _, id := range s.data.pending {
		me := s.data.events[id]
		if me == nil || me.acked {
			continue
		}
		if me.claimID != "" && now.Before(me.claimedUntil) {
			continue
		}
		n++
	}
	return n
}

// TotalEvents 返回存储中尚未 Ack 的事件数（含领取中、待投递）。
func (s *FileStore) TotalEvents() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, me := range s.data.events {
		if !me.acked {
			n++
		}
	}
	return n
}

// ListEvents 实现 txsaga.EventHistoryStore 接口：在同一把锁内扫描只增
// 审计链，每页只可能读到整次 Commit 已提交的事件；查询本身不修改任何
// 字段。已 Ack 的事件保留可审计副本。
func (s *FileStore) ListEvents(ctx context.Context, q txsaga.EventHistoryQuery) (txsaga.EventHistoryPage, error) {
	if err := ctx.Err(); err != nil {
		return txsaga.EventHistoryPage{}, err
	}
	limit := q.Limit
	if limit <= 0 {
		limit = defaultEventHistoryLimit
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return txsaga.EventHistoryPage{}, errStoreClosed
	}

	// 先校验执行身份与 Saga 名称：不存在（含仅有事件、没有执行记录的情形）
	// 或名称不符一律 ErrExecutionNotFound。
	st, ok := s.data.execs[execID(q.BusinessKey, q.IdempotencyKey)]
	if !ok || st.SagaName != q.SagaName {
		return txsaga.EventHistoryPage{}, txsaga.ErrExecutionNotFound
	}

	start := 0
	if q.AfterID != "" {
		idx := -1
		for i, id := range s.data.history {
			me := s.data.events[id]
			if me != nil && me.id == q.AfterID &&
				me.businessKey == q.BusinessKey &&
				me.idempotencyKey == q.IdempotencyKey {
				idx = i
				break
			}
		}
		// 游标不存在或属于其它执行：不返回部分结果，也不触碰任何状态。
		if idx < 0 {
			return txsaga.EventHistoryPage{}, txsaga.ErrEventCursorNotFound
		}
		start = idx + 1
	}

	page := txsaga.EventHistoryPage{Events: []txsaga.EventRecord{}}
	if q.AfterID != "" {
		// 空页（游标已是最后一条）时回传入参游标，重复查询保持幂等，
		// 不会因空游标而误回到链首。
		page.NextAfterID = q.AfterID
	}
	next := start // 本页之后第一个待检查的审计链位置
	for i := start; i < len(s.data.history) && len(page.Events) < limit; i++ {
		me := s.data.events[s.data.history[i]]
		if me == nil || me.businessKey != q.BusinessKey || me.idempotencyKey != q.IdempotencyKey {
			continue
		}
		page.Events = append(page.Events, toEventRecord(me))
		next = i + 1
	}
	if len(page.Events) > 0 {
		page.NextAfterID = page.Events[len(page.Events)-1].ID
	}
	for i := next; i < len(s.data.history); i++ {
		me := s.data.events[s.data.history[i]]
		if me != nil && me.businessKey == q.BusinessKey && me.idempotencyKey == q.IdempotencyKey {
			page.HasMore = true
			break
		}
	}
	return page, nil
}

// toEventRecord 把内部事件复制为脱离存储的只读记录。
func toEventRecord(me *fileEvent) txsaga.EventRecord {
	p := me.payload
	if p.SucceededSteps != nil {
		p.SucceededSteps = append([]string(nil), p.SucceededSteps...)
	}
	return txsaga.EventRecord{
		ID:            me.id,
		Type:          me.typ,
		OccurredAt:    me.occurredAt,
		Payload:       p,
		BusinessKey:   me.businessKey,
		Deliveries:    me.deliveries,
		LastAttemptAt: me.lastAttemptAt,
	}
}

// ---- 磁盘格式 ----

// diskStore 是 store.json 的顶层结构。普通领取的锁定标记（claimed）随快照
// 落盘，但加载时不恢复锁定：进程退出即 worker 失联，重开后未 Ack 的事件
// 重新入队、恢复可领取；租约按墙钟时间持久化，到期（含重开之后）自动恢复。
type diskStore struct {
	Version  int                       `json:"version"`
	Seq      uint64                    `json:"seq"`
	ClaimSeq uint64                    `json:"claim_seq"`
	Execs    map[string]diskExec       `json:"execs"`
	Bindings map[string]txsaga.Binding `json:"bindings"`
	Events   map[string]diskEvent      `json:"events"`
	Pending  []string                  `json:"pending"`
	History  []string                  `json:"history"`
}

// diskExec 在 ExecutionState 的 JSON 字段之外补充 Payload：
// txsaga.ExecutionState.Payload 本身标记为 json:"-"，这里以原始 JSON 保存。
type diskExec struct {
	txsaga.ExecutionState
	Payload json.RawMessage `json:"payload,omitempty"`
}

type diskEvent struct {
	ID             string          `json:"id"`
	BusinessKey    string          `json:"business_key"`
	IdempotencyKey string          `json:"idempotency_key"`
	Type           string          `json:"type"`
	OccurredAt     time.Time       `json:"occurred_at"`
	Payload        json.RawMessage `json:"payload"`
	Claimed        bool            `json:"claimed,omitempty"`
	ClaimID        string          `json:"claim_id,omitempty"`
	ClaimedUntil   time.Time       `json:"claimed_until,omitempty"`
	Deliveries     int             `json:"deliveries,omitempty"`
	LastAttemptAt  time.Time       `json:"last_attempt_at,omitempty"`
	Acked          bool            `json:"acked,omitempty"`
}

// encodeStore 把内存状态编码为快照字节。任一 Payload/EventPayload 无法用
// JSON 稳定表示时返回 ErrUnsupportedPayload，调用方据此放弃整次提交。
func encodeStore(d *storeData) ([]byte, error) {
	ds := diskStore{
		Version:  formatVersion,
		Seq:      d.seq,
		ClaimSeq: d.claimSeq,
		Execs:    make(map[string]diskExec, len(d.execs)),
		Bindings: d.bindings,
		Events:   make(map[string]diskEvent, len(d.events)),
		Pending:  d.pending,
		History:  d.history,
	}
	for k, st := range d.execs {
		de := diskExec{ExecutionState: *cloneState(st)}
		if st.Payload != nil {
			raw, err := json.Marshal(st.Payload)
			if err != nil {
				return nil, fmt.Errorf("filestore: encode payload of execution %q: %v: %w",
					st.BusinessKey, err, ErrUnsupportedPayload)
			}
			de.Payload = raw
		}
		ds.Execs[k] = de
	}
	for k, ev := range d.events {
		raw, err := json.Marshal(ev.payload)
		if err != nil {
			return nil, fmt.Errorf("filestore: encode payload of event %q: %v: %w",
				ev.id, err, ErrUnsupportedPayload)
		}
		ds.Events[k] = diskEvent{
			ID:             ev.id,
			BusinessKey:    ev.businessKey,
			IdempotencyKey: ev.idempotencyKey,
			Type:           ev.typ,
			OccurredAt:     ev.occurredAt,
			Payload:        raw,
			Claimed:        ev.claimed,
			ClaimID:        ev.claimID,
			ClaimedUntil:   ev.claimedUntil,
			Deliveries:     ev.deliveries,
			LastAttemptAt:  ev.lastAttemptAt,
			Acked:          ev.acked,
		}
	}
	out, err := json.Marshal(ds)
	if err != nil {
		return nil, fmt.Errorf("filestore: encode store: %v: %w", err, ErrUnsupportedPayload)
	}
	return out, nil
}

// decodeStore 解析并校验快照。任何解析失败、版本不符或引用不一致都返回
// 包装了 ErrCorruptStore 的错误；调用方不得据此覆盖原文件。
func decodeStore(raw []byte) (*storeData, error) {
	corrupt := func(format string, args ...any) error {
		return fmt.Errorf("filestore: %s: %w", fmt.Sprintf(format, args...), ErrCorruptStore)
	}

	var ds diskStore
	dec := json.NewDecoder(bytes.NewReader(raw))
	if err := dec.Decode(&ds); err != nil {
		return nil, corrupt("parse store: %v", err)
	}
	if dec.More() {
		return nil, corrupt("trailing data after store snapshot")
	}
	if ds.Version != formatVersion {
		return nil, corrupt("unsupported format version %d", ds.Version)
	}

	d := &storeData{
		seq:      ds.Seq,
		claimSeq: ds.ClaimSeq,
		execs:    make(map[string]*txsaga.ExecutionState, len(ds.Execs)),
		bindings: make(map[string]txsaga.Binding, len(ds.Bindings)),
		events:   make(map[string]*fileEvent, len(ds.Events)),
		pending:  slices.Clone(ds.Pending),
		history:  slices.Clone(ds.History),
	}
	for k, b := range ds.Bindings {
		d.bindings[k] = b
	}
	for k, de := range ds.Execs {
		de := de
		st := cloneState(&de.ExecutionState)
		if len(de.Payload) > 0 {
			var p any
			if err := json.Unmarshal(de.Payload, &p); err != nil {
				return nil, corrupt("decode payload of execution %q: %v", st.BusinessKey, err)
			}
			st.Payload = p
		}
		if k != execID(st.BusinessKey, st.IdempotencyKey) {
			return nil, corrupt("execution %q stored under inconsistent key", st.BusinessKey)
		}
		d.execs[k] = st
	}
	for k, dev := range ds.Events {
		var ep txsaga.EventPayload
		if len(dev.Payload) > 0 {
			if err := json.Unmarshal(dev.Payload, &ep); err != nil {
				return nil, corrupt("decode payload of event %q: %v", dev.ID, err)
			}
		}
		if dev.ID != k {
			return nil, corrupt("event %q stored under inconsistent key", dev.ID)
		}
		d.events[k] = &fileEvent{
			id:             dev.ID,
			businessKey:    dev.BusinessKey,
			idempotencyKey: dev.IdempotencyKey,
			typ:            dev.Type,
			occurredAt:     dev.OccurredAt,
			payload:        ep,
			claimed:        dev.Claimed,
			claimID:        dev.ClaimID,
			claimedUntil:   dev.ClaimedUntil,
			deliveries:     dev.Deliveries,
			lastAttemptAt:  dev.LastAttemptAt,
			acked:          dev.Acked,
		}
	}
	for _, id := range d.pending {
		if _, ok := d.events[id]; !ok {
			return nil, corrupt("pending event %q missing from store", id)
		}
	}
	for _, id := range d.history {
		if _, ok := d.events[id]; !ok {
			return nil, corrupt("history event %q missing from store", id)
		}
	}
	// 普通领取的锁定随进程存亡：加载时不恢复 claimed 锁定，把上次退出时
	// 仍被领取且未 Ack 的事件按追加顺序重新入队，恢复为可领取（至少一次）。
	// 租约锁按墙钟时间恢复，不在此处理。
	for _, id := range d.history {
		me := d.events[id]
		if me.claimed && !me.acked {
			me.claimed = false
			d.pending = append(d.pending, id)
		}
	}
	return d, nil
}

// writeFileAtomic 把 data 写入 dir/name：先写同目录临时文件并 fsync，
// 再原子重命名，最后尽力 fsync 目录。任何一步失败都不会损坏既有文件。
func writeFileAtomic(dir, name string, data []byte) error {
	tmp, err := os.CreateTemp(dir, "."+name+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, filepath.Join(dir, name)); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}
