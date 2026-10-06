package txsaga

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
)

// StepState 是单个步骤的持久状态。取值见 StepResult* 常量；
// 空串表示该步骤尚未被执行到（pending）。
type StepState struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	// Attempts 正向动作被实际调用的次数；已确认成功后重试不会增加。
	Attempts   int       `json:"attempts"`
	LastError  string    `json:"last_error,omitempty"`
	StartedAt  time.Time `json:"started_at,omitempty"`
	FinishedAt time.Time `json:"finished_at,omitempty"`
	// CompensationAttempts 补偿动作被实际调用的次数；已确认补偿后重试不会增加。
	CompensationAttempts int       `json:"compensation_attempts,omitempty"`
	CompensatedAt        time.Time `json:"compensated_at,omitempty"`
}

// ExecutionState 是一次执行的完整持久状态，Store 的实现负责其持久化。
// 本包不规定磁盘文件格式：实现可自行选择 JSON、数据库行等任何载体。
type ExecutionState struct {
	BusinessKey            string      `json:"business_key"`
	SagaName               string      `json:"saga_name"`
	IdempotencyKey         string      `json:"idempotency_key"`
	Fingerprint            string      `json:"fingerprint"`
	Status                 string      `json:"status"`
	Steps                  []StepState `json:"steps"`
	FailureReason          string      `json:"failure_reason,omitempty"`
	FailedStep             string      `json:"failed_step,omitempty"`
	CompensationError      string      `json:"compensation_error,omitempty"`
	FailedCompensationStep string      `json:"failed_compensation_step,omitempty"`
	Payload                any         `json:"-"`
	CreatedAt              time.Time   `json:"created_at"`
	UpdatedAt              time.Time   `json:"updated_at"`
}

// Terminal 报告状态是否为固定终态。
func (s *ExecutionState) Terminal() bool {
	switch s.Status {
	case StatusCompleted, StatusFailed, StatusCompensationFailed:
		return true
	default:
		return false
	}
}

// Binding 是业务键与 Saga 定义的绑定关系。同一业务键首次执行后即绑定其定义，
// 之后以不同定义执行将得到 ErrDefinitionConflict。
type Binding struct {
	SagaName    string
	Fingerprint string
}

// Tx 是一次原子状态提交的事务视图。
//
// 引擎在 Commit 的回调中通过本接口读取既有状态、写入新状态并追加事件；
// 回调返回 nil 时，状态变更与全部事件由 Store 原子持久化，
// 回调返回错误时二者一并丢弃。基于内存的实现以互斥锁保证该原子性，
// 其它实现应使用数据库事务等同等机制。
type Tx interface {
	// Binding 返回该业务键当前绑定的 Saga 定义；未绑定时 ok 为 false。
	Binding() (b Binding, ok bool)
	// State 返回当前执行状态的可修改副本；执行尚不存在时返回 nil。
	// 对返回对象的修改仅在回调成功返回后生效。
	State() *ExecutionState
	// Create 建立执行状态（同时建立业务键与定义的绑定）。
	// 状态的 CreatedAt/UpdatedAt 由 Store 填写。
	Create(state *ExecutionState)
	// AppendEvent 追加一条 Outbox 事件。事件 ID 与发生时间由 Store 分配。
	// 事件与本次状态修改在同一事务中落盘。
	AppendEvent(eventType string, payload EventPayload)
}

// Snapshot 是某执行身份的只读快照。
type Snapshot struct {
	// State 执行状态；该身份尚无执行时为 nil。
	State *ExecutionState
	// Binding 业务键的定义绑定；未绑定时 ok 为 false。
	Binding Binding
	// Bound 是否存在绑定。
	Bound bool
}

// Store 是执行状态与 Outbox 事件的存储。
//
// 实现必须保证 Commit 的原子性：执行状态与当次追加的事件要么全部可见，
// 要么全部不可见。本包不规定落盘格式。
type Store interface {
	// LoadSnapshot 读取执行身份 (businessKey, idempotencyKey) 的状态快照，
	// 同时返回该业务键的定义绑定。
	LoadSnapshot(ctx context.Context, businessKey, idempotencyKey string) (Snapshot, error)

	// Commit 在一个原子事务中运行 fn 并提交其修改与追加的事件。
	Commit(ctx context.Context, businessKey, idempotencyKey string, fn func(tx Tx) error) error

	// ClaimPendingEvents 领取至多 max 条尚未投递（含此前发送失败被退回）
	// 的事件。每次领取即代表一次投递尝试，事件的 Deliveries 计数加一；
	// 被领取但未 Ack/Nack 的事件不会被再次领取。max<=0 表示实现自选批量。
	ClaimPendingEvents(ctx context.Context, max int) ([]*Event, error)

	// AckEvent 标记事件已投递成功，事件不再参与后续领取。
	AckEvent(ctx context.Context, eventID string) error

	// NackEvent 发送失败后退回事件：保留原事件与负载、解除领取锁定，
	// 允许继续领取。投递次数由再次领取时累计。
	NackEvent(ctx context.Context, eventID string) error
}

// ClaimLeaseStore 是在 Store 之上可选实现的带租约领取接口。
//
// 普通 ClaimPendingEvents 一旦领取即永久锁定（直到 Ack/Nack），worker 在
// Publish、Ack 或 Nack 前退出会导致事件一直滞留领取中。带租约领取则为
// 每次领取分配一个带过期时间的 ClaimID：有效期内事件不得被再次领取，
// 到期后下一次领取自动恢复，失联事件据此重新投递（至少一次语义）。
//
// 只有持有当前 ClaimID 的调用方可 Ack 或 Nack；过期领取（旧 ClaimID）的
// 操作返回 ErrStaleClaim，且不改动当前事件与新租约。
type ClaimLeaseStore interface {
	Store

	// ClaimPendingEventsLeased 领取至多 max 条可投递事件（从未领取，或
	// 此前的领取租约已到期）。每条事件获得一个新生成的 ClaimID 与从领取
	// 成功起算的 ttl 租约，事件的 Deliveries 计数加一、LastAttemptAt 更新。
	// max<=0 表示实现自选批量。
	ClaimPendingEventsLeased(ctx context.Context, ttl time.Duration, max int) ([]ClaimedEvent, error)

	// AckLeasedEvent 确认当前 claimID 对应的领取：事件被永久移除。
	// claimID 不是该事件当前有效的领取时返回 ErrStaleClaim。
	AckLeasedEvent(ctx context.Context, eventID, claimID string) error

	// NackLeasedEvent 按当前 claimID 退回事件：解除当前租约，事件立即可被
	// 再次领取（原事件字段与追加顺序保持不变）。claimID 不是该事件当前
	// 有效领取时返回 ErrStaleClaim，且不改动事件与新租约。
	NackLeasedEvent(ctx context.Context, eventID, claimID string) error
}

// ClaimedEvent 是一次带租约领取的结果。
type ClaimedEvent struct {
	// Event 被领取的事件副本（含更新后的 Deliveries/LastAttemptAt）。
	Event *Event
	// ClaimID 本次领取的唯一标识；只有持有它的调用方可 Ack/Nack。
	ClaimID string
	// ClaimedUntil 租约到期时刻；在此之前事件不会被再次领取。
	ClaimedUntil time.Time
}

// 租约相关的哨兵错误，调用方可用 errors.Is 判定。
var (
	// ErrClaimLeaseUnsupported 启用了租约但 Store 未实现 ClaimLeaseStore。
	ErrClaimLeaseUnsupported = errors.New("txsaga: store does not support claim leases")
	// ErrStaleClaim Ack/Nack 携带的 ClaimID 已不是事件当前有效的领取
	// （领取已到期、事件已被重新领取并获得新 ClaimID，或事件从未被该
	// ClaimID 领取）。出现该错误时事件与当前租约不会被改动。
	ErrStaleClaim = errors.New("txsaga: stale event claim")
)

// fingerprint 计算定义指纹：名称、版本与有序步骤名共同决定。
// 动作函数无可比较标识，调用方应通过 Name/Version 区分不同实现。
func fingerprint(d Definition) string {
	h := sha256.New()
	fmt.Fprintf(h, "name=%s\x00version=%s", d.Name, d.Version)
	for _, s := range d.Steps {
		fmt.Fprintf(h, "\x00step=%s", s.Name)
		if s.Compensate != nil {
			h.Write([]byte("\x00comp=1"))
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

// ---- 内存实现 ----

type memEvent struct {
	event Event

	// 普通领取（ClaimPendingEvents）的锁定标记。
	claimed bool

	// 带租约领取的状态。claimID 非空表示存在一次领取；claimedUntil 之后
	// 该领取失效，事件可被重新领取并获得新的 claimID。
	claimID      string
	claimedUntil time.Time
}

type memTx struct {
	s        *MemoryStore
	business string
	idem     string
	binding  *Binding
	state    *ExecutionState
	created  bool
	staged   []Event
}

func (t *memTx) Binding() (Binding, bool) {
	if t.binding != nil {
		return *t.binding, true
	}
	return Binding{}, false
}

func (t *memTx) State() *ExecutionState { return t.state }

func (t *memTx) Create(state *ExecutionState) {
	state.BusinessKey = t.business
	state.IdempotencyKey = t.idem
	t.state = state
	t.created = true
}

func (t *memTx) AppendEvent(eventType string, p EventPayload) {
	t.staged = append(t.staged, Event{
		BusinessKey: t.business,
		Type:        eventType,
		Payload:     p,
	})
}

// MemoryStore 是进程内的 Store 实现，仅依赖标准库，适用于嵌入与测试。
// 不向磁盘写入任何文件；进程退出后状态不保留。
type MemoryStore struct {
	mu       sync.Mutex
	execs    map[string]*ExecutionState
	bindings map[string]Binding
	events   map[string]*memEvent
	pending  []string // 未领取事件 ID，保持追加顺序
	seq      uint64
	claimSeq uint64 // 租约 ClaimID 序列
	now      func() time.Time
}

// 编译期断言：MemoryStore 同时满足 Store 与 ClaimLeaseStore。
var (
	_ Store           = (*MemoryStore)(nil)
	_ ClaimLeaseStore = (*MemoryStore)(nil)
)

// NewMemoryStore 创建内存状态存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		execs:    make(map[string]*ExecutionState),
		bindings: make(map[string]Binding),
		events:   make(map[string]*memEvent),
		now:      time.Now,
	}
}

func execID(businessKey, idempotencyKey string) string {
	return businessKey + "\x00" + idempotencyKey
}

func cloneState(s *ExecutionState) *ExecutionState {
	if s == nil {
		return nil
	}
	c := *s
	if s.Steps != nil {
		c.Steps = append([]StepState(nil), s.Steps...)
	}
	return &c
}

// LoadSnapshot 实现 Store 接口。
func (s *MemoryStore) LoadSnapshot(_ context.Context, businessKey, idempotencyKey string) (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	snap := Snapshot{}
	if b, ok := s.bindings[businessKey]; ok {
		snap.Binding, snap.Bound = b, true
	}
	if st, ok := s.execs[execID(businessKey, idempotencyKey)]; ok {
		snap.State = cloneState(st)
	}
	return snap, nil
}

// Commit 实现 Store 接口：整库加锁，状态与事件原子可见。
func (s *MemoryStore) Commit(_ context.Context, businessKey, idempotencyKey string, fn func(Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	t := &memTx{s: s, business: businessKey, idem: idempotencyKey}
	if b, ok := s.bindings[businessKey]; ok {
		bc := b
		t.binding = &bc
	}
	if st, ok := s.execs[execID(businessKey, idempotencyKey)]; ok {
		t.state = cloneState(st)
	}

	if err := fn(t); err != nil {
		return err // 回调失败： staged 事件与状态副本一并丢弃
	}

	now := s.now()
	if t.state != nil {
		if t.created {
			if _, exists := s.execs[execID(businessKey, idempotencyKey)]; exists {
				return errors.New("txsaga: execution already exists")
			}
			t.state.CreatedAt = now
			// 首次执行建立业务键与定义的绑定；同键的后续执行定义一致，覆盖无害。
			if _, ok := s.bindings[businessKey]; !ok {
				s.bindings[businessKey] = Binding{SagaName: t.state.SagaName, Fingerprint: t.state.Fingerprint}
			}
		}
		t.state.UpdatedAt = now
		s.execs[execID(businessKey, idempotencyKey)] = cloneState(t.state)
	}

	for i := range t.staged {
		ev := t.staged[i]
		s.seq++
		ev.ID = fmt.Sprintf("evt-%020d", s.seq)
		ev.OccurredAt = now
		s.events[ev.ID] = &memEvent{event: ev}
		s.pending = append(s.pending, ev.ID)
	}
	return nil
}

// ClaimPendingEvents 实现 Store 接口。
func (s *MemoryStore) ClaimPendingEvents(_ context.Context, max int) ([]*Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if max <= 0 {
		max = 16
	}
	now := s.now()
	out := make([]*Event, 0, max)
	remaining := s.pending[:0]
	for _, id := range s.pending {
		me := s.events[id]
		if me == nil {
			continue // 已 Ack 移除，丢弃墓碑 ID
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
		me.claimed = true
		// 接管已到期的租约事件时清除旧租约，避免遗留失效的 ClaimID。
		me.claimID = ""
		me.claimedUntil = time.Time{}
		me.event.deliveries++
		me.event.lastAttemptAt = now
		cp := me.event
		out = append(out, &cp)
	}
	s.pending = remaining
	return out, nil
}

// ClaimPendingEventsLeased 实现 ClaimLeaseStore 接口。
// 从未领取或当前租约已到期的事件可被领取；每次领取生成新的 ClaimID。
// 租约内事件留在追加队列中原位跳过，因此到期恢复与 Nack 退回都不改变
// 事件间的追加顺序；已 Ack 的事件 ID 作为墓碑在扫描时清除。
func (s *MemoryStore) ClaimPendingEventsLeased(_ context.Context, ttl time.Duration, max int) ([]ClaimedEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if max <= 0 {
		max = 16
	}
	now := s.now()
	out := make([]ClaimedEvent, 0, max)
	live := s.pending[:0]
	for _, id := range s.pending {
		me := s.events[id]
		if me == nil {
			continue // 已 Ack 移除，丢弃墓碑 ID
		}
		if me.claimID != "" && now.Before(me.claimedUntil) {
			live = append(live, id) // 有效租约内：原位保留，本次跳过
			continue
		}
		if len(out) >= max {
			live = append(live, id) // 批量已满：保留顺序，下轮再领
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
		live = append(live, id) // 租约期内留在队列，到期后原位可恢复
	}
	s.pending = live
	return out, nil
}

// AckEvent 实现 Store 接口。
func (s *MemoryStore) AckEvent(_ context.Context, eventID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.events[eventID]; !ok {
		return fmt.Errorf("txsaga: event %q not found", eventID)
	}
	delete(s.events, eventID)
	return nil
}

// AckLeasedEvent 实现 ClaimLeaseStore 接口：仅当前 claimID 可确认。
func (s *MemoryStore) AckLeasedEvent(_ context.Context, eventID, claimID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	me, ok := s.events[eventID]
	if !ok {
		return fmt.Errorf("txsaga: event %q not found", eventID)
	}
	if me.claimID != claimID {
		// 含从未租约领取、旧 ClaimID 已到期、已被新领取取代三种情形，
		// 均不得改动当前事件或新租约。
		return fmt.Errorf("txsaga: event %q claim %q is not current: %w", eventID, claimID, ErrStaleClaim)
	}
	delete(s.events, eventID)
	return nil
}

// NackEvent 实现 Store 接口。
func (s *MemoryStore) NackEvent(_ context.Context, eventID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	me, ok := s.events[eventID]
	if !ok {
		return fmt.Errorf("txsaga: event %q not found", eventID)
	}
	if !me.claimed {
		return nil // 幂等退回：未在领取中的事件无需处理
	}
	me.claimed = false
	s.pending = append(s.pending, eventID)
	return nil
}

// NackLeasedEvent 实现 ClaimLeaseStore 接口：仅当前 claimID 可退回，
// 退回后租约解除、事件立即可领取，且保持原追加顺序。
func (s *MemoryStore) NackLeasedEvent(_ context.Context, eventID, claimID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	me, ok := s.events[eventID]
	if !ok {
		return fmt.Errorf("txsaga: event %q not found", eventID)
	}
	if me.claimID != claimID {
		return fmt.Errorf("txsaga: event %q claim %q is not current: %w", eventID, claimID, ErrStaleClaim)
	}
	// 租约事件始终留在追加队列中，退回只解除租约：事件立即可领取，
	// 且相对其它事件的顺序不变。
	me.claimID = ""
	me.claimedUntil = time.Time{}
	return nil
}

// PendingCount 返回当前立即可领取的事件数（不含有效租约内的事件），
// 便于观测与测试。
func (s *MemoryStore) PendingCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	n := 0
	for _, id := range s.pending {
		me := s.events[id]
		if me == nil {
			continue
		}
		if me.claimID != "" && now.Before(me.claimedUntil) {
			continue
		}
		n++
	}
	return n
}

// TotalEvents 返回存储中全部事件数（含领取中、待投递）。
func (s *MemoryStore) TotalEvents() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.events)
}
