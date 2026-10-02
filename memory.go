package saga

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// MemoryStore 是 Store 的进程内实现，适用于单进程嵌入、测试与示例。
// 它不做任何磁盘持久化；需要落盘的调用方自行实现 Store，本包不规定
// 文件格式。
type MemoryStore struct {
	mu sync.Mutex

	// executions 以 businessKey\x00idempotencyKey 为键。
	executions map[string]*Execution
	// bindings 记录业务键已绑定的定义指纹。
	bindings map[string]string
	// entries 为全部 Outbox 事件（含已投递），eventOrder 保留插入顺序。
	entries    map[string]*outboxEntry
	eventOrder []string

	claimSeq atomic.Uint64
	now      func() time.Time
}

// outboxEntry 是事件的投递管理状态。
type outboxEntry struct {
	event      OutboxEvent
	delivered  bool
	claimToken string
	claimUntil time.Time
}

// NewMemoryStore 创建一个空的内存状态存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		executions: make(map[string]*Execution),
		bindings:   make(map[string]string),
		entries:    make(map[string]*outboxEntry),
		now:        time.Now,
	}
}

func executionKey(businessKey, idempotencyKey string) string {
	return businessKey + "\x00" + idempotencyKey
}

// Begin 实现 Store。事务持有状态的私有副本，动作执行期间不持锁，
// 提交时再做原子校验与应用。
func (m *MemoryStore) Begin(_ context.Context) (Tx, error) {
	return &memoryTx{store: m, saves: make(map[string]*stagedSave)}, nil
}

// ClaimEvents 实现 Store：按插入顺序领取未投递且未被有效领取的事件。
func (m *MemoryStore) ClaimEvents(_ context.Context, max int, lease time.Duration) ([]OutboxEvent, error) {
	if max <= 0 {
		return nil, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	out := make([]OutboxEvent, 0, max)
	for _, id := range m.eventOrder {
		e := m.entries[id]
		if e.delivered {
			continue
		}
		if e.claimToken != "" && now.Before(e.claimUntil) {
			continue
		}
		token := fmt.Sprintf("claim-%d", m.claimSeq.Add(1))
		e.claimToken = token
		e.claimUntil = now.Add(lease)
		ev := cloneEvent(e.event)
		ev.Attempts = e.event.Attempts
		ev.LastError = e.event.LastError
		ev.ClaimToken = token
		out = append(out, ev)
		if len(out) >= max {
			break
		}
	}
	return out, nil
}

// MarkDelivered 实现 Store。
func (m *MemoryStore) MarkDelivered(_ context.Context, eventID, claimToken string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	e, ok := m.entries[eventID]
	if !ok {
		return ErrEventNotFound
	}
	if e.claimToken != claimToken || claimToken == "" {
		return ErrAlreadyClaimed
	}
	e.delivered = true
	e.claimToken = ""
	e.claimUntil = time.Time{}
	return nil
}

// FailDelivery 实现 Store：保留事件、递增投递次数并立即释放领取。
func (m *MemoryStore) FailDelivery(_ context.Context, eventID, claimToken string, cause error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	e, ok := m.entries[eventID]
	if !ok {
		return ErrEventNotFound
	}
	if e.claimToken != claimToken || claimToken == "" {
		return ErrAlreadyClaimed
	}
	e.event.Attempts++
	if cause != nil {
		e.event.LastError = cause.Error()
	}
	e.claimToken = ""
	e.claimUntil = time.Time{}
	return nil
}

// stagedSave 暂存一次保存：执行快照与同一原子单元内追加的事件。
type stagedSave struct {
	ex     *Execution
	events []OutboxEvent
}

// memoryTx 是 MemoryStore 的事务：所有修改暂存于私有副本，Commit 时
// 在单一互斥锁下完成冲突检查与应用，因此状态保存与事件追加是原子的。
type memoryTx struct {
	store    *MemoryStore
	create   *Execution
	saves    map[string]*stagedSave
	finished bool
}

func (t *memoryTx) LoadExecution(businessKey, idempotencyKey string) (*Execution, error) {
	t.store.mu.Lock()
	cur, ok := t.store.executions[executionKey(businessKey, idempotencyKey)]
	if ok {
		cur = cloneExecution(cur)
	}
	t.store.mu.Unlock()
	if !ok {
		return nil, ErrExecutionNotFound
	}
	return cur, nil
}

func (t *memoryTx) CreateExecution(ex *Execution) error {
	if ex == nil {
		return ErrInvalidDefinition
	}
	t.create = cloneExecution(ex)
	return nil
}

func (t *memoryTx) SaveExecution(ex *Execution, events []OutboxEvent) error {
	if ex == nil {
		return ErrInvalidDefinition
	}
	key := executionKey(ex.BusinessKey, ex.IdempotencyKey)
	staged, ok := t.saves[key]
	if !ok {
		staged = &stagedSave{}
		t.saves[key] = staged
	}
	// 同一事务内多次保存：采用最新快照，事件累积追加。
	staged.ex = cloneExecution(ex)
	staged.events = append(staged.events, events...)
	return nil
}

func (t *memoryTx) Commit() error {
	if t.finished {
		return nil
	}
	t.finished = true

	s := t.store
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()

	if t.create != nil {
		key := executionKey(t.create.BusinessKey, t.create.IdempotencyKey)
		if hash, bound := s.bindings[t.create.BusinessKey]; bound && hash != t.create.DefinitionHash {
			return ErrDefinitionConflict
		}
		if _, exists := s.executions[key]; exists {
			return ErrExecutionAlreadyExists
		}
		ex := cloneExecution(t.create)
		if ex.StartedAt.IsZero() {
			ex.StartedAt = now
		}
		ex.UpdatedAt = now
		s.executions[key] = ex
		s.bindings[ex.BusinessKey] = ex.DefinitionHash
	}

	for key, staged := range t.saves {
		cur, ok := s.executions[key]
		if !ok {
			return ErrExecutionNotFound
		}
		if cur.Version != staged.ex.Version {
			return ErrConcurrentUpdate
		}
		// 定义指纹绑定不可变。
		if cur.DefinitionHash != staged.ex.DefinitionHash {
			return ErrDefinitionConflict
		}
		next := cloneExecution(staged.ex)
		next.Version = cur.Version + 1
		next.UpdatedAt = now
		s.executions[key] = next

		for _, ev := range staged.events {
			if _, dup := s.entries[ev.ID]; dup {
				// 事件 ID 确定性派生；重复 ID 视为重复提交，幂等忽略。
				continue
			}
			s.entries[ev.ID] = &outboxEntry{event: cloneEvent(ev)}
			s.eventOrder = append(s.eventOrder, ev.ID)
		}
	}
	return nil
}

func (t *memoryTx) Rollback() {
	t.finished = true
}

func cloneExecution(in *Execution) *Execution {
	if in == nil {
		return nil
	}
	out := *in
	if in.Steps != nil {
		out.Steps = append([]StepRecord(nil), in.Steps...)
	}
	return &out
}

func cloneEvent(in OutboxEvent) OutboxEvent {
	if in.Payload != nil {
		in.Payload = append(json.RawMessage(nil), in.Payload...)
	}
	return in
}
