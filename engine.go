package saga

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// Engine 按 Saga 定义顺序执行正向步骤、在失败时逆序补偿，并把每次状态
// 变化以 Outbox 事件原子写入 Store。Engine 可并发使用；同一执行身份的
// 并发调用在本引擎内串行推进（动作不会因并发而重复调用），跨进程并发
// 则通过乐观锁收敛，未确认结果的重新调用由动作的幂等契约兜底。
type Engine struct {
	store Store
	now   func() time.Time

	keyMu    sync.Mutex
	keyLocks map[string]*keyedLock
}

// NewEngine 创建基于 store 的编排引擎。
func NewEngine(store Store) *Engine {
	return &Engine{store: store, now: time.Now, keyLocks: make(map[string]*keyedLock)}
}

// keyedLock 是一个执行身份的串行锁及其引用计数。
type keyedLock struct {
	mu   sync.Mutex
	refs int
}

// lockKey 获取同一执行身份的串行锁，返回释放函数。
func (e *Engine) lockKey(businessKey, idempotencyKey string) func() {
	key := executionKey(businessKey, idempotencyKey)
	e.keyMu.Lock()
	kl, ok := e.keyLocks[key]
	if !ok {
		kl = &keyedLock{}
		e.keyLocks[key] = kl
	}
	kl.refs++
	e.keyMu.Unlock()

	kl.mu.Lock()
	return func() {
		kl.mu.Unlock()
		e.keyMu.Lock()
		kl.refs--
		if kl.refs == 0 {
			delete(e.keyLocks, key)
		}
		e.keyMu.Unlock()
	}
}

// optimisticRounds 限制单个动作在乐观锁冲突下的重读轮数。
const optimisticRounds = 100

// Execute 按请求执行（或恢复）一次 Saga 执行，返回其确定结果。
//
// 执行身份由业务键、Saga 定义与外部幂等键共同确定：
//   - 定义非法或缺少业务键/幂等键：ErrInvalidDefinition；
//   - 相同业务键已绑定不同定义：ErrDefinitionConflict；
//   - 同身份重试：不重复执行已确认成功的正向或补偿步骤，未确认结果的
//     步骤可能被再次调用（动作须自身幂等）；
//   - 终态（completed/failed/compensation_failed）一经形成不再改变，
//     后续调用直接返回同一结果。
func (e *Engine) Execute(ctx context.Context, def Definition, req Request) (Result, error) {
	if err := def.validate(); err != nil {
		return Result{}, err
	}
	if err := req.validate(); err != nil {
		return Result{}, err
	}
	hash := def.fingerprint()

	// 同一执行身份在本引擎内串行：避免并发请求对同一未确认步骤重复调用动作。
	unlock := e.lockKey(req.BusinessKey, req.IdempotencyKey)
	defer unlock()

	// 确保执行已登记（execution.started 事件随创建原子落盘）。
	ex, err := e.ensureExecution(ctx, def, req, hash)
	if err != nil {
		return Result{}, err
	}
	if ex.State.Terminal() {
		return buildResult(ex), nil
	}

	// 推进循环：每轮“读快照 → 决定下一步 → 在锁外执行动作 → 重读提交”，
	// 提交遇乐观锁冲突则整轮重读。每步确认单独提交后才会进入下一步。
	for round := 0; round < optimisticRounds; round++ {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}

		tx, err := e.store.Begin(ctx)
		if err != nil {
			return Result{}, err
		}
		ex, err = tx.LoadExecution(req.BusinessKey, req.IdempotencyKey)
		if err != nil {
			tx.Rollback()
			return Result{}, err
		}
		tx.Rollback()

		if ex.State.Terminal() {
			return buildResult(ex), nil
		}

		work := nextWork(ex)
		if work.kind == workDone {
			return buildResult(ex), nil
		}

		// 在任何存储锁之外执行业务动作。
		var actionErr error
		switch work.kind {
		case workForward:
			actionErr = def.Steps[work.index].Do(ctx)
		case workCompensate:
			comp := def.Steps[work.index].Compensate
			if comp == nil {
				actionErr = nil // 无需补偿的步骤按补偿成功处理
			} else {
				actionErr = comp(ctx)
			}
		}

		committed, err := e.commitStep(ctx, def, req, work, actionErr)
		if err != nil {
			return Result{}, err
		}
		if !committed {
			// 状态已被并发调用推进，重读后再决策。
			continue
		}
	}
	return Result{}, fmt.Errorf("saga: execution %s/%s did not converge after %d rounds", req.BusinessKey, req.IdempotencyKey, optimisticRounds)
}

// GetResult 返回既有执行的当前结果。未知执行身份返回 ErrExecutionNotFound。
func (e *Engine) GetResult(ctx context.Context, businessKey, idempotencyKey string) (Result, error) {
	tx, err := e.store.Begin(ctx)
	if err != nil {
		return Result{}, err
	}
	ex, err := tx.LoadExecution(businessKey, idempotencyKey)
	tx.Rollback()
	if err != nil {
		return Result{}, err
	}
	return buildResult(ex), nil
}

// ensureExecution 加载既有执行；不存在则原子创建（含 started 事件）。
// 并发创建冲突时转为加载既有执行。
func (e *Engine) ensureExecution(ctx context.Context, def Definition, req Request, hash string) (*Execution, error) {
	for {
		tx, err := e.store.Begin(ctx)
		if err != nil {
			return nil, err
		}
		ex, loadErr := tx.LoadExecution(req.BusinessKey, req.IdempotencyKey)
		if loadErr == nil {
			tx.Rollback()
			if ex.DefinitionHash != hash {
				return nil, ErrDefinitionConflict
			}
			return ex, nil
		}
		if loadErr != ErrExecutionNotFound {
			tx.Rollback()
			return nil, loadErr
		}
		tx.Rollback()

		now := e.now().UTC()
		created := &Execution{
			BusinessKey:    req.BusinessKey,
			IdempotencyKey: req.IdempotencyKey,
			SagaName:       def.Name,
			DefinitionHash: hash,
			State:          StateRunning,
			Steps:          make([]StepRecord, len(def.Steps)),
			StartedAt:      now,
			UpdatedAt:      now,
		}
		for i, s := range def.Steps {
			created.Steps[i] = StepRecord{Name: s.Name, State: StepPending}
		}
		startedEvent := e.newEvent(created, EventExecutionStarted, EventPayload{State: StateRunning})

		createTx, err := e.store.Begin(ctx)
		if err != nil {
			return nil, err
		}
		if err := createTx.CreateExecution(created); err != nil {
			createTx.Rollback()
			return nil, err
		}
		if err := createTx.SaveExecution(created, []OutboxEvent{startedEvent}); err != nil {
			createTx.Rollback()
			return nil, err
		}
		switch err := createTx.Commit(); {
		case err == nil:
			return created, nil
		case err == ErrExecutionAlreadyExists:
			// 并发调用抢先创建：重读其结果。
			continue
		default:
			return nil, err
		}
	}
}

type workKind int

const (
	workDone workKind = iota
	workForward
	workCompensate
)

type nextStep struct {
	kind  workKind
	index int
}

// nextWork 依据持久化状态决定下一步：
//   - 尚无失败步骤：推进第一个 pending 正向步骤；全部成功则收敛为 completed；
//   - 已出现失败步骤：逆序选取第一个 succeeded 步骤进行补偿；全部处理完
//     则收敛为 failed。
func nextWork(ex *Execution) nextStep {
	compensating := false
	for i := range ex.Steps {
		if ex.Steps[i].State == StepFailed ||
			ex.Steps[i].State == StepCompensationFailed {
			compensating = true
			break
		}
	}

	if !compensating {
		for i := range ex.Steps {
			if ex.Steps[i].State == StepPending {
				return nextStep{kind: workForward, index: i}
			}
		}
		return nextStep{kind: workDone}
	}

	for i := len(ex.Steps) - 1; i >= 0; i-- {
		if ex.Steps[i].State == StepSucceeded {
			return nextStep{kind: workCompensate, index: i}
		}
	}
	return nextStep{kind: workDone}
}

// commitStep 重读执行、确认目标步骤仍是预期状态后提交该步结果；返回
// committed=false 表示已被并发调用推进，调用方应重读决策。
func (e *Engine) commitStep(ctx context.Context, def Definition, req Request, work nextStep, actionErr error) (bool, error) {
	tx, err := e.store.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	ex, err := tx.LoadExecution(req.BusinessKey, req.IdempotencyKey)
	if err != nil {
		return false, err
	}
	if ex.State.Terminal() {
		return true, nil // 终态已由并发调用形成，无需再写。
	}

	var events []OutboxEvent

	switch work.kind {
	case workForward:
		step := &ex.Steps[work.index]
		if step.State != StepPending {
			return false, nil // 已被确认（成功或失败），重读决策。
		}
		step.Attempts++
		if actionErr != nil {
			step.State = StepFailed
			step.LastError = actionErr.Error()
			events = append(events, e.newEvent(ex, EventStepFailed, EventPayload{
				Step: step.Name, State: ex.State, Reason: step.LastError, Attempts: step.Attempts,
			}))
			// 没有任何此前确认成功的步骤：无需补偿，立即形成 failed 终态。
			if !anySucceeded(ex.Steps) {
				ex.State = StateFailed
				ex.FailureReason = forwardFailureReason(ex.Steps)
				events = append(events, e.newEvent(ex, EventExecutionFailed, EventPayload{
					State: StateFailed, Reason: ex.FailureReason,
				}))
			}
			break
		}
		step.State = StepSucceeded
		step.LastError = ""
		events = append(events, e.newEvent(ex, EventStepSucceeded, EventPayload{
			Step: step.Name, State: ex.State, Attempts: step.Attempts,
		}))
		// 正向全部确认：形成 completed 终态。
		if allSucceeded(ex.Steps) {
			ex.State = StateCompleted
			events = append(events, e.newEvent(ex, EventExecutionCompleted, EventPayload{
				State: StateCompleted,
			}))
		}

	case workCompensate:
		step := &ex.Steps[work.index]
		if step.State != StepSucceeded {
			return false, nil // 补偿已被确认或执行已冻结，重读决策。
		}
		if actionErr != nil {
			step.State = StepCompensationFailed
			step.LastError = actionErr.Error()
			ex.State = StateCompensationFailed
			ex.FailureReason = fmt.Sprintf("compensation of step %q failed: %s", step.Name, actionErr.Error())
			events = append(events, e.newEvent(ex, EventCompensationFailed, EventPayload{
				Step: step.Name, State: StateCompensationFailed, Reason: ex.FailureReason,
			}))
			break
		}
		step.State = StepCompensated
		step.LastError = ""
		if def.Steps[work.index].Compensate != nil {
			step.CompensationAttempts++
		}
		events = append(events, e.newEvent(ex, EventStepCompensated, EventPayload{
			Step: step.Name, State: ex.State, Attempts: step.CompensationAttempts,
		}))
		// 逆序补偿全部完成：形成 failed 终态。
		if !anySucceeded(ex.Steps) {
			ex.State = StateFailed
			ex.FailureReason = forwardFailureReason(ex.Steps)
			events = append(events, e.newEvent(ex, EventExecutionFailed, EventPayload{
				State: StateFailed, Reason: ex.FailureReason,
			}))
		}
	}

	if err := tx.SaveExecution(ex, events); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		if err == ErrConcurrentUpdate {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// newEvent 追加一条事件：序号前移、ID 按执行身份与序号确定性生成。
func (e *Engine) newEvent(ex *Execution, typ EventType, p EventPayload) OutboxEvent {
	ex.EventSeq++
	payload, _ := json.Marshal(p)
	return OutboxEvent{
		ID:             fmt.Sprintf("%s:%s:%06d", ex.BusinessKey, ex.IdempotencyKey, ex.EventSeq),
		BusinessKey:    ex.BusinessKey,
		IdempotencyKey: ex.IdempotencyKey,
		Type:           typ,
		OccurredAt:     e.now().UTC(),
		Payload:        payload,
	}
}

func allSucceeded(steps []StepRecord) bool {
	for i := range steps {
		if steps[i].State != StepSucceeded {
			return false
		}
	}
	return true
}

func anySucceeded(steps []StepRecord) bool {
	for i := range steps {
		if steps[i].State == StepSucceeded {
			return true
		}
	}
	return false
}

func forwardFailureReason(steps []StepRecord) string {
	for i := range steps {
		if steps[i].State == StepFailed {
			return fmt.Sprintf("step %q failed: %s", steps[i].Name, steps[i].LastError)
		}
	}
	return ""
}

func buildResult(ex *Execution) Result {
	r := Result{
		BusinessKey:    ex.BusinessKey,
		IdempotencyKey: ex.IdempotencyKey,
		Saga:           ex.SagaName,
		State:          ex.State,
		FailureReason:  ex.FailureReason,
		Steps:          make([]StepResult, len(ex.Steps)),
	}
	for i, s := range ex.Steps {
		r.Steps[i] = StepResult{
			Name:                 s.Name,
			State:                s.State,
			Attempts:             s.Attempts,
			CompensationAttempts: s.CompensationAttempts,
			Error:                s.LastError,
		}
	}
	if r.State == StateFailed && r.FailureReason == "" {
		r.FailureReason = forwardFailureReason(ex.Steps)
	}
	return r
}
