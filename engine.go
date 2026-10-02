package txsaga

import (
	"context"
	"strings"
	"sync"
	"time"
)

// ValidateDefinition 校验 Saga 定义，非法时返回 ErrInvalidDefinition（可 errors.Is）。
// 规则：名称非空、至少一个步骤、每个步骤名非空且不重复、每个步骤都提供正向动作。
func ValidateDefinition(d Definition) error {
	if strings.TrimSpace(d.Name) == "" {
		return ErrInvalidDefinition
	}
	if len(d.Steps) == 0 {
		return ErrInvalidDefinition
	}
	seen := make(map[string]struct{}, len(d.Steps))
	for _, st := range d.Steps {
		if strings.TrimSpace(st.Name) == "" || st.Action == nil {
			return ErrInvalidDefinition
		}
		if _, dup := seen[st.Name]; dup {
			return ErrInvalidDefinition
		}
		seen[st.Name] = struct{}{}
	}
	return nil
}

// Engine 在给定 Store 上执行 Saga。零值不可用，请用 NewEngine 创建。
type Engine struct {
	store Store

	// 同执行身份的进程内串行化，避免并发调用重复进入同一未确认动作。
	locksMu sync.Mutex
	locks   map[string]*sync.Mutex

	// now 为可注入时钟；生产使用 time.Now。
	now func() time.Time
}

// NewEngine 创建基于 store 的编排引擎。
func NewEngine(store Store) *Engine {
	return &Engine{store: store, locks: make(map[string]*sync.Mutex), now: time.Now}
}

func (e *Engine) identityLock(businessKey, idempotencyKey string) *sync.Mutex {
	key := businessKey + "\x00" + idempotencyKey
	e.locksMu.Lock()
	m, ok := e.locks[key]
	if !ok {
		m = &sync.Mutex{}
		e.locks[key] = m
	}
	e.locksMu.Unlock()
	return m
}

type execView struct {
	state   *ExecutionState
	payload any
}

func (v execView) BusinessKey() string    { return v.state.BusinessKey }
func (v execView) SagaName() string       { return v.state.SagaName }
func (v execView) IdempotencyKey() string { return v.state.IdempotencyKey }
func (v execView) Payload() any           { return v.payload }

func (v execView) SucceededSteps() []string {
	var names []string
	for _, st := range v.state.Steps {
		if st.Status == StepResultSucceeded {
			names = append(names, st.Name)
		}
	}
	return names
}

// Execute 按 Definition 执行一次 Saga 请求。
//
// 执行身份由业务键、Saga 定义与外部幂等键共同确定：
//   - 新身份：创建执行并顺序执行未完成步骤；
//   - 同身份重试：不重复执行已确认成功的正向步骤或补偿步骤；
//     结果未确认的步骤允许被重新调用，重入次数累计在对应步骤的 Attempts，
//     动作自身须遵守幂等契约；
//   - 已到达终态：直接返回固定的终态结果，不再调用任何动作，也不再追加事件；
//   - 同业务键绑定不同定义：返回 ErrDefinitionConflict；
//   - 非法定义或缺失业务键/幂等键：返回 ErrInvalidDefinition。
//
// 每一步确认后，状态与对应 Outbox 事件在同一个 Store 事务中原子提交，
// 然后才执行下一步。正向动作失败即停止推进，仅逆序补偿此前确认成功的步骤；
// 任一补偿报错则固定为 compensation_failed 终态。
func (e *Engine) Execute(ctx context.Context, def Definition, req ExecutionRequest) (Result, error) {
	if err := ValidateDefinition(def); err != nil {
		return Result{}, err
	}
	if strings.TrimSpace(req.BusinessKey) == "" || strings.TrimSpace(req.IdempotencyKey) == "" {
		return Result{}, ErrInvalidDefinition
	}
	fp := fingerprint(def)

	mu := e.identityLock(req.BusinessKey, req.IdempotencyKey)
	mu.Lock()
	defer mu.Unlock()

	for {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		snap, err := e.store.LoadSnapshot(ctx, req.BusinessKey, req.IdempotencyKey)
		if err != nil {
			return Result{}, err
		}
		if snap.Bound && snap.Binding.Fingerprint != fp {
			return Result{}, ErrDefinitionConflict
		}
		st := snap.State
		if st == nil {
			if err := e.createExecution(ctx, def, fp, req); err != nil {
				return Result{}, err
			}
			continue
		}
		if st.Fingerprint != fp {
			return Result{}, ErrDefinitionConflict
		}
		if st.Terminal() {
			return toResult(st), nil
		}

		switch st.Status {
		case StatusRunning:
			advanced, err := e.advanceForward(ctx, def, req, st)
			if err != nil {
				return Result{}, err
			}
			if !advanced {
				// 所有步骤均已确认成功，重新读取以取得 completed 终态。
				snap2, err := e.store.LoadSnapshot(ctx, req.BusinessKey, req.IdempotencyKey)
				if err != nil {
					return Result{}, err
				}
				return toResult(snap2.State), nil
			}
		case StatusCompensating:
			done, err := e.advanceCompensation(ctx, def, req, st)
			if err != nil {
				return Result{}, err
			}
			if done {
				return e.finishFailed(ctx, req)
			}
		default:
			return Result{}, ErrInvalidDefinition
		}
	}
}

// createExecution 在一个原子事务中建立执行状态并追加 started 事件。
func (e *Engine) createExecution(ctx context.Context, def Definition, fp string, req ExecutionRequest) error {
	return e.store.Commit(ctx, req.BusinessKey, req.IdempotencyKey, func(tx Tx) error {
		if b, ok := tx.Binding(); ok && b.Fingerprint != fp {
			return ErrDefinitionConflict
		}
		if tx.State() != nil {
			return nil // 并发情况下已由其它调用建立，直接沿用
		}
		steps := make([]StepState, len(def.Steps))
		for i, s := range def.Steps {
			steps[i] = StepState{Name: s.Name}
		}
		tx.Create(&ExecutionState{
			SagaName:    def.Name,
			Fingerprint: fp,
			Status:      StatusRunning,
			Steps:       steps,
			Payload:     req.Payload,
		})
		tx.AppendEvent(EventExecutionStarted, EventPayload{
			SagaName:       def.Name,
			IdempotencyKey: req.IdempotencyKey,
			Status:         StatusRunning,
		})
		return nil
	})
}

// advanceForward 执行下一个未完成的正向步骤，在事务外调用动作，再把
// 成功/失败结果与事件原子提交。返回 false 表示已无待执行步骤。
func (e *Engine) advanceForward(ctx context.Context, def Definition, req ExecutionRequest, st *ExecutionState) (bool, error) {
	idx := -1
	for i := range st.Steps {
		if st.Steps[i].Status == "" {
			idx = i
			break
		}
	}
	if idx < 0 {
		return false, nil
	}
	step := def.Steps[idx]

	// Attempts 记录该步骤动作的实际调用次数；同身份重试已确认步骤时不会自增。
	// 未确认结果的步骤被重新调用时该值增大，调用方据此判断是否发生重复处理。
	st.Steps[idx].Attempts++
	st.Steps[idx].StartedAt = e.now()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	actionErr := step.Action(ctx, execView{state: st, payload: req.Payload})
	if err := ctx.Err(); err != nil {
		// 调用方取消：不把取消解释为业务失败，未确认的步骤留待后续同身份重试。
		return false, err
	}
	finishedAt := e.now()

	err := e.store.Commit(ctx, req.BusinessKey, req.IdempotencyKey, func(tx Tx) error {
		cur := tx.State()
		if cur == nil || cur.Terminal() {
			return nil // 状态已被其它路径推进，本次结果丢弃
		}
		cs := &cur.Steps[idx]
		cs.Attempts = st.Steps[idx].Attempts
		cs.StartedAt = st.Steps[idx].StartedAt
		cs.FinishedAt = finishedAt

		if actionErr == nil {
			cs.Status = StepResultSucceeded
			cs.LastError = ""
			succeeded := succeededNames(cur)
			if idx == len(cur.Steps)-1 {
				cur.Status = StatusCompleted
				tx.AppendEvent(EventStepSucceeded, stepEvent(def, req, step.Name, StatusCompleted, succeeded))
				tx.AppendEvent(EventExecutionCompleted, EventPayload{
					SagaName:       def.Name,
					IdempotencyKey: req.IdempotencyKey,
					Status:         StatusCompleted,
					SucceededSteps: succeeded,
				})
			} else {
				cur.Status = StatusRunning
				tx.AppendEvent(EventStepSucceeded, stepEvent(def, req, step.Name, StatusRunning, succeeded))
			}
			return nil
		}

		errMsg := actionErr.Error()
		cs.Status = StepResultFailed
		cs.LastError = errMsg
		cur.FailedStep = step.Name
		cur.FailureReason = errMsg
		cur.Status = StatusCompensating
		tx.AppendEvent(EventStepFailed, EventPayload{
			SagaName:       def.Name,
			IdempotencyKey: req.IdempotencyKey,
			Step:           step.Name,
			Status:         StatusCompensating,
			Reason:         errMsg,
			SucceededSteps: succeededNames(cur),
		})
		return nil
	})
	return err == nil, err
}

// advanceCompensation 逆序补偿下一个已确认成功的步骤。
// 返回 done=true 表示已无待补偿步骤，调用方应把执行收束为 failed 终态。
// 补偿动作返回错误时，执行在同一事务中固定为 compensation_failed 终态。
func (e *Engine) advanceCompensation(ctx context.Context, def Definition, req ExecutionRequest, st *ExecutionState) (bool, error) {
	idx := -1
	for i := len(st.Steps) - 1; i >= 0; i-- {
		if st.Steps[i].Status == StepResultSucceeded {
			idx = i
			break
		}
	}
	if idx < 0 {
		return true, nil
	}
	step := def.Steps[idx]

	var compErr error
	calledCompensation := step.Compensate != nil
	if calledCompensation {
		st.Steps[idx].CompensationAttempts++
		if err := ctx.Err(); err != nil {
			return false, err
		}
		compErr = step.Compensate(ctx, execView{state: st, payload: req.Payload})
		if err := ctx.Err(); err != nil {
			return false, err
		}
	}
	var compensatedAt time.Time
	if calledCompensation {
		compensatedAt = e.now()
	}

	err := e.store.Commit(ctx, req.BusinessKey, req.IdempotencyKey, func(tx Tx) error {
		cur := tx.State()
		if cur == nil || cur.Terminal() {
			return nil
		}
		cs := &cur.Steps[idx]
		if calledCompensation {
			cs.CompensationAttempts = st.Steps[idx].CompensationAttempts
			cs.CompensatedAt = compensatedAt
		}

		if compErr == nil {
			cs.Status = StepResultCompensated
			cs.LastError = ""
			cur.Status = StatusCompensating
			tx.AppendEvent(EventStepCompensated, EventPayload{
				SagaName:       def.Name,
				IdempotencyKey: req.IdempotencyKey,
				Step:           step.Name,
				Status:         StatusCompensating,
			})
			return nil
		}

		errMsg := compErr.Error()
		cs.Status = StepResultCompensationFailed
		cs.LastError = errMsg
		cur.FailedCompensationStep = step.Name
		cur.CompensationError = errMsg
		cur.Status = StatusCompensationFailed
		tx.AppendEvent(EventStepCompensationFailed, EventPayload{
			SagaName:       def.Name,
			IdempotencyKey: req.IdempotencyKey,
			Step:           step.Name,
			Status:         StatusCompensationFailed,
			Reason:         errMsg,
			Attempt:        cs.CompensationAttempts,
		})
		tx.AppendEvent(EventCompensationFailed, EventPayload{
			SagaName:       def.Name,
			IdempotencyKey: req.IdempotencyKey,
			Step:           step.Name,
			Status:         StatusCompensationFailed,
			Reason:         errMsg,
		})
		return nil
	})
	if err != nil {
		return false, err
	}
	return false, nil
}

// finishFailed 在补偿全部完成后把执行收束为 failed 终态并返回结果。
func (e *Engine) finishFailed(ctx context.Context, req ExecutionRequest) (Result, error) {
	err := e.store.Commit(ctx, req.BusinessKey, req.IdempotencyKey, func(tx Tx) error {
		cur := tx.State()
		if cur == nil || cur.Terminal() {
			return nil
		}
		cur.Status = StatusFailed
		tx.AppendEvent(EventExecutionFailed, EventPayload{
			SagaName:       cur.SagaName,
			IdempotencyKey: req.IdempotencyKey,
			Status:         StatusFailed,
			Step:           cur.FailedStep,
			Reason:         cur.FailureReason,
		})
		return nil
	})
	if err != nil {
		return Result{}, err
	}
	snap, err := e.store.LoadSnapshot(ctx, req.BusinessKey, req.IdempotencyKey)
	if err != nil {
		return Result{}, err
	}
	return toResult(snap.State), nil
}

func succeededNames(st *ExecutionState) []string {
	var names []string
	for _, s := range st.Steps {
		if s.Status == StepResultSucceeded {
			names = append(names, s.Name)
		}
	}
	return names
}

func stepEvent(def Definition, req ExecutionRequest, step, status string, succeeded []string) EventPayload {
	return EventPayload{
		SagaName:       def.Name,
		IdempotencyKey: req.IdempotencyKey,
		Step:           step,
		Status:         status,
		SucceededSteps: succeeded,
	}
}

func toResult(st *ExecutionState) Result {
	outcomes := make([]StepOutcome, len(st.Steps))
	for i, s := range st.Steps {
		r := s.Status
		if r == "" {
			r = StepResultPending
		}
		outcomes[i] = StepOutcome{
			Name:                 s.Name,
			Result:               r,
			Attempts:             s.Attempts,
			CompensationAttempts: s.CompensationAttempts,
			Error:                s.LastError,
			StartedAt:            s.StartedAt,
			FinishedAt:           s.FinishedAt,
		}
	}
	return Result{
		BusinessKey:            st.BusinessKey,
		SagaName:               st.SagaName,
		IdempotencyKey:         st.IdempotencyKey,
		Status:                 st.Status,
		FailureReason:          st.FailureReason,
		CompensationError:      st.CompensationError,
		FailedStep:             st.FailedStep,
		FailedCompensationStep: st.FailedCompensationStep,
		Steps:                  outcomes,
		Terminal:               st.Terminal(),
	}
}

// GetResult 查询某执行身份的当前结果，不执行任何动作。
// 未知执行身份（或 sagaName 与该执行的定义名不符）返回 ErrExecutionNotFound。
func (e *Engine) GetResult(ctx context.Context, sagaName, businessKey, idempotencyKey string) (Result, error) {
	if strings.TrimSpace(sagaName) == "" || strings.TrimSpace(businessKey) == "" || strings.TrimSpace(idempotencyKey) == "" {
		return Result{}, ErrInvalidDefinition
	}
	snap, err := e.store.LoadSnapshot(ctx, businessKey, idempotencyKey)
	if err != nil {
		return Result{}, err
	}
	if snap.State == nil || snap.State.SagaName != sagaName {
		return Result{}, ErrExecutionNotFound
	}
	return toResult(snap.State), nil
}
