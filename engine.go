package txsaga

import (
	"context"
	"strings"
	"sync"
	"time"
)

// ValidateDefinition 校验 Saga 定义，非法时返回 ErrInvalidDefinition（可 errors.Is）。
// 规则：名称非空、至少一个步骤、每个步骤名非空且不重复、每个步骤都提供正向
// 动作；任一动作/补偿的重试最大总调用次数为负、失败等待时长为负同样非法。
// 未配置的重试预算按最大 1 次、等待 0 处理。
// 步骤声明的前置（DependsOn）必须指向定义内已存在的其它步骤：前置名称未知、
// 自依赖、同一前置重复声明或前置关系成环均属非法定义。
// MaxConcurrency 为负数同样非法；0 表示不限制正向并发。
func ValidateDefinition(d Definition) error {
	if strings.TrimSpace(d.Name) == "" {
		return ErrInvalidDefinition
	}
	if len(d.Steps) == 0 {
		return ErrInvalidDefinition
	}
	if d.MaxConcurrency < 0 {
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
		if st.ActionRetry.MaxAttempts < 0 || st.ActionRetry.RetryWait < 0 ||
			st.CompensateRetry.MaxAttempts < 0 || st.CompensateRetry.RetryWait < 0 {
			return ErrInvalidDefinition
		}
		seen[st.Name] = struct{}{}
	}
	return validateDeps(d.Steps)
}

// validateDeps 校验各步骤声明的前置关系：前置必须存在、不得自依赖、不得重复，
// 且前置关系不得成环。
func validateDeps(steps []Step) error {
	index := make(map[string]int, len(steps))
	for i, st := range steps {
		index[st.Name] = i
	}
	deps := make([][]int, len(steps))
	for i, st := range steps {
		dup := make(map[string]struct{}, len(st.DependsOn))
		for _, dep := range st.DependsOn {
			if dep == st.Name {
				return ErrInvalidDefinition // 自依赖
			}
			j, ok := index[dep]
			if !ok {
				return ErrInvalidDefinition // 前置未知
			}
			if _, exists := dup[dep]; exists {
				return ErrInvalidDefinition // 重复前置
			}
			dup[dep] = struct{}{}
			deps[i] = append(deps[i], j)
		}
	}
	// 三色 DFS 检测环：灰色表示当前递归栈上，再次遇到即成环。
	const (
		white = iota // 未访问
		gray         // 递归栈上
		black        // 已完成
	)
	color := make([]int, len(steps))
	var cyclic func(i int) bool
	cyclic = func(i int) bool {
		color[i] = gray
		for _, j := range deps[i] {
			if color[j] == gray || (color[j] == white && cyclic(j)) {
				return true
			}
		}
		color[i] = black
		return false
	}
	for i := range steps {
		if color[i] == white && cyclic(i) {
			return ErrInvalidDefinition
		}
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
	return succeededNames(v.state)
}

// Execute 按 Definition 执行一次 Saga 请求。
//
// 执行身份由业务键、Saga 定义与外部幂等键共同确定：
//   - 新身份：创建执行并执行未完成步骤；
//   - 同身份重试：不重复执行已确认成功的正向步骤或补偿步骤；
//     结果未确认的步骤允许被重新调用，重入次数累计在对应步骤的 Attempts，
//     动作自身须遵守幂等契约；
//   - 已到达终态：直接返回固定的终态结果，不再调用任何动作，也不再追加事件；
//   - 同业务键绑定不同定义：返回 ErrDefinitionConflict；
//   - 非法定义或缺失业务键/幂等键：返回 ErrInvalidDefinition。
//
// 调度分两种模式：定义中所有步骤都未声明前置（DependsOn 为空）时按 Steps
// 声明顺序逐个执行；任一步骤声明前置后进入依赖图模式——步骤在其全部前置
// 确认成功后才启动，无前置的步骤同时启动、并发执行，某一步确认后立即调度
// 因此就绪的后续步骤。Definition.MaxConcurrency 为正数时限制依赖图模式下
// 已启动但尚未确认提交的步骤数量：额度占满后其余就绪步骤保持 pending，
// 额度有空位时按 Steps 声明顺序选择最早出现的就绪步骤启动；重试等待期间
// 额度不释放。顺序模式不并发，不受该配置影响。
// 任一正向动作预算耗尽仍失败时，停止调度未启动步骤，
// 已启动的兄弟动作继续运行到返回并各自原子提交结果，随后仅按确认成功顺序
// 的逆序补偿已确认成功的步骤；未执行到或未确认的步骤不补偿。
//
// 每一步确认后，状态与对应 Outbox 事件在同一个 Store 事务中原子提交，
// 然后才调度后续步骤。正向动作在其 ActionRetry 预算内的失败会在指定等待后
// 重试同一动作，中间失败不提交状态、不追加事件；预算耗尽仍失败才停止推进。
// 动作返回 Permanent 包装的永久失败时即使预算尚有剩余也只调用一次、不再
// 等待，按预算耗尽的同一口径提交失败。补偿动作同样可配置 CompensateRetry
// 预算，预算耗尽仍报错或返回永久失败则固定为 compensation_failed 终态。
// 重试等待与动作本身都响应 ctx：ctx 取消时（即使动作刚返回永久失败）
// Execute 返回 ctx 的错误，不确认当前动作、不调度新的兄弟步骤、不启动补偿，
// 也不改变已经提交的执行状态。
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

// advanceForward 推进正向步骤：顺序模式（定义未声明任何前置）执行下一个
// 未完成步骤；依赖图模式按前置满足情况并发调度全部就绪步骤。
// 动作在事务外调用，成功/失败结果与事件原子提交。返回 false 表示已无待执行步骤。
//
// 步骤配置了 ActionRetry 时，动作在预算内失败会等待指定时长后再次调用同一
// 动作；中间失败既不提交状态也不追加事件。等待与动作都响应 ctx：ctx 取消
// 时直接返回其错误，本次不确认动作、不启动补偿，已提交的步骤状态保持不变。
func (e *Engine) advanceForward(ctx context.Context, def Definition, req ExecutionRequest, st *ExecutionState) (bool, error) {
	if hasDeps(def) {
		return e.graphForward(ctx, def, req, st)
	}

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

	// Attempts 记录本阶段（含预算内重试）动作的实际调用次数，成功或预算
	// 耗尽后随状态一次提交；未确认的中间失败不会留下任何计数。
	startedAt := e.now()
	actionErr, attempts, err := e.runWithRetry(ctx, step.ActionRetry, func() error {
		return step.Action(ctx, execView{state: st, payload: req.Payload})
	})
	if err != nil {
		// 调用方取消（等待期间或动作随 ctx 结束）：不解释为业务失败，
		// 未确认的步骤留待后续同身份重试，已确认的提交状态不变。
		return false, err
	}
	finishedAt := e.now()
	st.Steps[idx].Attempts = attempts
	st.Steps[idx].StartedAt = startedAt

	if err := e.commitStepResult(ctx, def, req, idx, startedAt, finishedAt, attempts, actionErr); err != nil {
		return false, err
	}
	return true, nil
}

// hasDeps 报告定义是否声明了任何前置关系：是则进入依赖图调度模式，
// 否则保持按声明顺序逐个执行的顺序模式。
func hasDeps(def Definition) bool {
	for _, s := range def.Steps {
		if len(s.DependsOn) > 0 {
			return true
		}
	}
	return false
}

// graphForward 依赖图模式的正向调度：前置全部确认成功的步骤可启动，无前置
// 的步骤同时启动；某一步确认后立即检查并启动因此就绪的后续步骤。
//
// def.MaxConcurrency 为正数时限制并发额度：已启动但尚未确认提交的步骤数
// （含 ActionRetry 重试等待期间）不超过该值；额度占满后其余就绪步骤保持
// pending，任一步骤确认提交并释放额度后，按 Steps 声明顺序选择最早出现的
// 就绪步骤补位。0 表示不限，全部就绪步骤同时启动。
//
// 任一步骤预算耗尽仍失败（或 ctx 取消、提交出错）时停止调度未启动步骤，
// 但已启动的兄弟动作会继续运行到返回并各自原子提交结果，随后本函数才返回。
// 返回 false 表示没有任何步骤需要启动（全部已确认）。
func (e *Engine) graphForward(ctx context.Context, def Definition, req ExecutionRequest, st *ExecutionState) (bool, error) {
	index := make(map[string]int, len(def.Steps))
	for i, s := range def.Steps {
		index[s.Name] = i
	}
	deps := make([][]int, len(def.Steps))
	for i, s := range def.Steps {
		for _, d := range s.DependsOn {
			deps[i] = append(deps[i], index[d])
		}
	}

	// local 是调度器私有的状态镜像：随各动作提交结果而更新，用于计算就绪
	// 步骤与构造动作启动时刻的只读视图。只有本 goroutine 读写它，工作
	// goroutine 拿到的是启动时刻的克隆，无需加锁。
	local := cloneState(st)
	launched := make([]bool, len(def.Steps))
	for i := range local.Steps {
		if local.Steps[i].Status != "" {
			launched[i] = true // 已确认（成功/失败）的步骤不再进入
		}
	}

	type outcome struct {
		idx       int
		actionErr error // 预算耗尽后的业务错误；nil 表示确认成功
		callErr   error // ctx 取消或状态提交失败
	}
	results := make(chan outcome, len(def.Steps))
	inFlight := 0
	launchedAny := false
	halted := false // 失败/取消/出错后停止调度新步骤
	var firstErr error

	// limit 是正向并发额度：0 表示不限；正数表示已启动但未确认提交的步骤数
	// 上限。额度从动作启动占用到结果提交（含重试等待），确认后才释放。
	limit := def.MaxConcurrency

	for {
		if !halted {
			// 按声明顺序扫描，额度有空位时启动最早出现的就绪步骤；
			// 额度占满后其余就绪步骤保持 pending，等确认释放额度后
			// 外层循环重新扫描补位。
			for i := range def.Steps {
				if limit > 0 && inFlight >= limit {
					break
				}
				if launched[i] {
					continue
				}
				ready := true
				for _, d := range deps[i] {
					if local.Steps[d].Status != StepResultSucceeded {
						ready = false
						break
					}
				}
				if !ready {
					continue
				}
				launched[i] = true
				launchedAny = true
				inFlight++
				view := cloneState(local)
				go func(i int, view *ExecutionState) {
					step := def.Steps[i]
					startedAt := e.now()
					actionErr, attempts, callErr := e.runWithRetry(ctx, step.ActionRetry, func() error {
						return step.Action(ctx, execView{state: view, payload: req.Payload})
					})
					if callErr != nil {
						results <- outcome{idx: i, callErr: callErr}
						return
					}
					finishedAt := e.now()
					if err := e.commitStepResult(ctx, def, req, i, startedAt, finishedAt, attempts, actionErr); err != nil {
						results <- outcome{idx: i, callErr: err}
						return
					}
					results <- outcome{idx: i, actionErr: actionErr}
				}(i, view)
			}
		}
		if inFlight == 0 {
			break
		}
		r := <-results
		inFlight--
		switch {
		case r.callErr != nil:
			if firstErr == nil {
				firstErr = r.callErr
			}
			halted = true
		case r.actionErr != nil:
			local.Steps[r.idx].Status = StepResultFailed
			halted = true // 停止调度未启动步骤；已启动的兄弟动作继续跑完并提交
		default:
			local.Steps[r.idx].Status = StepResultSucceeded
			local.SucceededOrder = append(local.SucceededOrder, local.Steps[r.idx].Name)
		}
	}
	return launchedAny, firstErr
}

// commitStepResult 把一个正向步骤的确认结果（成功或预算耗尽的失败）与对应
// 事件在同一个 Store 事务中原子提交。顺序与依赖图模式共用本函数。
func (e *Engine) commitStepResult(ctx context.Context, def Definition, req ExecutionRequest, idx int, startedAt, finishedAt time.Time, attempts int, actionErr error) error {
	step := def.Steps[idx]
	return e.store.Commit(ctx, req.BusinessKey, req.IdempotencyKey, func(tx Tx) error {
		cur := tx.State()
		if cur == nil || cur.Terminal() {
			return nil // 状态已被其它路径推进，本次结果丢弃
		}
		cs := &cur.Steps[idx]
		cs.Attempts = attempts
		cs.StartedAt = startedAt
		cs.FinishedAt = finishedAt

		if actionErr == nil {
			cs.Status = StepResultSucceeded
			cs.LastError = ""
			cur.SucceededOrder = append(cur.SucceededOrder, step.Name)
			succeeded := succeededNames(cur)
			if allSucceeded(cur) {
				cur.Status = StatusCompleted
				tx.AppendEvent(EventStepSucceeded, stepEvent(def, req, step.Name, StatusCompleted, succeeded))
				tx.AppendEvent(EventExecutionCompleted, EventPayload{
					SagaName:       def.Name,
					IdempotencyKey: req.IdempotencyKey,
					Status:         StatusCompleted,
					SucceededSteps: succeeded,
				})
			} else {
				// 依赖图模式下兄弟步骤可能已失败：此时执行处于 compensating，
				// 成功结果仍照常提交，执行状态保持 compensating 不变。
				tx.AppendEvent(EventStepSucceeded, stepEvent(def, req, step.Name, cur.Status, succeeded))
			}
			return nil
		}

		errMsg := actionErr.Error()
		cs.Status = StepResultFailed
		cs.LastError = errMsg
		if cur.Status != StatusCompensating {
			// 首个失败驱动执行转入补偿；并发的兄弟步骤随后也失败时只记录
			// 该步自身的错误，不覆盖执行级失败信息。
			cur.FailedStep = step.Name
			cur.FailureReason = errMsg
			cur.Status = StatusCompensating
		}
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
}

// runWithRetry 在重试预算内反复调用同一动作。
//
// 返回 actionErr 为 nil 表示预算内成功（调用次数计入 attempts）；
// actionErr 非 nil、err 为 nil 表示预算耗尽仍失败（或失败被 Permanent
// 标记为永久失败），actionErr 为最近一次错误；
// err 非 nil 表示外层 context 已取消——可能发生在调用前、失败后的等待期间，
// 也可能动作刚返回（无论它返回 nil 还是错误），只要外层 context 已取消就
// 按取消处理：不确认动作，由调用方原样返回 ctx 错误。
//
// 永久失败优先于重试预算：动作返回 Permanent 包装的错误时，即使仍有剩余
// 次数也立即结束本阶段、不再等待，但 context 取消仍优先于永久失败。
func (e *Engine) runWithRetry(ctx context.Context, policy RetryPolicy, call func() error) (actionErr error, attempts int, err error) {
	budget := normalizeRetry(policy)
	for {
		if err := ctx.Err(); err != nil {
			return nil, attempts, err
		}
		attempts++
		actionErr = call()
		// 与基线一致：动作返回后先看外层 context，已取消则即使动作报告成功
		// 或永久失败也不确认该动作——结果可能已在下游生效，由同身份重入依赖
		// 动作幂等收敛。
		if err := ctx.Err(); err != nil {
			return nil, attempts, err
		}
		if actionErr == nil {
			return nil, attempts, nil
		}
		// 永久失败：业务上已明确不可能成功，即使预算尚有剩余也不再等待、
		// 不再调用，按与预算耗尽相同的口径交由调用方提交。
		if IsPermanent(actionErr) {
			return actionErr, attempts, nil
		}
		if attempts >= budget.maxAttempts {
			return actionErr, attempts, nil
		}
		// 仍有剩余次数：等待指定时长后再次调用同一动作，等待响应 context。
		if budget.wait > 0 {
			t := time.NewTimer(budget.wait)
			select {
			case <-ctx.Done():
				t.Stop()
				return nil, attempts, ctx.Err()
			case <-t.C:
			}
		}
	}
}

// advanceCompensation 按确认成功顺序的逆序补偿下一个已确认成功的步骤。
// 返回 done=true 表示已无待补偿步骤，调用方应把执行收束为 failed 终态。
// 补偿动作返回错误时，执行在同一事务中固定为 compensation_failed 终态。
func (e *Engine) advanceCompensation(ctx context.Context, def Definition, req ExecutionRequest, st *ExecutionState) (bool, error) {
	// 确认顺序的逆序：最后确认成功的步骤最先补偿。顺序模式下确认顺序即
	// 声明顺序，与此前按声明逆序补偿的行为一致。
	succeeded := succeededNames(st)
	idx := -1
	if len(succeeded) > 0 {
		target := succeeded[len(succeeded)-1]
		for i := range st.Steps {
			if st.Steps[i].Name == target {
				idx = i
				break
			}
		}
	}
	if idx < 0 {
		return true, nil
	}
	step := def.Steps[idx]

	// 未配置补偿动作的步骤视为补偿成功，不计数、不记录补偿时间。
	var compErr error
	var attempts int
	var compensatedAt time.Time
	if step.Compensate != nil {
		// CompensationAttempts 记录本阶段（含预算内重试）补偿的实际调用次数，
		// 成功或预算耗尽后随状态一次提交；中间失败不留计数、不追加事件。
		var callErr error
		compErr, attempts, callErr = e.runWithRetry(ctx, step.CompensateRetry, func() error {
			return step.Compensate(ctx, execView{state: st, payload: req.Payload})
		})
		if callErr != nil {
			return false, callErr
		}
		compensatedAt = e.now()
		st.Steps[idx].CompensationAttempts = attempts
	}

	err := e.store.Commit(ctx, req.BusinessKey, req.IdempotencyKey, func(tx Tx) error {
		cur := tx.State()
		if cur == nil || cur.Terminal() {
			return nil
		}
		cs := &cur.Steps[idx]
		if step.Compensate != nil {
			cs.CompensationAttempts = attempts
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

// succeededNames 返回当前仍为成功状态的步骤名，按确认成功的先后顺序排列。
// 旧版本写入的状态没有确认顺序记录，回退为声明顺序（与当时的确认顺序一致）。
func succeededNames(st *ExecutionState) []string {
	if len(st.SucceededOrder) == 0 {
		var names []string
		for _, s := range st.Steps {
			if s.Status == StepResultSucceeded {
				names = append(names, s.Name)
			}
		}
		return names
	}
	status := make(map[string]string, len(st.Steps))
	for _, s := range st.Steps {
		status[s.Name] = s.Status
	}
	var names []string
	for _, n := range st.SucceededOrder {
		if status[n] == StepResultSucceeded {
			names = append(names, n)
		}
	}
	return names
}

// allSucceeded 报告全部步骤是否均已确认成功。
func allSucceeded(st *ExecutionState) bool {
	for _, s := range st.Steps {
		if s.Status != StepResultSucceeded {
			return false
		}
	}
	return true
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
