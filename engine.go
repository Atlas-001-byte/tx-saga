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
// 使用 Step.DependsOn 声明依赖时，前置步骤必须存在、不得依赖自身、同一前置
// 不得重复列出，且依赖图必须无环；违反任一条同样返回 ErrInvalidDefinition。
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
		if st.ActionRetry.MaxAttempts < 0 || st.ActionRetry.RetryWait < 0 ||
			st.CompensateRetry.MaxAttempts < 0 || st.CompensateRetry.RetryWait < 0 {
			return ErrInvalidDefinition
		}
		seen[st.Name] = struct{}{}
	}
	if _, err := buildGraph(d); err != nil {
		return err
	}
	return nil
}

// graph 是归一化后的前置依赖图：deps[i] 为步骤 i 的全部前置步骤下标。
type graph struct {
	deps [][]int
}

// buildGraph 构建并校验依赖图。任一步骤声明了前置即进入依赖图模式；
// 全部步骤都没有前置时返回 nil 图，编排退化为顺序模式。
// 未知依赖、自依赖、重复前置或存在环均返回 ErrInvalidDefinition。
func buildGraph(def Definition) (*graph, error) {
	index := make(map[string]int, len(def.Steps))
	for i, s := range def.Steps {
		index[s.Name] = i
	}
	g := &graph{deps: make([][]int, len(def.Steps))}
	hasEdges := false
	for i, s := range def.Steps {
		if len(s.DependsOn) == 0 {
			continue
		}
		hasEdges = true
		listed := make(map[int]struct{}, len(s.DependsOn))
		for _, depName := range s.DependsOn {
			j, ok := index[depName]
			if !ok || j == i {
				// 前置指向不存在的步骤，或步骤把自己列为前置。
				return nil, ErrInvalidDefinition
			}
			if _, dup := listed[j]; dup {
				// 同一前置被重复列出（按步骤名判定）。
				return nil, ErrInvalidDefinition
			}
			listed[j] = struct{}{}
			g.deps[i] = append(g.deps[i], j)
		}
	}
	if !hasEdges {
		return nil, nil
	}
	if g.hasCycle() {
		return nil, ErrInvalidDefinition
	}
	return g, nil
}

// hasCycle 以三色 DFS 判定依赖图中是否存在环。
func (g *graph) hasCycle() bool {
	const (
		white = 0 // 尚未访问
		gray  = 1 // 在当前 DFS 路径上
		black = 2 // 已完全展开
	)
	color := make([]byte, len(g.deps))
	var visit func(i int) bool
	visit = func(i int) bool {
		color[i] = gray
		for _, j := range g.deps[i] {
			if color[j] == gray {
				return true // 回到当前路径上的节点，成环
			}
			if color[j] == white && visit(j) {
				return true
			}
		}
		color[i] = black
		return false
	}
	for i := range g.deps {
		if color[i] == white && visit(i) {
			return true
		}
	}
	return false
}

func stepIndex(def Definition) map[string]int {
	idx := make(map[string]int, len(def.Steps))
	for i, s := range def.Steps {
		idx[s.Name] = i
	}
	return idx
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
	// state 是调用时刻的执行状态静态快照，顺序路径与补偿路径使用。
	state *ExecutionState
	// current 非空时（依赖图并发路径）返回已提交状态的最新视图，
	// 使并发动作看到此前已确认成功的步骤；与 state 二选一。
	current func() *ExecutionState
	payload any
}

func (v execView) viewState() *ExecutionState {
	if v.current != nil {
		return v.current()
	}
	return v.state
}

func (v execView) BusinessKey() string    { return v.viewState().BusinessKey }
func (v execView) SagaName() string       { return v.viewState().SagaName }
func (v execView) IdempotencyKey() string { return v.viewState().IdempotencyKey }
func (v execView) Payload() any           { return v.payload }

func (v execView) SucceededSteps() []string {
	st := v.viewState()
	// 旧状态没有确认顺序快照时，回退枚举即最终口径。
	if len(st.SucceededOrder) == 0 {
		return succeededOrder(st)
	}
	// 以确认顺序快照为准排序，但只保留当前仍为 succeeded 的步骤：补偿阶段
	// 已 compensated 的步骤不再出现，与顺序模式下的既有视图口径保持一致。
	status := make(map[string]string, len(st.Steps))
	for _, s := range st.Steps {
		status[s.Name] = s.Status
	}
	names := make([]string, 0, len(st.SucceededOrder))
	for _, name := range st.SucceededOrder {
		if status[name] == StepResultSucceeded {
			names = append(names, name)
		}
	}
	return names
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
//   - 非法定义（含依赖未知、自依赖、重复前置或依赖成环）或缺失业务键/幂等键：
//     返回 ErrInvalidDefinition，且不创建执行、不调用动作、不追加事件。
//
// 步骤均未声明 Step.DependsOn 时按声明顺序串行推进；声明了依赖时按依赖图
// 调度：步骤只有在全部前置步骤确认成功后才启动，无前置（或前置均已确认）
// 的无依赖步骤并发执行。每一步确认后，状态与对应 Outbox 事件在同一个 Store
// 事务中原子提交。正向动作在其 ActionRetry 预算内的失败会在指定等待后重试
// 同一动作，中间失败不提交状态、不追加事件；预算耗尽仍失败才停止调度尚未
// 启动的步骤，已启动的兄弟动作仍会返回并提交各自结果，随后仅按成功步骤的
// 确认顺序逆序补偿此前确认成功的步骤。补偿动作同样可配置 CompensateRetry
// 预算，预算耗尽仍报错则固定为 compensation_failed 终态。重试等待与动作
// 本身都响应 ctx：ctx 在动作或等待中取消时 Execute 返回 ctx 的错误，不确认
// 当前动作、不调度新的兄弟步骤、不启动补偿，也不改变已提交状态与既有事件，
// 以同一业务键、Saga 定义与外部幂等键再次调用即可继续处理未确认部分。
func (e *Engine) Execute(ctx context.Context, def Definition, req ExecutionRequest) (Result, error) {
	g, err := validateAndGraph(def)
	if err != nil {
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
			var advanced bool
			var err error
			if g != nil {
				advanced, err = e.advanceGraph(ctx, def, g, req, st)
			} else {
				advanced, err = e.advanceForward(ctx, def, req, st)
			}
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

// validateAndGraph 校验定义并返回归一化依赖图；顺序模式（无任何前置声明）
// 返回 nil 图。
func validateAndGraph(def Definition) (*graph, error) {
	if err := ValidateDefinition(def); err != nil {
		return nil, err
	}
	g, err := buildGraph(def)
	if err != nil {
		return nil, err
	}
	return g, nil
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
//
// 步骤配置了 ActionRetry 时，动作在预算内失败会等待指定时长后再次调用同一
// 动作；中间失败既不提交状态也不追加事件。等待与动作都响应 ctx：ctx 取消
// 时直接返回其错误，本次不确认动作、不启动补偿，已提交的步骤状态保持不变。
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

	err = e.store.Commit(ctx, req.BusinessKey, req.IdempotencyKey, func(tx Tx) error {
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
			succeeded := recordSucceeded(cur, step.Name)
			if allStepsDecided(cur) {
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
			SucceededSteps: succeededOrder(cur),
		})
		return nil
	})
	return err == nil, err
}

// runWithRetry 在重试预算内反复调用同一动作。
//
// 返回 actionErr 为 nil 表示预算内成功（调用次数计入 attempts）；
// actionErr 非 nil、err 为 nil 表示预算耗尽仍失败，actionErr 为最近一次错误；
// err 非 nil 表示外层 context 已取消——可能发生在调用前、失败后的等待期间，
// 也可能动作刚返回（无论它返回 nil 还是错误），只要外层 context 已取消就
// 按取消处理：不确认动作，由调用方原样返回 ctx 错误。
func (e *Engine) runWithRetry(ctx context.Context, policy RetryPolicy, call func() error) (actionErr error, attempts int, err error) {
	budget := normalizeRetry(policy)
	for {
		if err := ctx.Err(); err != nil {
			return nil, attempts, err
		}
		attempts++
		actionErr = call()
		// 与基线一致：动作返回后先看外层 context，已取消则即使动作报告成功
		// 也不确认该动作——结果可能已在下游生效，由同身份重入依赖动作幂等收敛。
		if err := ctx.Err(); err != nil {
			return nil, attempts, err
		}
		if actionErr == nil {
			return nil, attempts, nil
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

// graphOutcome 是依赖图模式下单个步骤动作执行阶段（事务外）的结论。
type graphOutcome struct {
	idx        int
	actionErr  error // nil 表示确认成功；非 nil 为预算耗尽后的最近错误
	attempts   int
	startedAt  time.Time
	finishedAt time.Time
}

// advanceGraph 按依赖图调度并发波次：
//
// 选取所有「未启动且全部前置已确认成功」的步骤在同一波次内并发启动。
// 每个动作在事务外运行（含其 ActionRetry 预算），全部返回后按声明顺序逐个
// 独立提交：成功状态与事件在单个 Store 事务中原子可见，步骤名追加到确认
// 顺序快照末尾，随后波次中其下游才可能启动。任一步骤预算耗尽失败后不再
// 调度新的（尚未启动的）步骤，但已启动的兄弟动作都会运行到返回并提交各自
// 结果：成功的兄弟纳入补偿，失败步骤自身不补偿。ctx 在动作或等待中取消时
// 不确认当前波次的任何结果、不调度新的兄弟步骤，已提交状态与既有事件不变。
//
// 返回 advanced=false 表示全部步骤均已确认成功（执行已进入 completed）。
func (e *Engine) advanceGraph(ctx context.Context, def Definition, g *graph, req ExecutionRequest, initial *ExecutionState) (bool, error) {
	// live 是已提交状态的进程内视图：每个结果事务提交后更新，让后续波次
	// 的就绪判定与并发动作的 SucceededSteps 看到此前已确认的步骤。
	// 同一波次提交前不更新，因此波次内动作只见此前波次的已提交状态。
	live := initial
	var liveMu sync.Mutex
	loadLive := func() *ExecutionState {
		liveMu.Lock()
		defer liveMu.Unlock()
		return live
	}
	applyLive := func(updated *ExecutionState) {
		liveMu.Lock()
		live = updated
		liveMu.Unlock()
	}

	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		st := loadLive()
		if st.Status != StatusRunning {
			// 上一波次已提交失败，执行进入补偿阶段，正向调度结束。
			return true, nil
		}

		// 只有 succeeded 状态满足前置；failed 不满足，其下游保持 pending。
		ready := make([]int, 0)
		pending := 0
		for i := range st.Steps {
			if st.Steps[i].Status != "" {
				continue
			}
			pending++
			met := true
			for _, j := range g.deps[i] {
				if st.Steps[j].Status != StepResultSucceeded {
					met = false
					break
				}
			}
			if met {
				ready = append(ready, i)
			}
		}
		if pending == 0 {
			return false, nil
		}
		if len(ready) == 0 {
			// 有未启动步骤却无可启动步骤：它们的前置已失败，失败提交已使
			// 执行离开 running；交回外层重新加载并转入补偿。
			return true, nil
		}

		outcomes := make([]*graphOutcome, len(ready))
		var wg sync.WaitGroup
		for slot, idx := range ready {
			idx, slot := idx, slot
			step := def.Steps[idx]
			wg.Add(1)
			go func() {
				defer wg.Done()
				out := &graphOutcome{idx: idx, startedAt: e.now()}
				// Attempts 为本阶段（含预算内重试）动作的实际调用次数，
				// 随该步结果一次提交；中间失败不留计数、不追加事件。
				actionErr, attempts, _ := e.runWithRetry(ctx, step.ActionRetry, func() error {
					return step.Action(ctx, execView{current: loadLive, payload: req.Payload})
				})
				out.attempts = attempts
				if ctx.Err() != nil {
					// 调用方取消（等待期间或动作随 ctx 结束）：不解释为业务
					// 失败，不确认本动作；已提交的兄弟步骤保持不变，留待同
					// 身份重试依赖动作幂等收敛。
					return
				}
				out.actionErr = actionErr
				out.finishedAt = e.now()
				outcomes[slot] = out
			}()
		}
		wg.Wait()

		if err := ctx.Err(); err != nil {
			// 动作或等待中取消：本波次结果一律不确认、不调度新波次，
			// 已提交状态与既有事件不变。
			return false, err
		}

		// 全部动作均已返回且 ctx 未取消。按就绪下标声明顺序逐个原子提交，
		// 使事件追加顺序确定；每个结果各自独立成事务。
		for _, out := range outcomes {
			if out == nil {
				continue
			}
			committed, err := e.commitGraphOutcome(ctx, def, req, out)
			if err != nil {
				return false, err
			}
			if committed != nil {
				applyLive(committed)
			}
		}
	}
}

// commitGraphOutcome 把依赖图模式下单个步骤的动作结果原子提交。
// 返回提交后的最新状态副本；状态不存在或已终态（结果应丢弃）时返回 nil。
//
// 同一波次中若已有兄弟步骤先行提交失败，执行状态已为 compensating：本步若
// 成功仍确认成功并纳入补偿（追加成功事件），但不把状态改回 running，也不
// 追加 completed；本步若失败则记录该步错误，执行级失败信息保留首个失败。
func (e *Engine) commitGraphOutcome(ctx context.Context, def Definition, req ExecutionRequest, out *graphOutcome) (*ExecutionState, error) {
	step := def.Steps[out.idx]
	err := e.store.Commit(ctx, req.BusinessKey, req.IdempotencyKey, func(tx Tx) error {
		cur := tx.State()
		if cur == nil || cur.Terminal() {
			return nil // 状态已被其它路径推进，本次结果丢弃
		}
		cs := &cur.Steps[out.idx]
		if cs.Status != "" {
			return nil // 该步已被确认，丢弃本次结论
		}
		cs.Attempts = out.attempts
		cs.StartedAt = out.startedAt
		cs.FinishedAt = out.finishedAt

		if out.actionErr == nil {
			cs.Status = StepResultSucceeded
			cs.LastError = ""
			succeeded := recordSucceeded(cur, step.Name)
			if cur.Status == StatusRunning && allStepsDecided(cur) {
				cur.Status = StatusCompleted
				tx.AppendEvent(EventStepSucceeded, stepEvent(def, req, step.Name, StatusCompleted, succeeded))
				tx.AppendEvent(EventExecutionCompleted, EventPayload{
					SagaName:       def.Name,
					IdempotencyKey: req.IdempotencyKey,
					Status:         StatusCompleted,
					SucceededSteps: succeeded,
				})
				return nil
			}
			// 仍在 running，或同波次已有兄弟失败（compensating）：状态沿用
			// 当前值，成功事件携带当前状态，成功步骤照样纳入补偿。
			tx.AppendEvent(EventStepSucceeded, stepEvent(def, req, step.Name, cur.Status, succeeded))
			return nil
		}

		errMsg := out.actionErr.Error()
		cs.Status = StepResultFailed
		cs.LastError = errMsg
		if cur.Status == StatusRunning {
			// 首个失败：执行转入补偿，记录执行级失败信息。
			cur.FailedStep = step.Name
			cur.FailureReason = errMsg
			cur.Status = StatusCompensating
		}
		tx.AppendEvent(EventStepFailed, EventPayload{
			SagaName:       def.Name,
			IdempotencyKey: req.IdempotencyKey,
			Step:           step.Name,
			Status:         cur.Status,
			Reason:         errMsg,
			SucceededSteps: succeededOrder(cur),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	snap, err := e.store.LoadSnapshot(ctx, req.BusinessKey, req.IdempotencyKey)
	if err != nil {
		return nil, err
	}
	return snap.State, nil
}

// advanceCompensation 按确认顺序逆序补偿下一个已确认成功的步骤。
// 返回 done=true 表示已无待补偿步骤，调用方应把执行收束为 failed 终态。
// 补偿动作返回错误时，执行在同一事务中固定为 compensation_failed 终态。
func (e *Engine) advanceCompensation(ctx context.Context, def Definition, req ExecutionRequest, st *ExecutionState) (bool, error) {
	nameIdx := stepIndex(def)

	// 待补偿步骤：确认顺序快照中的最后一个、当前状态仍为 succeeded 的步骤。
	idx := -1
	order := succeededOrder(st)
	for i := len(order) - 1; i >= 0; i-- {
		j, ok := nameIdx[order[i]]
		if !ok {
			continue
		}
		if st.Steps[j].Status == StepResultSucceeded {
			idx = j
			break
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

// succeededOrder 返回已确认成功步骤的确认顺序快照副本。
// 优先使用持久化的 SucceededOrder（依赖图模式下由成功事务提交顺序确定）；
// 兼容旧状态中该字段为空的情形：此时回退为按 Steps 声明顺序枚举 succeeded，
// 顺序模式下两者完全一致。
func succeededOrder(st *ExecutionState) []string {
	if len(st.SucceededOrder) > 0 {
		return append([]string(nil), st.SucceededOrder...)
	}
	var names []string
	for _, s := range st.Steps {
		if s.Status == StepResultSucceeded {
			names = append(names, s.Name)
		}
	}
	return names
}

// recordSucceeded 把刚确认成功的步骤名写入确认顺序快照（已存在则不重复），
// 并回写状态。旧状态缺失 SucceededOrder 时借此按声明顺序自愈。
func recordSucceeded(cur *ExecutionState, name string) []string {
	order := succeededOrder(cur)
	for _, n := range order {
		if n == name {
			cur.SucceededOrder = order
			return order
		}
	}
	order = append(order, name)
	cur.SucceededOrder = order
	return order
}

// allStepsDecided 报告是否已无 pending（空状态）步骤。
func allStepsDecided(cur *ExecutionState) bool {
	for _, s := range cur.Steps {
		if s.Status == "" {
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
