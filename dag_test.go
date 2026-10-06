package txsaga

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// gate 是一个可开关的动作闸门：Close 后动作放行，否则一直阻塞到 ctx 取消。
// 用于精确控制并发步骤的完成时序。
type gate struct {
	ch chan struct{}
}

func newGate() *gate { return &gate{ch: make(chan struct{})} }

func (g *gate) open() { close(g.ch) }

func (g *gate) wait(ctx context.Context) error {
	select {
	case <-g.ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// dagRecorder 记录动作的开始与结束时刻，供断言并发与确认顺序。
type dagRecorder struct {
	mu      sync.Mutex
	entries []string
}

func (r *dagRecorder) log(s string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, s)
}

func (r *dagRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.entries...)
}

// indexOf 返回首个等于 want 的元素下标，不存在为 -1。
func indexOf(ss []string, want string) int {
	for i, s := range ss {
		if s == want {
			return i
		}
	}
	return -1
}

func TestGraphModeRunsIndependentStepsConcurrently(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &dagRecorder{}
	gateA, gateB := newGate(), newGate()
	gateC := newGate()
	gateC.open() // c 不阻塞，依赖满足后立即完成

	step := func(name string, g *gate, deps ...string) Step {
		return Step{
			Name:      name,
			DependsOn: deps,
			Action: func(ctx context.Context, _ ExecutionView) error {
				rec.log("start:" + name)
				if err := g.wait(ctx); err != nil {
					return err
				}
				rec.log("done:" + name)
				return nil
			},
		}
	}
	def := Definition{
		Name: "dag-saga", Version: "v1",
		Steps: []Step{
			step("a", gateA),
			step("b", gateB),
			step("c", gateC, "a", "b"), // c 依赖 a、b
		},
	}

	type execOut struct {
		res Result
		err error
	}
	done := make(chan execOut, 1)
	go func() {
		res, err := eng.Execute(context.Background(), def, ExecutionRequest{
			BusinessKey:    "biz-dag-1",
			IdempotencyKey: "req-dag-1",
		})
		done <- execOut{res, err}
	}()

	// 两个无前置步骤都应在任一放行前启动（并发执行）。
	deadline := time.After(2 * time.Second)
	for indexOf(rec.snapshot(), "start:a") < 0 || indexOf(rec.snapshot(), "start:b") < 0 {
		select {
		case <-deadline:
			t.Fatalf("steps did not both start: %v", rec.snapshot())
		default:
			time.Sleep(time.Millisecond)
		}
	}
	// c 依赖 a、b：任一未确认前不得启动。
	if indexOf(rec.snapshot(), "start:c") >= 0 {
		t.Fatalf("c started before deps confirmed: %v", rec.snapshot())
	}
	gateA.open()
	time.Sleep(20 * time.Millisecond)
	if indexOf(rec.snapshot(), "start:c") >= 0 {
		t.Fatalf("c started with only a confirmed: %v", rec.snapshot())
	}
	gateB.open()

	out := <-done
	if out.err != nil {
		t.Fatalf("Execute: %v", out.err)
	}
	if out.res.Status != StatusCompleted || !out.res.Terminal {
		t.Fatalf("status=%s want completed", out.res.Status)
	}
	got := rec.snapshot()
	if indexOf(got, "done:a") > indexOf(got, "start:c") || indexOf(got, "done:b") > indexOf(got, "start:c") {
		t.Fatalf("c started before both deps done: %v", got)
	}
	// StepOutcome 按声明顺序返回，与完成顺序无关。
	var names []string
	for _, so := range out.res.Steps {
		names = append(names, so.Name)
		if so.Result != StepResultSucceeded || so.Attempts != 1 {
			t.Fatalf("step %s = %+v", so.Name, so)
		}
	}
	if fmt.Sprint(names) != "[a b c]" {
		t.Fatalf("outcome order = %v, want declaration order", names)
	}
}

func TestGraphModeSchedulesDependentAsSoonAsDepsConfirm(t *testing.T) {
	// b 慢、a 快，c 只依赖 a：c 应在 b 仍在阻塞时就启动并完成，
	// 证明调度按依赖满足情况推进，而不是按波次屏障等待所有兄弟。
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &dagRecorder{}
	gateB := newGate()

	def := Definition{
		Name: "dag-saga", Version: "v1",
		Steps: []Step{
			{Name: "a", Action: func(context.Context, ExecutionView) error {
				rec.log("done:a")
				return nil
			}},
			{Name: "b", Action: func(ctx context.Context, _ ExecutionView) error {
				rec.log("start:b")
				if err := gateB.wait(ctx); err != nil {
					return err
				}
				rec.log("done:b")
				return nil
			}},
			{Name: "c", DependsOn: []string{"a"}, Action: func(context.Context, ExecutionView) error {
				rec.log("done:c")
				return nil
			}},
		},
	}

	done := make(chan error, 1)
	go func() {
		_, err := eng.Execute(context.Background(), def, ExecutionRequest{
			BusinessKey: "biz-dag-2", IdempotencyKey: "req-dag-2",
		})
		done <- err
	}()

	deadline := time.After(2 * time.Second)
	for indexOf(rec.snapshot(), "done:c") < 0 {
		select {
		case <-deadline:
			t.Fatalf("c not scheduled while b blocked: %v", rec.snapshot())
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if indexOf(rec.snapshot(), "done:b") >= 0 {
		t.Fatalf("b finished before c even though c only depends on a: %v", rec.snapshot())
	}
	gateB.open()
	if err := <-done; err != nil {
		t.Fatalf("Execute: %v", err)
	}
	res, err := eng.GetResult(context.Background(), "dag-saga", "biz-dag-2", "req-dag-2")
	if err != nil || res.Status != StatusCompleted {
		t.Fatalf("res=%+v err=%v", res, err)
	}
}

func TestValidateDefinitionDependencies(t *testing.T) {
	action := func(context.Context, ExecutionView) error { return nil }
	cases := map[string][]Step{
		"未知前置": {
			{Name: "a", Action: action, DependsOn: []string{"ghost"}},
		},
		"自依赖": {
			{Name: "a", Action: action, DependsOn: []string{"a"}},
		},
		"重复前置": {
			{Name: "a", Action: action},
			{Name: "b", Action: action, DependsOn: []string{"a", "a"}},
		},
		"两步成环": {
			{Name: "a", Action: action, DependsOn: []string{"b"}},
			{Name: "b", Action: action, DependsOn: []string{"a"}},
		},
		"多步成环": {
			{Name: "a", Action: action, DependsOn: []string{"c"}},
			{Name: "b", Action: action, DependsOn: []string{"a"}},
			{Name: "c", Action: action, DependsOn: []string{"b"}},
		},
		"空前置名": {
			{Name: "a", Action: action, DependsOn: []string{""}},
		},
	}
	for name, steps := range cases {
		t.Run(name, func(t *testing.T) {
			def := Definition{Name: "bad-dag", Version: "v1", Steps: steps}
			if err := ValidateDefinition(def); !errors.Is(err, ErrInvalidDefinition) {
				t.Fatalf("ValidateDefinition = %v, want ErrInvalidDefinition", err)
			}

			// Execute 同样拒绝，且不创建执行、不调用动作、不追加事件。
			store := NewMemoryStore()
			eng := NewEngine(store)
			called := false
			badSteps := append([]Step(nil), steps...)
			for i := range badSteps {
				orig := badSteps[i].Action
				badSteps[i].Action = func(ctx context.Context, v ExecutionView) error {
					called = true
					return orig(ctx, v)
				}
			}
			_, err := eng.Execute(context.Background(), Definition{Name: "bad-dag", Version: "v1", Steps: badSteps},
				ExecutionRequest{BusinessKey: "biz-bad", IdempotencyKey: "req-bad"})
			if !errors.Is(err, ErrInvalidDefinition) {
				t.Fatalf("Execute = %v, want ErrInvalidDefinition", err)
			}
			if called {
				t.Fatal("action called for invalid definition")
			}
			if store.TotalEvents() != 0 {
				t.Fatalf("events appended for invalid definition: %d", store.TotalEvents())
			}
			if _, err := eng.GetResult(context.Background(), "bad-dag", "biz-bad", "req-bad"); !errors.Is(err, ErrExecutionNotFound) {
				t.Fatalf("GetResult = %v, want ErrExecutionNotFound", err)
			}
		})
	}
}

func TestGraphModeCompensatesInReverseConfirmationOrder(t *testing.T) {
	// 声明顺序 [slow fast failer]，确认顺序为 fast、slow（fast 先完成），
	// 补偿应按确认逆序：先 slow 后 fast——与声明逆序不同，可区分两种口径。
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &dagRecorder{}
	gateSlow := newGate()

	mk := func(name string, deps ...string) Step {
		return Step{
			Name:      name,
			DependsOn: deps,
			Action: func(ctx context.Context, _ ExecutionView) error {
				if name == "slow" {
					if err := gateSlow.wait(ctx); err != nil {
						return err
					}
				}
				rec.log("done:" + name)
				return nil
			},
			Compensate: func(context.Context, ExecutionView) error {
				rec.log("undo:" + name)
				return nil
			},
		}
	}
	failer := Step{
		Name:      "failer",
		DependsOn: []string{"slow", "fast"},
		Action: func(context.Context, ExecutionView) error {
			rec.log("done:failer")
			return errors.New("boom-failer")
		},
		Compensate: func(context.Context, ExecutionView) error {
			rec.log("undo:failer")
			return nil
		},
	}
	def := Definition{
		Name: "dag-saga", Version: "v1",
		Steps: []Step{mk("slow"), mk("fast"), failer},
	}

	done := make(chan Result, 1)
	go func() {
		res, _ := eng.Execute(context.Background(), def, ExecutionRequest{
			BusinessKey: "biz-dag-3", IdempotencyKey: "req-dag-3",
		})
		done <- res
	}()
	// 等 fast 确认后再放行 slow，确保确认顺序为 fast -> slow。
	deadline := time.After(2 * time.Second)
	for indexOf(rec.snapshot(), "done:fast") < 0 {
		select {
		case <-deadline:
			t.Fatal("fast never done")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	gateSlow.open()
	res := <-done

	if res.Status != StatusFailed || !res.Terminal {
		t.Fatalf("status=%s want failed", res.Status)
	}
	if res.FailedStep != "failer" || res.FailureReason != "boom-failer" {
		t.Fatalf("failure info: %+v", res)
	}
	got := rec.snapshot()
	// 确认顺序：fast 先于 slow。
	if indexOf(got, "done:fast") > indexOf(got, "done:slow") {
		t.Fatalf("confirmation order unexpected: %v", got)
	}
	// 补偿顺序为确认逆序：slow 先补偿，fast 后补偿（声明逆序则相反）。
	if indexOf(got, "undo:slow") < 0 || indexOf(got, "undo:fast") < 0 ||
		indexOf(got, "undo:slow") > indexOf(got, "undo:fast") {
		t.Fatalf("compensation not in reverse confirmation order: %v", got)
	}
	// 失败步骤未确认成功，不补偿。
	if indexOf(got, "undo:failer") >= 0 {
		t.Fatalf("failed step compensated: %v", got)
	}
	results := map[string]string{}
	for _, so := range res.Steps {
		results[so.Name] = so.Result
	}
	if results["slow"] != StepResultCompensated || results["fast"] != StepResultCompensated ||
		results["failer"] != StepResultFailed {
		t.Fatalf("step results = %v", results)
	}
}

func TestGraphModeFailureStopsSchedulingAndCommitsInflight(t *testing.T) {
	// a 阻塞中，b 立即失败：c（依赖 a）不得启动；a 放行后仍提交成功并纳入补偿。
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &dagRecorder{}
	gateA := newGate()

	def := Definition{
		Name: "dag-saga", Version: "v1",
		Steps: []Step{
			{Name: "a", Action: func(ctx context.Context, _ ExecutionView) error {
				rec.log("start:a")
				if err := gateA.wait(ctx); err != nil {
					return err
				}
				rec.log("done:a")
				return nil
			}, Compensate: func(context.Context, ExecutionView) error {
				rec.log("undo:a")
				return nil
			}},
			{Name: "b", Action: func(context.Context, ExecutionView) error {
				rec.log("done:b")
				return errors.New("boom-b")
			}},
			{Name: "c", DependsOn: []string{"a"}, Action: func(context.Context, ExecutionView) error {
				rec.log("done:c")
				return nil
			}},
		},
	}

	done := make(chan Result, 1)
	go func() {
		res, _ := eng.Execute(context.Background(), def, ExecutionRequest{
			BusinessKey: "biz-dag-4", IdempotencyKey: "req-dag-4",
		})
		done <- res
	}()
	// 等 b 失败提交、a 仍在阻塞：此时 c 不得被调度。
	deadline := time.After(2 * time.Second)
	for {
		snap, _ := store.LoadSnapshot(context.Background(), "biz-dag-4", "req-dag-4")
		if snap.State != nil && snap.State.Status == StatusCompensating {
			break
		}
		select {
		case <-deadline:
			t.Fatal("execution never entered compensating")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	time.Sleep(20 * time.Millisecond)
	if indexOf(rec.snapshot(), "done:c") >= 0 {
		t.Fatalf("c scheduled after sibling failure: %v", rec.snapshot())
	}
	gateA.open()
	res := <-done

	if res.Status != StatusFailed {
		t.Fatalf("status=%s want failed", res.Status)
	}
	if res.FailedStep != "b" {
		t.Fatalf("failed step = %q want b", res.FailedStep)
	}
	got := rec.snapshot()
	// 已启动的 a 提交成功并被补偿；c 从未启动。
	if indexOf(got, "done:a") < 0 || indexOf(got, "undo:a") < 0 {
		t.Fatalf("inflight sibling not committed/compensated: %v", got)
	}
	if indexOf(got, "done:c") >= 0 {
		t.Fatalf("unstarted step ran: %v", got)
	}
	results := map[string]string{}
	for _, so := range res.Steps {
		results[so.Name] = so.Result
	}
	if results["a"] != StepResultCompensated || results["b"] != StepResultFailed ||
		results["c"] != StepResultPending {
		t.Fatalf("step results = %v", results)
	}

	// 事件：a 的成功事件在 b 的失败事件之后按提交顺序追加。
	events, _ := store.ClaimPendingEvents(context.Background(), 100)
	var types []string
	for _, ev := range events {
		p, _ := ev.Payload.(EventPayload)
		types = append(types, ev.Type+":"+p.Step)
	}
	want := []string{
		EventExecutionStarted + ":",
		EventStepFailed + ":b",
		EventStepSucceeded + ":a",
		EventStepCompensated + ":a",
		EventExecutionFailed + ":b",
	}
	if fmt.Sprint(types) != fmt.Sprint(want) {
		t.Fatalf("events = %v, want %v", types, want)
	}
}

func TestGraphModeSameIdentityRetryAfterCancel(t *testing.T) {
	// a 快速确认，b 阻塞至 ctx 取消：取消后 a 已提交、b 未确认；
	// 同身份再次调用只重入 b（及其下游 c），a 不得重复调用。
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &dagRecorder{}
	gateB := newGate()
	var aCalls, bCalls int32
	var mu sync.Mutex
	count := func(name string) {
		mu.Lock()
		defer mu.Unlock()
		if name == "a" {
			aCalls++
		} else {
			bCalls++
		}
	}

	def := Definition{
		Name: "dag-saga", Version: "v1",
		Steps: []Step{
			{Name: "a", Action: func(context.Context, ExecutionView) error {
				count("a")
				rec.log("done:a")
				return nil
			}},
			{Name: "b", Action: func(ctx context.Context, _ ExecutionView) error {
				count("b")
				return gateB.wait(ctx)
			}},
			{Name: "c", DependsOn: []string{"b"}, Action: func(context.Context, ExecutionView) error {
				rec.log("done:c")
				return nil
			}},
		},
	}
	req := ExecutionRequest{BusinessKey: "biz-dag-5", IdempotencyKey: "req-dag-5"}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := eng.Execute(ctx, def, req)
		done <- err
	}()
	// 等 a 确认提交后取消 ctx：b 的动作随 ctx 结束，不被确认。
	deadline := time.After(2 * time.Second)
	for indexOf(rec.snapshot(), "done:a") < 0 {
		select {
		case <-deadline:
			t.Fatal("a never done")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	time.Sleep(20 * time.Millisecond) // 让 a 的成功提交落库
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute = %v, want context.Canceled", err)
	}

	snap, err := store.LoadSnapshot(context.Background(), "biz-dag-5", "req-dag-5")
	if err != nil {
		t.Fatal(err)
	}
	if snap.State.Steps[0].Status != StepResultSucceeded || snap.State.Steps[1].Status != "" {
		t.Fatalf("state after cancel: %+v", snap.State.Steps)
	}

	// 同身份、新 context 继续：只重入未确认的 b 与下游 c。
	gateB.open()
	res, err := eng.Execute(context.Background(), def, req)
	if err != nil {
		t.Fatalf("retry Execute: %v", err)
	}
	if res.Status != StatusCompleted {
		t.Fatalf("status=%s want completed", res.Status)
	}
	mu.Lock()
	defer mu.Unlock()
	if aCalls != 1 {
		t.Fatalf("confirmed step re-invoked: a calls=%d", aCalls)
	}
	if bCalls != 2 {
		t.Fatalf("unconfirmed step not re-entered exactly once more: b calls=%d", bCalls)
	}
}

func TestGraphModeDefinitionConflictOnDependencyChange(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	action := func(context.Context, ExecutionView) error { return nil }
	def := Definition{
		Name: "dag-saga", Version: "v1",
		Steps: []Step{
			{Name: "a", Action: action},
			{Name: "b", Action: action, DependsOn: []string{"a"}},
		},
	}
	req := ExecutionRequest{BusinessKey: "biz-dag-6", IdempotencyKey: "req-dag-6"}
	if _, err := eng.Execute(context.Background(), def, req); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	// 步骤名相同但前置关系变化：定义指纹不同，同业务键冲突。
	changed := Definition{
		Name: "dag-saga", Version: "v1",
		Steps: []Step{
			{Name: "a", Action: action},
			{Name: "b", Action: action},
		},
	}
	if _, err := eng.Execute(context.Background(), changed, req); !errors.Is(err, ErrDefinitionConflict) {
		t.Fatalf("Execute = %v, want ErrDefinitionConflict", err)
	}
}

func TestGraphModeSucceededStepsSnapshotInEvents(t *testing.T) {
	// 确认顺序 slow 后于 fast：事件负载中的 SucceededSteps 快照应按确认顺序。
	store := NewMemoryStore()
	eng := NewEngine(store)
	gateSlow := newGate()

	def := Definition{
		Name: "dag-saga", Version: "v1",
		Steps: []Step{
			{Name: "slow", Action: func(ctx context.Context, _ ExecutionView) error {
				return gateSlow.wait(ctx)
			}},
			{Name: "fast", Action: func(context.Context, ExecutionView) error { return nil }},
			{Name: "join", DependsOn: []string{"slow", "fast"}, Action: func(context.Context, ExecutionView) error { return nil }},
		},
	}
	done := make(chan struct{})
	go func() {
		_, _ = eng.Execute(context.Background(), def, ExecutionRequest{
			BusinessKey: "biz-dag-7", IdempotencyKey: "req-dag-7",
		})
		close(done)
	}()
	deadline := time.After(2 * time.Second)
	for {
		snap, _ := store.LoadSnapshot(context.Background(), "biz-dag-7", "req-dag-7")
		if snap.State != nil && snap.State.Steps[1].Status == StepResultSucceeded {
			break
		}
		select {
		case <-deadline:
			t.Fatal("fast never confirmed")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	gateSlow.open()
	<-done

	events, _ := store.ClaimPendingEvents(context.Background(), 100)
	succ := map[string][]string{}
	var completedSnap []string
	for _, ev := range events {
		p, _ := ev.Payload.(EventPayload)
		switch ev.Type {
		case EventStepSucceeded:
			succ[p.Step] = p.SucceededSteps
		case EventExecutionCompleted:
			completedSnap = p.SucceededSteps
		}
	}
	if fmt.Sprint(succ["fast"]) != "[fast]" {
		t.Fatalf("fast snapshot = %v", succ["fast"])
	}
	if fmt.Sprint(succ["slow"]) != "[fast slow]" {
		t.Fatalf("slow snapshot = %v, want confirmation order [fast slow]", succ["slow"])
	}
	if fmt.Sprint(completedSnap) != "[fast slow join]" {
		t.Fatalf("completed snapshot = %v", completedSnap)
	}
}

func TestGraphModeCompensationFailureIsTerminal(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &dagRecorder{}

	def := Definition{
		Name: "dag-saga", Version: "v1",
		Steps: []Step{
			{Name: "a", Action: func(context.Context, ExecutionView) error { return nil },
				Compensate: func(context.Context, ExecutionView) error {
					rec.log("undo:a")
					return errors.New("comp-boom-a")
				}},
			{Name: "b", DependsOn: []string{"a"}, Action: func(context.Context, ExecutionView) error {
				return errors.New("boom-b")
			}},
		},
	}
	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "biz-dag-8", IdempotencyKey: "req-dag-8",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Status != StatusCompensationFailed || !res.Terminal {
		t.Fatalf("status=%s want compensation_failed", res.Status)
	}
	if res.FailedCompensationStep != "a" || res.CompensationError != "comp-boom-a" {
		t.Fatalf("compensation failure info: %+v", res)
	}
	// 终态稳定：同身份再调用不再触发任何动作。
	again, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "biz-dag-8", IdempotencyKey: "req-dag-8",
	})
	if err != nil || again.Status != StatusCompensationFailed {
		t.Fatalf("again=%+v err=%v", again, err)
	}
	if got := rec.snapshot(); fmt.Sprint(got) != "[undo:a]" {
		t.Fatalf("calls after terminal = %v", got)
	}
}

func TestGraphModeRetryPolicyAndEmptyCompensate(t *testing.T) {
	// 依赖图模式下单步重试预算仍生效：中间失败不追加事件；
	// 未配置补偿的步骤视为补偿成功。
	store := NewMemoryStore()
	eng := NewEngine(store)
	var mu sync.Mutex
	bFails := 2

	def := Definition{
		Name: "dag-saga", Version: "v1",
		Steps: []Step{
			{Name: "a", Action: func(context.Context, ExecutionView) error { return nil }},
			// b 前两次失败、第三次成功；无补偿配置。
			{Name: "b", DependsOn: []string{"a"},
				Action: func(context.Context, ExecutionView) error {
					mu.Lock()
					defer mu.Unlock()
					if bFails > 0 {
						bFails--
						return fmt.Errorf("flaky-%d", bFails)
					}
					return nil
				},
				ActionRetry: RetryPolicy{MaxAttempts: 3}},
			// c 失败触发补偿：b 无补偿配置视为成功，a 有补偿。
			{Name: "c", DependsOn: []string{"b"}, Action: func(context.Context, ExecutionView) error {
				return errors.New("boom-c")
			}},
		},
	}
	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "biz-dag-9", IdempotencyKey: "req-dag-9",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Status != StatusFailed {
		t.Fatalf("status=%s want failed", res.Status)
	}
	results := map[string]StepOutcome{}
	for _, so := range res.Steps {
		results[so.Name] = so
	}
	if results["b"].Attempts != 3 || results["b"].Result != StepResultCompensated {
		t.Fatalf("b = %+v, want 3 attempts and compensated", results["b"])
	}
	if results["a"].Result != StepResultCompensated || results["c"].Result != StepResultFailed {
		t.Fatalf("results = %v", results)
	}
	// 中间失败不追加事件：b 只有一次成功事件。
	events, _ := store.ClaimPendingEvents(context.Background(), 100)
	var types []string
	for _, ev := range events {
		p, _ := ev.Payload.(EventPayload)
		types = append(types, ev.Type+":"+p.Step)
	}
	want := []string{
		EventExecutionStarted + ":",
		EventStepSucceeded + ":a",
		EventStepSucceeded + ":b",
		EventStepFailed + ":c",
		EventStepCompensated + ":b",
		EventStepCompensated + ":a",
		EventExecutionFailed + ":c",
	}
	if fmt.Sprint(types) != fmt.Sprint(want) {
		t.Fatalf("events = %v, want %v", types, want)
	}
}

func TestGraphModeActionViewSeesConfirmedDeps(t *testing.T) {
	// 依赖步骤启动时，其 ExecutionView 应包含已确认的前置步骤。
	store := NewMemoryStore()
	eng := NewEngine(store)
	var mu sync.Mutex
	var viewAtC []string

	def := Definition{
		Name: "dag-saga", Version: "v1",
		Steps: []Step{
			{Name: "a", Action: func(context.Context, ExecutionView) error { return nil }},
			{Name: "b", Action: func(context.Context, ExecutionView) error { return nil }},
			{Name: "c", DependsOn: []string{"a", "b"}, Action: func(_ context.Context, v ExecutionView) error {
				mu.Lock()
				viewAtC = v.SucceededSteps()
				mu.Unlock()
				return nil
			}},
		},
	}
	if _, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "biz-dag-10", IdempotencyKey: "req-dag-10",
	}); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(viewAtC) != 2 ||
		!(strings.Contains(fmt.Sprint(viewAtC), "a") && strings.Contains(fmt.Sprint(viewAtC), "b")) {
		t.Fatalf("view at c = %v, want confirmed deps a,b", viewAtC)
	}
}
