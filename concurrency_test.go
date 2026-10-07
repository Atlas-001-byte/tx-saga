package txsaga

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// waitFor 轮询 cond 直到成立或超时，超时即失败。用于并发时序断言。
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for !cond() {
		select {
		case <-deadline:
			t.Fatalf("timeout waiting for %s", what)
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

// TestGraphMaxConcurrencyDeclarationOrderAndQuotaRelease 验证：额度占满时其余
// 就绪步骤保持 pending；任一步骤确认提交释放额度后，按声明顺序选择最早出现
// 的就绪步骤启动；依赖步骤在其前置全部确认前不得启动。
func TestGraphMaxConcurrencyDeclarationOrderAndQuotaRelease(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &dagRecorder{}
	gateA, gateB, gateC := newGate(), newGate(), newGate()
	gateD := newGate()
	gateD.open() // d 不阻塞，前置全部确认后即完成

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
		Name: "dag-limit", Version: "v1", MaxConcurrency: 2,
		Steps: []Step{
			step("a", gateA),
			step("b", gateB),
			step("c", gateC),
			step("d", gateD, "a", "b", "c"),
		},
	}

	done := make(chan Result, 1)
	go func() {
		res, _ := eng.Execute(context.Background(), def, ExecutionRequest{
			BusinessKey: "biz-lim-1", IdempotencyKey: "req-lim-1",
		})
		done <- res
	}()

	// 额度为 2：声明最早的 a、b 启动，c 虽已就绪但须保持 pending。
	waitFor(t, "a and b started", func() bool {
		got := rec.snapshot()
		return indexOf(got, "start:a") >= 0 && indexOf(got, "start:b") >= 0
	})
	time.Sleep(20 * time.Millisecond)
	if indexOf(rec.snapshot(), "start:c") >= 0 {
		t.Fatalf("c started while quota full: %v", rec.snapshot())
	}

	// a 确认释放额度后 c 立即启动；b 仍在阻塞，d 的前置未满足不得启动。
	gateA.open()
	waitFor(t, "c started after a confirmed", func() bool {
		return indexOf(rec.snapshot(), "start:c") >= 0
	})
	if indexOf(rec.snapshot(), "start:d") >= 0 {
		t.Fatalf("d started before deps confirmed: %v", rec.snapshot())
	}

	gateB.open()
	gateC.open()
	res := <-done
	if res.Status != StatusCompleted || !res.Terminal {
		t.Fatalf("status=%s want completed", res.Status)
	}
	got := rec.snapshot()
	// c 的启动必须晚于 a 的确认（额度由 a 释放），d 的启动晚于全部前置确认。
	if indexOf(got, "done:a") > indexOf(got, "start:c") {
		t.Fatalf("c started before a confirmed: %v", got)
	}
	for _, dep := range []string{"done:a", "done:b", "done:c"} {
		if indexOf(got, dep) > indexOf(got, "start:d") {
			t.Fatalf("d started before %s: %v", dep, got)
		}
	}
	for _, so := range res.Steps {
		if so.Result != StepResultSucceeded || so.Attempts != 1 {
			t.Fatalf("step %s = %+v", so.Name, so)
		}
	}
}

// TestGraphMaxConcurrencyReadySuccessorPickedInDeclarationOrder 验证：额度为 1
// 时调度严格按声明顺序推进；前置确认使多个步骤同时就绪时，选择声明顺序最早者。
func TestGraphMaxConcurrencyReadySuccessorPickedInDeclarationOrder(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &dagRecorder{}

	mk := func(name string, deps ...string) Step {
		return Step{
			Name:      name,
			DependsOn: deps,
			Action: func(context.Context, ExecutionView) error {
				rec.log("done:" + name)
				return nil
			},
		}
	}
	def := Definition{
		Name: "dag-limit", Version: "v1", MaxConcurrency: 1,
		Steps: []Step{
			mk("a"),
			mk("b"),
			mk("c", "a"), // a 确认后 b、c 同时就绪，须先选声明在前的 b
		},
	}
	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "biz-lim-2", IdempotencyKey: "req-lim-2",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Status != StatusCompleted {
		t.Fatalf("status=%s want completed", res.Status)
	}
	// 额度为 1 时全程串行，启动顺序确定：a -> b -> c（而非 a -> c -> b）。
	if got := rec.snapshot(); fmt.Sprint(got) != "[done:a done:b done:c]" {
		t.Fatalf("start order = %v, want declaration order [done:a done:b done:c]", got)
	}
}

// TestGraphMaxConcurrencyFailureStopsLaunching 验证：占用额度的步骤预算耗尽
// 失败后，未启动的就绪步骤不再启动；已确认步骤按确认逆序补偿。
func TestGraphMaxConcurrencyFailureStopsLaunching(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &dagRecorder{}

	def := Definition{
		Name: "dag-limit", Version: "v1", MaxConcurrency: 1,
		Steps: []Step{
			{Name: "a", Action: func(context.Context, ExecutionView) error {
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
			// b 失败后 c 虽已就绪（前置 a 已确认）也不得再启动。
			{Name: "c", DependsOn: []string{"a"}, Action: func(context.Context, ExecutionView) error {
				rec.log("done:c")
				return nil
			}},
		},
	}
	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "biz-lim-3", IdempotencyKey: "req-lim-3",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Status != StatusFailed || !res.Terminal {
		t.Fatalf("status=%s want failed", res.Status)
	}
	if res.FailedStep != "b" || res.FailureReason != "boom-b" {
		t.Fatalf("failure info: %+v", res)
	}
	got := rec.snapshot()
	if indexOf(got, "done:c") >= 0 {
		t.Fatalf("c launched after sibling failure: %v", got)
	}
	if indexOf(got, "undo:a") < 0 {
		t.Fatalf("confirmed step not compensated: %v", got)
	}
	results := map[string]string{}
	for _, so := range res.Steps {
		results[so.Name] = so.Result
	}
	if results["a"] != StepResultCompensated || results["b"] != StepResultFailed ||
		results["c"] != StepResultPending {
		t.Fatalf("step results = %v", results)
	}
}

// TestGraphMaxConcurrencyRetryWaitHoldsQuota 验证：ActionRetry 两次调用之间的
// 等待时间同样占用额度——步骤未形成确认结果前不释放额度，其它就绪步骤不得启动。
func TestGraphMaxConcurrencyRetryWaitHoldsQuota(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &dagRecorder{}
	var mu sync.Mutex
	aCalls := 0

	def := Definition{
		Name: "dag-limit", Version: "v1", MaxConcurrency: 1,
		Steps: []Step{
			{Name: "a",
				Action: func(context.Context, ExecutionView) error {
					mu.Lock()
					aCalls++
					n := aCalls
					mu.Unlock()
					rec.log(fmt.Sprintf("attempt:a:%d", n))
					if n == 1 {
						return errors.New("flaky-a")
					}
					return nil
				},
				ActionRetry: RetryPolicy{MaxAttempts: 2, RetryWait: 150 * time.Millisecond}},
			{Name: "b", Action: func(context.Context, ExecutionView) error {
				rec.log("start:b")
				return nil
			}},
		},
	}
	start := time.Now()
	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "biz-lim-4", IdempotencyKey: "req-lim-4",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Status != StatusCompleted {
		t.Fatalf("status=%s want completed", res.Status)
	}
	// 重试等待期间额度未释放：b 的启动晚于 a 的第二次（成功）调用，
	// 且整体耗时包含等待时长。
	got := rec.snapshot()
	if indexOf(got, "attempt:a:2") < 0 || indexOf(got, "attempt:a:2") > indexOf(got, "start:b") {
		t.Fatalf("b started before a confirmed (quota released early): %v", got)
	}
	if elapsed := time.Since(start); elapsed < 150*time.Millisecond {
		t.Fatalf("retry wait not observed: elapsed=%v", elapsed)
	}
	if res.Steps[0].Attempts != 2 {
		t.Fatalf("a attempts = %d, want 2", res.Steps[0].Attempts)
	}
}

// TestGraphMaxConcurrencyCancelAndResume 验证：ctx 取消时不确认占用额度的
// 步骤、不启动新步骤；同身份重入沿用已确认步骤结果，只重入未确认步骤。
func TestGraphMaxConcurrencyCancelAndResume(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	gateB := newGate()
	var mu sync.Mutex
	calls := map[string]int{}
	count := func(name string) {
		mu.Lock()
		calls[name]++
		mu.Unlock()
	}

	def := Definition{
		Name: "dag-limit", Version: "v1", MaxConcurrency: 1,
		Steps: []Step{
			{Name: "a", Action: func(context.Context, ExecutionView) error {
				count("a")
				return nil
			}},
			{Name: "b", Action: func(ctx context.Context, _ ExecutionView) error {
				count("b")
				return gateB.wait(ctx)
			}},
			{Name: "c", DependsOn: []string{"b"}, Action: func(context.Context, ExecutionView) error {
				count("c")
				return nil
			}},
		},
	}
	req := ExecutionRequest{BusinessKey: "biz-lim-5", IdempotencyKey: "req-lim-5"}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := eng.Execute(ctx, def, req)
		done <- err
	}()
	// 等 a 确认提交、b 占用额度阻塞后取消：b 不被确认，c 不得启动。
	waitFor(t, "a confirmed", func() bool {
		snap, _ := store.LoadSnapshot(context.Background(), "biz-lim-5", "req-lim-5")
		return snap.State != nil && snap.State.Steps[0].Status == StepResultSucceeded
	})
	waitFor(t, "b started", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls["b"] == 1
	})
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute = %v, want context.Canceled", err)
	}

	snap, err := store.LoadSnapshot(context.Background(), "biz-lim-5", "req-lim-5")
	if err != nil {
		t.Fatal(err)
	}
	if snap.State.Steps[0].Status != StepResultSucceeded ||
		snap.State.Steps[1].Status != "" || snap.State.Steps[2].Status != "" {
		t.Fatalf("state after cancel: %+v", snap.State.Steps)
	}

	// 同身份、新 context 继续：a 不重复调用，只重入 b 并推进 c。
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
	if calls["a"] != 1 || calls["b"] != 2 || calls["c"] != 1 {
		t.Fatalf("calls = %v, want a=1 b=2 c=1", calls)
	}
}

// TestGraphMaxConcurrencyNegativeInvalid 验证：MaxConcurrency 为负时
// ValidateDefinition 与 Execute 均返回可 errors.Is 判定的 ErrInvalidDefinition，
// 且不调用任何动作、不创建执行、不追加事件。
func TestGraphMaxConcurrencyNegativeInvalid(t *testing.T) {
	called := false
	def := Definition{
		Name: "dag-limit", Version: "v1", MaxConcurrency: -1,
		Steps: []Step{
			{Name: "a", Action: func(context.Context, ExecutionView) error {
				called = true
				return nil
			}},
			{Name: "b", DependsOn: []string{"a"}, Action: func(context.Context, ExecutionView) error {
				called = true
				return nil
			}},
		},
	}
	if err := ValidateDefinition(def); !errors.Is(err, ErrInvalidDefinition) {
		t.Fatalf("ValidateDefinition = %v, want ErrInvalidDefinition", err)
	}

	store := NewMemoryStore()
	eng := NewEngine(store)
	_, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "biz-lim-6", IdempotencyKey: "req-lim-6",
	})
	if !errors.Is(err, ErrInvalidDefinition) {
		t.Fatalf("Execute = %v, want ErrInvalidDefinition", err)
	}
	if called {
		t.Fatal("action called for invalid definition")
	}
	if store.TotalEvents() != 0 {
		t.Fatalf("events appended for invalid definition: %d", store.TotalEvents())
	}
	if _, err := eng.GetResult(context.Background(), "dag-limit", "biz-lim-6", "req-lim-6"); !errors.Is(err, ErrExecutionNotFound) {
		t.Fatalf("GetResult = %v, want ErrExecutionNotFound", err)
	}
}

// TestGraphMaxConcurrencyNeverExceedsLimit 验证：任意时刻并发调用的
// ActionFunc 数量不超过 MaxConcurrency，且全部步骤最终确认成功。
func TestGraphMaxConcurrencyNeverExceedsLimit(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	var inFlight, maxSeen atomic.Int32

	action := func(context.Context, ExecutionView) error {
		cur := inFlight.Add(1)
		for {
			old := maxSeen.Load()
			if cur <= old || maxSeen.CompareAndSwap(old, cur) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		inFlight.Add(-1)
		return nil
	}
	def := Definition{
		Name: "dag-limit", Version: "v1", MaxConcurrency: 2,
		Steps: []Step{
			{Name: "a", Action: action},
			{Name: "b", Action: action},
			{Name: "c", Action: action},
			{Name: "d", Action: action},
			{Name: "e", DependsOn: []string{"a"}, Action: action},
		},
	}
	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "biz-lim-7", IdempotencyKey: "req-lim-7",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Status != StatusCompleted {
		t.Fatalf("status=%s want completed", res.Status)
	}
	if got := maxSeen.Load(); got > 2 {
		t.Fatalf("concurrent actions = %d, exceeds MaxConcurrency=2", got)
	} else if got < 2 {
		t.Fatalf("concurrent actions = %d, want limit actually used (2)", got)
	}
	for _, so := range res.Steps {
		if so.Result != StepResultSucceeded {
			t.Fatalf("step %s = %+v", so.Name, so)
		}
	}
}

// TestGraphMaxConcurrencyGreaterThanStepCount 验证：上限大于步骤数时按实际
// 步骤数执行，语义与不限相同——全部就绪根步骤同时启动。
func TestGraphMaxConcurrencyGreaterThanStepCount(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &dagRecorder{}
	gateA, gateB := newGate(), newGate()
	gateC := newGate()
	gateC.open() // c 不阻塞，前置确认后即完成

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
		Name: "dag-limit", Version: "v1", MaxConcurrency: 10,
		Steps: []Step{
			step("a", gateA),
			step("b", gateB),
			step("c", gateC, "a", "b"),
		},
	}
	done := make(chan Result, 1)
	go func() {
		res, _ := eng.Execute(context.Background(), def, ExecutionRequest{
			BusinessKey: "biz-lim-8", IdempotencyKey: "req-lim-8",
		})
		done <- res
	}()
	// 上限超过步骤数：两个根步骤在任一放行前都已启动。
	waitFor(t, "both roots started", func() bool {
		got := rec.snapshot()
		return indexOf(got, "start:a") >= 0 && indexOf(got, "start:b") >= 0
	})
	gateA.open()
	gateB.open()
	res := <-done
	if res.Status != StatusCompleted {
		t.Fatalf("status=%s want completed", res.Status)
	}
}

// TestSequentialModeIgnoresMaxConcurrency 验证：顺序模式（所有步骤均未声明
// 前置）始终一次只执行一个步骤，不因 MaxConcurrency 配置改成并发。
func TestSequentialModeIgnoresMaxConcurrency(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &dagRecorder{}
	var inFlight, maxSeen atomic.Int32

	mk := func(name string) Step {
		return Step{
			Name: name,
			Action: func(context.Context, ExecutionView) error {
				cur := inFlight.Add(1)
				for {
					old := maxSeen.Load()
					if cur <= old || maxSeen.CompareAndSwap(old, cur) {
						break
					}
				}
				time.Sleep(10 * time.Millisecond)
				inFlight.Add(-1)
				rec.log("done:" + name)
				return nil
			},
		}
	}
	def := Definition{
		Name: "seq-limit", Version: "v1", MaxConcurrency: 3,
		Steps: []Step{mk("a"), mk("b"), mk("c")},
	}
	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "biz-lim-9", IdempotencyKey: "req-lim-9",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Status != StatusCompleted {
		t.Fatalf("status=%s want completed", res.Status)
	}
	if got := maxSeen.Load(); got != 1 {
		t.Fatalf("concurrent actions = %d, want 1 (sequential mode)", got)
	}
	if got := rec.snapshot(); fmt.Sprint(got) != "[done:a done:b done:c]" {
		t.Fatalf("order = %v, want declaration order", got)
	}
}

// TestGraphMaxConcurrencyZeroKeepsUnlimited 验证：MaxConcurrency 为零值时沿用
// 无上限语义——全部就绪根步骤同时启动。
func TestGraphMaxConcurrencyZeroKeepsUnlimited(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &dagRecorder{}
	gates := []*gate{newGate(), newGate(), newGate(), newGate()}
	gates[3].open() // d 不阻塞，前置确认后即完成

	mk := func(i int, deps ...string) Step {
		name := string(rune('a' + i))
		return Step{
			Name:      name,
			DependsOn: deps,
			Action: func(ctx context.Context, _ ExecutionView) error {
				rec.log("start:" + name)
				return gates[i].wait(ctx)
			},
		}
	}
	def := Definition{
		Name: "dag-limit", Version: "v1", // MaxConcurrency 零值：不限
		Steps: []Step{mk(0), mk(1), mk(2), mk(3, "a", "b", "c")},
	}
	done := make(chan struct{})
	go func() {
		_, _ = eng.Execute(context.Background(), def, ExecutionRequest{
			BusinessKey: "biz-lim-10", IdempotencyKey: "req-lim-10",
		})
		close(done)
	}()
	// 三个根步骤在任一放行前全部启动（无上限并发）。
	waitFor(t, "all roots started", func() bool {
		got := rec.snapshot()
		return indexOf(got, "start:a") >= 0 && indexOf(got, "start:b") >= 0 && indexOf(got, "start:c") >= 0
	})
	if indexOf(rec.snapshot(), "start:d") >= 0 {
		t.Fatalf("d started before deps confirmed: %v", rec.snapshot())
	}
	for _, g := range gates[:3] {
		g.open()
	}
	<-done
	res, err := eng.GetResult(context.Background(), "dag-limit", "biz-lim-10", "req-lim-10")
	if err != nil || res.Status != StatusCompleted {
		t.Fatalf("res=%+v err=%v", res, err)
	}
}
