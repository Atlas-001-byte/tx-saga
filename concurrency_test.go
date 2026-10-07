package txsaga

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// waitForEntry 轮询直到 recorder 中出现指定日志条目，超时失败。
func waitForEntry(t *testing.T, rec *dagRecorder, entry string) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for indexOf(rec.snapshot(), entry) < 0 {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %q: %v", entry, rec.snapshot())
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

// inflightTracker 统计动作体的并发在飞数与峰值。
type inflightTracker struct {
	cur int32
	max int32
}

func (tr *inflightTracker) enter() {
	n := atomic.AddInt32(&tr.cur, 1)
	for {
		m := atomic.LoadInt32(&tr.max)
		if n <= m || atomic.CompareAndSwapInt32(&tr.max, m, n) {
			return
		}
	}
}

func (tr *inflightTracker) exit() { atomic.AddInt32(&tr.cur, -1) }

func (tr *inflightTracker) peak() int32 { return atomic.LoadInt32(&tr.max) }

func TestGraphModeMaxConcurrencyLimitsInflightAndReleasesSlot(t *testing.T) {
	// 额度 2：a、b 先占满额度，c 保持 pending；a 确认释放额度后 c 补位，
	// c 确认后其下游 d 在 b 仍在阻塞时就绪并启动。
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &dagRecorder{}
	gateA, gateB, gateC := newGate(), newGate(), newGate()
	gateD := newGate()
	gateD.open()
	var tracker inflightTracker

	gated := func(name string, g *gate, deps ...string) Step {
		return Step{
			Name:      name,
			DependsOn: deps,
			Action: func(ctx context.Context, _ ExecutionView) error {
				tracker.enter()
				defer tracker.exit()
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
		Name: "dag-capped", Version: "v1", MaxConcurrency: 2,
		Steps: []Step{
			gated("a", gateA),
			gated("b", gateB),
			gated("c", gateC),
			gated("d", gateD, "c"),
		},
	}

	type execOut struct {
		res Result
		err error
	}
	done := make(chan execOut, 1)
	go func() {
		res, err := eng.Execute(context.Background(), def, ExecutionRequest{
			BusinessKey: "biz-cap-1", IdempotencyKey: "req-cap-1",
		})
		done <- execOut{res, err}
	}()

	// 额度被 a、b 占满：c 虽已就绪但不得启动。
	waitForEntry(t, rec, "start:a")
	waitForEntry(t, rec, "start:b")
	time.Sleep(20 * time.Millisecond)
	if indexOf(rec.snapshot(), "start:c") >= 0 {
		t.Fatalf("c started while slots full: %v", rec.snapshot())
	}

	// a 确认释放额度：c 作为最早就绪步骤补位；d 前置未满足仍不得启动。
	gateA.open()
	waitForEntry(t, rec, "start:c")
	if indexOf(rec.snapshot(), "start:d") >= 0 {
		t.Fatalf("d started before c confirmed: %v", rec.snapshot())
	}

	// c 确认后 d 就绪并补位，此时 b 仍在阻塞（额度释放后立即继续选择）。
	gateC.open()
	waitForEntry(t, rec, "start:d")
	gateB.open()

	out := <-done
	if out.err != nil {
		t.Fatalf("Execute: %v", out.err)
	}
	if out.res.Status != StatusCompleted || !out.res.Terminal {
		t.Fatalf("status=%s want completed", out.res.Status)
	}
	if got := tracker.peak(); got != 2 {
		t.Fatalf("peak inflight = %d, want exactly the limit 2", got)
	}
	got := rec.snapshot()
	if indexOf(got, "done:a") > indexOf(got, "start:c") {
		t.Fatalf("c started before a released its slot: %v", got)
	}
	if indexOf(got, "done:c") > indexOf(got, "start:d") {
		t.Fatalf("d started before c confirmed: %v", got)
	}
	if indexOf(got, "start:c") > indexOf(got, "start:d") {
		t.Fatalf("start order broken: %v", got)
	}
	for _, so := range out.res.Steps {
		if so.Result != StepResultSucceeded || so.Attempts != 1 {
			t.Fatalf("step %s = %+v", so.Name, so)
		}
	}
}

func TestGraphModeMaxConcurrencyOneSelectsInDeclarationOrder(t *testing.T) {
	// 额度 1 时正向阶段完全串行：每次确认后按声明顺序选择最早就绪步骤，
	// 启动顺序确定，与动作完成快慢无关。
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &dagRecorder{}

	mk := func(name string, deps ...string) Step {
		return Step{
			Name:      name,
			DependsOn: deps,
			Action: func(context.Context, ExecutionView) error {
				rec.log("start:" + name)
				rec.log("done:" + name)
				return nil
			},
		}
	}
	def := Definition{
		Name: "dag-capped-1", Version: "v1", MaxConcurrency: 1,
		Steps: []Step{
			mk("a"),
			mk("b"),
			mk("c"),
			mk("d", "a"), // a 确认后即就绪，但声明顺序在 b、c 之后
		},
	}
	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "biz-cap-2", IdempotencyKey: "req-cap-2",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Status != StatusCompleted {
		t.Fatalf("status=%s want completed", res.Status)
	}
	want := []string{
		"start:a", "done:a",
		"start:b", "done:b",
		"start:c", "done:c",
		"start:d", "done:d",
	}
	if got := rec.snapshot(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("start order = %v, want declaration order %v", got, want)
	}
}

func TestGraphModeMaxConcurrencyRetryWaitHoldsSlot(t *testing.T) {
	// 额度 1：a 首次调用失败进入重试等待，等待期间额度不释放，
	// 就绪的 b 不得启动；a 第二次调用成功提交后 b、c 依次补位。
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &dagRecorder{}
	var aCalls int32

	def := Definition{
		Name: "dag-capped-retry", Version: "v1", MaxConcurrency: 1,
		Steps: []Step{
			{Name: "a", Action: func(context.Context, ExecutionView) error {
				n := atomic.AddInt32(&aCalls, 1)
				rec.log(fmt.Sprintf("call:a:%d", n))
				if n == 1 {
					return errors.New("flaky-a")
				}
				return nil
			}, ActionRetry: RetryPolicy{MaxAttempts: 2, RetryWait: 300 * time.Millisecond}},
			{Name: "b", Action: func(context.Context, ExecutionView) error {
				rec.log("start:b")
				return nil
			}},
			{Name: "c", DependsOn: []string{"b"}, Action: func(context.Context, ExecutionView) error {
				rec.log("start:c")
				return nil
			}},
		},
	}

	type execOut struct {
		res Result
		err error
	}
	done := make(chan execOut, 1)
	go func() {
		res, err := eng.Execute(context.Background(), def, ExecutionRequest{
			BusinessKey: "biz-cap-3", IdempotencyKey: "req-cap-3",
		})
		done <- execOut{res, err}
	}()

	// a 第一次失败进入 300ms 重试等待：等待期间唯一的额度仍被 a 占用。
	waitForEntry(t, rec, "call:a:1")
	time.Sleep(100 * time.Millisecond)
	if indexOf(rec.snapshot(), "start:b") >= 0 {
		t.Fatalf("b started while a's retry wait held the only slot: %v", rec.snapshot())
	}

	out := <-done
	if out.err != nil {
		t.Fatalf("Execute: %v", out.err)
	}
	if out.res.Status != StatusCompleted {
		t.Fatalf("status=%s want completed", out.res.Status)
	}
	got := rec.snapshot()
	if indexOf(got, "call:a:2") < 0 || indexOf(got, "call:a:2") > indexOf(got, "start:b") ||
		indexOf(got, "start:b") > indexOf(got, "start:c") {
		t.Fatalf("slot not released in confirmation order: %v", got)
	}
	if out.res.Steps[0].Attempts != 2 {
		t.Fatalf("a attempts = %d, want 2", out.res.Steps[0].Attempts)
	}
}

func TestGraphModeMaxConcurrencyFailureStopsLaunching(t *testing.T) {
	// 额度 2：a 阻塞中、b 立即失败；c 虽就绪且有额度空位也不得启动，
	// 已启动的 a 放行后照常提交成功并纳入补偿。
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &dagRecorder{}
	gateA := newGate()

	def := Definition{
		Name: "dag-capped-fail", Version: "v1", MaxConcurrency: 2,
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
			{Name: "c", Action: func(context.Context, ExecutionView) error {
				rec.log("start:c")
				return nil
			}},
			{Name: "d", DependsOn: []string{"a"}, Action: func(context.Context, ExecutionView) error {
				rec.log("start:d")
				return nil
			}},
		},
	}

	done := make(chan Result, 1)
	go func() {
		res, _ := eng.Execute(context.Background(), def, ExecutionRequest{
			BusinessKey: "biz-cap-4", IdempotencyKey: "req-cap-4",
		})
		done <- res
	}()

	// b 失败提交、a 仍在阻塞：额度已有空位，但 c 不得被调度。
	deadline := time.After(2 * time.Second)
	for {
		snap, _ := store.LoadSnapshot(context.Background(), "biz-cap-4", "req-cap-4")
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
	if indexOf(rec.snapshot(), "start:c") >= 0 {
		t.Fatalf("c scheduled after sibling failure: %v", rec.snapshot())
	}
	gateA.open()
	res := <-done

	if res.Status != StatusFailed || !res.Terminal {
		t.Fatalf("status=%s want failed", res.Status)
	}
	if res.FailedStep != "b" {
		t.Fatalf("failed step = %q want b", res.FailedStep)
	}
	got := rec.snapshot()
	if indexOf(got, "done:a") < 0 || indexOf(got, "undo:a") < 0 {
		t.Fatalf("inflight sibling not committed/compensated: %v", got)
	}
	if indexOf(got, "start:c") >= 0 || indexOf(got, "start:d") >= 0 {
		t.Fatalf("unstarted steps ran after failure: %v", got)
	}
	results := map[string]string{}
	for _, so := range res.Steps {
		results[so.Name] = so.Result
	}
	if results["a"] != StepResultCompensated || results["b"] != StepResultFailed ||
		results["c"] != StepResultPending || results["d"] != StepResultPending {
		t.Fatalf("step results = %v", results)
	}
}

func TestGraphModeMaxConcurrencyCancelAndReenter(t *testing.T) {
	// 额度 1：a 阻塞至 ctx 取消，b 不得趁机启动；同身份重入后 a 被重新
	// 调用（动作须幂等），随后 b、c 按声明顺序依次完成。
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &dagRecorder{}
	gateA := newGate()
	var aCalls, bCalls, cCalls int32

	def := Definition{
		Name: "dag-capped-cancel", Version: "v1", MaxConcurrency: 1,
		Steps: []Step{
			{Name: "a", Action: func(ctx context.Context, _ ExecutionView) error {
				atomic.AddInt32(&aCalls, 1)
				rec.log("start:a")
				return gateA.wait(ctx)
			}},
			{Name: "b", Action: func(context.Context, ExecutionView) error {
				atomic.AddInt32(&bCalls, 1)
				rec.log("start:b")
				return nil
			}},
			{Name: "c", DependsOn: []string{"a"}, Action: func(context.Context, ExecutionView) error {
				atomic.AddInt32(&cCalls, 1)
				rec.log("start:c")
				return nil
			}},
		},
	}
	req := ExecutionRequest{BusinessKey: "biz-cap-5", IdempotencyKey: "req-cap-5"}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := eng.Execute(ctx, def, req)
		done <- err
	}()
	waitForEntry(t, rec, "start:a")
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute = %v, want context.Canceled", err)
	}
	if indexOf(rec.snapshot(), "start:b") >= 0 {
		t.Fatalf("b started after cancel with slot held by a: %v", rec.snapshot())
	}
	snap, err := store.LoadSnapshot(context.Background(), "biz-cap-5", "req-cap-5")
	if err != nil {
		t.Fatal(err)
	}
	if snap.State.Status != StatusRunning || snap.State.Steps[0].Status != "" {
		t.Fatalf("state after cancel: %+v", snap.State)
	}

	// 同身份、新 context 继续：未确认的 a 重入，随后 b、c 依次推进。
	gateA.open()
	res, err := eng.Execute(context.Background(), def, req)
	if err != nil {
		t.Fatalf("retry Execute: %v", err)
	}
	if res.Status != StatusCompleted {
		t.Fatalf("status=%s want completed", res.Status)
	}
	if atomic.LoadInt32(&aCalls) != 2 || atomic.LoadInt32(&bCalls) != 1 || atomic.LoadInt32(&cCalls) != 1 {
		t.Fatalf("calls a=%d b=%d c=%d, want 2/1/1", aCalls, bCalls, cCalls)
	}
}

func TestMaxConcurrencyNegativeIsInvalid(t *testing.T) {
	// 负并发上限：ValidateDefinition 与 Execute 都以 ErrInvalidDefinition
	// 失败，不调用动作、不创建执行、不追加事件。
	called := false
	def := Definition{
		Name: "bad-cap", Version: "v1", MaxConcurrency: -1,
		Steps: []Step{
			{Name: "a", Action: func(context.Context, ExecutionView) error {
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
		BusinessKey: "biz-bad-cap", IdempotencyKey: "req-bad-cap",
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
	if _, err := eng.GetResult(context.Background(), "bad-cap", "biz-bad-cap", "req-bad-cap"); !errors.Is(err, ErrExecutionNotFound) {
		t.Fatalf("GetResult = %v, want ErrExecutionNotFound", err)
	}
}

func TestGraphModeMaxConcurrencyExceedsStepCount(t *testing.T) {
	// 上限大于步骤数：按实际步骤数执行，行为与不设上限一致——
	// 全部无前置步骤在任一放行前同时启动。
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &dagRecorder{}
	gateA, gateB := newGate(), newGate()

	gated := func(name string, g *gate, deps ...string) Step {
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
	gateC := newGate()
	gateC.open()
	def := Definition{
		Name: "dag-capped-wide", Version: "v1", MaxConcurrency: 10,
		Steps: []Step{
			gated("a", gateA),
			gated("b", gateB),
			gated("c", gateC, "a", "b"),
		},
	}

	done := make(chan error, 1)
	go func() {
		_, err := eng.Execute(context.Background(), def, ExecutionRequest{
			BusinessKey: "biz-cap-6", IdempotencyKey: "req-cap-6",
		})
		done <- err
	}()
	// 两个根步骤都应在任一放行前启动（上限未构成约束）。
	waitForEntry(t, rec, "start:a")
	waitForEntry(t, rec, "start:b")
	if indexOf(rec.snapshot(), "start:c") >= 0 {
		t.Fatalf("c started before deps confirmed: %v", rec.snapshot())
	}
	gateA.open()
	gateB.open()
	if err := <-done; err != nil {
		t.Fatalf("Execute: %v", err)
	}
	res, err := eng.GetResult(context.Background(), "dag-capped-wide", "biz-cap-6", "req-cap-6")
	if err != nil || res.Status != StatusCompleted {
		t.Fatalf("res=%+v err=%v", res, err)
	}
}

func TestSequentialModeUnaffectedByMaxConcurrency(t *testing.T) {
	// 顺序模式（无任何 DependsOn）不并发：即使配置了正数上限，
	// 仍一次只执行一个步骤，按声明顺序逐个推进。
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &dagRecorder{}
	var tracker inflightTracker

	mk := func(name string) Step {
		return Step{
			Name: name,
			Action: func(context.Context, ExecutionView) error {
				tracker.enter()
				defer tracker.exit()
				rec.log("start:" + name)
				time.Sleep(5 * time.Millisecond)
				rec.log("done:" + name)
				return nil
			},
		}
	}
	def := Definition{
		Name: "seq-capped", Version: "v1", MaxConcurrency: 3,
		Steps: []Step{mk("a"), mk("b"), mk("c")},
	}
	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "biz-cap-7", IdempotencyKey: "req-cap-7",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Status != StatusCompleted {
		t.Fatalf("status=%s want completed", res.Status)
	}
	if got := tracker.peak(); got != 1 {
		t.Fatalf("sequential mode ran %d actions concurrently", got)
	}
	want := []string{"start:a", "done:a", "start:b", "done:b", "start:c", "done:c"}
	if got := rec.snapshot(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
}

// 确保并发上限下事件历史与 Outbox 行为不变：全部事件按提交顺序追加。
func TestGraphModeMaxConcurrencyEventHistory(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)

	mk := func(name string, deps ...string) Step {
		return Step{
			Name:      name,
			DependsOn: deps,
			Action:    func(context.Context, ExecutionView) error { return nil },
		}
	}
	def := Definition{
		Name: "dag-capped-events", Version: "v1", MaxConcurrency: 1,
		Steps: []Step{
			mk("a"),
			mk("b"),
			mk("c", "a", "b"),
		},
	}
	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "biz-cap-8", IdempotencyKey: "req-cap-8",
	})
	if err != nil || res.Status != StatusCompleted {
		t.Fatalf("res=%+v err=%v", res, err)
	}
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
		EventStepSucceeded + ":c",
		EventExecutionCompleted + ":",
	}
	if fmt.Sprint(types) != fmt.Sprint(want) {
		t.Fatalf("events = %v, want %v", types, want)
	}
}
