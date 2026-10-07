package txsaga

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// errPermSentinel 是用于 errors.Is 链判定的固定错误。
var errPermSentinel = errors.New("perm-sentinel")

func TestPermanentWrapperPreservesChain(t *testing.T) {
	if Permanent(nil) != nil {
		t.Fatal("Permanent(nil) must return nil")
	}
	if IsPermanent(nil) {
		t.Fatal("IsPermanent(nil) must be false")
	}
	if IsPermanent(errors.New("plain")) {
		t.Fatal("plain error must not be permanent")
	}

	err := Permanent(errPermSentinel)
	if !IsPermanent(err) {
		t.Fatal("IsPermanent must detect Permanent wrapping")
	}
	if !errors.Is(err, errPermSentinel) {
		t.Fatal("errors.Is must reach the underlying error")
	}
	if err.Error() != errPermSentinel.Error() {
		t.Fatalf("Error()=%q want underlying text %q", err.Error(), errPermSentinel.Error())
	}

	// 再包一层 fmt.Errorf：错误链判定仍然成立，文本保留底层错误。
	wrapped := fmt.Errorf("outer: %w", err)
	if !IsPermanent(wrapped) {
		t.Fatal("IsPermanent must see Permanent through further wrapping")
	}
	if !errors.Is(wrapped, errPermSentinel) {
		t.Fatal("errors.Is must reach the sentinel through both wrappers")
	}
}

func TestForwardPermanentFailureStopsRetryWithBudgetLeft(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &recorder{}

	calls := 0
	def := Definition{
		Name: "perm-forward", Version: "v1",
		Steps: []Step{
			{
				Name:        "reserve",
				Action:      flakyAction(rec, "reserve", 0),
				Compensate:  func(_ context.Context, _ ExecutionView) error { rec.call("undo:reserve"); return nil },
				ActionRetry: RetryPolicy{MaxAttempts: 5, RetryWait: time.Hour},
			},
			{
				Name: "charge",
				Action: func(_ context.Context, _ ExecutionView) error {
					rec.call("do:charge")
					calls++
					return Permanent(errors.New("business-impossible"))
				},
				Compensate:  func(_ context.Context, _ ExecutionView) error { rec.call("undo:charge"); return nil },
				ActionRetry: RetryPolicy{MaxAttempts: 5, RetryWait: time.Hour},
			},
			{
				Name:        "ship",
				Action:      flakyAction(rec, "ship", 0),
				Compensate:  func(_ context.Context, _ ExecutionView) error { rec.call("undo:ship"); return nil },
				ActionRetry: RetryPolicy{MaxAttempts: 5, RetryWait: time.Hour},
			},
		},
	}

	start := time.Now()
	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "bk", IdempotencyKey: "ik",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	// 永久失败不得进入任何重试等待。
	if time.Since(start) > time.Second {
		t.Fatal("permanent failure must not wait for retry")
	}
	if res.Status != StatusFailed || !res.Terminal {
		t.Fatalf("status=%s terminal=%v want failed", res.Status, res.Terminal)
	}
	if res.FailedStep != "charge" || res.FailureReason != "business-impossible" {
		t.Fatalf("failure info: %+v", res)
	}
	if calls != 1 {
		t.Fatalf("charge calls=%d want 1 despite remaining budget", calls)
	}
	if res.Steps[1].Result != StepResultFailed || res.Steps[1].Attempts != 1 ||
		res.Steps[1].Error != "business-impossible" {
		t.Fatalf("charge outcome = %+v", res.Steps[1])
	}
	if res.Steps[2].Result != StepResultPending {
		t.Fatalf("ship must never start: %+v", res.Steps[2])
	}
	// 仅确认成功的步骤按确认顺序逆序补偿；charge 自身未确认，不补偿。
	if got := rec.snapshot(); fmt.Sprint(got) !=
		"[do:reserve do:charge undo:reserve]" {
		t.Fatalf("calls = %v", got)
	}

	// 事件序列与预算耗尽失败完全一致，无任何中间失败事件。
	events, _ := store.ClaimPendingEvents(context.Background(), 100)
	wantTypes := []string{
		EventExecutionStarted,
		EventStepSucceeded,   // reserve
		EventStepFailed,      // charge（一次提交，原因是底层错误文本；自身不补偿）
		EventStepCompensated, // reserve
		EventExecutionFailed,
	}
	if len(events) != len(wantTypes) {
		t.Fatalf("events=%d want=%d (%v)", len(events), len(wantTypes), eventTypes(events))
	}
	for i, ev := range events {
		if ev.Type != wantTypes[i] {
			t.Fatalf("event[%d]=%s want %s, all=%v", i, ev.Type, wantTypes[i], eventTypes(events))
		}
	}
	failPayload := events[2].Payload.(EventPayload)
	if failPayload.Reason != "business-impossible" {
		t.Fatalf("fail event reason=%q", failPayload.Reason)
	}

	// GetResult 返回同一固定终态。
	got, err := eng.GetResult(context.Background(), "perm-forward", "bk", "ik")
	if err != nil || got.Status != StatusFailed || got.FailureReason != "business-impossible" {
		t.Fatalf("GetResult: %+v err=%v", got, err)
	}
}

func TestTransientErrorsStillRetryUntilPermanent(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)

	// 前两次普通错误按预算重试，第三次永久失败立即结束：共调用 3 次，
	// 不耗尽 MaxAttempts=5，也不等待。
	calls := 0
	def := Definition{
		Name: "mixed-retry", Version: "v1",
		Steps: []Step{{
			Name: "s",
			Action: func(_ context.Context, _ ExecutionView) error {
				calls++
				if calls < 3 {
					return errors.New("transient")
				}
				return Permanent(errPermSentinel)
			},
			ActionRetry: RetryPolicy{MaxAttempts: 5},
		}},
	}
	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "bk", IdempotencyKey: "ik",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if calls != 3 {
		t.Fatalf("calls=%d want 3", calls)
	}
	if res.Status != StatusFailed || res.Steps[0].Attempts != 3 {
		t.Fatalf("result = %+v", res)
	}
	if res.FailureReason != errPermSentinel.Error() {
		t.Fatalf("failure reason=%q want %q", res.FailureReason, errPermSentinel.Error())
	}
}

func TestGraphPermanentFailureAwaitsSiblingsAndCompensatesConfirmed(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &dagRecorder{}
	gateA := newGate() // a 阻塞，放行后返回永久失败

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
				if name == "a" {
					return Permanent(errors.New("perm-a"))
				}
				return nil
			},
			Compensate: func(_ context.Context, _ ExecutionView) error {
				rec.log("undo:" + name)
				return nil
			},
		}
	}
	openGate := newGate()
	openGate.open()
	def := Definition{
		Name: "perm-dag", Version: "v1",
		Steps: []Step{
			step("a", gateA),
			step("b", openGate),      // 无前置，立即成功
			step("c", openGate, "a"), // 依赖 a：a 永久失败后永不启动
		},
	}

	done := make(chan Result, 1)
	go func() {
		res, err := eng.Execute(context.Background(), def, ExecutionRequest{
			BusinessKey: "bk", IdempotencyKey: "ik",
		})
		if err != nil {
			t.Errorf("Execute: %v", err)
		}
		done <- res
	}()

	deadline := time.After(2 * time.Second)
	for indexOf(rec.snapshot(), "start:a") < 0 || indexOf(rec.snapshot(), "done:b") < 0 {
		select {
		case <-deadline:
			t.Fatalf("a and b did not both run: %v", rec.snapshot())
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if indexOf(rec.snapshot(), "start:c") >= 0 {
		t.Fatalf("c must not start before a succeeds: %v", rec.snapshot())
	}
	gateA.open()

	var res Result
	select {
	case res = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Execute did not finish")
	}

	if res.Status != StatusFailed || !res.Terminal || res.FailedStep != "a" ||
		res.FailureReason != "perm-a" {
		t.Fatalf("result = %+v", res)
	}
	outcomes := map[string]StepOutcome{}
	for _, s := range res.Steps {
		outcomes[s.Name] = s
	}
	if outcomes["a"].Result != StepResultFailed || outcomes["a"].Attempts != 1 {
		t.Fatalf("a = %+v", outcomes["a"])
	}
	if outcomes["b"].Result != StepResultCompensated {
		t.Fatalf("started sibling b must commit and be compensated: %+v", outcomes["b"])
	}
	if outcomes["c"].Result != StepResultPending {
		t.Fatalf("unstarted dependent c must stay pending: %+v", outcomes["c"])
	}
	log := rec.snapshot()
	if indexOf(log, "start:c") >= 0 {
		t.Fatalf("c started after halt: %v", log)
	}
	if indexOf(log, "undo:b") < 0 {
		t.Fatalf("confirmed sibling b must be compensated: %v", log)
	}
	for _, s := range log {
		if s == "undo:a" || s == "undo:c" {
			t.Fatalf("only confirmed steps may be compensated: %v", log)
		}
	}

	events, _ := store.ClaimPendingEvents(context.Background(), 100)
	wantTypes := []string{
		EventExecutionStarted,
		EventStepSucceeded, // b（已启动兄弟照常提交）
		EventStepFailed,    // a（永久失败按预算耗尽口径提交）
		EventStepCompensated,
		EventExecutionFailed,
	}
	if len(events) != len(wantTypes) {
		t.Fatalf("events=%d want=%d (%v)", len(events), len(wantTypes), eventTypes(events))
	}
	for i, ev := range events {
		if ev.Type != wantTypes[i] {
			t.Fatalf("event[%d]=%s want %s, all=%v", i, ev.Type, wantTypes[i], eventTypes(events))
		}
	}
}

func TestPermanentCompensationFailureIsTerminalOnFirstCall(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &recorder{}

	compCalls := 0
	def := Definition{
		Name: "perm-comp", Version: "v1",
		Steps: []Step{
			{
				Name:   "reserve",
				Action: flakyAction(rec, "reserve", 0),
				Compensate: func(_ context.Context, _ ExecutionView) error {
					rec.call("undo:reserve")
					compCalls++
					return Permanent(errors.New("undo-impossible"))
				},
				CompensateRetry: RetryPolicy{MaxAttempts: 5, RetryWait: time.Hour},
			},
			{Name: "ship", Action: flakyAction(rec, "ship", 1)},
		},
	}
	req := ExecutionRequest{BusinessKey: "bk", IdempotencyKey: "ik"}

	start := time.Now()
	res, err := eng.Execute(context.Background(), def, req)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("permanent compensation failure must not wait for retry")
	}
	if res.Status != StatusCompensationFailed || !res.Terminal {
		t.Fatalf("status=%s want compensation_failed", res.Status)
	}
	if res.FailedCompensationStep != "reserve" || res.CompensationError != "undo-impossible" {
		t.Fatalf("comp info: %+v", res)
	}
	if compCalls != 1 {
		t.Fatalf("compensation calls=%d want 1 despite remaining budget", compCalls)
	}
	if got := res.Steps[0]; got.Result != StepResultCompensationFailed ||
		got.CompensationAttempts != 1 || got.Error != "undo-impossible" {
		t.Fatalf("reserve outcome = %+v", got)
	}

	events, _ := store.ClaimPendingEvents(context.Background(), 100)
	var compFailPayload *EventPayload
	var terminalPayload *EventPayload
	for _, ev := range events {
		switch ev.Type {
		case EventStepCompensationFailed:
			p := ev.Payload.(EventPayload)
			compFailPayload = &p
		case EventCompensationFailed:
			p := ev.Payload.(EventPayload)
			terminalPayload = &p
		}
	}
	if compFailPayload == nil || terminalPayload == nil {
		t.Fatalf("missing compensation failure events: %v", eventTypes(events))
	}
	if compFailPayload.Attempt != 1 || compFailPayload.Reason != "undo-impossible" {
		t.Fatalf("comp failed payload = %+v", compFailPayload)
	}

	// 同身份重入：直接返回既有终态，不再调用补偿、不追加事件。
	eventsBefore := store.TotalEvents()
	res2, err := eng.Execute(context.Background(), def, req)
	if err != nil {
		t.Fatalf("terminal re-entry: %v", err)
	}
	if res2.Status != StatusCompensationFailed || !res2.Terminal ||
		res2.CompensationError != "undo-impossible" {
		t.Fatalf("terminal result changed: %+v", res2)
	}
	if compCalls != 1 {
		t.Fatalf("compensation invoked after terminal: %d", compCalls)
	}
	if store.TotalEvents() != eventsBefore {
		t.Fatalf("events appended after terminal: before=%d after=%d", eventsBefore, store.TotalEvents())
	}
}

func TestContextCancellationTakesPrecedenceOverPermanentFailure(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)

	started := make(chan struct{})
	release := make(chan struct{})
	calls := 0
	def := Definition{
		Name: "perm-cancel", Version: "v1",
		Steps: []Step{{
			Name: "s",
			Action: func(ctx context.Context, _ ExecutionView) error {
				calls++
				if calls > 1 {
					return nil // 同身份继续：本次成功
				}
				close(started)
				<-release
				// 即使动作返回永久失败，外层 ctx 已取消时仍按取消处理。
				return Permanent(errors.New("perm-but-canceled"))
			},
			ActionRetry: RetryPolicy{MaxAttempts: 5, RetryWait: time.Hour},
		}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-started
		cancel()
		close(release)
	}()

	if _, err := eng.Execute(ctx, def, ExecutionRequest{
		BusinessKey: "bk", IdempotencyKey: "ik",
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v want context.Canceled", err)
	}
	if calls != 1 {
		t.Fatalf("calls=%d want 1", calls)
	}
	snap, _ := store.LoadSnapshot(context.Background(), "bk", "ik")
	if snap.State.Status != StatusRunning || snap.State.Steps[0].Status != "" {
		t.Fatalf("permanent failure must not be confirmed after cancellation: %+v", snap.State)
	}
	if snap.State.Steps[0].Attempts != 0 || snap.State.FailedStep != "" {
		t.Fatalf("no failure may be recorded: %+v", snap.State.Steps[0])
	}

	// 同身份、新 context 继续：动作再次调用（本次成功），执行正常完成。
	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "bk", IdempotencyKey: "ik",
	})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if calls != 2 {
		t.Fatalf("calls after resume=%d want 2", calls)
	}
	if res.Status != StatusCompleted || res.Steps[0].Attempts != 1 {
		t.Fatalf("resume result = %+v", res)
	}
}
