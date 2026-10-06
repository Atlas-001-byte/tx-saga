package txsaga

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// flakyAction 前 failTimes 次返回固定错误，之后成功，并通过 recorder 记录调用。
func flakyAction(rec *recorder, name string, failTimes int) ActionFunc {
	n := 0
	return func(_ context.Context, _ ExecutionView) error {
		rec.call("do:" + name)
		n++
		if n <= failTimes {
			return errors.New("transient-" + name)
		}
		return nil
	}
}

// flakyCompensateN 前 failTimes 次返回固定错误，之后成功，并暴露调用次数。
func flakyCompensateN(rec *recorder, name string, failTimes int) (CompensationFunc, func() int) {
	n := 0
	return func(_ context.Context, _ ExecutionView) error {
		rec.call("undo:" + name)
		n++
		if n <= failTimes {
			return errors.New("comp-transient-" + name)
		}
		return nil
	}, func() int { return n }
}

func retrySagaDef(rec *recorder, chargeFails, shipFails int, actionRetry RetryPolicy) Definition {
	mk := func(name string, fails int) Step {
		return Step{
			Name:        name,
			Action:      flakyAction(rec, name, fails),
			Compensate:  func(_ context.Context, _ ExecutionView) error { rec.call("undo:" + name); return nil },
			ActionRetry: actionRetry,
		}
	}
	return Definition{
		Name:    "retry-saga",
		Version: "v1",
		Steps:   []Step{mk("reserve", 0), mk("charge", chargeFails), mk("ship", shipFails)},
	}
}

func TestActionRetrySucceedsWithinBudget(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &recorder{}
	// charge 前 2 次失败、第 3 次成功；预算 3 次、失败后立即重试。
	def := retrySagaDef(rec, 2, 0, RetryPolicy{MaxAttempts: 3})

	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "r-1", IdempotencyKey: "req-1",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Status != StatusCompleted || !res.Terminal {
		t.Fatalf("status=%s terminal=%v", res.Status, res.Terminal)
	}
	// charge 共调用 3 次，其余各 1 次。
	if got := rec.snapshot(); fmt.Sprint(got) !=
		"[do:reserve do:charge do:charge do:charge do:ship]" {
		t.Fatalf("calls = %v", got)
	}
	if res.Steps[1].Attempts != 3 {
		t.Fatalf("charge attempts=%d want 3", res.Steps[1].Attempts)
	}
	if res.Steps[1].Error != "" {
		t.Fatalf("error must be cleared after success: %q", res.Steps[1].Error)
	}
	if res.Steps[0].Attempts != 1 || res.Steps[2].Attempts != 1 {
		t.Fatalf("other step attempts = %d,%d want 1,1", res.Steps[0].Attempts, res.Steps[2].Attempts)
	}

	// 中间失败不追加事件：仍是 started -> 每步一次 succeeded -> completed。
	events, _ := store.ClaimPendingEvents(context.Background(), 100)
	wantTypes := []string{
		EventExecutionStarted,
		EventStepSucceeded, EventStepSucceeded, EventStepSucceeded,
		EventExecutionCompleted,
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

func TestActionRetryExhaustedTriggersCompensation(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &recorder{}
	// ship 连续失败 3 次耗尽预算；reserve、charge 已确认成功需逆序补偿。
	def := retrySagaDef(rec, 0, 3, RetryPolicy{MaxAttempts: 3})

	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "r-2", IdempotencyKey: "req-2",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Status != StatusFailed || !res.Terminal {
		t.Fatalf("status=%s want failed", res.Status)
	}
	if res.FailedStep != "ship" || res.FailureReason != "transient-ship" {
		t.Fatalf("failure info: %+v", res)
	}
	ship := res.Steps[2]
	if ship.Result != StepResultFailed || ship.Attempts != 3 || ship.Error != "transient-ship" {
		t.Fatalf("ship outcome = %+v", ship)
	}
	if got := rec.snapshot(); fmt.Sprint(got) !=
		"[do:reserve do:charge do:ship do:ship do:ship undo:charge undo:reserve]" {
		t.Fatalf("calls = %v", got)
	}

	// 中间重试不产生事件：仅 started、两次 succeeded、一次 failed、两次补偿、failed。
	events, _ := store.ClaimPendingEvents(context.Background(), 100)
	wantTypes := []string{
		EventExecutionStarted,
		EventStepSucceeded, // reserve
		EventStepSucceeded, // charge
		EventStepFailed,    // ship（仅在预算耗尽时提交一次）
		EventStepCompensated, EventStepCompensated,
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

func TestCompensationRetrySucceedsWithinBudget(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &recorder{}

	// ship 正向失败（默认预算 1，一次失败即转补偿）；此前已确认成功的步骤
	// 按逆序补偿：先 charge（前 2 次失败、第 3 次成功），再 reserve（始终成功）。
	chargeComp, chargeCompN := flakyCompensateN(rec, "charge", 2)
	reserveComp := func(_ context.Context, _ ExecutionView) error { rec.call("undo:reserve"); return nil }
	def := Definition{
		Name: "comp-retry-saga", Version: "v1",
		Steps: []Step{
			{
				Name:       "reserve",
				Action:     flakyAction(rec, "reserve", 0),
				Compensate: reserveComp,
			},
			{
				Name:            "charge",
				Action:          flakyAction(rec, "charge", 0),
				Compensate:      chargeComp,
				CompensateRetry: RetryPolicy{MaxAttempts: 3},
			},
			{Name: "ship", Action: flakyAction(rec, "ship", 1)},
		},
	}

	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "r-3", IdempotencyKey: "req-3",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Status != StatusFailed || !res.Terminal {
		t.Fatalf("status=%s want failed", res.Status)
	}
	if chargeCompN() != 3 {
		t.Fatalf("charge compensation calls=%d want 3", chargeCompN())
	}
	if got := res.Steps[1].CompensationAttempts; got != 3 {
		t.Fatalf("charge CompensationAttempts=%d want 3", got)
	}
	if res.Steps[1].Result != StepResultCompensated {
		t.Fatalf("charge result=%s want compensated", res.Steps[1].Result)
	}
	if got := rec.snapshot(); fmt.Sprint(got) !=
		"[do:reserve do:charge do:ship undo:charge undo:charge undo:charge undo:reserve]" {
		t.Fatalf("calls = %v", got)
	}

	events, _ := store.ClaimPendingEvents(context.Background(), 100)
	wantTypes := []string{
		EventExecutionStarted,
		EventStepSucceeded, // reserve
		EventStepSucceeded, // charge
		EventStepFailed,    // ship
		EventStepCompensated, EventStepCompensated,
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

func TestCompensationRetryExhaustedIsTerminalAndStable(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &recorder{}

	// ship 正向失败；逆序先补偿 charge，其补偿预算 3 次全部失败 =>
	// compensation_failed；reserve 的补偿不得被调用。
	reserveComp, reserveCompN := flakyCompensateN(rec, "reserve", 0)
	chargeComp, chargeCompN := flakyCompensateN(rec, "charge", 99)
	def := Definition{
		Name: "comp-retry-fail", Version: "v1",
		Steps: []Step{
			{Name: "reserve", Action: flakyAction(rec, "reserve", 0), Compensate: reserveComp},
			{
				Name:            "charge",
				Action:          flakyAction(rec, "charge", 0),
				Compensate:      chargeComp,
				CompensateRetry: RetryPolicy{MaxAttempts: 3},
			},
			{Name: "ship", Action: flakyAction(rec, "ship", 1)},
		},
	}
	req := ExecutionRequest{BusinessKey: "r-4", IdempotencyKey: "req-4"}

	res, err := eng.Execute(context.Background(), def, req)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Status != StatusCompensationFailed || !res.Terminal {
		t.Fatalf("status=%s want compensation_failed", res.Status)
	}
	if res.FailedCompensationStep != "charge" || res.CompensationError != "comp-transient-charge" {
		t.Fatalf("comp info: %+v", res)
	}
	if chargeCompN() != 3 {
		t.Fatalf("charge compensation calls=%d want 3", chargeCompN())
	}
	if reserveCompN() != 0 {
		t.Fatalf("reserve compensation must not run after earlier compensation failed: %d", reserveCompN())
	}
	if got := res.Steps[1].CompensationAttempts; got != 3 {
		t.Fatalf("CompensationAttempts=%d want 3", got)
	}
	if res.Steps[1].Result != StepResultCompensationFailed {
		t.Fatalf("charge result=%s", res.Steps[1].Result)
	}

	// 终态事件中 Attempt 为预算内总调用次数。
	events, _ := store.ClaimPendingEvents(context.Background(), 100)
	var compFailPayload *EventPayload
	for _, ev := range events {
		if ev.Type == EventStepCompensationFailed {
			p := ev.Payload.(EventPayload)
			compFailPayload = &p
		}
	}
	if compFailPayload == nil {
		t.Fatalf("missing %s event: %v", EventStepCompensationFailed, eventTypes(events))
	}
	if compFailPayload.Attempt != 3 {
		t.Fatalf("compensation failed event Attempt=%d want 3", compFailPayload.Attempt)
	}

	// 同身份再次调用：终态固定，不再调用补偿、不追加事件。
	eventsBefore := store.TotalEvents()
	if _, err := eng.Execute(context.Background(), def, req); err != nil {
		t.Fatalf("terminal retry: %v", err)
	}
	if chargeCompN() != 3 {
		t.Fatalf("compensation invoked again after terminal state: %d", chargeCompN())
	}
	if store.TotalEvents() != eventsBefore {
		t.Fatalf("events appended: before=%d after=%d", eventsBefore, store.TotalEvents())
	}
}

func TestRetryPolicyValidation(t *testing.T) {
	valid := func() Definition {
		return Definition{Name: "d", Steps: []Step{{
			Name:   "s",
			Action: func(context.Context, ExecutionView) error { return nil },
		}}}
	}
	cases := []struct {
		name string
		mut  func(*Step)
	}{
		{"negative action max attempts", func(s *Step) { s.ActionRetry.MaxAttempts = -1 }},
		{"negative action retry wait", func(s *Step) { s.ActionRetry.RetryWait = -time.Nanosecond }},
		{"negative compensation max attempts", func(s *Step) {
			s.CompensateRetry.MaxAttempts = -1
		}},
		{"negative compensation retry wait", func(s *Step) {
			s.CompensateRetry.RetryWait = -time.Nanosecond
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := valid()
			tc.mut(&d.Steps[0])
			if err := ValidateDefinition(d); !errors.Is(err, ErrInvalidDefinition) {
				t.Fatalf("ValidateDefinition err=%v want ErrInvalidDefinition", err)
			}
			if _, err := NewEngine(NewMemoryStore()).Execute(context.Background(), d, ExecutionRequest{
				BusinessKey: "b", IdempotencyKey: "i",
			}); !errors.Is(err, ErrInvalidDefinition) {
				t.Fatalf("Execute err=%v want ErrInvalidDefinition", err)
			}
		})
	}

	// MaxAttempts=0 按 1 处理：动作失败一次即转补偿，不发生重试。
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &recorder{}
	d := Definition{
		Name: "zero-default", Version: "v1",
		Steps: []Step{
			{Name: "a", Action: flakyAction(rec, "a", 0), Compensate: func(context.Context, ExecutionView) error { return nil }},
			{
				Name:        "b",
				Action:      flakyAction(rec, "b", 5),
				Compensate:  func(context.Context, ExecutionView) error { return nil },
				ActionRetry: RetryPolicy{}, // 零值 => 1 次、等待 0
			},
		},
	}
	res, err := eng.Execute(context.Background(), d, ExecutionRequest{BusinessKey: "b", IdempotencyKey: "i"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusFailed || res.Steps[1].Attempts != 1 {
		t.Fatalf("zero-value policy must behave as baseline: %+v", res)
	}
}

func TestActionRetryWaitElapsesBetweenAttempts(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &recorder{}
	// 前 2 次失败、第 3 次成功，每次失败等待 25ms => 至少等待 2*25ms。
	def := retrySagaDef(rec, 2, 0, RetryPolicy{MaxAttempts: 3, RetryWait: 25 * time.Millisecond})

	start := time.Now()
	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "r-5", IdempotencyKey: "req-5",
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusCompleted {
		t.Fatalf("status=%s", res.Status)
	}
	if elapsed < 50*time.Millisecond {
		t.Fatalf("elapsed=%v want >= 50ms (two retry waits)", elapsed)
	}
}

func TestCancellationDuringRetryWaitLeavesStepUnconfirmed(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &recorder{}

	// 首次调用失败后进入 1 小时等待；等待期间取消。第二次 Execute 同身份时成功。
	calls := 0
	def := Definition{
		Name: "wait-cancel", Version: "v1",
		Steps: []Step{{
			Name: "s",
			Action: func(_ context.Context, _ ExecutionView) error {
				rec.call("do:s")
				calls++
				if calls == 1 {
					return errors.New("transient")
				}
				return nil
			},
			ActionRetry: RetryPolicy{MaxAttempts: 3, RetryWait: time.Hour},
		}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	if _, err := eng.Execute(ctx, def, ExecutionRequest{
		BusinessKey: "bk", IdempotencyKey: "ik",
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v want context.Canceled", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("Execute did not return promptly after cancellation during wait")
	}

	// 未确认：状态仍 running、步骤仍 pending、没有失败记录，事件只有 started。
	snap, err := store.LoadSnapshot(context.Background(), "bk", "ik")
	if err != nil {
		t.Fatal(err)
	}
	if snap.State.Status != StatusRunning || snap.State.Steps[0].Status != "" {
		t.Fatalf("uncommitted step must remain pending: %+v", snap.State.Steps[0])
	}
	if snap.State.Steps[0].Attempts != 0 || snap.State.FailedStep != "" {
		t.Fatalf("failed attempt must not be committed: %+v", snap.State.Steps[0])
	}
	events, _ := store.ClaimPendingEvents(context.Background(), 100)
	if len(events) != 1 || events[0].Type != EventExecutionStarted {
		t.Fatalf("only started event allowed after cancellation, got %v", eventTypes(events))
	}
	_ = store.NackEvent(context.Background(), events[0].ID)

	// 新 context、同身份继续：动作再次调用并成功，Attempts 为本次阶段的 1 次。
	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "bk", IdempotencyKey: "ik",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusCompleted || res.Steps[0].Attempts != 1 {
		t.Fatalf("resume result = %+v", res)
	}
	if calls != 2 {
		t.Fatalf("action calls=%d want 2", calls)
	}
}

func TestActionEndingWithContextErrorIsNotRetried(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)

	started := make(chan struct{})
	release := make(chan struct{})
	calls := 0
	def := Definition{
		Name: "action-ctx-err", Version: "v1",
		Steps: []Step{{
			Name: "s",
			Action: func(ctx context.Context, _ ExecutionView) error {
				calls++
				close(started)
				<-release
				return ctx.Err() // 动作随外层 context 取消结束
			},
			// 预算再大也不得在取消后继续重试。
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
		t.Fatalf("action calls=%d want 1 (must not retry after context error)", calls)
	}
	snap, _ := store.LoadSnapshot(context.Background(), "bk", "ik")
	if snap.State.Steps[0].Status != "" || snap.State.Status != StatusRunning {
		t.Fatalf("step must stay unconfirmed: %+v", snap.State.Steps[0])
	}
}

func TestCancellationDuringCompensationRetryLeavesStateUnchanged(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &recorder{}

	// reserve 已确认成功；s2 正向失败转补偿；reserve 的补偿先失败一次再进入长等待。
	calls := 0
	reserveComp := func(_ context.Context, _ ExecutionView) error {
		rec.call("undo:reserve")
		calls++
		return errors.New("comp-transient") // 每次都失败；首次失败后等待期间取消
	}
	def := Definition{
		Name: "comp-wait-cancel", Version: "v1",
		Steps: []Step{
			{Name: "reserve", Action: flakyAction(rec, "reserve", 0), Compensate: reserveComp,
				CompensateRetry: RetryPolicy{MaxAttempts: 3, RetryWait: time.Hour}},
			{Name: "s2", Action: flakyAction(rec, "s2", 1)},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()
	if _, err := eng.Execute(ctx, def, ExecutionRequest{
		BusinessKey: "bk", IdempotencyKey: "ik",
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v want context.Canceled", err)
	}

	// 补偿未确认：执行仍为 compensating，reserve 仍为 succeeded，无补偿失败终态，
	// 已提交的正向状态不变。
	snap, _ := store.LoadSnapshot(context.Background(), "bk", "ik")
	if snap.State.Status != StatusCompensating {
		t.Fatalf("status=%s want compensating", snap.State.Status)
	}
	if snap.State.Steps[0].Status != StepResultSucceeded ||
		snap.State.Steps[0].CompensationAttempts != 0 {
		t.Fatalf("compensation must not be committed: %+v", snap.State.Steps[0])
	}
	if snap.State.FailedCompensationStep != "" {
		t.Fatalf("must not record compensation failure: %+v", snap.State)
	}
	if calls != 1 {
		t.Fatalf("compensation calls=%d want 1", calls)
	}
}
