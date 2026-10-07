package txsaga

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestPermanentWrapperSemantics(t *testing.T) {
	if Permanent(nil) != nil {
		t.Fatal("Permanent(nil) must return nil")
	}
	base := errors.New("boom")
	p := Permanent(base)
	if p.Error() != "boom" {
		t.Fatalf("Error()=%q want underlying text", p.Error())
	}
	if !errors.Is(p, base) {
		t.Fatal("errors.Is(Permanent(base), base) must be true")
	}
	if !errors.Is(p, p) {
		t.Fatal("errors.Is(p, p) must be true")
	}
	if errors.Unwrap(p) != base {
		t.Fatal("Unwrap must return the underlying error")
	}
	if !IsPermanent(p) {
		t.Fatal("IsPermanent(Permanent(base)) must be true")
	}
	if IsPermanent(base) || IsPermanent(nil) {
		t.Fatal("plain error and nil must not be permanent")
	}
	// 错误链任意层可判定：外层 fmt.Errorf 包装后仍可识别，且底层错误可达。
	wrapped := fmt.Errorf("charge failed: %w", p)
	if !IsPermanent(wrapped) {
		t.Fatal("IsPermanent must see through fmt.Errorf wrapping")
	}
	if !errors.Is(wrapped, base) {
		t.Fatal("errors.Is must still reach the underlying error")
	}
	// 重复包装不改变语义。
	if !IsPermanent(Permanent(p)) || Permanent(p).Error() != "boom" {
		t.Fatal("double wrapping must keep semantics and text")
	}
}

// 顺序模式：正向动作返回永久错误时，即使预算与等待充足也只调用一次，
// 按预算耗尽口径提交失败并逆序补偿已确认步骤。
func TestPermanentActionFailsImmediatelySequential(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &recorder{}

	declined := errors.New("card-declined")
	chargeCalls := 0
	def := Definition{
		Name: "perm-seq", Version: "v1",
		Steps: []Step{
			{
				Name:       "reserve",
				Action:     flakyAction(rec, "reserve", 0),
				Compensate: func(_ context.Context, _ ExecutionView) error { rec.call("undo:reserve"); return nil },
			},
			{
				Name: "charge",
				Action: func(_ context.Context, _ ExecutionView) error {
					rec.call("do:charge")
					chargeCalls++
					return Permanent(declined)
				},
				Compensate: func(_ context.Context, _ ExecutionView) error { rec.call("undo:charge"); return nil },
				// 预算与等待充足：若非永久失败会调用 5 次且每次等待 1 小时。
				ActionRetry: RetryPolicy{MaxAttempts: 5, RetryWait: time.Hour},
			},
			{Name: "ship", Action: flakyAction(rec, "ship", 0)},
		},
	}
	req := ExecutionRequest{BusinessKey: "p-1", IdempotencyKey: "req-1"}

	start := time.Now()
	res, err := eng.Execute(context.Background(), def, req)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if time.Since(start) > 10*time.Second {
		t.Fatal("permanent failure must not wait for retry budget")
	}
	if res.Status != StatusFailed || !res.Terminal {
		t.Fatalf("status=%s want failed", res.Status)
	}
	if chargeCalls != 1 {
		t.Fatalf("charge calls=%d want 1 (permanent stops retrying)", chargeCalls)
	}
	// 失败口径与预算耗尽一致：失败原因保留底层错误文本，Attempts 只记实际调用。
	if res.FailedStep != "charge" || res.FailureReason != "card-declined" {
		t.Fatalf("failure info: %+v", res)
	}
	charge := res.Steps[1]
	if charge.Result != StepResultFailed || charge.Attempts != 1 || charge.Error != "card-declined" {
		t.Fatalf("charge outcome = %+v", charge)
	}
	if res.Steps[2].Result != StepResultPending {
		t.Fatalf("ship must stay pending: %+v", res.Steps[2])
	}
	if got := rec.snapshot(); fmt.Sprint(got) != "[do:reserve do:charge undo:reserve]" {
		t.Fatalf("calls = %v", got)
	}

	// 永久失败不产生中间失败事件：与预算耗尽失败的事件序列一致。
	events, _ := store.ClaimPendingEvents(context.Background(), 100)
	wantTypes := []string{
		EventExecutionStarted,
		EventStepSucceeded, // reserve
		EventStepFailed,    // charge（仅提交一次）
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

	// 终态固定：同身份重入不再调用动作、不追加事件。
	eventsBefore := store.TotalEvents()
	res2, err := eng.Execute(context.Background(), def, req)
	if err != nil {
		t.Fatalf("terminal retry: %v", err)
	}
	if res2.Status != StatusFailed || chargeCalls != 1 || store.TotalEvents() != eventsBefore {
		t.Fatalf("terminal result must be stable: %+v calls=%d", res2, chargeCalls)
	}
}

// 永久错误出现在预算内的后续尝试中：先普通失败重试一次，再永久失败即停止。
func TestPermanentErrorAfterTransientAttempt(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)

	calls := 0
	def := Definition{
		Name: "perm-after-transient", Version: "v1",
		Steps: []Step{{
			Name: "s",
			Action: func(_ context.Context, _ ExecutionView) error {
				calls++
				if calls == 1 {
					return errors.New("transient")
				}
				return Permanent(errors.New("final"))
			},
			ActionRetry: RetryPolicy{MaxAttempts: 5},
		}},
	}
	res, err := eng.Execute(context.Background(), def, ExecutionRequest{BusinessKey: "p-2", IdempotencyKey: "req-2"})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("calls=%d want 2 (transient retried once, permanent stops)", calls)
	}
	if res.Status != StatusFailed || res.Steps[0].Attempts != 2 || res.FailureReason != "final" {
		t.Fatalf("result = %+v", res)
	}
}

// 依赖图模式：正向永久失败停止调度未启动步骤，已启动兄弟照常提交，
// 随后只按确认成功顺序逆序补偿。
func TestPermanentActionFailsImmediatelyGraph(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &recorder{}

	def := Definition{
		Name: "perm-graph", Version: "v1",
		Steps: []Step{
			{
				Name: "fast-fail",
				Action: func(_ context.Context, _ ExecutionView) error {
					rec.call("do:fast-fail")
					return Permanent(errors.New("no-such-order"))
				},
				Compensate:  func(_ context.Context, _ ExecutionView) error { rec.call("undo:fast-fail"); return nil },
				ActionRetry: RetryPolicy{MaxAttempts: 4, RetryWait: time.Hour},
			},
			{
				Name: "slow",
				Action: func(_ context.Context, _ ExecutionView) error {
					rec.call("do:slow")
					time.Sleep(50 * time.Millisecond)
					return nil
				},
				Compensate: func(_ context.Context, _ ExecutionView) error { rec.call("undo:slow"); return nil },
			},
			{
				Name:      "downstream",
				DependsOn: []string{"fast-fail", "slow"},
				Action:    func(_ context.Context, _ ExecutionView) error { rec.call("do:downstream"); return nil },
			},
		},
	}

	res, err := eng.Execute(context.Background(), def, ExecutionRequest{BusinessKey: "p-3", IdempotencyKey: "req-3"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusFailed || !res.Terminal {
		t.Fatalf("status=%s want failed", res.Status)
	}
	if res.FailedStep != "fast-fail" || res.FailureReason != "no-such-order" {
		t.Fatalf("failure info: %+v", res)
	}
	// 未启动的 downstream 不得执行；失败的 fast-fail 不补偿；已确认成功的
	// slow 被补偿。
	if rec.count("do:downstream") != 0 {
		t.Fatalf("downstream must not be scheduled: %v", rec.snapshot())
	}
	if rec.count("do:fast-fail") != 1 || rec.count("undo:fast-fail") != 0 {
		t.Fatalf("fast-fail calls: %v", rec.snapshot())
	}
	if rec.count("do:slow") != 1 || rec.count("undo:slow") != 1 {
		t.Fatalf("slow calls: %v", rec.snapshot())
	}
	if res.Steps[0].Attempts != 1 || res.Steps[0].Result != StepResultFailed {
		t.Fatalf("fast-fail outcome = %+v", res.Steps[0])
	}
	if res.Steps[1].Result != StepResultCompensated {
		t.Fatalf("slow outcome = %+v", res.Steps[1])
	}
	if res.Steps[2].Result != StepResultPending {
		t.Fatalf("downstream outcome = %+v", res.Steps[2])
	}

	// 事件序列与预算耗尽失败一致：started、failed、兄弟 succeeded、补偿、failed。
	events, _ := store.ClaimPendingEvents(context.Background(), 100)
	wantTypes := []string{
		EventExecutionStarted,
		EventStepFailed,    // fast-fail 先返回
		EventStepSucceeded, // slow 已启动，照常提交
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

// 补偿动作返回永久错误：第一次调用即固定为 compensation_failed，
// 即使补偿预算尚有剩余也不再重试；同身份重入直接返回既有终态。
func TestPermanentCompensationFailsImmediately(t *testing.T) {
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
					return Permanent(errors.New("cannot-undo"))
				},
				// 预算与等待充足：普通错误会调用 5 次。
				CompensateRetry: RetryPolicy{MaxAttempts: 5, RetryWait: time.Hour},
			},
			{Name: "ship", Action: flakyAction(rec, "ship", 1)},
		},
	}
	req := ExecutionRequest{BusinessKey: "p-4", IdempotencyKey: "req-4"}

	start := time.Now()
	res, err := eng.Execute(context.Background(), def, req)
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 10*time.Second {
		t.Fatal("permanent compensation failure must not wait for retry budget")
	}
	if res.Status != StatusCompensationFailed || !res.Terminal {
		t.Fatalf("status=%s want compensation_failed", res.Status)
	}
	if compCalls != 1 {
		t.Fatalf("compensation calls=%d want 1", compCalls)
	}
	if res.FailedCompensationStep != "reserve" || res.CompensationError != "cannot-undo" {
		t.Fatalf("comp info: %+v", res)
	}
	if res.Steps[0].Result != StepResultCompensationFailed || res.Steps[0].CompensationAttempts != 1 {
		t.Fatalf("reserve outcome = %+v", res.Steps[0])
	}

	events, _ := store.ClaimPendingEvents(context.Background(), 100)
	wantTypes := []string{
		EventExecutionStarted,
		EventStepSucceeded, // reserve
		EventStepFailed,    // ship
		EventStepCompensationFailed,
		EventCompensationFailed,
	}
	if len(events) != len(wantTypes) {
		t.Fatalf("events=%d want=%d (%v)", len(events), len(wantTypes), eventTypes(events))
	}
	for i, ev := range events {
		if ev.Type != wantTypes[i] {
			t.Fatalf("event[%d]=%s want %s, all=%v", i, ev.Type, wantTypes[i], eventTypes(events))
		}
	}

	// 同身份重入：直接返回既有终态，不再调用补偿、不追加事件。
	eventsBefore := store.TotalEvents()
	res2, err := eng.Execute(context.Background(), def, req)
	if err != nil {
		t.Fatalf("terminal retry: %v", err)
	}
	if res2.Status != StatusCompensationFailed || compCalls != 1 || store.TotalEvents() != eventsBefore {
		t.Fatalf("terminal must be stable: %+v calls=%d", res2, compCalls)
	}
}

// 动作返回经 fmt.Errorf 包装的永久错误同样可判定：只调用一次，
// 失败原因保留动作返回的完整错误文本。
func TestPermanentWrappedErrorFromAction(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)

	calls := 0
	def := Definition{
		Name: "perm-wrapped", Version: "v1",
		Steps: []Step{{
			Name: "charge",
			Action: func(_ context.Context, _ ExecutionView) error {
				calls++
				return fmt.Errorf("charge failed: %w", Permanent(errors.New("card-declined")))
			},
			ActionRetry: RetryPolicy{MaxAttempts: 3},
		}},
	}
	res, err := eng.Execute(context.Background(), def, ExecutionRequest{BusinessKey: "p-5", IdempotencyKey: "req-5"})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("calls=%d want 1", calls)
	}
	if res.Status != StatusFailed || res.FailureReason != "charge failed: card-declined" {
		t.Fatalf("result = %+v", res)
	}
	if res.Steps[0].Error != "charge failed: card-declined" || res.Steps[0].Attempts != 1 {
		t.Fatalf("step outcome = %+v", res.Steps[0])
	}
}

// context 取消优先于永久失败：动作随取消返回永久错误时按取消处理，
// 不确认动作、不启动补偿，同身份重入可继续。
func TestContextCancelBeatsPermanentFailure(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)

	started := make(chan struct{})
	release := make(chan struct{})
	calls := 0
	def := Definition{
		Name: "perm-ctx", Version: "v1",
		Steps: []Step{{
			Name: "s",
			Action: func(ctx context.Context, _ ExecutionView) error {
				calls++
				if calls == 1 {
					close(started)
					<-release
					return Permanent(errors.New("perm"))
				}
				return nil
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
		t.Fatalf("step must stay unconfirmed: %+v", snap.State.Steps[0])
	}

	// 同身份重入继续：第二次调用成功，执行完成。
	res, err := eng.Execute(context.Background(), def, ExecutionRequest{BusinessKey: "bk", IdempotencyKey: "ik"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusCompleted || calls != 2 {
		t.Fatalf("resume result=%+v calls=%d", res, calls)
	}
}
