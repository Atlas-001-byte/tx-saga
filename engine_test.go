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

// recorder 记录各动作被调用的顺序与次数，供断言编排顺序和是否重复处理。
type recorder struct {
	mu    sync.Mutex
	calls []string
}

func (r *recorder) call(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, name)
}

func (r *recorder) count(name string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, c := range r.calls {
		if c == name {
			n++
		}
	}
	return n
}

func (r *recorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

func threeStepDef(rec *recorder, failStep string, failComp string) Definition {
	step := func(name string) Step {
		return Step{
			Name: name,
			Action: func(_ context.Context, _ ExecutionView) error {
				rec.call("do:" + name)
				if name == failStep {
					return errors.New("boom-" + name)
				}
				return nil
			},
			Compensate: func(_ context.Context, _ ExecutionView) error {
				rec.call("undo:" + name)
				if name == failComp {
					return errors.New("comp-boom-" + name)
				}
				return nil
			},
		}
	}
	return Definition{
		Name:    "order-saga",
		Version: "v1",
		Steps:   []Step{step("reserve"), step("charge"), step("ship")},
	}
}

func TestExecuteSuccess(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &recorder{}
	def := threeStepDef(rec, "", "")

	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey:    "order-1",
		IdempotencyKey: "req-1",
		Payload:        map[string]int{"amount": 100},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Status != StatusCompleted || !res.Terminal {
		t.Fatalf("status=%s terminal=%v, want completed/terminal", res.Status, res.Terminal)
	}
	if got := rec.snapshot(); fmt.Sprint(got) != "[do:reserve do:charge do:ship]" {
		t.Fatalf("calls = %v", got)
	}
	for _, so := range res.Steps {
		if so.Result != StepResultSucceeded || so.Attempts != 1 {
			t.Fatalf("step %s = %+v", so.Name, so)
		}
		if so.StartedAt.IsZero() || so.FinishedAt.Before(so.StartedAt) {
			t.Fatalf("step %s bad timestamps: %v %v", so.Name, so.StartedAt, so.FinishedAt)
		}
	}

	// 事件顺序：started -> 每步 succeeded -> completed，且与状态同事务可见。
	events, err := store.ClaimPendingEvents(context.Background(), 100)
	if err != nil {
		t.Fatal(err)
	}
	wantTypes := []string{
		EventExecutionStarted,
		EventStepSucceeded, EventStepSucceeded, EventStepSucceeded,
		EventExecutionCompleted,
	}
	if len(events) != len(wantTypes) {
		t.Fatalf("events=%d want=%d", len(events), len(wantTypes))
	}
	for i, ev := range events {
		if ev.Type != wantTypes[i] {
			t.Fatalf("event[%d]=%s want %s", i, ev.Type, wantTypes[i])
		}
		if ev.ID == "" || ev.BusinessKey != "order-1" || ev.OccurredAt.IsZero() {
			t.Fatalf("event metadata incomplete: %+v", ev)
		}
		if ev.Deliveries() != 1 {
			t.Fatalf("deliveries=%d want 1", ev.Deliveries())
		}
	}
}

func TestExecuteForwardFailureCompensatesInReverse(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &recorder{}
	// charge 失败：reserve 已确认成功需补偿；ship 从未执行，不得补偿。
	def := threeStepDef(rec, "charge", "")

	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey:    "order-2",
		IdempotencyKey: "req-2",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Status != StatusFailed || !res.Terminal {
		t.Fatalf("status=%s want failed", res.Status)
	}
	if res.FailedStep != "charge" || res.FailureReason != "boom-charge" {
		t.Fatalf("failure info: %+v", res)
	}
	if got := rec.snapshot(); fmt.Sprint(got) != "[do:reserve do:charge undo:reserve]" {
		t.Fatalf("calls = %v, want forward then reverse compensation only", got)
	}
	results := map[string]string{}
	for _, so := range res.Steps {
		results[so.Name] = so.Result
	}
	if results["reserve"] != StepResultCompensated ||
		results["charge"] != StepResultFailed ||
		results["ship"] != StepResultPending {
		t.Fatalf("step results = %v", results)
	}

	events, _ := store.ClaimPendingEvents(context.Background(), 100)
	wantTypes := []string{
		EventExecutionStarted,
		EventStepSucceeded, // reserve
		EventStepFailed,    // charge
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

func TestCompensationFailureIsTerminalAndStable(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &recorder{}
	// charge 失败，reserve 的补偿也失败。
	def := threeStepDef(rec, "charge", "reserve")

	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey:    "order-3",
		IdempotencyKey: "req-3",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Status != StatusCompensationFailed || !res.Terminal {
		t.Fatalf("status=%s want compensation_failed", res.Status)
	}
	if res.FailedCompensationStep != "reserve" || res.CompensationError != "comp-boom-reserve" {
		t.Fatalf("comp info: %+v", res)
	}
	if res.FailureReason != "boom-charge" {
		t.Fatalf("original failure reason must be kept: %+v", res)
	}

	eventsBefore := store.TotalEvents()
	callsBefore := len(rec.snapshot())

	// 后续同身份调用直接返回固定结果：不再调用任何动作/补偿，不追加事件。
	res2, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey:    "order-3",
		IdempotencyKey: "req-3",
	})
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if res2.Status != StatusCompensationFailed {
		t.Fatalf("status changed to %s", res2.Status)
	}
	if store.TotalEvents() != eventsBefore {
		t.Fatalf("events appended on terminal retry: before=%d after=%d", eventsBefore, store.TotalEvents())
	}
	if len(rec.snapshot()) != callsBefore {
		t.Fatalf("actions invoked again on terminal retry: %v", rec.snapshot())
	}
}

func TestSameIdentityRetryDoesNotReinvokeConfirmedSteps(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &recorder{}
	def := threeStepDef(rec, "", "")
	req := ExecutionRequest{BusinessKey: "order-4", IdempotencyKey: "req-4"}

	res1, err := eng.Execute(context.Background(), def, req)
	if err != nil {
		t.Fatal(err)
	}
	eventsAfterFirst := store.TotalEvents()

	res2, err := eng.Execute(context.Background(), def, req)
	if err != nil {
		t.Fatal(err)
	}
	if res1.Status != StatusCompleted || res2.Status != StatusCompleted {
		t.Fatalf("statuses %s %s", res1.Status, res2.Status)
	}
	// 三个正向动作各只被调用一次，重试没有重复处理。
	if calls := rec.snapshot(); len(calls) != 3 {
		t.Fatalf("expected 3 action calls total, got %v", calls)
	}
	if store.TotalEvents() != eventsAfterFirst {
		t.Fatalf("duplicate events appended on retry: %d -> %d", eventsAfterFirst, store.TotalEvents())
	}
	// 结果中的 Attempts 也证明每步只处理一次。
	for _, so := range res2.Steps {
		if so.Attempts != 1 {
			t.Fatalf("step %s attempts=%d want 1", so.Name, so.Attempts)
		}
	}
}

func TestResumeRunningExecutionSkipsConfirmedStep(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &recorder{}
	def := threeStepDef(rec, "", "")

	// 模拟崩溃恢复：reserve 已确认成功（状态已提交），charge/ship 尚未执行。
	err := store.Commit(context.Background(), "order-5", "req-5", func(tx Tx) error {
		tx.Create(&ExecutionState{
			SagaName:    def.Name,
			Fingerprint: fingerprint(def),
			Status:      StatusRunning,
			Steps: []StepState{
				{Name: "reserve", Status: StepResultSucceeded, Attempts: 1},
				{Name: "charge"},
				{Name: "ship"},
			},
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey:    "order-5",
		IdempotencyKey: "req-5",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusCompleted {
		t.Fatalf("status=%s", res.Status)
	}
	if got := rec.snapshot(); fmt.Sprint(got) != "[do:charge do:ship]" {
		t.Fatalf("confirmed step must not be re-invoked, got %v", got)
	}
	if res.Steps[0].Attempts != 1 || res.Steps[1].Attempts != 1 {
		t.Fatalf("attempts = %d,%d", res.Steps[0].Attempts, res.Steps[1].Attempts)
	}
}

func TestResumeCompensatingSkipsConfirmedCompensations(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &recorder{}
	def := threeStepDef(rec, "", "")

	// ship 失败，reserve 已补偿；恢复时只能补偿 charge。
	err := store.Commit(context.Background(), "order-6", "req-6", func(tx Tx) error {
		tx.Create(&ExecutionState{
			SagaName:      def.Name,
			Fingerprint:   fingerprint(def),
			Status:        StatusCompensating,
			FailedStep:    "ship",
			FailureReason: "boom-ship",
			Steps: []StepState{
				{Name: "reserve", Status: StepResultCompensated, Attempts: 1},
				{Name: "charge", Status: StepResultSucceeded, Attempts: 1},
				{Name: "ship", Status: StepResultFailed, Attempts: 1, LastError: "boom-ship"},
			},
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey:    "order-6",
		IdempotencyKey: "req-6",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusFailed {
		t.Fatalf("status=%s want failed", res.Status)
	}
	if got := rec.snapshot(); fmt.Sprint(got) != "[undo:charge]" {
		t.Fatalf("only unconfirmed compensation should run, got %v", got)
	}
}

func TestDefinitionConflictSameBusinessKeyDifferentDefinition(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &recorder{}
	defV1 := threeStepDef(rec, "", "")
	defV2 := threeStepDef(rec, "", "")
	defV2.Version = "v2"

	if _, err := eng.Execute(context.Background(), defV1, ExecutionRequest{
		BusinessKey: "order-7", IdempotencyKey: "req-a",
	}); err != nil {
		t.Fatal(err)
	}
	// 同业务键 + 不同幂等键 + 不同定义 => 冲突。
	_, err := eng.Execute(context.Background(), defV2, ExecutionRequest{
		BusinessKey: "order-7", IdempotencyKey: "req-b",
	})
	if !errors.Is(err, ErrDefinitionConflict) {
		t.Fatalf("err=%v want ErrDefinitionConflict", err)
	}
	// 即使沿用同一执行身份，不同定义同样冲突。
	_, err = eng.Execute(context.Background(), defV2, ExecutionRequest{
		BusinessKey: "order-7", IdempotencyKey: "req-a",
	})
	if !errors.Is(err, ErrDefinitionConflict) {
		t.Fatalf("err=%v want ErrDefinitionConflict", err)
	}
	// 同业务键 + 同定义 + 不同幂等键 => 独立执行，允许。
	res, err := eng.Execute(context.Background(), defV1, ExecutionRequest{
		BusinessKey: "order-7", IdempotencyKey: "req-c",
	})
	if err != nil {
		t.Fatalf("same definition must be allowed: %v", err)
	}
	if res.Status != StatusCompleted {
		t.Fatalf("status=%s", res.Status)
	}
}

func TestDifferentBusinessKeysAreIndependent(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &recorder{}
	def := threeStepDef(rec, "charge", "")

	r1, err := eng.Execute(context.Background(), def, ExecutionRequest{BusinessKey: "k-a", IdempotencyKey: "idem"})
	if err != nil {
		t.Fatal(err)
	}
	r2, err := eng.Execute(context.Background(), def, ExecutionRequest{BusinessKey: "k-b", IdempotencyKey: "idem"})
	if err != nil {
		t.Fatal(err)
	}
	if r1.Status != StatusFailed || r2.Status != StatusFailed {
		t.Fatalf("statuses %s %s", r1.Status, r2.Status)
	}
	if rec.count("do:reserve") != 2 || rec.count("undo:reserve") != 2 {
		t.Fatalf("each business key must execute independently: %v", rec.snapshot())
	}
}

func TestGetResultUnknownExecution(t *testing.T) {
	eng := NewEngine(NewMemoryStore())
	if _, err := eng.GetResult(context.Background(), "order-saga", "nope", "nope"); !errors.Is(err, ErrExecutionNotFound) {
		t.Fatalf("err=%v want ErrExecutionNotFound", err)
	}
}

func TestInvalidDefinitionsAndRequests(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	valid := func() Definition {
		return Definition{Name: "d", Steps: []Step{{
			Name: "s", Action: func(context.Context, ExecutionView) error { return nil },
		}}}
	}

	cases := []struct {
		name string
		def  Definition
	}{
		{"empty name", func() Definition { d := valid(); d.Name = ""; return d }()},
		{"no steps", Definition{Name: "d"}},
		{"nil action", func() Definition { d := valid(); d.Steps[0].Action = nil; return d }()},
		{"blank step name", func() Definition { d := valid(); d.Steps[0].Name = "  "; return d }()},
		{"duplicate step names", func() Definition {
			d := valid()
			d.Steps = append(d.Steps, Step{Name: "s", Action: d.Steps[0].Action})
			return d
		}()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateDefinition(tc.def); !errors.Is(err, ErrInvalidDefinition) {
				t.Fatalf("err=%v", err)
			}
			if _, err := eng.Execute(context.Background(), tc.def, ExecutionRequest{
				BusinessKey: "b", IdempotencyKey: "i",
			}); !errors.Is(err, ErrInvalidDefinition) {
				t.Fatalf("Execute err=%v", err)
			}
		})
	}

	if _, err := eng.Execute(context.Background(), valid(), ExecutionRequest{
		BusinessKey: "b", IdempotencyKey: "",
	}); !errors.Is(err, ErrInvalidDefinition) {
		t.Fatalf("missing idempotency key must be ErrInvalidDefinition, got %v", err)
	}
	if _, err := eng.Execute(context.Background(), valid(), ExecutionRequest{
		BusinessKey: "  ", IdempotencyKey: "i",
	}); !errors.Is(err, ErrInvalidDefinition) {
		t.Fatalf("missing business key must be ErrInvalidDefinition, got %v", err)
	}
}

func TestOutboxRetryKeepsEventAndIncrementsDeliveries(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &recorder{}
	def := threeStepDef(rec, "", "")

	if _, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "order-8", IdempotencyKey: "req-8",
	}); err != nil {
		t.Fatal(err)
	}

	// 第一轮：事件被领取，但尚未 Ack/Nack 时不会重复领取。
	batch1, err := store.ClaimPendingEvents(context.Background(), 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch1) == 0 {
		t.Fatal("expected events")
	}
	again, _ := store.ClaimPendingEvents(context.Background(), 100)
	if len(again) != 0 {
		t.Fatalf("claimed events must be hidden from re-claim, got %d", len(again))
	}

	// 发送失败：退回全部原事件，负载不变，之后可再次领取且投递次数增加。
	ev := batch1[0]
	originalType := ev.Type
	for _, e := range batch1 {
		if err := store.NackEvent(context.Background(), e.ID); err != nil {
			t.Fatal(err)
		}
	}
	batch2, _ := store.ClaimPendingEvents(context.Background(), 100)
	if len(batch2) != len(batch1) {
		t.Fatalf("after nack all events reclaimable: got %d want %d", len(batch2), len(batch1))
	}
	var retried *Event
	for _, e := range batch2 {
		if e.ID == ev.ID {
			retried = e
		}
	}
	if retried == nil {
		t.Fatal("nacked event missing from reclaim")
	}
	if retried.Type != originalType {
		t.Fatalf("event payload/type altered: %s -> %s", originalType, retried.Type)
	}
	if retried.Deliveries() != 2 {
		t.Fatalf("deliveries=%d want 2", retried.Deliveries())
	}

	// 发送成功：标记已投递，事件不再出现。
	if err := store.AckEvent(context.Background(), retried.ID); err != nil {
		t.Fatal(err)
	}
	rest, _ := store.ClaimPendingEvents(context.Background(), 100)
	for _, e := range rest {
		_ = store.NackEvent(context.Background(), e.ID)
	}
	for _, e := range rest {
		if e.ID == retried.ID {
			t.Fatal("acked event must not be redelivered")
		}
	}
}

func TestRelayDeliversAndRetries(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &recorder{}
	def := threeStepDef(rec, "", "")
	if _, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "order-9", IdempotencyKey: "req-9",
	}); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	failFirst := map[string]bool{}
	var published []string
	pub := PublisherFunc(func(_ context.Context, ev Event) error {
		mu.Lock()
		defer mu.Unlock()
		if !failFirst[ev.ID] {
			failFirst[ev.ID] = true
			return errors.New("temporary publish failure")
		}
		published = append(published, ev.ID)
		return nil
	})
	relay := NewRelay(store, pub, WithBatch(2))

	// 第一轮：全部失败，事件保留。
	res, err := relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Delivered != 0 || res.Failed != res.Claimed {
		t.Fatalf("first round = %+v", res)
	}
	// DeliverOnce 在发布失败路径已自行 Nack，因此可直接重试至全部成功。
	for store.TotalEvents() > 0 {
		res, err := relay.DeliverOnce(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if res.Claimed == 0 {
			t.Fatal("events stuck: none claimable while events remain")
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(published) != 5 {
		t.Fatalf("published=%d want 5", len(published))
	}
}

func TestExecutionViewPayloadAndSucceededSteps(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)

	var seen []string
	var gotPayload any
	def := Definition{
		Name: "view-saga", Version: "v1",
		Steps: []Step{
			{
				Name: "a",
				Action: func(_ context.Context, ex ExecutionView) error {
					gotPayload = ex.Payload()
					seen = append(seen, ex.BusinessKey()+"/"+ex.IdempotencyKey())
					return nil
				},
			},
			{
				Name: "b",
				Action: func(_ context.Context, ex ExecutionView) error {
					seen = append(seen, strings.Join(ex.SucceededSteps(), ","))
					return nil
				},
			},
		},
	}
	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "bk", IdempotencyKey: "ik", Payload: "hello",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusCompleted {
		t.Fatal(res.Status)
	}
	if gotPayload != "hello" || seen[0] != "bk/ik" || seen[1] != "a" {
		t.Fatalf("view data wrong: %v payload=%v", seen, gotPayload)
	}
}

func TestContextCancellationLeavesStepUnconfirmed(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)

	started := make(chan struct{})
	release := make(chan struct{})
	var invocations int
	def := Definition{
		Name: "cancel-saga", Version: "v1",
		Steps: []Step{{
			Name: "slow",
			Action: func(ctx context.Context, _ ExecutionView) error {
				invocations++
				if invocations == 1 {
					// 首次调用被取消；动作遵守幂等契约，允许被重新调用。
					close(started)
					<-release
					return ctx.Err()
				}
				return nil
			},
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

	// 步骤未确认：执行仍为 running，事件只有 started。
	snap, err := store.LoadSnapshot(context.Background(), "bk", "ik")
	if err != nil {
		t.Fatal(err)
	}
	if snap.State.Status != StatusRunning || snap.State.Steps[0].Status != "" {
		t.Fatalf("uncommitted step must remain pending: %+v", snap.State.Steps[0])
	}

	// 以新 context 用同一身份重试：动作被重新调用并成功，每步 Attempts 最终为 1。
	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "bk", IdempotencyKey: "ik",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusCompleted || res.Steps[0].Attempts != 1 {
		t.Fatalf("retry result = %+v", res)
	}
}

func TestEventPayloadIsStructured(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &recorder{}
	def := threeStepDef(rec, "", "")
	if _, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "order-10", IdempotencyKey: "req-10",
	}); err != nil {
		t.Fatal(err)
	}
	events, _ := store.ClaimPendingEvents(context.Background(), 10)
	last := events[len(events)-1]
	p, ok := last.Payload.(EventPayload)
	if !ok {
		t.Fatalf("payload type %T", last.Payload)
	}
	if p.SagaName != "order-saga" || p.Status != StatusCompleted ||
		fmt.Sprint(p.SucceededSteps) != "[reserve charge ship]" {
		t.Fatalf("payload = %+v", p)
	}
}

func TestMemoryStoreCommitIsAtomicOnCallbackError(t *testing.T) {
	store := NewMemoryStore()
	wantErr := errors.New("callback abort")
	err := store.Commit(context.Background(), "bk", "ik", func(tx Tx) error {
		tx.Create(&ExecutionState{
			SagaName: "s", Fingerprint: "f", Status: StatusRunning,
			Steps: []StepState{{Name: "x"}},
		})
		tx.AppendEvent(EventExecutionStarted, EventPayload{SagaName: "s"})
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("err=%v", err)
	}
	snap, _ := store.LoadSnapshot(context.Background(), "bk", "ik")
	if snap.State != nil || snap.Bound {
		t.Fatal("state must not be visible after callback error")
	}
	if store.TotalEvents() != 0 {
		t.Fatalf("events must not be visible after callback error: %d", store.TotalEvents())
	}
}

func TestConcurrentSameIdentityExecutesOnce(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)

	var mu sync.Mutex
	calls := 0
	def := Definition{
		Name: "concurrent", Version: "v1",
		Steps: []Step{{
			Name: "only",
			Action: func(context.Context, ExecutionView) error {
				time.Sleep(10 * time.Millisecond)
				mu.Lock()
				calls++
				mu.Unlock()
				return nil
			},
		}},
	}
	var wg sync.WaitGroup
	results := make([]StatusResult, 5)
	errs := make([]error, 5)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, err := eng.Execute(context.Background(), def, ExecutionRequest{
				BusinessKey: "bk", IdempotencyKey: "ik",
			})
			results[i] = StatusResult{r.Status}
			errs[i] = err
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
		if results[i].status != StatusCompleted {
			t.Fatalf("goroutine %d status %s", i, results[i].status)
		}
	}
	if calls != 1 {
		t.Fatalf("action invoked %d times, want exactly 1", calls)
	}
}

type StatusResult struct{ status string }

func eventTypes(events []*Event) []string {
	out := make([]string, len(events))
	for i, e := range events {
		out[i] = e.Type
	}
	return out
}
