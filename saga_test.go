package saga_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	saga "txsaga"
)

// script 记录全部动作调用顺序，并可为指定步骤配置失败行为。
type script struct {
	mu          sync.Mutex
	calls       []string
	failForward map[string]error
	failComp    map[string]error
}

func newScript() *script {
	return &script{failForward: map[string]error{}, failComp: map[string]error{}}
}

func (s *script) callsCopy() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...)
}

func (s *script) def(name string, steps ...string) saga.Definition {
	d := saga.Definition{Name: name}
	for _, n := range steps {
		n := n
		d.Steps = append(d.Steps, saga.Step{
			Name:       n,
			Do:         s.forward(n),
			Compensate: s.compensate(n),
		})
	}
	return d
}

func (s *script) forward(name string) saga.Action {
	return func(context.Context) error {
		s.mu.Lock()
		s.calls = append(s.calls, "do:"+name)
		err := s.failForward[name]
		s.mu.Unlock()
		return err
	}
}

func (s *script) compensate(name string) saga.Compensation {
	return func(context.Context) error {
		s.mu.Lock()
		s.calls = append(s.calls, "undo:"+name)
		err := s.failComp[name]
		s.mu.Unlock()
		return err
	}
}

func threeStepDef(s *script) saga.Definition {
	return s.def("order", "reserve", "charge", "ship")
}

func drainEvents(t *testing.T, ctx context.Context, store saga.Store) []saga.OutboxEvent {
	t.Helper()
	var all []saga.OutboxEvent
	for {
		evs, err := store.ClaimEvents(ctx, 100, time.Minute)
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if len(evs) == 0 {
			return all
		}
		for _, ev := range evs {
			if err := store.MarkDelivered(ctx, ev.ID, ev.ClaimToken); err != nil {
				t.Fatalf("mark: %v", err)
			}
			all = append(all, ev)
		}
	}
}

func eventTypes(evs []saga.OutboxEvent) []saga.EventType {
	out := make([]saga.EventType, len(evs))
	for i, ev := range evs {
		out[i] = ev.Type
	}
	return out
}

func TestSuccessFlow(t *testing.T) {
	ctx := context.Background()
	s := newScript()
	store := saga.NewMemoryStore()
	engine := saga.NewEngine(store)

	res, err := engine.Execute(ctx, threeStepDef(s), saga.Request{BusinessKey: "bk-1", IdempotencyKey: "idem-1"})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.State != saga.StateCompleted {
		t.Fatalf("state = %s, want completed", res.State)
	}
	if res.FailureReason != "" {
		t.Fatalf("unexpected failure reason: %q", res.FailureReason)
	}
	wantCalls := []string{"do:reserve", "do:charge", "do:ship"}
	if got := s.callsCopy(); fmt.Sprint(got) != fmt.Sprint(wantCalls) {
		t.Fatalf("calls = %v, want %v", got, wantCalls)
	}
	for _, sr := range res.Steps {
		if sr.State != saga.StepSucceeded || sr.Attempts != 1 || sr.CompensationAttempts != 0 {
			t.Fatalf("step %s = %+v, want succeeded with 1 attempt", sr.Name, sr)
		}
	}

	evs := drainEvents(t, ctx, store)
	wantTypes := []saga.EventType{
		saga.EventExecutionStarted,
		saga.EventStepSucceeded, saga.EventStepSucceeded, saga.EventStepSucceeded,
		saga.EventExecutionCompleted,
	}
	if got := eventTypes(evs); fmt.Sprint(got) != fmt.Sprint(wantTypes) {
		t.Fatalf("events = %v, want %v", got, wantTypes)
	}
	// 事件字段完整且 ID 唯一。
	seen := map[string]bool{}
	for _, ev := range evs {
		if ev.ID == "" || seen[ev.ID] {
			t.Fatalf("bad/duplicate event id %q", ev.ID)
		}
		seen[ev.ID] = true
		if ev.BusinessKey != "bk-1" || ev.OccurredAt.IsZero() || len(ev.Payload) == 0 {
			t.Fatalf("event %q missing fields: %+v", ev.ID, ev)
		}
	}
}

func TestFailureCompensatesOnlyConfirmedStepsInReverse(t *testing.T) {
	ctx := context.Background()
	s := newScript()
	s.failForward["ship"] = errors.New("warehouse down")
	store := saga.NewMemoryStore()
	engine := saga.NewEngine(store)

	res, err := engine.Execute(ctx, threeStepDef(s), saga.Request{BusinessKey: "bk-2", IdempotencyKey: "idem-2"})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.State != saga.StateFailed {
		t.Fatalf("state = %s, want failed", res.State)
	}
	if res.FailureReason == "" || res.Steps[2].Error != "warehouse down" {
		t.Fatalf("failure reason not propagated: %+v", res)
	}
	// 失败步骤本身不补偿，仅此前确认成功的两步按逆序补偿。
	wantCalls := []string{"do:reserve", "do:charge", "do:ship", "undo:charge", "undo:reserve"}
	if got := s.callsCopy(); fmt.Sprint(got) != fmt.Sprint(wantCalls) {
		t.Fatalf("calls = %v, want %v", got, wantCalls)
	}
	wantStates := []saga.StepState{
		saga.StepCompensated, saga.StepCompensated, saga.StepFailed,
	}
	for i, want := range wantStates {
		if res.Steps[i].State != want {
			t.Fatalf("step %d state = %s, want %s", i, res.Steps[i].State, want)
		}
	}

	evs := drainEvents(t, ctx, store)
	wantTypes := []saga.EventType{
		saga.EventExecutionStarted,
		saga.EventStepSucceeded, saga.EventStepSucceeded,
		saga.EventStepFailed,
		saga.EventStepCompensated, saga.EventStepCompensated,
		saga.EventExecutionFailed,
	}
	if got := eventTypes(evs); fmt.Sprint(got) != fmt.Sprint(wantTypes) {
		t.Fatalf("events = %v, want %v", got, wantTypes)
	}
}

func TestFirstStepFailureReachesFailedTerminal(t *testing.T) {
	ctx := context.Background()
	s := newScript()
	s.failForward["reserve"] = errors.New("nope")
	store := saga.NewMemoryStore()
	engine := saga.NewEngine(store)

	res, err := engine.Execute(ctx, threeStepDef(s), saga.Request{BusinessKey: "bk", IdempotencyKey: "idem"})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.State != saga.StateFailed {
		t.Fatalf("state = %s, want failed", res.State)
	}
	if got := s.callsCopy(); fmt.Sprint(got) != fmt.Sprint([]string{"do:reserve"}) {
		t.Fatalf("calls = %v, want only the failed forward call", got)
	}
}

func TestCompensationFailureFreezesTerminal(t *testing.T) {
	ctx := context.Background()
	s := newScript()
	s.failForward["ship"] = errors.New("ship boom")
	s.failComp["charge"] = errors.New("refund boom") // 先补偿的 charge 失败
	store := saga.NewMemoryStore()
	engine := saga.NewEngine(store)
	req := saga.Request{BusinessKey: "bk-3", IdempotencyKey: "idem-3"}

	res, err := engine.Execute(ctx, threeStepDef(s), req)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.State != saga.StateCompensationFailed {
		t.Fatalf("state = %s, want compensation_failed", res.State)
	}
	// charge 补偿失败后冻结：reserve 仍 succeeded，未被补偿。
	if res.Steps[0].State != saga.StepSucceeded || res.Steps[1].State != saga.StepCompensationFailed {
		t.Fatalf("steps = %+v, want reserve succeeded / charge compensation_failed", res.Steps)
	}
	if res.FailureReason == "" {
		t.Fatalf("expected compensation failure reason")
	}
	callsAfterFirst := len(s.callsCopy())

	// 终态结果保持不变，后续调用直接返回，不新增动作或事件。
	res2, err := engine.Execute(ctx, threeStepDef(s), req)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if res2.State != saga.StateCompensationFailed || res2.FailureReason != res.FailureReason {
		t.Fatalf("terminal result changed: %+v vs %+v", res, res2)
	}
	if len(s.callsCopy()) != callsAfterFirst {
		t.Fatalf("terminal retry invoked actions: before=%d after=%d", callsAfterFirst, len(s.callsCopy()))
	}
}

func TestSameIdentityRetryIsIdempotent(t *testing.T) {
	ctx := context.Background()
	s := newScript()
	s.failForward["charge"] = errors.New("declined")
	store := saga.NewMemoryStore()
	engine := saga.NewEngine(store)
	req := saga.Request{BusinessKey: "bk-4", IdempotencyKey: "idem-4"}

	first, err := engine.Execute(ctx, threeStepDef(s), req)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	evsAfterFirst := drainEvents(t, ctx, store)
	callsAfterFirst := len(s.callsCopy())

	second, err := engine.Execute(ctx, threeStepDef(s), req)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if first.State != second.State || first.FailureReason != second.FailureReason {
		t.Fatalf("retry changed result: %+v vs %+v", first, second)
	}
	if len(s.callsCopy()) != callsAfterFirst {
		t.Fatalf("retry invoked actions: before=%d after=%d", callsAfterFirst, len(s.callsCopy()))
	}
	if more := drainEvents(t, ctx, store); len(more) != 0 {
		t.Fatalf("retry produced %d new events", len(more))
	}
	if len(evsAfterFirst) == 0 {
		t.Fatalf("expected events after first execution")
	}
}

func TestDifferentIdempotencyKeyIsIndependentExecution(t *testing.T) {
	ctx := context.Background()
	s := newScript()
	store := saga.NewMemoryStore()
	engine := saga.NewEngine(store)
	def := threeStepDef(s)

	r1, err := engine.Execute(ctx, def, saga.Request{BusinessKey: "bk-5", IdempotencyKey: "idem-a"})
	if err != nil {
		t.Fatalf("execute a: %v", err)
	}
	r2, err := engine.Execute(ctx, def, saga.Request{BusinessKey: "bk-5", IdempotencyKey: "idem-b"})
	if err != nil {
		t.Fatalf("execute b: %v", err)
	}
	if r1.State != saga.StateCompleted || r2.State != saga.StateCompleted {
		t.Fatalf("states = %s/%s", r1.State, r2.State)
	}
	// 同一业务键的两个独立执行各跑一遍。
	if got := len(s.callsCopy()); got != 6 {
		t.Fatalf("calls = %d, want 6 (two full executions)", got)
	}
}

func TestDefinitionConflict(t *testing.T) {
	ctx := context.Background()
	store := saga.NewMemoryStore()
	engine := saga.NewEngine(store)

	other := saga.Definition{
		Name:  "other-order",
		Steps: []saga.Step{{Name: "totally-different", Do: func(context.Context) error { return nil }}},
	}

	t.Run("same identity different definition", func(t *testing.T) {
		s := newScript()
		req := saga.Request{BusinessKey: "bk-c", IdempotencyKey: "idem-c"}
		if _, err := engine.Execute(ctx, threeStepDef(s), req); err != nil {
			t.Fatalf("first: %v", err)
		}
		if _, err := engine.Execute(ctx, other, req); !errors.Is(err, saga.ErrDefinitionConflict) {
			t.Fatalf("err = %v, want ErrDefinitionConflict", err)
		}
	})

	t.Run("same business key different identity and definition", func(t *testing.T) {
		s := newScript()
		if _, err := engine.Execute(ctx, threeStepDef(s), saga.Request{BusinessKey: "bk-d", IdempotencyKey: "idem-1"}); err != nil {
			t.Fatalf("first: %v", err)
		}
		if _, err := engine.Execute(ctx, other, saga.Request{BusinessKey: "bk-d", IdempotencyKey: "idem-2"}); !errors.Is(err, saga.ErrDefinitionConflict) {
			t.Fatalf("err = %v, want ErrDefinitionConflict", err)
		}
	})

	t.Run("same definition same business key binds fine", func(t *testing.T) {
		s := newScript()
		if _, err := engine.Execute(ctx, threeStepDef(s), saga.Request{BusinessKey: "bk-e", IdempotencyKey: "idem-1"}); err != nil {
			t.Fatalf("first: %v", err)
		}
		if _, err := engine.Execute(ctx, threeStepDef(s), saga.Request{BusinessKey: "bk-e", IdempotencyKey: "idem-2"}); err != nil {
			t.Fatalf("second same-def execution: %v", err)
		}
	})
}

func TestUnknownExecution(t *testing.T) {
	engine := saga.NewEngine(saga.NewMemoryStore())
	if _, err := engine.GetResult(context.Background(), "nope", "nope"); !errors.Is(err, saga.ErrExecutionNotFound) {
		t.Fatalf("err = %v, want ErrExecutionNotFound", err)
	}
}

func TestInvalidInputs(t *testing.T) {
	ctx := context.Background()
	engine := saga.NewEngine(saga.NewMemoryStore())
	okStep := saga.Step{Name: "x", Do: func(context.Context) error { return nil }}

	cases := []struct {
		name string
		def  saga.Definition
		req  saga.Request
	}{
		{"no steps", saga.Definition{Name: "d"}, saga.Request{BusinessKey: "b", IdempotencyKey: "i"}},
		{"blank step name", saga.Definition{Name: "d", Steps: []saga.Step{{Name: "  ", Do: okStep.Do}}}, saga.Request{BusinessKey: "b", IdempotencyKey: "i"}},
		{"nil action", saga.Definition{Name: "d", Steps: []saga.Step{{Name: "x"}}}, saga.Request{BusinessKey: "b", IdempotencyKey: "i"}},
		{"duplicate step names", saga.Definition{Name: "d", Steps: []saga.Step{okStep, okStep}}, saga.Request{BusinessKey: "b", IdempotencyKey: "i"}},
		{"missing business key", saga.Definition{Name: "d", Steps: []saga.Step{okStep}}, saga.Request{IdempotencyKey: "i"}},
		{"missing idempotency key", saga.Definition{Name: "d", Steps: []saga.Step{okStep}}, saga.Request{BusinessKey: "b"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := engine.Execute(ctx, tc.def, tc.req); !errors.Is(err, saga.ErrInvalidDefinition) {
				t.Fatalf("err = %v, want ErrInvalidDefinition", err)
			}
		})
	}
}

type pubFunc func(context.Context, saga.OutboxEvent) error

func (f pubFunc) Publish(ctx context.Context, ev saga.OutboxEvent) error { return f(ctx, ev) }

func TestOutboxFailureKeepsEventAndIncrementsAttempts(t *testing.T) {
	ctx := context.Background()
	s := newScript()
	store := saga.NewMemoryStore()
	engine := saga.NewEngine(store)
	if _, err := engine.Execute(ctx, threeStepDef(s), saga.Request{BusinessKey: "bk-6", IdempotencyKey: "idem-6"}); err != nil {
		t.Fatalf("execute: %v", err)
	}

	// 抖动 Publisher：每个事件首次发送失败、第二次成功。
	var mu sync.Mutex
	seen := map[string]int{} // 成功时观察到的既往投递次数
	failed := map[string]bool{}
	flaky := pubFunc(func(_ context.Context, ev saga.OutboxEvent) error {
		mu.Lock()
		defer mu.Unlock()
		if !failed[ev.ID] {
			failed[ev.ID] = true
			return errors.New("broker unavailable")
		}
		seen[ev.ID] = ev.Attempts
		return nil
	})

	// 首轮：5 条全部失败，事件保留并返回错误。
	n, err := saga.DeliverPending(ctx, store, flaky, 10, time.Minute)
	if err == nil || n != 0 {
		t.Fatalf("first round = (%d, %v), want 0 with error", n, err)
	}
	// 第二轮：失败后立即释放领取，同一事件可继续领取并发送成功。
	n, err = saga.DeliverPending(ctx, store, flaky, 10, time.Minute)
	if err != nil || n != 5 {
		t.Fatalf("second round = (%d, %v), want 5 nil", n, err)
	}
	for id, attempts := range seen {
		if attempts != 1 {
			t.Fatalf("event %s attempts at redelivery = %d, want 1", id, attempts)
		}
	}
	if more, _ := store.ClaimEvents(ctx, 10, time.Minute); len(more) != 0 {
		t.Fatalf("marked events still claimable: %d", len(more))
	}
}

func TestLeaseExpiryAllowsReclaim(t *testing.T) {
	ctx := context.Background()
	s := newScript()
	store := saga.NewMemoryStore()
	engine := saga.NewEngine(store)
	if _, err := engine.Execute(ctx, s.def("d", "a"), saga.Request{BusinessKey: "bk", IdempotencyKey: "idem"}); err != nil {
		t.Fatalf("execute: %v", err)
	}

	first, err := store.ClaimEvents(ctx, 10, time.Millisecond)
	if err != nil || len(first) == 0 {
		t.Fatalf("first claim: %v %d", err, len(first))
	}
	// 租约内不可再领取。
	if blocked, _ := store.ClaimEvents(ctx, 10, time.Minute); len(blocked) != 0 {
		t.Fatalf("claimed event available before lease expiry")
	}
	time.Sleep(3 * time.Millisecond)
	// 租约过期：旧令牌失效，事件可被新领取者接管。
	second, _ := store.ClaimEvents(ctx, 10, time.Minute)
	if len(second) != len(first) {
		t.Fatalf("after lease expiry claimed %d, want %d", len(second), len(first))
	}
	if err := store.MarkDelivered(ctx, first[0].ID, first[0].ClaimToken); !errors.Is(err, saga.ErrAlreadyClaimed) {
		t.Fatalf("stale token mark = %v, want ErrAlreadyClaimed", err)
	}
	if err := store.MarkDelivered(ctx, second[0].ID, second[0].ClaimToken); err != nil {
		t.Fatalf("new token mark: %v", err)
	}
}

func TestConcurrentSameIdentityExecutesStepsOnce(t *testing.T) {
	ctx := context.Background()
	s := newScript()
	store := saga.NewMemoryStore()
	engine := saga.NewEngine(store)
	def := s.def("slow", "a", "b", "c")
	// 让动作稍慢，制造并发窗口。
	for i := range def.Steps {
		orig := def.Steps[i].Do
		def.Steps[i].Do = func(ctx context.Context) error {
			time.Sleep(2 * time.Millisecond)
			return orig(ctx)
		}
	}

	const n = 12
	var wg sync.WaitGroup
	results := make([]saga.Result, n)
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			results[i], errs[i] = engine.Execute(ctx, def, saga.Request{BusinessKey: "bk-par", IdempotencyKey: "idem-par"})
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
		if results[i].State != saga.StateCompleted {
			t.Fatalf("goroutine %d state = %s", i, results[i].State)
		}
	}
	if got := len(s.callsCopy()); got != 3 {
		t.Fatalf("forward invocations under concurrency = %d, want 3", got)
	}
}

func TestStepCommittedBeforeNextRuns(t *testing.T) {
	ctx := context.Background()
	store := saga.NewMemoryStore()
	engine := saga.NewEngine(store)
	s := newScript()
	def := threeStepDef(s)

	// 第二步执行开始前，第一步必须已提交可查。
	seen := make(chan error, 1)
	orig := def.Steps[1].Do
	def.Steps[1].Do = func(ctx context.Context) error {
		res, err := engine.GetResult(ctx, "bk-7", "idem-7")
		if err != nil {
			seen <- err
			return err
		}
		if res.Steps[0].State != saga.StepSucceeded || res.Steps[1].State != saga.StepPending {
			seen <- fmt.Errorf("unexpected states during step 2: %+v", res.Steps)
			return nil
		}
		seen <- nil
		return orig(ctx)
	}
	if _, err := engine.Execute(ctx, def, saga.Request{BusinessKey: "bk-7", IdempotencyKey: "idem-7"}); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if err := <-seen; err != nil {
		t.Fatal(err)
	}
}

func TestNilCompensationTreatedAsSuccess(t *testing.T) {
	ctx := context.Background()
	calls := []string{}
	var mu sync.Mutex
	def := saga.Definition{Name: "d", Steps: []saga.Step{
		{Name: "a", Do: func(context.Context) error { mu.Lock(); calls = append(calls, "a"); mu.Unlock(); return nil }, Compensate: func(context.Context) error { mu.Lock(); calls = append(calls, "undo-a"); mu.Unlock(); return nil }},
		{Name: "b", Do: func(context.Context) error { mu.Lock(); calls = append(calls, "b"); mu.Unlock(); return nil }}, // 无补偿
		{Name: "c", Do: func(context.Context) error { return errors.New("c fail") }},
	}}
	engine := saga.NewEngine(saga.NewMemoryStore())
	res, err := engine.Execute(ctx, def, saga.Request{BusinessKey: "bk", IdempotencyKey: "idem"})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.State != saga.StateFailed {
		t.Fatalf("state = %s", res.State)
	}
	if res.Steps[1].State != saga.StepCompensated || res.Steps[1].CompensationAttempts != 0 {
		t.Fatalf("nil-comp step = %+v", res.Steps[1])
	}
}
