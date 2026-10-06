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

// graphHarness 记录每个动作实际并发执行时观察到的在途步骤。
type graphHarness struct {
	mu       sync.Mutex
	running  map[string]bool
	maxCon   int
	overlaps [][]string
	calls    []string
}

func newGraphHarness() *graphHarness {
	return &graphHarness{running: map[string]bool{}}
}

func (h *graphHarness) enter(name string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.running[name] = true
	h.calls = append(h.calls, name)
	var active []string
	for n := range h.running {
		active = append(active, n)
	}
	if len(active) > 1 {
		h.overlaps = append(h.overlaps, append([]string(nil), active...))
	}
	if len(active) > h.maxCon {
		h.maxCon = len(active)
	}
}

func (h *graphHarness) leave(name string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.running, name)
}

func (h *graphHarness) callCount(name string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, c := range h.calls {
		if c == name {
			n++
		}
	}
	return n
}

func (h *graphHarness) overlap() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.overlaps) > 0
}

// graphDef 构建一个菱形依赖图：
//
//	a (root)
//	├─ b (dep a)
//	└─ c (dep a)
//	   d (dep b, c)
//
// failStep 指定预算耗尽失败的步骤名；各步均带同名补偿（compFail 指定补偿失败）。
func diamondDef(h *graphHarness, gates map[string]chan struct{}, failStep string) Definition {
	mk := func(name string, deps ...string) Step {
		return Step{
			Name:      name,
			DependsOn: deps,
			Action: func(_ context.Context, _ ExecutionView) error {
				h.enter(name)
				if g := gates[name]; g != nil {
					<-g
				}
				err := errors.New("boom-" + name)
				h.leave(name)
				if name == failStep {
					return err
				}
				return nil
			},
			Compensate: func(_ context.Context, _ ExecutionView) error {
				h.enter("undo:" + name)
				h.leave("undo:" + name)
				return nil
			},
		}
	}
	return Definition{
		Name:    "diamond",
		Version: "v1",
		Steps: []Step{
			mk("a"),
			mk("b", "a"),
			mk("c", "a"),
			mk("d", "b", "c"),
		},
	}
}

func TestGraphIndependentStepsRunConcurrently(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	h := newGraphHarness()

	releaseB := make(chan struct{})
	releaseC := make(chan struct{})
	bStarted := make(chan struct{})
	cStarted := make(chan struct{})
	var bOnce, cOnce sync.Once
	plain := diamondDef(h, nil, "")
	// 为 b、c 安装闸门以观察并发，并在确认进入后回报启动。
	for i := range plain.Steps {
		name := plain.Steps[i].Name
		if name != "b" && name != "c" {
			continue
		}
		plain.Steps[i].Action = func(_ context.Context, _ ExecutionView) error {
			h.enter(name)
			switch name {
			case "b":
				bOnce.Do(func() { close(bStarted) })
				<-releaseB
			case "c":
				cOnce.Do(func() { close(cStarted) })
				<-releaseC
			}
			h.leave(name)
			return nil
		}
	}
	def := plain

	done := make(chan Result)
	go func() {
		res, err := eng.Execute(context.Background(), def, ExecutionRequest{
			BusinessKey: "dag-1", IdempotencyKey: "req-1",
		})
		if err != nil {
			t.Errorf("execute: %v", err)
			close(done)
			return
		}
		done <- res
	}()

	select {
	case <-bStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("b never started")
	}
	select {
	case <-cStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("c never started (b and c must run concurrently)")
	}
	close(releaseB)
	close(releaseC)

	res := <-done
	if res.Status != StatusCompleted || !res.Terminal {
		t.Fatalf("status=%s terminal=%v", res.Status, res.Terminal)
	}
	if !h.overlap() {
		t.Fatal("expected b and c to overlap in time")
	}
	if h.maxCon < 2 {
		t.Fatalf("max concurrency=%d, want >=2", h.maxCon)
	}
	// 声明顺序的结果视图不变。
	if names := stepNames(res.Steps); fmt.Sprint(names) != "[a b c d]" {
		t.Fatalf("step order = %v", names)
	}
}

func TestGraphCompensatesInConfirmationOrder(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	h := newGraphHarness()
	// 叶子 d 失败：a、b、c 均已确认，须补偿；d 自身未确认，不补偿。
	def := diamondDef(h, nil, "d")

	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "dag-2", IdempotencyKey: "req-2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusFailed || !res.Terminal {
		t.Fatalf("status=%s want failed", res.Status)
	}
	if res.FailedStep != "d" || res.FailureReason != "boom-d" {
		t.Fatalf("failure info: %+v", res)
	}
	results := map[string]string{}
	for _, so := range res.Steps {
		results[so.Name] = so.Result
	}
	for _, n := range []string{"a", "b", "c"} {
		if results[n] != StepResultCompensated {
			t.Fatalf("step %s = %s want compensated", n, results[n])
		}
	}
	if results["d"] != StepResultFailed {
		t.Fatalf("d = %s want failed", results["d"])
	}

	// 事件链：started，a/b/c/d 成功提交（d 为失败），随后逆确认顺序补偿，最后 failed。
	events, _ := store.ClaimPendingEvents(context.Background(), 100)
	var types []string
	var compOrder []string
	var stepSucceeded []string
	for _, ev := range events {
		types = append(types, ev.Type)
		p := ev.Payload.(EventPayload)
		if ev.Type == EventStepCompensated {
			compOrder = append(compOrder, p.Step)
		}
		if ev.Type == EventStepSucceeded {
			stepSucceeded = append(stepSucceeded, p.Step)
		}
	}
	// a 先确认；b、c 同波并发，按声明顺序提交；d 是失败而非成功。
	if fmt.Sprint(stepSucceeded) != "[a b c]" {
		t.Fatalf("succeeded events = %v", stepSucceeded)
	}
	// 补偿顺序必须是确认顺序的逆序（b、c 同波，按声明提交顺序的逆序）。
	if fmt.Sprint(compOrder) != "[c b a]" {
		t.Fatalf("compensation order = %v want [c b a]", compOrder)
	}
	if types[0] != EventExecutionStarted || types[len(types)-1] != EventExecutionFailed {
		t.Fatalf("event chain endpoints wrong: %v", types)
	}
}

func TestGraphSiblingFailureStillCommitsSucceededSiblings(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	h := newGraphHarness()
	// b 与 c 同波：b 失败，c 成功。c 已启动，其成功结果必须提交并纳入补偿；
	// 尚未启动的 d 保持 pending。
	def := diamondDef(h, nil, "b")

	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "dag-3", IdempotencyKey: "req-3",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusFailed {
		t.Fatalf("status=%s want failed", res.Status)
	}
	results := map[string]string{}
	for _, so := range res.Steps {
		results[so.Name] = so.Result
	}
	if results["a"] != StepResultCompensated {
		t.Fatalf("a=%s want compensated", results["a"])
	}
	if results["c"] != StepResultCompensated {
		t.Fatalf("c=%s want compensated: succeeded sibling must be committed and compensated", results["c"])
	}
	if results["b"] != StepResultFailed {
		t.Fatalf("b=%s want failed", results["b"])
	}
	if results["d"] != StepResultPending {
		t.Fatalf("d=%s want pending (never scheduled)", results["d"])
	}
	if got := h.callCount("d"); got != 0 {
		t.Fatalf("d action invoked %d times, must never start", got)
	}
	if got := h.callCount("undo:d"); got != 0 {
		t.Fatalf("d compensated despite never succeeding")
	}
	// c 在 b 之后确认，补偿时 c 先于 a。
	events, _ := store.ClaimPendingEvents(context.Background(), 100)
	var compOrder []string
	for _, ev := range events {
		if ev.Type == EventStepCompensated {
			compOrder = append(compOrder, ev.Payload.(EventPayload).Step)
		}
	}
	if fmt.Sprint(compOrder) != "[c a]" {
		t.Fatalf("comp order = %v want [c a]", compOrder)
	}
}

func TestGraphDownstreamWaitsForAllPrerequisites(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	h := newGraphHarness()

	aDone := make(chan struct{})
	bDone := make(chan struct{})
	var dStarted int32
	def := Definition{
		Name: "prereq", Version: "v1",
		Steps: []Step{
			{Name: "a", Action: gateActionFn("a", h, aDone)},
			{Name: "b", Action: gateActionFn("b", h, bDone)},
			{
				Name:      "d",
				DependsOn: []string{"a", "b"},
				Action: func(ctx context.Context, ev ExecutionView) error {
					atomic.StoreInt32(&dStarted, 1)
					for _, p := range []string{"a", "b"} {
						found := false
						for _, s := range ev.SucceededSteps() {
							if s == p {
								found = true
							}
						}
						if !found {
							t.Errorf("d started before prerequisite %s confirmed", p)
						}
					}
					h.enter("d")
					h.leave("d")
					return nil
				},
			},
		},
	}

	done := make(chan struct{})
	go func() {
		if _, err := eng.Execute(context.Background(), def, ExecutionRequest{
			BusinessKey: "dag-4", IdempotencyKey: "req-4",
		}); err != nil {
			t.Error(err)
		}
		close(done)
	}()

	// 仅释放 a：d 仍须等待 b，不得启动。
	close(aDone)
	time.Sleep(50 * time.Millisecond)
	if atomic.LoadInt32(&dStarted) != 0 {
		t.Fatal("d started while b unconfirmed")
	}
	close(bDone)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("execute hung")
	}
	if atomic.LoadInt32(&dStarted) != 1 {
		t.Fatal("d never ran")
	}
}

func gateActionFn(name string, h *graphHarness, gate chan struct{}) ActionFunc {
	return func(context.Context, ExecutionView) error {
		h.enter(name)
		<-gate
		h.leave(name)
		return nil
	}
}

func TestGraphSameIdentityRetrySkipsConfirmedSteps(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	h := newGraphHarness()
	def := diamondDef(h, nil, "")
	req := ExecutionRequest{BusinessKey: "dag-5", IdempotencyKey: "req-5"}

	r1, err := eng.Execute(context.Background(), def, req)
	if err != nil {
		t.Fatal(err)
	}
	if r1.Status != StatusCompleted {
		t.Fatalf("status=%s", r1.Status)
	}
	eventsAfterFirst := store.TotalEvents()

	r2, err := eng.Execute(context.Background(), def, req)
	if err != nil {
		t.Fatal(err)
	}
	if r2.Status != StatusCompleted {
		t.Fatalf("retry status=%s", r2.Status)
	}
	for _, n := range []string{"a", "b", "c", "d"} {
		if got := h.callCount(n); got != 1 {
			t.Fatalf("step %s invoked %d times across retries, want 1", n, got)
		}
	}
	if store.TotalEvents() != eventsAfterFirst {
		t.Fatalf("events appended on retry: %d -> %d", eventsAfterFirst, store.TotalEvents())
	}
}

func TestGraphResumeOnlyReentersUnconfirmed(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	h := newGraphHarness()
	// a、c 已确认；b 未确认；d 依赖 b、c，尚不能启动。
	def := diamondDef(h, nil, "")
	err := store.Commit(context.Background(), "dag-6", "req-6", func(tx Tx) error {
		tx.Create(&ExecutionState{
			SagaName:       def.Name,
			Fingerprint:    fingerprint(def),
			Status:         StatusRunning,
			SucceededOrder: []string{"a", "c"},
			Steps: []StepState{
				{Name: "a", Status: StepResultSucceeded, Attempts: 1, FinishedAt: time.Now()},
				{Name: "b"},
				{Name: "c", Status: StepResultSucceeded, Attempts: 1, FinishedAt: time.Now()},
				{Name: "d"},
			},
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "dag-6", IdempotencyKey: "req-6",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusCompleted {
		t.Fatalf("status=%s", res.Status)
	}
	if h.callCount("a") != 0 || h.callCount("c") != 0 {
		t.Fatalf("confirmed steps re-invoked: a=%d c=%d", h.callCount("a"), h.callCount("c"))
	}
	if h.callCount("b") != 1 || h.callCount("d") != 1 {
		t.Fatalf("b=%d d=%d, want each 1", h.callCount("b"), h.callCount("d"))
	}
	// d 在 b 确认之后才启动。
	var bi, di int
	for i, n := range h.callsSnapshot() {
		switch n {
		case "b":
			bi = i
		case "d":
			di = i
		}
	}
	if di <= bi {
		t.Fatalf("d (idx %d) must run after b (idx %d): %v", di, bi, h.callsSnapshot())
	}
}

func (h *graphHarness) callsSnapshot() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.calls...)
}

func TestGraphCancellationLeavesWaveUnconfirmed(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	h := newGraphHarness()

	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var invocations int32
	slow := func() ActionFunc {
		return func(ctx context.Context, _ ExecutionView) error {
			h.enter("slow")
			n := atomic.AddInt32(&invocations, 1)
			if n == 1 {
				once.Do(func() { close(started) })
				<-release
				h.leave("slow")
				return ctx.Err()
			}
			h.leave("slow")
			return nil
		}
	}
	fast := func(name string) ActionFunc {
		return func(context.Context, ExecutionView) error {
			h.enter(name)
			h.leave(name)
			return nil
		}
	}
	// slow 与 fast 同为根步骤、无依赖关系，同波并发。
	def := Definition{
		Name: "dag-cancel", Version: "v1",
		Steps: []Step{
			{Name: "slow", Action: slow()},
			{Name: "fast", Action: fast("fast")},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-started
		cancel()
		close(release)
	}()
	if _, err := eng.Execute(ctx, def, ExecutionRequest{
		BusinessKey: "dag-7", IdempotencyKey: "req-7",
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v want context.Canceled", err)
	}

	// 取消时该波次尚未提交任何结果：两步都保持 pending，事件只有 started。
	snap, err := store.LoadSnapshot(context.Background(), "dag-7", "req-7")
	if err != nil {
		t.Fatal(err)
	}
	if snap.State.Status != StatusRunning {
		t.Fatalf("status=%s want running", snap.State.Status)
	}
	for _, s := range snap.State.Steps {
		if s.Status != "" {
			t.Fatalf("step %s committed as %s on cancellation", s.Name, s.Status)
		}
	}
	if store.TotalEvents() != 1 {
		t.Fatalf("events=%d want only started", store.TotalEvents())
	}

	// 同身份用新 context 继续：两个根步骤重新进入，最终完成。
	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "dag-7", IdempotencyKey: "req-7",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusCompleted {
		t.Fatalf("resume status=%s", res.Status)
	}
}

func TestGraphInvalidDefinitions(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	act := func(context.Context, ExecutionView) error { return nil }
	mk := func(deps ...string) Step {
		return Step{Name: "placeholder", Action: act, DependsOn: deps}
	}

	cases := []struct {
		name  string
		steps []Step
	}{
		{"unknown dependency", func() []Step {
			a := mk()
			a.Name = "a"
			b := mk("ghost")
			b.Name = "b"
			return []Step{a, b}
		}()},
		{"self dependency", func() []Step {
			a := mk("a")
			a.Name = "a"
			return []Step{a}
		}()},
		{"duplicate dependency", func() []Step {
			a := mk()
			a.Name = "a"
			b := mk("a", "a")
			b.Name = "b"
			return []Step{a, b}
		}()},
		{"cycle", func() []Step {
			a := mk("b")
			a.Name = "a"
			b := mk("a")
			b.Name = "b"
			return []Step{a, b}
		}()},
		{"long cycle", func() []Step {
			a := mk("c")
			a.Name = "a"
			b := mk("a")
			b.Name = "b"
			c := mk("b")
			c.Name = "c"
			return []Step{a, b, c}
		}()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			def := Definition{Name: "bad", Steps: tc.steps}
			if err := ValidateDefinition(def); !errors.Is(err, ErrInvalidDefinition) {
				t.Fatalf("ValidateDefinition err=%v", err)
			}
			eventsBefore := store.TotalEvents()
			if _, err := eng.Execute(context.Background(), def, ExecutionRequest{
				BusinessKey: "bad-bk", IdempotencyKey: "bad-ik",
			}); !errors.Is(err, ErrInvalidDefinition) {
				t.Fatalf("Execute err=%v", err)
			}
			if store.TotalEvents() != eventsBefore {
				t.Fatal("events appended for invalid definition")
			}
			if snap, _ := store.LoadSnapshot(context.Background(), "bad-bk", "bad-ik"); snap.State != nil || snap.Bound {
				t.Fatal("execution created for invalid definition")
			}
		})
	}
}

func TestGraphDependencyChangeIsDefinitionConflict(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	act := func(context.Context, ExecutionView) error { return nil }
	v1 := Definition{Name: "evolve", Version: "v1", Steps: []Step{
		{Name: "a", Action: act},
		{Name: "b", Action: act},
	}}
	v2 := Definition{Name: "evolve", Version: "v1", Steps: []Step{
		{Name: "a", Action: act},
		{Name: "b", Action: act, DependsOn: []string{"a"}},
	}}
	if _, err := eng.Execute(context.Background(), v1, ExecutionRequest{
		BusinessKey: "bk", IdempotencyKey: "i1",
	}); err != nil {
		t.Fatal(err)
	}
	// 同名同版本但依赖集合不同：指纹不同，同业务键冲突。
	if _, err := eng.Execute(context.Background(), v2, ExecutionRequest{
		BusinessKey: "bk", IdempotencyKey: "i2",
	}); !errors.Is(err, ErrDefinitionConflict) {
		t.Fatalf("err=%v want ErrDefinitionConflict", err)
	}
}

func TestGraphSequentialModeBehaviorUnchanged(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &recorder{}
	def := threeStepDef(rec, "", "")
	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "seq", IdempotencyKey: "req",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusCompleted {
		t.Fatalf("status=%s", res.Status)
	}
	if got := rec.snapshot(); fmt.Sprint(got) != "[do:reserve do:charge do:ship]" {
		t.Fatalf("calls=%v", got)
	}
	// SucceededOrder 在顺序模式下与声明顺序一致。
	snap, _ := store.LoadSnapshot(context.Background(), "seq", "req")
	if got := fmt.Sprint(snap.State.SucceededOrder); got != "[reserve charge ship]" {
		t.Fatalf("succeeded order = %s", got)
	}
	events, _ := store.ClaimPendingEvents(context.Background(), 100)
	want := []string{
		EventExecutionStarted,
		EventStepSucceeded, EventStepSucceeded, EventStepSucceeded,
		EventExecutionCompleted,
	}
	if len(events) != len(want) {
		t.Fatalf("events=%v", eventTypes(events))
	}
	for i, ev := range events {
		if ev.Type != want[i] {
			t.Fatalf("event[%d]=%s want %s", i, ev.Type, want[i])
		}
	}
}

func TestGraphCompensationFailureIsTerminal(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	act := func(context.Context, ExecutionView) error { return nil }
	failAct := func(name string) ActionFunc {
		return func(context.Context, ExecutionView) error { return errors.New("boom-" + name) }
	}
	def := Definition{
		Name: "dag-compfail", Version: "v1",
		Steps: []Step{
			{Name: "a", Action: act, Compensate: func(context.Context, ExecutionView) error {
				return errors.New("comp-boom-a")
			}},
			{Name: "b", Action: failAct("b")},
		},
	}
	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "dag-8", IdempotencyKey: "req-8",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusCompensationFailed || !res.Terminal {
		t.Fatalf("status=%s want compensation_failed", res.Status)
	}
	if res.FailedCompensationStep != "a" || res.CompensationError != "comp-boom-a" {
		t.Fatalf("comp info %+v", res)
	}
	eventsBefore := store.TotalEvents()
	res2, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "dag-8", IdempotencyKey: "req-8",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res2.Status != StatusCompensationFailed {
		t.Fatalf("terminal changed: %s", res2.Status)
	}
	if store.TotalEvents() != eventsBefore {
		t.Fatal("events appended on terminal retry")
	}
}

func TestGraphNoCompensationStepTreatedSucceeded(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	act := func(context.Context, ExecutionView) error { return nil }
	failAct := func(context.Context, ExecutionView) error { return errors.New("boom-c") }
	def := Definition{
		Name: "dag-nocomp", Version: "v1",
		Steps: []Step{
			{Name: "a", Action: act}, // 无补偿：视为补偿成功
			{Name: "b", Action: act, Compensate: func(context.Context, ExecutionView) error { return nil }},
			{Name: "c", Action: failAct},
		},
	}
	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "dag-9", IdempotencyKey: "req-9",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusFailed {
		t.Fatalf("status=%s want failed", res.Status)
	}
	results := map[string]string{}
	for _, so := range res.Steps {
		results[so.Name] = so.Result
	}
	if results["a"] != StepResultCompensated || results["b"] != StepResultCompensated {
		t.Fatalf("results=%v", results)
	}
}

func TestGraphSucceededStepsSnapshotInEvents(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	h := newGraphHarness()
	def := diamondDef(h, nil, "")
	if _, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "dag-10", IdempotencyKey: "req-10",
	}); err != nil {
		t.Fatal(err)
	}
	events, _ := store.ClaimPendingEvents(context.Background(), 100)
	// b 成功事件的快照应包含其确认时已确认的步骤（a 与 b；c 是否在同波先
	// 提交取决于声明顺序，b 在 c 前，因此快照为 [a b]）；d 为最后一个，
	// 快照为完整确认顺序。
	for _, ev := range events {
		p := ev.Payload.(EventPayload)
		if ev.Type == EventStepSucceeded && p.Step == "d" {
			if got := fmt.Sprint(p.SucceededSteps); got != "[a b c d]" {
				t.Fatalf("d snapshot=%s", got)
			}
		}
		if ev.Type == EventExecutionCompleted {
			if got := fmt.Sprint(p.SucceededSteps); got != "[a b c d]" {
				t.Fatalf("completed snapshot=%s", got)
			}
		}
	}
	// completed 事件负载可序列化且顺序即确认顺序。
}

func TestGraphRetryWithinStepNoIntermediateEvents(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	var attempts int32
	def := Definition{
		Name: "dag-retry", Version: "v1",
		Steps: []Step{
			{Name: "a", Action: func(context.Context, ExecutionView) error { return nil }},
			{
				Name:      "b",
				DependsOn: []string{"a"},
				ActionRetry: RetryPolicy{
					MaxAttempts: 3,
					RetryWait:   time.Millisecond,
				},
				Action: func(context.Context, ExecutionView) error {
					if atomic.AddInt32(&attempts, 1) < 3 {
						return errors.New("transient")
					}
					return nil
				},
			},
		},
	}
	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "dag-11", IdempotencyKey: "req-11",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusCompleted {
		t.Fatalf("status=%s", res.Status)
	}
	var b StepOutcome
	for _, so := range res.Steps {
		if so.Name == "b" {
			b = so
		}
	}
	if b.Attempts != 3 {
		t.Fatalf("b attempts=%d want 3", b.Attempts)
	}
	events, _ := store.ClaimPendingEvents(context.Background(), 100)
	// started -> a succeeded -> b succeeded -> completed：中间失败不追加事件。
	want := []string{
		EventExecutionStarted,
		EventStepSucceeded,
		EventStepSucceeded,
		EventExecutionCompleted,
	}
	if len(events) != len(want) {
		t.Fatalf("events=%v", eventTypes(events))
	}
	for i, ev := range events {
		if ev.Type != want[i] {
			t.Fatalf("event[%d]=%s want %s", i, ev.Type, want[i])
		}
	}
}

func stepNames(outcomes []StepOutcome) []string {
	names := make([]string, len(outcomes))
	for i, o := range outcomes {
		names[i] = o.Name
	}
	return names
}
