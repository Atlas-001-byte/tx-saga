package filestore

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/txsaga/txsaga"
)

// failNTimes returns an action that fails the first n calls then succeeds.
func failNTimes(n int, calls *int) txsaga.ActionFunc {
	return func(_ context.Context, _ txsaga.ExecutionView) error {
		*calls++
		if *calls <= n {
			return errors.New("transient")
		}
		return nil
	}
}

func alwaysFail(calls *int) txsaga.ActionFunc {
	return func(_ context.Context, _ txsaga.ExecutionView) error {
		*calls++
		return errors.New("boom")
	}
}

func okAction(calls *int) txsaga.ActionFunc {
	return func(_ context.Context, _ txsaga.ExecutionView) error {
		*calls++
		return nil
	}
}

func compAction(calls *int) txsaga.CompensationFunc {
	return func(_ context.Context, _ txsaga.ExecutionView) error {
		*calls++
		return nil
	}
}

// TestEngine_ReopenContinuesAndReadsTerminal 验证规格主线：一次 Execute 的
// 状态变更与事件整体落盘；写入成功后重新打开目录仍可继续执行、查询终态
// 并读取完整历史。
func TestEngine_ReopenContinuesAndReadsTerminal(t *testing.T) {
	dir := storeDir(t)

	// 第一次打开：一个成功 Saga，中间步骤带一次重试。
	midCalls := 0
	def := txsaga.Definition{
		Name: "order", Version: "v1",
		Steps: []txsaga.Step{
			{Name: "reserve", Action: okAction(new(int)), Compensate: compAction(new(int))},
			{Name: "charge", Action: failNTimes(1, &midCalls),
				ActionRetry: txsaga.RetryPolicy{MaxAttempts: 3}},
			{Name: "ship", Action: okAction(new(int))},
		},
	}

	s1, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	eng1 := txsaga.NewEngine(s1)
	res, err := eng1.Execute(context.Background(), def, txsaga.ExecutionRequest{
		BusinessKey: "O-1", IdempotencyKey: "req-1",
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !res.Terminal || res.Status != txsaga.StatusCompleted {
		t.Fatalf("status=%s terminal=%v want completed", res.Status, res.Terminal)
	}
	if res.Steps[1].Attempts != 2 {
		t.Fatalf("charge attempts=%d want 2", res.Steps[1].Attempts)
	}
	// started + 3*succeeded + completed = 5
	if err := drainAll(s1); err != nil {
		t.Fatal(err)
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	// 重新打开：终态可查询，同身份 Execute 直接返回终态且不重复动作，
	// 历史完整（含已 Ack 事件的审计副本）。
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	eng2 := txsaga.NewEngine(s2)

	got, err := eng2.GetResult(context.Background(), "order", "O-1", "req-1")
	if err != nil {
		t.Fatalf("GetResult after reopen: %v", err)
	}
	if got.Status != txsaga.StatusCompleted || !got.Terminal {
		t.Fatalf("terminal state lost: %+v", got)
	}
	if got.Steps[1].Attempts != 2 {
		t.Fatalf("attempts lost after reopen: %d", got.Steps[1].Attempts)
	}

	again, err := eng2.Execute(context.Background(), def, txsaga.ExecutionRequest{
		BusinessKey: "O-1", IdempotencyKey: "req-1",
	})
	if err != nil {
		t.Fatalf("idempotent Execute after reopen: %v", err)
	}
	if !again.Terminal || again.Status != txsaga.StatusCompleted {
		t.Fatalf("re-execute status=%s", again.Status)
	}
	if midCalls != 2 {
		t.Fatalf("terminal re-execute invoked action again: calls=%d", midCalls)
	}

	page := allHistory(t, eng2, "order", "O-1", "req-1")
	wantTypes := []string{
		txsaga.EventExecutionStarted,
		txsaga.EventStepSucceeded,
		txsaga.EventStepSucceeded,
		txsaga.EventStepSucceeded,
		txsaga.EventExecutionCompleted,
	}
	if len(page) != len(wantTypes) {
		t.Fatalf("history len=%d want %d (%v)", len(page), len(wantTypes), typesOf(page))
	}
	for i, et := range wantTypes {
		if page[i].Type != et {
			t.Fatalf("event[%d]=%s want %s", i, page[i].Type, et)
		}
		// 已 Ack 事件仍可审计，且投递计数保留最后一次值。
		if page[i].Deliveries != 1 {
			t.Fatalf("event %s deliveries=%d want 1", page[i].ID, page[i].Deliveries)
		}
	}
	// EventPayload 结构化字段重开后完整往返：completed 事件带有按确认
	// 顺序排列的 SucceededSteps 快照。
	final := page[len(page)-1]
	ep, ok := final.Payload.(txsaga.EventPayload)
	if !ok {
		t.Fatalf("completed payload type=%T want txsaga.EventPayload", final.Payload)
	}
	if !equalStrings(ep.SucceededSteps, []string{"reserve", "charge", "ship"}) {
		t.Fatalf("SucceededSteps after reopen=%v", ep.SucceededSteps)
	}
	if ep.SagaName != "order" || ep.IdempotencyKey != "req-1" || ep.Status != txsaga.StatusCompleted {
		t.Fatalf("completed payload identity lost: %+v", ep)
	}
}

// TestEngine_ResumeCompensationAfterReopen 验证未完成执行可在重开后续跑：
// 补偿第一步进行中取消 context，执行停在 compensating；重开后用新 context
// 再次 Execute，应继续逆序补偿并收束到 failed 终态。
func TestEngine_ResumeCompensationAfterReopen(t *testing.T) {
	dir := storeDir(t)

	s1, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	eng := txsaga.NewEngine(s1)

	var compOrder []string
	var compMu sync.Mutex
	bCompCalls := 0
	comp := func(name string) txsaga.CompensationFunc {
		return func(_ context.Context, _ txsaga.ExecutionView) error {
			compMu.Lock()
			compOrder = append(compOrder, name)
			compMu.Unlock()
			return nil
		}
	}
	failCalls := 0
	ctx, cancel := context.WithCancel(context.Background())
	def := txsaga.Definition{
		Name: "saga", Version: "v1",
		Steps: []txsaga.Step{
			{Name: "a", Action: okAction(new(int)), Compensate: comp("a")},
			{Name: "b", Action: okAction(new(int)), Compensate: func(c context.Context, _ txsaga.ExecutionView) error {
				bCompCalls++
				// 首次补偿期间取消：引擎不确认该补偿，执行停在 compensating。
				cancel()
				return ctx.Err()
			}},
			{Name: "c", Action: alwaysFail(&failCalls)},
		},
	}
	_, err = eng.Execute(ctx, def, txsaga.ExecutionRequest{
		BusinessKey: "bk", IdempotencyKey: "ik",
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("execute err=%v want context.Canceled", err)
	}
	if bCompCalls != 1 {
		t.Fatalf("b comp calls=%d want 1 before reopen", bCompCalls)
	}
	// 已提交状态停在 compensating。
	mid, err := txsaga.NewEngine(s1).GetResult(context.Background(), "saga", "bk", "ik")
	if err != nil {
		t.Fatal(err)
	}
	if mid.Status != txsaga.StatusCompensating || mid.Terminal {
		t.Fatalf("mid status=%s terminal=%v want compensating/non-terminal", mid.Status, mid.Terminal)
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	// 重开后用未取消的 context 继续：补偿 b、a（逆序），收束 failed。
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	// 重开后需要一个补偿不再取消的定义：bCompCalls>=1 时直接成功。
	resumeDef := txsaga.Definition{
		Name: "saga", Version: "v1",
		Steps: []txsaga.Step{
			{Name: "a", Action: okAction(new(int)), Compensate: comp("a")},
			{Name: "b", Action: okAction(new(int)), Compensate: comp("b")},
			{Name: "c", Action: alwaysFail(new(int))},
		},
	}
	// resumeDef 与 def 指纹一致（仅函数体不同，指纹不包含函数）。
	res, err := txsaga.NewEngine(s2).Execute(context.Background(), resumeDef,
		txsaga.ExecutionRequest{BusinessKey: "bk", IdempotencyKey: "ik"})
	if err != nil {
		t.Fatalf("resume execute: %v", err)
	}
	if !res.Terminal || res.Status != txsaga.StatusFailed {
		t.Fatalf("after resume status=%s terminal=%v want failed/terminal", res.Status, res.Terminal)
	}
	if !equalStrings(compOrder, []string{"b", "a"}) {
		t.Fatalf("comp order=%v want [b a]", compOrder)
	}
	if bCompCalls != 1 {
		t.Fatalf("b compensation was committed/retried before cancel: calls=%d", bCompCalls)
	}

	// 再次重开，终态固定可读。
	s2.Close()
	s3, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	got, err := txsaga.NewEngine(s3).GetResult(context.Background(), "saga", "bk", "ik")
	if err != nil {
		t.Fatal(err)
	}
	if !got.Terminal || got.Status != txsaga.StatusFailed {
		t.Fatalf("after reopen status=%s want failed", got.Status)
	}
}

// TestEngine_CompensationFailedPersists 验证 compensation_failed 终态持久化。
func TestEngine_CompensationFailedPersists(t *testing.T) {
	dir := storeDir(t)
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	aCalls, bCalls := 0, 0
	compCalls := 0
	def := txsaga.Definition{
		Name: "saga", Version: "v1",
		Steps: []txsaga.Step{
			{Name: "a", Action: okAction(&aCalls),
				Compensate: func(_ context.Context, _ txsaga.ExecutionView) error {
					compCalls++
					return errors.New("comp boom")
				}},
			{Name: "b", Action: alwaysFail(&bCalls)},
		},
	}
	eng := txsaga.NewEngine(s)
	res, err := eng.Execute(context.Background(), def, txsaga.ExecutionRequest{BusinessKey: "bk", IdempotencyKey: "ik"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != txsaga.StatusCompensationFailed || !res.Terminal {
		t.Fatalf("status=%s", res.Status)
	}
	if res.FailedCompensationStep != "a" || res.CompensationError != "comp boom" {
		t.Fatalf("comp failure detail lost: %+v", res)
	}
	s.Close()

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	got, _ := txsaga.NewEngine(s2).GetResult(context.Background(), "saga", "bk", "ik")
	if got.Status != txsaga.StatusCompensationFailed {
		t.Fatalf("compensation_failed not persisted: %s", got.Status)
	}
	if got.FailedCompensationStep != "a" {
		t.Fatalf("failed comp step lost: %s", got.FailedCompensationStep)
	}
	if got.Steps[0].CompensationAttempts != 1 {
		t.Fatalf("compensation attempts lost: %d", got.Steps[0].CompensationAttempts)
	}
	if compCalls != 1 {
		t.Fatalf("terminal comp re-invoked after reopen? compCalls=%d", compCalls)
	}
}

// TestEngine_DefinitionConflictAcrossReopen 验证定义绑定持久化。
func TestEngine_DefinitionConflictAcrossReopen(t *testing.T) {
	dir := storeDir(t)
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	eng := txsaga.NewEngine(s)
	def1 := txsaga.Definition{Name: "saga", Version: "v1",
		Steps: []txsaga.Step{{Name: "a", Action: okAction(new(int))}}}
	if _, err := eng.Execute(context.Background(), def1,
		txsaga.ExecutionRequest{BusinessKey: "bk", IdempotencyKey: "ik"}); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	eng2 := txsaga.NewEngine(s2)
	def2 := txsaga.Definition{Name: "saga", Version: "v2",
		Steps: []txsaga.Step{{Name: "a", Action: okAction(new(int))}}}
	_, err = eng2.Execute(context.Background(), def2,
		txsaga.ExecutionRequest{BusinessKey: "bk", IdempotencyKey: "ik"})
	if !errors.Is(err, txsaga.ErrDefinitionConflict) {
		t.Fatalf("conflict after reopen err=%v want ErrDefinitionConflict", err)
	}
}

// TestEngine_PayloadIsFromCurrentExecute 验证动作始终收到本次 Execute 传入
// 的 Payload，而不是持久化后经 JSON 往返的值。
func TestEngine_PayloadIsFromCurrentExecute(t *testing.T) {
	dir := storeDir(t)
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	eng := txsaga.NewEngine(s)

	type custom struct{ N int }
	var seen any
	def := txsaga.Definition{
		Name: "saga", Version: "v1",
		Steps: []txsaga.Step{
			{Name: "a", Action: func(_ context.Context, exec txsaga.ExecutionView) error {
				seen = exec.Payload()
				return nil
			}},
			{Name: "b", Action: func(_ context.Context, exec txsaga.ExecutionView) error {
				// 依赖图模式下该步也应拿到本次 Execute 的同一负载指针。
				if exec.Payload() != seen {
					t.Errorf("payload identity differs across steps")
				}
				return nil
			}, DependsOn: []string{"a"}},
		},
	}
	p := &custom{N: 9}
	if _, err := eng.Execute(context.Background(), def,
		txsaga.ExecutionRequest{BusinessKey: "bk", IdempotencyKey: "ik", Payload: p}); err != nil {
		t.Fatal(err)
	}
	if seen != any(p) {
		t.Fatalf("action got %#v want exact payload pointer %#v", seen, p)
	}
}

// TestEngine_FileStoreMatchesMemoryStore 用同一组定义在 MemoryStore 与
// FileStore 上执行，比较终态结果与事件链（类型、顺序、计数），要求可观察
// 结果一致。覆盖顺序成功、顺序失败补偿、依赖图失败补偿、永久失败。
func TestEngine_FileStoreMatchesMemoryStore(t *testing.T) {
	dagA, dagB, dagD := 0, 0, 0
	compCalls := new(int)
	permanentCalls := 0
	calls1 := 0

	scenarios := []struct {
		name string
		def  txsaga.Definition
	}{
		{
			name: "sequential success",
			def: txsaga.Definition{Name: "m", Version: "1", Steps: []txsaga.Step{
				{Name: "a", Action: okAction(new(int))},
				{Name: "b", Action: okAction(new(int))},
			}},
		},
		{
			name: "sequential failure with compensation",
			def: txsaga.Definition{Name: "m", Version: "2", Steps: []txsaga.Step{
				{Name: "a", Action: okAction(new(int)), Compensate: compAction(compCalls)},
				{Name: "b", Action: okAction(new(int)), Compensate: compAction(new(int))},
				{Name: "c", Action: alwaysFail(&calls1)},
			}},
		},
		{
			name: "dag failure with reverse-order compensation",
			def: txsaga.Definition{Name: "m", Version: "3", Steps: []txsaga.Step{
				{Name: "a", Action: okAction(&dagA), Compensate: compAction(new(int))},
				{Name: "b", Action: okAction(&dagB)},
				{Name: "c", Action: alwaysFail(new(int)), DependsOn: []string{"a", "b"}},
				{Name: "d", Action: okAction(&dagD), Compensate: compAction(new(int)), DependsOn: []string{"a"}},
			}},
		},
		{
			name: "permanent failure short-circuits",
			def: txsaga.Definition{Name: "m", Version: "4", Steps: []txsaga.Step{
				{Name: "a", Action: okAction(new(int)), Compensate: compAction(new(int))},
				{Name: "b", Action: func(_ context.Context, _ txsaga.ExecutionView) error {
					permanentCalls++
					return txsaga.Permanent(errors.New("nope"))
				}, ActionRetry: txsaga.RetryPolicy{MaxAttempts: 5, RetryWait: time.Millisecond}},
			}},
		},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			mem := txsaga.NewMemoryStore()
			file := newTestStore(t)

			memEng := txsaga.NewEngine(mem)
			fileEng := txsaga.NewEngine(file)

			rMem, err := memEng.Execute(context.Background(), sc.def, txsaga.ExecutionRequest{BusinessKey: "bk", IdempotencyKey: "ik"})
			if err != nil {
				t.Fatal(err)
			}
			rFile, err := fileEng.Execute(context.Background(), sc.def, txsaga.ExecutionRequest{BusinessKey: "bk", IdempotencyKey: "ik"})
			if err != nil {
				t.Fatal(err)
			}
			if !resultsEqual(rMem, rFile) {
				t.Fatalf("result mismatch\nmem:  %+v\nfile: %+v", rMem, rFile)
			}
			if sc.name == "permanent failure short-circuits" && rFile.Steps[1].Attempts != 1 {
				t.Fatalf("permanent attempts=%d want 1", rFile.Steps[1].Attempts)
			}

			memTypes := historyTypes(t, mem, sc.def.Name)
			fileTypes := historyTypes(t, file, sc.def.Name)
			if !equalStrings(memTypes, fileTypes) {
				t.Fatalf("event chain mismatch\nmem:  %v\nfile: %v", memTypes, fileTypes)
			}
		})
	}
}

// TestRelay_PersistsAckAndReopen 验证 Relay 基于 FileStore 完成投递后，
// 已 Ack 事件离开 Outbox，但审计历史在重开后仍可读取。
func TestRelay_PersistsAckAndReopen(t *testing.T) {
	dir := storeDir(t)
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	eng := txsaga.NewEngine(s)
	def := txsaga.Definition{Name: "r", Version: "1", Steps: []txsaga.Step{
		{Name: "a", Action: okAction(new(int))},
	}}
	if _, err := eng.Execute(context.Background(), def,
		txsaga.ExecutionRequest{BusinessKey: "bk", IdempotencyKey: "ik"}); err != nil {
		t.Fatal(err)
	}

	var sent []string
	var mu sync.Mutex
	relay := txsaga.NewRelay(s, txsaga.PublisherFunc(func(_ context.Context, ev txsaga.Event) error {
		mu.Lock()
		sent = append(sent, ev.ID)
		mu.Unlock()
		return nil
	}), txsaga.WithRetryBackoff(time.Millisecond))

	res, err := relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Delivered != 3 || res.Failed != 0 { // started + succeeded + completed
		t.Fatalf("deliver result=%+v", res)
	}
	if res.Delivered != len(sent) {
		t.Fatalf("sent %d events but acked %d", len(sent), res.Delivered)
	}
	if s.TotalEvents() != 0 || s.PendingCount() != 0 {
		t.Fatalf("outbox not drained: total=%d pending=%d", s.TotalEvents(), s.PendingCount())
	}
	// 无事件可再领。
	again, err := s.ClaimPendingEvents(context.Background(), 16)
	if err != nil || len(again) != 0 {
		t.Fatalf("events claimable after ack: %d err=%v", len(again), err)
	}
	s.Close()

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	page := allHistory(t, txsaga.NewEngine(s2), "r", "bk", "ik")
	if len(page) != 3 {
		t.Fatalf("audited events after reopen=%d want 3", len(page))
	}
	for _, rec := range page {
		if rec.Deliveries != 1 {
			t.Fatalf("acked event %s deliveries=%d want 1", rec.ID, rec.Deliveries)
		}
	}
}

// ---- helpers ----

func storeDir(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "store")
}

func typesOf(recs []txsaga.EventRecord) []string {
	out := make([]string, len(recs))
	for i, r := range recs {
		out[i] = r.Type
	}
	return out
}

func allHistory(t *testing.T, eng *txsaga.Engine, saga, bk, ik string) []txsaga.EventRecord {
	t.Helper()
	var all []txsaga.EventRecord
	after := ""
	for {
		page, err := eng.ListEvents(context.Background(), txsaga.EventHistoryQuery{
			SagaName: saga, BusinessKey: bk, IdempotencyKey: ik, AfterID: after, Limit: 1,
		})
		if err != nil {
			t.Fatalf("ListEvents after=%q: %v", after, err)
		}
		all = append(all, page.Events...)
		if !page.HasMore {
			break
		}
		after = page.NextAfterID
	}
	return all
}

func drainAll(s *FileStore) error {
	relay := txsaga.NewRelay(s, txsaga.PublisherFunc(func(_ context.Context, _ txsaga.Event) error {
		return nil
	}), txsaga.WithRetryBackoff(time.Millisecond))
	for {
		res, err := relay.DeliverOnce(context.Background())
		if err != nil {
			return err
		}
		if res.Claimed == 0 {
			return nil
		}
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func resultsEqual(a, b txsaga.Result) bool {
	if a.Status != b.Status || a.FailureReason != b.FailureReason ||
		a.FailedStep != b.FailedStep || a.CompensationError != b.CompensationError ||
		a.FailedCompensationStep != b.FailedCompensationStep || a.Terminal != b.Terminal ||
		len(a.Steps) != len(b.Steps) {
		return false
	}
	for i := range a.Steps {
		if a.Steps[i].Name != b.Steps[i].Name ||
			a.Steps[i].Result != b.Steps[i].Result ||
			a.Steps[i].Attempts != b.Steps[i].Attempts ||
			a.Steps[i].CompensationAttempts != b.Steps[i].CompensationAttempts ||
			a.Steps[i].Error != b.Steps[i].Error {
			return false
		}
	}
	return true
}

func historyTypes(t *testing.T, store txsaga.EventHistoryStore, saga string) []string {
	t.Helper()
	page, err := store.ListEvents(context.Background(), txsaga.EventHistoryQuery{
		SagaName: saga, BusinessKey: "bk", IdempotencyKey: "ik", Limit: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	return typesOf(page.Events)
}
