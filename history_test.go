package txsaga

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

// historyEngine 准备一个成功三步 Saga 的已完成执行，返回引擎、存储与定义。
func historyEngine(t *testing.T, businessKey, idemKey string) (*Engine, *MemoryStore, Definition) {
	t.Helper()
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &recorder{}
	def := threeStepDef(rec, "", "")
	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: businessKey, IdempotencyKey: idemKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusCompleted {
		t.Fatalf("status=%s", res.Status)
	}
	return eng, store, def
}

func TestListEventsReturnsFullChainInAppendOrder(t *testing.T) {
	eng, store, _ := historyEngine(t, "order-h1", "req-h1")

	page, err := eng.ListEvents(context.Background(), EventHistoryQuery{
		SagaName: "order-saga", BusinessKey: "order-h1", IdempotencyKey: "req-h1",
	})
	if err != nil {
		t.Fatal(err)
	}
	wantTypes := []string{
		EventExecutionStarted,
		EventStepSucceeded, EventStepSucceeded, EventStepSucceeded,
		EventExecutionCompleted,
	}
	if len(page.Events) != len(wantTypes) {
		t.Fatalf("events=%d want=%d", len(page.Events), len(wantTypes))
	}
	if page.HasMore {
		t.Fatal("HasMore must be false on the single full page")
	}
	if page.NextAfterID != page.Events[len(page.Events)-1].ID {
		t.Fatal("NextAfterID must be the last event ID")
	}
	prevID := ""
	prevTime := time.Time{}
	for i, r := range page.Events {
		if r.Type != wantTypes[i] {
			t.Fatalf("record[%d]=%s want %s", i, r.Type, wantTypes[i])
		}
		if r.ID == "" || r.ID <= prevID {
			t.Fatalf("record[%d] ID not strictly increasing: %q after %q", i, r.ID, prevID)
		}
		prevID = r.ID
		if r.OccurredAt.IsZero() || r.OccurredAt.Before(prevTime) {
			t.Fatalf("record[%d] bad OccurredAt: %v", i, r.OccurredAt)
		}
		prevTime = r.OccurredAt
		if r.BusinessKey != "order-h1" {
			t.Fatalf("record[%d] business key=%q", i, r.BusinessKey)
		}
		p, ok := r.Payload.(EventPayload)
		if !ok {
			t.Fatalf("record[%d] payload type %T", i, r.Payload)
		}
		if p.SagaName != "order-saga" || p.IdempotencyKey != "req-h1" {
			t.Fatalf("record[%d] payload identity=%+v", i, p)
		}
		// 尚未领取：投递次数为 0，最近领取时间为零值。
		if r.Deliveries != 0 || !r.LastAttemptAt.IsZero() {
			t.Fatalf("record[%d] fresh delivery metadata: deliveries=%d last=%v",
				i, r.Deliveries, r.LastAttemptAt)
		}
	}

	// 查询只读：待领取数量与事件总数不变。
	if store.PendingCount() != len(wantTypes) || store.TotalEvents() != len(wantTypes) {
		t.Fatalf("query changed outbox: pending=%d total=%d", store.PendingCount(), store.TotalEvents())
	}
}

func TestListEventsPagination(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)

	// 直接在同一执行上追加 25 条事件（1 条随 Create 提交，其余分批提交），
	// 以覆盖跨 Commit 的分页与跨页顺序。
	err := store.Commit(context.Background(), "bk-page", "ik-page", func(tx Tx) error {
		tx.Create(&ExecutionState{
			SagaName: "page-saga", Fingerprint: "fp", Status: StatusCompleted,
			Steps: []StepState{{Name: "s"}},
		})
		tx.AppendEvent("e-00", EventPayload{SagaName: "page-saga"})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < 25; i++ {
		i := i
		err := store.Commit(context.Background(), "bk-page", "ik-page", func(tx Tx) error {
			tx.AppendEvent(fmt.Sprintf("e-%02d", i), EventPayload{SagaName: "page-saga"})
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	query := EventHistoryQuery{
		SagaName: "page-saga", BusinessKey: "bk-page", IdempotencyKey: "ik-page", Limit: 10,
	}
	var all []EventRecord
	for {
		page, err := eng.ListEvents(context.Background(), query)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Events) > 10 {
			t.Fatalf("page size=%d > limit", len(page.Events))
		}
		all = append(all, page.Events...)
		if !page.HasMore {
			if len(page.Events) != 5 {
				t.Fatalf("last page size=%d want 5", len(page.Events))
			}
			break
		}
		if len(page.Events) != 10 {
			t.Fatalf("full page size=%d want 10", len(page.Events))
		}
		if page.NextAfterID != page.Events[9].ID {
			t.Fatal("bad NextAfterID")
		}
		query.AfterID = page.NextAfterID
	}
	if len(all) != 25 {
		t.Fatalf("walked %d events want 25", len(all))
	}
	for i, r := range all {
		if r.Type != fmt.Sprintf("e-%02d", i) {
			t.Fatalf("record[%d]=%s, cross-page order broken", i, r.Type)
		}
	}

	// 页大小恰好等于事件总数：HasMore 为 false，一页取完。
	exact, err := eng.ListEvents(context.Background(), EventHistoryQuery{
		SagaName: "page-saga", BusinessKey: "bk-page", IdempotencyKey: "ik-page", Limit: 25,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(exact.Events) != 25 || exact.HasMore {
		t.Fatalf("exact-size page: n=%d hasMore=%v", len(exact.Events), exact.HasMore)
	}

	// Limit 非正数时按 100 条返回：构造 105 条事件的新执行。
	err = store.Commit(context.Background(), "bk-100", "ik-100", func(tx Tx) error {
		tx.Create(&ExecutionState{
			SagaName: "big-saga", Fingerprint: "fp", Status: StatusCompleted,
			Steps: []StepState{{Name: "s"}},
		})
		for i := 0; i < 105; i++ {
			tx.AppendEvent("big", EventPayload{SagaName: "big-saga"})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int{0, -1, -100} {
		page, err := eng.ListEvents(context.Background(), EventHistoryQuery{
			SagaName: "big-saga", BusinessKey: "bk-100", IdempotencyKey: "ik-100", Limit: limit,
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Events) != 100 || !page.HasMore {
			t.Fatalf("limit=%d: n=%d hasMore=%v, want 100/true", limit, len(page.Events), page.HasMore)
		}
	}

	// 无事件的执行（直接 Create 未追加事件）返回空页而非错误。
	err = store.Commit(context.Background(), "bk-empty", "ik-empty", func(tx Tx) error {
		tx.Create(&ExecutionState{
			SagaName: "empty-saga", Fingerprint: "fp", Status: StatusRunning,
			Steps: []StepState{{Name: "s"}},
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	empty, err := eng.ListEvents(context.Background(), EventHistoryQuery{
		SagaName: "empty-saga", BusinessKey: "bk-empty", IdempotencyKey: "ik-empty",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(empty.Events) != 0 || empty.HasMore || empty.NextAfterID != "" {
		t.Fatalf("empty page = %+v", empty)
	}
}

func TestListEventsKeepsAckedEventsAsAuditCopy(t *testing.T) {
	eng, store, _ := historyEngine(t, "order-ack", "req-ack")

	claimed, err := store.ClaimPendingEvents(context.Background(), 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 5 {
		t.Fatalf("claimed=%d", len(claimed))
	}
	// 全部投递成功并 Ack：活动事件被清空（既有 Outbox 语义不变）。
	for _, ev := range claimed {
		if err := store.AckEvent(context.Background(), ev.ID); err != nil {
			t.Fatal(err)
		}
	}
	if store.TotalEvents() != 0 || store.PendingCount() != 0 {
		t.Fatalf("ack must clear live outbox: total=%d pending=%d", store.TotalEvents(), store.PendingCount())
	}

	// 历史仍保留完整事件链，投递次数定格在最后一次领取值 1。
	page, err := eng.ListEvents(context.Background(), EventHistoryQuery{
		SagaName: "order-saga", BusinessKey: "order-ack", IdempotencyKey: "req-ack",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 5 || page.HasMore {
		t.Fatalf("acked chain must remain auditable: %+v", page)
	}
	for _, r := range page.Events {
		if r.Deliveries != 1 {
			t.Fatalf("event %s deliveries=%d want 1", r.ID, r.Deliveries)
		}
		if r.LastAttemptAt.IsZero() {
			t.Fatalf("event %s lost last attempt time", r.ID)
		}
	}

	// Ack 之后再用末条事件做游标翻页：游标仍可命中，只是后面没有事件。
	last, err := eng.ListEvents(context.Background(), EventHistoryQuery{
		SagaName: "order-saga", BusinessKey: "order-ack", IdempotencyKey: "req-ack",
		AfterID: page.Events[4].ID, Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(last.Events) != 0 || last.HasMore {
		t.Fatalf("page after last acked event = %+v", last)
	}
}

func TestListEventsReflectsClaimNackAndRedelivery(t *testing.T) {
	eng, store, _ := historyEngine(t, "order-nack", "req-nack")

	first, err := store.ClaimPendingEvents(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 {
		t.Fatalf("claimed=%d want 2", len(first))
	}
	// 领取中：前两条 deliveries=1 且有最近领取时间；其余仍为 0。
	mid, err := eng.ListEvents(context.Background(), EventHistoryQuery{
		SagaName: "order-saga", BusinessKey: "order-nack", IdempotencyKey: "req-nack",
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range mid.Events {
		wantDeliveries := 0
		if i < 2 {
			wantDeliveries = 1
		}
		if r.Deliveries != wantDeliveries {
			t.Fatalf("claimed record[%d] deliveries=%d want %d", i, r.Deliveries, wantDeliveries)
		}
	}
	// 查询不得解除领取锁定。
	if again, _ := store.ClaimPendingEvents(context.Background(), 100); len(again) != 3 {
		t.Fatalf("history query must not release claims, claimed=%d", len(again))
	}

	// 退回首条并重投：历史中的该条 deliveries=2、最近领取时间刷新，顺序不变。
	if err := store.NackEvent(context.Background(), first[0].ID); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := store.ClaimPendingEvents(context.Background(), 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(reclaimed) != 1 || reclaimed[0].ID != first[0].ID {
		t.Fatalf("reclaimed = %v", eventTypes(reclaimed))
	}
	page, err := eng.ListEvents(context.Background(), EventHistoryQuery{
		SagaName: "order-saga", BusinessKey: "order-nack", IdempotencyKey: "req-nack",
	})
	if err != nil {
		t.Fatal(err)
	}
	types := make([]string, len(page.Events))
	for i, r := range page.Events {
		types[i] = r.Type
	}
	wantTypes := []string{
		EventExecutionStarted,
		EventStepSucceeded, EventStepSucceeded, EventStepSucceeded,
		EventExecutionCompleted,
	}
	if fmt.Sprint(types) != fmt.Sprint(wantTypes) {
		t.Fatalf("order changed after nack/redelivery: %v", types)
	}
	if page.Events[0].Deliveries != 2 {
		t.Fatalf("redelivered deliveries=%d want 2", page.Events[0].Deliveries)
	}
}

func TestListEventsReflectsLeaseLifecycle(t *testing.T) {
	store, clk, ids := leasedTestStore(t, 3)
	// leasedTestStore 只追加原始事件；补上执行记录，使历史身份校验成立。
	if err := store.Commit(context.Background(), "bk", "ik", func(tx Tx) error {
		tx.Create(&ExecutionState{
			SagaName: "lease-saga", Fingerprint: "fp", Status: StatusCompleted,
			Steps: []StepState{{Name: "s0"}},
		})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	eng := NewEngine(store)
	q := EventHistoryQuery{SagaName: "lease-saga", BusinessKey: "bk", IdempotencyKey: "ik"}

	claims, err := store.ClaimPendingEventsLeased(context.Background(), time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	page, err := eng.ListEvents(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 3 {
		t.Fatalf("n=%d", len(page.Events))
	}
	for i, r := range page.Events {
		if r.ID != ids[i] {
			t.Fatalf("record[%d]=%s want %s", i, r.ID, ids[i])
		}
		if r.Deliveries != 1 || !r.LastAttemptAt.Equal(clk.now()) {
			t.Fatalf("record[%d] delivery metadata=%d/%v", i, r.Deliveries, r.LastAttemptAt)
		}
	}

	// 租约到期被重新领取：投递次数累计为 2，顺序不变。
	clk.advance(time.Minute + time.Second)
	recovered, err := store.ClaimPendingEventsLeased(context.Background(), time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 3 {
		t.Fatalf("recovered=%d", len(recovered))
	}
	page, err = eng.ListEvents(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range page.Events {
		if r.ID != ids[i] || r.Deliveries != 2 || !r.LastAttemptAt.Equal(clk.now()) {
			t.Fatalf("after recovery record[%d]=%+v", i, r)
		}
	}

	// 带租约 Ack 后活动副本消失，审计副本仍在，投递次数保留为 2。
	if err := store.AckLeasedEvent(context.Background(), claims[0].Event.ID, recovered[0].ClaimID); err != nil {
		t.Fatal(err)
	}
	if store.TotalEvents() != 2 {
		t.Fatalf("TotalEvents=%d want 2", store.TotalEvents())
	}
	page, err = eng.ListEvents(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 3 {
		t.Fatalf("leased ack must keep audit copy, n=%d", len(page.Events))
	}
	if page.Events[0].Deliveries != 2 {
		t.Fatalf("acked record deliveries=%d want 2", page.Events[0].Deliveries)
	}
}

func TestListEventsCompensationChain(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &recorder{}
	def := threeStepDef(rec, "charge", "")
	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "order-comp", IdempotencyKey: "req-comp",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusFailed {
		t.Fatalf("status=%s", res.Status)
	}
	page, err := eng.ListEvents(context.Background(), EventHistoryQuery{
		SagaName: "order-saga", BusinessKey: "order-comp", IdempotencyKey: "req-comp",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		EventExecutionStarted,
		EventStepSucceeded,
		EventStepFailed,
		EventStepCompensated,
		EventExecutionFailed,
	}
	if len(page.Events) != len(want) {
		t.Fatalf("n=%d", len(page.Events))
	}
	for i, r := range page.Events {
		if r.Type != want[i] {
			t.Fatalf("record[%d]=%s want %s", i, r.Type, want[i])
		}
	}
}

func TestListEventsIsolatesExecutions(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)

	// 同业务键、不同幂等键 = 不同执行；事件交错追加也必须按身份过滤。
	commit := func(idem string, n int) {
		for i := 0; i < n; i++ {
			err := store.Commit(context.Background(), "bk-multi", idem, func(tx Tx) error {
				if tx.State() == nil {
					tx.Create(&ExecutionState{
						SagaName: "multi-saga", Fingerprint: "fp", Status: StatusCompleted,
						Steps: []StepState{{Name: "s"}},
					})
				}
				tx.AppendEvent(idem, EventPayload{SagaName: "multi-saga", IdempotencyKey: idem})
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	commit("ik-a", 3)
	commit("ik-b", 2)
	commit("ik-a", 2) // 与 b 的事件交错后继续追加 a

	page, err := eng.ListEvents(context.Background(), EventHistoryQuery{
		SagaName: "multi-saga", BusinessKey: "bk-multi", IdempotencyKey: "ik-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 5 || page.HasMore {
		t.Fatalf("execution a n=%d hasMore=%v, want 5/false", len(page.Events), page.HasMore)
	}
	for _, r := range page.Events {
		if r.Type != "ik-a" {
			t.Fatalf("foreign event leaked into execution a: %+v", r)
		}
	}

	pageB, err := eng.ListEvents(context.Background(), EventHistoryQuery{
		SagaName: "multi-saga", BusinessKey: "bk-multi", IdempotencyKey: "ik-b",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(pageB.Events) != 2 {
		t.Fatalf("execution b n=%d want 2", len(pageB.Events))
	}
}

func TestListEventsErrorCases(t *testing.T) {
	eng, store, _ := historyEngine(t, "order-err", "req-err")

	// 缺少 Saga 名称、业务键、外部幂等键均为 ErrInvalidDefinition。
	for _, q := range []EventHistoryQuery{
		{SagaName: "  ", BusinessKey: "b", IdempotencyKey: "i"},
		{SagaName: "s", BusinessKey: "", IdempotencyKey: "i"},
		{SagaName: "s", BusinessKey: "b", IdempotencyKey: " "},
	} {
		if _, err := eng.ListEvents(context.Background(), q); !errors.Is(err, ErrInvalidDefinition) {
			t.Fatalf("q=%+v err=%v want ErrInvalidDefinition", q, err)
		}
	}

	// 执行不存在。
	if _, err := eng.ListEvents(context.Background(), EventHistoryQuery{
		SagaName: "order-saga", BusinessKey: "missing", IdempotencyKey: "req-err",
	}); !errors.Is(err, ErrExecutionNotFound) {
		t.Fatalf("unknown execution err=%v", err)
	}
	// Saga 名称不匹配。
	if _, err := eng.ListEvents(context.Background(), EventHistoryQuery{
		SagaName: "other-saga", BusinessKey: "order-err", IdempotencyKey: "req-err",
	}); !errors.Is(err, ErrExecutionNotFound) {
		t.Fatalf("saga name mismatch err=%v", err)
	}

	// afterID 完全不存在。
	if _, err := eng.ListEvents(context.Background(), EventHistoryQuery{
		SagaName: "order-saga", BusinessKey: "order-err", IdempotencyKey: "req-err",
		AfterID: "evt-does-not-exist",
	}); !errors.Is(err, ErrEventCursorNotFound) {
		t.Fatalf("bogus cursor err=%v want ErrEventCursorNotFound", err)
	}

	// afterID 属于另一个执行：同一存储内事件 ID 全局唯一，但不属于本身份。
	rec := &recorder{}
	otherDef := threeStepDef(rec, "", "")
	if _, err := eng.Execute(context.Background(), otherDef, ExecutionRequest{
		BusinessKey: "order-other", IdempotencyKey: "req-other",
	}); err != nil {
		t.Fatal(err)
	}
	otherPage, err := eng.ListEvents(context.Background(), EventHistoryQuery{
		SagaName: "order-saga", BusinessKey: "order-other", IdempotencyKey: "req-other",
	})
	if err != nil {
		t.Fatal(err)
	}
	otherID := otherPage.Events[0].ID
	if _, err := eng.ListEvents(context.Background(), EventHistoryQuery{
		SagaName: "order-saga", BusinessKey: "order-err", IdempotencyKey: "req-err",
		AfterID: otherID,
	}); !errors.Is(err, ErrEventCursorNotFound) {
		t.Fatalf("foreign cursor err=%v want ErrEventCursorNotFound", err)
	}

	// 出错时不得改变任何状态与投递元数据：两次执行共 10 条事件，全部未领取。
	if store.PendingCount() != 10 || store.TotalEvents() != 10 {
		t.Fatalf("error path mutated store: pending=%d total=%d", store.PendingCount(), store.TotalEvents())
	}
}

func TestListEventsUnsupportedByCustomStore(t *testing.T) {
	// plainStore 只实现 Store，刻意不实现 EventHistoryStore，
	// 但 Execute/GetResult 等既有功能必须照常工作。
	inner := NewMemoryStore()
	store := &plainStore{inner: inner}
	eng := NewEngine(store)
	rec := &recorder{}
	def := threeStepDef(rec, "", "")
	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "bk-plain", IdempotencyKey: "ik-plain",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusCompleted {
		t.Fatalf("status=%s", res.Status)
	}
	if _, err := eng.GetResult(context.Background(), "order-saga", "bk-plain", "ik-plain"); err != nil {
		t.Fatalf("GetResult on custom store: %v", err)
	}
	_, err = eng.ListEvents(context.Background(), EventHistoryQuery{
		SagaName: "order-saga", BusinessKey: "bk-plain", IdempotencyKey: "ik-plain",
	})
	if !errors.Is(err, ErrEventHistoryUnsupported) {
		t.Fatalf("err=%v want ErrEventHistoryUnsupported", err)
	}

	// MemoryStore 直接实现 EventHistoryStore（编译期断言在 store.go，
	// 这里再确认运行期类型断言成立）。
	var _ EventHistoryStore = inner
}

func TestListEventsReturnsDetachedRecords(t *testing.T) {
	eng, _, _ := historyEngine(t, "order-detach", "req-detach")
	page, err := eng.ListEvents(context.Background(), EventHistoryQuery{
		SagaName: "order-saga", BusinessKey: "order-detach", IdempotencyKey: "req-detach",
	})
	if err != nil {
		t.Fatal(err)
	}
	// 修改返回记录负载中的切片，不得污染存储内部。
	for i := range page.Events {
		if p, ok := page.Events[i].Payload.(EventPayload); ok && p.SucceededSteps != nil {
			p.SucceededSteps[0] = "TAMPERED"
			page.Events[i].Payload = p
		}
	}
	again, err := eng.ListEvents(context.Background(), EventHistoryQuery{
		SagaName: "order-saga", BusinessKey: "order-detach", IdempotencyKey: "req-detach",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range again.Events {
		if p, ok := r.Payload.(EventPayload); ok {
			for _, s := range p.SucceededSteps {
				if s == "TAMPERED" {
					t.Fatal("returned record aliases store payload")
				}
			}
		}
	}
}

func TestListEventsConcurrentWithExecutionAndDelivery(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	def := Definition{
		Name: "race-saga", Version: "v1",
		Steps: []Step{
			{Name: "a", Action: func(context.Context, ExecutionView) error { return nil }},
			{Name: "b", Action: func(context.Context, ExecutionView) error { return nil }},
		},
	}

	const execs = 8
	var execWG, workerWG sync.WaitGroup
	// 并发发起多个执行。
	for i := 0; i < execs; i++ {
		execWG.Add(1)
		go func(i int) {
			defer execWG.Done()
			_, err := eng.Execute(context.Background(), def, ExecutionRequest{
				BusinessKey:    fmt.Sprintf("bk-%d", i),
				IdempotencyKey: "ik",
			})
			if err != nil {
				t.Errorf("execute %d: %v", i, err)
			}
		}(i)
	}
	// 并发分页查询（可能观察到尚未追加完的链，但游标必须始终有效、
	// 每页只含原子提交事件、最终可走完整链）。
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		workerWG.Add(1)
		go func(i int) {
			defer workerWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				q := EventHistoryQuery{
					SagaName:       "race-saga",
					BusinessKey:    fmt.Sprintf("bk-%d", i%execs),
					IdempotencyKey: "ik", Limit: 2,
				}
				page, err := eng.ListEvents(context.Background(), q)
				if errors.Is(err, ErrExecutionNotFound) {
					continue // 执行尚未提交
				}
				if err != nil {
					t.Errorf("list %d: %v", i, err)
					return
				}
				for page.HasMore {
					q.AfterID = page.NextAfterID
					page, err = eng.ListEvents(context.Background(), q)
					if err != nil {
						t.Errorf("page %d: %v", i, err)
						return
					}
				}
				runtime.Gosched()
			}
		}(i)
	}
	// 并发领取与 Ack，历史读取不得受影响或 panic。
	workerWG.Add(1)
	go func() {
		defer workerWG.Done()
		pub := PublisherFunc(func(context.Context, Event) error { return nil })
		relay := NewRelay(store, pub, WithBatch(4))
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := relay.DeliverOnce(context.Background()); err != nil {
				t.Errorf("relay: %v", err)
				return
			}
		}
	}()
	execWG.Wait() // 等执行完成
	close(stop)
	workerWG.Wait()

	// 全部活动事件投递完后，每个执行的审计链仍完整且有序：
	// started -> a -> b -> completed。
	for i := 0; i < execs; i++ {
		var got []string
		q := EventHistoryQuery{
			SagaName: "race-saga", BusinessKey: fmt.Sprintf("bk-%d", i),
			IdempotencyKey: "ik", Limit: 2,
		}
		for {
			page, err := eng.ListEvents(context.Background(), q)
			if err != nil {
				t.Fatalf("final walk %d: %v", i, err)
			}
			for _, r := range page.Events {
				got = append(got, r.Type)
			}
			if !page.HasMore {
				break
			}
			q.AfterID = page.NextAfterID
		}
		want := []string{
			EventExecutionStarted, EventStepSucceeded, EventStepSucceeded, EventExecutionCompleted,
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("exec %d chain=%v want %v", i, got, want)
		}
	}
}
