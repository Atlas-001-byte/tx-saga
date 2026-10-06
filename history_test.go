package txsaga

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// historyTestStore 提交一个成功三步 Saga 并返回其全部事件 ID（追加顺序）。
func historyTestStore(t *testing.T, bk, ik string) (*MemoryStore, *Engine, []string) {
	t.Helper()
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &recorder{}
	def := threeStepDef(rec, "", "")
	res, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: bk, IdempotencyKey: ik,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Status != StatusCompleted {
		t.Fatalf("status=%s", res.Status)
	}
	// started + 3 * step_succeeded + completed
	wantTypes := []string{
		EventExecutionStarted,
		EventStepSucceeded, EventStepSucceeded, EventStepSucceeded,
		EventExecutionCompleted,
	}
	page, err := eng.ListEvents(context.Background(), EventHistoryQuery{
		SagaName: def.Name, BusinessKey: bk, IdempotencyKey: ik, Limit: 100,
	})
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	ids := make([]string, len(page.Records))
	for i, r := range page.Records {
		ids[i] = r.ID
		if r.Type != wantTypes[i] {
			t.Fatalf("record[%d]=%s want %s", i, r.Type, wantTypes[i])
		}
	}
	return store, eng, ids
}

func recordTypes(rs []EventRecord) []string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = r.Type
	}
	return out
}

func TestListEventsFullChainFields(t *testing.T) {
	store, eng, ids := historyTestStore(t, "order-h1", "req-h1")

	page, err := eng.ListEvents(context.Background(), EventHistoryQuery{
		SagaName: "order-saga", BusinessKey: "order-h1", IdempotencyKey: "req-h1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Records) != 5 || page.HasMore || page.NextAfterID != "" {
		t.Fatalf("page=%+v len=%d", page, len(page.Records))
	}
	prev := time.Time{}
	for i, r := range page.Records {
		if r.ID != ids[i] {
			t.Fatalf("record[%d] ID=%s want %s", i, r.ID, ids[i])
		}
		if r.ID == "" || r.BusinessKey != "order-h1" || r.OccurredAt.IsZero() {
			t.Fatalf("record[%d] metadata incomplete: %+v", i, r)
		}
		// 同一提交内多条事件共享发生时间（末尾 step_succeeded 与
		// execution_completed 同事务追加）；不同提交不得回拨。
		if r.OccurredAt.Before(prev) {
			t.Fatalf("record[%d] OccurredAt moved backwards: %v", i, r.OccurredAt)
		}
		prev = r.OccurredAt
		p, ok := r.Payload.(EventPayload)
		if !ok {
			t.Fatalf("record[%d] payload type %T", i, r.Payload)
		}
		if p.SagaName != "order-saga" || p.IdempotencyKey != "req-h1" {
			t.Fatalf("record[%d] payload=%+v", i, p)
		}
		// 从未进入领取流程：投递次数为 0，最近领取时间为零值。
		if r.Deliveries != 0 || !r.LastAttemptAt.IsZero() {
			t.Fatalf("record[%d] delivery metadata=%d/%v", i, r.Deliveries, r.LastAttemptAt)
		}
	}

	// 历史查询不改变 Outbox：全部事件仍可领取。
	claimable := store.PendingCount()
	if claimable != 5 {
		t.Fatalf("PendingCount=%d want 5 after read-only query", claimable)
	}
}

func TestListEventsKeepsOrderAcrossClaimNackAck(t *testing.T) {
	store, eng, ids := historyTestStore(t, "order-h2", "req-h2")
	ctx := context.Background()

	// 领取前两条：e1/e2 进入领取中。
	first, err := store.ClaimPendingEvents(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if got := eventTypesPtr(first); fmt.Sprint(got) != fmt.Sprint([]string{
		EventExecutionStarted, EventStepSucceeded,
	}) {
		t.Fatalf("first claim=%v", got)
	}
	// Ack 第一条，退回第二条（退回后在领取队列尾部，投递顺序已变）。
	if err := store.AckEvent(ctx, first[0].ID); err != nil {
		t.Fatal(err)
	}
	if err := store.NackEvent(ctx, first[1].ID); err != nil {
		t.Fatal(err)
	}
	// 再领两条：按领取队列是 e3/e4；此刻 e1 已投递、e2 待投递、e3/e4 领取中、e5 待投递。
	second, err := store.ClaimPendingEvents(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 2 {
		t.Fatalf("second claim=%d", len(second))
	}

	// 活跃事件只剩 4 条（e1 已 Ack），但历史链仍是完整 5 条且顺序不变。
	if store.TotalEvents() != 4 {
		t.Fatalf("TotalEvents=%d want 4", store.TotalEvents())
	}
	page, err := eng.ListEvents(ctx, EventHistoryQuery{
		SagaName: "order-saga", BusinessKey: "order-h2", IdempotencyKey: "req-h2", Limit: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	wantTypes := []string{
		EventExecutionStarted,
		EventStepSucceeded, EventStepSucceeded, EventStepSucceeded,
		EventExecutionCompleted,
	}
	if len(page.Records) != 5 {
		t.Fatalf("records=%d want 5 (acked copy must be retained): %v", len(page.Records), recordTypes(page.Records))
	}
	deliveries := map[string]int{}
	for i, r := range page.Records {
		if r.ID != ids[i] || r.Type != wantTypes[i] {
			t.Fatalf("record[%d]=%s/%s want %s/%s", i, r.ID, r.Type, ids[i], wantTypes[i])
		}
		deliveries[r.ID] = r.Deliveries
	}
	// e1：领取 1 次后 Ack，副本冻结在 1。
	if deliveries[ids[0]] != 1 {
		t.Fatalf("acked e1 deliveries=%d want 1", deliveries[ids[0]])
	}
	// e2：领取、退回、尚未被再次领取 => 1；e3/e4：各领取 1 次 => 1；e5：0。
	want := map[string]int{ids[1]: 1, ids[2]: 1, ids[3]: 1, ids[4]: 0}
	for id, n := range want {
		if deliveries[id] != n {
			t.Fatalf("event %s deliveries=%d want %d (all=%v)", id, deliveries[id], n, deliveries)
		}
	}

	// 退回的 e2 排在队列尾部：一次领取按 [e5, e2] 取得，e2 投递次数累计为 2。
	rest, _ := store.ClaimPendingEvents(ctx, 100)
	if len(rest) != 2 {
		t.Fatalf("claimable=%d want 2 (e5 then requeued e2)", len(rest))
	}
	if got := eventTypesPtr(rest); got[0] != EventExecutionCompleted ||
		(got[1] != EventStepSucceeded || rest[1].ID != ids[1]) {
		t.Fatalf("unexpected claim order: %v", got)
	}
	page2, err := eng.ListEvents(ctx, EventHistoryQuery{
		SagaName: "order-saga", BusinessKey: "order-h2", IdempotencyKey: "req-h2",
	})
	if err != nil {
		t.Fatal(err)
	}
	gotDel := map[string]int{}
	for _, r := range page2.Records {
		gotDel[r.ID] = r.Deliveries
	}
	if gotDel[ids[1]] != 2 {
		t.Fatalf("e2 deliveries=%d want 2 after reclaim", gotDel[ids[1]])
	}
	if gotDel[ids[0]] != 1 {
		t.Fatalf("acked e1 deliveries changed to %d, want frozen 1", gotDel[ids[0]])
	}
}

func TestListEventsLeasedAckKeepsAuditCopy(t *testing.T) {
	store, clk, ids := leasedTestStore(t, 3)
	ctx := context.Background()

	claims, err := store.ClaimPendingEventsLeased(ctx, time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	// 第一条租约内 Ack；第二条租约到期后被重新领取；第三条 Nack。
	if err := store.AckLeasedEvent(ctx, claims[0].Event.ID, claims[0].ClaimID); err != nil {
		t.Fatal(err)
	}
	if err := store.NackLeasedEvent(ctx, claims[2].Event.ID, claims[2].ClaimID); err != nil {
		t.Fatal(err)
	}
	clk.advance(time.Hour)
	recovered, err := store.ClaimPendingEventsLeased(ctx, time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 2 {
		t.Fatalf("recovered=%d want 2 (e2 expired + e3 nacked)", len(recovered))
	}

	eng := NewEngine(store)
	// leasedTestStore 只追加了事件，补建执行身份以匹配查询。
	if err := store.Commit(ctx, "bk", "ik", func(tx Tx) error {
		tx.Create(&ExecutionState{
			SagaName: "lease-saga", Fingerprint: "f", Status: StatusCompleted,
			Steps: []StepState{{Name: "s"}},
		})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	page, err := eng.ListEvents(ctx, EventHistoryQuery{
		SagaName: "lease-saga", BusinessKey: "bk", IdempotencyKey: "ik", Limit: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Records) != 3 {
		t.Fatalf("records=%d want 3", len(page.Records))
	}
	for i, r := range page.Records {
		if r.ID != ids[i] {
			t.Fatalf("record[%d]=%s want %s: lease/nack/ack must not reorder history",
				i, r.ID, ids[i])
		}
	}
	// e1 Ack 副本冻结在 1；e2 到期重领为 2；e3 Nack 后重领为 2。
	if d0 := page.Records[0].Deliveries; d0 != 1 {
		t.Fatalf("acked e1 deliveries=%d want 1", d0)
	}
	if d1 := page.Records[1].Deliveries; d1 != 2 {
		t.Fatalf("recovered e2 deliveries=%d want 2", d1)
	}
	if d2 := page.Records[2].Deliveries; d2 != 2 {
		t.Fatalf("nacked e3 deliveries=%d want 2", d2)
	}
}

func TestListEventsPagination(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	// 单次原子提交：建立执行并追加 7 条事件。
	err := store.Commit(ctx, "bk2", "ik", func(tx Tx) error {
		tx.Create(&ExecutionState{
			SagaName: "p", Fingerprint: "f", Status: StatusRunning,
			Steps: []StepState{{Name: "s"}},
		})
		for i := 0; i < 7; i++ {
			tx.AppendEvent(fmt.Sprintf("type-%d", i), EventPayload{SagaName: "p"})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	walk := func(limit int) []EventRecord {
		var all []EventRecord
		cursor := ""
		for {
			page, err := store.ListEvents(ctx, EventHistoryQuery{
				SagaName: "p", BusinessKey: "bk2", IdempotencyKey: "ik",
				AfterID: cursor, Limit: limit,
			})
			if err != nil {
				t.Fatalf("page after %q: %v", cursor, err)
			}
			if len(page.Records) == 0 {
				t.Fatalf("empty page while HasMore implied; all=%d", len(all))
			}
			all = append(all, page.Records...)
			if !page.HasMore {
				if page.NextAfterID != "" {
					t.Fatalf("NextAfterID=%q on last page", page.NextAfterID)
				}
				return all
			}
			if page.NextAfterID != page.Records[len(page.Records)-1].ID {
				t.Fatalf("NextAfterID mismatch")
			}
			cursor = page.NextAfterID
		}
	}

	for _, limit := range []int{2, 3, 7, 100} {
		all := walk(limit)
		if len(all) != 7 {
			t.Fatalf("limit=%d collected=%d want 7", limit, len(all))
		}
		for i, r := range all {
			if r.Type != fmt.Sprintf("type-%d", i) {
				t.Fatalf("limit=%d record[%d]=%s, order changed across pages", limit, i, r.Type)
			}
		}
	}

	// 恰好取满一页且后面还有：HasMore 由剩余条数决定。
	page, err := store.ListEvents(ctx, EventHistoryQuery{
		SagaName: "p", BusinessKey: "bk2", IdempotencyKey: "ik", Limit: 7,
	})
	if err != nil {
		t.Fatal(err)
	}
	if page.HasMore || len(page.Records) != 7 {
		t.Fatalf("exact-size page=%+v", page)
	}
}

func TestListEventsDefaultLimitAndCursorSemantics(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	err := store.Commit(ctx, "bk", "ik", func(tx Tx) error {
		tx.Create(&ExecutionState{
			SagaName: "big", Fingerprint: "f", Status: StatusRunning,
			Steps: []StepState{{Name: "s"}},
		})
		for i := 0; i < 105; i++ {
			tx.AppendEvent("t", EventPayload{SagaName: "big"})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Limit 为 0 或负数都按 100 条返回。
	for _, limit := range []int{0, -1, -100} {
		page, err := store.ListEvents(ctx, EventHistoryQuery{
			SagaName: "big", BusinessKey: "bk", IdempotencyKey: "ik", Limit: limit,
		})
		if err != nil {
			t.Fatalf("limit=%d: %v", limit, err)
		}
		if len(page.Records) != 100 || !page.HasMore {
			t.Fatalf("limit=%d size=%d hasMore=%v", limit, len(page.Records), page.HasMore)
		}
	}

	// 命中 afterID 后从其下一条开始；用第 100 条的游标取剩余 5 条。
	first, _ := store.ListEvents(ctx, EventHistoryQuery{
		SagaName: "big", BusinessKey: "bk", IdempotencyKey: "ik",
	})
	second, err := store.ListEvents(ctx, EventHistoryQuery{
		SagaName: "big", BusinessKey: "bk", IdempotencyKey: "ik",
		AfterID: first.NextAfterID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Records) != 5 || second.HasMore {
		t.Fatalf("tail page size=%d hasMore=%v", len(second.Records), second.HasMore)
	}
	// 游标指向最后一条时返回空页但无错误。
	tail, err := store.ListEvents(ctx, EventHistoryQuery{
		SagaName: "big", BusinessKey: "bk", IdempotencyKey: "ik",
		AfterID: second.Records[4].ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(tail.Records) != 0 || tail.HasMore || tail.NextAfterID != "" {
		t.Fatalf("past-tail page=%+v", tail)
	}
}

func TestListEventsExecutionsAreIsolated(t *testing.T) {
	store := NewMemoryStore()
	eng := NewEngine(store)
	rec := &recorder{}
	def := threeStepDef(rec, "", "")
	// 同业务键、同定义、不同幂等键 => 两条独立执行，各自完整事件链。
	for _, ik := range []string{"a", "b"} {
		if _, err := eng.Execute(context.Background(), def, ExecutionRequest{
			BusinessKey: "shared-bk", IdempotencyKey: ik,
		}); err != nil {
			t.Fatal(err)
		}
	}
	pageA, err := eng.ListEvents(context.Background(), EventHistoryQuery{
		SagaName: "order-saga", BusinessKey: "shared-bk", IdempotencyKey: "a",
	})
	if err != nil {
		t.Fatal(err)
	}
	pageB, err := eng.ListEvents(context.Background(), EventHistoryQuery{
		SagaName: "order-saga", BusinessKey: "shared-bk", IdempotencyKey: "b",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(pageA.Records) != 5 || len(pageB.Records) != 5 {
		t.Fatalf("sizes=%d/%d want 5/5", len(pageA.Records), len(pageB.Records))
	}
	for i := range pageA.Records {
		if pageA.Records[i].ID == pageB.Records[i].ID {
			t.Fatalf("execution histories must not share events at %d", i)
		}
		p, ok := pageB.Records[i].Payload.(EventPayload)
		if !ok || p.IdempotencyKey != "b" {
			t.Fatalf("execution b payload leaked: %+v", pageB.Records[i].Payload)
		}
	}
}

func TestListEventsErrors(t *testing.T) {
	store, eng, ids := historyTestStore(t, "order-err", "req-err")
	ctx := context.Background()

	checkZero := func(p EventHistoryPage) {
		t.Helper()
		if len(p.Records) != 0 || p.HasMore || p.NextAfterID != "" {
			t.Fatalf("error must return zero page, got %+v", p)
		}
	}

	// 缺少任一身份字段 => ErrInvalidDefinition。
	for _, q := range []EventHistoryQuery{
		{SagaName: "", BusinessKey: "b", IdempotencyKey: "i", AfterID: ids[0]},
		{SagaName: "  ", BusinessKey: "b", IdempotencyKey: "i"},
		{SagaName: "s", BusinessKey: "", IdempotencyKey: "i"},
		{SagaName: "s", BusinessKey: "b", IdempotencyKey: ""},
	} {
		p, err := eng.ListEvents(ctx, q)
		if !errors.Is(err, ErrInvalidDefinition) {
			t.Fatalf("q=%+v err=%v want ErrInvalidDefinition", q, err)
		}
		checkZero(p)
	}

	// 执行不存在 / Saga 名称不匹配 => ErrExecutionNotFound。
	p, err := eng.ListEvents(ctx, EventHistoryQuery{
		SagaName: "order-saga", BusinessKey: "missing", IdempotencyKey: "req-err",
	})
	if !errors.Is(err, ErrExecutionNotFound) {
		t.Fatalf("unknown execution err=%v", err)
	}
	checkZero(p)
	p, err = eng.ListEvents(ctx, EventHistoryQuery{
		SagaName: "other-saga", BusinessKey: "order-err", IdempotencyKey: "req-err",
	})
	if !errors.Is(err, ErrExecutionNotFound) {
		t.Fatalf("saga name mismatch err=%v", err)
	}
	checkZero(p)

	// afterID 不存在 => ErrEventCursorNotFound。
	p, err = eng.ListEvents(ctx, EventHistoryQuery{
		SagaName: "order-saga", BusinessKey: "order-err", IdempotencyKey: "req-err",
		AfterID: "evt-does-not-exist",
	})
	if !errors.Is(err, ErrEventCursorNotFound) {
		t.Fatalf("bad cursor err=%v want ErrEventCursorNotFound", err)
	}
	checkZero(p)

	// afterID 属于同一存储中的另一执行（同业务键、不同幂等键）
	// => ErrEventCursorNotFound（游标 ID 真实存在但不属于目标执行）。
	err = store.Commit(ctx, "order-err", "req-other", func(tx Tx) error {
		tx.Create(&ExecutionState{
			SagaName: "order-saga", Fingerprint: "f", Status: StatusCompleted,
			Steps: []StepState{{Name: "s"}},
		})
		tx.AppendEvent(EventExecutionStarted, EventPayload{SagaName: "order-saga"})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sib, err := eng.ListEvents(ctx, EventHistoryQuery{
		SagaName: "order-saga", BusinessKey: "order-err", IdempotencyKey: "req-other",
	})
	if err != nil {
		t.Fatalf("query sibling execution: %v", err)
	}
	foreignID := sib.Records[0].ID
	p, err = eng.ListEvents(ctx, EventHistoryQuery{
		SagaName: "order-saga", BusinessKey: "order-err", IdempotencyKey: "req-err",
		AfterID: foreignID,
	})
	if !errors.Is(err, ErrEventCursorNotFound) {
		t.Fatalf("foreign cursor err=%v want ErrEventCursorNotFound", err)
	}
	checkZero(p)

	// Store 不支持历史查询 => ErrEventHistoryUnsupported。
	plain := &plainStore{inner: store}
	plainEng := NewEngine(plain)
	p, err = plainEng.ListEvents(ctx, EventHistoryQuery{
		SagaName: "order-saga", BusinessKey: "order-err", IdempotencyKey: "req-err",
	})
	if !errors.Is(err, ErrEventHistoryUnsupported) {
		t.Fatalf("unsupported store err=%v want ErrEventHistoryUnsupported", err)
	}
	checkZero(p)
}

func TestListEventsIsReadOnlyAgainstDeliveryState(t *testing.T) {
	store, eng, ids := historyTestStore(t, "order-ro", "req-ro")
	ctx := context.Background()

	// 领取两条保持在领取中；Ack 一条；退回另一条后它重新回到待投递。
	claimed, err := store.ClaimPendingEvents(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AckEvent(ctx, claimed[0].ID); err != nil {
		t.Fatal(err)
	}
	if err := store.NackEvent(ctx, claimed[1].ID); err != nil {
		t.Fatal(err)
	}

	snapshot := func() map[string]int {
		p, err := eng.ListEvents(ctx, EventHistoryQuery{
			SagaName: "order-saga", BusinessKey: "order-ro", IdempotencyKey: "req-ro",
		})
		if err != nil {
			t.Fatal(err)
		}
		m := map[string]int{}
		for _, r := range p.Records {
			m[r.ID] = r.Deliveries
		}
		return m
	}
	before := snapshot()
	for i := 0; i < 10; i++ {
		p, err := eng.ListEvents(ctx, EventHistoryQuery{
			SagaName: "order-saga", BusinessKey: "order-ro", IdempotencyKey: "req-ro",
			AfterID: ids[i%len(ids)], Limit: 2,
		})
		// 最后一条作游标时是空页，其余正常；都不应改变状态。
		if err != nil {
			t.Fatalf("query %d: %v", i, err)
		}
		_ = p
	}
	after := snapshot()
	if fmt.Sprint(after) != fmt.Sprint(before) {
		t.Fatalf("delivery metadata changed by reads: before=%v after=%v", before, after)
	}

	// 退回事件仍只可被领取一次：查询没有解除/增加任何锁定。
	batch, err := store.ClaimPendingEvents(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 4 {
		// e1 已 Ack；e2 在队列尾，e3/e4/e5 在前，全部可领。
		t.Fatalf("claimable=%d want 4", len(batch))
	}
	again, err := store.ClaimPendingEvents(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 0 {
		t.Fatalf("claimed events must still be locked after queries: %d", len(again))
	}
}

// eventSeq 解析事件 ID 的追加序号。
func eventSeq(id string) uint64 {
	n, err := strconv.ParseUint(strings.TrimPrefix(id, "evt-"), 10, 64)
	if err != nil {
		panic(err)
	}
	return n
}

// stepClock 每次取时递增 1ms，让一次 Commit 的事件共享同一发生时间，
// 不同 Commit 之间时间严格可分。
type stepClock struct {
	mu sync.Mutex
	t  time.Time
	d  time.Duration
}

func (c *stepClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	cur := c.t
	c.t = c.t.Add(c.d)
	return cur
}

func TestListEventsConcurrentAppendsKeepOrderAndAtomicBatches(t *testing.T) {
	clk := &stepClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), d: time.Millisecond}
	store := NewMemoryStore()
	store.now = clk.now
	ctx := context.Background()

	// 建立执行（占用第一个时间点）。
	if err := store.Commit(ctx, "bk", "ik", func(tx Tx) error {
		tx.Create(&ExecutionState{
			SagaName: "c", Fingerprint: "f", Status: StatusRunning,
			Steps: []StepState{{Name: "s"}},
		})
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	const writers = 8
	const batches = 25
	const perCommit = 3
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for b := 0; b < batches; b++ {
				err := store.Commit(ctx, "bk", "ik", func(tx Tx) error {
					for k := 0; k < perCommit; k++ {
						tx.AppendEvent("t", EventPayload{SagaName: "c"})
					}
					return nil
				})
				if err != nil {
					t.Errorf("commit: %v", err)
					return
				}
			}
		}()
	}

	// 与追加并发翻页：每一页都必须是内部有序、无重复、不超过上限的一致快照。
	stop := make(chan struct{})
	go func() {
		wg.Wait()
		close(stop)
	}()
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			p, err := store.ListEvents(ctx, EventHistoryQuery{
				SagaName: "c", BusinessKey: "bk", IdempotencyKey: "ik", Limit: 7,
			})
			if err != nil {
				t.Errorf("concurrent read: %v", err)
				return
			}
			seen := map[uint64]bool{}
			var prev uint64
			for i, r := range p.Records {
				seq := eventSeq(r.ID)
				if seen[seq] {
					t.Errorf("duplicate id within page: %s", r.ID)
				}
				seen[seq] = true
				if i > 0 && seq <= prev {
					t.Errorf("page not in append order: %d after %d", seq, prev)
				}
				prev = seq
			}
		}
	}()
	wg.Wait()
	<-readerDone

	// 追加结束后跨页走完全链：严格按追加顺序、无重复、总数精确。
	total := writers * batches * perCommit
	var all []EventRecord
	cursor := ""
	for {
		page, err := store.ListEvents(ctx, EventHistoryQuery{
			SagaName: "c", BusinessKey: "bk", IdempotencyKey: "ik",
			AfterID: cursor, Limit: 11,
		})
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, page.Records...)
		if !page.HasMore {
			break
		}
		cursor = page.NextAfterID
	}
	if len(all) != total {
		t.Fatalf("collected=%d want %d", len(all), total)
	}
	seen := map[uint64]bool{}
	var prev uint64
	for i, r := range all {
		seq := eventSeq(r.ID)
		if seen[seq] {
			t.Fatalf("duplicate event across pages: %s", r.ID)
		}
		seen[seq] = true
		if i > 0 && seq <= prev {
			t.Fatalf("cross-page order broken at %d: %d after %d", i, seq, prev)
		}
		prev = seq
	}
	// 原子性：每次 Commit 的 perCommit 条共享一个 OccurredAt，
	// 全链中每个发生时间都必须成组完整出现，不得观察到半组。
	groups := map[time.Time]int{}
	for _, r := range all {
		groups[r.OccurredAt]++
	}
	for at, n := range groups {
		if n != perCommit {
			t.Fatalf("non-atomic batch at %v: %d of %d events visible", at, n, perCommit)
		}
	}
}

// eventTypesPtr 返回指针事件切片的类型序列。
func eventTypesPtr(events []*Event) []string {
	out := make([]string, len(events))
	for i, e := range events {
		out[i] = e.Type
	}
	return out
}
