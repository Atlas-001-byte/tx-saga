package txsaga

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// deadLetterTestStore 创建使用假时钟的 MemoryStore，建立一条执行记录并
// 提交 n 条待投递事件（历史查询要求执行存在）。
func deadLetterTestStore(t *testing.T, n int) (*MemoryStore, *fakeClock, []string) {
	t.Helper()
	clk := newFakeClock()
	store := NewMemoryStore()
	store.now = clk.now
	err := store.Commit(context.Background(), "bk", "ik", func(tx Tx) error {
		tx.Create(&ExecutionState{
			SagaName: "dl-saga", Fingerprint: "fp", Status: StatusCompleted,
			Steps: []StepState{{Name: "s"}},
		})
		for i := 0; i < n; i++ {
			tx.AppendEvent(fmt.Sprintf("evt-type-%d", i), EventPayload{
				SagaName: "dl-saga", IdempotencyKey: "ik", Step: fmt.Sprintf("s%d", i),
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for i := 1; i <= n; i++ {
		ids = append(ids, fmt.Sprintf("evt-%020d", i))
	}
	return store, clk, ids
}

// publishErr 是永远失败的 Publisher。
func publishErr(err error) Publisher {
	return PublisherFunc(func(context.Context, Event) error { return err })
}

func TestDeadLetterBoundedDeliveryPlain(t *testing.T) {
	store, _, ids := deadLetterTestStore(t, 1)
	pubErr := errors.New("boom")
	relay := NewRelay(store, publishErr(pubErr), WithMaxDeliveries(2))

	res, err := relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Claimed != 1 || res.Failed != 1 || res.Delivered != 0 || res.DeadLettered != 0 {
		t.Fatalf("round1 = %+v, want Claimed=1 Failed=1", res)
	}
	if got := store.PendingCount(); got != 1 {
		t.Fatalf("pending after round1 = %d, want 1", got)
	}

	res, err = relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Claimed != 1 || res.Failed != 0 || res.DeadLettered != 1 {
		t.Fatalf("round2 = %+v, want Claimed=1 DeadLettered=1", res)
	}
	if got := store.PendingCount(); got != 0 {
		t.Fatalf("pending after dead letter = %d, want 0", got)
	}
	// 死信退出普通领取：后续轮次领不到任何事件。
	res, err = relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Claimed != 0 {
		t.Fatalf("dead-lettered event claimed again: %+v", res)
	}

	// 死信记录：ID、类型、发生时间、负载、累计 Deliveries、最近领取时间、
	// 失败原因与进入死信时间齐全。
	page, err := store.ListEvents(context.Background(), EventHistoryQuery{
		SagaName: "dl-saga", BusinessKey: "bk", IdempotencyKey: "ik",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 1 {
		t.Fatalf("history len = %d, want 1", len(page.Events))
	}
	rec := page.Events[0]
	if rec.ID != ids[0] || rec.Type != "evt-type-0" || rec.OccurredAt.IsZero() {
		t.Fatalf("dead letter identity = %+v", rec)
	}
	if rec.Status != EventStatusDeadLettered {
		t.Fatalf("status = %q, want %q", rec.Status, EventStatusDeadLettered)
	}
	if rec.Deliveries != 2 || rec.LastAttemptAt.IsZero() {
		t.Fatalf("deliveries=%d last=%v, want 2 and non-zero", rec.Deliveries, rec.LastAttemptAt)
	}
	if !strings.Contains(rec.DeadLetterReason, "boom") || rec.DeadLetteredAt.IsZero() {
		t.Fatalf("reason=%q at=%v", rec.DeadLetterReason, rec.DeadLetteredAt)
	}
	if _, ok := rec.Payload.(EventPayload); !ok {
		t.Fatalf("payload type = %T, want EventPayload", rec.Payload)
	}
}

func TestDeadLetterAckStillWorksWithLimit(t *testing.T) {
	store, _, _ := deadLetterTestStore(t, 2)
	relay := NewRelay(store, PublisherFunc(func(context.Context, Event) error { return nil }),
		WithMaxDeliveries(1))

	res, err := relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Claimed != 2 || res.Delivered != 2 || res.Failed != 0 || res.DeadLettered != 0 {
		t.Fatalf("res = %+v, want Claimed=2 Delivered=2", res)
	}
	page, err := store.ListEvents(context.Background(), EventHistoryQuery{
		SagaName: "dl-saga", BusinessKey: "bk", IdempotencyKey: "ik",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, rec := range page.Events {
		if rec.Status != EventStatusDelivered {
			t.Fatalf("event %s status = %q, want delivered", rec.ID, rec.Status)
		}
	}
}

func TestDeadLetterBatchIndependence(t *testing.T) {
	store, _, ids := deadLetterTestStore(t, 2)
	// 只让第一条事件失败；上限 1 使其立即转死信，第二条照常投递。
	relay := NewRelay(store, PublisherFunc(func(_ context.Context, ev Event) error {
		if ev.ID == ids[0] {
			return errors.New("boom")
		}
		return nil
	}), WithMaxDeliveries(1))

	res, err := relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Claimed != 2 || res.Delivered != 1 || res.DeadLettered != 1 || res.Failed != 0 {
		t.Fatalf("res = %+v, want Claimed=2 Delivered=1 DeadLettered=1", res)
	}
	page, err := store.ListEvents(context.Background(), EventHistoryQuery{
		SagaName: "dl-saga", BusinessKey: "bk", IdempotencyKey: "ik",
	})
	if err != nil {
		t.Fatal(err)
	}
	if page.Events[0].Status != EventStatusDeadLettered || page.Events[1].Status != EventStatusDelivered {
		t.Fatalf("statuses = %q/%q", page.Events[0].Status, page.Events[1].Status)
	}
}

// stubStore 仅实现 Store 的自定义存储：不支持租约、历史与死信。
type stubStore struct{}

func (stubStore) LoadSnapshot(context.Context, string, string) (Snapshot, error) {
	return Snapshot{}, nil
}
func (stubStore) Commit(context.Context, string, string, func(Tx) error) error { return nil }
func (stubStore) ClaimPendingEvents(context.Context, int) ([]*Event, error)    { return nil, nil }
func (stubStore) AckEvent(context.Context, string) error                       { return nil }
func (stubStore) NackEvent(context.Context, string) error                      { return nil }

func TestDeadLetterUnsupportedStore(t *testing.T) {
	relay := NewRelay(stubStore{}, publishErr(errors.New("x")), WithMaxDeliveries(3))
	if _, err := relay.DeliverOnce(context.Background()); !errors.Is(err, ErrDeadLetterUnsupported) {
		t.Fatalf("DeliverOnce err = %v, want ErrDeadLetterUnsupported", err)
	}
	if err := relay.Run(context.Background()); !errors.Is(err, ErrDeadLetterUnsupported) {
		t.Fatalf("Run err = %v, want ErrDeadLetterUnsupported", err)
	}
	// 未配置上限时同一 Store 行为不变。
	plain := NewRelay(stubStore{}, publishErr(errors.New("x")))
	if _, err := plain.DeliverOnce(context.Background()); err != nil {
		t.Fatalf("unlimited DeliverOnce err = %v, want nil", err)
	}
	// 引擎入口同样报告不支持。
	eng := NewEngine(stubStore{})
	if err := eng.RequeueDeadLetter(context.Background(), "evt-1"); !errors.Is(err, ErrDeadLetterUnsupported) {
		t.Fatalf("RequeueDeadLetter err = %v, want ErrDeadLetterUnsupported", err)
	}
}

func TestDeadLetterLeaseRecoveryExhausted(t *testing.T) {
	store, clk, ids := deadLetterTestStore(t, 1)
	ttl := time.Minute
	max := 1

	// 模拟 worker 崩溃：领取（本轮次数即达上限）后不退回。
	claims, err := store.ClaimPendingEventsLeased(context.Background(), ttl, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(claims) != 1 || claims[0].Event.RoundDeliveries() != 1 {
		t.Fatalf("claims = %+v", claims)
	}
	clk.advance(2 * ttl) // 租约到期

	published := 0
	relay := NewRelay(store, PublisherFunc(func(context.Context, Event) error {
		published++
		return nil
	}), WithClaimLease(ttl), WithMaxDeliveries(max))
	res, err := relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if published != 0 {
		t.Fatalf("exhausted event published %d times, want 0", published)
	}
	if res.Claimed != 1 || res.DeadLettered != 1 || res.Delivered != 0 {
		t.Fatalf("res = %+v, want Claimed=1 DeadLettered=1", res)
	}
	page, err := store.ListEvents(context.Background(), EventHistoryQuery{
		SagaName: "dl-saga", BusinessKey: "bk", IdempotencyKey: "ik",
	})
	if err != nil {
		t.Fatal(err)
	}
	if page.Events[0].Status != EventStatusDeadLettered {
		t.Fatalf("status = %q, want dead_lettered", page.Events[0].Status)
	}
	if page.Events[0].ID != ids[0] {
		t.Fatalf("dead letter id = %q, want %q", page.Events[0].ID, ids[0])
	}
}

func TestDeadLetterLeasedBoundedDelivery(t *testing.T) {
	store, _, ids := deadLetterTestStore(t, 1)
	ttl := time.Minute
	relay := NewRelay(store, publishErr(errors.New("boom")), WithClaimLease(ttl), WithMaxDeliveries(2))

	res, err := relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Claimed != 1 || res.Failed != 1 || res.DeadLettered != 0 {
		t.Fatalf("round1 = %+v, want Claimed=1 Failed=1", res)
	}
	res, err = relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Claimed != 1 || res.Failed != 0 || res.DeadLettered != 1 {
		t.Fatalf("round2 = %+v, want Claimed=1 DeadLettered=1", res)
	}
	if got := store.PendingCount(); got != 0 {
		t.Fatalf("pending = %d, want 0", got)
	}

	// 租约路径的死信事件重新入队后，队列中仍只有一份：普通领取不会
	// 在同一批领到两个副本。
	eng := NewEngine(store)
	if err := eng.RequeueDeadLetter(context.Background(), ids[0]); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimPendingEvents(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].ID != ids[0] {
		t.Fatalf("claimed after requeue = %+v, want single %s", claimed, ids[0])
	}
	if got := store.PendingCount(); got != 0 {
		t.Fatalf("pending after claim = %d, want 0", got)
	}
}

func TestRequeueDeadLetter(t *testing.T) {
	store, _, ids := deadLetterTestStore(t, 1)
	pubErr := errors.New("boom")
	relay := NewRelay(store, publishErr(pubErr), WithMaxDeliveries(1))
	if _, err := relay.DeliverOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	eng := NewEngine(store)
	before, err := store.ListEvents(context.Background(), EventHistoryQuery{
		SagaName: "dl-saga", BusinessKey: "bk", IdempotencyKey: "ik",
	})
	if err != nil {
		t.Fatal(err)
	}
	if before.Events[0].Status != EventStatusDeadLettered {
		t.Fatalf("precondition status = %q", before.Events[0].Status)
	}

	if err := eng.RequeueDeadLetter(context.Background(), ids[0]); err != nil {
		t.Fatal(err)
	}
	// 重新入队后：回到待投递，累计 Deliveries 与历史失败信息不回退。
	rec, err := store.ListEvents(context.Background(), EventHistoryQuery{
		SagaName: "dl-saga", BusinessKey: "bk", IdempotencyKey: "ik",
	})
	if err != nil {
		t.Fatal(err)
	}
	if rec.Events[0].Status != EventStatusPending {
		t.Fatalf("status after requeue = %q, want pending", rec.Events[0].Status)
	}
	if rec.Events[0].Deliveries != 1 {
		t.Fatalf("cumulative deliveries = %d, want 1 (不回退)", rec.Events[0].Deliveries)
	}
	if rec.Events[0].DeadLetterReason == "" || rec.Events[0].DeadLetteredAt.IsZero() {
		t.Fatalf("dead letter history lost: %+v", rec.Events[0])
	}
	if len(rec.Events) != 1 || rec.Events[0].ID != before.Events[0].ID {
		t.Fatalf("requeue appended or reordered events: %+v", rec.Events)
	}

	// 重复调用：不追加事件、不改变状态，报 ErrEventNotDeadLettered。
	if err := eng.RequeueDeadLetter(context.Background(), ids[0]); !errors.Is(err, ErrEventNotDeadLettered) {
		t.Fatalf("second requeue err = %v, want ErrEventNotDeadLettered", err)
	}
	if got := store.PendingCount(); got != 1 {
		t.Fatalf("pending after duplicate requeue = %d, want 1", got)
	}

	// 新一轮有限计数：再次耗尽后仍进入死信。
	res, err := relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Claimed != 1 || res.DeadLettered != 1 {
		t.Fatalf("second round = %+v, want Claimed=1 DeadLettered=1", res)
	}
	rec, err = store.ListEvents(context.Background(), EventHistoryQuery{
		SagaName: "dl-saga", BusinessKey: "bk", IdempotencyKey: "ik",
	})
	if err != nil {
		t.Fatal(err)
	}
	if rec.Events[0].Status != EventStatusDeadLettered || rec.Events[0].Deliveries != 2 {
		t.Fatalf("after second exhaustion: %+v", rec.Events[0])
	}
}

func TestRequeueDeadLetterErrors(t *testing.T) {
	store, _, ids := deadLetterTestStore(t, 2)
	eng := NewEngine(store)

	// 事件不存在。
	if err := eng.RequeueDeadLetter(context.Background(), "evt-99999999999999999999"); !errors.Is(err, ErrEventNotFound) {
		t.Fatalf("missing event err = %v, want ErrEventNotFound", err)
	}
	if err := eng.RequeueDeadLetter(context.Background(), ""); !errors.Is(err, ErrEventNotFound) {
		t.Fatalf("empty id err = %v, want ErrEventNotFound", err)
	}
	// 尚可投递（待投递）。
	if err := eng.RequeueDeadLetter(context.Background(), ids[0]); !errors.Is(err, ErrEventNotDeadLettered) {
		t.Fatalf("pending event err = %v, want ErrEventNotDeadLettered", err)
	}
	// 领取中。
	if _, err := store.ClaimPendingEvents(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if err := eng.RequeueDeadLetter(context.Background(), ids[0]); !errors.Is(err, ErrEventNotDeadLettered) {
		t.Fatalf("claimed event err = %v, want ErrEventNotDeadLettered", err)
	}
	// 已投递（Ack）。
	if err := store.AckEvent(context.Background(), ids[0]); err != nil {
		t.Fatal(err)
	}
	if err := eng.RequeueDeadLetter(context.Background(), ids[0]); !errors.Is(err, ErrEventNotDeadLettered) {
		t.Fatalf("acked event err = %v, want ErrEventNotDeadLettered", err)
	}
	// context 取消：原样返回其错误，事件不变。
	if err := store.NackEvent(context.Background(), ids[1]); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := eng.RequeueDeadLetter(ctx, ids[1]); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled err = %v, want context.Canceled", err)
	}
	if got := store.PendingCount(); got != 1 {
		t.Fatalf("pending after canceled requeue = %d, want 1", got)
	}
}

func TestDeadLetterCancelBeforePublish(t *testing.T) {
	store, _, _ := deadLetterTestStore(t, 1)
	relay := NewRelay(store, PublisherFunc(func(context.Context, Event) error {
		t.Fatal("publish must not be called")
		return nil
	}), WithMaxDeliveries(1))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res, err := relay.DeliverOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// 取消退回：不计成功、不转死信，事件仍可领取。
	if res.Delivered != 0 || res.DeadLettered != 0 || res.Failed != 1 {
		t.Fatalf("res = %+v, want Failed=1 only", res)
	}
	if got := store.PendingCount(); got != 1 {
		t.Fatalf("pending = %d, want 1", got)
	}
}

func TestEventRecordStatusTransitions(t *testing.T) {
	store, clk, ids := deadLetterTestStore(t, 1)
	q := EventHistoryQuery{SagaName: "dl-saga", BusinessKey: "bk", IdempotencyKey: "ik"}

	status := func() string {
		page, err := store.ListEvents(context.Background(), q)
		if err != nil {
			t.Fatal(err)
		}
		return page.Events[0].Status
	}

	if got := status(); got != EventStatusPending {
		t.Fatalf("initial status = %q, want pending", got)
	}
	claims, err := store.ClaimPendingEventsLeased(context.Background(), time.Minute, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := status(); got != EventStatusClaimed {
		t.Fatalf("claimed status = %q, want claimed", got)
	}
	// 租约到期后视为待投递（可恢复）。
	clk.advance(2 * time.Minute)
	if got := status(); got != EventStatusPending {
		t.Fatalf("expired lease status = %q, want pending", got)
	}
	claims, err = store.ClaimPendingEventsLeased(context.Background(), time.Minute, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AckLeasedEvent(context.Background(), ids[0], claims[0].ClaimID); err != nil {
		t.Fatal(err)
	}
	if got := status(); got != EventStatusDelivered {
		t.Fatalf("acked status = %q, want delivered", got)
	}
}

func TestFailEventStaleClaim(t *testing.T) {
	store, clk, ids := deadLetterTestStore(t, 1)
	ttl := time.Minute

	claims, err := store.ClaimPendingEventsLeased(context.Background(), ttl, 1)
	if err != nil {
		t.Fatal(err)
	}
	clk.advance(2 * ttl) // 旧租约到期，事件被重新领取
	fresh, err := store.ClaimPendingEventsLeased(context.Background(), ttl, 1)
	if err != nil {
		t.Fatal(err)
	}
	// 旧 ClaimID 的 FailEvent 不得改动事件与新租约。
	if _, err := store.FailEvent(context.Background(), ids[0], claims[0].ClaimID, "x", 1); !errors.Is(err, ErrStaleClaim) {
		t.Fatalf("stale FailEvent err = %v, want ErrStaleClaim", err)
	}
	page, err := store.ListEvents(context.Background(), EventHistoryQuery{
		SagaName: "dl-saga", BusinessKey: "bk", IdempotencyKey: "ik",
	})
	if err != nil {
		t.Fatal(err)
	}
	if page.Events[0].Status != EventStatusClaimed {
		t.Fatalf("status after stale fail = %q, want claimed", page.Events[0].Status)
	}
	// 当前 ClaimID 正常退回。
	dead, err := store.FailEvent(context.Background(), ids[0], fresh[0].ClaimID, "x", 5)
	if err != nil || dead {
		t.Fatalf("FailEvent = %v, %v; want false, nil", dead, err)
	}
	if got := store.PendingCount(); got != 1 {
		t.Fatalf("pending after fail = %d, want 1", got)
	}
}

func TestRequeueConcurrentExclusivity(t *testing.T) {
	store, _, ids := deadLetterTestStore(t, 1)
	relay := NewRelay(store, publishErr(errors.New("boom")), WithMaxDeliveries(1))
	if _, err := relay.DeliverOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	eng := NewEngine(store)

	// 并发重新入队与领取：任一时刻事件只有一个当前领取或一个待投递状态，
	// 且不会制造第二个副本。
	const workers = 8
	errCh := make(chan error, workers*2)
	for i := 0; i < workers; i++ {
		go func() {
			if err := eng.RequeueDeadLetter(context.Background(), ids[0]); err != nil &&
				!errors.Is(err, ErrEventNotDeadLettered) {
				errCh <- err
				return
			}
			errCh <- nil
		}()
		go func() {
			_, err := store.ClaimPendingEvents(context.Background(), 1)
			errCh <- err
		}()
	}
	for i := 0; i < workers*2; i++ {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}
	page, err := store.ListEvents(context.Background(), EventHistoryQuery{
		SagaName: "dl-saga", BusinessKey: "bk", IdempotencyKey: "ik",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 1 {
		t.Fatalf("history len = %d, want 1 (无副本)", len(page.Events))
	}
	if got := store.TotalEvents(); got != 1 {
		t.Fatalf("total events = %d, want 1", got)
	}
	// 最终事件要么待投递一次、要么被领取一次：pending+claimed 恰为 1。
	claimed := 0
	if st := page.Events[0].Status; st == EventStatusClaimed {
		claimed = 1
	}
	if got := store.PendingCount() + claimed; got != 1 {
		t.Fatalf("pending+claimed = %d, want 1", got)
	}
}
