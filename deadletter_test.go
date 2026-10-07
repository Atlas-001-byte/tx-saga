package txsaga

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// boundedTestStore 创建使用假时钟的 MemoryStore，并提交 n 条待投递事件，
// 返回存储、时钟与事件 ID（与 leasedTestStore 同口径）。
func boundedTestStore(t *testing.T, n int) (*MemoryStore, *fakeClock, []string) {
	t.Helper()
	return leasedTestStore(t, n)
}

// deadLetterEngine 准备一个已完成的一步执行，返回引擎、存储与该执行的
// 事件 ID（按追加顺序），供 EventRecord 状态断言使用。
func deadLetterEngine(t *testing.T) (*Engine, *MemoryStore, []string) {
	t.Helper()
	store := NewMemoryStore()
	eng := NewEngine(store)
	def := Definition{
		Name: "dl-saga",
		Steps: []Step{
			{Name: "only", Action: func(context.Context, ExecutionView) error { return nil }},
		},
	}
	_, err := eng.Execute(context.Background(), def, ExecutionRequest{
		BusinessKey: "bk-dl", IdempotencyKey: "ik-dl",
	})
	if err != nil {
		t.Fatal(err)
	}
	page, err := eng.ListEvents(context.Background(), EventHistoryQuery{
		SagaName: "dl-saga", BusinessKey: "bk-dl", IdempotencyKey: "ik-dl",
	})
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, len(page.Events))
	for i, r := range page.Events {
		ids[i] = r.ID
	}
	return eng, store, ids
}

func listRecord(t *testing.T, eng *Engine, id string) EventRecord {
	t.Helper()
	page, err := eng.ListEvents(context.Background(), EventHistoryQuery{
		SagaName: "dl-saga", BusinessKey: "bk-dl", IdempotencyKey: "ik-dl",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range page.Events {
		if r.ID == id {
			return r
		}
	}
	t.Fatalf("event %s not in history", id)
	return EventRecord{}
}

func failPublisher(err error) Publisher {
	return PublisherFunc(func(context.Context, Event) error { return err })
}

func TestBoundedDeliveryDeadLettersAtLimit(t *testing.T) {
	store, _, ids := boundedTestStore(t, 1)
	pubErr := errors.New("downstream unavailable")
	relay := NewRelay(store, failPublisher(pubErr), WithMaxDeliveries(2))

	// 第 1 轮：领取（Deliveries=1），失败未达上限，Nack 保留。
	res, err := relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Claimed != 1 || res.Failed != 1 || res.Delivered != 0 || res.DeadLettered != 0 {
		t.Fatalf("round1 res=%+v", res)
	}
	if store.PendingCount() != 1 {
		t.Fatalf("event must be retained below the limit: pending=%d", store.PendingCount())
	}

	// 第 2 轮：领取（Deliveries=2），失败达到上限，同一操作转死信。
	res, err = relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Claimed != 1 || res.Failed != 0 || res.Delivered != 0 || res.DeadLettered != 1 {
		t.Fatalf("round2 res=%+v", res)
	}

	// 死信退出普通领取：后续轮次领不到；事件仍保留在存储中。
	res, err = relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Claimed != 0 || res.DeadLettered != 0 {
		t.Fatalf("round3 res=%+v", res)
	}
	if store.PendingCount() != 0 || store.TotalEvents() != 1 {
		t.Fatalf("dead letter must stay in store out of pending: pending=%d total=%d",
			store.PendingCount(), store.TotalEvents())
	}

	// 历史记录：死信状态、累计 Deliveries、失败原因与进入死信时间。
	rec := eventRecordByID(t, store, ids[0])
	if rec.Status != EventStatusDeadLettered {
		t.Fatalf("status=%q want %q", rec.Status, EventStatusDeadLettered)
	}
	if rec.Deliveries != 2 {
		t.Fatalf("deliveries=%d want 2", rec.Deliveries)
	}
	if rec.LastError != pubErr.Error() {
		t.Fatalf("last error=%q want %q", rec.LastError, pubErr.Error())
	}
	if rec.DeadLetteredAt.IsZero() || rec.LastAttemptAt.IsZero() {
		t.Fatalf("dead-letter metadata missing: deadAt=%v lastAttempt=%v",
			rec.DeadLetteredAt, rec.LastAttemptAt)
	}
}

// eventRecordByID 直接扫描 MemoryStore 审计链取一条记录（不依赖执行记录）。
func eventRecordByID(t *testing.T, store *MemoryStore, id string) EventRecord {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, me := range store.history {
		if me.event.ID == id {
			return toEventRecord(me, store.now())
		}
	}
	t.Fatalf("event %s not in history", id)
	return EventRecord{}
}

func TestBoundedDeliverySuccessStillAcks(t *testing.T) {
	store, _, ids := boundedTestStore(t, 2)
	relay := NewRelay(store, PublisherFunc(func(context.Context, Event) error { return nil }),
		WithMaxDeliveries(1))

	res, err := relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Claimed != 2 || res.Delivered != 2 || res.Failed != 0 || res.DeadLettered != 0 {
		t.Fatalf("res=%+v", res)
	}
	if store.TotalEvents() != 0 {
		t.Fatalf("acked events must leave the live outbox: total=%d", store.TotalEvents())
	}
	for _, id := range ids {
		if rec := eventRecordByID(t, store, id); rec.Status != EventStatusDelivered {
			t.Fatalf("event %s status=%q want delivered", id, rec.Status)
		}
	}
}

func TestBoundedDeliveryBatchIndependent(t *testing.T) {
	store, _, ids := boundedTestStore(t, 3)
	// 只有第二条事件发送失败；max=1 时它立即死信，其余照常 Ack。
	relay := NewRelay(store, PublisherFunc(func(_ context.Context, ev Event) error {
		if ev.ID == ids[1] {
			return errors.New("boom")
		}
		return nil
	}), WithMaxDeliveries(1))

	res, err := relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Claimed != 3 || res.Delivered != 2 || res.Failed != 0 || res.DeadLettered != 1 {
		t.Fatalf("res=%+v", res)
	}
	if rec := eventRecordByID(t, store, ids[1]); rec.Status != EventStatusDeadLettered {
		t.Fatalf("failed event status=%q want dead_lettered", rec.Status)
	}
	for _, id := range []string{ids[0], ids[2]} {
		if rec := eventRecordByID(t, store, id); rec.Status != EventStatusDelivered {
			t.Fatalf("event %s status=%q want delivered", id, rec.Status)
		}
	}
}

func TestUnconfiguredRelayKeepsUnlimitedRedelivery(t *testing.T) {
	store, _, ids := boundedTestStore(t, 1)
	relay := NewRelay(store, failPublisher(errors.New("always fails")))

	for round := 1; round <= 3; round++ {
		res, err := relay.DeliverOnce(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if res.Claimed != 1 || res.Failed != 1 || res.DeadLettered != 0 {
			t.Fatalf("round %d res=%+v", round, res)
		}
	}
	// 未配置上限：不实现死信接口的用法下事件始终保留待投递。
	rec := eventRecordByID(t, store, ids[0])
	if rec.Status != EventStatusPending || rec.Deliveries != 3 {
		t.Fatalf("rec=%+v", rec)
	}
}

// plainStore（lease_test.go）只实现 Store 接口（无死信语义），
// 用于验证 ErrDeadLetterUnsupported。
func TestMaxDeliveriesUnsupportedStore(t *testing.T) {
	mem, _, _ := boundedTestStore(t, 1)
	store := &plainStore{inner: mem}
	relay := NewRelay(store, PublisherFunc(func(context.Context, Event) error { return nil }),
		WithMaxDeliveries(2))

	if _, err := relay.DeliverOnce(context.Background()); !errors.Is(err, ErrDeadLetterUnsupported) {
		t.Fatalf("DeliverOnce err=%v want ErrDeadLetterUnsupported", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := relay.Run(ctx); !errors.Is(err, ErrDeadLetterUnsupported) {
		t.Fatalf("Run err=%v want ErrDeadLetterUnsupported", err)
	}

	// 同时启用租约也一样。
	relayLeased := NewRelay(store, PublisherFunc(func(context.Context, Event) error { return nil }),
		WithClaimLease(time.Minute), WithMaxDeliveries(2))
	if _, err := relayLeased.DeliverOnce(context.Background()); !errors.Is(err, ErrDeadLetterUnsupported) {
		t.Fatalf("leased DeliverOnce err=%v want ErrDeadLetterUnsupported", err)
	}

	// 不配置上限时同一 Store 正常工作（至少一次重投语义不变）。
	plain := NewRelay(store, PublisherFunc(func(context.Context, Event) error { return nil }))
	res, err := plain.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Delivered != 1 {
		t.Fatalf("unbounded res=%+v", res)
	}
}

func TestLeaseRecoveryIsolatesExhaustedEvent(t *testing.T) {
	store, clk, ids := boundedTestStore(t, 1)
	ttl := time.Minute

	// 模拟 worker 领取（Deliveries=1，已达 max=1）后在 Publish 前退出：
	// 不 Ack、不 Nack，等待租约到期。
	claims, dead, err := store.ClaimPendingEventsLeasedBounded(context.Background(), ttl, 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claims) != 1 || dead != 0 {
		t.Fatalf("claims=%d dead=%d", len(claims), dead)
	}
	clk.advance(2 * ttl)

	// 租约到期恢复：本轮次数已用尽，直接隔离为死信，不再调用 Publisher。
	publishCalled := false
	relay := NewRelay(store, PublisherFunc(func(context.Context, Event) error {
		publishCalled = true
		return nil
	}), WithClaimLease(ttl), WithMaxDeliveries(1))
	res, err := relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Claimed != 0 || res.DeadLettered != 1 {
		t.Fatalf("res=%+v", res)
	}
	if publishCalled {
		t.Fatal("exhausted event must not reach Publisher")
	}
	rec := eventRecordByID(t, store, ids[0])
	if rec.Status != EventStatusDeadLettered || rec.Deliveries != 1 {
		t.Fatalf("rec=%+v", rec)
	}
}

func TestLeasedBoundedNackDeadLettersAtLimit(t *testing.T) {
	store, _, ids := boundedTestStore(t, 1)
	relay := NewRelay(store, failPublisher(errors.New("nope")),
		WithClaimLease(time.Minute), WithMaxDeliveries(1))

	res, err := relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Claimed != 1 || res.Failed != 0 || res.DeadLettered != 1 {
		t.Fatalf("res=%+v", res)
	}
	if rec := eventRecordByID(t, store, ids[0]); rec.Status != EventStatusDeadLettered {
		t.Fatalf("status=%q want dead_lettered", rec.Status)
	}
	// 死信不再被租约领取。
	res, err = relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Claimed != 0 || res.DeadLettered != 0 {
		t.Fatalf("res=%+v", res)
	}
}

func TestRequeueDeadLetterRestartsBoundedRound(t *testing.T) {
	store, _, ids := boundedTestStore(t, 1)
	eng := NewEngine(store)
	pubErr := errors.New("flaky downstream")
	relay := NewRelay(store, failPublisher(pubErr), WithMaxDeliveries(2))

	// 两轮失败：Deliveries=2，进入死信。
	for i := 0; i < 2; i++ {
		if _, err := relay.DeliverOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if rec := eventRecordByID(t, store, ids[0]); rec.Status != EventStatusDeadLettered {
		t.Fatalf("status=%q want dead_lettered", rec.Status)
	}

	// 重新入队：恢复待投递，累计 Deliveries 与历史失败信息不回退。
	if err := eng.RequeueDeadLetterEvent(context.Background(), ids[0]); err != nil {
		t.Fatal(err)
	}
	rec := eventRecordByID(t, store, ids[0])
	if rec.Status != EventStatusPending {
		t.Fatalf("status=%q want pending after requeue", rec.Status)
	}
	if rec.Deliveries != 2 || rec.LastError != pubErr.Error() || rec.DeadLetteredAt.IsZero() {
		t.Fatalf("history must be preserved after requeue: %+v", rec)
	}

	// 新一轮有限计数从零开始：还能再失败两次才死信。
	res, err := relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Failed != 1 || res.DeadLettered != 0 {
		t.Fatalf("round1 after requeue res=%+v", res)
	}
	res, err = relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.DeadLettered != 1 {
		t.Fatalf("round2 after requeue res=%+v", res)
	}
	rec = eventRecordByID(t, store, ids[0])
	if rec.Status != EventStatusDeadLettered || rec.Deliveries != 4 {
		t.Fatalf("rec=%+v want dead_lettered with cumulative deliveries 4", rec)
	}
}

func TestRequeueDeadLetterErrors(t *testing.T) {
	store, _, ids := boundedTestStore(t, 3)
	eng := NewEngine(store)
	relay := NewRelay(store, failPublisher(errors.New("x")), WithMaxDeliveries(1))

	// 一轮全部失败：三条事件都进入死信。
	if _, err := relay.DeliverOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	// ids[0] 重新入队恢复待投递；ids[1] 保持死信；ids[2] 直接 Ack 成已投递。
	if err := eng.RequeueDeadLetterEvent(context.Background(), ids[0]); err != nil {
		t.Fatal(err)
	}
	if err := store.AckEvent(context.Background(), ids[2]); err != nil {
		t.Fatal(err)
	}

	// 事件不存在。
	if err := eng.RequeueDeadLetterEvent(context.Background(), "evt-99999999999999999999"); !errors.Is(err, ErrEventNotFound) {
		t.Fatalf("requeue missing err=%v want ErrEventNotFound", err)
	}
	// 尚可投递（待投递）。
	if err := eng.RequeueDeadLetterEvent(context.Background(), ids[0]); !errors.Is(err, ErrEventNotDeadLettered) {
		t.Fatalf("requeue pending err=%v want ErrEventNotDeadLettered", err)
	}
	// 已投递。
	if err := eng.RequeueDeadLetterEvent(context.Background(), ids[2]); !errors.Is(err, ErrEventNotDeadLettered) {
		t.Fatalf("requeue delivered err=%v want ErrEventNotDeadLettered", err)
	}

	// ctx 取消：返回该错误且不改变事件。
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := eng.RequeueDeadLetterEvent(ctx, ids[1]); !errors.Is(err, context.Canceled) {
		t.Fatalf("requeue canceled err=%v want context.Canceled", err)
	}
	if rec := eventRecordByID(t, store, ids[1]); rec.Status != EventStatusDeadLettered {
		t.Fatalf("canceled requeue changed event: status=%q", rec.Status)
	}

	// 成功后重复调用：事件已是待投递，返回 ErrEventNotDeadLettered，
	// 且不追加事件、不改变历史。
	before := eventHistoryLen(t, store)
	if err := eng.RequeueDeadLetterEvent(context.Background(), ids[1]); err != nil {
		t.Fatal(err)
	}
	if err := eng.RequeueDeadLetterEvent(context.Background(), ids[1]); !errors.Is(err, ErrEventNotDeadLettered) {
		t.Fatalf("second requeue err=%v want ErrEventNotDeadLettered", err)
	}
	if got := eventHistoryLen(t, store); got != before {
		t.Fatalf("requeue appended events: before=%d after=%d", before, got)
	}
}

func eventHistoryLen(t *testing.T, store *MemoryStore) int {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	return len(store.history)
}

func TestRequeueUnsupportedStore(t *testing.T) {
	mem, _, _ := boundedTestStore(t, 1)
	eng := NewEngine(&plainStore{inner: mem})
	if err := eng.RequeueDeadLetterEvent(context.Background(), "evt-00000000000000000001"); !errors.Is(err, ErrDeadLetterUnsupported) {
		t.Fatalf("err=%v want ErrDeadLetterUnsupported", err)
	}
}

func TestEventRecordStatusTransitions(t *testing.T) {
	eng, store, ids := deadLetterEngine(t)
	if len(ids) == 0 {
		t.Fatal("no events")
	}
	id := ids[0]

	if rec := listRecord(t, eng, id); rec.Status != EventStatusPending {
		t.Fatalf("fresh status=%q want pending", rec.Status)
	}

	// 领取中。
	if _, err := store.ClaimPendingEvents(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if rec := listRecord(t, eng, id); rec.Status != EventStatusClaimed {
		t.Fatalf("claimed status=%q want claimed", rec.Status)
	}

	// 已投递：Ack 后审计副本保留，状态为 delivered。
	if err := store.AckEvent(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	rec := listRecord(t, eng, id)
	if rec.Status != EventStatusDelivered || rec.Deliveries != 1 {
		t.Fatalf("delivered rec=%+v", rec)
	}
}

func TestBoundedCancelBeforePublishRetainsWithoutDeadLetter(t *testing.T) {
	store, _, ids := boundedTestStore(t, 1)
	publishCalled := false
	relay := NewRelay(store, PublisherFunc(func(context.Context, Event) error {
		publishCalled = true
		return nil
	}), WithMaxDeliveries(1))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res, err := relay.DeliverOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Publish 前取消：退回保留、不计成功，也不消耗投递上限转死信。
	if res.Claimed != 1 || res.Failed != 1 || res.Delivered != 0 || res.DeadLettered != 0 {
		t.Fatalf("res=%+v", res)
	}
	if publishCalled {
		t.Fatal("Publish must not be called after cancel")
	}
	if rec := eventRecordByID(t, store, ids[0]); rec.Status != EventStatusPending {
		t.Fatalf("status=%q want pending", rec.Status)
	}
}

func TestRequeueConcurrentWithDelivery(t *testing.T) {
	store, _, ids := boundedTestStore(t, 8)
	eng := NewEngine(store)
	// 全部先死信（max=1）。
	dead := NewRelay(store, failPublisher(errors.New("x")), WithMaxDeliveries(1))
	if _, err := dead.DeliverOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	// 并发：一半 worker 重新入队，一半 worker 持续投递（随机成败）。
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				_ = eng.RequeueDeadLetterEvent(context.Background(), id)
			}
		}(id)
	}
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			relay := NewRelay(store, PublisherFunc(func(context.Context, Event) error { return nil }),
				WithMaxDeliveries(1))
			for i := 0; i < 20; i++ {
				if _, err := relay.DeliverOnce(context.Background()); err != nil {
					return
				}
			}
		}()
	}
	wg.Wait()

	// 不变量：每条事件要么已投递（审计副本），要么在存储中恰好出现一次；
	// 历史链没有因为并发重新入队而追加任何事件。
	if got := eventHistoryLen(t, store); got != len(ids) {
		t.Fatalf("history grew: %d want %d", got, len(ids))
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	seen := map[string]int{}
	for _, id := range store.pending {
		seen[id]++
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("event %s appears %d times in pending queue", id, n)
		}
	}
	for _, me := range store.events {
		if me.dead && me.claimed {
			t.Fatalf("event %s both dead and claimed", me.event.ID)
		}
	}
}
