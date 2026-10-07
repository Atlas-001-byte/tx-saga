package filestore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/txsaga/txsaga"
)

// appendEvents 通过一次 Commit 追加 n 条事件（不建执行，与内存测试同口径），
// 返回事件 ID。
func appendEvents(t *testing.T, s *FileStore, bk, ik string, n int) []string {
	t.Helper()
	err := s.Commit(context.Background(), bk, ik, func(tx txsaga.Tx) error {
		for i := 0; i < n; i++ {
			tx.AppendEvent(fmt.Sprintf("evt-type-%d", i), txsaga.EventPayload{
				SagaName: "lease-saga", IdempotencyKey: ik, Step: fmt.Sprintf("s%d", i),
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, n)
	for i := 1; i <= n; i++ {
		ids[i-1] = fmt.Sprintf("evt-%020d", i)
	}
	return ids
}

func TestClaim_PlainClaimLocksUntilAckNack(t *testing.T) {
	s := newTestStore(t)
	ids := appendEvents(t, s, "bk", "ik", 3)

	got, err := s.ClaimPendingEvents(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("claimed=%d want 2", len(got))
	}
	// 保持追加顺序。
	if got[0].ID != ids[0] || got[1].ID != ids[1] {
		t.Fatalf("claim order=%s,%s want %s,%s", got[0].ID, got[1].ID, ids[0], ids[1])
	}
	for _, ev := range got {
		if ev.Deliveries() != 1 {
			t.Fatalf("deliveries=%d want 1", ev.Deliveries())
		}
	}
	// 已领取的两条不可再领，仅剩第三条。
	again, _ := s.ClaimPendingEvents(context.Background(), 5)
	if len(again) != 1 || again[0].ID != ids[2] {
		t.Fatalf("second claim=%v want only %s", again, ids[2])
	}
	// Nack 第一条后可重新领取，投递计数累计为 2。
	if err := s.NackEvent(context.Background(), ids[0]); err != nil {
		t.Fatal(err)
	}
	re, _ := s.ClaimPendingEvents(context.Background(), 5)
	if len(re) != 1 || re[0].ID != ids[0] || re[0].Deliveries() != 2 {
		t.Fatalf("reclaim after nack=%+v", re)
	}
	// Ack 后事件永久离开 Outbox。
	if err := s.AckEvent(context.Background(), ids[0]); err != nil {
		t.Fatal(err)
	}
	if err := s.AckEvent(context.Background(), ids[1]); err != nil {
		t.Fatal(err)
	}
	if err := s.AckEvent(context.Background(), ids[2]); err != nil {
		t.Fatal(err)
	}
	if s.TotalEvents() != 0 || s.PendingCount() != 0 {
		t.Fatalf("outbox after ack: total=%d pending=%d", s.TotalEvents(), s.PendingCount())
	}
	// Ack 不存在/已 Ack 的事件报错。
	if err := s.AckEvent(context.Background(), ids[0]); err == nil {
		t.Fatal("Ack of removed event should error")
	}
	// Nack 从未领取的存活事件幂等无错，也不会在队列中制造重复。
	if err := s.Commit(context.Background(), "bk2", "ik", func(tx txsaga.Tx) error {
		tx.AppendEvent("t", txsaga.EventPayload{SagaName: "s", IdempotencyKey: "ik"})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	idle := s.TotalEvents()
	if idle != 1 {
		t.Fatalf("fresh event total=%d want 1", idle)
	}
	freshID := fmt.Sprintf("evt-%020d", 4)
	if err := s.NackEvent(context.Background(), freshID); err != nil {
		t.Fatalf("nack unclaimed live event: %v", err)
	}
	if s.PendingCount() != 1 || s.TotalEvents() != 1 {
		t.Fatalf("idempotent nack changed store: pending=%d total=%d",
			s.PendingCount(), s.TotalEvents())
	}
}

func TestClaim_LeaseLifecycle(t *testing.T) {
	s := newTestStore(t)
	clk := withFakeClock(s)
	ids := appendEvents(t, s, "bk", "ik", 3)

	const ttl = time.Minute
	claims, err := s.ClaimPendingEventsLeased(context.Background(), ttl, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(claims) != 3 {
		t.Fatalf("leased=%d want 3", len(claims))
	}
	wantUntil := clk.now().Add(ttl)
	for i, c := range claims {
		if c.Event.ID != ids[i] {
			t.Fatalf("lease order[%d]=%s want %s", i, c.Event.ID, ids[i])
		}
		if !c.ClaimedUntil.Equal(wantUntil) {
			t.Fatalf("until=%v want %v", c.ClaimedUntil, wantUntil)
		}
		if c.ClaimID == "" || c.Event.Deliveries() != 1 || !c.Event.LastAttemptAt().Equal(clk.now()) {
			t.Fatalf("lease meta wrong: %+v", c)
		}
	}
	if s.PendingCount() != 0 {
		t.Fatalf("PendingCount=%d want 0 during lease", s.PendingCount())
	}
	if s.TotalEvents() != 3 {
		t.Fatalf("TotalEvents=%d want 3", s.TotalEvents())
	}
	// 租约内普通领取也取不到。
	if plain, _ := s.ClaimPendingEvents(context.Background(), 10); len(plain) != 0 {
		t.Fatalf("plain claim during lease=%d", len(plain))
	}

	// 过期 ClaimID 的 Ack/Nack 返回 ErrStaleClaim，且不影响新租约。
	clk.advance(ttl + time.Second)
	oldClaim := claims[0].ClaimID
	// 到期后重新领取第一条，获得新 ClaimID。
	reclaimed, err := s.ClaimPendingEventsLeased(context.Background(), ttl, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(reclaimed) != 1 || reclaimed[0].Event.ID != ids[0] || reclaimed[0].ClaimID == oldClaim {
		t.Fatalf("reclaim after expiry=%+v", reclaimed)
	}
	if reclaimed[0].Event.Deliveries() != 2 {
		t.Fatalf("deliveries after reclaim=%d want 2", reclaimed[0].Event.Deliveries())
	}
	err = s.AckLeasedEvent(context.Background(), ids[0], oldClaim)
	if !errors.Is(err, txsaga.ErrStaleClaim) {
		t.Fatalf("stale ack err=%v want ErrStaleClaim", err)
	}
	err = s.NackLeasedEvent(context.Background(), ids[0], oldClaim)
	if !errors.Is(err, txsaga.ErrStaleClaim) {
		t.Fatalf("stale nack err=%v want ErrStaleClaim", err)
	}
	// 旧租约操作不得移除事件。
	if s.TotalEvents() != 3 {
		t.Fatalf("stale op removed event: total=%d", s.TotalEvents())
	}

	// 当前 ClaimID 可 Nack：立即解除租约、可被领取，且顺序位置不变。
	cur := reclaimed[0].ClaimID
	if err := s.NackLeasedEvent(context.Background(), ids[0], cur); err != nil {
		t.Fatal(err)
	}
	nacked, err := s.ClaimPendingEventsLeased(context.Background(), ttl, 5)
	if err != nil {
		t.Fatal(err)
	}
	// 第一条回到队列最前；其余两条租约也已到期（时钟只推进过一次），
	// 因此本次应按原顺序领到全部三条。
	if len(nacked) != 3 {
		t.Fatalf("after nack reclaim=%d want 3", len(nacked))
	}
	for i, c := range nacked {
		if c.Event.ID != ids[i] {
			t.Fatalf("order changed after nack: [%d]=%s want %s", i, c.Event.ID, ids[i])
		}
	}

	// 用最新 ClaimID Ack 第一条，事件离开 Outbox，历史保留。
	if err := s.AckLeasedEvent(context.Background(), ids[0], nacked[0].ClaimID); err != nil {
		t.Fatal(err)
	}
	if s.TotalEvents() != 2 {
		t.Fatalf("TotalEvents=%d want 2 after ack", s.TotalEvents())
	}
}

func TestClaim_LeasePersistsAcrossReopen(t *testing.T) {
	s := newTestStore(t)
	clk := withFakeClock(s)
	ids := appendEvents(t, s, "bk", "ik", 2)
	claims, err := s.ClaimPendingEventsLeased(context.Background(), time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	dir := s.dir
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 重开后租约仍然有效：立即可领取数为 0，旧 ClaimID 仍能确认。
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	s2.now = clk.now
	if s2.PendingCount() != 0 {
		t.Fatalf("lease lost after reopen: pending=%d", s2.PendingCount())
	}
	if err := s2.AckLeasedEvent(context.Background(), ids[0], claims[0].ClaimID); err != nil {
		t.Fatalf("ack with persisted claim id: %v", err)
	}
	if s2.TotalEvents() != 1 {
		t.Fatalf("total=%d want 1", s2.TotalEvents())
	}
}

// appendExecEvent 追加事件前先建立执行，便于历史查询。
func appendExecEvent(t *testing.T, s *FileStore, saga, bk, ik, etype string) {
	t.Helper()
	err := s.Commit(context.Background(), bk, ik, func(tx txsaga.Tx) error {
		if tx.State() == nil {
			tx.Create(&txsaga.ExecutionState{
				SagaName: saga, Fingerprint: "fp", Status: txsaga.StatusRunning,
				Steps: []txsaga.StepState{{Name: "s"}},
			})
		}
		tx.AppendEvent(etype, txsaga.EventPayload{SagaName: saga, IdempotencyKey: ik})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestHistory_PaginationAndFilters(t *testing.T) {
	s := newTestStore(t)
	// 两个执行交错追加，历史必须按执行身份过滤。
	for i := 0; i < 5; i++ {
		appendExecEvent(t, s, "saga", "bk", "ik", fmt.Sprintf("a-%d", i))
		appendExecEvent(t, s, "saga", "bk2", "ik", fmt.Sprintf("b-%d", i))
	}

	q := txsaga.EventHistoryQuery{SagaName: "saga", BusinessKey: "bk", IdempotencyKey: "ik", Limit: 2}
	p1, err := s.ListEvents(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if len(p1.Events) != 2 || !p1.HasMore || p1.NextAfterID != p1.Events[1].ID {
		t.Fatalf("page1=%+v", p1)
	}
	if p1.Events[0].Type != "a-0" || p1.Events[1].Type != "a-1" {
		t.Fatalf("page1 types=%s,%s", p1.Events[0].Type, p1.Events[1].Type)
	}

	q.AfterID = p1.NextAfterID
	p2, err := s.ListEvents(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if len(p2.Events) != 2 || p2.Events[0].Type != "a-2" {
		t.Fatalf("page2=%+v", p2)
	}

	// 最后一页不足 limit，HasMore=false；再查一次返回空页并回传游标。
	q.AfterID = p2.NextAfterID
	p3, err := s.ListEvents(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if len(p3.Events) != 1 || p3.Events[0].Type != "a-4" || p3.HasMore {
		t.Fatalf("page3=%+v", p3)
	}
	q.AfterID = p3.NextAfterID
	p4, err := s.ListEvents(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if len(p4.Events) != 0 || p4.NextAfterID != p3.NextAfterID || p4.HasMore {
		t.Fatalf("empty page=%+v", p4)
	}

	// 游标属于其它执行：ErrEventCursorNotFound，不返回部分页。
	badQ := txsaga.EventHistoryQuery{SagaName: "saga", BusinessKey: "bk", IdempotencyKey: "ik",
		AfterID: "", Limit: 2}
	// 取 bk2 的一个事件 ID 作为非法游标。
	bPage, err := s.ListEvents(context.Background(),
		txsaga.EventHistoryQuery{SagaName: "saga", BusinessKey: "bk2", IdempotencyKey: "ik", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	badQ.AfterID = bPage.Events[0].ID
	if _, err := s.ListEvents(context.Background(), badQ); !errors.Is(err, txsaga.ErrEventCursorNotFound) {
		t.Fatalf("foreign cursor err=%v want ErrEventCursorNotFound", err)
	}
	// 完全不存在的游标。
	badQ.AfterID = "evt-99999999999999999999"
	if _, err := s.ListEvents(context.Background(), badQ); !errors.Is(err, txsaga.ErrEventCursorNotFound) {
		t.Fatalf("missing cursor err=%v want ErrEventCursorNotFound", err)
	}

	// 执行不存在 / Saga 名称不符。
	if _, err := s.ListEvents(context.Background(),
		txsaga.EventHistoryQuery{SagaName: "saga", BusinessKey: "nope", IdempotencyKey: "ik"}); !errors.Is(err, txsaga.ErrExecutionNotFound) {
		t.Fatalf("missing exec err=%v want ErrExecutionNotFound", err)
	}
	if _, err := s.ListEvents(context.Background(),
		txsaga.EventHistoryQuery{SagaName: "other", BusinessKey: "bk", IdempotencyKey: "ik"}); !errors.Is(err, txsaga.ErrExecutionNotFound) {
		t.Fatalf("wrong saga err=%v want ErrExecutionNotFound", err)
	}
}

// TestHistory_DeliveryMetaReflectsLatest 验证历史记录的投递计数与最近领取
// 时间反映读取当下的最新值，Ack 后保留最后一次值。
func TestHistory_DeliveryMetaReflectsLatest(t *testing.T) {
	s := newTestStore(t)
	clk := withFakeClock(s)
	appendExecEvent(t, s, "saga", "bk", "ik", "t")
	q := txsaga.EventHistoryQuery{SagaName: "saga", BusinessKey: "bk", IdempotencyKey: "ik"}

	r0, err := s.ListEvents(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if r0.Events[0].Deliveries != 0 || !r0.Events[0].LastAttemptAt.IsZero() {
		t.Fatalf("fresh event meta=%+v", r0.Events[0])
	}
	id := r0.Events[0].ID

	evs, _ := s.ClaimPendingEvents(context.Background(), 10)
	if len(evs) != 1 {
		t.Fatalf("claim=%d", len(evs))
	}
	if !evs[0].LastAttemptAt().Equal(clk.now()) {
		t.Fatalf("claim time mismatch")
	}
	r1, _ := s.ListEvents(context.Background(), q)
	if r1.Events[0].Deliveries != 1 || !r1.Events[0].LastAttemptAt.Equal(clk.now()) {
		t.Fatalf("after claim meta=%+v", r1.Events[0])
	}
	if err := s.NackEvent(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	clk.advance(time.Second)
	evs2, _ := s.ClaimPendingEvents(context.Background(), 10)
	if evs2[0].Deliveries() != 2 {
		t.Fatalf("deliveries=%d want 2", evs2[0].Deliveries())
	}
	if err := s.AckEvent(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	r2, _ := s.ListEvents(context.Background(), q)
	if len(r2.Events) != 1 || r2.Events[0].Deliveries != 2 || !r2.Events[0].LastAttemptAt.Equal(clk.now()) {
		t.Fatalf("after ack audit meta=%+v now=%v", r2.Events[0], clk.now())
	}
}

// TestConcurrent_GoroutinesShareDirectory 验证同进程多 goroutine 共享同一
// FileStore：大量并发提交与领取后状态自洽，重开不损坏。
func TestConcurrent_GoroutinesShareDirectory(t *testing.T) {
	s := newTestStore(t)
	var producers sync.WaitGroup
	for g := 0; g < 8; g++ {
		producers.Add(1)
		go func(g int) {
			defer producers.Done()
			bk := fmt.Sprintf("bk-%d", g)
			for i := 0; i < 10; i++ {
				ik := fmt.Sprintf("ik-%d", i)
				err := s.Commit(context.Background(), bk, ik, func(tx txsaga.Tx) error {
					tx.Create(&txsaga.ExecutionState{
						SagaName: "saga", Fingerprint: "fp", Status: txsaga.StatusCompleted,
						Steps: []txsaga.StepState{{Name: "s", Status: txsaga.StepResultSucceeded}},
					})
					tx.AppendEvent("e", txsaga.EventPayload{SagaName: "saga", IdempotencyKey: ik})
					return nil
				})
				if err != nil {
					t.Errorf("commit: %v", err)
					return
				}
			}
		}(g)
	}
	// 并发领取/Ack。
	var claimer sync.WaitGroup
	stop := make(chan struct{})
	claimer.Add(1)
	go func() {
		defer claimer.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			evs, err := s.ClaimPendingEvents(context.Background(), 16)
			if err != nil {
				t.Errorf("claim: %v", err)
				return
			}
			for _, ev := range evs {
				if err := s.AckEvent(context.Background(), ev.ID); err != nil {
					t.Errorf("ack: %v", err)
					return
				}
			}
		}
	}()
	producers.Wait()
	// 等排空后停止领取者。
	for s.PendingCount() > 0 {
		time.Sleep(time.Millisecond)
	}
	close(stop)
	claimer.Wait()

	// 80 次执行，每次 1 条事件，全部应已 Ack：活动事件为 0，
	// 但重开后每个执行的历史仍有 1 条审计记录。
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(s.dir)
	if err != nil {
		t.Fatalf("reopen after concurrency: %v", err)
	}
	defer s2.Close()
	if s2.TotalEvents() != 0 || s2.PendingCount() != 0 {
		t.Fatalf("post-concurrency outbox: total=%d pending=%d", s2.TotalEvents(), s2.PendingCount())
	}
	count := 0
	for g := 0; g < 8; g++ {
		for i := 0; i < 10; i++ {
			page, err := s2.ListEvents(context.Background(), txsaga.EventHistoryQuery{
				SagaName: "saga", BusinessKey: fmt.Sprintf("bk-%d", g), IdempotencyKey: fmt.Sprintf("ik-%d", i),
			})
			if err != nil {
				t.Fatalf("history g=%d i=%d: %v", g, i, err)
			}
			if len(page.Events) != 1 {
				t.Fatalf("history g=%d i=%d len=%d want 1", g, i, len(page.Events))
			}
			count++
		}
	}
	if count != 80 {
		t.Fatalf("audited events=%d want 80", count)
	}
}

// TestConcurrent_RelayWithLease 用带租约的 Relay 跑并发提交的事件，
// 验证全部事件恰好至少投递一次、最终排空，且重开无损坏。
func TestConcurrent_RelayWithLease(t *testing.T) {
	s := newTestStore(t)
	dir := s.dir
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				if err := s.Commit(context.Background(), fmt.Sprintf("bk-%d", g), "ik",
					func(tx txsaga.Tx) error {
						tx.AppendEvent("e", txsaga.EventPayload{SagaName: "s", IdempotencyKey: "ik"})
						return nil
					}); err != nil {
					t.Errorf("commit: %v", err)
					return
				}
			}
		}(g)
	}

	var mu sync.Mutex
	delivered := map[string]int{}
	relay := txsaga.NewRelay(s, txsaga.PublisherFunc(func(_ context.Context, ev txsaga.Event) error {
		mu.Lock()
		delivered[ev.ID]++
		mu.Unlock()
		return nil
	}), txsaga.WithClaimLease(50*time.Millisecond), txsaga.WithBatch(8))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { _ = relay.Run(ctx); close(done) }()
	wg.Wait()
	for {
		mu.Lock()
		n := len(delivered)
		mu.Unlock()
		if n == 80 && s.PendingCount() == 0 && s.TotalEvents() == 0 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	<-done

	mu.Lock()
	if len(delivered) != 80 {
		t.Fatalf("distinct delivered=%d want 80", len(delivered))
	}
	for id, n := range delivered {
		if n < 1 {
			t.Fatalf("event %s never delivered", id)
		}
	}
	mu.Unlock()

	s.Close()
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
}
