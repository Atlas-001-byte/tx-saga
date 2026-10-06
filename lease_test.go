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

// leaseClock 是可手动推进的时钟，用于确定性地控制租约到期。
type leaseClock struct {
	mu  sync.Mutex
	now time.Time
}

func newLeaseClock() *leaseClock {
	return &leaseClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *leaseClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *leaseClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// newLeaseStore 创建使用可控时钟的 MemoryStore。
func newLeaseStore(clock *leaseClock) *MemoryStore {
	store := NewMemoryStore()
	store.now = clock.Now
	return store
}

// appendEvents 直接通过 Commit 追加 n 条 Outbox 事件（不经过引擎），
// 事件 ID 按追加顺序分配。
func appendEvents(t *testing.T, store *MemoryStore, n int) {
	t.Helper()
	err := store.Commit(context.Background(), "biz", "idem", func(tx Tx) error {
		for i := 0; i < n; i++ {
			tx.AppendEvent(EventExecutionStarted, EventPayload{SagaName: "s", IdempotencyKey: fmt.Sprintf("k-%d", i)})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("append events: %v", err)
	}
}

func TestLeasedClaimBlocksReclaimUntilExpiry(t *testing.T) {
	clock := newLeaseClock()
	store := newLeaseStore(clock)
	appendEvents(t, store, 1)
	ctx := context.Background()

	first, err := store.ClaimPendingEventsLeased(ctx, 10, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 {
		t.Fatalf("claimed=%d want 1", len(first))
	}
	c1 := first[0]
	if c1.ClaimID == "" {
		t.Fatal("ClaimID must be assigned")
	}
	if want := clock.Now().Add(time.Hour); !c1.ClaimedUntil.Equal(want) {
		t.Fatalf("ClaimedUntil=%v want %v", c1.ClaimedUntil, want)
	}
	if c1.Event.Deliveries() != 1 {
		t.Fatalf("deliveries=%d want 1", c1.Event.Deliveries())
	}
	if c1.Event.LastAttemptAt().IsZero() {
		t.Fatal("LastAttemptAt must be set on claim")
	}

	// 租约有效期内：不得再次领取。
	again, _ := store.ClaimPendingEventsLeased(ctx, 10, time.Hour)
	if len(again) != 0 {
		t.Fatalf("event must be hidden within lease, got %d", len(again))
	}
	clock.Advance(30 * time.Minute)
	again, _ = store.ClaimPendingEventsLeased(ctx, 10, time.Hour)
	if len(again) != 0 {
		t.Fatalf("event must stay hidden before expiry, got %d", len(again))
	}

	// 租约到期：自动恢复，重新领取生成新 ClaimID，投递数加一。
	clock.Advance(31 * time.Minute)
	second, _ := store.ClaimPendingEventsLeased(ctx, 10, time.Hour)
	if len(second) != 1 {
		t.Fatalf("expired lease must be reclaimable, got %d", len(second))
	}
	c2 := second[0]
	if c2.Event.ID != c1.Event.ID {
		t.Fatalf("recovered event ID=%s want %s", c2.Event.ID, c1.Event.ID)
	}
	if c2.ClaimID == c1.ClaimID {
		t.Fatal("each claim must generate a new ClaimID")
	}
	if c2.Event.Deliveries() != 2 {
		t.Fatalf("deliveries=%d want 2", c2.Event.Deliveries())
	}
	if !c2.Event.LastAttemptAt().After(c1.Event.LastAttemptAt()) {
		t.Fatal("LastAttemptAt must advance on reclaim")
	}
	// 原事件字段不变。
	if c2.Event.Type != c1.Event.Type || c2.Event.BusinessKey != c1.Event.BusinessKey {
		t.Fatal("event fields must be preserved across reclaim")
	}
}

func TestLeasedAckRemovesEventPermanently(t *testing.T) {
	clock := newLeaseClock()
	store := newLeaseStore(clock)
	appendEvents(t, store, 2)
	ctx := context.Background()

	claims, err := store.ClaimPendingEventsLeased(ctx, 10, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AckLeasedEvent(ctx, claims[0].Event.ID, claims[0].ClaimID); err != nil {
		t.Fatal(err)
	}
	if got := store.TotalEvents(); got != 1 {
		t.Fatalf("TotalEvents=%d want 1 after ack", got)
	}
	// 即使租约到期，已 Ack 的事件也不会恢复。
	clock.Advance(2 * time.Hour)
	rest, _ := store.ClaimPendingEventsLeased(ctx, 10, time.Hour)
	if len(rest) != 1 || rest[0].Event.ID != claims[1].Event.ID {
		t.Fatalf("acked event must never reappear, got %+v", rest)
	}
	if err := store.AckLeasedEvent(ctx, rest[0].Event.ID, rest[0].ClaimID); err != nil {
		t.Fatal(err)
	}
	if got := store.TotalEvents(); got != 0 {
		t.Fatalf("TotalEvents=%d want 0", got)
	}
	if got := store.PendingCount(); got != 0 {
		t.Fatalf("PendingCount=%d want 0", got)
	}
}

func TestLeasedNackReleasesImmediately(t *testing.T) {
	clock := newLeaseClock()
	store := newLeaseStore(clock)
	appendEvents(t, store, 1)
	ctx := context.Background()

	claims, _ := store.ClaimPendingEventsLeased(ctx, 10, time.Hour)
	c1 := claims[0]
	if err := store.NackLeasedEvent(ctx, c1.Event.ID, c1.ClaimID); err != nil {
		t.Fatal(err)
	}
	// 不推进时钟：Nack 后立即可再次领取。
	again, _ := store.ClaimPendingEventsLeased(ctx, 10, time.Hour)
	if len(again) != 1 {
		t.Fatalf("nacked event must be immediately claimable, got %d", len(again))
	}
	c2 := again[0]
	if c2.ClaimID == c1.ClaimID {
		t.Fatal("reclaim after nack must use a new ClaimID")
	}
	if c2.Event.Deliveries() != 2 {
		t.Fatalf("deliveries=%d want 2", c2.Event.Deliveries())
	}
}

func TestStaleClaimRejectedAndLeaseUntouched(t *testing.T) {
	clock := newLeaseClock()
	store := newLeaseStore(clock)
	appendEvents(t, store, 1)
	ctx := context.Background()

	claims, _ := store.ClaimPendingEventsLeased(ctx, 10, time.Hour)
	c1 := claims[0]

	// 租约到期后被重新领取，旧 ClaimID 失效。
	clock.Advance(2 * time.Hour)
	claims, _ = store.ClaimPendingEventsLeased(ctx, 10, time.Hour)
	if len(claims) != 1 {
		t.Fatalf("expected recovery claim, got %d", len(claims))
	}
	c2 := claims[0]

	// 旧租约的 Ack/Nack 均被拒绝，且不影响当前事件与新租约。
	if err := store.AckLeasedEvent(ctx, c1.Event.ID, c1.ClaimID); !errors.Is(err, ErrStaleClaim) {
		t.Fatalf("stale ack = %v, want ErrStaleClaim", err)
	}
	if err := store.NackLeasedEvent(ctx, c1.Event.ID, c1.ClaimID); !errors.Is(err, ErrStaleClaim) {
		t.Fatalf("stale nack = %v, want ErrStaleClaim", err)
	}
	if got := store.TotalEvents(); got != 1 {
		t.Fatalf("stale operations must not remove the event, TotalEvents=%d", got)
	}
	if got := store.PendingCount(); got != 0 {
		t.Fatalf("stale nack must not release the new lease, PendingCount=%d", got)
	}
	// 新租约仍在有效期内：不可被再次领取。
	if again, _ := store.ClaimPendingEventsLeased(ctx, 10, time.Hour); len(again) != 0 {
		t.Fatalf("new lease must survive stale operations, got %d", len(again))
	}
	// 当前 ClaimID 可以正常确认。
	if err := store.AckLeasedEvent(ctx, c2.Event.ID, c2.ClaimID); err != nil {
		t.Fatalf("current claim ack: %v", err)
	}
	if got := store.TotalEvents(); got != 0 {
		t.Fatalf("TotalEvents=%d want 0", got)
	}
}

func TestStaleClaimAfterNackAndReclaim(t *testing.T) {
	clock := newLeaseClock()
	store := newLeaseStore(clock)
	appendEvents(t, store, 1)
	ctx := context.Background()

	claims, _ := store.ClaimPendingEventsLeased(ctx, 10, time.Hour)
	c1 := claims[0]
	if err := store.NackLeasedEvent(ctx, c1.Event.ID, c1.ClaimID); err != nil {
		t.Fatal(err)
	}
	claims, _ = store.ClaimPendingEventsLeased(ctx, 10, time.Hour)
	c2 := claims[0]

	// 已退回的旧 ClaimID 对新租约同样失效。
	if err := store.AckLeasedEvent(ctx, c1.Event.ID, c1.ClaimID); !errors.Is(err, ErrStaleClaim) {
		t.Fatalf("stale ack after nack = %v, want ErrStaleClaim", err)
	}
	if err := store.AckLeasedEvent(ctx, c2.Event.ID, c2.ClaimID); err != nil {
		t.Fatal(err)
	}
}

func TestPlainClaimAlsoRecoversExpiredLease(t *testing.T) {
	clock := newLeaseClock()
	store := newLeaseStore(clock)
	appendEvents(t, store, 1)
	ctx := context.Background()

	if _, err := store.ClaimPendingEventsLeased(ctx, 10, time.Hour); err != nil {
		t.Fatal(err)
	}
	// 未到期：普通领取也看不到租约中的事件。
	if evs, _ := store.ClaimPendingEvents(ctx, 10); len(evs) != 0 {
		t.Fatalf("leased event must be hidden from plain claim, got %d", len(evs))
	}
	// 到期后：普通领取路径同样触发恢复。
	clock.Advance(2 * time.Hour)
	evs, _ := store.ClaimPendingEvents(ctx, 10)
	if len(evs) != 1 {
		t.Fatalf("expired lease must be reclaimable via plain claim, got %d", len(evs))
	}
}

// plainStore 只暴露 Store 接口，用于验证未实现 ClaimLeaseStore 的自定义
// Store 保持原编译与行为。
type plainStore struct{ Store }

func TestRelayClaimLeaseUnsupportedStore(t *testing.T) {
	store := NewMemoryStore()
	appendEvents(t, store, 1)
	pub := PublisherFunc(func(context.Context, Event) error { return nil })

	relay := NewRelay(plainStore{store}, pub, WithClaimLease(time.Minute))
	if _, err := relay.DeliverOnce(context.Background()); !errors.Is(err, ErrClaimLeaseUnsupported) {
		t.Fatalf("DeliverOnce = %v, want ErrClaimLeaseUnsupported", err)
	}
	if err := relay.Run(context.Background()); !errors.Is(err, ErrClaimLeaseUnsupported) {
		t.Fatalf("Run = %v, want ErrClaimLeaseUnsupported", err)
	}
	// 事件未被领取或改动。
	if got := store.PendingCount(); got != 1 {
		t.Fatalf("PendingCount=%d want 1", got)
	}
}

func TestWithClaimLeaseNonPositiveKeepsDefault(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		store := NewMemoryStore()
		appendEvents(t, store, 1)
		var published int
		pub := PublisherFunc(func(context.Context, Event) error { published++; return nil })

		// 非正时长不启用租约：自定义 Store 无需实现 ClaimLeaseStore。
		relay := NewRelay(plainStore{store}, pub, WithClaimLease(d))
		res, err := relay.DeliverOnce(context.Background())
		if err != nil {
			t.Fatalf("lease=%v: %v", d, err)
		}
		if res.Claimed != 1 || res.Delivered != 1 || published != 1 {
			t.Fatalf("lease=%v: res=%+v published=%d", d, res, published)
		}
		if got := store.TotalEvents(); got != 0 {
			t.Fatalf("lease=%v: TotalEvents=%d want 0", d, got)
		}
	}
}

func TestRelayLeasedDeliverSuccess(t *testing.T) {
	clock := newLeaseClock()
	store := newLeaseStore(clock)
	appendEvents(t, store, 3)
	var published []string
	pub := PublisherFunc(func(_ context.Context, ev Event) error {
		published = append(published, ev.ID)
		return nil
	})
	relay := NewRelay(store, pub, WithClaimLease(time.Hour))

	res, err := relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Claimed != 3 || res.Delivered != 3 || res.Failed != 0 {
		t.Fatalf("res=%+v want 3/3/0", res)
	}
	if len(published) != 3 {
		t.Fatalf("published=%d want 3", len(published))
	}
	if got := store.TotalEvents(); got != 0 {
		t.Fatalf("TotalEvents=%d want 0", got)
	}
}

func TestRelayLeasedPublishFailureNacksImmediately(t *testing.T) {
	clock := newLeaseClock()
	store := newLeaseStore(clock)
	appendEvents(t, store, 1)
	fail := true
	pub := PublisherFunc(func(context.Context, Event) error {
		if fail {
			return errors.New("temporary publish failure")
		}
		return nil
	})
	relay := NewRelay(store, pub, WithClaimLease(time.Hour))

	res, err := relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Claimed != 1 || res.Delivered != 0 || res.Failed != 1 {
		t.Fatalf("res=%+v want 1/0/1", res)
	}
	// 失败后已按当前 ClaimID 退回：不必等租约到期即可重领。
	if got := store.PendingCount(); got != 1 {
		t.Fatalf("PendingCount=%d want 1", got)
	}
	fail = false
	res, err = relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Delivered != 1 {
		t.Fatalf("retry res=%+v", res)
	}
	if got := store.TotalEvents(); got != 0 {
		t.Fatalf("TotalEvents=%d want 0", got)
	}
}

func TestRelayLeasedAtMostOnePublishWithinLease(t *testing.T) {
	clock := newLeaseClock()
	store := newLeaseStore(clock)
	appendEvents(t, store, 1)
	var published int
	pub := PublisherFunc(func(context.Context, Event) error { published++; return nil })
	relay := NewRelay(store, pub, WithClaimLease(time.Hour))

	// 第一轮领取后模拟 worker 退出：不 Ack/Nack，直接开始新一轮。
	if _, err := store.ClaimPendingEventsLeased(context.Background(), 10, time.Hour); err != nil {
		t.Fatal(err)
	}
	res, err := relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Claimed != 0 || published != 0 {
		t.Fatalf("within lease nothing may be republished: res=%+v published=%d", res, published)
	}
	// 到期后恢复重投，仍属至少一次语义。
	clock.Advance(2 * time.Hour)
	res, err = relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Claimed != 1 || res.Delivered != 1 || published != 1 {
		t.Fatalf("after expiry res=%+v published=%d", res, published)
	}
}

func TestRelayLeasedRecoveryAfterWorkerExit(t *testing.T) {
	clock := newLeaseClock()
	store := newLeaseStore(clock)
	appendEvents(t, store, 2)
	var published []string
	pub := PublisherFunc(func(_ context.Context, ev Event) error {
		published = append(published, ev.ID)
		return nil
	})
	relay := NewRelay(store, pub, WithClaimLease(time.Hour))

	// worker 领取后在 Publish/Ack/Nack 前退出，事件锁在领取中。
	claims, err := store.ClaimPendingEventsLeased(context.Background(), 10, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(claims) != 2 {
		t.Fatalf("claimed=%d want 2", len(claims))
	}
	res, err := relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Claimed != 0 {
		t.Fatalf("claimed events must stay hidden until expiry, got %+v", res)
	}

	// 租约到期：下一次 DeliverOnce 自动恢复并完成投递。
	clock.Advance(time.Hour + time.Second)
	res, err = relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Claimed != 2 || res.Delivered != 2 {
		t.Fatalf("recovery res=%+v want 2/2", res)
	}
	if len(published) != 2 {
		t.Fatalf("published=%d want 2", len(published))
	}
	if got := store.TotalEvents(); got != 0 {
		t.Fatalf("TotalEvents=%d want 0", got)
	}
}

func TestRelayLeasedCancelBeforePublish(t *testing.T) {
	clock := newLeaseClock()
	store := newLeaseStore(clock)
	appendEvents(t, store, 2)
	var published int
	pub := PublisherFunc(func(context.Context, Event) error { published++; return nil })
	relay := NewRelay(store, pub, WithClaimLease(time.Hour))

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 领取后、Publish 前即已取消
	res, err := relay.DeliverOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Claimed != 2 || res.Delivered != 0 || res.Failed != 2 {
		t.Fatalf("res=%+v want 2/0/2", res)
	}
	if published != 0 {
		t.Fatalf("nothing may be published after cancel, got %d", published)
	}
	// 取消路径已按当前 ClaimID 立即 Nack：无需等待租约到期即可重领。
	if got := store.PendingCount(); got != 2 {
		t.Fatalf("PendingCount=%d want 2", got)
	}
	res, err = relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Delivered != 2 || published != 2 {
		t.Fatalf("redelivery res=%+v published=%d", res, published)
	}
}

func TestRelayLeasedCancelDuringPublish(t *testing.T) {
	clock := newLeaseClock()
	store := newLeaseStore(clock)
	appendEvents(t, store, 2)

	// Publish 中取消并按失败返回：事件被退回，可立即重领。
	ctx, cancel := context.WithCancel(context.Background())
	pub := PublisherFunc(func(context.Context, Event) error {
		cancel()
		return errors.New("publish aborted")
	})
	relay := NewRelay(store, pub, WithClaimLease(time.Hour))
	res, err := relay.DeliverOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Claimed != 2 || res.Failed != 2 {
		t.Fatalf("res=%+v want 2 claimed / 2 failed", res)
	}
	if got := store.PendingCount(); got != 2 {
		t.Fatalf("PendingCount=%d want 2", got)
	}

	// Publish 中取消但按成功返回：按返回结果确认，事件被移除；
	// 其余已领取事件在 Publish 前检测到取消，按当前 ClaimID 立即退回。
	ctx2, cancel2 := context.WithCancel(context.Background())
	pubOK := PublisherFunc(func(context.Context, Event) error {
		cancel2()
		return nil
	})
	relay2 := NewRelay(store, pubOK, WithClaimLease(time.Hour))
	res, err = relay2.DeliverOnce(ctx2)
	if err != nil {
		t.Fatal(err)
	}
	if res.Claimed != 2 || res.Delivered != 1 || res.Failed != 1 {
		t.Fatalf("res=%+v want 2 claimed / 1 delivered / 1 failed", res)
	}
	if got := store.TotalEvents(); got != 1 {
		t.Fatalf("TotalEvents=%d want 1", got)
	}
	// 被退回的事件可立即重领并完成投递。
	res, err = relay2.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Delivered != 1 {
		t.Fatalf("redelivery res=%+v", res)
	}
	if got := store.TotalEvents(); got != 0 {
		t.Fatalf("TotalEvents=%d want 0", got)
	}
}

func TestLeasedConcurrentClaimsAreExclusive(t *testing.T) {
	clock := newLeaseClock()
	store := newLeaseStore(clock)
	const total = 64
	appendEvents(t, store, total)
	ctx := context.Background()

	var mu sync.Mutex
	seen := map[string]string{} // eventID -> claimID
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				claims, err := store.ClaimPendingEventsLeased(ctx, 4, time.Hour)
				if err != nil {
					t.Error(err)
					return
				}
				if len(claims) == 0 {
					return
				}
				mu.Lock()
				for _, cl := range claims {
					if owner, dup := seen[cl.Event.ID]; dup {
						t.Errorf("event %s claimed twice: %s and %s", cl.Event.ID, owner, cl.ClaimID)
					}
					seen[cl.Event.ID] = cl.ClaimID
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(seen) != total {
		t.Fatalf("claimed=%d want %d", len(seen), total)
	}
	if got := store.PendingCount(); got != 0 {
		t.Fatalf("PendingCount=%d want 0", got)
	}
	// 有效租约内无任何事件可被再次领取。
	if claims, _ := store.ClaimPendingEventsLeased(ctx, 100, time.Hour); len(claims) != 0 {
		t.Fatalf("all events leased, got %d extra claims", len(claims))
	}
}

func TestRelayLeasedRunRecoversAfterExpiry(t *testing.T) {
	// 真实时钟：Run 以后台轮询驱动，租约到期后自动恢复投递。
	store := NewMemoryStore()
	appendEvents(t, store, 1)
	var published atomic.Int32
	pub := PublisherFunc(func(context.Context, Event) error { published.Add(1); return nil })
	relay := NewRelay(store, pub,
		WithClaimLease(50*time.Millisecond),
		WithIdleWait(5*time.Millisecond),
		WithRetryBackoff(5*time.Millisecond),
	)

	// worker 领取后失联，事件等待租约恢复。
	if _, err := store.ClaimPendingEventsLeased(context.Background(), 10, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- relay.Run(ctx) }()

	// 租约到期后 Run 的下一轮自动恢复并完成投递。
	deadline := time.After(3 * time.Second)
	for published.Load() == 0 {
		select {
		case <-deadline:
			cancel()
			t.Fatal("event was not recovered and published in time")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run exit = %v, want context.Canceled", err)
	}
	if got := store.TotalEvents(); got != 0 {
		t.Fatalf("TotalEvents=%d want 0", got)
	}
}
