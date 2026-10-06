package txsaga

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// fakeClock 是可手动推进的时钟，供 MemoryStore 租约测试使用。
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// leasedTestStore 创建使用假时钟的 MemoryStore，并提交 n 条待投递事件。
func leasedTestStore(t *testing.T, n int) (*MemoryStore, *fakeClock, []string) {
	t.Helper()
	clk := newFakeClock()
	store := NewMemoryStore()
	store.now = clk.now
	var ids []string
	err := store.Commit(context.Background(), "bk", "ik", func(tx Tx) error {
		for i := 0; i < n; i++ {
			tx.AppendEvent(fmt.Sprintf("evt-type-%d", i), EventPayload{
				SagaName: "lease-saga", IdempotencyKey: "ik", Step: fmt.Sprintf("s%d", i),
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// 事件 ID 由存储按追加顺序分配。
	for i := 1; i <= n; i++ {
		ids = append(ids, fmt.Sprintf("evt-%020d", i))
	}
	return store, clk, ids
}

func claimIDs(claims []ClaimedEvent) []string {
	out := make([]string, len(claims))
	for i, c := range claims {
		out[i] = c.ClaimID
	}
	return out
}

func eventIDs(claims []ClaimedEvent) []string {
	out := make([]string, len(claims))
	for i, c := range claims {
		out[i] = c.Event.ID
	}
	return out
}

func TestClaimLeaseHidesEventsUntilExpiry(t *testing.T) {
	store, clk, ids := leasedTestStore(t, 3)
	ttl := time.Minute

	claims, err := store.ClaimPendingEventsLeased(context.Background(), ttl, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claims) != 3 {
		t.Fatalf("claimed %d want 3", len(claims))
	}
	seenClaim := map[string]string{}
	for i, c := range claims {
		if c.Event.ID != ids[i] {
			t.Fatalf("claim %d event %s want %s（追加顺序不变）", i, c.Event.ID, ids[i])
		}
		if c.ClaimID == "" {
			t.Fatalf("claim %d missing ClaimID", i)
		}
		wantUntil := clk.now().Add(ttl)
		if !c.ClaimedUntil.Equal(wantUntil) {
			t.Fatalf("claim %d until %v want %v", i, c.ClaimedUntil, wantUntil)
		}
		if c.Event.Deliveries() != 1 {
			t.Fatalf("claim %d deliveries=%d want 1", i, c.Event.Deliveries())
		}
		if !c.Event.LastAttemptAt().Equal(clk.now()) {
			t.Fatalf("claim %d LastAttemptAt=%v want %v", i, c.Event.LastAttemptAt(), clk.now())
		}
		seenClaim[c.Event.ID] = c.ClaimID
	}

	// 租约有效期内：租约领取与普通领取都拿不到事件。
	mid, err := store.ClaimPendingEventsLeased(context.Background(), ttl, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(mid) != 0 {
		t.Fatalf("lease must hide events, got %d", len(mid))
	}
	old, err := store.ClaimPendingEvents(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(old) != 0 {
		t.Fatalf("plain claim must also respect active lease, got %d", len(old))
	}
	if store.PendingCount() != 0 {
		t.Fatalf("PendingCount=%d want 0 during lease", store.PendingCount())
	}
	if store.TotalEvents() != 3 {
		t.Fatalf("TotalEvents=%d want 3", store.TotalEvents())
	}

	// 到期前一刻仍不可领取；到期后下一次领取自动恢复。
	clk.advance(ttl - time.Nanosecond)
	mid, _ = store.ClaimPendingEventsLeased(context.Background(), ttl, 10)
	if len(mid) != 0 {
		t.Fatalf("event reclaimable just before expiry: %d", len(mid))
	}
	clk.advance(time.Nanosecond)
	recovered, err := store.ClaimPendingEventsLeased(context.Background(), ttl, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 3 {
		t.Fatalf("recovered %d want 3", len(recovered))
	}
	for i, c := range recovered {
		if c.Event.ID != ids[i] {
			t.Fatalf("recovery reordered: %v want %v", eventIDs(recovered), ids)
		}
		if c.ClaimID == seenClaim[c.Event.ID] {
			t.Fatalf("event %s reused old ClaimID", c.Event.ID)
		}
		if c.Event.Deliveries() != 2 {
			t.Fatalf("event %s deliveries=%d want 2", c.Event.ID, c.Event.Deliveries())
		}
		if !c.Event.LastAttemptAt().Equal(clk.now()) {
			t.Fatalf("event %s LastAttemptAt not refreshed", c.Event.ID)
		}
	}
}

func TestClaimLeaseAckPermanentlyRemoves(t *testing.T) {
	store, clk, ids := leasedTestStore(t, 2)
	ttl := time.Minute

	claims, _ := store.ClaimPendingEventsLeased(context.Background(), ttl, 10)
	if err := store.AckLeasedEvent(context.Background(), claims[0].Event.ID, claims[0].ClaimID); err != nil {
		t.Fatal(err)
	}
	if store.TotalEvents() != 1 {
		t.Fatalf("TotalEvents=%d want 1 after ack", store.TotalEvents())
	}

	// 到期扫描：已 Ack 的墓碑 ID 被清除，剩余事件照常恢复。
	clk.advance(ttl + time.Second)
	recovered, err := store.ClaimPendingEventsLeased(context.Background(), ttl, 10)
	if err != nil {
		t.Fatal(err)
	}
	got := eventIDs(recovered)
	if len(got) != 1 || got[0] != ids[1] {
		t.Fatalf("after ack + expiry got %v want [%s]", got, ids[1])
	}
}

func TestClaimLeaseNackReclaimsImmediately(t *testing.T) {
	store, _, ids := leasedTestStore(t, 3)
	ttl := time.Minute

	claims, _ := store.ClaimPendingEventsLeased(context.Background(), ttl, 10)
	// 退回中间一条，立即在原位可领取；其余两条仍受租约保护。
	if err := store.NackLeasedEvent(context.Background(), claims[1].Event.ID, claims[1].ClaimID); err != nil {
		t.Fatal(err)
	}
	next, err := store.ClaimPendingEventsLeased(context.Background(), ttl, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(next) != 1 || next[0].Event.ID != ids[1] {
		t.Fatalf("nacked event must be immediately reclaimable in place, got %v", eventIDs(next))
	}
	if next[0].ClaimID == claims[1].ClaimID {
		t.Fatal("nack reclaim must assign a new ClaimID")
	}
	if next[0].Event.Deliveries() != 2 {
		t.Fatalf("deliveries=%d want 2", next[0].Event.Deliveries())
	}
	if next[0].Event.Type != "evt-type-1" {
		t.Fatalf("event fields altered: type=%s", next[0].Event.Type)
	}
	if store.PendingCount() != 0 {
		t.Fatalf("PendingCount=%d want 0, others still leased", store.PendingCount())
	}

	// 旧 ClaimID 在 Nack 后再操作即失效。
	err = store.NackLeasedEvent(context.Background(), claims[1].Event.ID, claims[1].ClaimID)
	if !errors.Is(err, ErrStaleClaim) {
		t.Fatalf("old claim nack err=%v want ErrStaleClaim", err)
	}
}

func TestClaimLeaseStaleOperationsDoNotTouchCurrentClaim(t *testing.T) {
	store, clk, _ := leasedTestStore(t, 1)
	ttl := time.Minute

	first, _ := store.ClaimPendingEventsLeased(context.Background(), ttl, 10)
	oldClaimID := first[0].ClaimID

	// 到期后被另一领取者接管。
	clk.advance(ttl + time.Second)
	current, _ := store.ClaimPendingEventsLeased(context.Background(), ttl, 10)
	newClaimID := current[0].ClaimID
	if newClaimID == oldClaimID {
		t.Fatal("expected new ClaimID after expiry")
	}

	// 旧 ClaimID 的 Ack/Nack 均返回 ErrStaleClaim，且不改动当前事件与新租约。
	if err := store.AckLeasedEvent(context.Background(), first[0].Event.ID, oldClaimID); !errors.Is(err, ErrStaleClaim) {
		t.Fatalf("stale ack err=%v want ErrStaleClaim", err)
	}
	if err := store.NackLeasedEvent(context.Background(), first[0].Event.ID, oldClaimID); !errors.Is(err, ErrStaleClaim) {
		t.Fatalf("stale nack err=%v want ErrStaleClaim", err)
	}
	again, _ := store.ClaimPendingEventsLeased(context.Background(), ttl, 10)
	if len(again) != 0 {
		t.Fatal("current lease must remain valid after stale operations")
	}
	if store.TotalEvents() != 1 {
		t.Fatalf("stale ack must not remove event: TotalEvents=%d", store.TotalEvents())
	}

	// 未知事件 ID 是普通 not-found，而不是 ErrStaleClaim。
	if err := store.AckLeasedEvent(context.Background(), "evt-missing", newClaimID); errors.Is(err, ErrStaleClaim) {
		t.Fatalf("missing event must not map to ErrStaleClaim: %v", err)
	}

	// 当前 ClaimID 仍可正常 Ack。
	if err := store.AckLeasedEvent(context.Background(), current[0].Event.ID, newClaimID); err != nil {
		t.Fatalf("current claim ack: %v", err)
	}
	if store.TotalEvents() != 0 {
		t.Fatalf("TotalEvents=%d want 0", store.TotalEvents())
	}
}

func TestClaimLeasePreservesAppendOrderAcrossNackAndExpiry(t *testing.T) {
	store, clk, ids := leasedTestStore(t, 4)
	ttl := time.Minute

	first, _ := store.ClaimPendingEventsLeased(context.Background(), ttl, 10)
	// 退回第 2、4 条；第 1、3 条模拟 worker 失联，等租约到期。
	_ = store.NackLeasedEvent(context.Background(), first[1].Event.ID, first[1].ClaimID)
	_ = store.NackLeasedEvent(context.Background(), first[3].Event.ID, first[3].ClaimID)

	// 未到期：仅被 Nack 的两条可领，顺序仍是原追加顺序。
	next, _ := store.ClaimPendingEventsLeased(context.Background(), ttl, 10)
	if got := eventIDs(next); fmt.Sprint(got) != fmt.Sprint([]string{ids[1], ids[3]}) {
		t.Fatalf("immediate reclaim order=%v", got)
	}

	clk.advance(ttl + time.Second)
	all, _ := store.ClaimPendingEventsLeased(context.Background(), ttl, 10)
	if got := eventIDs(all); fmt.Sprint(got) != fmt.Sprint(ids) {
		t.Fatalf("recovery order=%v want %v", got, ids)
	}
}

func TestClaimLeaseConcurrentClaimantsAreExclusive(t *testing.T) {
	store, _, ids := leasedTestStore(t, 20)
	ttl := time.Hour

	const n = 20
	var wg sync.WaitGroup
	var mu sync.Mutex
	byEvent := map[string]string{}
	duplicates := 0
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			claims, err := store.ClaimPendingEventsLeased(context.Background(), ttl, 1)
			if err != nil {
				t.Error(err)
				return
			}
			if len(claims) != 1 {
				t.Errorf("claimant got %d events", len(claims))
				return
			}
			mu.Lock()
			defer mu.Unlock()
			id := claims[0].Event.ID
			if _, dup := byEvent[id]; dup {
				duplicates++
			}
			byEvent[id] = claims[0].ClaimID
		}()
	}
	wg.Wait()
	if duplicates != 0 || len(byEvent) != n {
		t.Fatalf("exclusive claim violated: duplicates=%d distinct=%d", duplicates, len(byEvent))
	}
	claimIDs := map[string]struct{}{}
	for _, cid := range byEvent {
		claimIDs[cid] = struct{}{}
	}
	if len(claimIDs) != n {
		t.Fatalf("ClaimIDs not unique: %d distinct for %d claims", len(claimIDs), n)
	}
	for _, id := range ids {
		if _, ok := byEvent[id]; !ok {
			t.Fatalf("event %s never claimed", id)
		}
	}
}

// ---- Relay 租约路径 ----

func TestRelayLeaseDeliversAndAcks(t *testing.T) {
	store, _, _ := leasedTestStore(t, 3)
	var mu sync.Mutex
	var published []string
	pub := PublisherFunc(func(_ context.Context, ev Event) error {
		mu.Lock()
		defer mu.Unlock()
		published = append(published, ev.ID)
		return nil
	})
	relay := NewRelay(store, pub, WithClaimLease(time.Minute))

	res, err := relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Claimed != 3 || res.Delivered != 3 || res.Failed != 0 {
		t.Fatalf("res=%+v", res)
	}
	if store.TotalEvents() != 0 {
		t.Fatalf("acked events must be removed: %d", store.TotalEvents())
	}
	if len(published) != 3 {
		t.Fatalf("published=%d want 3", len(published))
	}
}

func TestRelayLeasePublishFailureNacksForImmediateRetry(t *testing.T) {
	store, _, _ := leasedTestStore(t, 2)
	var mu sync.Mutex
	failFirst := map[string]bool{}
	pub := PublisherFunc(func(_ context.Context, ev Event) error {
		mu.Lock()
		defer mu.Unlock()
		if !failFirst[ev.ID] {
			failFirst[ev.ID] = true
			return errors.New("boom")
		}
		return nil
	})
	relay := NewRelay(store, pub, WithClaimLease(time.Minute))

	res, err := relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Claimed != 2 || res.Failed != 2 || res.Delivered != 0 {
		t.Fatalf("res=%+v", res)
	}
	// Nack 后立即重领成功，无需等待租约到期。
	res, err = relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Delivered != 2 || store.TotalEvents() != 0 {
		t.Fatalf("retry res=%+v remaining=%d", res, store.TotalEvents())
	}
}

func TestRelayLeaseRecoversAbandonedEventAfterExpiry(t *testing.T) {
	store, clk, _ := leasedTestStore(t, 1)
	var mu sync.Mutex
	publishes := 0
	pub := PublisherFunc(func(_ context.Context, ev Event) error {
		mu.Lock()
		defer mu.Unlock()
		publishes++
		return nil
	})
	relay := NewRelay(store, pub, WithClaimLease(time.Minute))

	// 首轮模拟 Publish 成功但 Ack 前 worker 退出：用包装 Store 让首次 Ack 失败。
	crash := &crashAckStore{MemoryStore: store}
	relayCrash := NewRelay(crash, pub, WithClaimLease(time.Minute))
	res, err := relayCrash.DeliverOnce(context.Background())
	if !errors.Is(err, errCrashAck) {
		t.Fatalf("err=%v want errCrashAck", err)
	}
	if res.Delivered != 0 {
		t.Fatalf("crashed ack must not count delivered: %+v", res)
	}

	// 租约内：事件不会被再次领取，Publish 在有效期内最多一次。
	res, err = relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Claimed != 0 {
		t.Fatalf("event must stay hidden within lease: %+v", res)
	}
	mu.Lock()
	if publishes != 1 {
		t.Fatalf("publish called %d times within lease, want 1", publishes)
	}
	mu.Unlock()

	// 到期后的下一轮自动恢复，重投一次并 Ack，Deliveries 累计为 2。
	clk.advance(time.Minute + time.Second)
	res, err = relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Claimed != 1 || res.Delivered != 1 {
		t.Fatalf("recovery res=%+v", res)
	}
	mu.Lock()
	if publishes != 2 {
		t.Fatalf("publish called %d times, want 2 after recovery", publishes)
	}
	mu.Unlock()
	if store.TotalEvents() != 0 {
		t.Fatalf("event not acked after recovery: %d", store.TotalEvents())
	}
}

func TestRelayLeaseCancelBeforePublishNacksImmediately(t *testing.T) {
	store, _, _ := leasedTestStore(t, 2)
	publishCalled := make(chan struct{}, 8)
	pub := PublisherFunc(func(context.Context, Event) error {
		publishCalled <- struct{}{}
		return nil
	})
	relay := NewRelay(store, pub, WithClaimLease(time.Minute))

	// 领取前 ctx 已取消：事件不应交给 Publish，按当前 ClaimID 立即 Nack。
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res, err := relay.DeliverOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Claimed != 2 || res.Failed != 2 || res.Delivered != 0 {
		t.Fatalf("res=%+v", res)
	}
	select {
	case <-publishCalled:
		t.Fatal("Publish must not be called when ctx canceled before publish")
	default:
	}

	// 立即 Nack 使其无需等待租约即可重投。
	res, err = relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Claimed != 2 || res.Delivered != 2 {
		t.Fatalf("res=%+v", res)
	}
}

func TestRelayLeaseCancelDuringPublishFollowsPublishResult(t *testing.T) {
	// 情形一：Publish 期间取消，但 Publish 返回 nil —— 照常 Ack。
	store, _, _ := leasedTestStore(t, 1)
	started := make(chan struct{})
	release := make(chan struct{})
	pub := PublisherFunc(func(ctx context.Context, _ Event) error {
		close(started)
		<-release
		// 即使 ctx 已取消，Publish 的业务结论是成功。
		return nil
	})
	relay := NewRelay(store, pub, WithClaimLease(time.Minute))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var res RelayResult
	var err error
	go func() {
		res, err = relay.DeliverOnce(ctx)
		close(done)
	}()
	<-started
	cancel()
	close(release)
	<-done
	if err != nil {
		t.Fatal(err)
	}
	if res.Delivered != 1 || store.TotalEvents() != 0 {
		t.Fatalf("successful publish despite cancel must ack: res=%+v remaining=%d", res, store.TotalEvents())
	}

	// 情形二：Publish 期间取消且 Publish 返回 error —— 照常 Nack，立即可重领。
	store2, _, _ := leasedTestStore(t, 1)
	started2 := make(chan struct{})
	release2 := make(chan struct{})
	var once2 sync.Once
	pub2 := PublisherFunc(func(context.Context, Event) error {
		once2.Do(func() {
			close(started2)
			<-release2
		})
		return errors.New("publish failed during shutdown")
	})
	relay2 := NewRelay(store2, pub2, WithClaimLease(time.Minute))
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan struct{})
	var res2 RelayResult
	go func() {
		res2, _ = relay2.DeliverOnce(ctx2)
		close(done2)
	}()
	<-started2
	cancel2()
	close(release2)
	<-done2
	if res2.Failed != 1 {
		t.Fatalf("failed publish during cancel must nack: %+v", res2)
	}
	next, err := relay2.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if next.Claimed != 1 {
		t.Fatalf("nacked event must be immediately reclaimable: %+v", next)
	}
}

func TestRelayToleratesStaleClaimAfterTakeover(t *testing.T) {
	store, clk, _ := leasedTestStore(t, 1)
	takeover := &staleNackStore{MemoryStore: store}
	pub := PublisherFunc(func(context.Context, Event) error {
		return errors.New("downstream unavailable")
	})
	relay := NewRelay(takeover, pub, WithClaimLease(time.Minute))

	res, err := relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatalf("relay must swallow ErrStaleClaim: %v", err)
	}
	if res.Failed != 1 {
		t.Fatalf("res=%+v", res)
	}

	// 首次 Nack 被判定为 stale（模拟已被接管），事件仍在旧/新租约中；
	// 到期后由下一轮恢复。
	clk.advance(time.Minute + time.Second)
	if store.PendingCount() != 1 {
		t.Fatalf("stale nack must not make event reclaimable early: %d", store.PendingCount())
	}
}

// plainStore 只实现 Store，刻意不实现 ClaimLeaseStore。
type plainStore struct {
	inner *MemoryStore
}

func (s *plainStore) LoadSnapshot(ctx context.Context, bk, ik string) (Snapshot, error) {
	return s.inner.LoadSnapshot(ctx, bk, ik)
}
func (s *plainStore) Commit(ctx context.Context, bk, ik string, fn func(Tx) error) error {
	return s.inner.Commit(ctx, bk, ik, fn)
}
func (s *plainStore) ClaimPendingEvents(ctx context.Context, max int) ([]*Event, error) {
	return s.inner.ClaimPendingEvents(ctx, max)
}
func (s *plainStore) AckEvent(ctx context.Context, id string) error {
	return s.inner.AckEvent(ctx, id)
}
func (s *plainStore) NackEvent(ctx context.Context, id string) error {
	return s.inner.NackEvent(ctx, id)
}

var _ Store = (*plainStore)(nil)

func TestRelayLeaseUnsupportedByStore(t *testing.T) {
	store, _, _ := leasedTestStore(t, 1)
	plain := &plainStore{inner: store}
	pub := PublisherFunc(func(context.Context, Event) error { return nil })
	relay := NewRelay(plain, pub, WithClaimLease(time.Minute))

	_, err := relay.DeliverOnce(context.Background())
	if !errors.Is(err, ErrClaimLeaseUnsupported) {
		t.Fatalf("DeliverOnce err=%v want ErrClaimLeaseUnsupported", err)
	}
	// Run 原样退出，不重试、不等待。
	done := make(chan error, 1)
	go func() { done <- relay.Run(context.Background()) }()
	select {
	case err := <-done:
		if !errors.Is(err, ErrClaimLeaseUnsupported) {
			t.Fatalf("Run err=%v want ErrClaimLeaseUnsupported", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run must return immediately when lease unsupported")
	}

	// 事件未被领取，默认路径仍可正常投递。
	defaultRelay := NewRelay(plain, pub)
	res, err := defaultRelay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Claimed != 1 || res.Delivered != 1 {
		t.Fatalf("default path after unsupported lease: %+v", res)
	}
}

func TestWithClaimLeaseNonPositiveIsIgnored(t *testing.T) {
	pub := PublisherFunc(func(context.Context, Event) error { return nil })

	for _, d := range []time.Duration{0, -time.Second, -time.Nanosecond} {
		store, _, _ := leasedTestStore(t, 1)
		plain := &plainStore{inner: store}
		relay := NewRelay(plain, pub, WithClaimLease(d))
		res, err := relay.DeliverOnce(context.Background())
		if err != nil {
			t.Fatalf("ttl=%v: %v", d, err)
		}
		if res.Claimed != 1 || res.Delivered != 1 || store.TotalEvents() != 0 {
			t.Fatalf("ttl=%v must keep default behavior: %+v", d, res)
		}
	}
}

// errCrashAck 模拟 worker 在 Ack 调用时崩溃/存储暂不可用。
var errCrashAck = errors.New("ack storage unavailable")

type crashAckStore struct {
	*MemoryStore
}

func (s *crashAckStore) AckLeasedEvent(ctx context.Context, eventID, claimID string) error {
	return errCrashAck
}

// staleNackStore 让第一次 NackLeasedEvent 返回 ErrStaleClaim，
// 模拟租约到期后事件已被新领取者接管。
type staleNackStore struct {
	*MemoryStore
	done bool
}

func (s *staleNackStore) NackLeasedEvent(ctx context.Context, eventID, claimID string) error {
	if !s.done {
		s.done = true
		return ErrStaleClaim
	}
	return s.MemoryStore.NackLeasedEvent(ctx, eventID, claimID)
}
