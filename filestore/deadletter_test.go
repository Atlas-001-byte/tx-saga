package filestore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/txsaga/txsaga"
)

// commitExecWithEvents 建立一条执行记录并追加 n 条事件，返回事件 ID。
func commitExecWithEvents(t *testing.T, s *FileStore, bk, ik string, n int) []string {
	t.Helper()
	err := s.Commit(context.Background(), bk, ik, func(tx txsaga.Tx) error {
		tx.Create(&txsaga.ExecutionState{
			SagaName: "dl-saga", Fingerprint: "fp", Status: txsaga.StatusCompleted,
			Steps: []txsaga.StepState{{Name: "s"}},
		})
		for i := 0; i < n; i++ {
			tx.AppendEvent(fmt.Sprintf("evt-type-%d", i), txsaga.EventPayload{
				SagaName: "dl-saga", IdempotencyKey: ik, Step: fmt.Sprintf("s%d", i),
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

func listOne(t *testing.T, s *FileStore, bk, ik string) txsaga.EventRecord {
	t.Helper()
	page, err := s.ListEvents(context.Background(), txsaga.EventHistoryQuery{
		SagaName: "dl-saga", BusinessKey: bk, IdempotencyKey: ik,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 1 {
		t.Fatalf("history len = %d, want 1", len(page.Events))
	}
	return page.Events[0]
}

func TestFileStoreDeadLetterBoundedDelivery(t *testing.T) {
	s := newTestStore(t)
	ids := commitExecWithEvents(t, s, "bk", "ik", 1)

	relay := txsaga.NewRelay(s, txsaga.PublisherFunc(func(context.Context, txsaga.Event) error {
		return errors.New("boom")
	}), txsaga.WithMaxDeliveries(2))

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
	if res.Claimed != 1 || res.DeadLettered != 1 || res.Failed != 0 {
		t.Fatalf("round2 = %+v, want Claimed=1 DeadLettered=1", res)
	}
	if got := s.PendingCount(); got != 0 {
		t.Fatalf("pending = %d, want 0", got)
	}

	rec := listOne(t, s, "bk", "ik")
	if rec.ID != ids[0] || rec.Status != txsaga.EventStatusDeadLettered {
		t.Fatalf("rec = %+v", rec)
	}
	if rec.Deliveries != 2 || rec.LastAttemptAt.IsZero() {
		t.Fatalf("deliveries=%d last=%v", rec.Deliveries, rec.LastAttemptAt)
	}
	if !strings.Contains(rec.DeadLetterReason, "boom") || rec.DeadLetteredAt.IsZero() {
		t.Fatalf("reason=%q at=%v", rec.DeadLetterReason, rec.DeadLetteredAt)
	}
}

func TestFileStoreDeadLetterPersistsAcrossReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	clk := withFakeClock(s)
	ids := commitExecWithEvents(t, s, "bk", "ik", 1)

	relay := txsaga.NewRelay(s, txsaga.PublisherFunc(func(context.Context, txsaga.Event) error {
		return errors.New("boom")
	}), txsaga.WithMaxDeliveries(1))
	if _, err := relay.DeliverOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	before := listOne(t, s, "bk", "ik")
	if before.Status != txsaga.EventStatusDeadLettered {
		t.Fatalf("precondition status = %q", before.Status)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 重新 Open：死信记录（含原因、时间、累计 Deliveries、轮次计数）一致，
	// 事件仍退出普通领取。
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	s2.now = clk.now
	after := listOne(t, s2, "bk", "ik")
	if after.Status != txsaga.EventStatusDeadLettered ||
		after.DeadLetterReason != before.DeadLetterReason ||
		!after.DeadLetteredAt.Equal(before.DeadLetteredAt) ||
		after.Deliveries != before.Deliveries ||
		!after.LastAttemptAt.Equal(before.LastAttemptAt) ||
		after.ID != ids[0] || after.Type != before.Type ||
		!after.OccurredAt.Equal(before.OccurredAt) {
		t.Fatalf("dead letter record changed across reopen:\nbefore=%+v\nafter=%+v", before, after)
	}
	if got := s2.PendingCount(); got != 0 {
		t.Fatalf("pending after reopen = %d, want 0", got)
	}
	claimed, err := s2.ClaimPendingEvents(context.Background(), 5)
	if err != nil || len(claimed) != 0 {
		t.Fatalf("claimed dead letter after reopen: %v %v", claimed, err)
	}

	// 重开后重新入队：回到待投递、本轮次数归零、累计与历史失败信息保留。
	eng := txsaga.NewEngine(s2)
	if err := eng.RequeueDeadLetter(context.Background(), ids[0]); err != nil {
		t.Fatal(err)
	}
	rec := listOne(t, s2, "bk", "ik")
	if rec.Status != txsaga.EventStatusPending || rec.Deliveries != 1 ||
		rec.DeadLetterReason == "" || rec.DeadLetteredAt.IsZero() {
		t.Fatalf("after requeue: %+v", rec)
	}
	// 新轮次再次耗尽仍进入死信。
	relay2 := txsaga.NewRelay(s2, txsaga.PublisherFunc(func(context.Context, txsaga.Event) error {
		return errors.New("boom-2")
	}), txsaga.WithMaxDeliveries(1))
	res, err := relay2.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.DeadLettered != 1 {
		t.Fatalf("second exhaustion = %+v, want DeadLettered=1", res)
	}
	rec = listOne(t, s2, "bk", "ik")
	if rec.Status != txsaga.EventStatusDeadLettered || rec.Deliveries != 2 ||
		!strings.Contains(rec.DeadLetterReason, "boom-2") {
		t.Fatalf("after second exhaustion: %+v", rec)
	}
}

func TestFileStoreRequeueDeadLetterErrors(t *testing.T) {
	s := newTestStore(t)
	ids := commitExecWithEvents(t, s, "bk", "ik", 2)
	eng := txsaga.NewEngine(s)

	if err := eng.RequeueDeadLetter(context.Background(), "evt-99999999999999999999"); !errors.Is(err, txsaga.ErrEventNotFound) {
		t.Fatalf("missing err = %v, want ErrEventNotFound", err)
	}
	// 待投递。
	if err := eng.RequeueDeadLetter(context.Background(), ids[0]); !errors.Is(err, txsaga.ErrEventNotDeadLettered) {
		t.Fatalf("pending err = %v, want ErrEventNotDeadLettered", err)
	}
	// 领取中。
	if _, err := s.ClaimPendingEvents(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if err := eng.RequeueDeadLetter(context.Background(), ids[0]); !errors.Is(err, txsaga.ErrEventNotDeadLettered) {
		t.Fatalf("claimed err = %v, want ErrEventNotDeadLettered", err)
	}
	// 已投递。
	if err := s.AckEvent(context.Background(), ids[0]); err != nil {
		t.Fatal(err)
	}
	if err := eng.RequeueDeadLetter(context.Background(), ids[0]); !errors.Is(err, txsaga.ErrEventNotDeadLettered) {
		t.Fatalf("acked err = %v, want ErrEventNotDeadLettered", err)
	}
	// context 取消：原样返回，事件不变。
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := eng.RequeueDeadLetter(ctx, ids[1]); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled err = %v, want context.Canceled", err)
	}
	if got := s.PendingCount(); got != 1 {
		t.Fatalf("pending after canceled requeue = %d, want 1", got)
	}
}

func TestFileStoreDeadLetterLeaseRecovery(t *testing.T) {
	s := newTestStore(t)
	clk := withFakeClock(s)
	ids := commitExecWithEvents(t, s, "bk", "ik", 1)
	ttl := time.Minute

	// 模拟 worker 崩溃：领取（本轮次数即达上限）后不退回，租约到期。
	if _, err := s.ClaimPendingEventsLeased(context.Background(), ttl, 1); err != nil {
		t.Fatal(err)
	}
	clk.advance(2 * ttl)

	published := 0
	relay := txsaga.NewRelay(s, txsaga.PublisherFunc(func(context.Context, txsaga.Event) error {
		published++
		return nil
	}), txsaga.WithClaimLease(ttl), txsaga.WithMaxDeliveries(1))
	res, err := relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if published != 0 {
		t.Fatalf("exhausted event published %d times, want 0", published)
	}
	if res.Claimed != 1 || res.DeadLettered != 1 {
		t.Fatalf("res = %+v, want Claimed=1 DeadLettered=1", res)
	}
	if rec := listOne(t, s, "bk", "ik"); rec.Status != txsaga.EventStatusDeadLettered || rec.ID != ids[0] {
		t.Fatalf("rec = %+v", rec)
	}
}

func TestFileStoreCorruptDeadLetterSnapshot(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ids := commitExecWithEvents(t, s, "bk", "ik", 1)
	if _, err := s.ClaimPendingEvents(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 篡改快照：领取中的事件同时被标记死信，重开必须报损坏且不覆盖原文件。
	raw, err := os.ReadFile(filepath.Join(dir, stateFile))
	if err != nil {
		t.Fatal(err)
	}
	tampered := strings.Replace(string(raw),
		fmt.Sprintf(`"id": "%s"`, ids[0]),
		fmt.Sprintf(`"id": "%s", "dead_lettered": true, "dead_lettered_at": "2026-01-01T00:00:00Z"`, ids[0]), 1)
	if tampered == string(raw) {
		t.Fatal("tamper failed: id not found")
	}
	if err := os.WriteFile(filepath.Join(dir, stateFile), []byte(tampered), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); !errors.Is(err, ErrCorruptStore) {
		t.Fatalf("reopen err = %v, want ErrCorruptStore", err)
	}
}
