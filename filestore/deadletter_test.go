package filestore

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/txsaga/txsaga"
)

// openAt 打开指定目录的 FileStore 并注册清理。
func openAt(t *testing.T, dir string) *FileStore {
	t.Helper()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// recordOf 直接读取存储内部事件并生成只读记录（同包测试辅助）。
func recordOf(t *testing.T, s *FileStore, id string) txsaga.EventRecord {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	me := s.d.events[id]
	if me == nil {
		t.Fatalf("event %s not found", id)
	}
	return toEventRecord(me, s.now())
}

func TestDeadLetterPersistsAcrossReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	s := openAt(t, dir)
	clk := withFakeClock(s)
	ids := appendEvents(t, s, "bk", "ik", 1)

	pubErr := errors.New("downstream gone")
	relay := txsaga.NewRelay(s, txsaga.PublisherFunc(func(context.Context, txsaga.Event) error {
		return pubErr
	}), txsaga.WithMaxDeliveries(2))

	// 第 1 轮：未达上限，Nack 保留。
	res, err := relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Claimed != 1 || res.Failed != 1 || res.DeadLettered != 0 {
		t.Fatalf("round1 res=%+v", res)
	}
	clk.advance(time.Minute)
	// 第 2 轮：达到上限，同一操作转死信。
	res, err = relay.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Claimed != 1 || res.Failed != 0 || res.DeadLettered != 1 {
		t.Fatalf("round2 res=%+v", res)
	}
	before := recordOf(t, s, ids[0])
	if before.Status != txsaga.EventStatusDeadLettered {
		t.Fatalf("status=%q want dead_lettered", before.Status)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 重新 Open：死信身份、负载、累计 Deliveries、最近领取时间、失败原因
	// 与进入死信时间全部保留。
	s2 := openAt(t, dir)
	after := recordOf(t, s2, ids[0])
	if after.Status != txsaga.EventStatusDeadLettered {
		t.Fatalf("after reopen status=%q", after.Status)
	}
	if after.ID != before.ID || after.Type != before.Type || !after.OccurredAt.Equal(before.OccurredAt) {
		t.Fatalf("identity changed across reopen: %+v -> %+v", before, after)
	}
	if after.Deliveries != 2 || after.LastError != pubErr.Error() {
		t.Fatalf("dead-letter metadata lost: %+v", after)
	}
	if !after.DeadLetteredAt.Equal(before.DeadLetteredAt) || after.DeadLetteredAt.IsZero() {
		t.Fatalf("dead-lettered at not preserved: %v -> %v", before.DeadLetteredAt, after.DeadLetteredAt)
	}
	if !after.LastAttemptAt.Equal(before.LastAttemptAt) || after.LastAttemptAt.IsZero() {
		t.Fatalf("last attempt not preserved: %v -> %v", before.LastAttemptAt, after.LastAttemptAt)
	}
	if s2.PendingCount() != 0 || s2.TotalEvents() != 1 {
		t.Fatalf("dead letter must stay out of pending: pending=%d total=%d",
			s2.PendingCount(), s2.TotalEvents())
	}

	// 死信不参与任何领取（有限或默认）。
	claimed, dead, err := s2.ClaimPendingEventsBounded(context.Background(), 2, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 0 || dead != 0 {
		t.Fatalf("bounded claim got %d events, %d dead", len(claimed), dead)
	}
	evs, err := s2.ClaimPendingEvents(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 0 {
		t.Fatalf("unbounded claim got %d events", len(evs))
	}
}

func TestRequeueAfterReopenRestartsRound(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	s := openAt(t, dir)
	ids := appendEvents(t, s, "bk", "ik", 1)
	fail := txsaga.PublisherFunc(func(context.Context, txsaga.Event) error {
		return errors.New("flaky")
	})
	relay := txsaga.NewRelay(s, fail, txsaga.WithMaxDeliveries(2))

	// 两轮失败进入死信（Deliveries=2）。
	for i := 0; i < 2; i++ {
		if _, err := relay.DeliverOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if rec := recordOf(t, s, ids[0]); rec.Status != txsaga.EventStatusDeadLettered {
		t.Fatalf("status=%q", rec.Status)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 重开后重新入队：恢复待投递，累计 Deliveries 与历史失败信息不回退。
	s2 := openAt(t, dir)
	eng := txsaga.NewEngine(s2)
	if err := eng.RequeueDeadLetterEvent(context.Background(), ids[0]); err != nil {
		t.Fatal(err)
	}
	rec := recordOf(t, s2, ids[0])
	if rec.Status != txsaga.EventStatusPending || rec.Deliveries != 2 ||
		rec.LastError != "flaky" || rec.DeadLetteredAt.IsZero() {
		t.Fatalf("after requeue rec=%+v", rec)
	}
	if s2.PendingCount() != 1 {
		t.Fatalf("requeued event must be claimable: pending=%d", s2.PendingCount())
	}

	// 新一轮有限计数：再失败两次才重新死信，累计 Deliveries 到 4。
	relay2 := txsaga.NewRelay(s2, fail, txsaga.WithMaxDeliveries(2))
	res, err := relay2.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Failed != 1 || res.DeadLettered != 0 {
		t.Fatalf("round1 after requeue res=%+v", res)
	}
	res, err = relay2.DeliverOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.DeadLettered != 1 {
		t.Fatalf("round2 after requeue res=%+v", res)
	}
	rec = recordOf(t, s2, ids[0])
	if rec.Status != txsaga.EventStatusDeadLettered || rec.Deliveries != 4 {
		t.Fatalf("rec=%+v want dead_lettered with cumulative deliveries 4", rec)
	}
}

func TestRequeueErrorsFileStore(t *testing.T) {
	s := newTestStore(t)
	ids := appendEvents(t, s, "bk", "ik", 3)
	eng := txsaga.NewEngine(s)
	relay := txsaga.NewRelay(s, txsaga.PublisherFunc(func(context.Context, txsaga.Event) error {
		return errors.New("x")
	}), txsaga.WithMaxDeliveries(1))

	// 一轮全部失败：三条事件都进入死信。
	if _, err := relay.DeliverOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	// ids[0] 重新入队恢复待投递；ids[1] 保持死信；ids[2] Ack 成已投递。
	if err := eng.RequeueDeadLetterEvent(context.Background(), ids[0]); err != nil {
		t.Fatal(err)
	}
	if err := s.AckEvent(context.Background(), ids[2]); err != nil {
		t.Fatal(err)
	}

	if err := eng.RequeueDeadLetterEvent(context.Background(), "evt-99999999999999999999"); !errors.Is(err, txsaga.ErrEventNotFound) {
		t.Fatalf("missing err=%v want ErrEventNotFound", err)
	}
	if err := eng.RequeueDeadLetterEvent(context.Background(), ids[0]); !errors.Is(err, txsaga.ErrEventNotDeadLettered) {
		t.Fatalf("pending err=%v want ErrEventNotDeadLettered", err)
	}
	if err := eng.RequeueDeadLetterEvent(context.Background(), ids[2]); !errors.Is(err, txsaga.ErrEventNotDeadLettered) {
		t.Fatalf("delivered err=%v want ErrEventNotDeadLettered", err)
	}

	// ctx 取消：返回该错误且不改变事件（也不落盘）。
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := eng.RequeueDeadLetterEvent(ctx, ids[1]); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled err=%v want context.Canceled", err)
	}
	if rec := recordOf(t, s, ids[1]); rec.Status != txsaga.EventStatusDeadLettered {
		t.Fatalf("canceled requeue changed event: status=%q", rec.Status)
	}

	// 成功后重复调用：不追加事件、不制造第二个副本。
	if err := eng.RequeueDeadLetterEvent(context.Background(), ids[1]); err != nil {
		t.Fatal(err)
	}
	if err := eng.RequeueDeadLetterEvent(context.Background(), ids[1]); !errors.Is(err, txsaga.ErrEventNotDeadLettered) {
		t.Fatalf("second requeue err=%v want ErrEventNotDeadLettered", err)
	}
	if s.PendingCount() != 2 { // ids[0] 与 ids[1] 各一份
		t.Fatalf("duplicate requeue copies: pending=%d want 2", s.PendingCount())
	}
}

func TestLeaseRecoveryIsolationAcrossReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	s := openAt(t, dir)
	clk := withFakeClock(s)
	ids := appendEvents(t, s, "bk", "ik", 1)
	ttl := time.Minute

	// 租约领取（Deliveries=1，已达 max=1）后 worker 退出：不 Ack、不 Nack。
	claims, dead, err := s.ClaimPendingEventsLeasedBounded(context.Background(), ttl, 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claims) != 1 || dead != 0 {
		t.Fatalf("claims=%d dead=%d", len(claims), dead)
	}
	clk.advance(2 * ttl)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 重开后租约已到期：本轮次数已用尽，领取操作直接隔离为死信，
	// 不再生成新租约、不交给 Publisher。
	s2 := openAt(t, dir)
	clk2 := withFakeClock(s2)
	clk2.advance(2 * ttl)
	claims, dead, err = s2.ClaimPendingEventsLeasedBounded(context.Background(), ttl, 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claims) != 0 || dead != 1 {
		t.Fatalf("claims=%d dead=%d want 0/1", len(claims), dead)
	}
	rec := recordOf(t, s2, ids[0])
	if rec.Status != txsaga.EventStatusDeadLettered || rec.Deliveries != 1 {
		t.Fatalf("rec=%+v", rec)
	}
}

func TestDeadLetteredEventInPendingIsCorrupt(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	s := openAt(t, dir)
	ids := appendEvents(t, s, "bk", "ik", 1)
	relay := txsaga.NewRelay(s, txsaga.PublisherFunc(func(context.Context, txsaga.Event) error {
		return errors.New("x")
	}), txsaga.WithMaxDeliveries(1))
	if _, err := relay.DeliverOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rec := recordOf(t, s, ids[0]); rec.Status != txsaga.EventStatusDeadLettered {
		t.Fatalf("status=%q", rec.Status)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 篡改快照：把死信事件塞回待领取队列，Open 必须拒绝且不覆盖原文件。
	raw, err := os.ReadFile(filepath.Join(dir, stateFile))
	if err != nil {
		t.Fatal(err)
	}
	var snap map[string]any
	if err := json.Unmarshal(raw, &snap); err != nil {
		t.Fatal(err)
	}
	snap["pending"] = []any{ids[0]}
	tampered, err := json.Marshal(snap)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, stateFile), tampered, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); !errors.Is(err, ErrCorruptStore) {
		t.Fatalf("Open err=%v want ErrCorruptStore", err)
	}
}
