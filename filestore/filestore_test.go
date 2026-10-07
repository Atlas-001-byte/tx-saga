package filestore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/txsaga/txsaga"
)

// threeStepDef 返回一个三步骤顺序定义；各动作可通过 hooks 定制。
func threeStepDef(hooks map[string]txsaga.ActionFunc) txsaga.Definition {
	action := func(name string) txsaga.ActionFunc {
		if h, ok := hooks[name]; ok {
			return h
		}
		return func(context.Context, txsaga.ExecutionView) error { return nil }
	}
	return txsaga.Definition{
		Name:    "order",
		Version: "v1",
		Steps: []txsaga.Step{
			{Name: "reserve", Action: action("reserve")},
			{Name: "charge", Action: action("charge")},
			{Name: "notify", Action: action("notify")},
		},
	}
}

func testRequest(key string) txsaga.ExecutionRequest {
	return txsaga.ExecutionRequest{
		BusinessKey:    key,
		IdempotencyKey: "idem-1",
		Payload:        map[string]any{"order": key},
	}
}

func openStore(t *testing.T, dir string) *FileStore {
	t.Helper()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open(%q): %v", dir, err)
	}
	return s
}

// TestExecuteInterruptReopenResume 验证：执行被 ctx 取消打断后，已提交的
// 状态与事件完整落盘；重新打开目录可继续执行到终态，历史完整。
func TestExecuteInterruptReopenResume(t *testing.T) {
	dir := t.TempDir()
	store := openStore(t, dir)
	engine := txsaga.NewEngine(store)

	ctx, cancel := context.WithCancel(context.Background())
	var chargeCalls atomic.Int32
	var gotPayload any
	def := threeStepDef(map[string]txsaga.ActionFunc{
		"reserve": func(_ context.Context, exec txsaga.ExecutionView) error {
			gotPayload = exec.Payload()
			return nil
		},
		"charge": func(cctx context.Context, _ txsaga.ExecutionView) error {
			if chargeCalls.Add(1) == 1 {
				cancel() // 模拟进程崩溃：动作未确认，Execute 随 ctx 取消退出
				return cctx.Err()
			}
			return nil
		},
	})

	req := testRequest("order-1")
	if _, err := engine.Execute(ctx, def, req); !errors.Is(err, context.Canceled) {
		t.Fatalf("first Execute err=%v, want context.Canceled", err)
	}
	// 动作收到的是本次 Execute 传入的 Payload 本体。
	wantPayload := map[string]any{"order": "order-1"}
	if !reflect.DeepEqual(gotPayload, wantPayload) {
		t.Fatalf("action payload=%v, want %v", gotPayload, wantPayload)
	}
	// 取消前已提交：running，reserve 成功，2 条事件（started + step_succeeded）。
	snap, err := store.LoadSnapshot(context.Background(), req.BusinessKey, req.IdempotencyKey)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if snap.State == nil || snap.State.Status != txsaga.StatusRunning {
		t.Fatalf("status=%v, want running", snap.State)
	}
	if snap.State.Steps[0].Status != txsaga.StepResultSucceeded {
		t.Fatalf("reserve status=%q, want succeeded", snap.State.Steps[0].Status)
	}
	if got := store.TotalEvents(); got != 2 {
		t.Fatalf("TotalEvents=%d, want 2", got)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 重新打开：继续执行到 completed 终态。
	store2 := openStore(t, dir)
	defer store2.Close()
	engine2 := txsaga.NewEngine(store2)
	res, err := engine2.Execute(context.Background(), def, req)
	if err != nil {
		t.Fatalf("second Execute: %v", err)
	}
	if res.Status != txsaga.StatusCompleted || !res.Terminal {
		t.Fatalf("status=%q terminal=%v, want completed", res.Status, res.Terminal)
	}
	if res.Steps[0].Attempts != 1 || res.Steps[1].Attempts != 1 {
		t.Fatalf("attempts=%d/%d, want 1/1", res.Steps[0].Attempts, res.Steps[1].Attempts)
	}
	if chargeCalls.Load() != 2 {
		t.Fatalf("charge calls=%d, want 2", chargeCalls.Load())
	}

	// Payload 经 JSON 往返后语义不变（解码为通用 JSON 值）。
	snap2, err := store2.LoadSnapshot(context.Background(), req.BusinessKey, req.IdempotencyKey)
	if err != nil {
		t.Fatalf("LoadSnapshot after reopen: %v", err)
	}
	if !reflect.DeepEqual(snap2.State.Payload, wantPayload) {
		t.Fatalf("persisted payload=%#v, want %#v", snap2.State.Payload, wantPayload)
	}

	// 完整事件链：started + 3×step_succeeded + execution_completed。
	page, err := engine2.ListEvents(context.Background(), txsaga.EventHistoryQuery{
		SagaName: "order", BusinessKey: req.BusinessKey, IdempotencyKey: req.IdempotencyKey,
	})
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	wantTypes := []string{
		txsaga.EventExecutionStarted,
		txsaga.EventStepSucceeded, txsaga.EventStepSucceeded, txsaga.EventStepSucceeded,
		txsaga.EventExecutionCompleted,
	}
	if len(page.Events) != len(wantTypes) || page.HasMore {
		t.Fatalf("history len=%d hasMore=%v, want %d events", len(page.Events), page.HasMore, len(wantTypes))
	}
	for i, wt := range wantTypes {
		if page.Events[i].Type != wt {
			t.Fatalf("event[%d].Type=%q, want %q", i, page.Events[i].Type, wt)
		}
	}
}

// TestFailureCompensationPersistsAcrossReopen 验证失败与逆序补偿的终态、
// 事件链在重开后保持一致。
func TestFailureCompensationPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	store := openStore(t, dir)
	engine := txsaga.NewEngine(store)

	var compensated []string
	def := txsaga.Definition{
		Name:    "order",
		Version: "v1",
		Steps: []txsaga.Step{
			{Name: "a", Action: ok, Compensate: compensate("a", &compensated)},
			{Name: "b", Action: ok, Compensate: compensate("b", &compensated)},
			{Name: "c", Action: func(context.Context, txsaga.ExecutionView) error {
				return errors.New("boom")
			}},
		},
	}
	req := testRequest("order-fail")
	res, err := engine.Execute(context.Background(), def, req)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Status != txsaga.StatusFailed {
		t.Fatalf("status=%q, want failed", res.Status)
	}
	if !reflect.DeepEqual(compensated, []string{"b", "a"}) {
		t.Fatalf("compensation order=%v, want [b a]", compensated)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	store2 := openStore(t, dir)
	defer store2.Close()
	engine2 := txsaga.NewEngine(store2)
	got, err := engine2.GetResult(context.Background(), "order", req.BusinessKey, req.IdempotencyKey)
	if err != nil {
		t.Fatalf("GetResult after reopen: %v", err)
	}
	if got.Status != txsaga.StatusFailed || got.FailedStep != "c" {
		t.Fatalf("reopened result=%+v, want failed at c", got)
	}
	if got.Steps[0].Result != txsaga.StepResultCompensated ||
		got.Steps[1].Result != txsaga.StepResultCompensated ||
		got.Steps[2].Result != txsaga.StepResultFailed {
		t.Fatalf("step results=%v/%v/%v", got.Steps[0].Result, got.Steps[1].Result, got.Steps[2].Result)
	}
	// 终态固定：同身份再次执行不再调用任何动作。
	again, err := engine2.Execute(context.Background(), def, req)
	if err != nil || again.Status != txsaga.StatusFailed {
		t.Fatalf("terminal re-execute: status=%q err=%v", again.Status, err)
	}
	// 事件链：started + 2×step_succeeded + step_failed + 2×step_compensated + execution_failed。
	page, err := engine2.ListEvents(context.Background(), txsaga.EventHistoryQuery{
		SagaName: "order", BusinessKey: req.BusinessKey, IdempotencyKey: req.IdempotencyKey,
	})
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if len(page.Events) != 7 {
		t.Fatalf("history len=%d, want 7", len(page.Events))
	}
}

func ok(context.Context, txsaga.ExecutionView) error { return nil }

func compensate(name string, log *[]string) txsaga.CompensationFunc {
	return func(context.Context, txsaga.ExecutionView) error {
		*log = append(*log, name)
		return nil
	}
}

// TestDefinitionConflictAcrossReopen 验证业务键与定义的绑定关系持久化。
func TestDefinitionConflictAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	store := openStore(t, dir)
	engine := txsaga.NewEngine(store)
	def := threeStepDef(nil)
	if _, err := engine.Execute(context.Background(), def, testRequest("order-x")); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	store2 := openStore(t, dir)
	defer store2.Close()
	engine2 := txsaga.NewEngine(store2)
	other := def
	other.Version = "v2"
	_, err := engine2.Execute(context.Background(), other, txsaga.ExecutionRequest{
		BusinessKey: "order-x", IdempotencyKey: "idem-2",
	})
	if !errors.Is(err, txsaga.ErrDefinitionConflict) {
		t.Fatalf("err=%v, want ErrDefinitionConflict", err)
	}
	// 同定义、不同幂等键：允许。
	if _, err := engine2.Execute(context.Background(), def, txsaga.ExecutionRequest{
		BusinessKey: "order-x", IdempotencyKey: "idem-2",
	}); err != nil {
		t.Fatalf("same definition new idempotency key: %v", err)
	}
}

// TestOpenLocked 验证同一进程内同一目录只允许一个 FileStore。
func TestOpenLocked(t *testing.T) {
	dir := t.TempDir()
	store := openStore(t, dir)

	if _, err := Open(dir); !errors.Is(err, ErrStoreLocked) {
		t.Fatalf("second Open err=%v, want ErrStoreLocked", err)
	}
	// 经符号链接/不同写法指向同一目录同样被锁定。
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(dir, link); err == nil {
		if _, err := Open(link); !errors.Is(err, ErrStoreLocked) {
			t.Fatalf("Open via symlink err=%v, want ErrStoreLocked", err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Close 释放占用后可重新打开。
	store2 := openStore(t, dir)
	defer store2.Close()
	if err := store.Close(); err != nil { // 重复 Close 安全
		t.Fatalf("second Close: %v", err)
	}
}

// TestOpenInvalidDir 验证目录不可创建/不可写时返回 ErrStoreOpen。
func TestOpenInvalidDir(t *testing.T) {
	file := filepath.Join(t.TempDir(), "regular-file")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(filepath.Join(file, "sub")); !errors.Is(err, ErrStoreOpen) {
		t.Fatalf("Open under regular file err=%v, want ErrStoreOpen", err)
	}
}

// TestCorruptStore 验证损坏、截断、不一致的快照一律报 ErrCorruptStore，
// 且原文件不被覆盖。
func TestCorruptStore(t *testing.T) {
	cases := map[string][]byte{
		"garbage":   []byte("{not json"),
		"empty":     {},
		"truncated": []byte(`{"version":1,"seq":3,"claim_seq":0,"execs":{"order`),
		"version":   []byte(`{"version":2,"seq":0,"claim_seq":0,"execs":{},"bindings":{},"events":{},"pending":[],"history":[]}`),
		"dangling":  []byte(`{"version":1,"seq":0,"claim_seq":0,"execs":{},"bindings":{},"events":{},"pending":["evt-1"],"history":[]}`),
		"bad_exec_key": []byte(`{"version":1,"seq":0,"claim_seq":0,` +
			`"execs":{"wrong-key":{"business_key":"b","saga_name":"s","idempotency_key":"i","fingerprint":"f","status":"running","steps":[],"created_at":"2024-01-01T00:00:00Z","updated_at":"2024-01-01T00:00:00Z"}},` +
			`"bindings":{},"events":{},"pending":[],"history":[]}`),
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, storeFileName)
			if err := os.WriteFile(path, content, 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(dir); !errors.Is(err, ErrCorruptStore) {
				t.Fatalf("Open err=%v, want ErrCorruptStore", err)
			}
			got, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, content) {
				t.Fatalf("original file was modified: %q", got)
			}
		})
	}

	// 真实数据被截断后同样报错，且修复文件（重新写回完整快照）前无法打开。
	t.Run("truncated_real", func(t *testing.T) {
		dir := t.TempDir()
		store := openStore(t, dir)
		engine := txsaga.NewEngine(store)
		if _, err := engine.Execute(context.Background(), threeStepDef(nil), testRequest("order-t")); err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if err := store.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		path := filepath.Join(dir, storeFileName)
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw[:len(raw)/2], 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(dir); !errors.Is(err, ErrCorruptStore) {
			t.Fatalf("Open truncated err=%v, want ErrCorruptStore", err)
		}
	})
}

// TestUnsupportedPayload 验证无法 JSON 表示的 Payload 使写入返回
// ErrUnsupportedPayload，且不留下半次提交。
func TestUnsupportedPayload(t *testing.T) {
	dir := t.TempDir()
	store := openStore(t, dir)
	engine := txsaga.NewEngine(store)

	req := txsaga.ExecutionRequest{
		BusinessKey:    "order-bad",
		IdempotencyKey: "idem-1",
		Payload:        map[string]any{"callback": func() {}},
	}
	if _, err := engine.Execute(context.Background(), threeStepDef(nil), req); !errors.Is(err, ErrUnsupportedPayload) {
		t.Fatalf("Execute err=%v, want ErrUnsupportedPayload", err)
	}
	// 状态与事件都没有留下半次提交。
	snap, err := store.LoadSnapshot(context.Background(), req.BusinessKey, req.IdempotencyKey)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if snap.State != nil || snap.Bound {
		t.Fatalf("snapshot=%+v, want empty", snap)
	}
	if got := store.TotalEvents(); got != 0 {
		t.Fatalf("TotalEvents=%d, want 0", got)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// 重开后依然干净。
	store2 := openStore(t, dir)
	defer store2.Close()
	if got := store2.TotalEvents(); got != 0 {
		t.Fatalf("TotalEvents after reopen=%d, want 0", got)
	}
}

// TestClaimAckNackDeliveries 验证普通领取的锁定语义、投递计数与
// 已 Ack 事件的审计保留。
func TestClaimAckNackDeliveries(t *testing.T) {
	dir := t.TempDir()
	store := openStore(t, dir)
	defer store.Close()
	engine := txsaga.NewEngine(store)
	req := testRequest("order-claim")
	if _, err := engine.Execute(context.Background(), threeStepDef(nil), req); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	ctx := context.Background()

	evs, err := store.ClaimPendingEvents(ctx, 10)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(evs) != 5 {
		t.Fatalf("claimed=%d, want 5", len(evs))
	}
	for i, ev := range evs {
		if ev.Deliveries() != 1 || ev.LastAttemptAt().IsZero() {
			t.Fatalf("event[%d] deliveries=%d lastAttempt=%v", i, ev.Deliveries(), ev.LastAttemptAt())
		}
	}
	// 已领取事件被锁定，不能重复领取。
	if again, _ := store.ClaimPendingEvents(ctx, 10); len(again) != 0 {
		t.Fatalf("re-claim got %d events, want 0", len(again))
	}
	// Nack 退回后可再次领取，投递计数累计。
	if err := store.NackEvent(ctx, evs[0].ID); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	again, err := store.ClaimPendingEvents(ctx, 10)
	if err != nil {
		t.Fatalf("re-claim: %v", err)
	}
	if len(again) != 1 || again[0].ID != evs[0].ID || again[0].Deliveries() != 2 {
		t.Fatalf("re-claimed=%+v, want first event with deliveries 2", again)
	}
	// Ack 后事件退出活动 Outbox，但审计记录保留。
	for _, ev := range evs {
		if err := store.AckEvent(ctx, ev.ID); err != nil {
			t.Fatalf("Ack %s: %v", ev.ID, err)
		}
	}
	if got := store.PendingCount(); got != 0 {
		t.Fatalf("PendingCount=%d, want 0", got)
	}
	if err := store.AckEvent(ctx, evs[0].ID); err == nil {
		t.Fatal("Ack of removed event should fail")
	}
	page, err := store.ListEvents(ctx, txsaga.EventHistoryQuery{
		SagaName: "order", BusinessKey: req.BusinessKey, IdempotencyKey: req.IdempotencyKey,
	})
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if len(page.Events) != 5 {
		t.Fatalf("history len=%d, want 5 (acked retained)", len(page.Events))
	}
	if page.Events[0].Deliveries != 2 {
		t.Fatalf("acked record deliveries=%d, want 2", page.Events[0].Deliveries)
	}
}

// TestClaimLeaseExpiry 验证带租约领取：租约期内不重领，到期后重新领取
// 并获得新 ClaimID，旧 ClaimID 的 Ack/Nack 报 ErrStaleClaim。
func TestClaimLeaseExpiry(t *testing.T) {
	dir := t.TempDir()
	store := openStore(t, dir)
	defer store.Close()
	clk := time.Now()
	store.now = func() time.Time { return clk }

	engine := txsaga.NewEngine(store)
	if _, err := engine.Execute(context.Background(), threeStepDef(nil), testRequest("order-lease")); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	ctx := context.Background()

	claims, err := store.ClaimPendingEventsLeased(ctx, time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimLeased: %v", err)
	}
	if len(claims) != 5 {
		t.Fatalf("claimed=%d, want 5", len(claims))
	}
	for i, c := range claims {
		if c.Event.Deliveries() != 1 || !c.Event.LastAttemptAt().Equal(clk) {
			t.Fatalf("claim[%d] deliveries=%d lastAttempt=%v", i, c.Event.Deliveries(), c.Event.LastAttemptAt())
		}
		if !c.ClaimedUntil.Equal(clk.Add(time.Minute)) {
			t.Fatalf("claim[%d] until=%v", i, c.ClaimedUntil)
		}
	}
	// 租约期内：普通与租约领取都拿不到。
	if got, _ := store.ClaimPendingEventsLeased(ctx, time.Minute, 10); len(got) != 0 {
		t.Fatalf("re-claim within lease got %d, want 0", len(got))
	}
	if got, _ := store.ClaimPendingEvents(ctx, 10); len(got) != 0 {
		t.Fatalf("plain claim within lease got %d, want 0", len(got))
	}
	// 到期后重新领取：新 ClaimID、投递计数累计。
	clk = clk.Add(2 * time.Minute)
	claims2, err := store.ClaimPendingEventsLeased(ctx, time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimLeased after expiry: %v", err)
	}
	if len(claims2) != 5 {
		t.Fatalf("re-claimed=%d, want 5", len(claims2))
	}
	if claims2[0].ClaimID == claims[0].ClaimID {
		t.Fatal("ClaimID not rotated after lease expiry")
	}
	if claims2[0].Event.Deliveries() != 2 {
		t.Fatalf("deliveries=%d, want 2", claims2[0].Event.Deliveries())
	}
	// 旧 ClaimID 已失效。
	if err := store.AckLeasedEvent(ctx, claims2[0].Event.ID, claims[0].ClaimID); !errors.Is(err, txsaga.ErrStaleClaim) {
		t.Fatalf("stale Ack err=%v, want ErrStaleClaim", err)
	}
	if err := store.NackLeasedEvent(ctx, claims2[0].Event.ID, claims[0].ClaimID); !errors.Is(err, txsaga.ErrStaleClaim) {
		t.Fatalf("stale Nack err=%v, want ErrStaleClaim", err)
	}
	// 当前 ClaimID 可 Nack：事件立即可领取且顺序不变。
	if err := store.NackLeasedEvent(ctx, claims2[0].Event.ID, claims2[0].ClaimID); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	claims3, err := store.ClaimPendingEventsLeased(ctx, time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimLeased after nack: %v", err)
	}
	if len(claims3) != 1 || claims3[0].Event.ID != claims2[0].Event.ID {
		t.Fatalf("after nack claimed=%v, want the nacked event", claims3)
	}
	// 当前 ClaimID 可 Ack。
	if err := store.AckLeasedEvent(ctx, claims3[0].Event.ID, claims3[0].ClaimID); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestLeaseSurvivesReopen 验证租约按墙钟持久化：重开后租约仍有效，
// 到期后可恢复领取；普通领取的锁定不持久化（worker 失联即恢复）。
func TestLeaseSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	store := openStore(t, dir)
	engine := txsaga.NewEngine(store)
	if _, err := engine.Execute(context.Background(), threeStepDef(nil), testRequest("order-lr")); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	ctx := context.Background()

	leased, err := store.ClaimPendingEventsLeased(ctx, 50*time.Millisecond, 2)
	if err != nil || len(leased) != 2 {
		t.Fatalf("ClaimLeased: %v (%d)", err, len(leased))
	}
	plain, err := store.ClaimPendingEvents(ctx, 10)
	if err != nil || len(plain) != 3 {
		t.Fatalf("ClaimPendingEvents: %v (%d)", err, len(plain))
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 立即重开：租约仍有效（2 条不可领），普通锁定已释放（3 条可领）。
	store2 := openStore(t, dir)
	got, err := store2.ClaimPendingEvents(ctx, 10)
	if err != nil {
		t.Fatalf("Claim after reopen: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("claimed after reopen=%d, want 3 (plain locks released)", len(got))
	}
	if got := store2.PendingCount(); got != 0 {
		t.Fatalf("PendingCount=%d, want 0 (leased still held)", got)
	}
	// 租约到期后：两条租约事件恢复可领取。
	time.Sleep(60 * time.Millisecond)
	got, err = store2.ClaimPendingEvents(ctx, 10)
	if err != nil {
		t.Fatalf("Claim after lease expiry: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("claimed after lease expiry=%d, want 2", len(got))
	}
	store2.Close()
}

// TestHistoryPagination 验证 EventHistoryQuery 分页与游标错误语义。
func TestHistoryPagination(t *testing.T) {
	dir := t.TempDir()
	store := openStore(t, dir)
	defer store.Close()
	engine := txsaga.NewEngine(store)
	req := testRequest("order-page")
	if _, err := engine.Execute(context.Background(), threeStepDef(nil), req); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	// 另一执行的事件不应混入。
	if _, err := engine.Execute(context.Background(), threeStepDef(nil), testRequest("order-other")); err != nil {
		t.Fatalf("Execute other: %v", err)
	}
	ctx := context.Background()
	q := txsaga.EventHistoryQuery{SagaName: "order", BusinessKey: req.BusinessKey, IdempotencyKey: req.IdempotencyKey, Limit: 2}

	var ids []string
	cursor := ""
	for {
		page, err := store.ListEvents(ctx, txsaga.EventHistoryQuery{
			SagaName: q.SagaName, BusinessKey: q.BusinessKey, IdempotencyKey: q.IdempotencyKey,
			AfterID: cursor, Limit: q.Limit,
		})
		if err != nil {
			t.Fatalf("ListEvents: %v", err)
		}
		if len(page.Events) > 2 {
			t.Fatalf("page size=%d, want <=2", len(page.Events))
		}
		for _, r := range page.Events {
			ids = append(ids, r.ID)
		}
		if !page.HasMore {
			if page.NextAfterID != ids[len(ids)-1] {
				t.Fatalf("last NextAfterID=%q, want %q", page.NextAfterID, ids[len(ids)-1])
			}
			break
		}
		cursor = page.NextAfterID
	}
	if len(ids) != 5 {
		t.Fatalf("total events=%d, want 5", len(ids))
	}
	// 空页（游标已是最后一条）：回传游标，不报错。
	empty, err := store.ListEvents(ctx, txsaga.EventHistoryQuery{
		SagaName: q.SagaName, BusinessKey: q.BusinessKey, IdempotencyKey: q.IdempotencyKey, AfterID: ids[len(ids)-1],
	})
	if err != nil || len(empty.Events) != 0 || empty.NextAfterID != ids[len(ids)-1] {
		t.Fatalf("empty page=%+v err=%v", empty, err)
	}
	// 游标不存在 / 属于其它执行：ErrEventCursorNotFound，不返回部分页。
	if _, err := store.ListEvents(ctx, txsaga.EventHistoryQuery{
		SagaName: q.SagaName, BusinessKey: q.BusinessKey, IdempotencyKey: q.IdempotencyKey, AfterID: "evt-999",
	}); !errors.Is(err, txsaga.ErrEventCursorNotFound) {
		t.Fatalf("bad cursor err=%v, want ErrEventCursorNotFound", err)
	}
	otherPage, err := store.ListEvents(ctx, txsaga.EventHistoryQuery{
		SagaName: "order", BusinessKey: "order-other", IdempotencyKey: "idem-1",
	})
	if err != nil || len(otherPage.Events) != 5 {
		t.Fatalf("other exec page=%+v err=%v", otherPage, err)
	}
	if _, err := store.ListEvents(ctx, txsaga.EventHistoryQuery{
		SagaName: q.SagaName, BusinessKey: q.BusinessKey, IdempotencyKey: q.IdempotencyKey, AfterID: otherPage.Events[0].ID,
	}); !errors.Is(err, txsaga.ErrEventCursorNotFound) {
		t.Fatalf("foreign cursor err=%v, want ErrEventCursorNotFound", err)
	}
	// 执行不存在 / Saga 名称不符：ErrExecutionNotFound。
	if _, err := store.ListEvents(ctx, txsaga.EventHistoryQuery{
		SagaName: "nope", BusinessKey: q.BusinessKey, IdempotencyKey: q.IdempotencyKey,
	}); !errors.Is(err, txsaga.ErrExecutionNotFound) {
		t.Fatalf("wrong saga err=%v, want ErrExecutionNotFound", err)
	}
	if _, err := store.ListEvents(ctx, txsaga.EventHistoryQuery{
		SagaName: "order", BusinessKey: "unknown", IdempotencyKey: "idem-1",
	}); !errors.Is(err, txsaga.ErrExecutionNotFound) {
		t.Fatalf("unknown exec err=%v, want ErrExecutionNotFound", err)
	}
}

// TestContextCancellation 验证 ctx 取消返回 context 错误且不影响已提交数据。
func TestContextCancellation(t *testing.T) {
	dir := t.TempDir()
	store := openStore(t, dir)
	defer store.Close()
	engine := txsaga.NewEngine(store)
	req := testRequest("order-ctx")
	if _, err := engine.Execute(context.Background(), threeStepDef(nil), req); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	before := store.TotalEvents()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.LoadSnapshot(ctx, req.BusinessKey, req.IdempotencyKey); !errors.Is(err, context.Canceled) {
		t.Fatalf("LoadSnapshot err=%v, want context.Canceled", err)
	}
	if err := store.Commit(ctx, req.BusinessKey, req.IdempotencyKey, func(tx txsaga.Tx) error {
		tx.State().Status = "tampered"
		return nil
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Commit err=%v, want context.Canceled", err)
	}
	if _, err := store.ClaimPendingEvents(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("Claim err=%v, want context.Canceled", err)
	}
	if _, err := store.ClaimPendingEventsLeased(ctx, time.Minute, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("ClaimLeased err=%v, want context.Canceled", err)
	}
	if _, err := store.ListEvents(ctx, txsaga.EventHistoryQuery{
		SagaName: "order", BusinessKey: req.BusinessKey, IdempotencyKey: req.IdempotencyKey,
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("ListEvents err=%v, want context.Canceled", err)
	}

	// 已提交数据不受影响。
	snap, err := store.LoadSnapshot(context.Background(), req.BusinessKey, req.IdempotencyKey)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if snap.State.Status != txsaga.StatusCompleted {
		t.Fatalf("status=%q, want completed (committed data intact)", snap.State.Status)
	}
	if got := store.TotalEvents(); got != before {
		t.Fatalf("TotalEvents=%d, want %d", got, before)
	}
}

// TestRelayDelivery 验证 Relay 在 FileStore 上的领取/投递/Ack 全流程，
// 以及重开后审计记录保留。
func TestRelayDelivery(t *testing.T) {
	dir := t.TempDir()
	store := openStore(t, dir)
	engine := txsaga.NewEngine(store)
	req := testRequest("order-relay")
	if _, err := engine.Execute(context.Background(), threeStepDef(nil), req); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	var mu sync.Mutex
	var published []txsaga.Event
	relay := txsaga.NewRelay(store, txsaga.PublisherFunc(func(_ context.Context, ev txsaga.Event) error {
		mu.Lock()
		published = append(published, ev)
		mu.Unlock()
		return nil
	}), txsaga.WithBatch(2))

	ctx := context.Background()
	res, err := relay.DeliverOnce(ctx)
	if err != nil || res.Claimed != 2 || res.Delivered != 2 {
		t.Fatalf("DeliverOnce 1: %+v err=%v", res, err)
	}
	res, err = relay.DeliverOnce(ctx)
	if err != nil || res.Claimed != 2 || res.Delivered != 2 {
		t.Fatalf("DeliverOnce 2: %+v err=%v", res, err)
	}
	res, err = relay.DeliverOnce(ctx)
	if err != nil || res.Claimed != 1 || res.Delivered != 1 {
		t.Fatalf("DeliverOnce 3: %+v err=%v", res, err)
	}
	if got := store.PendingCount(); got != 0 {
		t.Fatalf("PendingCount=%d, want 0", got)
	}
	if len(published) != 5 {
		t.Fatalf("published=%d, want 5", len(published))
	}
	for i, ev := range published {
		if ev.Deliveries() != 1 {
			t.Fatalf("published[%d] deliveries=%d, want 1", i, ev.Deliveries())
		}
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 重开：已 Ack 事件的审计记录（含投递计数）仍然可读。
	store2 := openStore(t, dir)
	defer store2.Close()
	page, err := store2.ListEvents(ctx, txsaga.EventHistoryQuery{
		SagaName: "order", BusinessKey: req.BusinessKey, IdempotencyKey: req.IdempotencyKey,
	})
	if err != nil {
		t.Fatalf("ListEvents after reopen: %v", err)
	}
	if len(page.Events) != 5 {
		t.Fatalf("history len=%d, want 5", len(page.Events))
	}
	for i, r := range page.Events {
		if r.Deliveries != 1 || r.LastAttemptAt.IsZero() {
			t.Fatalf("record[%d] deliveries=%d lastAttempt=%v", i, r.Deliveries, r.LastAttemptAt)
		}
	}
}

// TestConcurrentExecute 验证单进程内多个 goroutine 共享同一 FileStore。
func TestConcurrentExecute(t *testing.T) {
	dir := t.TempDir()
	store := openStore(t, dir)
	defer store.Close()
	engine := txsaga.NewEngine(store)
	def := threeStepDef(nil)

	const n = 16
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := txsaga.ExecutionRequest{
				BusinessKey:    fmt.Sprintf("order-%d", i),
				IdempotencyKey: "idem-1",
			}
			res, err := engine.Execute(context.Background(), def, req)
			if err != nil {
				errs <- err
				return
			}
			if res.Status != txsaga.StatusCompleted {
				errs <- fmt.Errorf("%s: status=%q", req.BusinessKey, res.Status)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if got := store.PendingCount(); got != n*5 {
		t.Fatalf("PendingCount=%d, want %d", got, n*5)
	}
	// 重开后全部可查询。
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	store2 := openStore(t, dir)
	defer store2.Close()
	for i := 0; i < n; i++ {
		snap, err := store2.LoadSnapshot(context.Background(), fmt.Sprintf("order-%d", i), "idem-1")
		if err != nil || snap.State == nil || snap.State.Status != txsaga.StatusCompleted {
			t.Fatalf("order-%d: snap=%+v err=%v", i, snap.State, err)
		}
	}
}

// TestGraphModeConcurrentCommits 验证依赖图模式下并发提交的步骤结果
// 在 FileStore 上原子落盘，重开后终态与历史完整。
func TestGraphModeConcurrentCommits(t *testing.T) {
	dir := t.TempDir()
	store := openStore(t, dir)
	engine := txsaga.NewEngine(store)

	var mu sync.Mutex
	started := map[string]bool{}
	action := func(name string) txsaga.ActionFunc {
		return func(context.Context, txsaga.ExecutionView) error {
			mu.Lock()
			started[name] = true
			mu.Unlock()
			return nil
		}
	}
	def := txsaga.Definition{
		Name:    "graph",
		Version: "v1",
		Steps: []txsaga.Step{
			{Name: "a", Action: action("a")},
			{Name: "b", Action: action("b")},
			{Name: "c", Action: action("c"), DependsOn: []string{"a", "b"}},
			{Name: "d", Action: action("d"), DependsOn: []string{"c"}},
		},
	}
	req := testRequest("order-graph")
	res, err := engine.Execute(context.Background(), def, req)
	if err != nil || res.Status != txsaga.StatusCompleted {
		t.Fatalf("Execute: %+v err=%v", res, err)
	}
	if len(started) != 4 {
		t.Fatalf("started=%v, want all 4 steps", started)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	store2 := openStore(t, dir)
	defer store2.Close()
	engine2 := txsaga.NewEngine(store2)
	got, err := engine2.GetResult(context.Background(), "graph", req.BusinessKey, req.IdempotencyKey)
	if err != nil || got.Status != txsaga.StatusCompleted {
		t.Fatalf("GetResult after reopen: %+v err=%v", got, err)
	}
	for _, s := range got.Steps {
		if s.Result != txsaga.StepResultSucceeded {
			t.Fatalf("step %s result=%q, want succeeded", s.Name, s.Result)
		}
	}
	page, err := engine2.ListEvents(context.Background(), txsaga.EventHistoryQuery{
		SagaName: "graph", BusinessKey: req.BusinessKey, IdempotencyKey: req.IdempotencyKey,
	})
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	// started + 4×step_succeeded + execution_completed。
	if len(page.Events) != 6 {
		t.Fatalf("history len=%d, want 6", len(page.Events))
	}
}

// TestRetryAndPermanent 验证有限重试与永久失败在 FileStore 上的提交口径。
func TestRetryAndPermanent(t *testing.T) {
	dir := t.TempDir()
	store := openStore(t, dir)
	defer store.Close()
	engine := txsaga.NewEngine(store)

	var transientCalls, permanentCalls atomic.Int32
	def := txsaga.Definition{
		Name:    "retry",
		Version: "v1",
		Steps: []txsaga.Step{
			{Name: "flaky", Action: func(context.Context, txsaga.ExecutionView) error {
				if transientCalls.Add(1) < 3 {
					return errors.New("try again")
				}
				return nil
			}, ActionRetry: txsaga.RetryPolicy{MaxAttempts: 3}},
			{Name: "hopeless", Action: func(context.Context, txsaga.ExecutionView) error {
				permanentCalls.Add(1)
				return txsaga.Permanent(errors.New("never"))
			}, ActionRetry: txsaga.RetryPolicy{MaxAttempts: 5}},
		},
	}
	req := testRequest("order-retry")
	res, err := engine.Execute(context.Background(), def, req)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Status != txsaga.StatusFailed {
		t.Fatalf("status=%q, want failed", res.Status)
	}
	if transientCalls.Load() != 3 {
		t.Fatalf("transient calls=%d, want 3 (budget exhausted then succeeded)", transientCalls.Load())
	}
	if permanentCalls.Load() != 1 {
		t.Fatalf("permanent calls=%d, want 1 (permanent failure stops retry)", permanentCalls.Load())
	}
	if res.Steps[0].Attempts != 3 || res.Steps[1].Attempts != 1 {
		t.Fatalf("attempts=%d/%d, want 3/1", res.Steps[0].Attempts, res.Steps[1].Attempts)
	}
}

// TestIdempotentRetryAcrossReopen 验证同身份重试在重开后不重复已确认步骤。
func TestIdempotentRetryAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	store := openStore(t, dir)
	engine := txsaga.NewEngine(store)
	var calls atomic.Int32
	def := txsaga.Definition{
		Name:    "order",
		Version: "v1",
		Steps: []txsaga.Step{
			{Name: "only", Action: func(context.Context, txsaga.ExecutionView) error {
				calls.Add(1)
				return nil
			}},
		},
	}
	req := testRequest("order-idem")
	res, err := engine.Execute(context.Background(), def, req)
	if err != nil || res.Status != txsaga.StatusCompleted {
		t.Fatalf("Execute: %+v err=%v", res, err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	store2 := openStore(t, dir)
	defer store2.Close()
	engine2 := txsaga.NewEngine(store2)
	res, err = engine2.Execute(context.Background(), def, req)
	if err != nil || res.Status != txsaga.StatusCompleted {
		t.Fatalf("re-Execute: %+v err=%v", res, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("action calls=%d, want 1 (terminal result replayed)", calls.Load())
	}
	if got := store2.TotalEvents(); got != 3 {
		t.Fatalf("TotalEvents=%d, want 3 (no new events appended)", got)
	}
}
