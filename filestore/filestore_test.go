package filestore

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/txsaga/txsaga"
)

// newTestStore 在临时目录上打开 FileStore，并在测试结束时自动 Close 与
// （默认）清理目录。
func newTestStore(t *testing.T) *FileStore {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func withFakeClock(s *FileStore) *fakeClock {
	clk := &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	s.now = clk.now
	return clk
}

type fakeClock struct {
	t time.Time
}

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func TestOpen_CreatesDirectoryAndSnapshot(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "nested", "store")

	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open on missing dir: %v", err)
	}
	if info, err := os.Stat(filepath.Join(dir, stateFile)); err != nil || info.IsDir() {
		t.Fatalf("initial snapshot not created: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 重开已初始化的目录：历史为空但可正常工作。
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	if got := s2.TotalEvents(); got != 0 {
		t.Fatalf("fresh store TotalEvents=%d want 0", got)
	}
}

func TestOpen_EmptyPathAndFilePath(t *testing.T) {
	if _, err := Open(""); !errors.Is(err, ErrStoreOpen) {
		t.Fatalf("Open(\"\") err=%v want ErrStoreOpen", err)
	}
	path := filepath.Join(t.TempDir(), "a-file")
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); !errors.Is(err, ErrStoreOpen) {
		t.Fatalf("Open(file) err=%v want ErrStoreOpen", err)
	}
}

func TestOpen_UnwritableDirectory(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses permission bits; read-only directory case not meaningful")
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
	// MkdirAll 对已存在目录不报错，但锁文件无法创建。
	if _, err := Open(dir); !errors.Is(err, ErrStoreOpen) {
		t.Fatalf("Open(read-only dir) err=%v want ErrStoreOpen", err)
	}
}

// TestOpen_ConcurrentSameDirectory 验证并发 Open 同一目录：恰好一个成功，
// 其余得到 ErrStoreLocked，且成功者之后仍能正常使用、锁文件未被失败者破坏。
func TestOpen_ConcurrentSameDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	const n = 16
	var wg sync.WaitGroup
	results := make([]error, n)
	stores := make([]*FileStore, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			s, err := Open(dir)
			results[i] = err
			stores[i] = s
		}(i)
	}
	close(start)
	wg.Wait()

	winners := 0
	var winner *FileStore
	for i, err := range results {
		if err == nil {
			winners++
			winner = stores[i]
			continue
		}
		if !errors.Is(err, ErrStoreLocked) {
			t.Fatalf("opener %d err=%v want ErrStoreLocked", i, err)
		}
		if stores[i] != nil {
			t.Fatalf("failed opener %d returned non-nil store", i)
		}
	}
	if winners != 1 || winner == nil {
		t.Fatalf("winners=%d want exactly 1", winners)
	}
	// 成功者可用：提交/领取/重开语义正常。
	if err := winner.Commit(context.Background(), "bk", "ik", func(tx txsaga.Tx) error {
		tx.Create(&txsaga.ExecutionState{SagaName: "s", Fingerprint: "fp",
			Status: txsaga.StatusCompleted, Steps: []txsaga.StepState{{Name: "x", Status: txsaga.StepResultSucceeded}}})
		return nil
	}); err != nil {
		t.Fatalf("winner commit: %v", err)
	}
	if err := winner.Close(); err != nil {
		t.Fatalf("winner close: %v", err)
	}
	// 成功者 Close 后，目录立即可被新实例打开且数据完好。
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen after winner close: %v", err)
	}
	defer s2.Close()
	snap, _ := s2.LoadSnapshot(context.Background(), "bk", "ik")
	if snap.State == nil || snap.State.Status != txsaga.StatusCompleted {
		t.Fatalf("winner data lost: %+v", snap.State)
	}
}

func TestOpen_DoubleInstanceSameDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	s1, err := Open(dir)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	defer s1.Close()

	s2, err := Open(dir)
	if !errors.Is(err, ErrStoreLocked) {
		t.Fatalf("second Open err=%v want ErrStoreLocked", err)
	}
	if s2 != nil {
		t.Fatal("second Open returned a non-nil store")
	}

	// Close 释放占用后可再次打开。
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	s3, err := Open(dir)
	if err != nil {
		t.Fatalf("Open after Close: %v", err)
	}
	s3.Close()

	// 重复 Close 返回 nil 且不影响后续打开。
	if err := s1.Close(); err != nil {
		t.Fatalf("double Close: %v", err)
	}
}

func TestOpen_StaleLockFromUncleanExit(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	s1, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	// 模拟进程崩溃：不 Close，直接以新实例接管（注册表随进程存活，
	// 这里通过移除注册项模拟旧进程已消失、锁文件残留）。
	openMu.Lock()
	delete(openStores, s1.dir)
	openMu.Unlock()

	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("take over stale lock: %v", err)
	}
	defer s2.Close()
}

// corruptCases 覆盖各类无法组成一致快照的磁盘内容。
func TestOpen_CorruptSnapshots(t *testing.T) {
	cases := map[string][]byte{
		"empty":        []byte(""),
		"garbage":      []byte("{this is not json"),
		"truncated":    []byte(`{"format":"txsaga-filestore-v1","version":1,"events":[`),
		"bad format":   []byte(`{"format":"something-else","version":1}`),
		"bad version":  []byte(`{"format":"txsaga-filestore-v1","version":99}`),
		"bad event id": []byte(`{"format":"txsaga-filestore-v1","version":1,"events":[{"id":"nope","business_key":"b","type":"t","idempotency_key":"i","occurred_at":"2026-01-01T00:00:00Z","payload":{}}]}`),
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "store")
			if err := os.MkdirAll(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, stateFile)
			if err := os.WriteFile(path, content, 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := Open(dir)
			if !errors.Is(err, ErrCorruptStore) {
				t.Fatalf("Open err=%v want ErrCorruptStore", err)
			}
			// 损坏时不得覆盖原文件。
			got, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if name == "empty" {
				if len(got) != 0 {
					t.Fatalf("empty snapshot overwritten: %q", got)
				}
			} else if !reflect.DeepEqual(got, content) {
				t.Fatalf("corrupt file modified by Open")
			}
			// 未留下锁文件之外的半成品（锁在失败时已释放）。
			if _, err := os.Stat(filepath.Join(dir, stateTmpFile)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("temp file left behind on corrupt Open: %v", err)
			}
		})
	}
}

func TestOpen_RemovesStaleTempFile(t *testing.T) {
	s := newTestStore(t)
	dir := s.dir
	// 制造一次提交，保证 state.json 是有效快照。
	if err := s.Commit(context.Background(), "bk", "ik", func(tx txsaga.Tx) error {
		tx.AppendEvent(txsaga.EventExecutionStarted, txsaga.EventPayload{SagaName: "s", IdempotencyKey: "ik"})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, stateTmpFile), []byte("junk"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen with stale temp: %v", err)
	}
	defer s2.Close()
	if _, err := os.Stat(filepath.Join(dir, stateTmpFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale temp not removed: %v", err)
	}
	if s2.TotalEvents() != 1 {
		t.Fatalf("events lost after cleaning temp: %d", s2.TotalEvents())
	}
}

func TestUnsupportedPayload_CommitLeavesNothing(t *testing.T) {
	s := newTestStore(t)

	cases := map[string]any{
		"channel": make(chan int),
		"func":    func() {},
		"nan":     map[string]any{"x": math.NaN()},
		"cycle":   newCyclicMap(),
	}
	for name, bad := range cases {
		t.Run(name, func(t *testing.T) {
			err := s.Commit(context.Background(), "bk-"+name, "ik", func(tx txsaga.Tx) error {
				tx.Create(&txsaga.ExecutionState{
					SagaName: "s", Fingerprint: "fp", Status: txsaga.StatusRunning,
					Steps:   []txsaga.StepState{{Name: "step"}},
					Payload: bad,
				})
				tx.AppendEvent(txsaga.EventExecutionStarted, txsaga.EventPayload{
					SagaName: "s", IdempotencyKey: "ik",
				})
				return nil
			})
			if !errors.Is(err, ErrUnsupportedPayload) {
				t.Fatalf("Commit err=%v want ErrUnsupportedPayload", err)
			}
			snap, err := s.LoadSnapshot(context.Background(), "bk-"+name, "ik")
			if err != nil {
				t.Fatal(err)
			}
			if snap.State != nil || snap.Bound {
				t.Fatalf("half commit visible after unsupported payload: %+v", snap)
			}
			if s.TotalEvents() != 0 || s.PendingCount() != 0 {
				t.Fatalf("events leaked after unsupported payload: total=%d pending=%d",
					s.TotalEvents(), s.PendingCount())
			}
		})
	}

	// 失败后存储仍可正常提交，且重开目录不报错（文件仍是上一份一致快照）。
	if err := s.Commit(context.Background(), "bk-ok", "ik", func(tx txsaga.Tx) error {
		tx.Create(&txsaga.ExecutionState{
			SagaName: "s", Fingerprint: "fp", Status: txsaga.StatusCompleted,
			Steps: []txsaga.StepState{{Name: "step", Status: txsaga.StepResultSucceeded}},
		})
		return nil
	}); err != nil {
		t.Fatalf("commit after failures: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(s.dir)
	if err != nil {
		t.Fatalf("reopen after unsupported payload: %v", err)
	}
	defer s2.Close()
	snap, _ := s2.LoadSnapshot(context.Background(), "bk-ok", "ik")
	if snap.State == nil || snap.State.Status != txsaga.StatusCompleted {
		t.Fatalf("good commit lost: %+v", snap.State)
	}
}

func newCyclicMap() map[string]any {
	m := map[string]any{}
	m["self"] = m
	return m
}

func TestContext_CancellationLeavesCommittedData(t *testing.T) {
	s := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())

	// 先成功提交一条事件。
	if err := s.Commit(ctx, "bk", "ik", func(tx txsaga.Tx) error {
		tx.AppendEvent(txsaga.EventExecutionStarted, txsaga.EventPayload{SagaName: "s", IdempotencyKey: "ik"})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	events, err := s.ClaimPendingEvents(ctx, 8)
	if err != nil || len(events) != 1 {
		t.Fatalf("claim: %v %d", err, len(events))
	}

	cancel()

	// 已取消的 context：所有读写原样返回 context 错误，且不改状态。
	if err := s.Commit(ctx, "bk2", "ik", func(tx txsaga.Tx) error {
		tx.Create(&txsaga.ExecutionState{SagaName: "s"})
		tx.AppendEvent("x", txsaga.EventPayload{})
		return nil
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Commit err=%v want context.Canceled", err)
	}
	if _, err := s.LoadSnapshot(ctx, "bk", "ik"); !errors.Is(err, context.Canceled) {
		t.Fatalf("LoadSnapshot err=%v want context.Canceled", err)
	}
	if _, err := s.ClaimPendingEvents(ctx, 8); !errors.Is(err, context.Canceled) {
		t.Fatalf("Claim err=%v want context.Canceled", err)
	}
	if _, err := s.ClaimPendingEventsLeased(ctx, time.Minute, 8); !errors.Is(err, context.Canceled) {
		t.Fatalf("ClaimLeased err=%v want context.Canceled", err)
	}
	if err := s.AckEvent(ctx, events[0].ID); !errors.Is(err, context.Canceled) {
		t.Fatalf("Ack err=%v want context.Canceled", err)
	}

	// 用未取消 context 观察：已提交事件仍在，投递计数未被取消的领取改动，
	// 取消路径上的 Create/AppendEvent 也未留下任何痕迹。
	fresh := context.Background()
	if s.TotalEvents() != 1 || s.PendingCount() != 0 {
		t.Fatalf("canceled calls changed outbox: total=%d pending=%d", s.TotalEvents(), s.PendingCount())
	}
	if got := events[0].Deliveries(); got != 1 {
		t.Fatalf("deliveries changed by canceled claim: %d", got)
	}
	if err := s.AckEvent(fresh, events[0].ID); err != nil {
		t.Fatalf("ack after cancellation: %v", err)
	}
	snap, err := s.LoadSnapshot(fresh, "bk2", "ik")
	if err != nil {
		t.Fatal(err)
	}
	if snap.State != nil || snap.Bound {
		t.Fatal("canceled Commit became visible")
	}
}

func TestOperationsAfterClose(t *testing.T) {
	s := newTestStore(t)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.LoadSnapshot(context.Background(), "b", "i"); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("LoadSnapshot after Close: %v", err)
	}
	if err := s.Commit(context.Background(), "b", "i", func(tx txsaga.Tx) error { return nil }); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("Commit after Close: %v", err)
	}
}

// TestPayload_RoundTrip 验证 ExecutionState.Payload 经 JSON 落盘重开后
// 语义不变（按 JSON 类型口径：对象为 map、数字为 float64）。
func TestPayload_RoundTrip(t *testing.T) {
	type nested struct {
		OrderID string   `json:"order_id"`
		Amount  float64  `json:"amount"`
		Tags    []string `json:"tags"`
	}
	original := map[string]any{
		"obj":  nested{OrderID: "O-1", Amount: 42.5, Tags: []string{"a", "b"}},
		"num":  7,
		"str":  "hello",
		"bool": true,
		"nil":  nil,
		"arr":  []any{float64(1), "two"},
	}

	s := newTestStore(t)
	err := s.Commit(context.Background(), "bk", "ik", func(tx txsaga.Tx) error {
		tx.Create(&txsaga.ExecutionState{
			SagaName: "s", Fingerprint: "fp", Status: txsaga.StatusRunning,
			Steps:   []txsaga.StepState{{Name: "step"}},
			Payload: original,
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// 内存中的负载保持本次 Execute/Commit 传入的原始 Go 值（与 MemoryStore
	// 的 clone 口径一致）；JSON 往返只发生在重开之后。
	got, _ := s.LoadSnapshot(context.Background(), "bk", "ik")
	if !reflect.DeepEqual(got.State.Payload, original) {
		t.Fatalf("in-memory payload mismatch\ngot:  %#v\nwant: %#v", got.State.Payload, original)
	}

	want := jsonRoundTrip(t, original)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(s.dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	got2, _ := s2.LoadSnapshot(context.Background(), "bk", "ik")
	if !reflect.DeepEqual(got2.State.Payload, want) {
		t.Fatalf("reloaded payload mismatch\ngot:  %#v\nwant: %#v", got2.State.Payload, want)
	}
}

func jsonRoundTrip(t *testing.T, v any) any {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var out any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return out
}
