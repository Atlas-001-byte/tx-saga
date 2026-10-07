package filestore

import (
	"context"
	"errors"
	"testing"

	"github.com/txsaga/txsaga"
)

// TestEngine_MaxConcurrencyPersistedAndTerminalAfterReopen 验证：并发上限下的
// 依赖图执行在 FileStore 上照常原子提交状态与事件；重新打开目录后终态结果
// 可读，同身份重入直接返回既有终态、不再调用任何动作。
func TestEngine_MaxConcurrencyPersistedAndTerminalAfterReopen(t *testing.T) {
	dir := storeDir(t)

	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	eng := txsaga.NewEngine(store)

	calls := map[string]int{}
	count := func(name string) txsaga.ActionFunc {
		return func(context.Context, txsaga.ExecutionView) error {
			calls[name]++
			return nil
		}
	}
	def := txsaga.Definition{
		Name: "dag-limit", Version: "v1", MaxConcurrency: 1,
		Steps: []txsaga.Step{
			{Name: "a", Action: count("a")},
			{Name: "b", Action: count("b")},
			{Name: "c", DependsOn: []string{"a", "b"}, Action: count("c")},
		},
	}
	req := txsaga.ExecutionRequest{BusinessKey: "biz-fs-lim", IdempotencyKey: "req-fs-lim"}
	res, err := eng.Execute(context.Background(), def, req)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Status != txsaga.StatusCompleted || !res.Terminal {
		t.Fatalf("status=%s want completed", res.Status)
	}
	if calls["a"] != 1 || calls["b"] != 1 || calls["c"] != 1 {
		t.Fatalf("calls = %v, want each step once", calls)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 重新打开：终态与事件历史完整可读；同身份重入不再调用动作。
	store2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer store2.Close()
	eng2 := txsaga.NewEngine(store2)

	again, err := eng2.Execute(context.Background(), def, req)
	if err != nil {
		t.Fatalf("re-Execute: %v", err)
	}
	if again.Status != txsaga.StatusCompleted || !again.Terminal {
		t.Fatalf("again status=%s want completed", again.Status)
	}
	if calls["a"] != 1 || calls["b"] != 1 || calls["c"] != 1 {
		t.Fatalf("actions re-invoked after terminal: %v", calls)
	}
	for _, so := range again.Steps {
		if so.Result != txsaga.StepResultSucceeded || so.Attempts != 1 {
			t.Fatalf("step %s = %+v", so.Name, so)
		}
	}
}

// TestEngine_MaxConcurrencyNegativeRejected 验证：FileStore 上负数并发上限
// 同样以 ErrInvalidDefinition 拒绝，且不落盘任何状态或事件。
func TestEngine_MaxConcurrencyNegativeRejected(t *testing.T) {
	dir := storeDir(t)
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	eng := txsaga.NewEngine(store)

	def := txsaga.Definition{
		Name: "dag-limit", Version: "v1", MaxConcurrency: -2,
		Steps: []txsaga.Step{
			{Name: "a", Action: func(context.Context, txsaga.ExecutionView) error { return nil }},
			{Name: "b", DependsOn: []string{"a"}, Action: func(context.Context, txsaga.ExecutionView) error { return nil }},
		},
	}
	_, err = eng.Execute(context.Background(), def, txsaga.ExecutionRequest{
		BusinessKey: "biz-fs-bad", IdempotencyKey: "req-fs-bad",
	})
	if !errors.Is(err, txsaga.ErrInvalidDefinition) {
		t.Fatalf("Execute = %v, want ErrInvalidDefinition", err)
	}
	if _, err := eng.GetResult(context.Background(), "dag-limit", "biz-fs-bad", "req-fs-bad"); !errors.Is(err, txsaga.ErrExecutionNotFound) {
		t.Fatalf("GetResult = %v, want ErrExecutionNotFound", err)
	}
}
