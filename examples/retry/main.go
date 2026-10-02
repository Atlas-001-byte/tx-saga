// 相同幂等键重试 + Outbox 投递示例：
// 同一执行身份重复提交不会再次执行已确认成功的步骤；
// 事件由调用方实现的 Publisher 发送，失败事件保留并按投递次数继续领取。
//
// 运行：go run ./examples/retry
package main

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/txsaga/txsaga"
)

func main() {
	ctx := context.Background()
	store := txsaga.NewMemoryStore()
	engine := txsaga.NewEngine(store)

	var mu sync.Mutex
	actionCalls := map[string]int{}
	counting := func(name string) txsaga.ActionFunc {
		return func(context.Context, txsaga.ExecutionView) error {
			mu.Lock()
			actionCalls[name]++
			mu.Unlock()
			return nil
		}
	}
	def := txsaga.Definition{
		Name:    "order-saga",
		Version: "v1",
		Steps: []txsaga.Step{
			{Name: "reserve", Action: counting("reserve"), Compensate: func(context.Context, txsaga.ExecutionView) error { return nil }},
			{Name: "charge", Action: counting("charge"), Compensate: func(context.Context, txsaga.ExecutionView) error { return nil }},
		},
	}
	req := txsaga.ExecutionRequest{BusinessKey: "ORDER-1003", IdempotencyKey: "request-dedup-01"}

	first, err := engine.Execute(ctx, def, req)
	if err != nil {
		panic(err)
	}
	// 网络超时后客户端用完全相同的业务键与幂等键重试。
	second, err := engine.Execute(ctx, def, req)
	if err != nil {
		panic(err)
	}

	fmt.Printf("首次状态=%s 重试状态=%s 终态=%v\n", first.Status, second.Status, second.Terminal)
	mu.Lock()
	for name, n := range actionCalls {
		// 每个动作的调用次数都恰好为 1：重试没有重复处理。
		fmt.Printf("正向动作 %s 实际调用次数=%d\n", name, n)
	}
	mu.Unlock()
	for _, s := range second.Steps {
		fmt.Printf("  步骤 %-8s 结果=%-10s 处理次数=%d\n", s.Name, s.Result, s.Attempts)
	}

	// 由调用方提供 Publisher；模拟第一个事件首次发送失败、重试成功。
	failedOnce := map[string]bool{}
	publisher := txsaga.PublisherFunc(func(_ context.Context, ev txsaga.Event) error {
		if ev.Type == txsaga.EventExecutionStarted && !failedOnce[ev.ID] {
			failedOnce[ev.ID] = true
			fmt.Printf("投递事件 %s 失败（第 %d 次尝试），事件保留\n", ev.Type, ev.Deliveries())
			return errors.New("broker unavailable")
		}
		fmt.Printf("投递事件 %-18s 成功（第 %d 次尝试），标记已投递\n", ev.Type, ev.Deliveries())
		return nil
	})
	relay := txsaga.NewRelay(store, publisher)

	// 第一轮：started 事件失败被保留，其余事件成功。
	if _, err := relay.DeliverOnce(ctx); err != nil {
		panic(err)
	}
	// 第二轮：重新领取此前失败的事件，此时发送成功。
	if _, err := relay.DeliverOnce(ctx); err != nil {
		panic(err)
	}
	fmt.Printf("剩余待投递事件数: %d\n", store.PendingCount())
}
