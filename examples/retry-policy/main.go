// 正向动作与补偿动作的有限重试示例：
// 每一步可分别用 ActionRetry / CompensateRetry 配置最大总调用次数与
// 每次失败后的等待时长；未配置时仍为一步一次调用、失败即转补偿。
// 中间失败不追加事件，只有状态确认时才原子提交事件。
//
// 运行：go run ./examples/retry-policy
package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/txsaga/txsaga"
)

func main() {
	ctx := context.Background()
	store := txsaga.NewMemoryStore()
	engine := txsaga.NewEngine(store)

	var mu sync.Mutex
	chargeCalls := 0
	def := txsaga.Definition{
		Name:    "order-saga",
		Version: "v1",
		Steps: []txsaga.Step{
			{
				Name:       "reserve_inventory",
				Action:     act("reserve_inventory"),
				Compensate: comp("release_inventory"),
				// 未配置 ActionRetry：只调用一次，保持基线语义。
			},
			{
				Name:       "charge_payment",
				Compensate: comp("refund_payment"),
				// 正向动作前两次返回临时错误，第三次成功；
				// 最大总调用次数 3，每次失败后等待 50ms。
				ActionRetry: txsaga.RetryPolicy{MaxAttempts: 3, RetryWait: 50 * time.Millisecond},
				Action: func(_ context.Context, _ txsaga.ExecutionView) error {
					mu.Lock()
					chargeCalls++
					n := chargeCalls
					mu.Unlock()
					if n < 3 {
						fmt.Printf("正向动作: charge_payment 第 %d 次调用 -> 临时失败\n", n)
						return errors.New("payment gateway temporarily unavailable")
					}
					fmt.Println("正向动作: charge_payment 第 3 次调用 -> 成功")
					return nil
				},
			},
			{
				Name:       "create_shipping",
				Action:     act("create_shipping"),
				Compensate: comp("cancel_shipping"),
			},
		},
	}

	result, err := engine.Execute(ctx, def, txsaga.ExecutionRequest{
		BusinessKey:    "ORDER-1004",
		IdempotencyKey: "request-retry-01",
	})
	if err != nil {
		panic(err)
	}

	fmt.Printf("最终状态: %s (终态=%v)\n", result.Status, result.Terminal)
	for _, s := range result.Steps {
		// charge_payment 的 Attempts 为 3：首次失败到最后成功的总调用次数都计入；
		// 成功后该步 Error 被清空。中间两次失败没有追加任何事件。
		fmt.Printf("  步骤 %-20s 结果=%-10s 正向调用次数=%d 错误=%q\n",
			s.Name, s.Result, s.Attempts, s.Error)
	}

	// 事件与无重试时完全一致：started -> 每步一次 succeeded -> completed。
	events, _ := store.ClaimPendingEvents(ctx, 100)
	fmt.Println("事件顺序（中间失败不产生事件）:")
	for _, ev := range events {
		fmt.Printf("  %s\n", ev.Type)
	}
}

func act(name string) txsaga.ActionFunc {
	return func(_ context.Context, _ txsaga.ExecutionView) error {
		fmt.Println("正向动作:", name)
		return nil
	}
}

func comp(name string) txsaga.CompensationFunc {
	return func(_ context.Context, _ txsaga.ExecutionView) error {
		fmt.Println("补偿动作:", name)
		return nil
	}
}
