// 成功执行示例：三步顺序完成，状态与 Outbox 事件同步推进。
//
// 运行：go run ./examples/success
package main

import (
	"context"
	"fmt"

	"github.com/txsaga/txsaga"
)

func main() {
	ctx := context.Background()
	store := txsaga.NewMemoryStore()
	engine := txsaga.NewEngine(store)

	// 每个正向动作都是幂等的：重复调用不会产生重复业务效果。
	def := txsaga.Definition{
		Name:    "order-saga",
		Version: "v1",
		Steps: []txsaga.Step{
			{
				Name:       "reserve_inventory",
				Action:     act("reserve_inventory"),
				Compensate: comp("release_inventory"),
			},
			{
				Name:       "charge_payment",
				Action:     act("charge_payment"),
				Compensate: comp("refund_payment"),
			},
			{
				Name:       "create_shipping",
				Action:     act("create_shipping"),
				Compensate: nil, // 该步骤无需补偿
			},
		},
	}

	result, err := engine.Execute(ctx, def, txsaga.ExecutionRequest{
		BusinessKey:    "ORDER-1001",
		IdempotencyKey: "request-7a3f",
		Payload:        map[string]int{"amount_cents": 2500},
	})
	if err != nil {
		panic(err)
	}

	fmt.Printf("最终状态: %s (终态=%v)\n", result.Status, result.Terminal)
	for _, s := range result.Steps {
		// Attempts=1 表明每步只处理了一次，没有重复执行。
		fmt.Printf("  步骤 %-20s 结果=%-12s 处理次数=%d\n", s.Name, s.Result, s.Attempts)
	}

	// 状态每次变化都追加了事件，且与状态在同一事务中可见。
	events, err := store.ClaimPendingEvents(ctx, 100)
	if err != nil {
		panic(err)
	}
	fmt.Println("事件顺序:")
	for _, ev := range events {
		p := ev.Payload.(txsaga.EventPayload)
		fmt.Printf("  %-20s id=%s step=%-20s\n", ev.Type, ev.ID, p.Step)
	}
}

func act(name string) txsaga.ActionFunc {
	return func(_ context.Context, exec txsaga.ExecutionView) error {
		fmt.Printf("执行正向动作: %s (业务键=%s)\n", name, exec.BusinessKey())
		return nil
	}
}

func comp(name string) txsaga.CompensationFunc {
	return func(_ context.Context, exec txsaga.ExecutionView) error {
		fmt.Printf("执行补偿动作: %s (业务键=%s)\n", name, exec.BusinessKey())
		return nil
	}
}
