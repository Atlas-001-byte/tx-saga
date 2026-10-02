// 正向失败后的逆序补偿示例：第二步失败，引擎停止推进，
// 仅补偿此前已确认成功的第一步，从未执行的第三步不参与补偿。
//
// 运行：go run ./examples/failure
package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/txsaga/txsaga"
)

func main() {
	ctx := context.Background()
	store := txsaga.NewMemoryStore()
	engine := txsaga.NewEngine(store)

	def := txsaga.Definition{
		Name:    "order-saga",
		Version: "v1",
		Steps: []txsaga.Step{
			{Name: "reserve_inventory", Action: act("reserve_inventory"), Compensate: comp("release_inventory")},
			{Name: "charge_payment", Action: failAction("charge_payment"), Compensate: comp("refund_payment")},
			{Name: "create_shipping", Action: act("create_shipping"), Compensate: comp("cancel_shipping")},
		},
	}

	result, err := engine.Execute(ctx, def, txsaga.ExecutionRequest{
		BusinessKey:    "ORDER-1002",
		IdempotencyKey: "request-9c1e",
	})
	if err != nil {
		panic(err)
	}

	// 确定状态：failed。失败原因、失败步骤与各步结果都可从结果中读出。
	fmt.Printf("最终状态: %s (终态=%v)\n", result.Status, result.Terminal)
	fmt.Printf("失败步骤: %s 原因: %s\n", result.FailedStep, result.FailureReason)
	for _, s := range result.Steps {
		// Attempts 是正向动作调用次数，CompensationAttempts 是补偿动作调用次数；
		// 两者各自为 1 即说明没有发生重复处理。
		fmt.Printf("  步骤 %-20s 结果=%-12s 正向=%d 补偿=%d %s\n",
			s.Name, s.Result, s.Attempts, s.CompensationAttempts, s.Error)
	}

	// 事件完整记录了“前进两步、失败、逆序补偿”的过程。
	events, _ := store.ClaimPendingEvents(ctx, 100)
	fmt.Println("事件顺序:")
	for _, ev := range events {
		p := ev.Payload.(txsaga.EventPayload)
		fmt.Printf("  %-22s step=%-20s reason=%s\n", ev.Type, p.Step, p.Reason)
	}
}

func act(name string) txsaga.ActionFunc {
	return func(_ context.Context, _ txsaga.ExecutionView) error {
		fmt.Println("正向动作:", name)
		return nil
	}
}

func failAction(name string) txsaga.ActionFunc {
	return func(_ context.Context, _ txsaga.ExecutionView) error {
		fmt.Println("正向动作:", name, "-> 返回失败")
		return errors.New("payment gateway rejected the charge")
	}
}

func comp(name string) txsaga.CompensationFunc {
	return func(_ context.Context, _ txsaga.ExecutionView) error {
		fmt.Println("补偿动作:", name)
		return nil
	}
}
