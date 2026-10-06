// 依赖图模式示例：无依赖关系的步骤并发执行，前置全部确认后才启动下游；
// 正向失败后按成功步骤的确认顺序逆序补偿。
//
// 运行：go run ./examples/graph
package main

import (
	"context"
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
	start := time.Now()
	log := func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		elapsed := time.Since(start).Milliseconds()
		fmt.Printf("%3dms  "+format+"\n", append([]any{elapsed}, args...)...)
	}

	slow := func(name string, d time.Duration) txsaga.ActionFunc {
		return func(_ context.Context, exec txsaga.ExecutionView) error {
			log("启动 %s（已确认: %v）", name, exec.SucceededSteps())
			time.Sleep(d) // 模拟慢下游；无依赖的步骤会在此期间并发推进
			log("完成 %s", name)
			return nil
		}
	}
	comp := func(name string) txsaga.CompensationFunc {
		return func(context.Context, txsaga.ExecutionView) error {
			log("补偿 %s", name)
			return nil
		}
	}

	// reserve 先执行；charge 与 notify 仅依赖 reserve，二者并发；
	// ship 必须等 charge、notify 都确认成功后才启动。
	def := txsaga.Definition{
		Name:    "order-saga",
		Version: "v2",
		Steps: []txsaga.Step{
			{Name: "reserve", Action: slow("reserve", 20*time.Millisecond), Compensate: comp("reserve")},
			{Name: "charge", DependsOn: []string{"reserve"},
				Action: slow("charge", 40*time.Millisecond), Compensate: comp("charge")},
			{Name: "notify", DependsOn: []string{"reserve"},
				Action: slow("notify", 40*time.Millisecond)}, // 无需补偿
			{Name: "ship", DependsOn: []string{"charge", "notify"},
				Action: slow("ship", 10*time.Millisecond), Compensate: comp("ship")},
		},
	}

	result, err := engine.Execute(ctx, def, txsaga.ExecutionRequest{
		BusinessKey:    "ORDER-2002",
		IdempotencyKey: "request-graph-1",
	})
	if err != nil {
		panic(err)
	}

	fmt.Printf("\n最终状态: %s (终态=%v)\n", result.Status, result.Terminal)
	for _, s := range result.Steps { // 结果仍按声明顺序返回
		fmt.Printf("  步骤 %-8s 结果=%-10s 处理次数=%d\n", s.Name, s.Result, s.Attempts)
	}

	events, err := store.ClaimPendingEvents(ctx, 100)
	if err != nil {
		panic(err)
	}
	fmt.Println("事件顺序:")
	for _, ev := range events {
		p := ev.Payload.(txsaga.EventPayload)
		fmt.Printf("  %-20s step=%-8s succeeded=%v\n", ev.Type, p.Step, p.SucceededSteps)
	}
}
