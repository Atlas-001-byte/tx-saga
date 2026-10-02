// 最小公开示例：演示一次成功执行、正向失败后的逆序补偿，以及相同幂等键
// 重试时终态结果与事件保持不变、动作不被重复调用。
// 运行：go run ./examples/basic
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	saga "txsaga"
)

// recorder 同时充当动作日志与 Publisher，便于从外部观察调用与投递。
type recorder struct {
	calls     []string
	published []saga.OutboxEvent
}

func (r *recorder) do(name string) saga.Action {
	return func(context.Context) error {
		r.calls = append(r.calls, "do:"+name)
		return nil
	}
}

func (r *recorder) failing(name string, cause error) saga.Action {
	return func(context.Context) error {
		r.calls = append(r.calls, "do:"+name)
		return cause
	}
}

func (r *recorder) compensate(name string) saga.Compensation {
	return func(context.Context) error {
		r.calls = append(r.calls, "compensate:"+name)
		return nil
	}
}

func (r *recorder) Publish(_ context.Context, ev saga.OutboxEvent) error {
	r.published = append(r.published, ev)
	return nil
}

func orderDef(r *recorder) saga.Definition {
	return saga.Definition{
		Name: "order",
		Steps: []saga.Step{
			{Name: "reserve-inventory", Do: r.do("reserve-inventory"), Compensate: r.compensate("reserve-inventory")},
			{Name: "charge-payment", Do: r.do("charge-payment"), Compensate: r.compensate("charge-payment")},
			{Name: "create-shipment", Do: r.do("create-shipment"), Compensate: r.compensate("create-shipment")},
		},
	}
}

func printResult(label string, res saga.Result) {
	fmt.Printf("== %s ==\n", label)
	fmt.Printf("state=%s businessKey=%s failure=%q\n", res.State, res.BusinessKey, res.FailureReason)
	for _, s := range res.Steps {
		fmt.Printf("  step=%-18s state=%-20s doAttempts=%d compAttempts=%d\n",
			s.Name, s.State, s.Attempts, s.CompensationAttempts)
	}
}

func printEvents(label string, events []saga.OutboxEvent) {
	fmt.Printf("-- %s: %d events, in order --\n", label, len(events))
	for _, ev := range events {
		fmt.Printf("  %-30s %s\n", ev.Type, ev.ID)
	}
}

// drain 领取并投递全部待发送事件，返回投递条数。
func drain(ctx context.Context, store *saga.MemoryStore, pub *recorder) int {
	total := 0
	for {
		n, err := saga.DeliverPending(ctx, store, pub, 16, 30*time.Second)
		if err != nil {
			fmt.Fprintln(os.Stderr, "deliver:", err)
			os.Exit(1)
		}
		if n == 0 {
			return total
		}
		total += n
	}
}

func main() {
	ctx := context.Background()

	// ---------- 场景 1：全部成功 ----------
	rec := &recorder{}
	store := saga.NewMemoryStore()
	engine := saga.NewEngine(store)

	res, err := engine.Execute(ctx, orderDef(rec), saga.Request{
		BusinessKey:    "order-1001",
		IdempotencyKey: "cmd-0001",
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "execute:", err)
		os.Exit(1)
	}
	printResult("success", res)
	fmt.Println("action order:", rec.calls)
	pub := &recorder{}
	drain(ctx, store, pub)
	printEvents("outbox after success", pub.published)
	fmt.Println()

	// ---------- 场景 2：末步失败，仅逆序补偿此前确认成功的两步 ----------
	rec2 := &recorder{}
	store2 := saga.NewMemoryStore()
	engine2 := saga.NewEngine(store2)
	def2 := orderDef(rec2)
	def2.Steps[2].Do = rec2.failing("create-shipment", errors.New("warehouse rejected shipment"))

	res2, err := engine2.Execute(ctx, def2, saga.Request{
		BusinessKey:    "order-1002",
		IdempotencyKey: "cmd-0002",
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "execute:", err)
		os.Exit(1)
	}
	printResult("forward failure + reverse compensation", res2)
	fmt.Println("action order:", rec2.calls)
	pub2 := &recorder{}
	drain(ctx, store2, pub2)
	printEvents("outbox after failure", pub2.published)
	fmt.Println()

	// ---------- 场景 3：相同幂等键重试 ----------
	// 首次执行在第二步确认失败：reserve-inventory 已补偿，执行进入 failed
	// 终态。以相同业务键 + 相同幂等键 + 相同定义再次调用：终态结果原样返回，
	// 不新增动作调用，也不产生新事件。
	rec3 := &recorder{}
	store3 := saga.NewMemoryStore()
	engine3 := saga.NewEngine(store3)
	def3 := orderDef(rec3)
	def3.Steps[1].Do = rec3.failing("charge-payment", errors.New("payment declined"))
	req := saga.Request{BusinessKey: "order-1003", IdempotencyKey: "cmd-0003"}

	first, err := engine3.Execute(ctx, def3, req)
	if err != nil {
		fmt.Fprintln(os.Stderr, "execute:", err)
		os.Exit(1)
	}
	pub3 := &recorder{}
	drain(ctx, store3, pub3)
	eventsAfterFirst := len(pub3.published)

	second, err := engine3.Execute(ctx, def3, req) // 同身份重试
	if err != nil {
		fmt.Fprintln(os.Stderr, "retry:", err)
		os.Exit(1)
	}
	eventsAfterRetry := drain(ctx, store3, pub3) + eventsAfterFirst

	printResult("retry with same idempotency key", second)
	fmt.Printf("first state=%s  retry state=%s  identical=%v\n",
		first.State, second.State, first.State == second.State && first.FailureReason == second.FailureReason)
	fmt.Println("action order across both calls:", rec3.calls)
	fmt.Printf("events: %d after first run, %d after retry => retry produced %d new events\n",
		eventsAfterFirst, eventsAfterRetry, eventsAfterRetry-eventsAfterFirst)
	fmt.Println("=> 已确认成功的正向步骤与已确认的补偿步骤均未重复执行")
}
