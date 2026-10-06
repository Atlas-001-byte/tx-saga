# Tx Saga

分布式事务 Saga 编排引擎：补偿编排、幂等与 Outbox 投递。

## 范围

本仓库从零开始实现上述方向的可用工具，不依赖外部同类实现。

当前版本只依赖 Go 标准库，不引入外部数据库、消息代理或同类 Saga 实现，
可直接嵌入应用进程。

## 状态

第一版已实现，覆盖：

- **Saga 编排**：按定义顺序执行未完成的正向步骤；一步确认成功后，
  先在同一事务中提交该步完成状态与事件，再执行下一步。
- **补偿**：正向步骤失败即停止推进，仅按相反顺序补偿此前已确认成功的步骤；
  未执行到的步骤不补偿。任一补偿报错则执行固定为 `compensation_failed`，
  终态结果不再改变。
- **执行幂等**：执行身份由业务键、Saga 定义与外部幂等键共同确定。
  同身份重试不重复执行已确认成功的正向步骤或补偿步骤；未确认结果的步骤
  允许被重新调用，动作自身遵守幂等契约。
- **Outbox**：状态存储原子保存执行状态与待投递事件；调用方可领取事件、
  交给 `Publisher` 发送，成功后标记已投递，失败后保留原事件、累计投递次数
  并可继续领取。
- **领取租约**：实现 `ClaimLeaseStore` 的存储（含 `MemoryStore`）支持带租约
  领取。`Relay` 以 `WithClaimLease` 启用后，事件在租约内不会被重复领取，
  同一有效租约内最多投递一次；worker 失联导致租约到期后，事件在下一次
  `DeliverOnce`/`Run` 自动恢复重投，维持至少一次投递。

本包不规定磁盘文件格式：持久化由 `Store` 接口承载，仓库提供进程内
`MemoryStore`，调用方可另行实现基于数据库事务的存储。

## 公开入口

| 类型/函数 | 说明 |
| --- | --- |
| `Definition` / `Step` | Saga 定义：名称、版本、有序步骤；每步提供幂等 `Action`，可选 `Compensate` |
| `ExecutionRequest` | 执行请求：业务键、外部幂等键、透传负载 |
| `Result` / `StepOutcome` | 确定状态、失败原因、业务键、各步处理结果与调用次数 |
| `Store` | 状态存储接口：原子提交状态变更与事件、领取/Ack/Nack 事件 |
| `ClaimLeaseStore` | 可选的带租约领取接口：租约领取、按 ClaimID 确认/退回，到期自动恢复 |
| `MemoryStore` | 内存状态存储实现（同时实现 `ClaimLeaseStore`） |
| `Engine` | 编排引擎，`Execute` 发起/继续执行，`GetResult` 查询结果 |
| `Publisher` / `Relay` | 调用方实现发送，`Relay` 负责领取、发送与成败回写；`WithClaimLease` 启用租约领取 |

执行状态：`running`、`compensating` 为中间状态；`completed`、`failed`、
`compensation_failed` 为固定终态，终态结果在后续同身份调用中直接返回。

哨兵错误（以 `errors.Is` 判定）：

- `ErrInvalidDefinition`：非法定义，或缺失业务键/外部幂等键；
- `ErrExecutionNotFound`：查询未知执行身份；
- `ErrDefinitionConflict`：相同业务键使用了不同 Saga 定义；
- `ErrClaimLeaseUnsupported`：`Relay` 已启用租约领取，但 Store 未实现 `ClaimLeaseStore`；
- `ErrStaleClaim`：用于 Ack/Nack 的 ClaimID 已不是该事件的当前租约，操作被拒绝。

## 最小用法

```go
store := txsaga.NewMemoryStore()
engine := txsaga.NewEngine(store)

def := txsaga.Definition{
    Name: "order-saga", Version: "v1",
    Steps: []txsaga.Step{
        {Name: "reserve", Action: reserve, Compensate: release},
        {Name: "charge",  Action: charge,  Compensate: refund},
    },
}

result, err := engine.Execute(ctx, def, txsaga.ExecutionRequest{
    BusinessKey:    "ORDER-1001", // 唯一业务键
    IdempotencyKey: "request-7a3f", // 外部幂等键
})
```

Outbox 投递由调用方提供 `Publisher`：

```go
relay := txsaga.NewRelay(store, txsaga.PublisherFunc(func(ctx context.Context, ev txsaga.Event) error {
    // 发送到实际下游；返回 error 时事件保留并可再次领取
    return nil
}))
if _, err := relay.DeliverOnce(ctx); err != nil { /* ... */ }
```

启用租约恢复（要求 Store 实现 `ClaimLeaseStore`，如 `MemoryStore`）：

```go
relay := txsaga.NewRelay(store, publisher, txsaga.WithClaimLease(30*time.Second))
// 领取后 worker 失联的事件在租约到期后自动恢复，由后续 DeliverOnce/Run 重投。
```

## 示例

- `examples/success`：成功执行，展示终态、各步一次处理与事件顺序；
- `examples/failure`：正向失败后的逆序补偿，展示 `failed` 结果与事件；
- `examples/retry`：相同幂等键重试不重复处理，并演示失败事件保留重投。

```sh
go run ./examples/success
go run ./examples/failure
go run ./examples/retry
```

## 约定

- 公开行为以 README 与源码为准。
- 后续需求在此基线上增量实现。
