# Tx Saga

分布式事务 Saga 编排引擎：补偿编排、幂等与 Outbox 投递。

## 范围

本仓库从零开始实现上述方向的可用工具，不依赖外部同类实现。

当前版本只依赖 Go 标准库，不引入外部数据库、消息代理或同类 Saga 实现，
可直接嵌入应用进程。

## 状态

第一版已实现，覆盖：

- **Saga 编排**：默认按定义顺序执行未完成的正向步骤；一步确认成功后，
  先在同一事务中提交该步完成状态与事件，再执行下一步。步骤也可用
  `Step.DependsOn` 声明前置步骤启用**依赖图模式**：仅当全部前置确认成功
  后该步才启动，无依赖关系的步骤并发执行，状态与事件仍按步独立原子提交。
  任一步骤均未声明依赖时，行为与顺序模式完全一致。
- **有限重试（可选）**：每一步可分别为正向动作与补偿动作配置最大总调用次数
  与每次失败后的等待时长（`ActionRetry` / `CompensateRetry`）。未配置时
  仍是一步一次调用、失败即转补偿。重试期间的中间失败不提交状态、不追加
  事件；等待与动作都响应 `context`。
- **补偿**：正向动作在重试预算内成功则继续推进；预算耗尽仍失败才停止推进，
  仅按相反顺序补偿此前已确认成功的步骤；未执行到的步骤不补偿。补偿动作
  同样可配置重试预算；预算耗尽仍报错则执行固定为 `compensation_failed`，
  终态结果不再改变。
- **执行幂等**：执行身份由业务键、Saga 定义与外部幂等键共同确定。
  同身份重试不重复执行已确认成功的正向步骤或补偿步骤；未确认结果的步骤
  允许被重新调用，动作自身遵守幂等契约。
- **Outbox**：状态存储原子保存执行状态与待投递事件；调用方可领取事件、
  交给 `Publisher` 发送，成功后标记已投递，失败后保留原事件、累计投递次数
  并可继续领取。
- **执行事件历史**：按执行身份（Saga 名称、业务键、外部幂等键）只读回看
  完整事件链，按追加顺序分页；待投递、领取中、发送失败退回以及**已 Ack**
  的事件都可审计，投递次数与最近领取时间随记录一并暴露。

本包不规定磁盘文件格式：持久化由 `Store` 接口承载，仓库提供进程内
`MemoryStore`，调用方可另行实现基于数据库事务的存储。

## 公开入口

| 类型/函数 | 说明 |
| --- | --- |
| `Definition` / `Step` | Saga 定义：名称、版本、有序步骤；每步提供幂等 `Action`，可选 `Compensate`、可选前置依赖 `DependsOn`，并可分别为动作/补偿配置可选 `RetryPolicy` |
| `RetryPolicy` | 单个动作的有限重试预算：`MaxAttempts` 最大总调用次数（含首次）、`RetryWait` 每次失败后的等待时长；零值按 1 次、0 等待处理 |
| `ExecutionRequest` | 执行请求：业务键、外部幂等键、透传负载 |
| `Result` / `StepOutcome` | 确定状态、失败原因、业务键、各步处理结果与调用次数 |
| `Store` | 状态存储接口：原子提交状态变更与事件、领取/Ack/Nack 事件 |
| `EventHistoryStore` | 可选的只读历史接口：`ListEvents` 按执行身份分页回看事件链 |
| `ClaimLeaseStore` | 可选的带租约领取接口：租约期内事件不被重领，失联事件到期自动恢复 |
| `ClaimedEvent` | 带租约领取结果：`Event`、`ClaimID`、`ClaimedUntil` |
| `MemoryStore` | 内存状态存储实现，同时实现 `Store`、`ClaimLeaseStore` 与 `EventHistoryStore` |
| `Engine` | 编排引擎，`Execute` 发起/继续执行，`GetResult` 查询结果，`ListEvents` 查询事件历史 |
| `EventHistoryQuery` / `EventRecord` / `EventHistoryPage` | 历史查询输入（Saga 名称、业务键、幂等键、`AfterID`、`Limit`）、单条事件记录与分页结果 |
| `Publisher` / `Relay` | 调用方实现发送，`Relay` 负责领取、发送与成败回写 |

执行状态：`running`、`compensating` 为中间状态；`completed`、`failed`、
`compensation_failed` 为固定终态，终态结果在后续同身份调用中直接返回。

哨兵错误（以 `errors.Is` 判定）：

- `ErrInvalidDefinition`：非法定义（含负的重试次数/等待时长，或依赖未知、
  自依赖、重复前置、依赖成环），或缺失业务键/外部幂等键；
- `ErrExecutionNotFound`：查询未知执行身份；
- `ErrDefinitionConflict`：相同业务键使用了不同 Saga 定义；
- `ErrClaimLeaseUnsupported`：`WithClaimLease` 启用了租约，但 Store 未实现 `ClaimLeaseStore`；
- `ErrStaleClaim`：Ack/Nack 携带的 `ClaimID` 已失效（租约到期后被重新领取），当前事件与新租约不会被改动；
- `ErrEventHistoryUnsupported`：Store 未实现 `EventHistoryStore`，不支持事件历史查询；
- `ErrEventCursorNotFound`：历史查询的 `AfterID` 不存在，或不属于给定执行身份；出错时不返回部分页。

事件历史按执行身份只读查询，`AfterID` 为空从头开始，`Limit<=0` 按 100 条
返回，页内按事件发生及追加顺序排列，以 `NextAfterID`/`HasMore` 续页。
查询不推进执行、不调用动作、不追加事件，也不改变领取、退回、租约或投递
计数；Ack、Nack、租约到期与投递次数增加都不改变事件顺序，已 Ack 事件
保留可审计副本（投递次数与最近领取时间为其最后一次值）。

```go
page, err := engine.ListEvents(ctx, txsaga.EventHistoryQuery{
    SagaName:       "order-saga",
    BusinessKey:    "ORDER-1001",
    IdempotencyKey: "request-7a3f",
    AfterID:        "", // 上一页的 NextAfterID；首页留空
    Limit:          50, // 非正数按 100
})
// page.Events []txsaga.EventRecord：ID、Type、OccurredAt、Payload、
// BusinessKey、Deliveries、LastAttemptAt
```

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

## 依赖图模式（可选并发）

步骤可以用 `Step.DependsOn` 声明一个或多个前置步骤名。只要定义中任一步骤
声明了依赖，引擎就改按依赖图调度；否则保持上述顺序模式，公开行为不变。

```go
def := txsaga.Definition{
    Name: "order-saga", Version: "v2",
    Steps: []txsaga.Step{
        {Name: "reserve", Action: reserve, Compensate: release},
        // charge 与 notify 都只依赖 reserve：reserve 确认后二者并发执行。
        {Name: "charge", DependsOn: []string{"reserve"}, Action: charge, Compensate: refund},
        {Name: "notify", DependsOn: []string{"reserve"}, Action: notify},
        // ship 必须等 charge、notify 都确认成功后才启动。
        {Name: "ship",   DependsOn: []string{"charge", "notify"}, Action: ship},
    },
}
```

调度与确认语义：

- 步骤只有在**全部前置步骤确认成功**后才可启动；无前置步骤（或前置均已
  确认）的步骤在同一波次内并发启动。下游在整波结果提交后才可能启动，
  因此动作通过 `ExecutionView.SucceededSteps()` 看到的是此前波次已确认的
  步骤。
- 每个动作返回 `nil` 才确认成功；成功状态与事件在**各自独立的 Store 事务**
  中原子提交。动作仍须遵守幂等契约：崩溃/超时后同身份重试只重新进入
  未确认步骤，已确认步骤不会被重复调用。
- 某步预算耗尽返回错误时，记录该步最近错误并**停止调度尚未启动的步骤**；
  同一波次中已启动的兄弟动作都会运行到返回并提交结果：成功的兄弟纳入
  补偿，失败步骤自身不补偿，其依赖者保持 `pending` 且永不启动。
- 任一正向失败后，仅按**成功步骤确认顺序的逆序**补偿；同波次并发确认的
  步骤按其成功事务的提交顺序排列（引擎按声明顺序逐个提交同波次结果，
  因此同波补偿次序即声明次序的逆序）。未配置补偿仍视为成功，补偿预算
  耗尽仍失败时执行固定为 `compensation_failed`。
- `ActionRetry` / `CompensateRetry` 在单步内照常生效，中间失败不追加事件。
- 事件负载中的 `SucceededSteps` 是事件提交时已确认步骤的确认顺序快照；
  后续事件仍严格按 Store 的原子追加顺序排列。
- 依赖未知、自依赖、重复列出同一前置或依赖成环均为非法定义：
  `ValidateDefinition` 与 `Execute` 返回 `ErrInvalidDefinition`，且不创建
  执行、不调用动作、不追加事件。前置集合属于定义指纹的一部分：同业务键
  上以不同依赖图再次执行返回 `ErrDefinitionConflict`。
- `Result.Steps` 始终按 `Definition.Steps` 的声明顺序返回；`GetResult`、
  幂等身份、结果状态、事件类型与 Store 原子提交语义均与顺序模式一致。
- context 在动作或等待中取消时，不确认当前波次的任何动作、不调度新的
  兄弟步骤、不启动补偿，也不改变已提交状态与既有事件；以同一业务键、
  Saga 定义与外部幂等键再次调用即可继续处理未确认部分。

## 有限重试

每个步骤可分别为正向动作与补偿动作配置可选的有限重试：

```go
step := txsaga.Step{
    Name:       "charge",
    Action:     charge,
    Compensate: refund,
    // 正向动作最多总共调用 3 次（含首次），每次失败后等待 100ms 再调用同一动作。
    ActionRetry: txsaga.RetryPolicy{MaxAttempts: 3, RetryWait: 100 * time.Millisecond},
    // 补偿动作最多总共调用 2 次；等待时长为 0，失败后立即再次调用。
    CompensateRetry: txsaga.RetryPolicy{MaxAttempts: 2},
}
```

语义约定：

- `MaxAttempts` 是**包含首次调用在内**的最大总调用次数；零值按 `1` 处理，
  即默认的“一步一次调用”。`RetryWait` 为每次失败后、再次调用前的等待，
  零值表示立即重试。`MaxAttempts < 1`（负数）或 `RetryWait < 0` 属于非法
  定义，`ValidateDefinition` 与 `Execute` 都返回可由 `errors.Is` 判定的
  `ErrInvalidDefinition`。
- 动作每次返回非 nil 错误且仍有剩余次数时，引擎等待 `RetryWait` 后再次
  调用**同一动作**；动作实现仍须遵守跨进程幂等契约。
- 预算内成功：从首次失败到最后成功的总调用次数计入
  `StepOutcome.Attempts`（补偿计入 `CompensationAttempts`），成功后清空
  该步 `Error` 并推进下一步（补偿则继续逆序补偿）。
- 正向动作预算耗尽仍失败：最近一次错误写入该步，按既有规则停止正向推进、
  逆序补偿此前已确认成功的步骤。补偿动作预算耗尽仍失败：执行固定为
  `compensation_failed`，保留失败补偿步骤与错误，后续同身份调用不改变终态。
- **中间失败不追加新事件**：事件仍只在状态确认时随状态原子提交，事件顺序
  与无重试时一致。
- 等待与动作都响应传入的 `context`：等待期间 context 取消，`Execute`
  返回该 context 的错误；动作以 context 错误结束且外层 context 已取消时
  同样返回该取消错误。两种情况下都不确认当前动作、不启动补偿，也不改变
  已经提交的执行状态；以新 context 用同一执行身份再次调用即可继续。

Outbox 投递由调用方提供 `Publisher`：

```go
relay := txsaga.NewRelay(store, txsaga.PublisherFunc(func(ctx context.Context, ev txsaga.Event) error {
    // 发送到实际下游；返回 error 时事件保留并可再次领取
    return nil
}))
if _, err := relay.DeliverOnce(ctx); err != nil { /* ... */ }
```

默认领取一经锁定即不可重领：worker 在 Ack/Nack 前退出会滞留事件。
传入 `WithClaimLease(ttl)`（仅正时长生效）可启用租约：每次领取生成新的
`ClaimID`，租约有效期内事件最多交给一次 `Publish`；worker 在 `Publish`、
Ack 或 Nack 前退出时，事件于租约到期后的下一次 `DeliverOnce`/`Run`
自动恢复重投，维持至少一次投递。领取后、`Publish` 前上下文取消会按当前
`ClaimID` 立即 Nack；`Publish` 进行中取消则按其返回结果处理。Store 需
实现 `ClaimLeaseStore`（`MemoryStore` 已实现），否则返回
`ErrClaimLeaseUnsupported`。

## 示例

- `examples/success`：成功执行，展示终态、各步一次处理与事件顺序；
- `examples/failure`：正向失败后的逆序补偿，展示 `failed` 结果与事件；
- `examples/retry`：相同幂等键重试不重复处理，并演示失败事件保留重投；
- `examples/retry-policy`：正向动作预算内重试成功，展示总调用次数、
  失败等待与“中间失败不追加事件”；
- `examples/graph`：依赖图模式，展示无依赖步骤并发、前置门槛、声明顺序
  结果与确认顺序事件快照。

```sh
go run ./examples/success
go run ./examples/failure
go run ./examples/retry
go run ./examples/retry-policy
go run ./examples/graph
```

## 约定

- 公开行为以 README 与源码为准。
- 后续需求在此基线上增量实现。
