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
- **依赖图并发（可选）**：任一步骤通过 `DependsOn` 声明前置步骤名称后，
  整个定义进入依赖图模式——步骤在全部前置确认成功后才启动，无前置的
  步骤同时启动、并发执行，某一步确认后立即调度因此就绪的后续步骤。
  所有步骤都未声明前置时保持顺序模式，公开行为不变。
- **有限重试（可选）**：每一步可分别为正向动作与补偿动作配置最大总调用次数
  与每次失败后的等待时长（`ActionRetry` / `CompensateRetry`）。未配置时
  仍是一步一次调用、失败即转补偿。重试期间的中间失败不提交状态、不追加
  事件；等待与动作都响应 `context`。
- **永久失败（可判定）**：正向或补偿动作用 `Permanent(err)` 包装返回的错误
  表示业务上已明确不可能成功：即使重试预算尚有剩余也只按实际调用次数计、
  立即结束重试阶段且不再等待，提交口径与预算耗尽一致。`IsPermanent(err)`
  沿错误链判定，`errors.Is` 与 `Unwrap` 语义保留，`Permanent(nil)` 为 nil。
  context 取消仍优先于永久失败。
- **补偿**：正向动作在重试预算内成功则继续推进；预算耗尽仍失败才停止推进
  （依赖图模式下同时停止调度未启动步骤，已启动的兄弟动作跑完并照常提交），
  仅按确认成功顺序的相反方向补偿已确认成功的步骤；未执行到或未确认的
  步骤不补偿。补偿动作同样可配置重试预算；预算耗尽仍报错则执行固定为
  `compensation_failed`，终态结果不再改变。
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

仓库另在 `filestore` 子包提供只依赖标准库的本地目录持久化实现
`filestore.FileStore`：一次 `Commit` 的状态变更与事件整体写入目录下的
单个 JSON 快照（临时文件 + fsync + 原子改名），写入成功后即使进程退出，
重新 `Open` 同一目录仍可继续执行、查询终态并读取完整事件历史。
`FileStore` 同时实现 `Store`、`ClaimLeaseStore` 与 `EventHistoryStore`，
可直接交给 `NewEngine` 与 `NewRelay` 使用；它支持同进程多 goroutine
共享一个实例，但不支持多进程（或同进程多实例）同时写同一目录。

## 公开入口

| 类型/函数 | 说明 |
| --- | --- |
| `Definition` / `Step` | Saga 定义：名称、版本、步骤列表；每步提供幂等 `Action`，可选 `Compensate`、可选 `DependsOn` 前置步骤（依赖图模式），并可分别为动作与补偿配置可选 `RetryPolicy` |
| `RetryPolicy` | 单个动作的有限重试预算：`MaxAttempts` 最大总调用次数（含首次）、`RetryWait` 每次失败后的等待时长；零值按 1 次、0 等待处理 |
| `Permanent(err)` / `IsPermanent(err)` | 永久错误包装与判定：动作返回 `Permanent(err)` 立即结束重试（即使预算尚有剩余），提交口径与预算耗尽一致；保留 `errors.Is`/`Unwrap`，nil 输入返回 nil |
| `ExecutionRequest` | 执行请求：业务键、外部幂等键、透传负载 |
| `Result` / `StepOutcome` | 确定状态、失败原因、业务键、各步处理结果与调用次数 |
| `Store` | 状态存储接口：原子提交状态变更与事件、领取/Ack/Nack 事件 |
| `EventHistoryStore` | 可选的只读历史接口：`ListEvents` 按执行身份分页回看事件链 |
| `ClaimLeaseStore` | 可选的带租约领取接口：租约期内事件不被重领，失联事件到期自动恢复 |
| `ClaimedEvent` | 带租约领取结果：`Event`、`ClaimID`、`ClaimedUntil` |
| `DeadLetterStore` / `DeadLetterLeaseStore` | 可选的有限投递与死信接口：达到投递上限的失败事件转死信，死信可重新入队 |
| `MemoryStore` | 内存状态存储实现，同时实现 `Store`、`ClaimLeaseStore`、`EventHistoryStore` 与死信扩展接口 |
| `filestore.FileStore` | 子包 `filestore` 的本地目录持久化实现：`filestore.Open(dir)` 打开/初始化、`Close()` 释放占用；快照原子落盘，重开目录可续跑、查终态、读历史；同样实现全部 Store 契约与死信扩展接口 |
| `Engine` | 编排引擎，`Execute` 发起/继续执行，`GetResult` 查询结果，`ListEvents` 查询事件历史，`RequeueDeadLetterEvent` 重新入队死信事件 |
| `EventHistoryQuery` / `EventRecord` / `EventHistoryPage` | 历史查询输入（Saga 名称、业务键、幂等键、`AfterID`、`Limit`）、单条事件记录与分页结果 |
| `Publisher` / `Relay` | 调用方实现发送，`Relay` 负责领取、发送与成败回写 |

执行状态：`running`、`compensating` 为中间状态；`completed`、`failed`、
`compensation_failed` 为固定终态，终态结果在后续同身份调用中直接返回。

哨兵错误（以 `errors.Is` 判定）：

- `ErrInvalidDefinition`：非法定义（含负的重试次数/等待时长，以及前置步骤
  未知、自依赖、重复前置、前置关系成环），或缺失业务键/外部幂等键；
- `ErrExecutionNotFound`：查询未知执行身份；
- `ErrDefinitionConflict`：相同业务键使用了不同 Saga 定义；
- `ErrClaimLeaseUnsupported`：`WithClaimLease` 启用了租约，但 Store 未实现 `ClaimLeaseStore`；
- `ErrStaleClaim`：Ack/Nack 携带的 `ClaimID` 已失效（租约到期后被重新领取），当前事件与新租约不会被改动；
- `ErrEventHistoryUnsupported`：Store 未实现 `EventHistoryStore`，不支持事件历史查询；
- `ErrEventCursorNotFound`：历史查询的 `AfterID` 不存在，或不属于给定执行身份；出错时不返回部分页。
- `ErrDeadLetterUnsupported`：`WithMaxDeliveries` 启用了有限投递，或请求死信重新入队，但 Store 未实现 `DeadLetterStore`；
- `ErrEventNotFound`：重新入队的事件 ID 不存在；
- `ErrEventNotDeadLettered`：重新入队的事件不在死信状态（尚可投递、领取中或已投递），事件不发生任何变化。

`filestore` 子包另有自己的哨兵错误（以 `errors.Is` 判定，错误链保留）：

- `filestore.ErrStoreOpen`：目录不可创建、不是目录或不可读写；
- `filestore.ErrStoreLocked`：同一进程已有另一个 `FileStore` 占用该目录；
- `filestore.ErrCorruptStore`：既有快照损坏、截断或无法组成一致快照；此时不会覆盖原数据文件；
- `filestore.ErrUnsupportedPayload`：`Payload` 或事件负载无法被 `encoding/json` 稳定表示（如 chan、func、循环引用、NaN）；本次提交整体丢弃，不留半次提交。

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
// BusinessKey、Deliveries、LastAttemptAt、Status（pending/claimed/
// delivered/dead_lettered）、LastError、DeadLetteredAt
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

## 本地目录持久化（filestore 子包）

`filestore.FileStore` 把执行状态、定义绑定与 Outbox 事件（含已 Ack 事件的
审计副本）保存在给定目录下的单个 JSON 快照中。每次 `Commit` 或投递元数据
变更都在同一临界区内生成整库新快照，经“临时文件写入 → fsync → 原子改名
替换”落盘：一次提交中的状态修改与事件追加要么整体可见、要么整体不可见，
中断写入不会只留下状态或只留下事件。

```go
import "github.com/txsaga/txsaga/filestore"

store, err := filestore.Open("./data/saga") // 目录不存在会自动创建
if err != nil {
    // errors.Is(err, filestore.ErrStoreOpen)     目录不可创建/不可读写
    // errors.Is(err, filestore.ErrStoreLocked)   同进程已有实例占用该目录
    // errors.Is(err, filestore.ErrCorruptStore)  既有快照损坏且未被覆盖
    return err
}
defer store.Close()

engine := txsaga.NewEngine(store) // 与 MemoryStore 完全相同的用法
relay  := txsaga.NewRelay(store, publisher, txsaga.WithClaimLease(time.Minute))
```

写入成功返回后即使进程退出，重新 `filestore.Open` 同一目录即可：继续执行
未完成的执行（同身份幂等、补偿逆序等语义不变）、用 `GetResult` 查询终态、
用 `ListEvents` 读取完整事件历史；租约领取状态、投递计数与最近领取时间
也随快照保留，到期事件在重开后仍可重新领取。

- 支持同一进程内多个 goroutine 共享一个 `FileStore`；不支持多进程或同进程
  多个 `FileStore` 同时写同一目录（构造返回 `filestore.ErrStoreLocked`）。
- `Payload` 与事件负载需能被 `encoding/json` 稳定表示；无法表示时该次
  `Commit` 返回 `filestore.ErrUnsupportedPayload` 且不留半次提交。
  内存中动作仍收到本次 `Execute` 传入的原始 `Payload`（不经过 JSON 转换），
  仅重开目录后按 JSON 语义读回。
- `context` 取消时读写原样返回该 context 错误，已提交数据不受影响。
- 只依赖 Go 标准库；`MemoryStore` 与 `Engine` 的既有行为保持不变。

## 依赖图并发（可选）

步骤可通过 `DependsOn` 声明前置步骤名称；定义中任一步骤声明前置后，
整个定义进入依赖图模式：

```go
def := txsaga.Definition{
    Name: "order-saga", Version: "v2",
    Steps: []txsaga.Step{
        {Name: "reserve", Action: reserve, Compensate: release},
        {Name: "audit",   Action: audit},                          // 与 reserve 并发启动
        {Name: "charge",  Action: charge, Compensate: refund,
            DependsOn: []string{"reserve", "audit"}},              // 两者确认后才启动
        {Name: "ship",    Action: ship,   DependsOn: []string{"charge"}},
    },
}
```

语义约定：

- 步骤只有在**全部前置确认成功后**才可启动；无前置的步骤同时启动、并发
  执行；某一步确认后立即调度因此就绪的后续步骤。所有步骤都未声明前置时
  保持顺序模式，按声明顺序逐个执行，公开行为与此前版本一致。
- 前置名称必须指向定义内已存在的其它步骤：未知前置、自依赖、重复前置或
  前置关系成环时，`ValidateDefinition` 与 `Execute` 都返回
  `ErrInvalidDefinition`，且不创建执行、不调用动作、不追加事件。
  `DependsOn` 参与定义指纹，同业务键下变更前置关系会得到
  `ErrDefinitionConflict`。
- 任一正向动作预算耗尽仍失败：停止调度未启动步骤，已启动的兄弟动作继续
  运行到返回并各自原子提交结果（成功步骤照常追加 `step_succeeded` 事件并
  纳入补偿），随后按**确认成功顺序的逆序**补偿；未执行到或未确认的步骤
  不补偿。顺序模式下确认顺序即声明顺序，补偿顺序与此前版本一致。
- 每个动作仍须遵守幂等契约；`ActionRetry` / `CompensateRetry` 在单步内
  继续生效，中间失败不追加事件。事件负载中的 `SucceededSteps` 快照与
  `ExecutionView.SucceededSteps()` 均按确认成功的先后顺序排列；
  `Result.Steps` 始终按 `Definition.Steps` 声明顺序返回。
- `context` 在动作或等待中取消时：不确认当前动作、不调度新的兄弟步骤，
  已提交状态与既有事件不变；以同一业务键、Saga 定义与外部幂等键再次
  调用即可继续处理未确认部分。

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

### 永久失败

业务动作已明确不可能成功（如参数非法、账户被冻结、前置约束永久不满足）
时，重试只会浪费预算，可返回 `Permanent(err)` 标记：

```go
func charge(ctx context.Context, exec txsaga.ExecutionView) error {
    acc, err := loadAccount(exec.BusinessKey())
    if err != nil {
        return err // 普通错误：仍按 ActionRetry 预算重试
    }
    if acc.Closed {
        return txsaga.Permanent(errAccountClosed) // 永久失败：立即结束重试
    }
    // ...
}
```

- 正向动作返回永久失败时，即使 `MaxAttempts` 尚有剩余也只调用一次、不再
  等待；该步按预算耗尽的同一口径提交失败：`Attempts` 只记实际调用次数，
  `Error` 与执行级 `FailureReason` 保留底层错误文本，随后停止调度未启动
  步骤，已启动的依赖图兄弟动作照常返回并提交，只按已确认成功顺序逆序补偿。
- 补偿动作返回永久失败时同样立即结束补偿重试：第一次调用即把该步记为
  `compensation_failed` 并把执行固定为 `compensation_failed` 终态；
  后续同身份调用直接返回既有终态。
- 永久失败不产生中间失败事件，最终事件序列与预算耗尽失败完全一致。
- `Permanent` 保留错误链：`errors.Is(permanentErr, target)` 与
  `errors.Unwrap` 行为不变，外层再包一层（如 `fmt.Errorf("…: %w", …)`）
  后 `IsPermanent` 仍可判定；`Permanent(nil)` 返回 nil。
- context 在动作执行或等待期间取消仍**优先于**永久失败：不确认当前动作、
  不启动补偿，返回 context 错误且已提交状态不变，同身份重入可继续。

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

传入 `WithMaxDeliveries(n)`（仅正数生效）可启用有限投递与死信：每次领取
使事件累计 `Deliveries` 加一，`Publish` 返回 nil 仍 Ack；返回错误时本轮
投递次数未达上限则 Nack 保留，达到上限则在同一存储操作中原子转为死信并
退出普通领取（`RelayResult.DeadLettered` 计数）。租约到期恢复沿用同一上限：
已用尽的事件不再调用 `Publish`，而是在领取操作中直接隔离。死信保留事件
ID、类型、发生时间、负载、累计 `Deliveries`、最近领取时间、失败原因与进入
死信时间（`filestore` 重开目录后一致）。不配置该选项时，任何 Store 的领取、
租约领取、Ack、Nack 与至少一次重投语义都不变；配置后 Store 需实现
`DeadLetterStore`（启用租约时为 `DeadLetterLeaseStore`，`MemoryStore` 与
`filestore.FileStore` 均已实现），否则 `DeliverOnce`/`Run` 返回
`ErrDeadLetterUnsupported`。

死信事件可通过 `Engine.RequeueDeadLetterEvent(ctx, eventID)` 重新入队：
清除领取锁、恢复待投递并开启新一轮有限计数；累计 `Deliveries` 与历史失败
信息不回退，再次耗尽仍进入死信。重复调用不追加事件、不改变顺序，也不制造
第二个副本；事件不存在返回 `ErrEventNotFound`，尚可投递、领取中或已投递
返回 `ErrEventNotDeadLettered`，context 取消时返回该错误且不改变事件。

## 示例

- `examples/success`：成功执行，展示终态、各步一次处理与事件顺序；
- `examples/failure`：正向失败后的逆序补偿，展示 `failed` 结果与事件；
- `examples/retry`：相同幂等键重试不重复处理，并演示失败事件保留重投；
- `examples/retry-policy`：正向动作预算内重试成功，展示总调用次数、
  失败等待与“中间失败不追加事件”。

```sh
go run ./examples/success
go run ./examples/failure
go run ./examples/retry
go run ./examples/retry-policy
```

## 约定

- 公开行为以 README 与源码为准。
- 后续需求在此基线上增量实现。
