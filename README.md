# Tx Saga

分布式事务 Saga 编排引擎：补偿编排、幂等与 Outbox 投递。

## 范围

本仓库从零开始实现上述方向的可用工具，不依赖外部同类实现。

仅使用 Go 标准库，不引入外部数据库、消息代理或同类 Saga 实现；状态如何
落盘由 `Store` 实现自行决定，本项目不规定磁盘文件格式。

## 状态

第一版可嵌入编排能力已实现：

- Saga 定义、执行请求与确定结果（`Definition` / `Request` / `Result`）；
- 顺序执行正向步骤，一步成功后先原子提交该步状态，再执行下一步；
- 正向步骤失败即停止推进，仅对已确认成功的步骤按相反顺序补偿；
- 终态 `completed` / `failed` / `compensation_failed` 一经形成不再改变；
- 执行身份 = 业务键 + Saga 定义 + 外部幂等键，同身份重试不重复执行已
  确认的正向或补偿步骤，未确认结果的步骤允许重新调用（动作自身幂等）；
- 状态存储接口 `Store`（事务原子保存执行状态与 Outbox 事件）与内存实现
  `MemoryStore`；
- 每次状态变化追加事件（唯一 ID、业务键、类型、发生时间、结构化负载），
  支持领取、发送成功标记、失败保留并递增投递次数后继续领取；
- 公开 `Publisher` 接口与 `DeliverPending` 投递循环（至少一次语义）。

错误约定：

- `ErrInvalidDefinition`：非法定义（无步骤、步骤名为空、动作缺失、步骤
  重名）或缺失业务键/幂等键；
- `ErrDefinitionConflict`：相同业务键绑定了不同 Saga 定义；
- `ErrExecutionNotFound`：未知执行身份。

## 用法

```go
engine := saga.NewEngine(saga.NewMemoryStore())

def := saga.Definition{
    Name: "order",
    Steps: []saga.Step{
        {Name: "reserve", Do: reserve, Compensate: release},
        {Name: "charge",  Do: charge,  Compensate: refund},
    },
}

res, err := engine.Execute(ctx, def, saga.Request{
    BusinessKey:    "order-1001", // 唯一业务键
    IdempotencyKey: "cmd-0001",   // 外部幂等键
})
// res.State: completed / failed / compensation_failed
// res.Steps: 各步状态、确认调用次数、失败原因
```

投递 Outbox 事件：

```go
// 领取待投递事件 -> Publisher 发送 -> 成功标记 / 失败保留并计数
n, err := saga.DeliverPending(ctx, store, publisher, 100, 30*time.Second)
```

完整可运行示例见 [`examples/basic`](examples/basic)，展示成功执行、
正向失败后的逆序补偿、相同幂等键重试三种场景，以及如何从结果与事件中
确定状态、顺序和是否发生重复处理：

```
go run ./examples/basic
go test ./...
```

## 约定

- 公开行为以 README 与源码为准。
- 后续需求在此基线上增量实现。
