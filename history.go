package txsaga

import (
	"context"
	"errors"
	"strings"
	"time"
)

// 事件历史查询相关的哨兵错误，调用方可用 errors.Is 判定。
var (
	// ErrEventHistoryUnsupported 当前 Store 未实现 EventHistoryStore。
	// 既有自定义 Store 无需新增任何方法即可继续使用，只是不支持历史查询。
	ErrEventHistoryUnsupported = errors.New("txsaga: store does not support event history")
	// ErrEventCursorNotFound 分页游标 AfterID 未命中：事件不存在，或不属于
	// 本次查询给定的执行身份。返回该错误时不返回任何记录，也不改变任何状态。
	ErrEventCursorNotFound = errors.New("txsaga: event cursor not found")
)

// defaultEventHistoryLimit 是单页上限非正数时采用的默认页大小。
const defaultEventHistoryLimit = 100

// EventHistoryQuery 是一次只读执行事件历史查询的输入。
//
// 执行身份由 Saga 名称、业务键与外部幂等键共同确定，与 Execute/GetResult
// 保持同一口径。事件严格按发生（追加）顺序分页，Ack、Nack、租约到期与
// 投递次数增加都不会改变顺序。
type EventHistoryQuery struct {
	// SagaName Saga 定义名称，必填；与执行记录的名称不符时返回 ErrExecutionNotFound。
	SagaName string
	// BusinessKey 业务键，必填。
	BusinessKey string
	// IdempotencyKey 外部幂等键，必填。
	IdempotencyKey string
	// AfterID 上一页最后一条事件 ID；为空表示从头查询。
	// 命中后从该事件的下一条开始返回；不属于本执行时返回 ErrEventCursorNotFound。
	AfterID string
	// Limit 单页事件数上限；非正数时按 100 条返回。
	Limit int
}

// EventRecord 是执行事件链中的一条只读审计记录。
//
// 身份、类型、发生时间与结构化负载在追加时确定；投递元数据
// （Deliveries/LastAttemptAt）反映查询当下的最新值。已 Ack 的事件同样
// 保留一份不可变身份字段、可继续审计的副本。记录为快照值，与存储内部
// 结构脱离，调用方修改不影响后续领取、投递与再次查询。
type EventRecord struct {
	// ID 事件唯一 ID，由存储在追加时分配，追加顺序单调。
	ID string
	// Type 事件类型，取值为 Event* 常量。
	Type string
	// OccurredAt 事件发生时间（状态提交时刻）。
	OccurredAt time.Time
	// Payload 结构化负载，即追加时的 EventPayload，可用 encoding/json 序列化。
	Payload any
	// BusinessKey 业务键。
	BusinessKey string
	// Deliveries 截至查询时该事件已进入领取/投递流程的次数（含首次）。
	// 发送失败退回后再次领取会增大；Ack 后保留最后一次计数。
	Deliveries int
	// LastAttemptAt 最近一次领取时间；从未被领取为零值。
	LastAttemptAt time.Time
}

// EventHistoryPage 是一页按追加顺序排列的执行事件。
type EventHistoryPage struct {
	// Events 本页事件，按事件发生及追加顺序升序排列。
	Events []EventRecord
	// NextAfterID 下一页查询应使用的游标：本页最后一条事件 ID。
	// 本页为空时与入参 AfterID 语义一致（从头查询则为空）。
	NextAfterID string
	// HasMore 游标之后是否还有事件。
	HasMore bool
}

// EventHistoryStore 是在 Store 之上可选实现的只读执行事件历史接口。
//
// 持久化 Store 通过实现本接口提供与 MemoryStore 一致的历史结果：
// 查询必须只读（不得推进执行、调用动作、追加事件或改变 Outbox 领取/投递
// 状态），并且每页只读取原子提交后可见的事件——一次 Commit 追加的多条
// 事件要么整体可见、要么整体不可见，跨页顺序始终等于追加顺序。
type EventHistoryStore interface {
	Store

	// ListEvents 按追加顺序返回一次执行的一页事件。
	//
	// 查询语义：
	//   - 执行不存在（或事件链为空但无执行记录）返回 ErrExecutionNotFound；
	//   - sagaName 与执行绑定的 Saga 名称不符返回 ErrExecutionNotFound；
	//   - afterID 非空且未命中（事件不存在或属于其它执行）返回 ErrEventCursorNotFound；
	//   - limit<=0 时按 100 条返回。
	//
	// 出错时不得返回部分结果，也不得改变任何状态与投递元数据。
	// 返回的记录须包含待投递、领取中、发送失败退回以及已 Ack 的全部事件，
	// 已 Ack 事件保留可审计副本。
	ListEvents(ctx context.Context, q EventHistoryQuery) (EventHistoryPage, error)
}

// ListEvents 按执行身份只读回看本次执行的完整事件链中的一页。
//
// 执行身份为 Saga 名称、业务键与外部幂等键，与 GetResult 同口径：
//   - 缺少 Saga 名称、业务键或外部幂等键：ErrInvalidDefinition；
//   - 执行不存在或 Saga 名称不匹配：ErrExecutionNotFound；
//   - AfterID 不属于该执行：ErrEventCursorNotFound；
//   - Store 未实现 EventHistoryStore：ErrEventHistoryUnsupported。
//
// 查询不推进执行、不调用动作、不追加事件，也不改变 Outbox 的领取、退回、
// 投递计数或 Ack 状态；事件顺序固定为追加顺序，分页通过
// EventHistoryPage.NextAfterID 继续，HasMore 报告游标之后是否还有事件。
func (e *Engine) ListEvents(ctx context.Context, q EventHistoryQuery) (EventHistoryPage, error) {
	if strings.TrimSpace(q.SagaName) == "" ||
		strings.TrimSpace(q.BusinessKey) == "" ||
		strings.TrimSpace(q.IdempotencyKey) == "" {
		return EventHistoryPage{}, ErrInvalidDefinition
	}
	if q.Limit <= 0 {
		q.Limit = defaultEventHistoryLimit
	}
	hs, ok := e.store.(EventHistoryStore)
	if !ok {
		return EventHistoryPage{}, ErrEventHistoryUnsupported
	}
	return hs.ListEvents(ctx, q)
}
