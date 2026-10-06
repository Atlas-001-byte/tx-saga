package txsaga

import (
	"context"
	"errors"
	"strings"
	"time"
)

// 执行事件历史单页上限的缺省值。
const defaultEventHistoryLimit = 100

// 历史查询相关的哨兵错误，调用方可用 errors.Is 判定。
var (
	// ErrEventHistoryUnsupported Store 未实现 EventHistoryStore，无法提供
	// 事件历史查询。该错误不会改变任何执行与投递状态。
	ErrEventHistoryUnsupported = errors.New("txsaga: store does not support event history")
	// ErrEventCursorNotFound afterID 非空但不属于本次查询的执行身份
	// （事件不存在或属于其它执行）。返回该错误时不得给出部分页。
	ErrEventCursorNotFound = errors.New("txsaga: event cursor not found")
)

// EventRecord 是执行事件链中的一条只读审计记录。
//
// 记录按事件发生（追加）顺序排列；该顺序不随 Ack、Nack、租约到期或投递
// 次数增加而改变。已 Ack 的事件仍以相同 ID 保留可审计副本。
type EventRecord struct {
	// ID 事件唯一 ID，与 Outbox 领取到的 Event.ID 相同。
	ID string
	// Type 事件类型，取值为 Event* 常量。
	Type string
	// OccurredAt 事件发生时间（随状态原子提交的时刻）。
	OccurredAt time.Time
	// Payload 追加时写入的结构化负载。
	Payload any
	// BusinessKey 业务键。
	BusinessKey string

	// Deliveries 事件已进入领取/投递流程的次数（含首次）；
	// Ack 事件的副本冻结在确认时的计数。
	Deliveries int
	// LastAttemptAt 最近一次领取时间；从未被领取为零值；
	// Ack 事件的副本冻结在确认时的取值。
	LastAttemptAt time.Time
}

// EventHistoryPage 是一次事件历史查询返回的一页。
type EventHistoryPage struct {
	// Records 本页事件，按事件发生及追加顺序排列。
	Records []EventRecord
	// NextAfterID 下一页应使用的游标（本页最后一条事件的 ID）；
	// HasMore 为 false 时为空。
	NextAfterID string
	// HasMore 是否仍有后续事件。
	HasMore bool
}

// EventHistoryQuery 是事件历史查询的输入。
type EventHistoryQuery struct {
	// SagaName Saga 定义名称，必填；与执行记录的名称不符时返回
	// ErrExecutionNotFound。
	SagaName string
	// BusinessKey 业务键，必填。
	BusinessKey string
	// IdempotencyKey 外部幂等键，必填。
	IdempotencyKey string
	// AfterID 上一页最后一条事件的 ID；为空表示从头查询，
	// 命中后从该事件的下一条开始返回。不属于本执行时返回
	// ErrEventCursorNotFound。
	AfterID string
	// Limit 单页上限；非正数按 100 条返回。
	Limit int
}

// EventHistoryStore 是在 Store 之上可选实现的只读事件历史接口。
//
// 实现必须满足：
//   - 按执行身份 (sagaName, businessKey, idempotencyKey) 返回该执行的全部
//     事件，顺序与追加顺序一致，且不随 Ack、Nack、租约到期或投递次数变化；
//   - 已 Ack 事件必须保留可审计副本（相同 ID 与负载）；
//   - 每页只读取原子提交的事件：一次 Commit 中追加的若干事件要么同时对
//     查询可见，要么都不可见；
//   - 查询只读：不得推进执行、调用动作、追加事件或改变 Outbox 投递元数据；
//   - 执行不存在或 sagaName 不匹配返回 ErrExecutionNotFound；
//     afterID 非空且不属于该执行返回 ErrEventCursorNotFound；
//     任何错误都不得返回部分页。
type EventHistoryStore interface {
	Store

	// ListEvents 返回某次执行按追加顺序排列的一页事件。
	// afterID 为空表示从头查询；limit<=0 按 100 条返回。
	ListEvents(ctx context.Context, q EventHistoryQuery) (EventHistoryPage, error)
}

// ListEvents 只读查询某次执行的 Outbox 事件历史，不推进执行、不调用动作、
// 不追加事件，也不改变领取/投递状态。
//
// 事件按发生及追加顺序排列，覆盖待投递、领取中、发送失败被退回以及
// 已 Ack 的全部事件（Ack 事件保留相同 ID 的可审计副本）。投递元数据
// Deliveries 与 LastAttemptAt 反映最近一次领取/确认时的取值。
//
// 错误口径（错误时返回零值页，不返回部分页，也不改变任何状态）：
//   - 缺少 Saga 名称、业务键或外部幂等键：ErrInvalidDefinition；
//   - 执行不存在或 sagaName 与执行记录不符：ErrExecutionNotFound；
//   - afterID 不属于该执行：ErrEventCursorNotFound；
//   - Store 未实现 EventHistoryStore：ErrEventHistoryUnsupported。
func (e *Engine) ListEvents(ctx context.Context, q EventHistoryQuery) (EventHistoryPage, error) {
	if strings.TrimSpace(q.SagaName) == "" ||
		strings.TrimSpace(q.BusinessKey) == "" ||
		strings.TrimSpace(q.IdempotencyKey) == "" {
		return EventHistoryPage{}, ErrInvalidDefinition
	}
	hs, ok := e.store.(EventHistoryStore)
	if !ok {
		return EventHistoryPage{}, ErrEventHistoryUnsupported
	}
	if q.Limit <= 0 {
		q.Limit = defaultEventHistoryLimit
	}
	return hs.ListEvents(ctx, q)
}
