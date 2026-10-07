package txsaga

import (
	"context"
	"errors"
	"strings"
)

// 死信相关的哨兵错误，调用方可用 errors.Is 判定。
var (
	// ErrDeadLetterUnsupported Relay 配置了投递上限（WithMaxDeliveries），
	// 或调用方请求重新入队死信事件，但 Store 未实现 DeadLetterStore。
	// 既有自定义 Store 无需新增任何方法即可继续使用，只是不支持死信语义。
	ErrDeadLetterUnsupported = errors.New("txsaga: store does not support dead letters")
	// ErrEventNotFound 重新入队时给定的事件 ID 不存在（从未追加，或不属于
	// 本存储的任何执行）。返回该错误时不改变任何状态。
	ErrEventNotFound = errors.New("txsaga: event not found")
	// ErrEventNotDeadLettered 重新入队时事件当前不在死信状态：仍可投递、
	// 正在领取中或已投递（Ack）。返回该错误时不改变任何状态。
	ErrEventNotDeadLettered = errors.New("txsaga: event is not dead-lettered")
)

// 事件投递状态，用于 EventRecord.Status。
const (
	// EventStatusPending 事件待投递（含发送失败被退回、租约已到期可恢复）。
	EventStatusPending = "pending"
	// EventStatusClaimed 事件正在被领取（普通领取锁定中或租约仍有效）。
	EventStatusClaimed = "claimed"
	// EventStatusDelivered 事件已投递成功并 Ack；审计副本永久保留。
	EventStatusDelivered = "delivered"
	// EventStatusDeadLettered 事件已达投递上限被隔离为死信，退出普通领取，
	// 只能通过 RequeueDeadLetter 重新入队。
	EventStatusDeadLettered = "dead_lettered"
)

// DeadLetterStore 是在 Store 之上可选实现的死信接口。
//
// Relay 通过 WithMaxDeliveries 配置投递上限后，要求 Store 实现本接口：
// 失败退回与上限判定在同一个存储操作中完成，达到上限的事件转为死信并
// 退出普通领取；未实现的自定义 Store 在 DeliverOnce/Run 时得到
// ErrDeadLetterUnsupported，不配置上限时一切行为与既有语义一致。
type DeadLetterStore interface {
	Store

	// FailEvent 记录一次投递失败并退回事件：事件当前轮次的领取次数
	// （RoundDeliveries）未达 maxDeliveries 时按 Nack 语义退回（普通领取
	// 解除锁定、租约领取解除租约），允许继续领取；达到上限时在同一存储
	// 操作中将事件转为死信（记录失败原因与进入死信时间）并退出普通领取，
	// 返回 deadLettered=true。maxDeliveries<=0 时只退回、永不转死信。
	//
	// claimID 非空时按租约口径校验：不是该事件当前有效领取时返回
	// ErrStaleClaim，且不改动事件与新租约。claimID 为空时按普通领取口径
	// 处理，未在领取中的事件幂等退回（不改动、不转死信）。
	FailEvent(ctx context.Context, eventID, claimID, reason string, maxDeliveries int) (deadLettered bool, err error)

	// RequeueDeadLetter 把处于死信状态的事件重新入队：清除领取锁、回到
	// 待投递状态并开启新一轮有限重投（本轮次数从零重新计数）。累计
	// Deliveries、最近领取时间与历史失败信息不回退；再次耗尽本轮次数后
	// 仍进入死信。重复调用（事件已不在死信状态）返回
	// ErrEventNotDeadLettered，不追加事件、不改变顺序、不制造第二个副本。
	//
	// 事件不存在返回 ErrEventNotFound；尚可投递、领取中或已投递返回
	// ErrEventNotDeadLettered；ctx 取消时原样返回其错误且不改变事件。
	// 本操作与并发的领取、Ack、Nack 原子互斥：同一事件任一时刻只有一个
	// 当前领取或一个待投递状态。
	RequeueDeadLetter(ctx context.Context, eventID string) error
}

// RequeueDeadLetter 把处于死信状态的 Outbox 事件重新入队，开启新一轮
// 有限重投。只作用于死信事件：
//   - 事件 ID 为空或不存在：ErrEventNotFound；
//   - 事件尚可投递、领取中或已投递：ErrEventNotDeadLettered；
//   - Store 未实现 DeadLetterStore：ErrDeadLetterUnsupported；
//   - ctx 已取消：原样返回其错误，事件不变。
//
// 成功调用清除领取锁并回到待投递状态：累计 Deliveries 与历史失败信息
// 不回退，新轮次的有限次数从零重新计数，再次耗尽仍进入死信。重复调用
// 不追加事件、不改变顺序、不制造第二个副本。
func (e *Engine) RequeueDeadLetter(ctx context.Context, eventID string) error {
	if strings.TrimSpace(eventID) == "" {
		return ErrEventNotFound
	}
	ds, ok := e.store.(DeadLetterStore)
	if !ok {
		return ErrDeadLetterUnsupported
	}
	return ds.RequeueDeadLetter(ctx, eventID)
}
