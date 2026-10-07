package txsaga

import (
	"context"
	"errors"
	"time"
)

// Publisher 由调用方实现，负责把一条 Outbox 事件发送到实际的下游
// （消息代理、HTTP 回调、其它进程内通道等）。本包不提供任何具体中间件适配。
//
// 实现应当满足至少一次投递语义：返回 error 时事件会被保留并可再次领取，
// 因此下游消费侧需要自身幂等。返回 nil 即视为投递成功，事件被标记已投递。
type Publisher interface {
	// Publish 发送单个事件。event.Payload 为结构化负载，可用 encoding/json 序列化。
	Publish(ctx context.Context, event Event) error
}

// PublisherFunc 让普通函数满足 Publisher 接口。
type PublisherFunc func(ctx context.Context, event Event) error

// Publish 实现 Publisher 接口。
func (f PublisherFunc) Publish(ctx context.Context, event Event) error { return f(ctx, event) }

// Relay 在 Store 与 Publisher 之间搬运 Outbox 事件：
// 领取待投递事件、交给 Publisher、成功 Ack、失败 Nack 后保留并允许重试。
// Relay 可被多个 worker 并发使用；零值不可用，请用 NewRelay 创建。
type Relay struct {
	store     Store
	publisher Publisher
	// batch 单次领取上限。
	batch int
	// retryBackoff 一批事件全部发送失败后的再次领取间隔。
	retryBackoff time.Duration
	// idleWait 没有待投递事件时的轮询等待。
	idleWait time.Duration
	// leaseTTL > 0 时启用带租约领取：worker 在 Publish/Ack/Nack 前退出，
	// 事件在租约到期后的下一轮自动恢复；0 表示沿用普通领取的默认行为。
	leaseTTL time.Duration
	// maxDeliveries > 0 时启用有界投递：同一轮次内领取次数达到上限的
	// 事件在失败退回的同一存储操作中转为死信；0 表示不限制，保持
	// 至少一次重投的既有行为。
	maxDeliveries int
	// now 等时间依赖直接使用 time 包标准行为。
	sleep func(ctx context.Context, d time.Duration) error
}

// RelayOption 配置 Relay。
type RelayOption func(*Relay)

// WithBatch 设置单次领取事件数，默认 16。
func WithBatch(n int) RelayOption {
	return func(r *Relay) {
		if n > 0 {
			r.batch = n
		}
	}
}

// WithRetryBackoff 设置发送失败后的重试间隔，默认 200ms。
func WithRetryBackoff(d time.Duration) RelayOption {
	return func(r *Relay) {
		if d > 0 {
			r.retryBackoff = d
		}
	}
}

// WithIdleWait 设置无事件时的轮询间隔，默认 500ms。
func WithIdleWait(d time.Duration) RelayOption {
	return func(r *Relay) {
		if d > 0 {
			r.idleWait = d
		}
	}
}

// WithClaimLease 启用带租约领取并设置租约时长。
//
// 仅正时长生效；传入 0 或负时长不启用租约，Relay 沿用普通 ClaimPendingEvents
// 的默认行为。启用后，每次领取的事件在 ttl 内不会被再次领取；worker 在
// Publish、Ack 或 Nack 前退出时，事件于租约到期后的下一次 DeliverOnce/Run
// 自动恢复重投，从而保证至少一次投递。Store 必须实现 ClaimLeaseStore，
// 否则 DeliverOnce 返回 ErrClaimLeaseUnsupported（可 errors.Is）。
func WithClaimLease(ttl time.Duration) RelayOption {
	return func(r *Relay) {
		if ttl > 0 {
			r.leaseTTL = ttl
		}
	}
}

// WithMaxDeliveries 设置单个投递轮次的次数上限，启用有界投递与死信。
//
// 仅正数生效；传入 0 或负数不启用上限，Relay 保持至少一次重投的既有
// 行为。启用后，每次领取仍使 Deliveries 加一，Publisher 返回 nil 照常
// Ack；返回错误且本轮次数（RoundDeliveries）未达上限时按原语义 Nack
// 保留，达到上限时在同一存储操作中转为死信并退出普通领取。租约到期
// 恢复同样受上限约束：本轮次数已用尽的事件不再调用 Publisher，直接
// 隔离为死信。死信事件只能经 Engine.RequeueDeadLetter 重新入队，重新
// 入队后开启新一轮有限计数。
//
// Store 必须实现 DeadLetterStore，否则 DeliverOnce/Run 返回
// ErrDeadLetterUnsupported（可 errors.Is）。
func WithMaxDeliveries(n int) RelayOption {
	return func(r *Relay) {
		if n > 0 {
			r.maxDeliveries = n
		}
	}
}

// NewRelay 创建事件投递器。
func NewRelay(store Store, publisher Publisher, opts ...RelayOption) *Relay {
	r := &Relay{
		store:        store,
		publisher:    publisher,
		batch:        16,
		retryBackoff: 200 * time.Millisecond,
		idleWait:     500 * time.Millisecond,
	}
	r.sleep = sleepWithContext
	for _, opt := range opts {
		opt(r)
	}
	return r
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// RelayResult 是单轮投递的结果统计。
type RelayResult struct {
	// Claimed 本轮领取的事件数。
	Claimed int
	// Delivered 发送成功并 Ack 的事件数。
	Delivered int
	// Failed 发送失败并 Nack 保留的事件数。
	Failed int
	// DeadLettered 本轮达到投递上限被转为死信的事件数（含租约恢复时
	// 已用尽而直接隔离的事件）；未配置 WithMaxDeliveries 时恒为 0。
	DeadLettered int
}

// DeliverOnce 执行一轮领取与投递，不阻塞等待。没有事件时立即返回空结果。
// 单个事件发送失败不影响同批其它事件；失败事件被 Nack，可在后续轮次继续领取。
//
// 通过 WithClaimLease 启用租约且 Store 未实现 ClaimLeaseStore 时，
// 返回包装了 ErrClaimLeaseUnsupported 的错误（可 errors.Is 判定）。
// 通过 WithMaxDeliveries 启用投递上限且 Store 未实现 DeadLetterStore 时，
// 返回 ErrDeadLetterUnsupported（可 errors.Is 判定）。
func (r *Relay) DeliverOnce(ctx context.Context) (RelayResult, error) {
	var dls DeadLetterStore
	if r.maxDeliveries > 0 {
		var ok bool
		dls, ok = r.store.(DeadLetterStore)
		if !ok {
			return RelayResult{}, ErrDeadLetterUnsupported
		}
	}
	if r.leaseTTL > 0 {
		leased, ok := r.store.(ClaimLeaseStore)
		if !ok {
			return RelayResult{}, ErrClaimLeaseUnsupported
		}
		return r.deliverOnceLeased(ctx, leased, dls)
	}
	return r.deliverOnce(ctx, dls)
}

// deliverOnce 是未启用租约时的默认路径：领取即锁定，失败立即退回。
func (r *Relay) deliverOnce(ctx context.Context, dls DeadLetterStore) (RelayResult, error) {
	events, err := r.store.ClaimPendingEvents(ctx, r.batch)
	if err != nil {
		return RelayResult{}, err
	}
	res := RelayResult{Claimed: len(events)}
	for _, ev := range events {
		if err := ctx.Err(); err != nil {
			// 退出前把尚未处理的已领取事件退回，避免它们滞留到进程重启。
			_ = r.store.NackEvent(ctx, ev.ID)
			res.Failed++
			continue
		}
		if dls != nil && ev.RoundDeliveries() > r.maxDeliveries {
			// 本轮次数已用尽（上次领取后未退回即退出）：不再调用
			// Publisher，直接在同一存储操作中隔离为死信。
			if _, err := dls.FailEvent(ctx, ev.ID, "", "txsaga: delivery attempts exhausted", r.maxDeliveries); err != nil {
				return res, err
			}
			res.DeadLettered++
			continue
		}
		if err := r.publisher.Publish(ctx, *ev); err != nil {
			if dls != nil {
				dead, failErr := dls.FailEvent(ctx, ev.ID, "", err.Error(), r.maxDeliveries)
				if failErr != nil {
					return res, failErr
				}
				if dead {
					res.DeadLettered++
				} else {
					res.Failed++
				}
				continue
			}
			if nackErr := r.store.NackEvent(ctx, ev.ID); nackErr != nil {
				return res, nackErr
			}
			res.Failed++
			continue
		}
		if err := r.store.AckEvent(ctx, ev.ID); err != nil {
			return res, err
		}
		res.Delivered++
	}
	return res, nil
}

// deliverOnceLeased 是启用租约后的投递路径。
//
// 取消语义：
//   - 领取后、Publish 前 ctx 已取消：事件从未交给 Publish，按当前 ClaimID
//     立即 Nack，等不到租约到期；
//   - Publish 进行中取消：按 Publish 的返回结果处理，nil 走 Ack，error 走
//     Nack，不因 ctx 取消改变结论；
//   - worker 在任何一步退出（未 Nack）：租约到期后事件自动恢复重投。
//
// 配置投递上限（dls 非 nil）时：Publish 失败按当前 ClaimID 走 FailEvent，
// 未达上限退回、达到上限在同一存储操作中转死信；租约恢复时发现本轮次数
// 已用尽的事件不再调用 Publisher，直接隔离为死信。
//
// Ack/Nack 使用独立上下文，避免调用方 ctx 已取消导致确认/退回被 Store
// 拒绝而被迫等待租约到期。旧 ClaimID（租约已到期、事件被他人重新领取）
// 返回 ErrStaleClaim 时不作任何操作：当前事件与新租约由接管方负责。
func (r *Relay) deliverOnceLeased(ctx context.Context, store ClaimLeaseStore, dls DeadLetterStore) (RelayResult, error) {
	claims, err := store.ClaimPendingEventsLeased(ctx, r.leaseTTL, r.batch)
	if err != nil {
		return RelayResult{}, err
	}
	res := RelayResult{Claimed: len(claims)}
	for _, c := range claims {
		if err := ctx.Err(); err != nil {
			if nackErr := store.NackLeasedEvent(context.Background(), c.Event.ID, c.ClaimID); nackErr != nil &&
				!errors.Is(nackErr, ErrStaleClaim) {
				return res, nackErr
			}
			res.Failed++
			continue
		}
		if dls != nil && c.Event.RoundDeliveries() > r.maxDeliveries {
			// 租约恢复时本轮次数已用尽：不再调用 Publisher，直接隔离。
			dead, failErr := dls.FailEvent(context.Background(), c.Event.ID, c.ClaimID,
				"txsaga: delivery attempts exhausted", r.maxDeliveries)
			if failErr != nil && !errors.Is(failErr, ErrStaleClaim) {
				return res, failErr
			}
			if failErr == nil && dead {
				res.DeadLettered++
			}
			continue
		}
		pubErr := r.publisher.Publish(ctx, *c.Event)
		if pubErr != nil {
			if dls != nil {
				dead, failErr := dls.FailEvent(context.Background(), c.Event.ID, c.ClaimID,
					pubErr.Error(), r.maxDeliveries)
				if failErr != nil && !errors.Is(failErr, ErrStaleClaim) {
					return res, failErr
				}
				if failErr == nil && dead {
					res.DeadLettered++
				} else {
					res.Failed++
				}
				continue
			}
			if nackErr := store.NackLeasedEvent(context.Background(), c.Event.ID, c.ClaimID); nackErr != nil &&
				!errors.Is(nackErr, ErrStaleClaim) {
				return res, nackErr
			}
			res.Failed++
			continue
		}
		if ackErr := store.AckLeasedEvent(context.Background(), c.Event.ID, c.ClaimID); ackErr != nil &&
			!errors.Is(ackErr, ErrStaleClaim) {
			return res, ackErr
		}
		res.Delivered++
	}
	return res, nil
}

// Run 循环执行投递，直到 ctx 被取消。至少一轮成功投递后返回 ctx.Err()。
// 全部事件发送失败的批次后按重试间隔等待；无事件时按空闲间隔等待。
func (r *Relay) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		res, err := r.DeliverOnce(ctx)
		if err != nil {
			return err
		}
		var wait time.Duration
		switch {
		case res.Claimed == 0:
			wait = r.idleWait
		case res.Failed > 0 && res.Delivered == 0:
			wait = r.retryBackoff
		default:
			wait = 0 // 本轮有成功投递，立即继续领取，尽快排空积压
		}
		if wait > 0 {
			if err := r.sleep(ctx, wait); err != nil {
				return err
			}
		}
	}
}
