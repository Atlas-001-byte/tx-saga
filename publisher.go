package txsaga

import (
	"context"
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
}

// DeliverOnce 执行一轮领取与投递，不阻塞等待。没有事件时立即返回空结果。
// 单个事件发送失败不影响同批其它事件；失败事件被 Nack，可在后续轮次继续领取。
func (r *Relay) DeliverOnce(ctx context.Context) (RelayResult, error) {
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
		if err := r.publisher.Publish(ctx, *ev); err != nil {
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
