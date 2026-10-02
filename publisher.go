package saga

import (
	"context"
	"errors"
	"time"
)

// Publisher 把一条 Outbox 事件投递到外部（消息代理、HTTP 回调等）。
// 实现无需保证去重：投递采用至少一次语义，订阅方应按事件 ID 去重。
type Publisher interface {
	// Publish 成功返回 nil；返回错误时事件保留在 Outbox 中并可再次领取。
	Publish(ctx context.Context, event OutboxEvent) error
}

// DeliverPending 领取至多 batch 条待投递事件并交给 pub 发送：
//   - 发送成功：标记已投递，不再被领取；
//   - 发送失败：保留原事件、投递次数加一、记录原因并立即释放领取；
//   - 标记失败（如领取已过期）：事件保持可领取，计入返回错误。
//
// 返回成功投递条数与本轮遇到的全部错误（errors.Join 聚合，全成功为 nil）。
// lease 为领取独占时长，应大于单条发送的预期耗时。
func DeliverPending(ctx context.Context, store Store, pub Publisher, batch int, lease time.Duration) (int, error) {
	events, err := store.ClaimEvents(ctx, batch, lease)
	if err != nil {
		return 0, err
	}

	var delivered int
	var errs []error
	for _, ev := range events {
		if err := pub.Publish(ctx, ev); err != nil {
			// 保留原事件、递增投递次数并释放领取；发送失败原因返回给调用方。
			if failErr := store.FailDelivery(ctx, ev.ID, ev.ClaimToken, err); failErr != nil {
				err = errors.Join(err, failErr)
			}
			errs = append(errs, err)
			continue
		}
		if err := store.MarkDelivered(ctx, ev.ID, ev.ClaimToken); err != nil {
			errs = append(errs, err)
			continue
		}
		delivered++
	}
	return delivered, errors.Join(errs...)
}
