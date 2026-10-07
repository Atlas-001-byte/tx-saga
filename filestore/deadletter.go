package filestore

import (
	"context"
	"fmt"
	"time"

	"github.com/txsaga/txsaga"
)

// deadLetter 把事件转为死信：清除全部领取标记并记录进入死信时间。
// 调用方负责把事件移出待领取队列（领取扫描路径不保留该 ID，
// 退回路径显式调用 removePending）。
func deadLetter(me *fsEvent, now time.Time) {
	me.dead = true
	me.claimed = false
	me.claimID = ""
	me.claimedUntil = time.Time{}
	me.deadLetteredAt = now
}

// removePending 从待领取队列移除事件 ID（死信隔离时使用）。
func removePending(d *fsData, eventID string) {
	kept := d.pending[:0]
	for _, id := range d.pending {
		if id != eventID {
			kept = append(kept, id)
		}
	}
	d.pending = kept
}

// roundExhausted 报告本轮投递次数是否已达上限。
func roundExhausted(me *fsEvent, maxDeliveries int) bool {
	return me.event.Deliveries()-me.roundBase >= maxDeliveries
}

// ClaimPendingEventsBounded 实现 txsaga.DeadLetterStore，语义与 MemoryStore
// 一致：本轮投递次数已达上限的事件不再返回，而是在本次领取操作中原子隔离
// 为死信（同一份快照落盘）。
func (s *FileStore) ClaimPendingEventsBounded(ctx context.Context, maxDeliveries, max int) ([]*txsaga.Event, int, error) {
	var out []*txsaga.Event
	dead := 0
	err := s.mutate(ctx, func(d *fsData, now time.Time) error {
		if max <= 0 {
			max = 16
		}
		out = make([]*txsaga.Event, 0, max)
		remaining := d.pending[:0]
		for _, eid := range d.pending {
			me := d.events[eid]
			if me == nil || !me.alive || me.dead {
				continue // 已 Ack 或已死信隔离，丢弃墓碑 ID
			}
			if me.claimID != "" && now.Before(me.claimedUntil) {
				remaining = append(remaining, eid)
				continue
			}
			if roundExhausted(me, maxDeliveries) {
				// 本轮次数已用尽（如租约到期恢复）：不再交给 Publisher，
				// 在本次领取操作中直接隔离为死信。
				deadLetter(me, now)
				dead++
				continue
			}
			if len(out) >= max {
				remaining = append(remaining, eid)
				continue
			}
			me.claimed = true
			me.claimID = ""
			me.claimedUntil = time.Time{}
			me.event = me.event.WithDeliveryMeta(me.event.Deliveries()+1, now)
			cp := me.event
			out = append(out, &cp)
		}
		d.pending = remaining
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return out, dead, nil
}

// NackEventBounded 实现 txsaga.DeadLetterStore：本轮投递次数未达上限时与
// NackEvent 相同；达到上限时在同一操作中原子转为死信（记录失败原因），
// 事件退出普通领取。
func (s *FileStore) NackEventBounded(ctx context.Context, eventID, reason string, maxDeliveries int) (bool, error) {
	dead := false
	err := s.mutate(ctx, func(d *fsData, now time.Time) error {
		me, ok := d.events[eventID]
		if !ok || !me.alive {
			return fmt.Errorf("txsaga: event %q not found", eventID)
		}
		if me.dead {
			dead = true // 已被隔离：幂等
			return nil
		}
		if !me.claimed {
			return nil // 幂等退回：未在领取中的事件无需处理
		}
		me.lastError = reason
		if roundExhausted(me, maxDeliveries) {
			deadLetter(me, now)
			dead = true
			return nil
		}
		me.claimed = false
		d.pending = append(d.pending, eventID)
		return nil
	})
	if err != nil {
		return false, err
	}
	return dead, nil
}

// ClaimPendingEventsLeasedBounded 实现 txsaga.DeadLetterLeaseStore：租约
// 到期恢复时发现本轮投递次数已用尽的事件不再返回，而是在本次领取操作中
// 原子隔离为死信。
func (s *FileStore) ClaimPendingEventsLeasedBounded(ctx context.Context, ttl time.Duration, maxDeliveries, max int) ([]txsaga.ClaimedEvent, int, error) {
	var out []txsaga.ClaimedEvent
	dead := 0
	err := s.mutate(ctx, func(d *fsData, now time.Time) error {
		if max <= 0 {
			max = 16
		}
		out = make([]txsaga.ClaimedEvent, 0, max)
		live := d.pending[:0]
		for _, eid := range d.pending {
			me := d.events[eid]
			if me == nil || !me.alive || me.dead {
				continue
			}
			if me.claimID != "" && now.Before(me.claimedUntil) {
				live = append(live, eid)
				continue
			}
			if roundExhausted(me, maxDeliveries) {
				// 租约到期恢复时发现本轮次数已用尽：直接隔离为死信，
				// 不再生成新租约、不再交给 Publisher。
				deadLetter(me, now)
				dead++
				continue
			}
			if len(out) >= max {
				live = append(live, eid)
				continue
			}
			d.claimSeq++
			cid := fmt.Sprintf("claim-%020d", d.claimSeq)
			me.claimID = cid
			me.claimedUntil = now.Add(ttl)
			me.event = me.event.WithDeliveryMeta(me.event.Deliveries()+1, now)
			cp := me.event
			out = append(out, txsaga.ClaimedEvent{
				Event:        &cp,
				ClaimID:      cid,
				ClaimedUntil: me.claimedUntil,
			})
			live = append(live, eid)
		}
		d.pending = live
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return out, dead, nil
}

// NackLeasedEventBounded 实现 txsaga.DeadLetterLeaseStore：仅当前 claimID
// 可退回；达到上限时在同一操作中原子转为死信，事件退出普通领取。
func (s *FileStore) NackLeasedEventBounded(ctx context.Context, eventID, claimID, reason string, maxDeliveries int) (bool, error) {
	dead := false
	err := s.mutate(ctx, func(d *fsData, now time.Time) error {
		me, ok := d.events[eventID]
		if !ok || !me.alive {
			return fmt.Errorf("txsaga: event %q not found", eventID)
		}
		if me.claimID != claimID {
			return fmt.Errorf("txsaga: event %q claim %q is not current: %w", eventID, claimID, txsaga.ErrStaleClaim)
		}
		me.lastError = reason
		if roundExhausted(me, maxDeliveries) {
			deadLetter(me, now)
			removePending(d, eventID) // 租约事件原位于待领取队列中，移出
			dead = true
			return nil
		}
		me.claimID = ""
		me.claimedUntil = time.Time{}
		return nil
	})
	if err != nil {
		return false, err
	}
	return dead, nil
}

// RequeueDeadLetterEvent 实现 txsaga.DeadLetterStore：只作用于死信事件，
// 与并发领取、Ack、Nack 在同一临界区与同一快照内原子互斥。成功调用清除
// 领取锁并恢复待投递，开启新一轮有限计数；累计 Deliveries、最近失败原因
// 与进入死信时间作为历史信息保留，不回退。
func (s *FileStore) RequeueDeadLetterEvent(ctx context.Context, eventID string) error {
	return s.mutate(ctx, func(d *fsData, _ time.Time) error {
		me, ok := d.events[eventID]
		if !ok {
			return fmt.Errorf("txsaga: event %q: %w", eventID, txsaga.ErrEventNotFound)
		}
		if !me.alive {
			return fmt.Errorf("txsaga: event %q already delivered: %w", eventID, txsaga.ErrEventNotDeadLettered)
		}
		if !me.dead {
			return fmt.Errorf("txsaga: event %q is deliverable: %w", eventID, txsaga.ErrEventNotDeadLettered)
		}
		me.dead = false
		me.claimed = false
		me.claimID = ""
		me.claimedUntil = time.Time{}
		me.roundBase = me.event.Deliveries() // 新一轮有限计数从零开始
		d.pending = append(d.pending, eventID)
		return nil
	})
}
