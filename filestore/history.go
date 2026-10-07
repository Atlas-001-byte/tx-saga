package filestore

import (
	"context"

	"github.com/txsaga/txsaga"
)

const defaultEventHistoryLimit = 100

// ListEvents 实现 txsaga.EventHistoryStore：按追加顺序返回一次执行的一页
// 事件，语义与 MemoryStore.ListEvents 完全一致。
//
//   - 执行不存在（含仅有事件、没有执行记录）或 Saga 名称不符：
//     txsaga.ErrExecutionNotFound；
//   - AfterID 非空但未命中或属于其它执行：txsaga.ErrEventCursorNotFound，
//     不返回任何记录，也不改变任何状态；
//   - limit<=0 时按 100 条返回。
//
// 查询只读：不推进执行、不改变领取/退回/租约/投递计数，也不触发落盘。
// 待投递、领取中、退回以及已 Ack 的事件都在审计链中，按追加顺序排列。
func (s *FileStore) ListEvents(ctx context.Context, q txsaga.EventHistoryQuery) (txsaga.EventHistoryPage, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = defaultEventHistoryLimit
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ready(ctx); err != nil {
		return txsaga.EventHistoryPage{}, err
	}

	st, ok := s.d.execs[joinExecID(q.BusinessKey, q.IdempotencyKey)]
	if !ok || st.SagaName != q.SagaName {
		return txsaga.EventHistoryPage{}, txsaga.ErrExecutionNotFound
	}

	start := 0
	if q.AfterID != "" {
		idx := -1
		for i, me := range s.d.history {
			if me.event.ID == q.AfterID &&
				me.event.BusinessKey == q.BusinessKey &&
				me.idempotencyKey == q.IdempotencyKey {
				idx = i
				break
			}
		}
		// 游标不存在或属于其它执行：不返回部分结果，也不触碰任何状态。
		if idx < 0 {
			return txsaga.EventHistoryPage{}, txsaga.ErrEventCursorNotFound
		}
		start = idx + 1
	}

	page := txsaga.EventHistoryPage{Events: []txsaga.EventRecord{}}
	if q.AfterID != "" {
		// 空页（游标已是最后一条）时回传入参游标，重复查询保持幂等。
		page.NextAfterID = q.AfterID
	}
	next := start
	for i := start; i < len(s.d.history) && len(page.Events) < limit; i++ {
		me := s.d.history[i]
		if me.event.BusinessKey != q.BusinessKey || me.idempotencyKey != q.IdempotencyKey {
			continue
		}
		page.Events = append(page.Events, toEventRecord(me))
		next = i + 1
	}
	if len(page.Events) > 0 {
		page.NextAfterID = page.Events[len(page.Events)-1].ID
	}
	for i := next; i < len(s.d.history); i++ {
		me := s.d.history[i]
		if me.event.BusinessKey == q.BusinessKey && me.idempotencyKey == q.IdempotencyKey {
			page.HasMore = true
			break
		}
	}
	return page, nil
}

// toEventRecord 把内部事件复制为脱离存储的只读记录；投递元数据取读取当下
// 的最新值，已 Ack 事件同样可读出（计数与最近领取时间为其最后一次值）。
func toEventRecord(me *fsEvent) txsaga.EventRecord {
	ep, _ := me.event.Payload.(txsaga.EventPayload)
	if ep.SucceededSteps != nil {
		ep.SucceededSteps = append([]string(nil), ep.SucceededSteps...)
	}
	return txsaga.EventRecord{
		ID:            me.event.ID,
		Type:          me.event.Type,
		OccurredAt:    me.event.OccurredAt,
		Payload:       ep,
		BusinessKey:   me.event.BusinessKey,
		Deliveries:    me.event.Deliveries(),
		LastAttemptAt: me.event.LastAttemptAt(),
	}
}
