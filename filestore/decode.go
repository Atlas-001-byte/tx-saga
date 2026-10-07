package filestore

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/txsaga/txsaga"
)

// decodeSnapshot 解析并交叉校验整库快照。任何 JSON 语法错误、被截断、
// 未知格式/版本、引用缺失或身份不一致都视为损坏：返回包装了
// ErrCorruptStore 的错误，调用方不得覆盖原文件。
func decodeSnapshot(raw []byte) (*fsData, error) {
	var ps persistSnapshot
	if err := json.Unmarshal(raw, &ps); err != nil {
		return nil, fmt.Errorf("parse snapshot: %v: %w", err, ErrCorruptStore)
	}
	if ps.Format != snapshotFormat {
		return nil, fmt.Errorf("unknown snapshot format %q: %w", ps.Format, ErrCorruptStore)
	}
	if ps.Version != snapshotVersion {
		return nil, fmt.Errorf("unsupported snapshot version %d: %w", ps.Version, ErrCorruptStore)
	}

	d := newData()
	d.seq = ps.Seq
	d.claimSeq = ps.ClaimSeq

	// 执行记录：键必须与内部身份一致，状态体必须可解析。
	for id, pe := range ps.Execs {
		business, idem, ok := splitExecID(id)
		if !ok {
			return nil, fmt.Errorf("execution key %q malformed: %w", id, ErrCorruptStore)
		}
		var st txsaga.ExecutionState
		if err := json.Unmarshal(pe.State, &st); err != nil {
			return nil, fmt.Errorf("execution %q state: %v: %w", id, err, ErrCorruptStore)
		}
		if len(pe.Payload) > 0 {
			var p any
			if err := json.Unmarshal(pe.Payload, &p); err != nil {
				return nil, fmt.Errorf("execution %q payload: %v: %w", id, err, ErrCorruptStore)
			}
			st.Payload = p
		}
		if st.BusinessKey != business || st.IdempotencyKey != idem {
			return nil, fmt.Errorf("execution %q identity mismatch (got %q/%q): %w",
				id, st.BusinessKey, st.IdempotencyKey, ErrCorruptStore)
		}
		if st.Steps == nil {
			// 归一化：createExecution 总是写入非 nil 的步骤切片。
			st.Steps = []txsaga.StepState{}
		}
		d.execs[id] = &st
	}

	// 定义绑定：业务键必须与某个执行一致，名称/指纹必须匹配。
	for business, b := range ps.Bindings {
		if b.SagaName == "" || b.Fingerprint == "" {
			return nil, fmt.Errorf("binding for %q is incomplete: %w", business, ErrCorruptStore)
		}
		matched := false
		for id, st := range d.execs {
			bk, _, _ := splitExecID(id)
			if bk == business {
				if st.SagaName != b.SagaName || st.Fingerprint != b.Fingerprint {
					return nil, fmt.Errorf("binding for %q disagrees with execution %q: %w",
						business, id, ErrCorruptStore)
				}
				matched = true
			}
		}
		if !matched {
			return nil, fmt.Errorf("binding for %q has no execution: %w", business, ErrCorruptStore)
		}
		d.bindings[business] = txsaga.Binding{SagaName: b.SagaName, Fingerprint: b.Fingerprint}
	}

	// 事件：ID 唯一且编号单调（与审计链顺序一致），身份必须能找到执行，
	// 投递元数据自洽，已 Ack 事件不得出现在待领取队列。
	knownIDs := make(map[string]*fsEvent, len(ps.Events))
	var lastSeqNum uint64
	for i := range ps.Events {
		pe := &ps.Events[i]
		if pe.ID == "" || pe.BusinessKey == "" || pe.Type == "" || pe.IdempotencyKey == "" {
			return nil, fmt.Errorf("event at %d missing identity fields: %w", i, ErrCorruptStore)
		}
		if pe.OccurredAt.IsZero() {
			return nil, fmt.Errorf("event %q has zero occurrence time: %w", pe.ID, ErrCorruptStore)
		}
		num, err := parseEventID(pe.ID)
		if err != nil {
			return nil, fmt.Errorf("%v: %w", err, ErrCorruptStore)
		}
		if i > 0 && num <= lastSeqNum {
			return nil, fmt.Errorf("event %q out of append order: %w", pe.ID, ErrCorruptStore)
		}
		lastSeqNum = num
		if _, dup := knownIDs[pe.ID]; dup {
			return nil, fmt.Errorf("duplicated event id %q: %w", pe.ID, ErrCorruptStore)
		}
		// 事件允许引用尚不存在的执行（MemoryStore 同样允许只追加事件）：
		// 历史查询按身份过滤，执行不存在时查询侧返回 ErrExecutionNotFound。
		var ep txsaga.EventPayload
		if err := json.Unmarshal(pe.Payload, &ep); err != nil {
			return nil, fmt.Errorf("event %q payload: %v: %w", pe.ID, err, ErrCorruptStore)
		}
		if pe.Deliveries < 0 {
			return nil, fmt.Errorf("event %q has negative deliveries: %w", pe.ID, ErrCorruptStore)
		}
		if (pe.ClaimID == "") != pe.ClaimedUntil.IsZero() {
			return nil, fmt.Errorf("event %q lease fields inconsistent: %w", pe.ID, ErrCorruptStore)
		}
		if pe.Claimed && pe.ClaimID != "" {
			// 普通领取与租约领取互斥（内存实现接管到期租约会清除 ClaimID）。
			return nil, fmt.Errorf("event %q held by both claim kinds: %w", pe.ID, ErrCorruptStore)
		}
		if pe.RoundBase < 0 || pe.RoundBase > pe.Deliveries {
			return nil, fmt.Errorf("event %q round base %d out of range [0,%d]: %w",
				pe.ID, pe.RoundBase, pe.Deliveries, ErrCorruptStore)
		}
		if pe.Dead {
			// 死信事件已退出普通领取：不得持有任何领取标记，且必须记录
			// 进入死信时间；死信与已 Ack 互斥。
			if pe.Acked || pe.Claimed || pe.ClaimID != "" {
				return nil, fmt.Errorf("event %q dead-lettered while acked or claimed: %w", pe.ID, ErrCorruptStore)
			}
			if pe.DeadLetteredAt.IsZero() {
				return nil, fmt.Errorf("event %q dead-lettered without dead-letter time: %w", pe.ID, ErrCorruptStore)
			}
		}
		ev := txsaga.Event{
			ID:          pe.ID,
			BusinessKey: pe.BusinessKey,
			Type:        pe.Type,
			OccurredAt:  pe.OccurredAt,
			Payload:     ep,
		}
		ev = ev.WithDeliveryMeta(pe.Deliveries, pe.LastAttemptAt)
		me := &fsEvent{
			event:          ev,
			idempotencyKey: pe.IdempotencyKey,
			claimed:        pe.Claimed,
			claimID:        pe.ClaimID,
			claimedUntil:   pe.ClaimedUntil,
			alive:          !pe.Acked,
			dead:           pe.Dead,
			lastError:      pe.LastError,
			deadLetteredAt: pe.DeadLetteredAt,
			roundBase:      pe.RoundBase,
		}
		knownIDs[pe.ID] = me
		d.history = append(d.history, me)
	}
	if ps.Seq < lastSeqNum {
		return nil, fmt.Errorf("snapshot seq %d behind last event %d: %w",
			ps.Seq, lastSeqNum, ErrCorruptStore)
	}

	// 待领取队列：引用必须存在、未 Ack、未死信、不重复。
	seen := make(map[string]struct{}, len(ps.Pending))
	for _, id := range ps.Pending {
		me, ok := knownIDs[id]
		if !ok {
			return nil, fmt.Errorf("pending event %q missing from event chain: %w", id, ErrCorruptStore)
		}
		if !me.alive {
			return nil, fmt.Errorf("acked event %q still pending: %w", id, ErrCorruptStore)
		}
		if me.dead {
			return nil, fmt.Errorf("dead-lettered event %q still pending: %w", id, ErrCorruptStore)
		}
		if _, dup := seen[id]; dup {
			return nil, fmt.Errorf("pending event %q duplicated: %w", id, ErrCorruptStore)
		}
		seen[id] = struct{}{}
		d.pending = append(d.pending, id)
	}
	d.events = knownIDs
	return d, nil
}

// joinExecID / splitExecID 与根包内部 execID 同构（business+'\x00'+idem），
// 快照 map 直接以该串为键，避免再引入一套键格式。
func joinExecID(businessKey, idempotencyKey string) string {
	return businessKey + "\x00" + idempotencyKey
}

func splitExecID(id string) (business, idem string, ok bool) {
	idx := strings.IndexByte(id, 0)
	if idx <= 0 || idx == len(id)-1 {
		return "", "", false
	}
	return id[:idx], id[idx+1:], true
}

// parseEventID 解析根包分配的 evt-%020d 事件编号，用于顺序与序号校验。
func parseEventID(id string) (uint64, error) {
	const prefix = "evt-"
	if !strings.HasPrefix(id, prefix) || len(id) != len(prefix)+20 {
		return 0, fmt.Errorf("event %q has unexpected id shape", id)
	}
	n, err := strconv.ParseUint(id[len(prefix):], 10, 64)
	if err != nil || n == 0 {
		return 0, fmt.Errorf("event %q has non-numeric or zero sequence", id)
	}
	return n, nil
}
