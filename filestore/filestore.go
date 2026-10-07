// Package filestore 提供 txsaga.Store 的本地目录持久化实现 FileStore。
//
// FileStore 把执行状态、业务键定义绑定与 Outbox 事件（含已 Ack 事件的审计
// 副本）保存在构造时给定目录下的单个 JSON 快照文件中。每次状态变更或事件
// 投递元数据变更都在同一临界区内生成整库新快照，经“临时文件写入、fsync、
// 原子改名替换”落盘：一次 Commit 中的状态修改与事件追加要么整体可见、要么
// 整体不可见，中断写入不会只留下状态或只留下事件；写入成功返回后，即使
// 进程退出，重新 Open 同一目录仍可继续执行、查询终态并读取完整事件历史。
//
// 并发模型：FileStore 支持同一进程内多个 goroutine 共享同一实例（与同一
// 目录）；不支持多个进程（或同一进程内的多个 FileStore 实例）同时写同一
// 目录——后者在构造时返回 ErrStoreLocked。
//
// 除错误哨兵外，FileStore 只依赖 Go 标准库，并且不改变 txsaga 根包中
// MemoryStore 与 Engine 的既有行为。
package filestore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/txsaga/txsaga"
)

// 哨兵错误，调用方可用 errors.Is 判定。
var (
	// ErrStoreOpen 存储目录无法打开：路径不可创建、不是目录、不可读写，
	// 或初始化快照无法落盘。
	ErrStoreOpen = errors.New("filestore: cannot open store directory")
	// ErrStoreLocked 同一进程内已有另一个 FileStore 占用该目录。
	// FileStore 不支持同目录的多实例（或多进程）并发写入。
	ErrStoreLocked = errors.New("filestore: store directory is locked by another FileStore in this process")
	// ErrCorruptStore 目录中的既有快照损坏、被截断或无法组成一致快照
	// （引用缺失、身份不一致、未知版本等）。返回该错误时不会覆盖原文件。
	ErrCorruptStore = errors.New("filestore: corrupt store snapshot")
	// ErrUnsupportedPayload Payload 或事件负载无法被 encoding/json 稳定
	// 表示（如 chan、func、循环引用、NaN 等）。返回该错误时本次提交整体
	// 丢弃，不会留下半次提交。
	ErrUnsupportedPayload = errors.New("filestore: payload cannot be represented stably as JSON")
	// ErrStoreClosed FileStore 已 Close，之后的任何读写都返回该错误。
	ErrStoreClosed = errors.New("filestore: store already closed")
)

const (
	// stateFile 是目录中保存整库快照的唯一数据文件。
	stateFile = "state.json"
	// stateTmpFile 是原子改名前的临时文件名，与快照同目录。
	stateTmpFile = ".state.json.tmp"
	// lockFile 标记目录被一个存活的 FileStore 占用。
	lockFile = "filestore.lock"

	snapshotFormat  = "txsaga-filestore-v1"
	snapshotVersion = 1
)

// fsEvent 是 FileStore 内部的事件记录，等价于根包内存实现里的 memEvent：
// 审计链与活动 Outbox 索引共享同一个指针，投递/领取元数据只有一份。
type fsEvent struct {
	event txsaga.Event

	// idempotencyKey 是事件所属执行的外部幂等键，历史查询按它过滤。
	idempotencyKey string

	// claimed 普通领取（ClaimPendingEvents）的锁定标记。
	claimed bool

	// claimID 非空表示存在一次带租约领取；claimedUntil 之后该领取失效。
	claimID      string
	claimedUntil time.Time

	// alive 为 false 表示事件已被 Ack：不再参与领取，但审计链继续保留。
	// 这与内存实现“从活动 map 删除、历史链保留指针”等价。
	alive bool

	// 死信状态。deadLettered 为 true 表示事件已达投递上限被隔离：退出
	// 普通领取，只能经 RequeueDeadLetter 重新入队。deadLetterReason 与
	// deadLetteredAt 记录最近一次进入死信的原因与时刻，重新入队后作为
	// 历史失败信息保留，不回退。
	deadLettered     bool
	deadLetterReason string
	deadLetteredAt   time.Time
}

// fsData 是整库内存态的不可变替换根：每次变更都在其深拷贝上进行，
// 快照落盘成功后整根替换，使读路径永远只看到已落盘的一致版本。
type fsData struct {
	execs    map[string]*txsaga.ExecutionState
	bindings map[string]txsaga.Binding
	events   map[string]*fsEvent
	pending  []string // 活动事件 ID，保持追加顺序（可能含已 Ack 的墓碑 ID）
	history  []*fsEvent
	seq      uint64
	claimSeq uint64
}

func newData() *fsData {
	return &fsData{
		execs:    make(map[string]*txsaga.ExecutionState),
		bindings: make(map[string]txsaga.Binding),
		events:   make(map[string]*fsEvent),
		pending:  []string{},
		history:  []*fsEvent{},
	}
}

// FileStore 是建立在本地目录上的 txsaga.Store 实现，同时实现
// txsaga.ClaimLeaseStore、txsaga.EventHistoryStore 与 txsaga.DeadLetterStore。
//
// 零值不可用，请使用 Open 构造；用完必须调用 Close 释放目录占用。
type FileStore struct {
	dir      string // 规范化后的绝对目录
	lockPath string

	mu     sync.Mutex
	d      *fsData
	closed bool

	// now 为时间来源，生产固定为 time.Now；测试可在同包内替换。
	now func() time.Time
}

// 编译期断言：FileStore 满足全部四个 Store 契约。
var (
	_ txsaga.Store             = (*FileStore)(nil)
	_ txsaga.ClaimLeaseStore   = (*FileStore)(nil)
	_ txsaga.EventHistoryStore = (*FileStore)(nil)
	_ txsaga.DeadLetterStore   = (*FileStore)(nil)
)

// openRegistry 记录本进程内被存活 FileStore 占用的目录（规范路径 -> 实例）。
// 规格只要求同一进程内单实例占用同一目录。
var (
	openMu     sync.Mutex
	openStores = map[string]*FileStore{}
)

// Open 打开（或初始化）dir 下的 FileStore。
//
//   - 目录不存在时按需创建；不可创建、不是目录或不可读写时返回包装了
//     ErrStoreOpen 的错误；
//   - 同一进程已有另一个 FileStore 占用该目录时返回 ErrStoreLocked；
//   - 目录中已有快照但损坏、截断或无法组成一致快照时返回 ErrCorruptStore，
//     且不会覆盖原数据文件；
//   - 目录为空时初始化一份空快照并落盘，确保目录可写。
func Open(dir string) (*FileStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("filestore: empty directory path: %w", ErrStoreOpen)
	}
	// 0755 与 MkdirAll 的常规用法一致；进程 umask 会收敛实际权限。
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("filestore: create directory %q: %v: %w", dir, err, ErrStoreOpen)
	}
	info, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("filestore: stat directory %q: %v: %w", dir, err, ErrStoreOpen)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("filestore: path %q is not a directory: %w", dir, ErrStoreOpen)
	}
	canonical, err := canonicalDir(dir)
	if err != nil {
		return nil, fmt.Errorf("filestore: resolve directory %q: %v: %w", dir, err, ErrStoreOpen)
	}

	// 整个打开过程在进程级注册表锁内串行完成：并发 Open 同一目录时，失败
	// 者仅凭注册表判定 ErrStoreLocked，不会截断或改写占有者的锁文件。
	openMu.Lock()
	defer openMu.Unlock()

	if holder, ok := openStores[canonical]; ok && !holder.isClosed() {
		return nil, fmt.Errorf("filestore: directory %q: %w", canonical, ErrStoreLocked)
	}

	s := &FileStore{
		dir:      canonical,
		lockPath: filepath.Join(canonical, lockFile),
		now:      time.Now,
	}

	// 注册表无存活实例时，已存在的锁文件只能来自上次未正常 Close 的进程，
	// 按陈旧锁接管；创建/接管失败说明目录不可写。
	if err := s.acquireLock(); err != nil {
		return nil, fmt.Errorf("filestore: lock directory %q: %v: %w", canonical, err, ErrStoreOpen)
	}
	// 任何失败路径都释放本次锁占用，绝不留下“锁定但无实例”的状态。
	releaseLockOnFail := true
	defer func() {
		if releaseLockOnFail {
			_ = os.Remove(s.lockPath)
		}
	}()

	raw, err := os.ReadFile(filepath.Join(canonical, stateFile))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("filestore: read snapshot in %q: %v: %w", canonical, err, ErrStoreOpen)
	}

	if errors.Is(err, os.ErrNotExist) {
		// 空目录：初始化空快照并落盘，验证可写性。
		s.d = newData()
		if werr := writeSnapshot(canonical, mustEncode(s.d)); werr != nil {
			return nil, fmt.Errorf("filestore: initialize snapshot in %q: %v: %w", canonical, werr, ErrStoreOpen)
		}
	} else {
		if len(raw) == 0 {
			return nil, fmt.Errorf("filestore: snapshot %q is empty (truncated): %w",
				filepath.Join(canonical, stateFile), ErrCorruptStore)
		}
		d, verr := decodeSnapshot(raw)
		if verr != nil {
			// 损坏：不覆盖原数据文件（state.json 未被触碰），只释放锁占用。
			return nil, fmt.Errorf("filestore: snapshot in %q: %v: %w", canonical, verr, ErrCorruptStore)
		}
		s.d = d
	}

	// 上次崩溃可能在 rename 前留下临时文件；状态文件本身仍是上一份一致
	// 快照，临时文件永远不会被读取，这里顺手清理。
	_ = os.Remove(filepath.Join(canonical, stateTmpFile))

	openStores[canonical] = s
	releaseLockOnFail = false
	return s, nil
}

func (s *FileStore) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// Close 释放目录占用并移除锁文件。重复关闭返回 nil。Close 之后的任何
// 读写返回包装了 ErrStoreClosed 的错误。
func (s *FileStore) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()

	openMu.Lock()
	if cur, ok := openStores[s.dir]; ok && cur == s {
		delete(openStores, s.dir)
	}
	openMu.Unlock()

	return os.Remove(s.lockPath)
}

// ready 在持锁状态下检查 context 与关闭标志。context 取消原样返回其错误，
// 且不触碰任何状态；已关闭返回 ErrStoreClosed。
func (s *FileStore) ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return ErrStoreClosed
	}
	return nil
}

// acquireLock 创建或接管锁文件。规格不支持多进程同写：注册表无存活实例时，
// 已存在的锁文件只能来自上次未正常 Close 的进程，按陈旧锁接管。
func (s *FileStore) acquireLock() error {
	f, err := os.OpenFile(s.lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		// 接管陈旧锁：截断重写。
		f, err = os.OpenFile(s.lockPath, os.O_TRUNC|os.O_WRONLY, 0o600)
		if err != nil {
			return err
		}
	}
	token := fmt.Sprintf("pid=%d opened=%d\n", os.Getpid(), s.now().UnixNano())
	if _, err := f.WriteString(token); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func canonicalDir(dir string) (string, error) {
	abs, err := filepath.Abs(filepath.Clean(dir))
	if err != nil {
		return "", err
	}
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		return resolved, nil
	}
	return abs, nil
}

// ---- 快照编解码 ----

type persistBinding struct {
	SagaName    string `json:"saga_name"`
	Fingerprint string `json:"fingerprint"`
}

type persistExec struct {
	// State 是 ExecutionState 的 JSON；其 Payload 字段带 json:"-" 标签，
	// 不会包含在内，由下方 Payload 单独承载，保证请求负载可跨重开往返。
	State   json.RawMessage `json:"state"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

type persistEvent struct {
	ID             string          `json:"id"`
	BusinessKey    string          `json:"business_key"`
	Type           string          `json:"type"`
	OccurredAt     time.Time       `json:"occurred_at"`
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey string          `json:"idempotency_key"`

	Deliveries    int       `json:"deliveries"`
	LastAttemptAt time.Time `json:"last_attempt_at,omitempty"`
	// RoundDeliveries 当前投递轮次的领取次数，用于有界投递上限判定，
	// 重新入队死信时归零；累计 Deliveries 不回退。
	RoundDeliveries int       `json:"round_deliveries,omitempty"`
	Claimed         bool      `json:"claimed"`
	ClaimID         string    `json:"claim_id,omitempty"`
	ClaimedUntil    time.Time `json:"claimed_until,omitempty"`
	Acked           bool      `json:"acked"`

	DeadLettered     bool      `json:"dead_lettered,omitempty"`
	DeadLetterReason string    `json:"dead_letter_reason,omitempty"`
	DeadLetteredAt   time.Time `json:"dead_lettered_at,omitempty"`
}

type persistSnapshot struct {
	Format   string                    `json:"format"`
	Version  int                       `json:"version"`
	Seq      uint64                    `json:"seq"`
	ClaimSeq uint64                    `json:"claim_seq"`
	Bindings map[string]persistBinding `json:"bindings"`
	Execs    map[string]persistExec    `json:"execs"`
	Events   []persistEvent            `json:"events"`
	Pending  []string                  `json:"pending"`
}

// mustEncode 编码一份确定可序列化的内存态（空态或已通过提交校验的状态）；
// 仅用于初始化等理论上不会失败的路径。
func mustEncode(d *fsData) []byte {
	b, err := encodeSnapshot(d)
	if err != nil {
		panic(fmt.Sprintf("filestore: internal error: encode snapshot failed: %v", err))
	}
	return b
}

func encodeSnapshot(d *fsData) ([]byte, error) {
	out := persistSnapshot{
		Format:   snapshotFormat,
		Version:  snapshotVersion,
		Seq:      d.seq,
		ClaimSeq: d.claimSeq,
		Bindings: make(map[string]persistBinding, len(d.bindings)),
		Execs:    make(map[string]persistExec, len(d.execs)),
		Events:   make([]persistEvent, 0, len(d.history)),
		Pending:  make([]string, 0, len(d.pending)),
	}
	for k, b := range d.bindings {
		out.Bindings[k] = persistBinding{SagaName: b.SagaName, Fingerprint: b.Fingerprint}
	}
	for id, st := range d.execs {
		rawState, err := json.Marshal(st)
		if err != nil {
			return nil, err
		}
		pe := persistExec{State: rawState}
		if st.Payload != nil {
			rawPayload, err := json.Marshal(st.Payload)
			if err != nil {
				return nil, err
			}
			pe.Payload = rawPayload
		}
		out.Execs[id] = pe
	}
	// 事件严格按审计链（追加顺序）输出，已 Ack 的事件以 Acked 标记保留。
	for _, me := range d.history {
		ep, ok := me.event.Payload.(txsaga.EventPayload)
		if !ok {
			return nil, fmt.Errorf("event %s: payload is %T, not txsaga.EventPayload", me.event.ID, me.event.Payload)
		}
		rawPayload, err := json.Marshal(ep)
		if err != nil {
			return nil, err
		}
		out.Events = append(out.Events, persistEvent{
			ID:              me.event.ID,
			BusinessKey:     me.event.BusinessKey,
			Type:            me.event.Type,
			OccurredAt:      me.event.OccurredAt,
			Payload:         rawPayload,
			IdempotencyKey:  me.idempotencyKey,
			Deliveries:      me.event.Deliveries(),
			LastAttemptAt:   me.event.LastAttemptAt(),
			RoundDeliveries: me.event.RoundDeliveries(),
			Claimed:         me.claimed,
			ClaimID:         me.claimID,
			ClaimedUntil:    me.claimedUntil,
			Acked:           !me.alive,

			DeadLettered:     me.deadLettered,
			DeadLetterReason: me.deadLetterReason,
			DeadLetteredAt:   me.deadLetteredAt,
		})
	}
	// 落盘时压缩墓碑：已 Ack 或已死信的事件保留在审计链，但不在待领取队列中。
	seenPending := make(map[string]struct{}, len(d.pending))
	for _, id := range d.pending {
		me := d.events[id]
		if me == nil || !me.alive || me.deadLettered {
			continue
		}
		if _, dup := seenPending[id]; dup {
			return nil, fmt.Errorf("duplicated pending event id %s", id)
		}
		seenPending[id] = struct{}{}
		out.Pending = append(out.Pending, id)
	}
	return json.MarshalIndent(out, "", "  ")
}

func writeSnapshot(dir string, data []byte) error {
	tmp := filepath.Join(dir, stateTmpFile)
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	// 写完并 Sync 文件内容后再改名；任一步失败都清理临时文件，状态文件不动。
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, stateFile)); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	// fsync 目录，确保 rename 在掉电等场景下也持久化。
	if df, err := os.Open(dir); err == nil {
		_ = df.Sync()
		_ = df.Close()
	}
	return nil
}

// persist 把下一版内存态落盘；成功前不会触碰当前内存根。
func (s *FileStore) persist(next *fsData) error {
	data, err := encodeSnapshot(next)
	if err != nil {
		// 唯一的 any 负载是 ExecutionState.Payload；固定结构不会序列化失败。
		return fmt.Errorf("%w: %v", ErrUnsupportedPayload, err)
	}
	if err := writeSnapshot(s.dir, data); err != nil {
		return fmt.Errorf("filestore: write snapshot in %q: %w", s.dir, err)
	}
	return nil
}
