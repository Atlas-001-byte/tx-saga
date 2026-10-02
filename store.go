package saga

import (
	"context"
	"encoding/json"
	"time"
)

// EventType 标识一次执行状态变化。
type EventType string

const (
	// EventExecutionStarted 执行已创建（首个事件）。
	EventExecutionStarted EventType = "execution.started"
	// EventStepSucceeded 某正向步骤已确认成功。
	EventStepSucceeded EventType = "step.succeeded"
	// EventStepFailed 某正向步骤失败，执行转入补偿。
	EventStepFailed EventType = "step.failed"
	// EventStepCompensated 某已成功步骤的补偿已确认完成。
	EventStepCompensated EventType = "step.compensated"
	// EventExecutionCompleted 全部正向步骤成功，执行完成。
	EventExecutionCompleted EventType = "execution.completed"
	// EventExecutionFailed 补偿全部完成，执行以失败终态结束。
	EventExecutionFailed EventType = "execution.failed"
	// EventCompensationFailed 某补偿步骤失败，执行冻结为补偿异常终态。
	EventCompensationFailed EventType = "execution.compensation_failed"
)

// EventPayload 是事件的结构化负载，按事件类型取用相关字段。
type EventPayload struct {
	// Step 为相关步骤名（步骤级事件）。
	Step string `json:"step,omitempty"`
	// State 为事件发生后执行所处的状态。
	State ExecutionState `json:"state,omitempty"`
	// Reason 为失败原因（失败类事件）。
	Reason string `json:"reason,omitempty"`
	// Attempts 为该动作已确认的调用次数。
	Attempts int `json:"attempts,omitempty"`
}

// OutboxEvent 是一条待投递的状态变化事件。事件与执行状态在同一事务中
// 原子落盘，调用方通过 Claim/Publish/Mark 完成至少一次投递。
type OutboxEvent struct {
	// ID 为事件唯一 ID，在一次执行内按序号确定性生成。
	ID string
	// BusinessKey 为事件所属业务键。
	BusinessKey string
	// IdempotencyKey 为事件所属执行的外部幂等键。
	IdempotencyKey string
	// Type 为事件类型。
	Type EventType
	// OccurredAt 为状态变化发生时间（UTC）。
	OccurredAt time.Time
	// Payload 为结构化负载（JSON 编码的 EventPayload）。
	Payload json.RawMessage

	// Attempts 为已尝试投递的次数，投递失败时递增。
	Attempts int
	// LastError 为最近一次投递失败原因。
	LastError string

	// ClaimToken 为本次领取令牌，调用方需原样传回 Mark/Fail。
	ClaimToken string
}

// StepRecord 是执行中单个步骤的持久化状态。
type StepRecord struct {
	Name                 string    `json:"name"`
	State                StepState `json:"state"`
	Attempts             int       `json:"attempts"`              // 已确认的正向动作调用次数
	CompensationAttempts int       `json:"compensation_attempts"` // 已确认的补偿动作调用次数
	LastError            string    `json:"last_error,omitempty"`
}

// Execution 是一次执行的持久化状态。Store 实现以此结构为准自行决定
// 序列化与落盘方式，本包不规定磁盘文件格式。
type Execution struct {
	BusinessKey    string         `json:"business_key"`
	IdempotencyKey string         `json:"idempotency_key"`
	SagaName       string         `json:"saga_name"`
	DefinitionHash string         `json:"definition_hash"`
	State          ExecutionState `json:"state"`
	Steps          []StepRecord   `json:"steps"`
	// FailureReason 为终态失败原因；成功时为空。
	FailureReason string `json:"failure_reason,omitempty"`
	// EventSeq 为已追加事件序号，用于生成唯一事件 ID。
	EventSeq int `json:"event_seq"`
	// Version 为乐观并发版本，每次持久化递增。
	Version   int       `json:"version"`
	StartedAt time.Time `json:"started_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Store 原子保存执行状态与待投递 Outbox 事件。实现可以是内存、SQL
// 数据库或任何支持事务与唯一约束的后端；本包只提供内存实现。
type Store interface {
	// Begin 开启一个事务。事务内的变更在 Commit 成功前对外不可见；
	// Commit 后 Rollback 为 no-op。
	Begin(ctx context.Context) (Tx, error)

	// ClaimEvents 领取至多 max 条待投递事件，领取记录在 lease 内独占，
	// 租约到期未确认的事件可被再次领取。按事件发生顺序返回。
	ClaimEvents(ctx context.Context, max int, lease time.Duration) ([]OutboxEvent, error)

	// MarkDelivered 确认事件投递成功，事件不再被领取。claimToken 不匹配
	// （领取已过期或被他人接管）时返回 ErrAlreadyClaimed。
	MarkDelivered(ctx context.Context, eventID, claimToken string) error

	// FailDelivery 报告投递失败：保留原事件、Attempts 加一、记录原因并
	// 立即释放领取，使事件可继续被领取。
	FailDelivery(ctx context.Context, eventID, claimToken string, cause error) error
}

// Tx 是 Store 上的一个事务。
type Tx interface {
	// LoadExecution 按执行身份加载。不存在返回 ErrExecutionNotFound。
	LoadExecution(businessKey, idempotencyKey string) (*Execution, error)

	// CreateExecution 登记新执行，同时建立业务键与定义指纹的绑定：
	//   - 执行身份已存在：返回 ErrExecutionAlreadyExists；
	//   - 业务键已绑定不同指纹：返回 ErrDefinitionConflict。
	// 变更在 Commit 时生效。
	CreateExecution(ex *Execution) error

	// SaveExecution 保存执行当前状态，并在同一原子单元内追加 events。
	// 若记录版本已被其他事务推进，返回 ErrConcurrentUpdate。
	SaveExecution(ex *Execution, events []OutboxEvent) error

	// Commit 原子提交本事务全部变更。
	Commit() error

	// Rollback 放弃本事务。
	Rollback()
}
