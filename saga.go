// Package txsaga 提供可嵌入应用的 Saga 编排能力：带补偿的顺序步骤执行、
// 基于业务键与外部幂等键的执行身份去重，以及与状态原子共写的 Outbox 事件。
//
// 本包只依赖 Go 标准库，不规定任何磁盘文件格式；状态持久化由 Store 接口承载。
package txsaga

import (
	"context"
	"errors"
	"time"
)

// 终态状态：执行一旦进入以下任一状态即固定，后续同一执行身份的调用直接返回既有结果。
const (
	// StatusCompleted 所有正向步骤均已确认成功。
	StatusCompleted = "completed"
	// StatusFailed 某正向步骤失败，且此前确认成功步骤的补偿均已成功。
	StatusFailed = "failed"
	// StatusCompensationFailed 正向失败后，补偿过程中发生错误，结果固定在此状态。
	StatusCompensationFailed = "compensation_failed"
)

// 中间状态：执行尚未到达终态。
const (
	// StatusRunning 至少一个步骤已确认成功，仍有步骤待执行。
	StatusRunning = "running"
	// StatusCompensating 正向步骤已失败，正在逆序补偿此前确认成功的步骤。
	StatusCompensating = "compensating"
)

// 步骤处理结果。
const (
	// StepResultPending 步骤尚未被执行到（仅出现在 Result 中，持久状态以空串表示）。
	StepResultPending = "pending"
	// StepResultSucceeded 正向动作已确认成功。
	StepResultSucceeded = "succeeded"
	// StepResultFailed 正向动作返回错误（该步骤未确认成功，不会被补偿）。
	StepResultFailed = "failed"
	// StepResultCompensated 补偿动作已确认成功。
	StepResultCompensated = "compensated"
	// StepResultCompensationFailed 补偿动作返回错误（补偿步骤未确认，允许重试）。
	StepResultCompensationFailed = "compensation_failed"
)

// Outbox 事件类型，随状态变化追加。
const (
	// EventExecutionStarted 一次执行开始（首个步骤执行前）。
	EventExecutionStarted = "execution_started"
	// EventStepSucceeded 某正向步骤已提交成功状态。
	EventStepSucceeded = "step_succeeded"
	// EventStepFailed 某正向步骤失败，执行转入补偿。
	EventStepFailed = "step_failed"
	// EventStepCompensated 某步骤补偿已提交。
	EventStepCompensated = "step_compensated"
	// EventStepCompensationFailed 某步骤补偿失败。
	EventStepCompensationFailed = "step_compensation_failed"
	// EventExecutionCompleted 全部正向步骤完成，执行到达 completed 终态。
	EventExecutionCompleted = "execution_completed"
	// EventExecutionFailed 补偿全部完成，执行到达 failed 终态。
	EventExecutionFailed = "execution_failed"
	// EventCompensationFailed 补偿异常，执行到达 compensation_failed 终态。
	EventCompensationFailed = "compensation_failed"
)

// 哨兵错误，调用方可用 errors.Is 判定。
var (
	// ErrInvalidDefinition 定义非法（名称为空、步骤为空、步骤名为空、
	// 缺少正向动作、前置步骤未知/自依赖/重复/成环）或执行请求缺少外部幂等键。
	ErrInvalidDefinition = errors.New("txsaga: invalid saga definition")
	// ErrExecutionNotFound 未知执行身份：该业务键下不存在由给定 Saga 定义
	// 与外部幂等键标识的执行。
	ErrExecutionNotFound = errors.New("txsaga: execution not found")
	// ErrDefinitionConflict 相同业务键已绑定不同的 Saga 定义。
	// 同一业务键上的所有执行必须共用同一份定义。
	ErrDefinitionConflict = errors.New("txsaga: saga definition conflict for business key")
)

// ActionFunc 是一个步骤的正向幂等动作。
//
// 动作必须遵守幂等契约：同一步骤在进程崩溃、调用超时后可能被再次调用，
// 重复调用不得产生重复的业务效果。动作返回 nil 即视为该步骤确认成功，
// 之后引擎先提交成功状态，再执行下一步。返回由 Permanent 包装的错误
// 表示永久失败：重试立即结束，按预算耗尽的失败口径提交；普通错误按
// ActionRetry 预算重试。
type ActionFunc func(ctx context.Context, exec ExecutionView) error

// CompensationFunc 是已确认成功步骤的补偿幂等动作。
//
// 补偿按步骤成功顺序的相反方向逐个执行。补偿同样可能被重复调用，
// 实现必须自身幂等。返回 nil 表示补偿确认成功；返回错误时执行固定在
// compensation_failed 终态，未确认的补偿允许在后续调用中重试（终态结果不变）。
// 返回由 Permanent 包装的永久失败时立即结束补偿重试：即使 CompensateRetry
// 预算尚有剩余也只调用一次、不再等待，第一次确认失败即把该步记为
// compensation_failed 并把执行固定为 compensation_failed 终态。
type CompensationFunc func(ctx context.Context, exec ExecutionView) error

// RetryPolicy 配置一个动作的有限重试预算。
//
// 零值即默认语义：动作只调用一次（MaxAttempts 视为 1），失败后不等待。
// 每次动作返回非 nil 错误且仍有剩余次数时，引擎等待 RetryWait 后再次调用
// 同一动作；等待与动作本身都响应传入的 context。MaxAttempts 是包含首次
// 调用在内的最大总调用次数；负数次数或负等待时长属于非法定义。
//
// 动作返回由 Permanent 包装的永久失败时重试立即结束：即使 MaxAttempts
// 尚有剩余也只按实际调用次数计、不再等待，按与预算耗尽相同的口径提交该步
// 失败。普通错误仍按上述预算重试。
type RetryPolicy struct {
	// MaxAttempts 最大总调用次数（含首次）。零值按 1 处理；负数为非法定义。
	MaxAttempts int
	// RetryWait 每次失败后、再次调用前的等待时长。零值表示立即重试；
	// 负数为非法定义。
	RetryWait time.Duration
}

// Step 定义 Saga 中的一个正向步骤及其补偿。
type Step struct {
	// Name 步骤在 Saga 内唯一的名称。
	Name string
	// DependsOn 本步骤的前置步骤名称列表：全部前置步骤确认成功后本步骤
	// 才可启动。为空表示无前置，可与其它无前置步骤同时启动。
	// 一旦定义中任一步骤声明了前置，整个定义进入依赖图模式：调度只依据
	// 各步骤声明的前置关系，无前置的步骤并发执行；所有步骤都未声明前置时
	// 仍按 Steps 声明顺序逐个执行（顺序模式，公开行为与此前版本一致）。
	// 前置名称必须指向定义内已存在的其它步骤：未知前置、自依赖、重复前置
	// 或前置关系成环均属非法定义。
	DependsOn []string
	// Action 正向幂等动作，必填。
	Action ActionFunc
	// Compensate 反向补偿动作；无需补偿的步骤可留空，留空视为补偿成功。
	Compensate CompensationFunc
	// ActionRetry 正向动作的可选有限重试配置；零值表示只调用一次、失败即转补偿。
	ActionRetry RetryPolicy
	// CompensateRetry 补偿动作的可选有限重试配置；零值表示只调用一次、
	// 失败即固定为 compensation_failed。
	CompensateRetry RetryPolicy
}

// retryBudget 是归一化后的重试预算。
type retryBudget struct {
	maxAttempts int
	wait        time.Duration
}

func normalizeRetry(p RetryPolicy) retryBudget {
	n := p.MaxAttempts
	if n < 1 {
		n = 1
	}
	return retryBudget{maxAttempts: n, wait: p.RetryWait}
}

// Definition 是一个 Saga 的不可变定义。
type Definition struct {
	// Name Saga 定义名称；同时参与执行身份判定。
	Name string
	// Version 定义版本，参与定义指纹以区分同名但内容不同的定义。
	Version string
	// Steps Saga 的全部步骤，至少一个。未声明任何前置（DependsOn 均为空）时
	// 按声明顺序逐个执行；任一步骤声明前置后进入依赖图模式，按前置关系调度，
	// 无前置的步骤并发执行。Result.Steps 始终按本切片声明顺序返回。
	Steps []Step
}

// ExecutionRequest 发起一次 Saga 执行。
type ExecutionRequest struct {
	// BusinessKey 调用方的唯一业务键（如订单号），必填。
	BusinessKey string
	// IdempotencyKey 外部幂等键，必填；与业务键、Saga 定义共同确定执行身份。
	IdempotencyKey string
	// Payload 透传给动作的结构化业务输入，可为 nil。
	Payload any
}

// StepOutcome 是单个步骤在一次执行中的处理结果。
type StepOutcome struct {
	// Name 步骤名。
	Name string `json:"name"`
	// Result 取值为 StepResult* 常量。
	Result string `json:"result"`
	// Attempts 正向动作在本次确认阶段的实际调用次数（含预算内重试）；
	// 同身份重试已确认步骤时保持不变，大于 1 说明该步曾在预算内重试，
	// 或未确认步骤被同身份调用重新进入（动作须幂等）。
	Attempts int `json:"attempts"`
	// CompensationAttempts 补偿动作在本次确认阶段的实际调用次数（含预算内
	// 重试）；已确认补偿后保持不变。
	CompensationAttempts int `json:"compensation_attempts,omitempty"`
	// Error 最近一次失败原因；成功时为空。
	Error string `json:"error,omitempty"`
	// StartedAt 最近一次开始执行正向动作的时间，未执行过为零值。
	StartedAt time.Time `json:"started_at,omitempty"`
	// FinishedAt 最近一次正向动作返回的时间，未完成则为零值。
	FinishedAt time.Time `json:"finished_at,omitempty"`
}

// Result 是一次执行在某次调用后返回的确定状态快照。
type Result struct {
	// BusinessKey 业务键。
	BusinessKey string `json:"business_key"`
	// SagaName Saga 定义名称。
	SagaName string `json:"saga_name"`
	// IdempotencyKey 外部幂等键。
	IdempotencyKey string `json:"idempotency_key"`
	// Status 当前状态，取值为 Status* 常量；终态之一表示已确定。
	Status string `json:"status"`
	// FailureReason 正向失败原因；仅在 failed/compensation_failed 时非空。
	FailureReason string `json:"failure_reason,omitempty"`
	// CompensationError 最近一次补偿错误；仅 compensation_failed 时非空。
	CompensationError string `json:"compensation_error,omitempty"`
	// FailedStep 正向失败的步骤名；未失败为空。
	FailedStep string `json:"failed_step,omitempty"`
	// FailedCompensationStep 补偿失败的步骤名；未发生为空。
	FailedCompensationStep string `json:"failed_compensation_step,omitempty"`
	// Steps 各步骤处理结果，顺序与定义中的正向顺序一致。
	Steps []StepOutcome `json:"steps"`
	// Terminal 是否为固定终态。终态结果在后续调用中保持不变。
	Terminal bool `json:"terminal"`
}

// ExecutionView 是传给动作与补偿的执行只读视图。
type ExecutionView interface {
	// BusinessKey 返回本次执行的业务键。
	BusinessKey() string
	// SagaName 返回 Saga 定义名称。
	SagaName() string
	// IdempotencyKey 返回外部幂等键。
	IdempotencyKey() string
	// Payload 返回发起执行时透传的结构化负载。
	Payload() any
	// SucceededSteps 返回截至当前已确认成功的步骤名（按确认成功的先后
	// 顺序；顺序模式下与声明顺序一致）。
	SucceededSteps() []string
}

// Event 是随状态变化追加的 Outbox 事件。
type Event struct {
	// ID 事件唯一 ID，由存储在追加时分配。
	ID string
	// BusinessKey 业务键。
	BusinessKey string
	// Type 事件类型，取值为 Event* 常量。
	Type string
	// OccurredAt 事件发生时间（状态提交时刻）。
	OccurredAt time.Time
	// Payload 结构化负载，使用 encoding/json 可直接序列化。
	Payload any

	// 投递元数据，由 Claim/Ack/Nack 维护，不参与业务语义。
	deliveries    int
	lastAttemptAt time.Time
}

// Deliveries 返回该事件已进入领取/投递流程的次数（含首次）。
// 发送失败后保留事件并增加投递次数，调用方可据此判断是否重复投递。
func (e *Event) Deliveries() int { return e.deliveries }

// LastAttemptAt 返回最近一次领取时间；从未被领取为零值。
func (e *Event) LastAttemptAt() time.Time { return e.lastAttemptAt }

// WithDeliveryMeta 返回事件的一份副本，并写入投递元数据（投递次数与最近
// 领取时间）。本方法供 Store 的持久化实现使用：从磁盘等外部载体重建事件
// 时恢复 Deliveries 与 LastAttemptAt，使领取/退回/租约语义在重开存储后
// 保持连续。事件的身份字段（ID、类型、发生时间、负载等）保持不变。
// 内存实现直接维护活动对象，无需经过本方法；业务调用方一般不应使用。
func (e Event) WithDeliveryMeta(deliveries int, lastAttemptAt time.Time) Event {
	e.deliveries = deliveries
	e.lastAttemptAt = lastAttemptAt
	return e
}

// EventPayload 是事件的结构化负载。
type EventPayload struct {
	SagaName       string `json:"saga_name"`
	IdempotencyKey string `json:"idempotency_key"`
	Step           string `json:"step,omitempty"`
	Status         string `json:"status,omitempty"`
	Reason         string `json:"reason,omitempty"`
	Attempt        int    `json:"attempt,omitempty"`
	// SucceededSteps 事件发生时已确认成功的步骤顺序快照。
	SucceededSteps []string `json:"succeeded_steps,omitempty"`
}
