// Package saga 提供可嵌入应用的 Saga 编排引擎：顺序正向步骤、失败逆序补偿、
// 执行幂等以及基于 Outbox 的事件投递。
//
// 执行身份由业务键（BusinessKey）、Saga 定义（Definition）与外部幂等键
// （IdempotencyKey）共同确定。调用方为每个正向步骤提供遵守幂等契约的动作，
// 为可能成功的步骤提供补偿动作。引擎只依赖 Go 标准库；状态如何落盘由
// Store 实现决定，本包不规定任何磁盘文件格式。
package saga

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
)

// Sentinel errors returned by the engine and stores.
var (
	// ErrInvalidDefinition 表示 Saga 定义非法，或执行请求缺失幂等键/业务键。
	ErrInvalidDefinition = errors.New("saga: invalid definition")

	// ErrDefinitionConflict 表示相同业务键已绑定另一个 Saga 定义。
	ErrDefinitionConflict = errors.New("saga: definition conflict for business key")

	// ErrExecutionNotFound 表示执行身份（业务键 + 幂等键）不存在。
	ErrExecutionNotFound = errors.New("saga: execution not found")

	// ErrAlreadyClaimed 表示 Outbox 事件已被其他领取者占用（乐观并发）。
	ErrAlreadyClaimed = errors.New("saga: outbox event already claimed")

	// ErrConcurrentUpdate 表示执行记录已被其他事务推进（乐观锁冲突）。
	ErrConcurrentUpdate = errors.New("saga: concurrent execution update")

	// ErrExecutionAlreadyExists 表示执行身份在创建时已存在（并发重试）。
	ErrExecutionAlreadyExists = errors.New("saga: execution already exists")

	// ErrEventNotFound 表示 Outbox 事件不存在。
	ErrEventNotFound = errors.New("saga: outbox event not found")
)

// ExecutionState 是一次执行在某时刻的确定状态。
type ExecutionState string

const (
	// StateRunning 正向步骤仍在推进，尚未进入终态。
	StateRunning ExecutionState = "running"
	// StateCompleted 所有正向步骤均已确认成功。
	StateCompleted ExecutionState = "completed"
	// StateFailed 某正向步骤失败，且此前成功步骤均已补偿完成。
	StateFailed ExecutionState = "failed"
	// StateCompensationFailed 补偿过程中出现错误，终态被冻结。
	StateCompensationFailed ExecutionState = "compensation_failed"
)

// Terminal 报告状态是否为终态。终态结果一经形成不再改变。
func (s ExecutionState) Terminal() bool {
	switch s {
	case StateCompleted, StateFailed, StateCompensationFailed:
		return true
	default:
		return false
	}
}

// StepState 是单个步骤的处理状态。
type StepState string

const (
	// StepPending 步骤尚未开始。
	StepPending StepState = "pending"
	// StepSucceeded 正向动作已成功并确认提交。
	StepSucceeded StepState = "succeeded"
	// StepFailed 正向动作返回错误（该步未确认成功，不会被补偿）。
	StepFailed StepState = "failed"
	// StepCompensated 该步的补偿动作已成功确认。
	StepCompensated StepState = "compensated"
	// StepCompensationFailed 该步的补偿动作返回错误。
	StepCompensationFailed StepState = "compensation_failed"
)

// Action 执行一个正向步骤。
//
// 动作必须遵守幂等契约：网络抖动、进程重启等未确认场景下引擎可能以相同
// 输入再次调用同一动作，动作自身需保证重复调用与单次调用等价。
type Action func(ctx context.Context) error

// Compensation 执行一个已成功步骤的补偿动作，同样需满足幂等契约：
// 未确认结果时引擎可能再次调用。无需补偿的步骤可在 Step 中留空。
type Compensation func(ctx context.Context) error

// Step 定义 Saga 中的一个正向步骤及其补偿。
type Step struct {
	// Name 是步骤在其所属 Definition 内的唯一名称。
	Name string
	// Do 为正向动作，必填。
	Do Action
	// Compensate 为补偿动作；无需补偿的步骤可留空，补偿时视为成功。
	Compensate Compensation
}

// Definition 是一个具名 Saga 的步骤编排定义。
type Definition struct {
	// Name 为 Saga 类型名，用于诊断，不参与执行身份判定。
	Name string
	// Steps 按正向执行顺序排列；补偿按相反顺序执行。
	Steps []Step
}

// fingerprint 返回定义内容的稳定指纹：相同的步骤名序列产生相同指纹。
// 动作是代码而非数据，不纳入指纹；执行身份由调用方通过 Definition 的
// 步骤结构与名称保证一致性。
func (d Definition) fingerprint() string {
	h := sha256.New()
	h.Write([]byte(d.Name))
	h.Write([]byte{0})
	for _, s := range d.Steps {
		h.Write([]byte(s.Name))
		h.Write([]byte{0})
	}
	sum := h.Sum(nil)
	return hex.EncodeToString(sum[:12])
}

// validate 校验定义与请求的基本不变量。
func (d Definition) validate() error {
	if len(d.Steps) == 0 {
		return ErrInvalidDefinition
	}
	seen := make(map[string]struct{}, len(d.Steps))
	for _, s := range d.Steps {
		if strings.TrimSpace(s.Name) == "" {
			return ErrInvalidDefinition
		}
		if s.Do == nil {
			return ErrInvalidDefinition
		}
		if _, dup := seen[s.Name]; dup {
			return ErrInvalidDefinition
		}
		seen[s.Name] = struct{}{}
	}
	return nil
}

// Request 是一次 Saga 执行请求。
type Request struct {
	// BusinessKey 是调用方的唯一业务键，同一业务键绑定同一 Saga 定义。
	BusinessKey string
	// IdempotencyKey 是本次执行的外部幂等键；与 BusinessKey、定义共同
	// 确定执行身份。缺失时返回 ErrInvalidDefinition。
	IdempotencyKey string
}

func (r Request) validate() error {
	if strings.TrimSpace(r.BusinessKey) == "" {
		return ErrInvalidDefinition
	}
	if strings.TrimSpace(r.IdempotencyKey) == "" {
		return ErrInvalidDefinition
	}
	return nil
}

// StepResult 是单个步骤的处理结果，按步骤定义顺序返回。
type StepResult struct {
	// Name 为步骤名。
	Name string
	// State 为该步当前状态。
	State StepState
	// Attempts 为正向动作已确认的调用次数（结果已持久化的次数）。
	// 未确认结果导致的重新调用无法在崩溃后由引擎计数，动作须自身幂等。
	Attempts int
	// CompensationAttempts 为补偿动作已确认的调用次数。
	CompensationAttempts int
	// Error 为最近一次失败的错误描述；成功时为空。
	Error string
}

// Result 是一次执行的确定结果。终态结果在后续调用中原样返回。
type Result struct {
	// BusinessKey 为业务键。
	BusinessKey string
	// IdempotencyKey 为外部幂等键。
	IdempotencyKey string
	// Saga 为 Saga 定义名。
	Saga string
	// State 为执行状态。
	State ExecutionState
	// FailureReason 为导致失败/补偿失败的原因；成功时为空。
	FailureReason string
	// Steps 为各步骤的处理结果，顺序与定义一致。
	Steps []StepResult
}
