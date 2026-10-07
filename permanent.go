package txsaga

import "errors"

// permanentError 标记业务上已经明确不可能成功的动作失败。被标记的失败立即
// 结束当前动作的重试阶段：即使 RetryPolicy.MaxAttempts 尚有剩余也不再等待、
// 不再调用，按预算耗尽的失败口径提交。
type permanentError struct {
	err error
}

// Permanent 把 err 包装为永久失败，表明动作在业务上已明确不可能成功，
// 重试不会改变结果。
//
// 被包装的错误在重试预算中立即终止当前动作的重试阶段（不再等待、不再
// 调用），其余提交口径与普通错误预算耗尽一致：正向动作按失败提交并转入
// 逆序补偿，补偿动作按补偿失败提交并把执行固定为 compensation_failed。
// 包装保留错误链：errors.Is(err, target) 继续按底层错误判定，Unwrap
// 返回底层错误。err 为 nil 时返回 nil。
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err: err}
}

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

// IsPermanent 报告 err（或其错误链上的任一错误）是否由 Permanent 包装。
// nil 返回 false。
func IsPermanent(err error) bool {
	var pe permanentError
	return errors.As(err, &pe)
}
