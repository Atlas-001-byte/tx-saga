package txsaga

import "errors"

// permanentError 是被 Permanent 标记的永久失败：业务上已明确不可能成功，
// 继续按有限预算重试没有意义。错误文本与 Unwrap 均直达底层错误。
type permanentError struct{ err error }

func (p permanentError) Error() string { return p.err.Error() }
func (p permanentError) Unwrap() error { return p.err }

// Permanent 把 err 标记为永久失败。
//
// 正向动作或补偿动作返回被 Permanent 标记的错误（可在错误链任意层）时，
// 引擎立即结束当前动作的重试阶段：即使重试预算（MaxAttempts）尚有剩余，
// 也不再等待、不再调用，按预算耗尽失败的同一口径提交结果与事件——
// 正向永久失败即转入补偿，补偿永久失败即固定为 compensation_failed。
//
// 返回的错误保留 errors.Is/errors.As 与 Unwrap 语义：Unwrap 得到原错误，
// errors.Is(Permanent(err), target) 与 errors.Is(err, target) 等价；
// 错误文本与原错误一致，结果中的失败原因保留底层错误文本。
// err 为 nil 时返回 nil。
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err: err}
}

// IsPermanent 报告 err 是否被 Permanent 标记为永久失败（可在错误链任意层，
// 例如 fmt.Errorf("...: %w", Permanent(err))）。普通错误与 nil 均返回 false。
func IsPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}
