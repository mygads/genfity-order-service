package voucher

import "net/http"

type ErrorCode string

const (
	ErrVoucherNotFound            ErrorCode = "VOUCHER_NOT_FOUND"
	ErrVoucherTemplateRequired    ErrorCode = "VOUCHER_TEMPLATE_REQUIRED"
	ErrVoucherInactive            ErrorCode = "VOUCHER_INACTIVE"
	ErrVoucherNotApplicable       ErrorCode = "VOUCHER_NOT_APPLICABLE"
	ErrVoucherOrderTypeNotAllowed ErrorCode = "VOUCHER_ORDER_TYPE_NOT_ALLOWED"
	ErrVoucherNotActiveYet        ErrorCode = "VOUCHER_NOT_ACTIVE_YET"
	ErrVoucherExpired             ErrorCode = "VOUCHER_EXPIRED"
	ErrVoucherNotAvailableToday   ErrorCode = "VOUCHER_NOT_AVAILABLE_TODAY"
	ErrVoucherNotAvailableNow     ErrorCode = "VOUCHER_NOT_AVAILABLE_NOW"
	ErrVoucherScheduleInvalid     ErrorCode = "VOUCHER_SCHEDULE_INVALID"
	ErrVoucherRequiresLogin       ErrorCode = "VOUCHER_REQUIRES_LOGIN"
	ErrVoucherMinOrderNotMet      ErrorCode = "VOUCHER_MIN_ORDER_NOT_MET"
	ErrVoucherUsageLimitReached   ErrorCode = "VOUCHER_USAGE_LIMIT_REACHED"
	ErrVoucherDiscountCapReached  ErrorCode = "VOUCHER_DISCOUNT_CAP_REACHED"
	ErrVoucherNotApplicableItems  ErrorCode = "VOUCHER_NOT_APPLICABLE_ITEMS"
	ErrVoucherAlreadyApplied      ErrorCode = "VOUCHER_ALREADY_APPLIED"
	ErrVoucherCannotStackManual   ErrorCode = "VOUCHER_CANNOT_STACK_MANUAL"
	ErrVoucherDiscountZero        ErrorCode = "VOUCHER_DISCOUNT_ZERO"
)

type Error struct {
	Code       ErrorCode
	Message    string
	StatusCode int
	Details    map[string]any
}

func (e *Error) Error() string {
	return e.Message
}

func newError(code ErrorCode, message string, status int, details map[string]any) *Error {
	return &Error{Code: code, Message: message, StatusCode: status, Details: details}
}

func ValidationError(code ErrorCode, message string, details map[string]any) *Error {
	return newError(code, message, http.StatusBadRequest, details)
}
