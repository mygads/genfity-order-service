package handlers

import "go.uber.org/zap"

func (h *Handler) logBillingEvent(action string, fields ...zap.Field) {
	if h == nil || h.Logger == nil {
		return
	}
	payload := append([]zap.Field{zap.String("action", action)}, fields...)
	h.Logger.Info("billing telemetry", payload...)
}
