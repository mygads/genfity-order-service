package handlers

import (
	"context"

	"genfity-order-services/internal/services"
)

type subscriptionState = services.SubscriptionState

func (h *Handler) fetchSubscriptionState(ctx context.Context, merchantID int64) (subscriptionState, error) {
	return services.GetSubscriptionState(ctx, h.DB, merchantID)
}
