package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	EventsExchange           = "genfity.events"
	EventsQueue              = "genfity.notifications"
	NotificationJobsExchange = "genfity.notification_jobs"
	NotificationJobsQueue    = "genfity.notification_jobs.process"
	NotificationJobsDLQ      = "genfity.notification_jobs.dlq"
	NotificationJobsRK       = "process"
	NotificationJobsDeadRK   = "dead"

	CompletedEmailExchange = "genfity.completed_email"
	CompletedEmailQueue    = "genfity.completed_email.send"
	CompletedEmailDLQ      = "genfity.completed_email.dlq"
	CompletedEmailRK       = "send"
	CompletedEmailDeadRK   = "dead"
)

type orderStatusUpdatedEvent struct {
	Type      string     `json:"type"`
	OrderID   int64      `json:"orderId"`
	Merchant  int64      `json:"merchantId"`
	Status    string     `json:"status"`
	UpdatedAt *time.Time `json:"updatedAt"`
}

func EnsureNotificationJobsTopology(ctx context.Context, qc *Client) error {
	if qc == nil {
		return nil
	}

	if err := qc.EnsureExchangeKind(NotificationJobsExchange, "direct"); err != nil {
		return err
	}

	if _, err := qc.EnsureQueue(NotificationJobsDLQ); err != nil {
		return err
	}
	if err := qc.BindQueue(NotificationJobsDLQ, NotificationJobsExchange, NotificationJobsDeadRK); err != nil {
		return err
	}

	_, err := qc.EnsureQueueWithArgs(NotificationJobsQueue, amqp.Table{
		"x-dead-letter-exchange":    NotificationJobsExchange,
		"x-dead-letter-routing-key": NotificationJobsDeadRK,
	})
	if err != nil {
		return err
	}
	return qc.BindQueue(NotificationJobsQueue, NotificationJobsExchange, NotificationJobsRK)
}

func EnsureCompletedEmailTopology(ctx context.Context, qc *Client) error {
	if qc == nil {
		return nil
	}

	if err := qc.EnsureExchangeKind(CompletedEmailExchange, "direct"); err != nil {
		return err
	}

	if _, err := qc.EnsureQueue(CompletedEmailDLQ); err != nil {
		return err
	}
	if err := qc.BindQueue(CompletedEmailDLQ, CompletedEmailExchange, CompletedEmailDeadRK); err != nil {
		return err
	}

	_, err := qc.EnsureQueueWithArgs(CompletedEmailQueue, amqp.Table{
		"x-dead-letter-exchange":    CompletedEmailExchange,
		"x-dead-letter-routing-key": CompletedEmailDeadRK,
	})
	if err != nil {
		return err
	}
	return qc.BindQueue(CompletedEmailQueue, CompletedEmailExchange, CompletedEmailRK)
}

func ProcessEventToJobs(ctx context.Context, db *pgxpool.Pool, qc *Client, body []byte) error {
	if db == nil || qc == nil {
		return nil
	}

	var evt orderStatusUpdatedEvent
	if err := json.Unmarshal(body, &evt); err != nil {
		return err
	}
	if strings.TrimSpace(evt.Type) == "" {
		// unknown envelope
		return nil
	}
	if evt.Type != "order.status.updated" {
		// ignore
		return nil
	}

	// Load data required by the Next worker job schema.
	var (
		orderNumber   string
		orderType     string
		merchantCode  string
		merchantName  string
		customerID    *int64
		customerEmail *string
	)

	query := `
		select o.order_number, o.order_type, m.code, m.name, o.customer_id, c.email
		from orders o
		join merchants m on m.id = o.merchant_id
		left join customers c on c.id = o.customer_id
		where o.id = $1 and o.merchant_id = $2
	`
	if err := db.QueryRow(ctx, query, evt.OrderID, evt.Merchant).Scan(&orderNumber, &orderType, &merchantCode, &merchantName, &customerID, &customerEmail); err != nil {
		return err
	}

	pushStatus := mapOrderStatusToPushStatus(evt.Status)
	if pushStatus != "" {
		payload := map[string]any{
			"kind":         "push.customer_order_status",
			"orderNumber":  orderNumber,
			"status":       pushStatus,
			"merchantName": merchantName,
			"merchantCode": merchantCode,
			"customerId":   nil,
			"orderType":    orderType,
		}
		if customerID != nil {
			payload["customerId"] = fmt.Sprintf("%d", *customerID)
		}

		job := map[string]any{
			"kind":      "push.customer_order_status",
			"payload":   payload,
			"createdAt": time.Now().UTC().Format(time.RFC3339),
			"attempt":   1,
		}

		if err := qc.PublishJSON(ctx, NotificationJobsExchange, NotificationJobsRK, job); err != nil {
			return err
		}
	}

	if strings.EqualFold(evt.Status, "COMPLETED") {
		// Enqueue completed email job; fee can be computed by the Next worker when 0.
		toEmail := ""
		if customerEmail != nil {
			toEmail = strings.TrimSpace(*customerEmail)
		}
		if toEmail != "" {
			job := map[string]any{
				"orderId":           fmt.Sprintf("%d", evt.OrderID),
				"merchantId":        fmt.Sprintf("%d", evt.Merchant),
				"customerEmail":     toEmail,
				"orderNumber":       orderNumber,
				"completedEmailFee": 0,
				"createdAt":         time.Now().UTC().Format(time.RFC3339),
				"attempt":           1,
			}
			if err := qc.PublishJSON(ctx, CompletedEmailExchange, CompletedEmailRK, job); err != nil {
				return err
			}
		}
	}

	return nil
}

func mapOrderStatusToPushStatus(status string) string {
	upper := strings.ToUpper(strings.TrimSpace(status))
	switch upper {
	case "PENDING", "ACCEPTED", "IN_PROGRESS":
		return "PREPARING"
	case "READY":
		return "READY"
	case "COMPLETED":
		return "COMPLETED"
	case "CANCELLED":
		return "CANCELLED"
	default:
		return ""
	}
}
