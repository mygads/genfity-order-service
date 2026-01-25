package queue

import (
	"context"
	"errors"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type HandlerFunc func(ctx context.Context, body []byte) error

func (c *Client) ConsumeWithRetry(queue string, handler HandlerFunc, maxRetries int, retryDelay time.Duration) error {
	msgs, err := c.ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	for msg := range msgs {
		ctx := context.Background()
		err := handler(ctx, msg.Body)
		if err == nil {
			_ = msg.Ack(false)
			continue
		}

		retryCount := getRetryCount(msg.Headers)
		if retryCount >= maxRetries {
			_ = msg.Nack(false, false)
			continue
		}

		retryCount++
		headers := msg.Headers
		if headers == nil {
			headers = amqp.Table{}
		}
		headers["x-retry-count"] = retryCount

		time.Sleep(retryDelay)
		_ = c.ch.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{
			ContentType: msg.ContentType,
			Body:        msg.Body,
			Headers:     headers,
			Timestamp:   time.Now(),
		})
		_ = msg.Ack(false)
	}

	return errors.New("consumer closed")
}

func getRetryCount(headers amqp.Table) int {
	if headers == nil {
		return 0
	}
	if v, ok := headers["x-retry-count"]; ok {
		switch t := v.(type) {
		case int32:
			return int(t)
		case int64:
			return int(t)
		case int:
			return t
		}
	}
	return 0
}
