package queue

import (
	"context"
	"encoding/json"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Client struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

func New(url string) (*Client, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}
	return &Client{conn: conn, ch: ch}, nil
}

func (c *Client) Close() error {
	if c.ch != nil {
		_ = c.ch.Close()
	}
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *Client) EnsureExchange(name string) error {
	return c.EnsureExchangeKind(name, "topic")
}

func (c *Client) EnsureExchangeKind(name string, kind string) error {
	if kind == "" {
		kind = "topic"
	}
	return c.ch.ExchangeDeclare(
		name,
		kind,
		true,
		false,
		false,
		false,
		nil,
	)
}

func (c *Client) EnsureQueue(name string) (amqp.Queue, error) {
	return c.EnsureQueueWithArgs(name, nil)
}

func (c *Client) EnsureQueueWithArgs(name string, args amqp.Table) (amqp.Queue, error) {
	return c.ch.QueueDeclare(
		name,
		true,
		false,
		false,
		false,
		args,
	)
}

func (c *Client) BindQueue(queueName, exchange, routingKey string) error {
	return c.ch.QueueBind(queueName, routingKey, exchange, false, nil)
}

func (c *Client) PublishJSON(ctx context.Context, exchange, routingKey string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return c.ch.PublishWithContext(ctx, exchange, routingKey, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
		Timestamp:   time.Now(),
	})
}

func (c *Client) Get(queue string) (amqp.Delivery, bool, error) {
	return c.ch.Get(queue, false)
}
