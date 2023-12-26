package rabbitmq

import (
	"context"
	"io"
)

type MessageQueue interface {
	io.Closer
	DeclareQueue(name string, durable, autoDelete, exclusive, noWait bool, args map[string]interface{}) error
	DeclareExchange(name, kind string, durable, autoDelete, internal, noWait bool, args map[string]interface{}) error
	DeclareQueueBind(name, key, exchange string, noWait bool, args map[string]interface{}) error
}

type Producer interface {
	MessageQueue
	Publish(ctx context.Context, exchange, routingKey string, body []byte) error
}

type Consumer interface {
	MessageQueue
	Consume(queue string, autoAck, exclusive, noLocal, noWait bool, args map[string]interface{}) (<-chan Message, error)
	Ack(id uint64, multiple bool) error
	Nack(id uint64, multiple bool, requeue bool) error
	Reject(id uint64, requeue bool) error
}

type Message struct {
	ID   uint64
	Body []byte
}
