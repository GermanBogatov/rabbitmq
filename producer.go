package rabbitmq

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ProducerConfig struct {
	BaseConfig
}

type rabbitMQProducer struct {
	*rabbitMQBase
}

func NewRabbitMQProducer(cfg ProducerConfig) (Producer, error) {
	producer := &rabbitMQProducer{
		rabbitMQBase: &rabbitMQBase{
			done: make(chan bool),
		},
	}

	addr := fmt.Sprintf("amqp://%s:%s@%s:%s/%s", cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Vhost)
	err := producer.connect(addr)
	if err != nil {
		return nil, err
	}

	go producer.handleReconnect(addr)

	return producer, nil
}

/*
Publish sends a Publishing from the client to an exchange on the server.

When you want a single message to be delivered to a single queue, you can
publish to the default exchange with the routingKey of the queue name.  This is
because every declared queue gets an implicit route to the default exchange.

Since publishings are asynchronous, any undeliverable message will get returned
by the server.  Add a listener with Channel.NotifyReturn to handle any
undeliverable message when calling publish with either the mandatory or
immediate parameters as true.

Publishings can be undeliverable when the mandatory flag is true and no queue is
bound that matches the routing key, or when the immediate flag is true and no
consumer on the matched queue is ready to accept the delivery.

This can return an error when the channel, connection or socket is closed.  The
error or lack of an error does not indicate whether the server has received this
publishing.

It is possible for publishing to not reach the broker if the underlying socket
is shut down without pending publishing packets being flushed from the kernel
buffers.  The easy way of making it probable that all publishings reach the
server is to always call Connection.Close before terminating your publishing
application.  The way to ensure that all publishings reach the server is to add
a listener to Channel.NotifyPublish and put the channel in confirm mode with
Channel.Confirm.  Publishing delivery tags and their corresponding
confirmations start at 1.  Exit when all publishings are confirmed.

When Publish does not return an error and the channel is in confirm mode, the
internal counter for DeliveryTags with the first confirmation starts at 1.
*/
func (r *rabbitMQProducer) Publish(ctx context.Context, exchange, routingKey string, body []byte) error {
	if !r.Connected() {
		return errNotConnected
	}

	err := r.ch.PublishWithContext(
		ctx,
		exchange,
		routingKey,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         body,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish message due %v", err)
	}

	return nil
}

func (r *rabbitMQProducer) Close() error {
	if err := r.close(); err != nil {
		return err
	}

	return nil
}
