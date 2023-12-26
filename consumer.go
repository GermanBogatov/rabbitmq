package rabbitmq

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	logging "gitlab.gid.team/gid-pro/backend/libs/logger/v2"
	"time"
)

type ConsumerConfig struct {
	BaseConfig
	//Name наименование консюмера
	Name string
	// prefetchCount сообщает RabbitMQ, что не следует одновременно передавать больше сообщений потребителю, чем указано
	PrefetchCount int
}

type rabbitMQConsumer struct {
	*rabbitMQBase
	name          string
	reconnectCh   chan bool
	prefetchCount int
}

const (
	consumeDelay = 1 * time.Second
)

func NewRabbitMQConsumer(cfg ConsumerConfig) (Consumer, error) {
	consumer := &rabbitMQConsumer{
		name:          cfg.Name,
		prefetchCount: cfg.PrefetchCount,
		reconnectCh:   make(chan bool),
		rabbitMQBase: &rabbitMQBase{
			done: make(chan bool),
		},
	}

	addr := fmt.Sprintf("amqp://%s:%s@%s:%s/", cfg.Username, cfg.Password, cfg.Host, cfg.Port)
	err := consumer.connect(addr)
	if err != nil {
		return nil, err
	}

	consumer.notifyReconnect(consumer.reconnectCh)
	go consumer.handleReconnect(addr)

	return consumer, nil
}

/*
Consume immediately starts delivering queued messages.

Begin receiving on the returned chan Delivery before any other operation on the
Connection or Channel.

Continues deliveries to the returned chan Delivery until Channel.Cancel,
Connection.Close, Channel.Close, or an AMQP exception occurs.  Consumers must
range over the chan to ensure all deliveries are received.  Unreceived
deliveries will block all methods on the same connection.

All deliveries in AMQP must be acknowledged.  It is expected of the consumer to
call Delivery.Ack after it has successfully processed the delivery.  If the
consumer is cancelled or the channel or connection is closed any unacknowledged
deliveries will be requeued at the end of the same queue.

The consumer is identified by a string that is unique and scoped for all
consumers on this channel.  If you wish to eventually cancel the consumer, use
the same non-empty identifier in Channel.Cancel.  An empty string will cause
the library to generate a unique identity.  The consumer identity will be
included in every Delivery in the ConsumerTag field

When autoAck (also known as noAck) is true, the server will acknowledge
deliveries to this consumer prior to writing the delivery to the network.  When
autoAck is true, the consumer should not call Delivery.Ack. Automatically
acknowledging deliveries means that some deliveries may get lost if the
consumer is unable to process them after the server delivers them.
See http://www.rabbitmq.com/confirms.html for more details.

When exclusive is true, the server will ensure that this is the sole consumer
from this queue. When exclusive is false, the server will fairly distribute
deliveries across multiple consumers.

The noLocal flag is not supported by RabbitMQ.

It's advisable to use separate connections for
Channel.Publish and Channel.Consume so not to have TCP pushback on publishing
affect the ability to consume messages, so this parameter is here mostly for
completeness.

When noWait is true, do not wait for the server to confirm the request and
immediately begin deliveries.  If it is not possible to consume, a channel
exception will be raised and the channel will be closed.

Optional arguments can be provided that have specific semantics for the queue
or server.

Inflight messages, limited by Channel.Qos will be buffered until received from
the returned chan.

When the Channel or Connection is closed, all buffered and inflight messages will
be dropped. RabbitMQ will requeue messages not acknowledged. In other words, dropped
messages in this way won't be lost.

When the consumer tag is cancelled, all inflight messages will be delivered until
the returned chan is closed.
*/
func (r *rabbitMQConsumer) Consume(queue string, autoAck, exclusive, noLocal, noWait bool, args map[string]interface{}) (<-chan Message, error) {
	if !r.Connected() {
		return nil, errNotConnected
	}
	messages, err := r.consume(queue, autoAck, exclusive, noLocal, noWait, args)
	if err != nil {
		return nil, fmt.Errorf("failed to consume messages due %v", err)
	}

	ch := make(chan Message)
	go func() {
		for {
			select {
			case message, ok := <-messages:
				if !ok {
					time.Sleep(consumeDelay)
					continue
				}

				ch <- Message{
					ID:   message.DeliveryTag,
					Body: message.Body,
				}
			case <-r.reconnectCh:
				logging.Info("start to reconsume messages")
				for {
					messages, err = r.consume(queue, autoAck, exclusive, noLocal, noWait, args)
					if err == nil {
						break
					}

					logging.Errorf("failed to reconsume messages due %v", err)
				}
			case <-r.done:
				close(ch)
				return
			}
		}
	}()

	return ch, nil
}

func (r *rabbitMQConsumer) consume(queue string, autoAck, exclusive, noLocal, noWait bool, args map[string]interface{}) (<-chan amqp.Delivery, error) {
	err := r.ch.Qos(r.prefetchCount, 0, false)
	if err != nil {
		return nil, fmt.Errorf("failed to set Qos due %v", err)
	}

	messages, err := r.ch.Consume(
		queue,
		r.name,
		autoAck,
		exclusive,
		noLocal,
		noWait,
		args,
	)
	if err != nil {
		return nil, err
	}

	return messages, nil
}

/*
Ack acknowledges a delivery by its delivery tag when having been consumed with
Channel.Consume or Channel.Get.

Ack acknowledges all message received prior to the delivery tag when multiple
is true.

See also Delivery.Ack
*/
func (r *rabbitMQConsumer) Ack(id uint64, multiple bool) error {
	if !r.Connected() {
		return errNotConnected
	}

	err := r.ch.Ack(id, multiple)
	if err != nil {
		return fmt.Errorf("failed to ack message with id %d due %v", id, err)
	}
	return nil
}

/*
Nack negatively acknowledges a delivery by its delivery tag.  Prefer this
method to notify the server that you were not able to process this delivery and
it must be redelivered or dropped.

See also Delivery.Nack
*/
func (r *rabbitMQConsumer) Nack(id uint64, multiple bool, requeue bool) error {
	if !r.Connected() {
		return errNotConnected
	}

	err := r.ch.Nack(id, multiple, requeue)
	if err != nil {
		return fmt.Errorf("failed to nack message with %d due %v", id, err)
	}
	return nil
}

/*
Reject negatively acknowledges a delivery by its delivery tag.  Prefer Nack
over Reject when communicating with a RabbitMQ server because you can Nack
multiple messages, reducing the amount of protocol messages to exchange.

See also Delivery.Reject
*/
func (r *rabbitMQConsumer) Reject(id uint64, requeue bool) error {
	if !r.Connected() {
		return errNotConnected
	}

	err := r.ch.Reject(id, requeue)
	if err != nil {
		return fmt.Errorf("failed to reject message with %d due %v", id, err)
	}
	return nil
}

func (r *rabbitMQConsumer) Close() error {
	if err := r.close(); err != nil {
		return err
	}
	return nil
}
