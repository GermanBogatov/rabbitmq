package rabbitmq

import (
	"errors"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	logging "gitlab.gid.team/gid-pro/backend/libs/logger/v2"
	"sync"
	"time"
)

var typesMap = map[string]struct{}{
	"direct":  {},
	"fanout":  {},
	"topic":   {},
	"headers": {},
}

type BaseConfig struct {
	Host     string
	Port     string
	Username string
	Password string
	Vhost    string
}

const (
	reconnectDelay = 5 * time.Second
)

var (
	errNotConnected  = errors.New("no connection to RabbitMQ")
	errAlreadyClosed = errors.New("already connection closed to RabbitMQ")
)

type rabbitMQBase struct {
	lock        sync.Mutex
	isConnected bool
	conn        *amqp.Connection
	ch          *amqp.Channel
	done        chan bool
	notifyClose chan *amqp.Error
	reconnects  []chan<- bool
}

/*
DeclareQueue declares a queue to hold messages and deliver to consumers.
Declaring creates a queue if it doesn't already exist, or ensures that an
existing queue matches the same parameters.

Every queue declared gets a default binding to the empty exchange "" which has
the type "direct" with the routing key matching the queue's name.  With this
default binding, it is possible to publish messages that route directly to
this queue by publishing to "" with the routing key of the queue name.

	QueueDeclare("alerts", true, false, false, false, nil)
	Publish("", "alerts", false, false, Publishing{Body: []byte("...")})

	Delivery       Exchange  Key       Queue
	-----------------------------------------------
	key: alerts -> ""     -> alerts -> alerts

The queue name may be empty, in which case the server will generate a unique name
which will be returned in the Name field of Queue struct.

Durable and Non-Auto-Deleted queues will survive server restarts and remain
when there are no remaining consumers or bindings.  Persistent publishings will
be restored in this queue on server restart.  These queues are only able to be
bound to durable exchanges.

Non-Durable and Auto-Deleted queues will not be redeclared on server restart
and will be deleted by the server after a short time when the last consumer is
canceled or the last consumer's channel is closed.  Queues with this lifetime
can also be deleted normally with QueueDelete.  These durable queues can only
be bound to non-durable exchanges.

Non-Durable and Non-Auto-Deleted queues will remain declared as long as the
server is running regardless of how many consumers.  This lifetime is useful
for temporary topologies that may have long delays between consumer activity.
These queues can only be bound to non-durable exchanges.

Durable and Auto-Deleted queues will be restored on server restart, but without
active consumers will not survive and be removed.  This Lifetime is unlikely
to be useful.

Exclusive queues are only accessible by the connection that declares them and
will be deleted when the connection closes.  Channels on other connections
will receive an error when attempting  to declare, bind, consume, purge or
delete a queue with the same name.

When noWait is true, the queue will assume to be declared on the server.  A
channel exception will arrive if the conditions are met for existing queues
or attempting to modify an existing queue from a different connection.

When the error return value is not nil, you can assume the queue could not be
declared with these parameters, and the channel will be closed.
*/
func (r *rabbitMQBase) DeclareQueue(name string, durable, autoDelete, exclusive, noWait bool, args map[string]interface{}) error {
	if !r.Connected() {
		return errNotConnected
	}
	_, err := r.ch.QueueDeclare(
		name,
		durable,
		autoDelete,
		exclusive,
		noWait,
		args,
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue due %v", err)
	}

	return nil
}

/*
DeclareExchange declares an exchange on the server. If the exchange does not
already exist, the server will create it.  If the exchange exists, the server
verifies that it is of the provided type, durability and auto-delete flags.

Errors returned from this method will close the channel.

Exchange names starting with "amq." are reserved for pre-declared and
standardized exchanges. The client MAY declare an exchange starting with
"amq." if the passive option is set, or the exchange already exists.  Names can
consist of a non-empty sequence of letters, digits, hyphen, underscore,
period, or colon.

Each exchange belongs to one of a set of exchange kinds/types implemented by
the server. The exchange types define the functionality of the exchange - i.e.
how messages are routed through it. Once an exchange is declared, its type
cannot be changed.  The common types are "direct", "fanout", "topic" and
"headers".

Durable and Non-Auto-Deleted exchanges will survive server restarts and remain
declared when there are no remaining bindings.  This is the best lifetime for
long-lived exchange configurations like stable routes and default exchanges.

Non-Durable and Auto-Deleted exchanges will be deleted when there are no
remaining bindings and not restored on server restart.  This lifetime is
useful for temporary topologies that should not pollute the virtual host on
failure or after the consumers have completed.

Non-Durable and Non-Auto-deleted exchanges will remain as long as the server is
running including when there are no remaining bindings.  This is useful for
temporary topologies that may have long delays between bindings.

Durable and Auto-Deleted exchanges will survive server restarts and will be
removed before and after server restarts when there are no remaining bindings.
These exchanges are useful for robust temporary topologies or when you require
binding durable queues to auto-deleted exchanges.

Note: RabbitMQ declares the default exchange types like 'amq.fanout' as
durable, so queues that bind to these pre-declared exchanges must also be
durable.

Exchanges declared as `internal` do not accept publishings. Internal
exchanges are useful when you wish to implement inter-exchange topologies
that should not be exposed to users of the broker.

When noWait is true, declare without waiting for a confirmation from the server.
The channel may be closed as a result of an error.  Add a NotifyClose listener
to respond to any exceptions.

Optional amqp.Table of arguments that are specific to the server's implementation of
the exchange can be sent for exchange types that require extra parameters.
*/
func (r *rabbitMQBase) DeclareExchange(name, kind string, durable, autoDelete, internal, noWait bool, args map[string]interface{}) error {
	if !r.Connected() {
		return errNotConnected
	}

	if _, ok := typesMap[kind]; !ok {
		return fmt.Errorf("undefined exchange type %v", kind)
	}

	err := r.ch.ExchangeDeclare(
		name,
		kind,
		durable,
		autoDelete,
		internal,
		noWait,
		args,
	)
	if err != nil {
		return fmt.Errorf("failed to declare exchange due %v", err)
	}

	return nil
}

/*
DeclareQueueBind binds an exchange to a queue so that publishings to the exchange will
be routed to the queue when the publishing routing key matches the binding
routing key.

	QueueBind("pagers", "alert", "log", false, nil)
	QueueBind("emails", "info", "log", false, nil)

	Delivery       Exchange  Key       Queue
	-----------------------------------------------
	key: alert --> log ----> alert --> pagers
	key: info ---> log ----> info ---> emails
	key: debug --> log       (none)    (dropped)

If a binding with the same key and arguments already exists between the
exchange and queue, the attempt to rebind will be ignored and the existing
binding will be retained.

In the case that multiple bindings may cause the message to be routed to the
same queue, the server will only route the publishing once.  This is possible
with topic exchanges.

	QueueBind("pagers", "alert", "amq.topic", false, nil)
	QueueBind("emails", "info", "amq.topic", false, nil)
	QueueBind("emails", "#", "amq.topic", false, nil) // match everything

	Delivery       Exchange        Key       Queue
	-----------------------------------------------
	key: alert --> amq.topic ----> alert --> pagers
	key: info ---> amq.topic ----> # ------> emails
	                         \---> info ---/
	key: debug --> amq.topic ----> # ------> emails

It is only possible to bind a durable queue to a durable exchange regardless of
whether the queue or exchange is auto-deleted.  Bindings between durable queues
and exchanges will also be restored on server restart.

If the binding could not complete, an error will be returned and the channel
will be closed.

When noWait is false and the queue could not be bound, the channel will be
closed with an error.
*/
func (r *rabbitMQBase) DeclareQueueBind(name, key, exchange string, noWait bool, args map[string]interface{}) error {
	if !r.Connected() {
		return errNotConnected
	}

	err := r.ch.QueueBind(
		name,
		key,
		exchange,
		noWait,
		args,
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue bind due %v", err)
	}

	return nil
}

func (r *rabbitMQBase) handleReconnect(addr string) {
	for {
		select {
		case <-r.done:
			return
		case err := <-r.notifyClose:
			r.setConnected(false)
			if err == nil {
				return
			}

			logging.Info("trying to reconnect to RabbitMQ...")
			for !r.boolConnect(addr) {
				logging.Info("failed to connect to RabbitMQ. Retrying...")
				time.Sleep(reconnectDelay)
			}

			logging.Info("send signal about successfully reconnect to RabbitMQ")
			for _, ch := range r.reconnects {
				ch <- true
			}
		}
	}
}

func (r *rabbitMQBase) notifyReconnect(ch chan<- bool) {
	r.reconnects = append(r.reconnects, ch)
}

func (r *rabbitMQBase) boolConnect(addr string) bool {
	return r.connect(addr) == nil
}

func (r *rabbitMQBase) connect(addr string) error {
	if r.Connected() {
		return nil
	}

	conn, err := amqp.Dial(addr)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ due %v", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel due %v", err)
	}

	r.conn = conn
	r.ch = ch
	r.notifyClose = make(chan *amqp.Error)
	r.setConnected(true)

	ch.NotifyClose(r.notifyClose)
	logging.Info("successfully connected to RabbitMQ")

	return nil
}

func (r *rabbitMQBase) setConnected(flag bool) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.isConnected = flag
}

func (r *rabbitMQBase) Connected() bool {
	r.lock.Lock()
	defer r.lock.Unlock()

	return r.isConnected
}

func (r *rabbitMQBase) close() error {
	if !r.Connected() {
		return errAlreadyClosed
	}

	if err := r.ch.Close(); err != nil {
		return fmt.Errorf("failed to close channel due %v", err)
	}

	if err := r.conn.Close(); err != nil {
		return fmt.Errorf("failed to close connection due %v", err)
	}

	close(r.done)
	r.setConnected(false)
	return nil
}
