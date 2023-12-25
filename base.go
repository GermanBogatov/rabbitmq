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
