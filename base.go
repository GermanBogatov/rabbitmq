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

/*
DeclareQueue объявляет очередь для хранения сообщений и их доставки потребителям.
Объявление создает очередь, если она еще не существует, или гарантирует, что
существующая очередь соответствует тем же параметрам.

Каждая объявленная очередь получает привязку по умолчанию к пустому обмену "", который имеет
тип «прямой» с ключом маршрутизации, соответствующим имени очереди. С этим
привязка по умолчанию, можно публиковать сообщения, которые направляются непосредственно в
эту очередь путем публикации в "" с ключом маршрутизации имени очереди.

DeclareQueue("alerts", true, false, false, false, false)
Publish("", "alerts", false, false, Publishing{Body: []byte("...")})

доставка    ключ обмена    очередь
----------------------------------------------
ключ: alerts -> «» -> alerts -> alerts

Имя очереди может быть пустым, в этом случае сервер сгенерирует уникальное имя.
который будет возвращен в поле Name структуры Queue.

Durable and Non-Auto-Deleted очереди сохранятся после перезапуска сервера.
когда нет оставшихся потребителей или привязок. Постоянные публикации будут
быть восстановлены в этой очереди при перезапуске сервера. Эти очереди могут быть только
связаны с долгосрочным обменом.

Non-Durable and Auto-Deleted очереди не будут переобъявляться при перезапуске сервера.
и будет удален сервером через некоторое время, когда последний потребитель
отменен или последний канал потребителя закрыт. Очереди с этим временем жизни
также можно удалить обычным способом с помощью QueueDelete. Эти устойчивые очереди могут только
быть привязаны к недолговечным обменам.

Очереди Non-Durable и Non-Auto-Deleted останутся объявленными до тех пор, пока
сервер работает независимо от количества потребителей. Эта жизнь полезна
для временных топологий, которые могут иметь длительные задержки между потребительскими действиями.
Эти очереди могут быть привязаны только к недолговечным обменам.

Durable and Auto-Deleted очереди будут восстановлены при перезапуске сервера, но без
активные потребители не выживут и будут удалены. Эта жизнь маловероятна
быть полезным.

Эксклюзивные очереди доступны только тому соединению, которое их объявляет и
будут удалены при закрытии соединения. Каналы на других соединениях
получит сообщение об ошибке при попытке объявить, связать, использовать, очистить или
удалить очередь с таким же именем.

Если noWait имеет значение true, предполагается, что очередь объявлена на сервере. А
исключение канала поступит, если условия для существующих очередей выполнены
или попытка изменить существующую очередь из другого соединения.

Если возвращаемое значение ошибки не равно нулю, можно предположить, что очередь не может быть
объявлен с этими параметрами, и канал будет закрыт.
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
DeclareExchange объявляет обмен на сервере. Если обмен не состоится
уже существует, сервер создаст его. Если обмен существует, сервер
проверяет, что он имеет указанный тип, долговечность и флаги автоматического удаления.

Ошибки, возвращаемые этим методом, закроют канал.

Имена бирж, начинающиеся с «amq». зарезервированы для заранее объявленных и
стандартизированные обмены. Клиент МОЖЕТ объявить обмен, начиная с
"amq." если установлена пассивная опция или биржа уже существует. Имена могут
состоять из непустой последовательности букв, цифр, дефиса, подчеркивания,
точка или двоеточие.

Каждый обмен принадлежит к одному из набора видов/типов обмена, реализованных
сервер. Типы обмена определяют функциональность обмена, т.е.
как через него проходят сообщения. После объявления обмена его тип
не может быть изменено. Распространенными типами являются "direct", "fanout", "topic" и
"headers".

Durable and Non-Auto-Deleted exchanges выдержат перезагрузку сервера и останутся
объявляется, когда нет оставшихся привязок. Это лучшая жизнь для
долгоживущие конфигурации обмена, такие как стабильные маршруты и обмены по умолчанию.

Non-Durable and Auto-Deleted exchanges будут удаляться при отсутствии
оставшиеся привязки и не восстанавливаются при перезапуске сервера. Эта жизнь
полезно для временных топологий, которые не должны загрязнять виртуальный хост на
сбой или после того, как потребители завершили работу.

Non-Durable and Non-Auto-deleted будут оставаться до тех пор, пока сервер
работает в том числе и тогда, когда не осталось привязок. Это полезно для
временные топологии, которые могут иметь длительные задержки между привязками.

Durable and Auto-Deleted exchanges выдержат перезагрузку сервера и будут
удаляется до и после перезапуска сервера, когда не осталось привязок.
Эти обмены полезны для надежных временных топологий или когда вам требуется
привязка устойчивых очередей к автоматически удаляемым обменам.

Примечание. RabbitMQ объявляет типы обмена по умолчанию, такие как «amq.fanout», как
durable, поэтому очереди, которые привязываются к этим заранее объявленным обменам, также должны быть
прочный.

Exchanges, объявленные «внутренними», не принимают публикации. Внутренний
exchanges полезны, когда вы хотите реализовать топологии между обменами
это не должно быть доступно пользователям брокера.

Если noWait имеет значение true, объявляйте, не дожидаясь подтверждения от сервера.
Канал может быть закрыт в результате ошибки. Добавьте прослушиватель NotifyClose
реагировать на любые исключения.

Необязательный amqp. Таблица аргументов, специфичных для реализации сервера.
обмен может быть отправлен для типов обмена, требующих дополнительных параметров.
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
DeclareQueueBind привязывает обмен к очереди, чтобы публикации на обмене
направляться в очередь, когда ключ маршрутизации публикации соответствует привязке
ключ маршрутизации.

	DeclareQueueBind("pagers", "alert", "log", false, nil)
	DeclareQueueBind("emails", "info", "log", false, nil)

	Delivery       Exchange  Key       Queue
	-----------------------------------------------
	key: alert --> log ----> alert --> pagers
	key: info ---> log ----> info ---> emails
	key: debug --> log       (none)    (dropped)

Если привязка с тем же ключом и аргументами уже существует между
обмена и постановки в очередь, попытка перепривязки будет проигнорирована и существующие
привязка сохранится.

В случае, если несколько привязок могут привести к перенаправлению сообщения на
той же очереди, сервер будет маршрутизировать публикацию только один раз. Это возможно
с обменом темами.

	DeclareQueueBind("pagers", "alert", "amq.topic", false, nil)
	DeclareQueueBind("emails", "info", "amq.topic", false, nil)
	DeclareQueueBind("emails", "#", "amq.topic", false, nil) // match everything

	Delivery       Exchange        Key       Queue
	-----------------------------------------------
	key: alert --> amq.topic ----> alert --> pagers
	key: info ---> amq.topic ----> # ------> emails
	                         \---> info ---/
	key: debug --> amq.topic ----> # ------> emails

Привязать устойчивую очередь к устойчивому обмену можно только независимо от
удаляется ли очередь или обмен автоматически. Привязки между устойчивыми очередями
и обмены также будут восстановлены при перезапуске сервера.

Если привязка не может быть завершена, будет возвращена ошибка и канал
будет закрыт.

Если noWait имеет значение false и очередь не может быть привязана, канал будет
закрыто с ошибкой.
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
