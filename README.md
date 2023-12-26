# RabbitMQ client library

# Установить последнюю версию тега

```
go get gitlab.gid.team/gid-pro/backend/libs/rabbitmq@latest
```

# Пример продюсера
примеры использования продюсера [example/producer/example.go](example%2Fproducer%2Fexample.go)

# Пример консюмера
примеры использования консюмера [example/consumer/example.go](example%2Fconsumer%2Fexample.go)

# Конфигурация продюсера и консюмера
```
Для подключения продюсера и консьюмера используется структура:
type BaseConfig struct {
	Host     string - хост rabbitMQ
	Port     string - порт rabbitMQ
	Username string - логин для rabbitMQ
	Password string - пароль для rabbitMQ
}

 что не следует одновременно передавать больше сообщений потребителю, чем указано
 
Также для консьюмера есть отдельное поле 
`PrefetchCount` - которое указывает, сколько максимально одновременно можно передать сообщений консюмеру
```

# Создание очередей, обменников и биндингов.
Создавать все можно как в продюсере, так и в консюмере. Если какой-либо сущности не существует, она просто создатся с нуля.
Если же такая сущность уже есть, то вернется просто nil.

### Создание обычной очереди.
```
DeclareQueue (name string, durable, autoDelete, exclusive, noWait bool, args map[string]interface{}), где
name - название очереди
durable - флаг, постоянная или нет очередь
autoDelete - флаг, автоудаления очереди
exclusive - флаг, разрешающий подключаться только одному консюмеру
noWait - флаг, не дожидаться ответа от сервера (Если серверу не удалось завершить метод, он выдаст исключение канала или соединения)
args - дополнительные аргументы
```

### Создание обменника.
```
DeclareExchange(name, kind string, durable, autoDelete, internal, noWait bool, args map[string]interface{}), где
name - название обменника
kind - тип обменника (direct, fanout, topic, headers)
durable - флаг, постоянный или нет обменник
internal -  флаг, если true, то можно будет использовать только для E2E.
noWait - флаг, не дожидаться ответа от сервера (Если серверу не удалось завершить метод, он выдаст исключение канала или соединения)
args - дополнительные аргументы
```

### Создание биндинга.
```
DeclareQueueBind(name, key, exchange string, noWait bool, args map[string]interface{}), где
name - название очереди
key - ключ роутинга
exchange - название обменника
noWait - флаг, не дожидаться ответа от сервера (Если серверу не удалось завершить метод, он выдаст исключение канала или соединения)
args - дополнительные аргументы
```