package main

import (
	"fmt"
	"github.com/GermanBogatov/rabbitmq"
	logging "gitlab.gid.team/gid-pro/backend/libs/logger/v2"
	"log"
	"os"
	"sync"
)

func init() {
	systemName := os.Getenv("SYSTEM_NAME")
	if systemName == "" {
		systemName = "system_name"
	}
	serviceEnv := os.Getenv("SERVICE_ENV")
	if serviceEnv == "" {
		serviceEnv = "dev"
	}
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "INFO"
	}

	err := logging.InitLogging(&logging.Config{
		SystemName: systemName,
		Env:        serviceEnv,
		Level:      logLevel,
	})
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	consumer, err := rabbitmq.NewRabbitMQConsumer(rabbitmq.ConsumerConfig{
		BaseConfig: rabbitmq.BaseConfig{
			Host:     "localhost",
			Port:     "5672",
			Username: "guest",
			Password: "guest",
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	err = exampleQueue(&wg, consumer)
	if err != nil {
		log.Fatal(err)
	}

	err = exampleBinding(&wg, consumer)
	if err != nil {
		log.Fatal(err)
	}

	wg.Wait()
}

func exampleQueue(wg *sync.WaitGroup, consumer rabbitmq.Consumer) error {
	err := consumer.DeclareQueue("test-queue", true, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}

	messages, err := consumer.Consume("test-queue", "", true, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		defer wg.Done()
		for msg := range messages {
			fmt.Printf("from exampleQueue: messageID=%v; message=%s \n", msg.ID, string(msg.Body))
		}
	}()

	return nil
}

func exampleBinding(wg *sync.WaitGroup, consumer rabbitmq.Consumer) error {

	err := consumer.DeclareExchange(
		"logs",   // name
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)

	if err != nil {
		return err
	}

	err = consumer.DeclareQueue(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return err
	}

	err = consumer.DeclareQueueBind(
		"",     // queue name
		"",     // routing key
		"logs", // exchange
		false,
		nil,
	)
	if err != nil {
		return err
	}

	messages, err := consumer.Consume("", "", true, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		defer wg.Done()
		for msg := range messages {
			fmt.Printf("from exampleBinding: messageID=%v; message=%s \n", msg.ID, string(msg.Body))
		}
	}()

	return nil
}
