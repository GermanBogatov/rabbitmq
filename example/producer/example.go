package main

import (
	"context"
	"encoding/json"
	"github.com/GermanBogatov/rabbitmq"
	logging "gitlab.gid.team/gid-pro/backend/libs/logger/v2"
	"log"
	"os"
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
	producer, err := rabbitmq.NewRabbitMQProducer(rabbitmq.ProducerConfig{
		BaseConfig: rabbitmq.BaseConfig{
			Host:     "localhost",
			Port:     "5672",
			Username: "guest",
			Password: "guest",
		},
	})
	if err != nil {
		logging.Fatal(err.Error())
	}

	ctx := context.Background()

	err = exampleQueue(ctx, producer)
	if err != nil {
		logging.Fatal(err.Error())
	}

	err = exampleBinding(ctx, producer)
	if err != nil {
		logging.Fatal(err.Error())
	}
}

type Data struct {
	Name    string `json:"name"`
	Surname string `json:"surname"`
	Age     int    `json:"age"`
}

func exampleQueue(ctx context.Context, producer rabbitmq.Producer) error {
	err := producer.DeclareQueue("test-queue", true, false, false, false, nil)
	if err != nil {
		return err
	}

	data := Data{
		Name:    "name-test",
		Surname: "surname-test",
		Age:     300,
	}

	dataBytes, err := json.Marshal(data)
	if err != nil {
		log.Fatal(err)
	}

	err = producer.Publish(ctx, "", "test-queue", dataBytes)
	if err != nil {
		log.Fatal(err)
	}

	return nil
}

func exampleBinding(ctx context.Context, producer rabbitmq.Producer) error {
	err := producer.DeclareExchange("logs", "fanout", true, false, false, false, nil)
	if err != nil {
		return err
	}

	data := Data{
		Name:    "name-test",
		Surname: "surname-test",
		Age:     300,
	}

	dataBytes, err := json.Marshal(data)
	if err != nil {
		log.Fatal(err)
	}

	err = producer.Publish(ctx, "logs", "", dataBytes)
	if err != nil {
		log.Fatal(err)
	}

	return nil
}
