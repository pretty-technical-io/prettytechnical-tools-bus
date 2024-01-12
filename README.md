# Bus

**Bus** This package abstracts all the logic and configuration to create a connection with Kafka.
![Go](https://img.shields.io/badge/Golang-1.21-blue.svg?logo=go&longCache=true&style=flat)

This package makes use of the external package [sarama](github.com/Shopify/sarama) and our [logger](gitlab.com/prettytechnical/tools/logger) package.


## Getting Started

[Go](https://golang.org/) is required in version 1.21 or higher.

### Install

`go get -u gitlab.com/prettytechnical/tools/bus`

### Features

* [x] **Lightweight**, less than 300 lines of code.
* [x] **Easy** to use.

### Basic use

```go
package main

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"os"
	"time"

	"gitlab.com/prettytechnical-tools/bus"
	"gitlab.com/prettytechnical-tools/logger"
)

func main() {
	l := logger.New("transaction service", false)

	var (
		brokers    = "localhost:9091"
		topic      = "test-topic"
		consumerID = "test-consumer"
		producerID = "test-producer"
	)

	go func() {
		//init the producer config
		producerConfig := &bus.ProducerConfig{
			KafkaRetries:          2,
			KafkaBrokers:          brokers, // add brokers
			KafkaUser:             "",
			KafkaPassword:         "",
			KafkaClientProducerID: producerID,
		}

		broker, err := bus.NewProducer(producerConfig, l.With("kafka_service", "producer")) // create the producer
		if err != nil {
			l.With("kafka_error", "create producer").Error(err)
			os.Exit(1)
		}
		defer broker.Close()

		for i := 0; true; i++ {
			err = broker.Send(context.Background(), []byte(fmt.Sprint("test-message-", i)), topic, "key", 0) //send message
			if err != nil {
				l.Error(err)
			}
			time.Sleep(time.Second * 2)
		}

	}()

	//init the client config
	kafkaConfig := bus.ClientConfig{}
	kafkaClient, err := bus.New(brokers, consumerID, kafkaConfig, l.With("scope", "kafka-client")) // create the client
	if err != nil {
		l.Error(err)
		os.Exit(1)
	}
	defer kafkaClient.Close()

	handlerMap := make(map[string]func(msg *sarama.ConsumerMessage) error)
	handlerMap[topic] = MessageHandler
	kafkaHandlers := bus.NewHandler(handlerMap, l)

	err = kafkaClient.Start(context.Background(), []string{topic}, kafkaHandlers) //start to consume message
	if err != nil {
		l.Error(err)
		os.Exit(1)
	}
}

func MessageHandler(message *sarama.ConsumerMessage) error {
	//add any logic you need to process kafka's message
	fmt.Println(fmt.Sprintf("value:%s, key: %s", message.Value, message.Key))
	return nil
}

```