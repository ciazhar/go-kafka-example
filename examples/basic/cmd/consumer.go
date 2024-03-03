package main

import (
	"fmt"
	"github.com/ciazhar/go-kafka-example/examples/basic/model"
	"github.com/ciazhar/go-kafka-example/pkg/env"
	"github.com/ciazhar/go-kafka-example/pkg/kafka"
	"github.com/spf13/viper"
)

func main() {

	env.Init(".env")
	kafkaBrokers := viper.GetString("KAFKA_BROKERS")

	consumer := kafka.NewConsumer(kafkaBrokers)
	defer consumer.Close()

	admin := kafka.NewAdmin(kafkaBrokers)
	defer admin.Close()

	admin.CreateTopic(model.TopicName)
	admin.CreateTopic(model.Topic2Name)

	KafkaConsumer(consumer)

	forever := make(chan bool)
	<-forever
}

func KafkaConsumer(consumer kafka.Consumer) {
	go consumer.ConsumeMessages(model.TopicName, HandleMessage)
	go consumer.ConsumeMessages(model.Topic2Name, HandleMessage2)
}

func HandleMessage(string2 string) {
	fmt.Printf("From Topic 1. Received a message: %s\n", string2)
}

func HandleMessage2(string2 string) {
	fmt.Printf("From Topic 2. Received a message: %s\n", string2)
}
