package main

import (
	"github.com/ciazhar/go-kafka-example/examples/basic/model"
	"github.com/ciazhar/go-kafka-example/pkg/env"
	"github.com/ciazhar/go-kafka-example/pkg/kafka"
	"github.com/spf13/viper"
)

func main() {

	env.Init(".env")
	kafkaBrokers := viper.GetString("KAFKA_BROKERS")

	producer := kafka.NewSyncProducer(kafkaBrokers)
	defer producer.Close()

	producer.PublishMessage(model.TopicName, "Hello, Kafka!")
	producer.PublishMessage(model.Topic2Name, "Hello, Kafka!")
}
