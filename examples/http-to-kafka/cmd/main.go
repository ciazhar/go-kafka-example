package main

import (
	"github.com/ciazhar/go-kafka-example/examples/http-to-kafka/controller"
	"github.com/ciazhar/go-kafka-example/examples/http-to-kafka/model"
	"github.com/ciazhar/go-kafka-example/pkg/env"
	"github.com/ciazhar/go-kafka-example/pkg/kafka"
	"github.com/spf13/viper"
	"log"

	"github.com/gofiber/fiber/v2"
)

func main() {

	env.Init(".env")
	kafkaBrokers := viper.GetString("KAFKA_BROKERS")

	admin := kafka.NewAdmin(kafkaBrokers)
	admin.CreateTopic(model.TopicSync, kafka.CreateTopicConfig{
		NumPartitions: 3,
	})
	admin.CreateTopic(model.TopicAsync, kafka.CreateTopicConfig{
		NumPartitions: 3,
	})

	syncProducer := kafka.NewSyncProducer(kafkaBrokers)
	asyncProducer := kafka.NewAsyncProducer(kafkaBrokers)

	app := fiber.New()
	controller.Init(app, syncProducer, asyncProducer)

	log.Fatal(app.Listen(":" + viper.GetString("PORT")))
}
