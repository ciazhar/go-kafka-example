package controller

import (
	"github.com/ciazhar/go-kafka-example/pkg/kafka"
	"github.com/gofiber/fiber/v2"
)

func Init(app *fiber.App,
	syncConsumer *kafka.SyncProducer,
	asyncConsumer *kafka.AsyncProducer) {
	controller := NewController(syncConsumer, asyncConsumer)

	r := app.Group("/")
	r.Get("/async", controller.AsyncProducer)
	r.Get("/sync", controller.SyncProducer)
}
