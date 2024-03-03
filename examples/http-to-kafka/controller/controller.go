package controller

import (
	"github.com/ciazhar/go-kafka-example/examples/http-to-kafka/model"
	"github.com/ciazhar/go-kafka-example/pkg/kafka"
	"github.com/gofiber/fiber/v2"
)

type Controller interface {
	SyncProducer(ctx *fiber.Ctx) error
	AsyncProducer(ctx *fiber.Ctx) error
}

type controller struct {
	syncConsumer  *kafka.SyncProducer
	asyncConsumer *kafka.AsyncProducer
}

func NewController(
	syncConsumer *kafka.SyncProducer,
	asyncConsumer *kafka.AsyncProducer,
) Controller {
	return controller{
		syncConsumer:  syncConsumer,
		asyncConsumer: asyncConsumer,
	}
}

func (c controller) SyncProducer(ctx *fiber.Ctx) error {
	text := "Message Sent To Sync Topic!"
	c.syncConsumer.PublishMessage(model.TopicSync, text)
	return ctx.SendString(text)
}

func (c controller) AsyncProducer(ctx *fiber.Ctx) error {
	text := "Message Sent To Async Topic!"
	c.syncConsumer.PublishMessage(model.TopicAsync, text)
	return ctx.SendString(text)
}
