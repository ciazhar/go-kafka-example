package kafka

import (
	"fmt"
	"github.com/IBM/sarama"
	"github.com/ciazhar/go-zhar/pkg"
	"log"
	"os"
	"os/signal"
	"strings"
)

type Consumer struct {
	consumer sarama.Consumer
}

func NewConsumer(brokers string) Consumer {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer(strings.Split(brokers, ","), config)
	pkg.FailOnError(err, "Failed to create Kafka Consumer")
	fmt.Println("Connected to Kafka Consumer")
	return Consumer{
		consumer: consumer,
	}
}

func (c *Consumer) ConsumeMessages(topicName string, out func(msg string)) {

	consumer, err := c.consumer.ConsumePartition(topicName, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatal("Error creating partition consumer:", err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatal("Error closing partition consumer:", err)
		}
	}()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				log.Println(err)
			case msg := <-consumer.Messages():
				out(string(msg.Value))
			case <-signals:
				log.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()
	<-doneCh
}

func (c *Consumer) Close() {
	err := c.consumer.Close()
	if err != nil {
		panic(err.Error())
	}
}
