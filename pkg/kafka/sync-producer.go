package kafka

import (
	"fmt"
	"github.com/IBM/sarama"
	"github.com/ciazhar/go-zhar/pkg"
	"strings"
)

type SyncProducer struct {
	producer sarama.SyncProducer
}

type SyncProducerConfig struct {
	Version string
	Retry   int
}

func NewSyncProducer(brokers string, producerConfig ...SyncProducerConfig) *SyncProducer {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Return.Successes = true

	if len(producerConfig) > 0 {
		version, err := sarama.ParseKafkaVersion(producerConfig[0].Version)
		pkg.FailOnError(err, "Error parsing Kafka version: %v")
		config.Version = version
		config.Producer.Retry.Max = producerConfig[0].Retry
	}

	producer, err := sarama.NewSyncProducer(strings.Split(brokers, ","), config)
	pkg.FailOnError(err, "Failed to create Kafka Producer")
	fmt.Println("Connected to Kafka Producer")
	return &SyncProducer{
		producer: producer,
	}
}

func (p *SyncProducer) PublishMessage(topic string, value string) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(value),
	}

	_, _, err := p.producer.SendMessage(msg)
	pkg.FailOnError(err, "Failed to publish a message")
	fmt.Printf("Message '%s' published to topic '%s'\n", value, topic)
}

func (p *SyncProducer) PublishMessageWithKey(topic string, key string, message string) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(message),
	}

	_, _, err := p.producer.SendMessage(msg)
	pkg.FailOnError(err, "Failed to publish a message")
	fmt.Printf("Key '%s' Message '%s' published to topic '%s'\n", key, message, topic)
}

func (p *SyncProducer) Close() {
	err := p.producer.Close()
	if err != nil {
		panic(err.Error())
	}
}
