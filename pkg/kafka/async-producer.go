package kafka

import (
	"fmt"
	"github.com/IBM/sarama"
	"github.com/ciazhar/go-zhar/pkg"
	"log"
	"strings"
	"time"
)

type AsyncProducer struct {
	producer sarama.AsyncProducer
}

type AsyncProducerConfig struct {
	Version string
	Retry   int
}

func NewAsyncProducer(brokers string, producerConfig ...AsyncProducerConfig) *AsyncProducer {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
	config.Producer.Compression = sarama.CompressionSnappy   // Compress messages
	config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms

	if len(producerConfig) > 0 {
		version, err := sarama.ParseKafkaVersion(producerConfig[0].Version)
		pkg.FailOnError(err, "Error parsing Kafka version: %v")
		config.Version = version
		config.Producer.Retry.Max = producerConfig[0].Retry
	}

	producer, err := sarama.NewAsyncProducer(strings.Split(brokers, ","), config)
	pkg.FailOnError(err, "Failed to create Kafka Producer")
	fmt.Println("Connected to Kafka Producer")

	// We will just log to STDOUT if we're not able to produce messages.
	// Note: messages will only be returned here after all retry attempts are exhausted.
	go func() {
		for err := range producer.Errors() {
			log.Println("Failed to write access log entry:", err)
		}
	}()

	return &AsyncProducer{
		producer: producer,
	}
}

func (p *AsyncProducer) PublishMessage(topic string, value string) {
	p.producer.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(value),
	}
	//fmt.Printf("Message '%s' published to topic '%s'\n", value, topic)
}

func (p *AsyncProducer) PublishMessageWithKey(topic string, key string, value string) {
	p.producer.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(value),
		Key:   sarama.StringEncoder(key),
	}
	//fmt.Printf("Key '%s' Message '%s' published to topic '%s'\n", key, value, topic)
}

func (p *AsyncProducer) Close() {
	err := p.producer.Close()
	if err != nil {
		panic(err.Error())
	}
}
