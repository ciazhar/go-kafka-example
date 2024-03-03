package kafka

import (
	"fmt"
	"github.com/IBM/sarama"
	"github.com/ciazhar/go-zhar/pkg"
	"strings"
)

type Admin struct {
	admin sarama.ClusterAdmin
}

func NewAdmin(brokers string) Admin {
	config := sarama.NewConfig()

	admin, err := sarama.NewClusterAdmin(strings.Split(brokers, ","), config)
	pkg.FailOnError(err, "error creating Kafka admin client")

	return Admin{
		admin: admin,
	}
}

type CreateTopicConfig struct {
	NumPartitions     int32
	ReplicationFactor int16
}

func (a *Admin) CreateTopic(topicName string, config ...CreateTopicConfig) {
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     1,   // Number of partitions for the topic
		ReplicationFactor: 1,   // Replication factor for the topic
		ConfigEntries:     nil, // Additional topic configuration (can be nil)
	}

	if len(config) > 0 {
		topicDetail.NumPartitions = config[0].NumPartitions
		topicDetail.ReplicationFactor = config[0].ReplicationFactor
	}

	// Create the topic
	err := a.admin.CreateTopic(topicName, topicDetail, true)
	if err != nil {
		fmt.Printf("Error creating Kafka topic: %v\n", err)
		return
	}

	fmt.Printf("Topic '%s' created successfully.\n", topicName)
}

func (c *Admin) Close() {
	err := c.admin.Close()
	if err != nil {
		panic(err.Error())
	}
}
