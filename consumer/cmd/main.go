// Package main is an Entry point to Kafka consumer
package main

import (
	kafkaConfig "github.com/eugenshima/kafka/config"
	"github.com/eugenshima/kafka/consumer"
)

func main() {
	topic := kafkaConfig.ConstTopic
	limit := kafkaConfig.ConstLimitMsg
	consumer.KafkaGoConsumer(topic, limit)
}
