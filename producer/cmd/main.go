// Package main is an Entry point to Kafka producer
package main

import (
	kafkaConfig "github.com/eugenshima/kafka/config"
	"github.com/eugenshima/kafka/producer"
)

func main() {
	topic := kafkaConfig.ConstTopic
	limit := kafkaConfig.ConstLimitMsg
	producer.KafkaGoProducer(topic, limit)
}
