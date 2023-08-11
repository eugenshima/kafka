package main

import (
	kafkaConfig "github.com/eugenshima/kafka/config"
	"github.com/eugenshima/kafka/consumer"
)

func main() {
	topic := kafkaConfig.CONST_TOPIC
	consumer.KafkaGoConsumer(topic)
}
