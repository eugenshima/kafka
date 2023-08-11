package main

import (
	kafkaConfig "github.com/eugenshima/kafka/config"
	"github.com/eugenshima/kafka/producer"
)

func main() {
	topic := kafkaConfig.CONST_TOPIC
	limit := kafkaConfig.CONST_LIMIT_MSG
	producer.KafkaGoProducer(topic, limit)
}
