package main

import (
	kafkaConfig "producer/config"
	"producer/consumer"
)

func main() {
	topic := kafkaConfig.CONST_TOPIC
	consumer.KafkaGoConsumer(topic)
}
