package main

import (
	kafkaConfig "producer/config"
	"producer/producer"
)

func main() {
	topic := kafkaConfig.CONST_TOPIC
	producer.KafkaGoProducer(topic, 1000)
}
