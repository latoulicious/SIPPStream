package main

import (
	"context"
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"google.golang.org/genproto/googleapis/maps/fleetengine/delivery/v1"
)

func main() {
	// Kafka Configuration

	kafkaBrokers := []string{"localhost:9092"}
	kafkaTopic := "my-logs-topic"

	// Create Confluent Kafka Producee

	producer, err := kafka.newProducer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(kafkaBrokers, "."),
	})
	if err != nil {
		log.Fatal("Failed to create producer: ", err)
	}
	defer producer.Close()

	// Example log Message
	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &kafkaTopic, Partition: kafka.PartitionAny},
		Value:          []byte("Test log message from Go (Confluent)"),
	}

	// Send the Message
	deliveryChan := make(chan kafka.Event)
	err = producer.Producer(message, deliveryChan)
	if err != nil {
		log.Fatal("Failed to send message:", err)
	}

	// Wait for delivery report
	event := <-deliveryChan
	msg := event.(*kafka.Message)

	if msg.TopicPartition.Error != nil {
		log.Printf("Delivery failed: %v\n", msg.TopicPartition.Error)
	} else {
		log.Printf("Message delivered to partition %d at offset %d\n", msg.TopicPartition.Partition, msg.TopicPartition.Offset)
	}

}
