package main

import (
	"log"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	// Kafka Configuration

	kafkaBrokers := []string{"kafka:9092"} // Use 'kafka' as the hostname
	kafkaTopic := "my-logs-topic"

	// Create Confluent Kafka Producee

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(kafkaBrokers, "."),
	})

	// Introduce a short sleep
	time.Sleep(5 * time.Second) // Sleep for 5 seconds

	if err != nil {
		log.Fatal("Failed to create producer: ", err)
	}
	defer producer.Close()

	// After your producer initialization
	for i := 0; i < 5; i++ {
		_, err := producer.GetMetadata(nil, false, 1000) // 1-second timeout
		if err == nil {
			log.Println("Kafka is ready to accept connections")
			break // Kafka is ready
		}

		if i == 4 {
			log.Fatal("Failed to connect to Kafka after retries")
		}

		time.Sleep(1 * time.Second) // Retry interval
	}

	// Example log Message
	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &kafkaTopic, Partition: kafka.PartitionAny},
		Value:          []byte("Test log message from Go (Confluent)"),
	}

	// Send the Message
	deliveryChan := make(chan kafka.Event)
	err = producer.Produce(message, deliveryChan) // Use 'Produce' method
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
