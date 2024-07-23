package main

import (
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("CONSUMER PANEL")
	brokers := []string{"localhost:9092"}
	topics := []string{"srj1"}
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest // Start from the beginning

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalf("Error creating consumer: %v", err)
	}
	defer consumer.Close()

	for {
		for _, topic := range topics {
			partitionList, err := consumer.Partitions(topic)
			if err != nil {
				log.Fatalf("Error fetching partitions: %v", err)
			}

			for _, partition := range partitionList {
				pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest) // Start from the beginning
				if err != nil {
					log.Fatalf("Error starting partition consumer: %v", err)
				}
				defer pc.Close()

				for msg := range pc.Messages() {
					broadcast := string(msg.Value)
					fmt.Println(broadcast)
				}
			}
		}
	}

}
