package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/IBM/sarama"
)

type Order struct {
	ID        string `json:id`
	ProductId string `json:product_id`
	UserId    string `json:product_id`
	Amount    string `json:amount`
}

func main() {
	fmt.Println("************************* PRODUCER PANEL *****************************")
	producerServer := []string{"localhost:9092"}
	producer, err := sarama.NewSyncProducer(producerServer, nil)
	if err != nil {
		log.Fatalln("Failed to start sarama producer server")
		os.Exit(1)
	}
	message := Order{
		ID:        "1234",
		ProductId: "7654",
		UserId:    "0976134",
		Amount:    "100",
	}
	jsonMessage, err := json.Marshal(message)
	if err != nil {
		log.Fatalln("Failed to marshal message:", err)
		os.Exit(1)
	}
	custom_message := &sarama.ProducerMessage{
		Topic: "srj1",
		Value: sarama.StringEncoder(jsonMessage),
	}
	_, _, err = producer.SendMessage(custom_message)
	if err != nil {
		log.Fatalln("Failed to send message:", err)
		os.Exit(1)
	}
	log.Println("Message sent!")

}
