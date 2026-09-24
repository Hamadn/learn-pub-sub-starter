package main

import (
	"fmt"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

func main() {
	connString := "amqp://guest:guest@localhost:5672/"
	conection, err := amqp.Dial(connString)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conection.Close()
	fmt.Println("Connection Successful to RabbitMQ!")

	channel, err := conection.Channel()
	if err != nil {
		log.Fatalf("could not create channel: %v", err)
	}

	val := routing.PlayingState{
		IsPaused: true,
	}

	err = pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, val)

	if err != nil {
		log.Printf("could not publish: %v", err)
	}
	fmt.Println("Pause message sent!")

}
