package main

import (
	"fmt"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"os"
	"os/signal"
)

func main() {
	connString := "amqp://guest:guest@localhost:5672/"
	conection, err := amqp.Dial(connString)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conection.Close()
	fmt.Println("Connection Successful to RabbitMQ!")

	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Printf("Username not found: %v", err)
	}

	channel, queue, err := pubsub.DeclareAndBind(conection, routing.ExchangePerilDirect, "pause."+userName, routing.PauseKey, pubsub.Transient)
	if err != nil {
		log.Printf("Could not declare and bind queue %v to channel %v: %v", queue, channel, err)
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
}
