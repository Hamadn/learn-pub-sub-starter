package main

import (
	"fmt"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
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
	fmt.Println("Connection Successful to RabbitMQ!\n")

	channel, err := conection.Channel()
	if err != nil {
		log.Fatalf("could not create channel: %v", err)
	}

	gamelogic.PrintServerHelp()

	for {
		words := gamelogic.GetInput()

		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "pause":
			err = pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})

			if err != nil {
				log.Printf("could not publish: %v", err)
			}
			fmt.Println("Pause message sent!")
		case "resume":
			err = pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})

			if err != nil {
				log.Printf("could not publish: %v", err)
			}
			fmt.Println("Resume message sent!")
		case "quit":
			log.Println("Quitting the game...")
			return
		default:
			log.Println("Unknown command")
		}

	}
}
