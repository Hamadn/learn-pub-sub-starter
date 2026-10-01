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
	connection, err := amqp.Dial(connString)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer connection.Close()
	fmt.Println("Connection Successful to RabbitMQ!\n")

	channel, err := connection.Channel()
	if err != nil {
		log.Fatalf("could not create channel: %v", err)
	}

	err = pubsub.SubscribeGob(connection, routing.ExchangePerilTopic, routing.GameLogSlug, routing.GameLogSlug+".*", pubsub.Durable, handlerGameLog())
	if err != nil {
		log.Fatalf("error: %v\n", err)
	}
	fmt.Printf("Queue bound to channel\n")

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

func handlerGameLog() func(routing.GameLog) pubsub.AckType {
	return func(gameLog routing.GameLog) pubsub.AckType {
		defer fmt.Print("> ")
		err := gamelogic.WriteLog(gameLog)
		if err != nil {
			fmt.Printf("Error writing game log: %v", err)
			return pubsub.NackDiscard
		}
		return pubsub.Ack
	}
}
