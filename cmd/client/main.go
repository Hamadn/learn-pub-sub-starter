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
	fmt.Println("Starting Peril Client...")
	connString := "amqp://guest:guest@localhost:5672/"

	conection, err := amqp.Dial(connString)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conection.Close()
	fmt.Println("Connection Successful to RabbitMQ!")

	ch, err := conection.Channel()
	if err != nil {
		log.Fatalf("could not create channel: %v", err)
	}

	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("could not get username: %v", err)
	}

	gameState := gamelogic.NewGameState(userName)

	err = pubsub.SubscribeJSON(conection, routing.ExchangePerilDirect, "pause."+userName, routing.PauseKey, pubsub.Transient, handlerPause(gameState))
	if err != nil {
		log.Fatalf("could not subscribe to pause channel: %v", err)
	}

	err = pubsub.SubscribeJSON(conection, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+userName, routing.ArmyMovesPrefix+".*", pubsub.Transient, handlerMove(gameState, ch))
	if err != nil {
		log.Fatalf("could not subscribe to move channel: %v", err)
	}

	err = pubsub.SubscribeJSON(conection, routing.ExchangePerilTopic, "war", routing.WarRecognitionsPrefix+".*", pubsub.Durable, handlerWar(gameState, ch))
	if err != nil {
		log.Fatalf("could not subscribe to war channel: %v", err)
	}

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		switch words[0] {
		case "spawn":
			if words[1] == "" || words[2] == "" {
				log.Println("Please provide a location and unit type")
			}
			err := gameState.CommandSpawn(words)
			if err != nil {
				fmt.Println(err)
				continue
			}
		case "move":
			move, err := gameState.CommandMove(words)
			if err != nil {
				fmt.Println(err)
				continue
			}

			err = pubsub.PublishJSON(ch, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+move.Player.Username, move)
			if err != nil {
				fmt.Printf("Error publishing move: %v", err)
				continue
			}
			fmt.Printf("Moved %v unit %v to %s\n", len(move.Units), move.ToLocation)

		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("Unknown command. Type 'help' for a list of commands.")
			continue
		}
	}
}
