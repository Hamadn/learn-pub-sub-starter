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

	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Printf("could not get username: %v", err)
	}

	gameState := gamelogic.NewGameState(userName)

	err = pubsub.SubscribeJSON(conection, routing.ExchangePerilDirect, "pause."+userName, routing.PauseKey, pubsub.Transient, handlerPause(gameState))
	if err != nil {
		log.Fatalf("could not subscribe to pause channel: %v", err)
	}
	fmt.Println("Subscribed to pause channel!")

	for {
		switch words := gamelogic.GetInput(); words[0] {
		case "spawn":
			if words[1] == "" || words[2] == "" {
				log.Println("Please provide a location and unit type")
			}
			err := gameState.CommandSpawn(words)
			if err != nil {
				log.Printf("Error spawning unit: %v", err)
			}
		case "move":
			if words[1] == "" || words[2] == "" {
				log.Println("Please provide a unit ID and location")
			}
			_, err := gameState.CommandMove(words)
			if err != nil {
				log.Printf("Error moving unit: %v", err)
				continue
			}
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			log.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("Unknown command. Type 'help' for a list of commands.")
			continue
		}
	}
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(state routing.PlayingState) {
		defer fmt.Print("> ")
		gs.HandlePause(state)
	}
}
