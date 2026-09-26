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

	channel, queue, err := pubsub.DeclareAndBind(conection, routing.ExchangePerilDirect, "pause."+userName, routing.PauseKey, pubsub.Transient)
	if err != nil {
		log.Fatalf("Could not declare and bind queue %v to channel %v: %v", queue.Name, channel, err)
	}
	fmt.Printf("Queue %v bound to channel %v\n", queue.Name, channel)

	gameState := gamelogic.NewGameState(userName)

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
