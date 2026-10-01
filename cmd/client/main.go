package main

import (
	"fmt"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"time"
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

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(state routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(state)
		return pubsub.Ack
	}
}
func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		mvOutcome := gs.HandleMove(move)

		switch mvOutcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix+"."+gs.GetUsername(), gamelogic.RecognitionOfWar{
				Attacker: move.Player,
				Defender: gs.GetPlayerSnap(),
			})
			if err != nil {
				log.Printf("error publishing war recognition: %v", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		}
		fmt.Println("error: unknown move outcome")
		return pubsub.NackDiscard
	}
}

func PublishGameLog(ch *amqp.Channel, username string, msg string) error {
	gameLog := routing.GameLog{
		CurrentTime: time.Now(),
		Message:     msg,
		Username:    username,
	}

	err := pubsub.PublishGob(ch, routing.ExchangePerilTopic, routing.GameLogSlug+"."+gameLog.Username, gameLog)
	if err != nil {
		fmt.Printf("Error publishing game log: %v", err)
	}
	return err
}

func handlerWar(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(recog gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		warOutcome, winner, loser := gs.HandleWar(recog)
		switch warOutcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			err := PublishGameLog(ch, recog.Attacker.Username, fmt.Sprintf("%s won a war against %s", winner, loser))
			if err != nil {
				return pubsub.NackRequeue
			}
			fmt.Printf("%s won a war against %s\n", winner, loser)
			return pubsub.Ack
		case gamelogic.WarOutcomeYouWon:
			err := PublishGameLog(ch, recog.Attacker.Username, fmt.Sprintf("%s won a war against %s", winner, loser))
			if err != nil {
				return pubsub.NackRequeue
			}
			fmt.Printf("%s won a war against %s\n", winner, loser)
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			err := PublishGameLog(ch, recog.Attacker.Username, fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser))
			if err != nil {
				return pubsub.NackRequeue
			}
			fmt.Printf("A war between %s and %s resulted in a draw\n", winner, loser)
			return pubsub.Ack
		default:
			fmt.Println("error: unknown war outcome")
			return pubsub.NackDiscard
		}
	}
}
