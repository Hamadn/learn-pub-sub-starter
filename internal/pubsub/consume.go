package pubsub

import (
	"encoding/json"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	Transient SimpleQueueType = iota
	Durable
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {

	channel, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not create channel: %v", err)
	}

	newQueue, err := channel.QueueDeclare(queueName, queueType == Durable, queueType == Transient, queueType == Transient, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not declare queue: %v", err)
	}

	err = channel.QueueBind(newQueue.Name, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not bind queue: %v", err)
	}

	return channel, newQueue, nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T),
) error {

	channel, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		fmt.Errorf("could not declare and bind queue: %v", err)
	}

	msgs, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		fmt.Errorf("could not start consuming: %v", err)
	}

	go func() {
		defer channel.Close()
		for msg := range msgs {
			var target T
			err := json.Unmarshal(msg.Body, &target)
			if err != nil {
				fmt.Printf("could not unmarshal message: %v", err)
				continue
			}
			handler(target)
			msg.Ack(false)
		}
	}()
	return nil
}
