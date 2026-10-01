package pubsub

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

type AckType int

type SimpleQueueType int

const (
	Transient SimpleQueueType = iota
	Durable
)

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
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

	table := amqp.Table{
		"x-dead-letter-exchange": "peril_dlx",
	}

	newQueue, err := channel.QueueDeclare(queueName, queueType == Durable, queueType == Transient, queueType == Transient, false, table)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not declare queue: %v", err)
	}

	err = channel.QueueBind(newQueue.Name, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not bind queue: %v", err)
	}

	return channel, newQueue, nil
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	decode func([]byte) (T, error),
	handler func(T) AckType,
) error {
	channel, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("could not declare and bind queue: %v", err)
	}

	err = channel.Qos(1, 0, false)
	if err != nil {
		return fmt.Errorf("could not set QoS: %v", err)
	}

	msgs, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("could not start consuming: %v", err)
	}

	go func() {
		defer channel.Close()
		for msg := range msgs {
			target, err := decode(msg.Body)
			if err != nil {
				log.Printf("could not decode message: %v", err)
				msg.Nack(false, false)
				continue
			}
			ackType := handler(target)
			switch ackType {
			case Ack:
				msg.Ack(false)
				log.Printf("Message acknowledged: %s", msg.Body)
			case NackRequeue:
				msg.Nack(false, true)
				log.Printf("Message negatively acknowledged and requeued: %s", msg.Body)
			case NackDiscard:
				msg.Nack(false, false)
				log.Printf("Message negatively acknowledged and discarded: %s", msg.Body)
			default:
				msg.Nack(false, false)
				log.Printf("Unknown AckType returned from handler, discarding message: %s", msg.Body)
			}
		}
	}()
	return nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	return subscribe(conn, exchange, queueName, key, queueType, jsonDecode[T], handler)
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	return subscribe(conn, exchange, queueName, key, queueType, gobDecode[T], handler)
}
