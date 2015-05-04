package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"time"
)

func main() {
	// open connection to rabbit server
	pubconn := dial("amqp://guest:guest@localhost")
	defer pubconn.Close()
	fmt.Println("Got connection for publishing")

	// get connection channel
	pubch := channel(pubconn)
	defer pubch.Close()
	fmt.Println("Got channel for publishing")

	// declare queue (create if non-existent)
	// QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
	queue := queuedeclare(pubch, "test", false, false, false, false, nil)
	fmt.Println("Created queue 'test': ", queue)

	// create message to publish
	msg := amqp.Publishing{
		DeliveryMode: amqp.Transient,
		Timestamp:    time.Now(),
		ContentType:  "text/plain",
		Body:         []byte("Hello!"),
	}

	// Publish(exchange, key, mandatory, immediate, msg)
	publish(pubch, "", "test", true, false, msg)
	fmt.Printf("Published message:  %s\n", msg.Body)

	// open connection to rabbit server
	conconn := dial("amqp://guest:guest@localhost")
	defer conconn.Close()
	fmt.Println("Got connection for consuming")

	// get connection channel
	conch := channel(conconn)
	defer conch.Close()
	fmt.Println("Got channel for consuming")

	// Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
	deliveries := consume(conch, "test", "tester", false, false, false, false, nil)
	fmt.Println("Got delivery channel")

	// read some msgs
	quit, done := make(chan int), make(chan int)
	reader := func() {
		defer func() { done <- 0 }() // signal when done

		// read msgs until told to quit
		for {
			select {
			case <-quit:
				return
			case delivery := <-deliveries:
				fmt.Printf("Received message (%s):  %s\n", delivery.Timestamp, delivery.Body)
				ack(&delivery, false) // we set autoAck to false above
				fmt.Println("Message acked")
			}
		}
	}
	go reader()
	time.Sleep(time.Second)

	// shut down
	quit <- 0 // signal reader to quit
	<-done    // wait for reader to quit
}

///////////////////////////////////////////////////////////////////////////
// Helper functions:  wrappers with simple error handling
///////////////////////////////////////////////////////////////////////////

func dial(url string) (conn *amqp.Connection) {
	conn, err := amqp.Dial(url)
	if err != nil {
		fmt.Errorf("ERROR from amqp.Dial():  %s\n", err.Error())
		panic(err)
	}
	return
}

func channel(conn *amqp.Connection) (ch *amqp.Channel) {
	ch, err := conn.Channel()
	if err != nil {
		fmt.Errorf("ERROR from amqp.Connection.Channel():  %s\n", err.Error())
		panic(err)
	}
	return
}

func queuedeclare(ch *amqp.Channel, name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (queue amqp.Queue) {
	queue, err := ch.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
	if err != nil {
		fmt.Errorf("ERROR from amqp.Channel.QueueDeclare():  %s\n", err.Error())
		panic(err)
	}
	return
}

func publish(ch *amqp.Channel, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) {
	err := ch.Publish(exchange, key, mandatory, immediate, msg)
	if err != nil {
		fmt.Errorf("ERROR from amqp.Channel.Publish():  %s\n", err.Error())
		panic(err)
	}
}

func consume(ch *amqp.Channel, queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (deliveries <-chan amqp.Delivery) {
	deliveries, err := ch.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
	if err != nil {
		fmt.Errorf("ERROR from amqp.Channel.Consume():  %s\n", err.Error())
		panic(err)
	}
	return
}

func ack(delivery *amqp.Delivery, multiple bool) {
	err := delivery.Ack(multiple)
	if err != nil {
		fmt.Errorf("ERROR from amqp.Delivery.Ack():  %s\n", err.Error())
	}
}
