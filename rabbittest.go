package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"os"
	"strconv"
	"strings"
	"time"
)

//////////////////////////////////////////////////////////
// Publisher
//////////////////////////////////////////////////////////

// publishes messages to queue on rabbit server at <url>
// runs until <quit> is signaled or <numMsgs> published (if numMsgs > 0)
// signals <done> when done
func publisher(url, queue string, numMsgs uint64, quit, done chan int) {
	defer func() { done <- 0 }() // signal when done

	// connect to server
	conn := dial(url)
	defer conn.Close()
	ch := channel(conn)
	defer ch.Close()

	// declare queue (create if non-existent)
	// QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
	_ = queuedeclare(ch, queue, false, false, false, false, nil)

	// loop, publishing messages
	var i uint64
loop:
	for ; numMsgs > 0 && i < numMsgs; i++ { // stop after numMsgs if >0
		select {
		case <-quit: // quit signaled?
			fmt.Println("PUBLISHER:  got quit signal")
			break loop // not just the select
		default: // publish msg
			msg := amqp.Publishing{
				DeliveryMode: amqp.Transient,
				Timestamp:    time.Now(),
				ContentType:  "text/plain",
				Body:         []byte(fmt.Sprintf("Hello! (%v)", i)),
			}
			// Publish(exchange, key, mandatory, immediate, msg)
			publish(ch, "", queue, true, false, msg)
		}
	}

	fmt.Printf("PUBLISHER:  %v msgs sent successfully; exiting\n", i)
}

//////////////////////////////////////////////////////////
// Consumer
//////////////////////////////////////////////////////////

// consumes messages from queue on rabbit server at <url>
// outputs counts of successful messages read on <counts>
// runs until <quit> is signaled
// signals <done> when done
func startConsumer(url, queue string, quit, done chan int) (counts chan uint64) {
	counts = make(chan uint64, 100)

	// define worker routine
	consumer := func() {
		defer func() { done <- 0 }() // signal when done

		// connect to server
		conn := dial(url)
		defer conn.Close()
		ch := channel(conn)
		defer ch.Close()

		// Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
		deliveries := consume(ch, queue, "", false, false, false, true, nil)
		// read msgs until told to quit
		var i uint64
	loop:
		for {
			select {
			case <-quit: // quit signaled?
				fmt.Println("CONSUMER:  got quit signal")
				break loop // not just the select
			case delivery := <-deliveries:
				//fmt.Printf("Received message (%s):  %s\n", delivery.Timestamp, delivery.Body)
				ack(&delivery, false) // we set autoAck to false above
				i++
				counts <- i
			}
		}

		fmt.Printf("CONSUMER:  %v msgs received successfully; exiting\n", i)
	}

	// start worker routine
	go consumer()
	return
}

///////////////////////////////////////////
// main
///////////////////////////////////////////

func main() {
	const URL string = "amqp://guest:guest@localhost"
	const QUEUE string = "test"

	// get cmdline options
	numMsgs, publish, consume, wait, ok := parseCmdLine()
	if !ok {
		printUsage()
		return
	}

	// start routines
	quit, done := make(chan int), make(chan int)
	if publish {
		go publisher(URL, QUEUE, numMsgs, quit, done)
		if wait {
			<-done // wait for publisher to quit
		}
	}
	if consume {
		counts := startConsumer(URL, QUEUE, quit, done)

		// drain counts
		for i := range counts {
			if i%1000 == 0 {
				fmt.Printf("Got %v msgs\n", i)
			}
			if i >= numMsgs {
				break
			}
		}
	}

	// shut down
	if publish && !wait { // only wait for publisher to quit if it's running and we didn't already wait
		<-done // wait for publisher to quit
	}
	if consume {
		close(quit) // signal consumer to quit (publisher should have quit by now)
		<-done      // wait for consumer to quit
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "USAGE:  rabbittest [<numMsgs> [publish|consume|publish{+|,}consume]]\n")
}

func parseCmdLine() (numMsgs uint64, publish, consume, wait, ok bool) {
	// defaults
	numMsgs = 10 * 1000
	publish = true
	consume = true
	wait = false
	ok = true

	args := os.Args[1:] // ignore program name
	if len(args) >= 1 { // parse numMsgs
		var err error
		numMsgs, err = strconv.ParseUint(args[0], 0, 64) // allow decimal, hex, or octal
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR parsing numMsgs:  %s\n", err.Error())
			ok = false
		}
	}
	if len(args) >= 2 { // parse run mode
		switch strings.ToUpper(args[1]) {
		case "PUBLISH":
			publish, consume, wait = true, false, false
		case "CONSUME":
			publish, consume, wait = false, true, false
		case "PUBLISH,CONSUME":
			publish, consume, wait = true, true, true
		case "PUBLISH+CONSUME", "PUBLISH&CONSUME", "CONSUME+PUBLISH", "CONSUME&PUBLISH":
			publish, consume, wait = true, true, false
		default:
			fmt.Fprintf(os.Stderr, "ERROR:  invalid run mode:  '%s'\n", args[1])
			ok = false
		}
	}
	if len(args) > 2 {
		fmt.Fprintf(os.Stderr, "ERROR:  too many arguments\n")
		ok = false
	}

	return
}

///////////////////////////////////////////////////////////////////////////
// Helper functions:  wrappers with simple error handling
///////////////////////////////////////////////////////////////////////////

func dial(url string) (conn *amqp.Connection) {
	conn, err := amqp.Dial(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR from amqp.Dial():  %s\n", err.Error())
		panic(err)
	}
	return
}

func channel(conn *amqp.Connection) (ch *amqp.Channel) {
	ch, err := conn.Channel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR from amqp.Connection.Channel():  %s\n", err.Error())
		panic(err)
	}
	return
}

func queuedeclare(ch *amqp.Channel, name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (queue amqp.Queue) {
	queue, err := ch.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR from amqp.Channel.QueueDeclare():  %s\n", err.Error())
		panic(err)
	}
	return
}

func publish(ch *amqp.Channel, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) {
	err := ch.Publish(exchange, key, mandatory, immediate, msg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR from amqp.Channel.Publish():  %s\n", err.Error())
		panic(err)
	}
}

func consume(ch *amqp.Channel, queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (deliveries <-chan amqp.Delivery) {
	deliveries, err := ch.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR from amqp.Channel.Consume():  %s\n", err.Error())
		panic(err)
	}
	return
}

func ack(delivery *amqp.Delivery, multiple bool) {
	err := delivery.Ack(multiple)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR from amqp.Delivery.Ack():  %s\n", err.Error())
	}
}
