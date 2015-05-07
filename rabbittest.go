package main

import (
	"fmt"
	"github.com/jbwarren/syncutil"
	"github.com/streadway/amqp"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

//////////////////////////////////////////////////////////
// Publisher
//////////////////////////////////////////////////////////

// publishes messages to queue on rabbit server at <url>
// outputs counts of successful messages read on <counts>
// runs until <quit> is signaled or <numMsgs> published (if numMsgs >= 0)
// closes <counts> when done
func startPublisher(url, queue string, numMsgs int64, quit chan int) (counts chan int64) {
	counts = make(chan int64, 1000)

	// define worker routine
	publisher := func() {
		defer close(counts) // signal when done

		// connect to server
		conn := dial(url)
		defer conn.Close()
		ch := channel(conn)
		defer ch.Close()

		// declare queue (create if non-existent)
		// QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
		q := queuedeclare(ch, queue, false, false, false, false, nil)
		fmt.Printf("PUBLISHER:  queue '%s' has %d messages to start\n", queue, q.Messages)

		// loop, publishing messages
		var i int64
	loop:
		for numMsgs < 0 || i < numMsgs { // stop after numMsgs if >=0
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
				i++
				counts <- i
			}
		}

		fmt.Printf("PUBLISHER:  %v msgs sent successfully; exiting\n", i)
	}

	// start worker routine
	go publisher()
	return
}

//////////////////////////////////////////////////////////
// Consumer
//////////////////////////////////////////////////////////

// consumes messages from queue on rabbit server at <url>
// outputs counts of successful messages read on <counts>
// runs until <quit> is signaled or <numMsgs> read (if numMsgs >= 0)
// closes <counts> when done
func startConsumer(url, queue string, numMsgs int64, quit chan int) (counts chan int64) {
	counts = make(chan int64, 1000)

	// define worker routine
	consumer := func() {
		defer close(counts) // close when done

		// connect to server
		conn := dial(url)
		defer conn.Close()
		ch := channel(conn)
		defer ch.Close()

		// declare queue (create if non-existent)
		// QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
		q := queuedeclare(ch, queue, false, false, false, false, nil)
		fmt.Printf("CONSUMER:  queue '%s' has %d messages to start\n", queue, q.Messages)

		// Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
		deliveries := consume(ch, queue, "", false, false, false, true, nil)
		// read msgs until told to quit
		var i int64
	loop:
		for numMsgs < 0 || i < numMsgs { // stop after numMsgs if >= 0
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
// counter
///////////////////////////////////////////

// drains the <counts> channel, printing every so often
// runs until <counts> channel is closed
// signals <done> when done
func counter(counts chan int64, every int64, label string, done *syncutil.Waiter) {
	defer done.Done() // signal when done

	// drain counts
	for i := range counts {
		if i%every == 0 {
			fmt.Printf("STATS:  %v msgs successfully %s\n", i, label)
		}
	}
}

///////////////////////////////////////////
// main
///////////////////////////////////////////

func main() {
	const URL string = "amqp://guest:guest@localhost"
	const QUEUE string = "test"
	const COUNT_EVERY int64 = 1000

	// get cmdline options
	numMsgs, publish, consume, wait, ok := parseCmdLine()
	if !ok {
		printUsage()
		return
	}

	// start pipelines to publish/consume
	quit := make(chan int)
	var done syncutil.Waiter
	if publish {
		counts := startPublisher(URL, QUEUE, numMsgs, quit) // runs until told to quit if numMsgs < 0
		done.Add(1)                                         // he who goes, adds (first)
		go counter(counts, COUNT_EVERY, "published", &done) // runs until <counts> closed (upstream)
		if wait {
			// wait for pipeline to be done before proceeding
			waitWithInterrupt(&done, func() { quit <- 0 }) // signal quit on SIGINT (only one routine to signal, and we may need it later)
		}
	}
	if consume {
		counts := startConsumer(URL, QUEUE, numMsgs, quit) // runs until told to quit if numMsgs < 0
		done.Add(1)                                        // he who goes, adds (first)
		go counter(counts, COUNT_EVERY, "consumed", &done) // runs until <counts> closed (upstream)
	}

	// wait for everything to complete, and handle SIGINT
	waitWithInterrupt(&done, func() { close(quit) }) // close quit on SIGINT (perhaps multiple routines to signal)
}

// waits for <waitfor> before returning
// handles SIGINT by calling <oninterrupt>() (and keeps waiting)
// will only call <oninterrupt>() once, even if multiple SIGINTs
func waitWithInterrupt(waitfor *syncutil.Waiter, oninterrupt func()) {
	// register for SIGINT notifications
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT)

	// wait for signal or completion
	wait_ch := waitfor.Channel() // allows us to use it in select{}
	var once sync.Once
	for {
		select {
		case <-sigs: // SIGINT
			once.Do(func() {
				fmt.Println(" Got SIGINT...")
				oninterrupt()
			})
		case <-wait_ch: // done
			return
		}
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "USAGE:  rabbittest <numMsgs> {{p|publish} | {c|consume} | {p|publish}{+|,}{c|consume}}\n")
}

func parseCmdLine() (numMsgs int64, publish, consume, wait, ok bool) {
	ok = true // set to false if we encounter an error

	args := os.Args[1:] // ignore program name
	switch {
	case len(args) < 2:
		fmt.Fprintf(os.Stderr, "ERROR:  too few arguments\n")
		ok = false
	case len(args) > 2:
		fmt.Fprintf(os.Stderr, "ERROR:  too many arguments\n")
		ok = false
	case len(args) == 2:
		// parse numMsgs
		var err error
		numMsgs, err = strconv.ParseInt(args[0], 0, 64) // allow decimal, hex, or octal
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR parsing numMsgs:  %s\n", err.Error())
			ok = false
		}

		// parse run mode
		switch strings.ToUpper(args[1]) {
		case "P", "PUBLISH":
			publish, consume, wait = true, false, false
		case "C", "CONSUME":
			publish, consume, wait = false, true, false
		case "P,C", "P,CONSUME", "PUBLISH,C", "PUBLISH,CONSUME":
			publish, consume, wait = true, true, true
		case "P+C", "P+CONSUME", "PUBLISH+C", "PUBLISH+CONSUME":
			publish, consume, wait = true, true, false
		default:
			fmt.Fprintf(os.Stderr, "ERROR:  invalid run mode:  '%s'\n", args[1])
			ok = false
		}
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

func queueinspect(ch *amqp.Channel, name string) (queue amqp.Queue) {
	queue, err := ch.QueueInspect(name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR from amqp.Channel.QueueInspect():  %s\n", err.Error())
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
