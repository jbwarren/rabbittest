package main

import (
	"fmt"
	"github.com/jbwarren/amqputil"
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
// runs until <quit> is signaled or <numToSend> published (if numToSend >= 0)
// closes <counts> and signals <done> when done
func startPublisher(url, queue string, numToSend int64, quit chan int, done *syncutil.Waiter) (counts chan int64) {
	counts = make(chan int64, 1000)

	// define worker routine
	manager := func() {
		defer close(counts) // close when done
		defer done.Done()   // signal when done

		// connect to server
		// dial(url, blockings, cancels, closings, flows, returns, confirms)
		conn := dial(url, true, true, true, true, true, 1000)
		defer conn.Close()
		ch := conn.Channel

		// watch for server blocking (TCP flow control) in separate routine
		done.Add(1)                                         // he who goes, adds (first)
		go blockingAgent("PUBLISHER", conn.Blockings, done) // blockings channel will close when connection closed

		// declare queue (create if non-existent)
		// QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
		q := queuedeclare(ch, queue, false, false, false, false, nil)
		fmt.Printf("PUBLISHER:  queue '%s' has %d messages to start\n", queue, q.Messages)

		// start publisher routine
		// so we can handle events while Publish() call is blocking
		// lock-step signaling (req/rep pattern)
		msgs := make(chan *amqp.Publishing) // capacity=0 so we're never queuing requests internally
		presults := make(chan error, 1)     // capacity=1 so we can queue one response (so publisher can exit after event loop exits)
		defer close(msgs)                   // will signal routine to stop
		done.Add(1)                         // he who goes, adds (first)
		go publisher(ch, queue, msgs, presults, done)

		// for managing event loop
		var num int64 = 0                        // num successfully sent
		pausing := false                         // in flow control?
		ready := true                            // publisher ready for msg?
		handlePublishResult := func(err error) { // so we can use it both in and after the event loop
			ready = true // for next publish
			switch {
			case err == nil: // successful publish
				num++
				counts <- num
			case err != nil: // error
				fmt.Printf("PUBLISHER:  got error from Publish(): %s\n", err.Error())
			}
		}

		// event loop
	loop:
		for numToSend < 0 || num < numToSend { // stop after numToSend if >=0
			select {
			case <-quit: // quit signaled?
				fmt.Println("PUBLISHER:  got quit signal")
				break loop
			case c := <-ch.Cancels: // basic.cancel on channel
				fmt.Printf("PUBLISHER:  got basic.cancel on '%s'\n", c)
				break loop
			case c := <-ch.Closings: // connection or channel exception
				fmt.Printf("PUBLISHER:  got connection/channel exception: %v\n", c)
				break loop
			case f := <-ch.Flows: // basic.flow methods (RabbitMQ does TCP blocking instead)
				if f {
					fmt.Println("PUBLISHER:  got basic.flow=true; pausing...")
				} else {
					fmt.Println("PUBLISHER:  got basic.flow=false; resuming...")
				}
				pausing = f
			case r := <-ch.Returns: // basic.return = msg undeliverable
				fmt.Printf("PUBLISHER:  got basic.return (msg undeliverable): %s\n", r.Body)
			case a := <-ch.Acks: // basic.ack (seq#)
				fmt.Printf("PUBLISHER:  got basic.ack (%v)\n", a)
			case n := <-ch.Nacks: // basic.nack (seq#)
				fmt.Printf("PUBLISHER:  got basic.nack (%v)\n", n)
			case e := <-presults: // result from publisher
				handlePublishResult(e)
			default: // no signals/events
				switch {
				case pausing:
					time.Sleep(time.Millisecond)
				case !ready:
					time.Sleep(time.Microsecond)
				default:
					msgs <- &amqp.Publishing{
						DeliveryMode: amqp.Transient,
						Timestamp:    time.Now(),
						ContentType:  "text/plain",
						Body:         []byte(fmt.Sprintf("Hello! (%v)", num)),
					}
					ready = false
				}
			}
		}

		// get the last result from publisher
		if !ready { // waiting for publisher?
			e := <-presults
			handlePublishResult(e)
		}

		fmt.Printf("PUBLISHER:  %v msgs sent successfully; exiting\n", num)
	}

	// start worker routine
	done.Add(1) // he who goes, adds (first)
	go manager()
	return
}

// watches for connection blocking events
// reads until <blockings> channel is closed (probably at connection close)
// signals <done> when done
func blockingAgent(label string, blockings chan amqp.Blocking, done *syncutil.Waiter) {
	defer done.Done() // signal when done

	// watch flor blocking events
	for b := range blockings {
		fmt.Printf("%s:  got connection Blocking event, active=%v, reason='%s'\n", label, b.Active, b.Reason)
	}
}

// publishes to queue
// reads input from <msgs>, writes results to <errors>
// reads until <msgs> closed
// signals <done> on exit
func publisher(ch *amqputil.Channel, queue string, msgs chan *amqp.Publishing, errs chan error, done *syncutil.Waiter) {
	defer done.Done() // signal when done

	// publish from input
	for msg := range msgs {
		// Publish(exchange, key, mandatory, immediate, msg)
		errs <- ch.Publish("", queue, true, false, *msg)
	}
}

//////////////////////////////////////////////////////////
// Consumer
//////////////////////////////////////////////////////////

// consumes messages from queue on rabbit server at <url>
// outputs counts of successful messages read on <counts>
// runs until <quit> is signaled or <numToRecv> read (if numToRecv >= 0)
// closes <counts> and signals <done> when done
func startConsumer(url, queue string, numToRecv int64, quit chan int, done *syncutil.Waiter) (counts chan int64) {
	counts = make(chan int64, 1000)

	// define worker routine
	consumer := func() {
		defer close(counts) // close when done
		defer done.Done()   // signal when done

		// connect to server
		// dialtoconsume(url, closings)
		conn := dialtoconsume(url, true)
		defer conn.Close()
		ch := conn.Channel

		// declare queue (create if non-existent)
		// QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
		q := queuedeclare(ch, queue, false, false, false, false, nil)
		fmt.Printf("CONSUMER:  queue '%s' has %d messages to start\n", queue, q.Messages)

		// loop reading msgs
		// Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
		deliveries := consume(ch, queue, "", false, false, false, true, nil)
		var num int64
	loop:
		for numToRecv < 0 || num < numToRecv { // stop after numToRecv if >= 0
			select {
			case <-quit: // quit signaled?
				fmt.Println("CONSUMER:  got quit signal")
				break loop
			case c := <-ch.Closings: // connection or channel exception
				fmt.Printf("CONSUMER:  got connection/channel exception: %v\n", c)
				break loop
			case delivery, ok := <-deliveries:
				//fmt.Printf("Received message (%s):  %s\n", delivery.Timestamp, delivery.Body)
				switch {
				case ok: // got a message
					ack(&delivery, false) // we set autoAck to false above
					num++
					counts <- num
				case !ok: // channel closed (we'll usually get exception on Closings first)
					fmt.Println("CONSUMER:  delivery channel closed")
					break loop
				}
			}
		}

		fmt.Printf("CONSUMER:  %v msgs received successfully; exiting\n", num)
	}

	// start worker routine
	done.Add(1) // he who goes, adds (first)
	go consumer()
	return
}

///////////////////////////////////////////
// helpers
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
	const COUNT_EVERY int64 = 5000

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
		counts := startPublisher(URL, QUEUE, numMsgs, quit, &done) // runs until told to quit if numMsgs < 0
		done.Add(1)                                                // he who goes, adds (first)
		go counter(counts, COUNT_EVERY, "published", &done)        // runs until <counts> closed (upstream)
		if wait {
			// wait for pipeline to be done before proceeding
			waitWithInterrupt(&done, func() { quit <- 0 }) // signal quit on SIGINT (only one routine to signal, and we may need it later)
		}
	}
	if consume {
		counts := startConsumer(URL, QUEUE, numMsgs, quit, &done) // runs until told to quit if numMsgs < 0
		done.Add(1)                                               // he who goes, adds (first)
		go counter(counts, COUNT_EVERY, "consumed", &done)        // runs until <counts> closed (upstream)
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

func dial(url string, blockings, cancels, closings, flows, returns bool, confirms int) (conn *amqputil.Connection) {
	conn, derr, cherr := amqputil.Dial(url, blockings, cancels, closings, flows, returns, confirms)
	if derr != nil {
		fmt.Fprintf(os.Stderr, "ERROR from amqp.Dial():  %s\n", derr.Error())
		panic(derr)
	}
	if cherr != nil {
		fmt.Fprintf(os.Stderr, "ERROR from amqp.Connection.Channel():  %s\n", cherr.Error())
		panic(cherr)
	}
	return
}

func dialtoconsume(url string, closings bool) (conn *amqputil.Connection) {
	return dial(url, false, false, closings, false, false, -1)
}

func queuedeclare(ch *amqputil.Channel, name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (queue amqp.Queue) {
	queue, err := ch.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR from amqp.Channel.QueueDeclare():  %s\n", err.Error())
		panic(err)
	}
	return
}

func queueinspect(ch *amqputil.Channel, name string) (queue amqp.Queue) {
	queue, err := ch.QueueInspect(name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR from amqp.Channel.QueueInspect():  %s\n", err.Error())
		panic(err)
	}
	return
}

func publish(ch *amqputil.Channel, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) {
	err := ch.Publish(exchange, key, mandatory, immediate, msg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR from amqp.Channel.Publish():  %s\n", err.Error())
		panic(err)
	}
}

func consume(ch *amqputil.Channel, queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (deliveries <-chan amqp.Delivery) {
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
