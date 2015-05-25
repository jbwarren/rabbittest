package main

import (
	"fmt"
	"github.com/jbwarren/amqputil"
	"github.com/jbwarren/syncutil"
	"github.com/streadway/amqp"
	"os"
	"os/signal"
	"runtime"
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
// runs until <quit> is signaled or <numToSend> published (if numToSend >= 0)
// outputs stats every <every> successful messages
// signals <done> when done
func startPublisher(url, queue string, numToSend, every int64, quit chan int, done *syncutil.Waiter) {
	manager := func() {
		defer done.Done() // signal when done
		//fmt.Println("PUBLISHER:  Manager routine started")

		// connect to server
		rp, err := amqputil.NewReliablePublisher(url)
		panicOnError(err, "amqputil.NewReliablePublisher()")
		defer rp.Close(10 * time.Second) // timeout

		// declare queue (create if non-existent)
		ch, err := rp.NewChannel(nil) // don't subscribe to events here
		panicOnError(err, "amqputil.ReliablePublisher().NewChannel()")
		defer ch.Close()
		// QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
		q, err := ch.QueueDeclare(queue, false, false, false, false, nil)
		panicOnError(err, "amqp.Channel.QueueDeclare()")
		fmt.Printf("PUBLISHER:  queue '%s' has %d messages to start\n", queue, q.Messages)

		// register for amqp events (on rp channel)
		connopts := &amqputil.ConnectionOptions{GetBlockings: true}
		chanopts := &amqputil.ChannelOptions{GetClosings: true, GetFlows: true, GetReturns: true}
		connevents, chanevents, err := rp.RegisterForEvents(connopts, chanopts)
		panicOnError(err, "amqputil.ReliablePublisher.RegisterForEvents()")

		// start publishing pipeline
		myquit := make(chan int)     // used to shut down pipeline
		mydone := &syncutil.Waiter{} // used to tell when pipeline is closed (esp. if numToSend satisfied)
		requests := startRequestGenerator(queue, numToSend, myquit)
		acked, nacked, err := rp.Run(requests)
		panicOnError(err, "amqputil.ReliablePublisher.Run()")
		startResultCounter(acked, nacked, every, mydone)
		mydone_ch := mydone.WaitChannel() // closed when Wait() is satisfied

		// event loop (for message-independent control events)
		// reliablePublisher handles message-specific events
	loop:
		for {
			select {
			case <-quit: // quit signaled?
				fmt.Println("PUBLISHER:  got quit signal")
				break loop
			case b := <-connevents.Blockings(): // connection blocking events (for flow control)
				fmt.Printf("PUBLISHER:  got connection blocking event:  active=%v, reason='%s'\n", b.Active, b.Reason)
			case c := <-chanevents.Closings(): // connection or channel exception
				fmt.Printf("PUBLISHER:  got connection/channel exception: %v\n", c)
				break loop
			case f := <-chanevents.Flows(): // basic.flow methods (RabbitMQ does TCP blocking instead)
				fmt.Printf("PUBLISHER:  got basic.flow=%v\n", f)
			case r := <-chanevents.Returns(): // basic.return = msg undeliverable (queue deleted?)
				fmt.Printf("PUBLISHER:  got basic.return (msg undeliverable): %s\n", r.Body)
			case <-mydone_ch: // pipeline closed (probably because numToSend satisfied)
				break loop
			}
		}

		// shut down
		close(myquit) // signal generator (head of pipeline)
		select {
		case <-mydone_ch: // wait for counter (end of pipeline)
		case <-time.After(10 * time.Second): // timeout
			fmt.Println("PUBLISHER:  ERROR:  timed out waiting for routines to exit")
		}

		// get result from ReliablePublisher
		ok := rp.Wait(0)
		switch {
		case !ok: // timeout
			fmt.Println("PUBLISHER:  ERROR:  timed out waiting for ReliablePublisher to exit")
		default:
			rperr, outstanding := rp.GetError()
			if rperr != nil {
				fmt.Println("PUBLISHER:  ERROR:  ReliablePublisher exited with error: ", rperr)
			}
			if len(outstanding) > 0 {
				fmt.Printf("PUBLISHER:  ERROR:  ReliablePublisher exited with %v messages outstanding\n", len(outstanding))
			}
		}
	}

	// start worker routine
	done.Add(1) // he who goes, adds (first)
	go manager()
}

// starts a routine to generate messages to publish
// quits after <numMsgs> msgs, unless < 0
// quits when <quit> signaled
// closes output on exit
func startRequestGenerator(queue string, numMsgs int64, quit chan int) <-chan *amqputil.PublishRequest {
	requests := make(chan *amqputil.PublishRequest) // capacity=0, to minimize the number of messages buffered in the pipeline

	messageGenerator := func() {
		defer close(requests) // close channel when done

		// run until numMsgs sent, unless < 0
		for i := int64(0); numMsgs < 0 || i < numMsgs; i++ {
			msg := &amqp.Publishing{
				DeliveryMode: amqp.Transient,
				Timestamp:    time.Now(),
				ContentType:  "text/plain",
				Body:         []byte(fmt.Sprintf("Hello! (%v)", i)),
			}
			req := &amqputil.PublishRequest{
				Exchange:  "",
				Key:       queue,
				Mandatory: false,
				Immediate: false,
				Message:   msg,
			}

			// push request onto channel (or quit)
			select {
			case <-quit: // quit signaled
				return
			case requests <- req:
				continue
			}
		}
	}

	go messageGenerator()
	return requests
}

// handles acks/nacks/errs for reliable publishing
// reads until channels closed
// signals <done> when done
func startResultCounter(acked, nacked <-chan *amqputil.PublishResult, every int64, done *syncutil.Waiter) {
	resultCounter := func() {
		defer done.Done() // signal when done

		// read from channels until all are closed
		var numAcked, numNacked, numErrs int64
		for acked != nil || nacked != nil { // we'll nil them as they're closed
			select {
			case _, ok := <-acked: // got an ack (or channel closed)
				switch {
				case !ok: // channel closed
					acked = nil
				default: // acked msg
					numAcked++
					if numAcked%every == 0 {
						fmt.Printf("PUBLISHER:  %v msgs successfully sent\n", numAcked)
					}
				}
			case pr, ok := <-nacked: // got a nack (or channel closed)
				switch {
				case !ok: // channel closed
					nacked = nil
				case pr.Error != nil: // msg error
					numErrs++
					fmt.Printf("PUBLISHER:  ERROR received attempting to send msg %v:  %s:  %s\n", pr.RequestNum, pr.Error, pr.Request.Message.Body)
				default: // nacked msg
					numNacked++ // nack
					fmt.Printf("PUBLISHER:  got a nack for msg %v:  %s\n", pr.Request.Message.Body)
				}
			}
		}

		fmt.Printf("PUBLISHER:  %v msgs acked, %v nacked, %v errors\n", numAcked, numNacked, numErrs)
	}
	done.Add(1) // he who goes, adds (first)
	go resultCounter()
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
		conn, err := amqputil.DialToConsume(url, true, true) // get cancels, closings
		panicOnError(err, "amqputil.DialToConsume()")
		defer conn.Close()
		ch := conn.Channel()

		// declare queue (create if non-existent)
		// QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
		q, err := ch.QueueDeclare(queue, false, false, false, false, nil)
		panicOnError(err, "amqp.Channel.QueueDeclare()")
		fmt.Printf("CONSUMER:  queue '%s' has %d messages to start\n", queue, q.Messages)

		// loop reading msgs
		// Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
		deliveries, err := ch.Consume(queue, "", false, false, false, true, nil)
		panicOnError(err, "amqp.Channel.Consume()")
		var num int64
	loop:
		for numToRecv < 0 || num < numToRecv { // stop after numToRecv if >= 0
			select {
			case <-quit: // quit signaled?
				fmt.Println("CONSUMER:  got quit signal")
				break loop
			case c := <-ch.CancelEvents(): // cancel event
				fmt.Printf("CONSUMER:  got basic.cancel event:  %s\n", c)
				break loop
			case c := <-ch.CloseEvents(): // connection or channel exception
				fmt.Printf("CONSUMER:  got connection/channel exception:  %v\n", c)
				break loop
			case delivery, ok := <-deliveries: // delivery (or channel closed)
				switch {
				case ok: // got a message
					err = delivery.Ack(false) // because we set autoAck to false above
					panicOnError(err, "amqp.Delivery.Ack()")
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

// drains the <counts> channel, printing every so often
// runs until <counts> channel is closed
// signals <done> when done
func consumerCounter(counts chan int64, every int64, done *syncutil.Waiter) {
	defer done.Done() // signal when done

	// drain counts
	for i := range counts {
		if i%every == 0 {
			fmt.Printf("CONSUMER:  %v msgs successfully received\n", i)
		}
	}
}

///////////////////////////////////////////
// main
///////////////////////////////////////////

const (
	URL         string = "amqp://guest:guest@localhost"
	QUEUE       string = "test"
	COUNT_EVERY int64  = 5000
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	test()
}

func test() {
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
		startPublisher(URL, QUEUE, numMsgs, COUNT_EVERY, quit, &done) // runs until told to quit if numMsgs < 0
		if wait {                                                     // wait for pipeline to be done before proceeding?
			waitWithInterrupt(&done, func() { close(quit) }) // close quit on SIGINT (perhaps multiple routines to signal)
			quit = make(chan int)                            // may be needed again below
		}
	}
	if consume {
		counts := startConsumer(URL, QUEUE, numMsgs, quit, &done) // runs until told to quit if numMsgs < 0
		done.Add(1)                                               // he who goes, adds (first)
		go consumerCounter(counts, COUNT_EVERY, &done)            // runs until <counts> closed (upstream)
	}

	// wait for everything to complete, and handle SIGINT
	waitWithInterrupt(&done, func() { close(quit) }) // close quit on SIGINT (perhaps multiple routines to signal)
}

/////////////////////////////////////////////////////////
// Helper functions
/////////////////////////////////////////////////////////

// waits for <waitfor> before returning
// handles SIGINT by calling <oninterrupt>() (and keeps waiting)
// will only call <oninterrupt>() once, even if multiple SIGINTs
func waitWithInterrupt(waitfor *syncutil.Waiter, oninterrupt func()) {
	// register for SIGINT notifications
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT)

	// wait for signal or completion
	wait_ch := waitfor.WaitChannel() // allows us to use it in select{}
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
	fmt.Printf("USAGE:  rabbittest <numMsgs> {{p|publish} | {c|consume} | {p|publish}{+|,}{c|consume}}\n")
}

func parseCmdLine() (numMsgs int64, publish, consume, wait, ok bool) {
	ok = true // set to false if we encounter an error

	args := os.Args[1:] // ignore program name
	switch {
	case len(args) < 2:
		fmt.Printf("ERROR:  too few arguments\n")
		ok = false
	case len(args) > 2:
		fmt.Printf("ERROR:  too many arguments\n")
		ok = false
	case len(args) == 2:
		// parse numMsgs
		var err error
		numMsgs, err = strconv.ParseInt(args[0], 0, 64) // allow decimal, hex, or octal
		if err != nil {
			fmt.Printf("ERROR parsing numMsgs:  %s\n", err.Error())
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
			fmt.Printf("ERROR:  invalid run mode:  '%s'\n", args[1])
			ok = false
		}
	}

	return
}

func panicOnError(err error, label string) {
	if err != nil {
		panic(fmt.Errorf("ERROR from %s:  %s", label, err))
	}
}
