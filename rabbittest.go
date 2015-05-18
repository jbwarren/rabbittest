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
		// dial(url, blockings, cancels, closings, flows, returns, confirms)
		conn := dial(url, true, true, true, true, true, 1000)
		defer conn.Close()
		ch := conn.Channel
		// declare queue (create if non-existent)
		// QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
		q := queuedeclare(ch, queue, false, false, false, false, nil)
		fmt.Printf("PUBLISHER:  queue '%s' has %d messages to start\n", queue, q.Messages)

		// watch for server blocking (TCP flow control) in separate routine
		done.Add(1)                            // he who goes, adds (first)
		go blockingAgent(conn.Blockings, done) // blockings channel will close when connection closed

		// start publishing pipeline
		myquit := make(chan int)     // used to shut down pipeline
		mydone := &syncutil.Waiter{} // used to tell when pipeline is closed (esp. if numToSend satisfied)
		msgs := startMessageGenerator(numToSend, myquit, mydone)
		acked, nacked, errs := startReliablePublisher(ch, queue, msgs, mydone)
		startResultCounter(acked, nacked, errs, every, mydone)
		mydone_ch := mydone.WaitChannel() // closed when Wait() is satisfied

		// event loop
	loop:
		for {
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
				fmt.Printf("PUBLISHER:  got basic.flow=%v\n", f)
			case r := <-ch.Returns: // basic.return = msg undeliverable (queue deleted?)
				fmt.Printf("PUBLISHER:  got basic.return (msg undeliverable): %s\n", r.Body)
			case <-mydone_ch: // pipeline closed (probably because numToSend satisfied)
				break loop
			}
		}

		// shut down
		close(myquit) // signal generator (head of pipeline)
		select {
		case <-mydone_ch: // wait for everyone
		case <-time.NewTimer(10 * time.Second).C: // timeout
			fmt.Println("PUBLISHER:  ERROR:  timed out waiting for routines to exit")
		}
		//fmt.Println("PUBLISHER:  Manager routine exiting")
	}

	// start worker routine
	done.Add(1) // he who goes, adds (first)
	go manager()
}

// watches for connection blocking events
// reads until <blockings> channel is closed (probably at connection close)
// signals <done> when done
func blockingAgent(blockings chan amqp.Blocking, done *syncutil.Waiter) {
	defer done.Done() // signal when done
	//fmt.Println("PUBLISHER:  blocking agent routine started")

	// watch flor blocking events
	for b := range blockings {
		fmt.Printf("PUBLISHER:  got connection Blocking event, active=%v, reason='%s'\n", b.Active, b.Reason)
	}
	//fmt.Println("PUBLISHER:  blocking agent routine exiting")
}

// starts a routine to generate messages to publish
// quits after <numMsgs> msgs, unless < 0
// quits when <quit> signaled
// signals <done> when done
func startMessageGenerator(numMsgs int64, quit chan int, done *syncutil.Waiter) (msgs chan *amqp.Publishing) {
	msgs = make(chan *amqp.Publishing) // capacity=0, to minimize the number of messages buffered in the pipeline

	messageGenerator := func() {
		defer done.Done() // signal when done
		defer close(msgs) // close channel when done
		//fmt.Println("PUBLISHER:  message generator routine started")

		// run until numMsgs sent, unless < 0
		for i := int64(0); numMsgs < 0 || i < numMsgs; i++ {
			msg := &amqp.Publishing{
				DeliveryMode: amqp.Transient,
				Timestamp:    time.Now(),
				ContentType:  "text/plain",
				Body:         []byte(fmt.Sprintf("Hello! (%v)", i)),
			}
			select {
			case <-quit: // quit signaled
				return
			case msgs <- msg:
				continue
			}
		}
		//fmt.Println("PUBLISHER:  message generator routine exiting")
	}

	done.Add(1) // he who goes, adds (first)
	go messageGenerator()
	return
}

// for returning publishing errors
type pubResult struct {
	E error
	P *amqp.Publishing
}

// starts a goroutine to handle reliable publishing
// this routine must be the first to publish anything on the channel after it is put into Confirm mode
// will handle the Acks & Nacks channels on <ch> (nobody else should)
// takes input off <msgs>, and writes output to <acked> & <nacked>
// writes publishing errors to <errs>
// runs until <msgs> is closed, and signals <done> on exit
func startReliablePublisher(ch *amqputil.Channel, queue string, msgs chan *amqp.Publishing, done *syncutil.Waiter) (acked, nacked chan *amqp.Publishing, errs chan *pubResult) {
	acked = make(chan *amqp.Publishing, 100)
	nacked = make(chan *amqp.Publishing, 100)
	errs = make(chan *pubResult, 100)

	// define worker routine
	reliablePublisher := func() {
		defer done.Done() // signal done on exit
		defer close(acked)
		defer close(nacked)
		defer close(errs)
		//fmt.Println("PUBLISHER:  reliable publisher routine started")

		// for managing publisher routine
		// lock-step signaling (req/rep pattern)
		topublish := make(chan *amqp.Publishing) // capacity=0 so we're never queuing requests internally
		pubresults := make(chan error, 1)        // capacity=1 so we can queue one response (so publisher can always exit)
		defer close(topublish)                   // will signal routine to stop

		// define raw publisher routine (because Publish() may block)
		// reads input from <msgs>, writes results to <errors>
		// reads until <msgs> closed
		// signals <done> on exit
		publisher := func() {
			defer done.Done() // signal when done
			//fmt.Println("PUBLISHER:  raw publisher routine started")
			for p := range topublish {
				// Publish(exchange, key, mandatory, immediate, msg)
				pubresults <- ch.Publish("", queue, true, false, *p) // may block
			}
			//fmt.Println("PUBLISHER:  raw publisher routine exiting")
		}
		done.Add(1) // he who goes, adds (first)
		go publisher()

		// loop, publishing messages
		var lastseqnum uint64 = 0                    // first is 1
		outstanding := map[uint64]*amqp.Publishing{} // keyed on sequence number
		ready := true                                // publish routine ready?
		quitting := false
	loop:
		for {
			select {
			case acknum, ok := <-ch.Acks: // got an ack (or ack channel closed)
				if ok { // ack
					msg, ok := outstanding[acknum] // get message acked
					if ok {
						delete(outstanding, acknum) // remove it from the map
						acked <- msg                // send it to output channel
					} else { // not found
						fmt.Println("PUBLISHER:  ERROR:  received unexpected ack:", acknum)
					}
				} else { // ack channel closed (probably resulting from channel close exception)
					fmt.Println("PUBLISHER:  ack channel closed unexpectedly")
					break loop
				}
			case nacknum, ok := <-ch.Nacks: // got a nack (or nack channel closed)
				if ok { // nack
					msg, ok := outstanding[nacknum] // get message nacked
					if ok {
						delete(outstanding, nacknum) // remove it from the map
						nacked <- msg                // send it to output channel
					} else { // not found
						fmt.Println("PUBLISHER:  ERROR:  received unexpected nack:", nacknum)
					}
				} else { // nack channel closed (probably resulting from channel close exception)
					fmt.Println("PUBLISHER:  ERROR:  nack channel closed unexpectedly")
					break loop
				}
			case err := <-pubresults: // got a result from publisher
				ready = true    // for next publish
				if err != nil { // error
					msg := outstanding[lastseqnum]  // get last message
					delete(outstanding, lastseqnum) // remove it from the map
					errs <- &pubResult{err, msg}    // send it to error output channel
				}
			default: // no acks/nacks/pubresults
				switch {
				case quitting: // still have to handle acks/nacks in flight
					if len(outstanding) == 0 { // done
						break loop
					} else { // keep going
						time.Sleep(time.Millisecond)
					}
				case !ready: // waiting for a response from publisher routine
					time.Sleep(time.Microsecond) // and continue
				default: // running & ready
					select { // try to get next msg to publish
					case msg, ok := <-msgs: // got one (or channel closed)
						if ok { // got one
							topublish <- msg              // send it to publisher
							ready = false                 // publisher busy
							lastseqnum++                  // update seqnum
							outstanding[lastseqnum] = msg // put it in the map
						} else { // input closed
							quitting = true // continue looping to handle remaining acks/nacks
							fmt.Printf("PUBLISHER:  handling acks for %v outstanding msgs before exiting\n", len(outstanding))
						}
					}
				}
			}
		}

		if len(outstanding) > 0 {
			fmt.Printf("PUBLISHER:  WARNING:  exiting with %v msgs not acked/nacked\n", len(outstanding))
		}
		//fmt.Println("PUBLISHER:  reliable publisher routine exiting")
	}

	// start worker routine
	done.Add(1) // he who goes, adds (first)
	go reliablePublisher()
	return
}

// handles acks/nacks/errs for reliable publishing
// reads until channels closed
// signals <done> when done
func startResultCounter(acked, nacked chan *amqp.Publishing, errs chan *pubResult, every int64, done *syncutil.Waiter) {
	resultCounter := func() {
		defer done.Done() // signal when done
		//fmt.Println("PUBLISHER:  result counter routine started")

		// read from channels until all are closed
		var numAcks, numNacks, numErrs int64
		for acked != nil || nacked != nil || errs != nil { // we'll nil them as they're closed
			select {
			case _, ok := <-acked: // got an ack (or channel closed)
				if ok {
					numAcks++ // ack
					if numAcks%every == 0 {
						fmt.Printf("PUBLISHER:  %v msgs successfully sent\n", numAcks)
					}
				} else {
					acked = nil // closed
				}
			case p, ok := <-nacked: // got a nack (or channel closed)
				if ok {
					numNacks++ // nack
					fmt.Printf("PUBLISHER:  got a nack for msg:  %s\n", p.Body)
				} else {
					nacked = nil // closed
				}
			case pr, ok := <-errs: // got an error (or channed closed)
				if ok {
					numErrs++ // error
					fmt.Printf("PUBLISHER:  ERROR received attempting to send msg:  %s:  %s\n", pr.E.Error(), pr.P.Body)
				} else {
					errs = nil // closed
				}
			}
		}

		fmt.Printf("PUBLISHER:  %v msgs acked, %v nacked, %v errors\n", numAcks, numNacks, numErrs)
		//fmt.Println("PUBLISHER:  result counter routine exiting")
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

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

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

///////////////////////////////////////////////////////////////////////////
// Helper functions:  wrappers with simple error handling
///////////////////////////////////////////////////////////////////////////

func dial(url string, blockings, cancels, closings, flows, returns bool, confirms int) (conn *amqputil.Connection) {
	conn, derr, cherr := amqputil.Dial(url, blockings, cancels, closings, flows, returns, confirms)
	if derr != nil {
		fmt.Printf("ERROR from amqp.Dial():  %s\n", derr.Error())
		panic(derr)
	}
	if cherr != nil {
		fmt.Printf("ERROR from amqp.Connection.Channel():  %s\n", cherr.Error())
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
		fmt.Printf("ERROR from amqp.Channel.QueueDeclare():  %s\n", err.Error())
		panic(err)
	}
	return
}

func queueinspect(ch *amqputil.Channel, name string) (queue amqp.Queue) {
	queue, err := ch.QueueInspect(name)
	if err != nil {
		fmt.Printf("ERROR from amqp.Channel.QueueInspect():  %s\n", err.Error())
		panic(err)
	}
	return
}

func publish(ch *amqputil.Channel, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) {
	err := ch.Publish(exchange, key, mandatory, immediate, msg)
	if err != nil {
		fmt.Printf("ERROR from amqp.Channel.Publish():  %s\n", err.Error())
		panic(err)
	}
}

func consume(ch *amqputil.Channel, queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (deliveries <-chan amqp.Delivery) {
	deliveries, err := ch.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
	if err != nil {
		fmt.Printf("ERROR from amqp.Channel.Consume():  %s\n", err.Error())
		panic(err)
	}
	return
}

func ack(delivery *amqp.Delivery, multiple bool) {
	err := delivery.Ack(multiple)
	if err != nil {
		fmt.Printf("ERROR from amqp.Delivery.Ack():  %s\n", err.Error())
	}
}
