package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nsqio/go-nsq"
)

func publish(nsqdHost string, topic string, message string) {
	// Instantiate a producer.
	config := nsq.NewConfig()
	producer, err := nsq.NewProducer(nsqdHost, config)
	if err != nil {
		log.Fatal(err)
	}

	messageBody := []byte(message)

	// // REMOVE
	// producer.Ping()
	// fmt.Println("Enter message to publish:")
	// scanner := bufio.NewScanner(os.Stdin)
	// scanner.Scan()
	// //

	// Synchronously publish a single message to the specified topic.
	// Messages can also be sent asynchronously and/or in batches.
	err = producer.Publish(topic, messageBody)
	if err != nil {
		log.Fatal(err)
	}

	// Gracefully stop the producer when appropriate (e.g. before shutting down the service)
	producer.Stop()
}

func publishV2(nsqdHost string, topic string, message string) {
	// Instantiate a producer.
	config := nsq.NewConfig()
	producer, err := nsq.NewProducer(nsqdHost, config)
	if err != nil {
		log.Fatal(err)
	}

	messageBody := []byte(message)

	// // REMOVE
	// producer.Ping()
	// fmt.Println("Enter message to publish:")
	// scanner := bufio.NewScanner(os.Stdin)
	// scanner.Scan()
	// //

	// Synchronously publish a single message to the specified topic.
	// Messages can also be sent asynchronously and/or in batches.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err = producer.PublishV2(ctx, topic, messageBody)
	if err != nil {
		log.Fatal(err)
	}

	// Gracefully stop the producer when appropriate (e.g. before shutting down the service)
	producer.Stop()
}

func consume(nsqlookupdHost string, topic string, channel string) {
	// Instantiate a consumer that will subscribe to the provided channel.
	config := nsq.NewConfig()
	consumer, err := nsq.NewConsumer(topic, channel, config)
	if err != nil {
		log.Fatal(err)
	}

	// Set the message handler
	consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		log.Printf("Received message: %s", message.Body)
		return nil
	}))

	// Connect the consumer to the NSQD server.
	err = consumer.ConnectToNSQLookupd(nsqlookupdHost)
	if err != nil {
		log.Fatal(err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	consumer.Stop()
}

func main() {
	publishFlag := flag.Bool("publish", false, "publish messages")
	consumeFlag := flag.Bool("consume", false, "consume messages")
	topicFlag := flag.String("topic", "matt-test", "topic name")
	channelFlag := flag.String("channel", "matt-test-channel", "channel name")
	nsqdHostFlag := flag.String("nsqd-host", "127.0.0.1:4150", "nsqd host")
	nsqlookupdHostFlag := flag.String("nsqlookupd-host", "127.0.0.1:4161", "nsqlookupd host")
	messageFlag := flag.String("message", "hello", "message to publish")
	flag.Parse()

	if *publishFlag && *consumeFlag {
		log.Fatal("Cannot publish and consume at the same time")
	}

	if !*publishFlag && !*consumeFlag {
		log.Fatal("Must specify either -publish or -consume")
	}

	if *publishFlag {
		// publish(*nsqdHostFlag, *topicFlag, *messageFlag)
		publishV2(*nsqdHostFlag, *topicFlag, *messageFlag)
	} else {
		consume(*nsqlookupdHostFlag, *topicFlag, *channelFlag)
	}
}
