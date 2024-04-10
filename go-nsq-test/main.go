package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nsqio/go-nsq"
)

func testPublish(id int, errorChan chan<- error, producer *nsq.Producer, topic string, messageBody []byte, timeout int) {
	// fmt.Println(fmt.Sprintf("%d: Publishing message", id))
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Microsecond)
	defer cancel()
	err := producer.PublishV2(ctx, topic, messageBody)
	if err == nil {
		fmt.Println(fmt.Sprintf("%d: Published message", id))
	}
	wrappedErr := fmt.Errorf("%d: %w", id, err)
	errorChan <- wrappedErr
}

func publish(nsqdHost string, topic string, message string, timeout int, count int) {
	// Instantiate a producer.
	config := nsq.NewConfig()
	producer, err := nsq.NewProducer(nsqdHost, config)
	if err != nil {
		log.Fatal(err)
	}

	messageBody := []byte(message)

	// errorChan := make(chan error, 1)
	// for i := 0; i < count; i++ {
	// 	go func(id int) {
	// 		testPublish(id, errorChan, producer, topic, messageBody, timeout)
	// 	}(i)
	// }

	// for i := 0; i < count; i++ {
	// 	err := <-errorChan
	// 	if err != nil {
	// 		fmt.Println("Error publishing message:", err)
	// 	}

	// }
	success := 0
	for i := 0; i < count; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Microsecond)
		defer cancel()
		err := producer.PublishV2(ctx, topic, messageBody)
		if err != nil {
			fmt.Println("#", i, "err:", err)
			continue
		}
		success++
	}
	fmt.Println("Published", success, "messages")
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
	timeoutFlag := flag.Int("timeout", 1000, "timeout in microseconds")
	messageCountFlag := flag.Int("message-count", 1, "number of messages to publish")
	flag.Parse()

	// cfg := nsq.NewConfig()
	// fmt.Println("ReadTimeout:", cfg.ReadTimeout)
	// fmt.Println("WriteTimeout:", cfg.WriteTimeout)

	if *publishFlag && *consumeFlag {
		log.Fatal("Cannot publish and consume at the same time")
	}

	if !*publishFlag && !*consumeFlag {
		log.Fatal("Must specify either -publish or -consume")
	}

	if *publishFlag {
		publish(*nsqdHostFlag, *topicFlag, *messageFlag, *timeoutFlag, *messageCountFlag)
	} else {
		consume(*nsqlookupdHostFlag, *topicFlag, *channelFlag)
	}
}
