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

func initProducer(nsqdHost string) *nsq.Producer {
	// Instantiate a producer.
	config := nsq.NewConfig()
	producer, err := nsq.NewProducer(nsqdHost, config)
	if err != nil {
		log.Fatal(err)
	}

	producer.Ping()

	return producer
}

func publish(nsqdHost string, topic string, timeout int, count int) {
	producer := initProducer(nsqdHost)

	success := 0
	for i := 0; i < count; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Microsecond)
		defer cancel()
		message := []byte(fmt.Sprintf("message #%d", i))
		err := producer.PublishWithContext(ctx, topic, message)
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

func publishAsync(nsqdHost string, topic string, timeout int, count int) {
	producer := initProducer(nsqdHost)

	success := 0
	for i := 0; i < count; i++ {
		doneChan := make(chan *nsq.ProducerTransaction)
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Microsecond)
		defer cancel()
		message := []byte(fmt.Sprintf("message #%d", i))
		err := producer.PublishAsyncWithContext(ctx, topic, message, doneChan)
		if err != nil {
			close(doneChan)
			fmt.Println("#", i, "err:", err)
			continue
		}
		t := <-doneChan
		fmt.Println("waited")
		if t.Error != nil {
			fmt.Println("#", i, "err:", t.Error)
			continue
		}
		success++
	}
	fmt.Println("Published", success, "messages")

	// Gracefully stop the producer when appropriate (e.g. before shutting down the service)
	producer.Stop()
}

func multiPublish(nsqdHost string, topic string, timeout int, count int) {
	producer := initProducer(nsqdHost)

	messages := make([][]byte, count)
	for i := 0; i < count; i++ {
		messages[i] = []byte(fmt.Sprintf("message #%d", i))
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Microsecond)
	defer cancel()
	err := producer.MultiPublishWithContext(ctx, topic, messages)
	if err != nil {
		log.Fatal(err)
	}

	// Gracefully stop the producer when appropriate (e.g. before shutting down the service)
	producer.Stop()
}

func deferredPublish(nsqdHost string, topic string, timeout int, count int) {
	producer := initProducer(nsqdHost)

	success := 0
	for i := 0; i < count; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Microsecond)
		defer cancel()
		message := []byte(fmt.Sprintf("message #%d", i))
		delay := time.Duration(1) * time.Second
		err := producer.DeferredPublishWithContext(ctx, topic, delay, message)
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
	timeoutFlag := flag.Int("timeout", 1000, "timeout in microseconds")
	countFlag := flag.Int("count", 1, "number of messages to publish")
	flag.Parse()

	if *publishFlag && *consumeFlag {
		log.Fatal("Cannot publish and consume at the same time")
	}

	if !*publishFlag && !*consumeFlag {
		log.Fatal("Must specify either -publish or -consume")
	}

	if *publishFlag {
		// publish(*nsqdHostFlag, *topicFlag, *timeoutFlag, *countFlag)
		publishAsync(*nsqdHostFlag, *topicFlag, *timeoutFlag, *countFlag)
		// multiPublish(*nsqdHostFlag, *topicFlag, *timeoutFlag, *countFlag)
		// deferredPublish(*nsqdHostFlag, *topicFlag, *timeoutFlag, *countFlag)
	} else {
		consume(*nsqlookupdHostFlag, *topicFlag, *channelFlag)
	}
}
