package mq

import (
	"golang.org/x/net/context"
)

type Client interface {
	Topic(name string) (Topic, error)
	Close() error
}

type Topic interface {
	Name() string

	// Publish a message on the Topic. Blocks until the message has been sent.
	Publish(ctx context.Context, data []byte, attributes map[string]string) error

	// Create a new subscription. Will return an error if the subscription already exists.
	CreateSubscription(ctx context.Context, subscriptionName string) error

	DeleteSubscription(ctx context.Context, subscriptionName string) error

	// Subscribe to an existing subscription. Only one subscriper of a subscription will receive a specific message on the topic.
	Subscribe(ctx context.Context, subscriptionName string, subscriber Subscriber) error

	// Create a new unique subscription that will receive all messages on the Topic.
	Listen(ctx context.Context, subscriber Subscriber) error
}

// Subscribers run in its own goroutine
// if an error is not thrown the message is acknowledged
type Subscriber func(msg Message) (err error)

type Message struct {
	Data       []byte
	Attributes map[string]string
}
