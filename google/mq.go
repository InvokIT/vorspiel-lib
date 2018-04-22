package googlemq

import (
	"cloud.google.com/go/pubsub"
	"fmt"
	"github.com/invokit/go-util/debug"
	"github.com/invokit/vorspiel-lib/mq"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
	"github.com/rs/xid"
	"os"
	"strings"
)

var dbg = debug.NewLogger("github.com/invokit/vorspiel-lib/googlemq")

func New(projectId string, apiKey string) (mq.Client, error) {
	pubsubClient, err := pubsub.NewClient(context.Background(), projectId, option.WithAPIKey(apiKey))
	if err != nil {
		dbg.Printf("error when creating pubsub client for project '%s': %s", projectId, err)
		return nil, err
	}

	topics := make(map[string]*Topic)
	client := &Client{pubsubClient, topics}

	dbg.Printf("creating pubsub client for project '%s'", projectId)

	return client, nil
}

type Client struct {
	pubsubClient *pubsub.Client
	topics       map[string]*Topic
}

func (client *Client) Topic(name string) (mq.Topic, error) {
	topic, ok := client.topics[name]
	if !ok {
		pubsubTopic := client.pubsubClient.Topic(name)
		topic = &Topic{client, pubsubTopic}
		client.topics[name] = topic
	}

	return topic, nil
}

func (client *Client) Close() error {
	return client.pubsubClient.Close()
}

// implements mq.Topic
type Topic struct {
	client      *Client
	pubsubTopic *pubsub.Topic
}

func (topic *Topic) CreateSubscription(ctx context.Context, subscriptionName string) error {
	_, err := topic.client.pubsubClient.CreateSubscription(ctx, subscriptionName, pubsub.SubscriptionConfig{Topic: topic.pubsubTopic})
	if err != nil {
		dbg.Printf("error when creating subscription '%s' on topic '%s': %s", subscriptionName, topic.pubsubTopic.ID(), err)
		return err
	}

	dbg.Printf("created subscription '%s' on topic '%s'", subscriptionName, topic.pubsubTopic.ID())

	return nil
}

func (topic *Topic) DeleteSubscription(ctx context.Context, subscriptionName string) error {
	subscription := topic.client.pubsubClient.Subscription(subscriptionName)
	err := subscription.Delete(ctx)
	return err
}

// Blocks until message has been sent or context cancelled
func (topic *Topic) Publish(ctx context.Context, data []byte, attributes map[string]string) error {
	msg := &pubsub.Message{Data: data, Attributes: attributes}

	publishResult := topic.pubsubTopic.Publish(context.Background(), msg)

	_, err := publishResult.Get(ctx)
	return err
}

func (topic *Topic) Subscribe(ctx context.Context, subscriptionName string, subscriber mq.Subscriber) error {
	subscription := topic.client.pubsubClient.Subscription(subscriptionName)

	exists, err := subscription.Exists(ctx)
	if err != nil {
		dbg.Printf("error when checking if subscription '%s' on topic '%s' exists: %s", subscriptionName, topic.pubsubTopic.ID(), err)
		return err
	}

	if !exists {
		dbg.Printf("tried to subscribe to non-existing subscription '%s' on topic '%s'", subscriptionName, topic.pubsubTopic.ID())
		return fmt.Errorf("subscription '%s' on topic '%s' does not exist", subscriptionName, topic.pubsubTopic.ID())
	}

	go receive(ctx, subscription, subscriber)

	return nil
}

func (topic *Topic) Listen(ctx context.Context, subscriptionNamePrefix string, subscriber mq.Subscriber) error {
	subscriptionName := generateUniqueSubscriptionName(subscriptionNamePrefix)

	subscription, err := topic.client.pubsubClient.CreateSubscription(ctx, subscriptionName, pubsub.SubscriptionConfig{Topic: topic.pubsubTopic})
	if err != nil {
		dbg.Printf("error when creating subscription '%s' on topic '%s': %s", subscriptionName, topic.pubsubTopic.ID(), err)
		return err
	}

	go receive(ctx, subscription, subscriber)

	go func() {
		// When the context is done delete the subscription
		<-ctx.Done()

		if err := subscription.Delete(context.Background()); err == nil {
			dbg.Printf("deleted subscription '%s' on topic '%s'", subscriptionName, topic.pubsubTopic.ID())
		} else {
			dbg.Printf("error when deleting subscription '%s' on topic '%s'", subscriptionName, topic.pubsubTopic.ID())
		}
	}()

	return nil
}

func receive(ctx context.Context, subscription *pubsub.Subscription, subscriber mq.Subscriber) {
	for ctx.Err() == nil {
		err := subscription.Receive(ctx, func(ctx context.Context, message *pubsub.Message) {
			dbg.Printf("received message '%s' from subscription '%s'", message.ID, subscription.ID())

			err := subscriber(mq.Message{Data: message.Data, Attributes: message.Attributes})
			if err == nil {
				dbg.Printf("ack'ing message '%s' from subscription '%s'", message.ID, subscription.ID())
				message.Ack()
			} else {
				dbg.Printf("nack'ing message '%s' from subscription '%s' because of error: %s", message.ID, subscription.ID(), err)
				message.Nack()
			}
		})
		if err != nil {
			dbg.Printf("error when receiving messages from subscription '%s': %s", subscription.ID(), err)
		}
	}

	dbg.Printf("stopped receiving messages from subscription '%s'", subscription.ID())
}

func generateUniqueSubscriptionName(prefix string) string {
	var nameBuilder strings.Builder

	if prefix != "" {
		nameBuilder.WriteString(prefix)
		nameBuilder.WriteRune('-')
	}

	if hostname, err := os.Hostname(); err != nil {
		nameBuilder.WriteString(hostname)
		nameBuilder.WriteRune('-')
	} else {
		dbg.Printf("unable to get hostname: %s", err)
	}

	nameBuilder.WriteString(xid.New().String())

	name := nameBuilder.String()

	dbg.Printf("generated subscription name '%s'", name)

	return name
}
