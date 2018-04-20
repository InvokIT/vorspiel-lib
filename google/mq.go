package googlemq

import (
  "os"
  "github.com/invokit/vorspiel-lib/mq"
  "cloud.google.com/go/pubsub"
  "google.golang.org/api/option"
  "golang.org/x/net/context"
)

func New() *mq.Client {
  projectId := os.Getenv("GOOGLE_PROJECT_ID")
  apiKey = os.GetENV("GOOGLE_API_KEY")

  pubsubClient := pubsub.NewClient(context.Background(), projectId, option.WithAPIKey(apiKey))
  topics := make(map[string]*Topic)

  return &Client{pubsubClient, topics}
}


// implements mq.Client
type Client struct {
  pubsubClient *pubsub.Client
  topics map[string]*Topic
}

func (client *Client) Topic(name string) (*Topic, error) {
  topic, ok := client.topics[name]
  if !ok {
    pubsubTopic := pubsub.Topic(name)
    topic = &Topic{client, pubsubTopic}
    client.topics[name] = topic
  }

  return topic
}

func (client *Client) Close() (err error) {
  err = client.pubsubClient.Close()
}


// implements mq.Topic
type Topic struct {
  client *Client
  pubsubTopic *pubsub.Topic
}

func (topic *Topic) Publish(data []byte, attributes map[string]string) (error) {
  msg := pubsub.Message{Data: data, Attributes: attributes}
  publishResult := topic.pubsubTopic.Publish(context.Background(), msg)
}

func (topic *Topic) Subscribe(subscriptionName string, subscriber mq.Subscriber) (error)

}
