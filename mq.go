package mq

type Client interface {
  Topic(name string) (*Topic, error)
  Close() error
}

type Topic interface {
  Publish(data []byte) error
  Subscribe(subscriptionName string, subscriber Subscriber) (error)
}

// msg will automaticaly be acknowledged if an error is not returned
type Subscriber func(msg Message) (error)

type Message struct {
  data []byte
}
