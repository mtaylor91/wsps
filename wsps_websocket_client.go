package wsps

import (
	"context"
	"reflect"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

// PubSubClient is a websocket-based pubsub client.
type PubSubClient struct {
	auth        AuthHandler
	dialer      *websocket.Dialer
	localPubSub *LocalPubSub
}

// NewPubSubClient creates a new PubSubClient.
func NewPubSubClient(localPubSub *LocalPubSub) *PubSubClient {
	return &PubSubClient{
		dialer:      &websocket.Dialer{},
		localPubSub: localPubSub,
	}
}

// SetAuthHandler sets the authentication handler.
func (c *PubSubClient) SetAuthHandler(auth AuthHandler) {
	c.auth = auth
}

// PubSubConnection is a websocket-based pubsub connection.
type PubSubConnection struct {
	topic    string
	ctx      context.Context
	cancel   context.CancelFunc
	client   *PubSubClient
	send     chan<- *EventWrapper
	finished <-chan error
}

// NewConnection creates a new PubSubConnection.
func (c *PubSubClient) NewPubSubConnection(
	endpoint, topic string,
	prototype interface{},
) (*PubSubConnection, error) {
	// Create context
	ctx := context.Background()
	return c.NewPubSubConnectionContext(ctx, endpoint, topic, prototype)
}

// NewConnection creates a new PubSubConnection.
func (c *PubSubClient) NewPubSubConnectionContext(
	ctx context.Context,
	endpoint, topic string,
	prototype interface{},
) (*PubSubConnection, error) {
	// Resolve content type
	msgType := reflect.TypeOf(prototype)
	if msgType.Kind() == reflect.Ptr {
		// Resolve pointer type
		msgType = msgType.Elem()
	}

	// Create channels.
	recv := make(chan *EventWrapper)
	send := make(chan *EventWrapper)
	errs := make(chan error)
	finished := make(chan error)

	// Create cancel context
	ctx, cancel := context.WithCancel(ctx)

	// Dial the endpoint.
	wsConn, _, err := c.dialer.Dial(endpoint, nil)
	if err != nil {
		return nil, err
	}

	// Authenticate.
	if c.auth != nil {
		if err := c.auth(wsConn); err != nil {
			return nil, err
		}
	}

	// Run the connection.
	go sendMessages(ctx, wsConn, send, errs)
	go readMessages(topic, msgType, wsConn, recv, errs)
	go runConnection(
		ctx,
		topic,
		c.localPubSub,
		recv,
		send,
		errs,
		finished,
	)

	// Return the connection.
	return &PubSubConnection{
		topic:    topic,
		ctx:      ctx,
		cancel:   cancel,
		client:   c,
		send:     send,
		finished: finished,
	}, nil
}

// Publish publishes a message to the connection's topic.
func (c *PubSubConnection) Publish(streamId uuid.UUID, msg interface{}) {
	// Wrap the message.
	evt := &EventWrapper{Decoded: &Event{
		Topic:   c.topic,
		Stream:  streamId,
		Content: msg,
	}}

	// Send the message.
	c.send <- evt
}

// Subscribe subscribes to a stream on the connection's topic.
func (c *PubSubConnection) Subscribe(stream uuid.UUID, ch chan<- *EventWrapper) {
	c.client.localPubSub.Subscribe(c.topic, stream, ch)

	// Send subscribe message.
	evt := &EventWrapper{Decoded: &Event{
		Topic:     c.topic,
		Stream:    stream,
		Subscribe: true,
	}}

	c.send <- evt
}

// Unsubscribe unsubscribes from a stream on the connection's topic.
func (c *PubSubConnection) Unsubscribe(stream uuid.UUID, ch chan<- *EventWrapper) {
	c.client.localPubSub.Unsubscribe(c.topic, stream, ch)

	// Send unsubscribe message.
	evt := &EventWrapper{Decoded: &Event{
		Topic:       c.topic,
		Stream:      stream,
		Unsubscribe: true,
	}}

	c.send <- evt
}

// Shutdown shuts down a PubSubConnection.
func (c *PubSubConnection) Shutdown() error {
	c.cancel()
	return <-c.finished
}

// runConnection runs a PubSubConnection.
func runConnection(
	ctx context.Context,
	topic string,
	localPubSub *LocalPubSub,
	recv <-chan *EventWrapper,
	send chan<- *EventWrapper,
	errs <-chan error,
	finished chan<- error,
) {
	for {
		select {
		case <-ctx.Done():
			close(finished)
			return
		case evt := <-recv:
			if evt.Decoded.Content != nil {
				logrus.WithFields(logrus.Fields{
					"topic":  topic,
					"stream": evt.Decoded.Stream,
				}).Trace("Client received message")
				localPubSub.publish <- evt
			}
		case err := <-errs:
			if err != nil && !websocket.IsCloseError(err,
				websocket.CloseNormalClosure, websocket.CloseGoingAway,
			) {
				logrus.WithError(err).Error("WebSocket client error")
				finished <- err
			}
		}
	}
}