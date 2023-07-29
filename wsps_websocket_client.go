package wsps

import (
	"context"
	"net/url"
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

	// Resolve websocket url
	url, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}

	if url.Scheme == "http" {
		url.Scheme = "ws"
	} else if url.Scheme == "https" {
		url.Scheme = "wss"
	}

	// Dial the endpoint.
	wsConn, _, err := c.dialer.Dial(url.String(), nil)
	if err != nil {
		return nil, err
	}

	// Authenticate.
	if c.auth != nil {
		if err = c.auth(wsConn); err != nil {
			return nil, err
		}
	}

	// Run the connection.
	go sendMessages(ctx, wsConn, send, errs)
	go readMessages(topic, msgType, wsConn, recv, errs)
	go runConnection(
		ctx,
		c.localPubSub,
		topic,
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

// Close shuts down a PubSubConnection.
func (c *PubSubConnection) Close() error {
	c.cancel()
	return <-c.finished
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

// runConnection runs a PubSubConnection.
func runConnection(
	ctx context.Context,
	localPubSub *LocalPubSub,
	topic string,
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
