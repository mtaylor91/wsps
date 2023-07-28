package wsps

import (
	"reflect"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

// AuthHandler is a function that handles authentication.
type AuthHandler func(*websocket.Conn) error

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
	client    *PubSubClient
	topic     string
	prototype interface{}
	sendEvent chan<- *EventWrapper
	finished  <-chan error
	shutdown  chan<- struct{}
}

// NewConnection creates a new PubSubConnection.
func (c *PubSubClient) NewPubSubConnection(
	endpoint, topic string,
	prototype interface{},
) (*PubSubConnection, error) {
	// Check prototype.
	if reflect.TypeOf(prototype).Kind() == reflect.Ptr {
		return nil, ErrPrototypeIsPointer
	}

	// Create channels.
	recv := make(chan *EventWrapper)
	send := make(chan *EventWrapper)
	errs := make(chan error)
	finished := make(chan error)
	shutdown := make(chan struct{})

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

	go sendMessages(wsConn, send, shutdown, errs)
	go readMessages(topic, prototype, wsConn, recv, errs)
	go runConnection(
		c.localPubSub,
		topic,
		prototype,
		wsConn,
		recv,
		send,
		errs,
		finished,
		shutdown,
	)

	return &PubSubConnection{
		client:    c,
		topic:     topic,
		prototype: prototype,
		sendEvent: send,
		finished:  finished,
		shutdown:  shutdown,
	}, nil
}

// Subscribe subscribes to a stream on the connection's topic.
func (c *PubSubConnection) Subscribe(stream uuid.UUID, ch chan<- *EventWrapper) error {
	c.client.localPubSub.Subscribe(c.topic, stream, ch)

	// Send subscribe message.
	evt := &EventWrapper{Decoded: &Event{
		Topic:     c.topic,
		Stream:    stream,
		Subscribe: true,
	}}

	select {
	case err := <-c.finished:
		return err
	case c.sendEvent <- evt:
		return nil
	}
}

// Shutdown shuts down a PubSubConnection.
func (c *PubSubConnection) Shutdown() error {
	close(c.shutdown)
	return <-c.finished
}

// runConnection runs a PubSubConnection.
func runConnection(
	localPubSub *LocalPubSub,
	topic string,
	prototype interface{},
	wsConn *websocket.Conn,
	recv <-chan *EventWrapper,
	send chan<- *EventWrapper,
	errs <-chan error,
	finished chan<- error,
	shutdown <-chan struct{},
) {
	defer wsConn.Close()

	for {
		select {
		case <-shutdown:
			close(finished)
			return
		case evt := <-recv:
			if evt.Decoded.Subscribe {
				logrus.WithFields(logrus.Fields{
					"topic":  topic,
					"stream": evt.Decoded.Stream,
				}).Trace("Client received subscribe")
				localPubSub.Subscribe(topic, evt.Decoded.Stream, send)
			} else if evt.Decoded.Unsubscribe {
				logrus.WithFields(logrus.Fields{
					"topic":  topic,
					"stream": evt.Decoded.Stream,
				}).Trace("Client received unsubscribe")
				localPubSub.Unsubscribe(topic, evt.Decoded.Stream, send)
			} else {
				logrus.WithFields(logrus.Fields{
					"topic":  topic,
					"stream": evt.Decoded.Stream,
				}).Trace("Client received message")
				localPubSub.publish <- evt
			}
		case err := <-errs:
			if err != nil {
				finished <- err
			}
		}
	}
}
