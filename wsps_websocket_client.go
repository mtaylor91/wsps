package wsps

import (
	"context"
	"errors"
	"net/url"
	"reflect"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

var ErrConnectionClosed = errors.New("connection closed")

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
	topic       string
	ctx         context.Context
	cancel      context.CancelFunc
	client      *PubSubClient
	publish     chan<- *EventWrapper
	subscribe   chan<- *subscription
	unsubscribe chan<- *subscription
	finished    <-chan error
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
	subscribe := make(chan *subscription)
	unsubscribe := make(chan *subscription)
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
	wsUrl := url.String()
	logrus.WithFields(logrus.Fields{
		"topic": topic,
		"url":   wsUrl,
	}).Debug("Dialing websocket endpoint")
	wsConn, _, err := c.dialer.Dial(wsUrl, nil)
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
		subscribe,
		unsubscribe,
		errs,
		finished,
	)

	// Return the connection.
	return &PubSubConnection{
		topic:       topic,
		ctx:         ctx,
		cancel:      cancel,
		client:      c,
		publish:     send,
		subscribe:   subscribe,
		unsubscribe: unsubscribe,
		finished:    finished,
	}, nil
}

// Close shuts down a PubSubConnection.
func (c *PubSubConnection) Close() error {
	c.cancel()
	return <-c.finished
}

// Publish publishes a message to the connection's topic.
func (c *PubSubConnection) Publish(streamId uuid.UUID, msg interface{}) error {
	// Wrap the message.
	evt := NewEvent(c.topic, streamId, msg)
	wrapped, err := evt.Wrap()
	if err != nil {
		return err
	}

	// Send the message.
	select {
	case err := <-c.finished:
		if err != nil {
			return err
		} else {
			return ErrConnectionClosed
		}
	case c.publish <- wrapped:
		return nil
	}
}

// Subscribe subscribes to a stream on the connection's topic.
func (c *PubSubConnection) Subscribe(stream uuid.UUID) error {
	ack := make(chan *EventWrapper)
	c.subscribe <- &subscription{c.topic, stream, ack}
	select {
	case err := <-c.finished:
		if err != nil {
			return err
		} else {
			return ErrConnectionClosed
		}
	case <-ack:
		return nil
	}
}

// SubscribeChannel subscribes to a stream on the connection's topic.
func (c *PubSubConnection) SubscribeChannel(
	stream uuid.UUID, ch chan<- *EventWrapper,
) error {
	c.client.localPubSub.SubscribeChannel(c.topic, stream, ch)
	return c.Subscribe(stream)
}

// Unsubscribe unsubscribes from a stream on the connection's topic.
func (c *PubSubConnection) Unsubscribe(stream uuid.UUID) error {
	ack := make(chan *EventWrapper)
	c.unsubscribe <- &subscription{c.topic, stream, ack}
	select {
	case err := <-c.finished:
		if err != nil {
			return err
		} else {
			return ErrConnectionClosed
		}
	case <-ack:
		return nil
	}
}

// UnsubscribeChannel unsubscribes from a stream on the connection's topic.
func (c *PubSubConnection) UnsubscribeChannel(
	stream uuid.UUID, ch chan<- *EventWrapper,
) error {
	c.client.localPubSub.UnsubscribeChannel(c.topic, stream, ch)
	return c.Unsubscribe(stream)
}

// runConnection runs a PubSubConnection.
func runConnection(
	ctx context.Context,
	localPubSub *LocalPubSub,
	topic string,
	recv <-chan *EventWrapper,
	send chan<- *EventWrapper,
	subscribe <-chan *subscription,
	unsubscribe <-chan *subscription,
	errs <-chan error,
	finished chan<- error,
) {
	pendingSubscriptions := make(map[uuid.UUID][]*subscription)
	pendingUnsubscriptions := make(map[uuid.UUID][]*subscription)

	for {
		select {
		case <-ctx.Done():
			close(finished)
			return
		case evt := <-recv:
			if evt.Decoded.Subscribe {
				subs := pendingSubscriptions[evt.Decoded.Stream]
				// Resolve pending subscriptions.
				for _, sub := range subs {
					sub.ch <- evt
				}
				delete(pendingSubscriptions, evt.Decoded.Stream)
			} else if evt.Decoded.Unsubscribe {
				subs := pendingUnsubscriptions[evt.Decoded.Stream]
				// Resolve pending unsubscriptions.
				for _, sub := range subs {
					sub.ch <- evt
				}
				delete(pendingUnsubscriptions, evt.Decoded.Stream)
			} else {
				logrus.WithFields(logrus.Fields{
					"topic":  topic,
					"stream": evt.Decoded.Stream,
				}).Trace("Client received message")
				// Publish the message.
				localPubSub.publish <- evt
			}
		case sub := <-subscribe:
			logrus.WithFields(logrus.Fields{
				"topic":  topic,
				"stream": sub.stream,
			}).Trace("Client subscription request")
			subs := pendingSubscriptions[sub.stream]
			pendingSubscriptions[sub.stream] = append(subs, sub)
			evt := NewSubscribeEvent(topic, sub.stream)
			wrapped, err := evt.Wrap()
			if err != nil {
				panic(err)
			}

			send <- wrapped
		case sub := <-unsubscribe:
			logrus.WithFields(logrus.Fields{
				"topic":  topic,
				"stream": sub.stream,
			}).Trace("Client unsubscription request")
			subs := pendingUnsubscriptions[sub.stream]
			pendingUnsubscriptions[sub.stream] = append(subs, sub)
			evt := NewUnsubscribeEvent(topic, sub.stream)
			wrapped, err := evt.Wrap()
			if err != nil {
				panic(err)
			}

			send <- wrapped
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
