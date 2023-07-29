package wsps

import (
	"context"
	"net/http"
	"reflect"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

// PubSubServer is a websocket-based pubsub server.
type PubSubServer struct {
	localPubSub *LocalPubSub
}

// PubSubEndpoint is a websocket-based pubsub endpoint.
type PubSubEndpoint struct {
	topic    string
	server   *PubSubServer
	msgType  reflect.Type
	upgrader websocket.Upgrader
}

// NewPubSubServer creates a new PubSubServer.
func NewPubSubServer(localPubSub *LocalPubSub) *PubSubServer {
	return &PubSubServer{localPubSub}
}

// Publish publishes a message to a topic.
func (s *PubSubServer) Publish(topic string, stream uuid.UUID, message interface{}) {
	s.localPubSub.Publish(topic, stream, message)
}

// Subscribe subscribes to a topic.
func (s *PubSubServer) Subscribe(
	topic string,
	stream uuid.UUID,
	ch chan<- *EventWrapper,
) {
	s.localPubSub.Subscribe(topic, stream, ch)
}

// Unsubscribe unsubscribes from a topic.
func (s *PubSubServer) Unsubscribe(
	topic string,
	stream uuid.UUID,
	ch chan<- *EventWrapper,
) {
	s.localPubSub.Unsubscribe(topic, stream, ch)
}

// NewPubSubEndpoint creates a new PubSubEndpoint.
func (s *PubSubServer) NewPubSubEndpoint(
	topic string,
	prototype interface{},
) (*PubSubEndpoint, error) {
	// Resolve the type of the message.
	msgType := reflect.TypeOf(prototype)
	if msgType.Kind() == reflect.Ptr {
		msgType = msgType.Elem()
	}

	return &PubSubEndpoint{
		topic:   topic,
		server:  s,
		msgType: msgType,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  16384,
			WriteBufferSize: 16384,
		},
	}, nil
}

// Publish publishes a message to a stream on the endpoint's topic
func (e *PubSubEndpoint) Publish(stream uuid.UUID, message interface{}) {
	e.server.Publish(e.topic, stream, message)
}

// Subscribe subscribes to a stream on the endpoint's topic
func (e *PubSubEndpoint) Subscribe(
	stream uuid.UUID,
	ch chan<- *EventWrapper,
) {
	e.server.Subscribe(e.topic, stream, ch)
}

// Unsubscribe unsubscribes from a stream on the endpoint's topic
func (e *PubSubEndpoint) Unsubscribe(
	stream uuid.UUID,
	ch chan<- *EventWrapper,
) {
	e.server.Unsubscribe(e.topic, stream, ch)
}

// Handler handles a websocket connection.
func (e *PubSubEndpoint) Handler(w http.ResponseWriter, r *http.Request) {
	wsConn, err := e.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	recv := make(chan *EventWrapper)
	send := make(chan *EventWrapper)
	errs := make(chan error)

	ctx, cancel := context.WithCancel(r.Context())

	go sendMessages(ctx, wsConn, send, errs)
	go readMessages(e.topic, e.msgType, wsConn, recv, errs)

	for {
		select {
		case evt := <-recv:
			if evt.Decoded.Subscribe {
				logrus.WithFields(logrus.Fields{
					"topic":  e.topic,
					"stream": evt.Decoded.Stream,
				}).Trace("Server received subscribe")
				e.server.Subscribe(e.topic, evt.Decoded.Stream, send)
			} else if evt.Decoded.Unsubscribe {
				logrus.WithFields(logrus.Fields{
					"topic":  e.topic,
					"stream": evt.Decoded.Stream,
				}).Trace("Server received unsubscribe")
				e.server.Unsubscribe(e.topic, evt.Decoded.Stream, send)
			} else {
				logrus.WithFields(logrus.Fields{
					"topic":  e.topic,
					"stream": evt.Decoded.Stream,
				}).Trace("Server received message")
				e.server.localPubSub.publish <- evt
			}
		case err := <-errs:
			if err != nil && !websocket.IsCloseError(err,
				websocket.CloseGoingAway, websocket.CloseNormalClosure,
			) {
				logrus.WithError(err).Error("WebSocket error")
				cancel()
				return
			} else if err != nil {
				cancel()
				return
			}
		}
	}
}
