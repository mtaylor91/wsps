package wsps

import (
	"context"
	"net/http"
	"reflect"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

type OnSubscribe func(topic string, stream uuid.UUID)
type OnUnsubscribe func(topic string, stream uuid.UUID)

// PubSubServer is a websocket-based pubsub server.
type PubSubServer struct {
	authentication Authentication
	authorization  Authorization
	localPubSub    *LocalPubSub
	onSubscribe    OnSubscribe
	onUnsubscribe  OnUnsubscribe
}

// NewPubSubServer creates a new PubSubServer.
func NewPubSubServer(localPubSub *LocalPubSub) *PubSubServer {
	return &PubSubServer{localPubSub: localPubSub}
}

// Publish publishes a message to a topic.
func (s *PubSubServer) Publish(e *Event) {
	s.localPubSub.Publish(e)
}

// SubscribeChannel subscribes to a topic.
func (s *PubSubServer) SubscribeChannel(
	topic string,
	stream uuid.UUID,
	ch chan<- *EventWrapper,
) {
	s.localPubSub.SubscribeChannel(topic, stream, ch)
}

// UnsubscribeChannel unsubscribes from a topic.
func (s *PubSubServer) UnsubscribeChannel(
	topic string,
	stream uuid.UUID,
	ch chan<- *EventWrapper,
) {
	s.localPubSub.UnsubscribeChannel(topic, stream, ch)
}

// SetAuthenticator sets the authentication handler.
func (s *PubSubServer) SetAuthentication(auth Authentication) {
	s.authentication = auth
}

// SetAuthorization sets the authorization handler.
func (s *PubSubServer) SetAuthorization(auth Authorization) {
	s.authorization = auth
}

// SetOnSubscribe sets the onSubscribe handler.
func (s *PubSubServer) SetOnSubscribe(onSubscribe OnSubscribe) {
	s.onSubscribe = onSubscribe
}

// SetOnUnsubscribe sets the onUnsubscribe handler.
func (s *PubSubServer) SetOnUnsubscribe(onUnsubscribe OnUnsubscribe) {
	s.onUnsubscribe = onUnsubscribe
}

// PubSubEndpoint is a websocket-based pubsub endpoint.
type PubSubEndpoint struct {
	topic    string
	server   *PubSubServer
	msgType  reflect.Type
	upgrader websocket.Upgrader
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
func (ep *PubSubEndpoint) Publish(evt *Event) {
	ep.server.Publish(evt)
}

// SubscribeChannel subscribes to a stream on the endpoint's topic
func (e *PubSubEndpoint) SubscribeChannel(
	stream uuid.UUID,
	ch chan<- *EventWrapper,
) {
	e.server.SubscribeChannel(e.topic, stream, ch)
}

// UnsubscribeChannel unsubscribes from a stream on the endpoint's topic
func (e *PubSubEndpoint) UnsubscribeChannel(
	stream uuid.UUID,
	ch chan<- *EventWrapper,
) {
	e.server.UnsubscribeChannel(e.topic, stream, ch)
}

// Handler handles a websocket connection.
func (e *PubSubEndpoint) Handler(w http.ResponseWriter, r *http.Request) {
	wsConn, err := e.upgrader.Upgrade(w, r, nil)
	if err != nil {
		logrus.WithError(err).Debug("Failed to upgrade websocket")
		return
	}

	defer wsConn.Close()

	// Authenticate the connection.
	var session interface{}
	if e.server.authentication != nil {
		session, err = e.server.authentication(wsConn)
		if err != nil {
			logrus.WithError(err).Debug("Failed to authenticate")
			wsConn.Close()
			return
		}
	}

	recv := make(chan *EventWrapper)
	send := make(chan *EventWrapper)
	recvErrs := make(chan error)
	sendErrs := make(chan error)

	ctx, cancel := context.WithCancel(r.Context())

	go readMessages(e.topic, e.msgType, wsConn, recv, recvErrs)
	go sendMessages(ctx, wsConn, send, sendErrs)

	recvClosed := false
	sendClosed := false

	for {
		if recvClosed && sendClosed {
			break
		}

		select {
		case evt := <-recv:
			// Authenticate the message.
			if e.server.authorization != nil {
				if !e.server.authorization(session, evt) {
					continue
				}
			}

			if evt.Decoded.Subscribe {
				logrus.WithFields(logrus.Fields{
					"topic":  e.topic,
					"stream": evt.Decoded.Stream,
				}).Trace("Server received subscribe")
				e.server.SubscribeChannel(
					e.topic, evt.Decoded.Stream, send)
				send <- evt
				if e.server.onSubscribe != nil {
					e.server.onSubscribe(
						e.topic, evt.Decoded.Stream)
				}
			} else if evt.Decoded.Unsubscribe {
				logrus.WithFields(logrus.Fields{
					"topic":  e.topic,
					"stream": evt.Decoded.Stream,
				}).Trace("Server received unsubscribe")
				e.server.UnsubscribeChannel(
					e.topic, evt.Decoded.Stream, send)
				send <- evt
				if e.server.onUnsubscribe != nil {
					e.server.onUnsubscribe(
						e.topic, evt.Decoded.Stream)
				}
			} else {
				logrus.WithFields(logrus.Fields{
					"topic":  e.topic,
					"stream": evt.Decoded.Stream,
				}).Trace("Server received message")
				e.server.localPubSub.publish <- evt
			}
		case err := <-recvErrs:
			if err != nil && !websocket.IsCloseError(err,
				websocket.CloseGoingAway, websocket.CloseNormalClosure,
			) {
				logrus.WithError(err).Error(
					"WebSocket server receive error")
				recvClosed = true
				cancel()
			} else if err != nil {
				recvClosed = true
				cancel()
			}
		case err := <-sendErrs:
			if err != nil && !websocket.IsCloseError(err,
				websocket.CloseGoingAway, websocket.CloseNormalClosure,
			) {
				logrus.WithError(err).Error(
					"WebSocket server send error")
				sendClosed = true
			} else if err != nil {
				sendClosed = true
			}
		}
	}
}
