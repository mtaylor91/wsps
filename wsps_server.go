package wsps

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

type PubSubServer struct {
	local *LocalPubSub
}

type PubSubEndpoint struct {
	topic     string
	server    *PubSubServer
	prototype interface{}
	upgrader  websocket.Upgrader
}

var ErrPrototypeIsPointer = fmt.Errorf(
	"prototype must be a struct, not a pointer to a struct")

func NewPubSubServer(local *LocalPubSub) *PubSubServer {
	return &PubSubServer{local}
}

func (s *PubSubServer) Publish(topic string, stream uuid.UUID, message interface{}) {
	s.local.Publish(topic, stream, message)
}

func (s *PubSubServer) Subscribe(
	topic string,
	stream uuid.UUID,
	ch chan<- *EventWrapper,
) {
	s.local.Subscribe(topic, stream, ch)
}

func (s *PubSubServer) Unsubscribe(
	topic string,
	stream uuid.UUID,
	ch chan<- *EventWrapper,
) {
	s.local.Unsubscribe(topic, stream, ch)
}

func (s *PubSubServer) NewEndpoint(
	topic string,
	prototype interface{},
) (*PubSubEndpoint, error) {
	// Ensure that the prototype is a struct, not a pointer to a struct.
	// This is because we want to be able to create new instances of the
	// prototype, and we can't do that if it's a pointer.
	prototypeType := reflect.TypeOf(prototype)
	if prototypeType.Kind() == reflect.Ptr {
		return nil, ErrPrototypeIsPointer
	}

	return &PubSubEndpoint{
		topic:     topic,
		server:    s,
		prototype: prototype,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  16384,
			WriteBufferSize: 16384,
		},
	}, nil
}

func (e *PubSubEndpoint) Publish(stream uuid.UUID, message interface{}) {
	e.server.Publish(e.topic, stream, message)
}

func (e *PubSubEndpoint) Subscribe(
	stream uuid.UUID,
	ch chan<- *EventWrapper,
) {
	e.server.Subscribe(e.topic, stream, ch)
}

func (e *PubSubEndpoint) Unsubscribe(
	stream uuid.UUID,
	ch chan<- *EventWrapper,
) {
	e.server.Unsubscribe(e.topic, stream, ch)
}

func (e *PubSubEndpoint) Handler(w http.ResponseWriter, r *http.Request) {
	conn, err := e.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	defer conn.Close()

	recv := make(chan *EventWrapper)
	send := make(chan *EventWrapper)
	errs := make(chan error)
	finished := make(chan struct{})

	go func() {
		for {
			select {
			case evt := <-send:
				if evt.Encoded != nil {
					err := conn.WriteMessage(
						websocket.TextMessage, evt.Encoded)
					if err != nil {
						errs <- err
						return
					}
				} else {
					err := conn.WriteJSON(evt.Decoded)
					if err != nil {
						errs <- err
						return
					}
				}
			case <-finished:
				return
			}
		}
	}()

	go func() {
		for {
			content := reflect.New(reflect.TypeOf(e.prototype))
			event := Event{
				Topic:   e.topic,
				Content: &content,
			}

			_, msgReader, err := conn.NextReader()
			if err != nil {
				errs <- err
				return
			}

			buf := bytes.Buffer{}
			_, err = buf.ReadFrom(msgReader)
			if err != nil {
				errs <- err
				return
			}

			data := buf.Bytes()
			err = json.Unmarshal(data, &event)
			if err != nil {
				errs <- err
				return
			}

			recv <- &EventWrapper{data, &event}
		}
	}()

	for {
		select {
		case evt := <-recv:
			if evt.Decoded.Subscribe {
				e.server.Subscribe(e.topic, evt.Decoded.Stream, send)
			} else if evt.Decoded.Unsubscribe {
				e.server.Unsubscribe(e.topic, evt.Decoded.Stream, send)
			} else {
				e.server.local.publish <- evt
			}
		case err := <-errs:
			if err != nil {
				finished <- struct{}{}
				return
			}
		}
	}
}
