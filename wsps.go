package wsps

import (
	"github.com/google/uuid"
)

type Event struct {
	Topic       string      `json:"topic"`
	Stream      uuid.UUID   `json:"stream"`
	Content     interface{} `json:"content",omitempty`
	Subscribe   bool        `json:"subscribe",omitempty`
	Unsubscribe bool        `json:"unsubscribe",omitempty`
}

type EventWrapper struct {
	Encoded []byte
	Decoded *Event
}

type PubSub interface {
	Publish(topic string, stream uuid.UUID, content interface{}) error
	Subscribe(topic string, stream uuid.UUID, ch chan<- *EventWrapper) error
	Unsubscribe(topic string, stream uuid.UUID, ch chan<- *EventWrapper) error
}
