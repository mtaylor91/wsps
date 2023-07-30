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
