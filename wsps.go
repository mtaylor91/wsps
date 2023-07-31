package wsps

import (
	"encoding/json"

	"github.com/google/uuid"
)

type Event struct {
	Topic       string      `json:"topic"`
	Stream      uuid.UUID   `json:"stream"`
	EventId     uuid.UUID   `json:"event_id"`
	Content     interface{} `json:"content",omitempty`
	Subscribe   bool        `json:"subscribe",omitempty`
	Unsubscribe bool        `json:"unsubscribe",omitempty`
}

type EventWrapper struct {
	Encoded []byte
	Decoded *Event
}

func NewEvent(topic string, stream uuid.UUID, content interface{}) *Event {
	return &Event{
		Topic:       topic,
		Stream:      stream,
		EventId:     uuid.New(),
		Content:     content,
		Subscribe:   false,
		Unsubscribe: false,
	}
}

func NewSubscribeEvent(topic string, stream uuid.UUID) *Event {
	return &Event{
		Topic:       topic,
		Stream:      stream,
		EventId:     uuid.New(),
		Content:     nil,
		Subscribe:   true,
		Unsubscribe: false,
	}
}

func NewUnsubscribeEvent(topic string, stream uuid.UUID) *Event {
	return &Event{
		Topic:       topic,
		Stream:      stream,
		EventId:     uuid.New(),
		Content:     nil,
		Subscribe:   false,
		Unsubscribe: true,
	}
}

func (e *Event) Encode() ([]byte, error) {
	return json.Marshal(e)
}

func (e *Event) Decode(data []byte) error {
	return json.Unmarshal(data, e)
}

func (e *Event) Wrap() (*EventWrapper, error) {
	encoded, err := e.Encode()
	if err != nil {
		return nil, err
	}

	return &EventWrapper{
		Encoded: encoded,
		Decoded: e,
	}, nil
}
