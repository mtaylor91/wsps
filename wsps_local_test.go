package wsps

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

type TestMessage struct {
	Message string `json:"message"`
}

func TestLocalPubSub(t *testing.T) {
	// Create a new local pubsub instance.
	ps := NewLocalPubSub()

	// Create a new channel to receive events.
	ch := make(chan *EventWrapper)

	// Create a new event stream.
	evtStream := uuid.New()

	// Subscribe to a topic.
	ps.Subscribe("test", evtStream, ch)

	// Publish an event.
	ps.Publish("test", evtStream, TestMessage{Message: "Hello World!"})

	// Receive the event.
	event := <-ch

	// Unsubscribe from the topic.
	ps.Unsubscribe("test", evtStream, ch)

	// Check the event.
	assert.Equal(t, "test", event.Decoded.Topic)
	assert.Equal(t, evtStream, event.Decoded.Stream)
	assert.Equal(t, TestMessage{Message: "Hello World!"}, event.Decoded.Content)
}
