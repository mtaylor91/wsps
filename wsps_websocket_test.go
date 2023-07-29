package wsps

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestWebsocketPubSub(t *testing.T) {
	// Set log level to trace
	logrus.SetLevel(logrus.TraceLevel)

	// Create local pubsub dispatchers
	serverPubSub := NewLocalPubSub()
	clientPubSub := NewLocalPubSub()

	// Create server
	pubSubServer := NewPubSubServer(serverPubSub)
	// Create server endpoint
	pubSubEndpoint, err := pubSubServer.NewPubSubEndpoint("test", TestMessage{})
	if err != nil {
		t.Fatal(err)
	}

	// Start server
	httpServer := httptest.NewServer(http.HandlerFunc(pubSubEndpoint.Handler))
	defer httpServer.Close()

	// Create client
	pubSubClient := NewPubSubClient(clientPubSub)
	// Create client connection
	pubSubConnection, err := pubSubClient.NewPubSubConnection(
		httpServer.URL, "test", TestMessage{})
	if err != nil {
		t.Fatal(err)
	}

	// Create a new event stream
	evtStream := uuid.New()

	// Create a channel to receive events
	ch := make(chan *EventWrapper)

	// Subscribe to the event stream
	pubSubConnection.Subscribe(evtStream, ch)

	// Wait for the subscription to be registered
	time.Sleep(100 * time.Millisecond)

	// Publish a message to the event stream
	pubSubConnection.Publish(evtStream, TestMessage{
		"Hello, world!",
	})

	// Receive the message
	evt := <-ch

	// Unsubscribe from the event stream
	pubSubConnection.Unsubscribe(evtStream, ch)

	// Wait for the unsubscription to be registered
	time.Sleep(100 * time.Millisecond)

	// Assert that the message is correct
	assert.Equal(t, &TestMessage{"Hello, world!"}, evt.Decoded.Content)

	// Shutdown client
	assert.NoError(t, pubSubConnection.Shutdown())
}
