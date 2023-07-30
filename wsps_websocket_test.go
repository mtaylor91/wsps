package wsps

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestWebsocketPubSub(t *testing.T) {
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
	pubSubConnection.SubscribeChannel(evtStream, ch)

	// Publish a message to the event stream
	pubSubEndpoint.Publish(evtStream, TestMessage{
		"Hello, world!",
	})

	// Receive the message
	evt := <-ch

	// Unsubscribe from the event stream
	pubSubConnection.UnsubscribeChannel(evtStream, ch)

	// Assert that the message is correct
	assert.Equal(t, &TestMessage{"Hello, world!"}, evt.Decoded.Content)

	// Shut down client
	assert.NoError(t, pubSubConnection.Close())
}

func benchmarkWebsocketPubSub(b *testing.B, clients, messages int) {
	// Create a new event stream
	evtStream := uuid.New()

	// Create local pubsub dispatchers
	serverPubSub := NewLocalPubSub()

	// Create server
	pubSubServer := NewPubSubServer(serverPubSub)
	// Create server endpoint
	pubSubEndpoint, err := pubSubServer.NewPubSubEndpoint("test", TestMessage{})
	if err != nil {
		b.Fatal(err)
	}

	// Start server
	httpServer := httptest.NewServer(http.HandlerFunc(pubSubEndpoint.Handler))
	defer httpServer.Close()

	// Create client sync channels
	clientReady := make([]chan struct{}, clients)
	clientFinished := make([]chan struct{}, clients)
	for i := 0; i < clients; i++ {
		clientReady[i] = make(chan struct{})
		clientFinished[i] = make(chan struct{})
	}

	// Dispatch clients
	for i := 0; i < clients; i++ {
		go func(i int) {
			// Create client pubsub dispatcher
			clientPubSub := NewLocalPubSub()
			// Create client
			pubSubClient := NewPubSubClient(clientPubSub)
			// Create client connection
			pubSubConnection, err := pubSubClient.NewPubSubConnection(
				httpServer.URL, "test", TestMessage{})
			if err != nil {
				b.Fatal(err)
			}

			// Create a channel to receive events
			ch := make(chan *EventWrapper)

			// Subscribe to the event stream
			pubSubConnection.SubscribeChannel(evtStream, ch)

			// Signal that the client is ready
			clientReady[i] <- struct{}{}

			// Receive messages messages
			for j := 0; j < messages; j++ {
				<-ch
			}

			// Unsubscribe from the event stream
			pubSubConnection.UnsubscribeChannel(evtStream, ch)

			// Signal that the client is done
			clientFinished[i] <- struct{}{}
		}(i)
	}

	// Wait for clients to be ready
	for i := 0; i < clients; i++ {
		<-clientReady[i]
	}

	// Publish messages messages
	for i := 0; i < messages; i++ {
		// Publish a message to the event stream
		pubSubEndpoint.Publish(evtStream, TestMessage{"Hello, world!"})
	}

	// Wait for clients to finish
	for i := 0; i < clients; i++ {
		<-clientFinished[i]
	}
}

func BenchmarkWebsocketPubSub10000(b *testing.B) {
	benchmarkWebsocketPubSub(b, b.N, 100)
}
