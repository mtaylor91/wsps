package wsps

import (
	"bytes"
	"context"
	"reflect"

	"github.com/gorilla/websocket"
)

// AuthHandler is a function that handles authentication.
type AuthHandler func(*websocket.Conn) error

// Authentication is a function that authenticates a websocket connection.
type Authentication func(*websocket.Conn) (interface{}, error)

// Authorization is a function that authorizes a websocket event.
type Authorization func(interface{}, *EventWrapper) bool

// sendMessages sends messages to the websocket connection.
func sendMessages(
	ctx context.Context,
	conn *websocket.Conn,
	send <-chan *EventWrapper,
	errs chan<- error,
) {
	defer close(errs)

	for {
		select {
		case <-ctx.Done():
			goto finish
		case evt := <-send:
			if evt.Encoded != nil {
				// Send encoded message.
				err := conn.WriteMessage(
					websocket.TextMessage, evt.Encoded)
				if err != nil {
					errs <- err
				}
			} else {
				// Encode and send message.
				err := conn.WriteJSON(evt.Decoded)
				if err != nil {
					errs <- err
				}
			}
		}
	}

finish:
	// Send close message.
	conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(
		websocket.CloseGoingAway, ""))
}

// readMessages reads messages from the websocket connection.
func readMessages(
	topic string,
	msgType reflect.Type,
	conn *websocket.Conn,
	recv chan<- *EventWrapper,
	errs chan<- error,
) {
	defer close(errs)

	for {
		// Create event.
		content := reflect.New(msgType).Interface()
		event := Event{Topic: topic, Content: content}

		// Get message reader.
		_, msgReader, err := conn.NextReader()
		if err != nil {
			errs <- err
			return
		}

		// Read message.
		buf := bytes.Buffer{}
		_, err = buf.ReadFrom(msgReader)
		if err != nil {
			errs <- err
			return
		}

		// Decode message.
		data := buf.Bytes()
		err = event.Decode(data)
		if err != nil {
			errs <- err
			return
		}

		// Send event to receiver.
		recv <- &EventWrapper{data, &event}
	}
}
