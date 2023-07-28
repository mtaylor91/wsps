package wsps

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

var ErrPrototypeIsPointer = fmt.Errorf(
	"prototype must be a struct, not a pointer to a struct")

func sendMessages(
	conn *websocket.Conn,
	send <-chan *EventWrapper,
	shutdown <-chan struct{},
	errs chan<- error,
) {
	for {
		select {
		case evt := <-send:
			logrus.WithFields(logrus.Fields{
				"topic":  evt.Decoded.Topic,
				"stream": evt.Decoded.Stream,
			}).Trace("Sending websocket event")
			if evt.Encoded != nil {
				err := conn.WriteMessage(
					websocket.TextMessage, evt.Encoded)
				if err != nil {
					errs <- err
					goto finish
				}
			} else {
				err := conn.WriteJSON(evt.Decoded)
				if err != nil {
					errs <- err
					goto finish
				}
			}
		case <-shutdown:
			goto finish
		}
	}

finish:

	conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(
		websocket.CloseGoingAway, ""))
}

func readMessages(
	topic string,
	prototype interface{},
	conn *websocket.Conn,
	recv chan<- *EventWrapper,
	errs chan<- error,
) {
	for {
		content := reflect.New(reflect.TypeOf(prototype)).Interface()
		event := Event{
			Topic:   topic,
			Content: content,
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
}
