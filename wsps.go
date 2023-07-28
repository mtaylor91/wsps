package wsps

import (
	"encoding/json"

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

type subscription struct {
	topic  string
	stream uuid.UUID
	ch     chan<- *EventWrapper
}

type LocalPubSub struct {
	publish     chan<- *EventWrapper
	subscribe   chan<- *subscription
	unsubscribe chan<- *subscription
}

type localPubSub struct {
	finish      chan<- struct{}
	publish     chan<- *EventWrapper
	subscribe   chan<- *subscription
	unsubscribe chan<- *subscription
	subscribers int
}

type localSub struct {
	finish  chan<- struct{}
	publish chan<- *EventWrapper
}

func NewLocalPubSub() *LocalPubSub {
	publish := make(chan *EventWrapper)
	subscribe := make(chan *subscription)
	unsubscribe := make(chan *subscription)

	go runLocalPubSub(publish, subscribe, unsubscribe)

	return &LocalPubSub{
		publish:     publish,
		subscribe:   subscribe,
		unsubscribe: unsubscribe,
	}
}

func newLocalPubSubTopic() *localPubSub {
	finish := make(chan struct{})
	publish := make(chan *EventWrapper)
	subscribe := make(chan *subscription)
	unsubscribe := make(chan *subscription)

	go runLocalPubSubTopic(finish, publish, subscribe, unsubscribe)

	return &localPubSub{
		finish:      finish,
		publish:     publish,
		subscribe:   subscribe,
		unsubscribe: unsubscribe,
		subscribers: 0,
	}
}

func newLocalPubSubStream() *localPubSub {
	finish := make(chan struct{})
	publish := make(chan *EventWrapper)
	subscribe := make(chan *subscription)
	unsubscribe := make(chan *subscription)

	go runLocalPubSubStream(finish, publish, subscribe, unsubscribe)

	return &localPubSub{
		finish:      finish,
		publish:     publish,
		subscribe:   subscribe,
		unsubscribe: unsubscribe,
		subscribers: 0,
	}
}

func newLocalSub(ch chan<- *EventWrapper) *localSub {
	finish := make(chan struct{})
	publish := make(chan *EventWrapper)

	go runLocalSub(ch, finish, publish)

	return &localSub{
		finish:  finish,
		publish: publish,
	}
}

func (ps *LocalPubSub) Publish(
	topic string,
	stream uuid.UUID,
	content interface{},
) error {
	evt := &Event{
		Topic:   topic,
		Stream:  stream,
		Content: content,
	}

	data, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	ps.publish <- &EventWrapper{
		Encoded: data,
		Decoded: evt,
	}

	return nil
}

func (ps *LocalPubSub) Subscribe(
	topic string,
	stream uuid.UUID,
	ch chan<- *EventWrapper,
) error {
	ps.subscribe <- &subscription{
		topic:  topic,
		stream: stream,
		ch:     ch,
	}

	return nil
}

func (ps *LocalPubSub) Unsubscribe(
	topic string,
	stream uuid.UUID,
	ch chan<- *EventWrapper,
) error {
	ps.unsubscribe <- &subscription{
		topic:  topic,
		stream: stream,
		ch:     ch,
	}

	return nil
}

func runLocalPubSub(
	publish <-chan *EventWrapper,
	subscribe <-chan *subscription,
	unsubscribe <-chan *subscription,
) {
	subscriptions := make(map[string]*localPubSub)

	for {
		select {
		case evt := <-publish:
			if topic, ok := subscriptions[evt.Decoded.Topic]; ok {
				topic.publish <- evt
			}
		case sub := <-subscribe:
			topic, ok := subscriptions[sub.topic]
			if !ok {
				topic = newLocalPubSubTopic()
				subscriptions[sub.topic] = topic
			}

			topic.subscribe <- sub
		case sub := <-unsubscribe:
			if topic, ok := subscriptions[sub.topic]; ok {
				topic.unsubscribe <- sub
				topic.subscribers--
				if topic.subscribers == 0 {
					topic.finish <- struct{}{}
					delete(subscriptions, sub.topic)
				}
			}
		}
	}
}

func runLocalPubSubTopic(
	finish <-chan struct{},
	publish <-chan *EventWrapper,
	subscribe <-chan *subscription,
	unsubscribe <-chan *subscription,
) {
	subscriptions := make(map[uuid.UUID]*localPubSub)

	for {
		select {
		case <-finish:
			return
		case evt := <-publish:
			if stream, ok := subscriptions[evt.Decoded.Stream]; ok {
				stream.publish <- evt
			}
		case sub := <-subscribe:
			stream, ok := subscriptions[sub.stream]
			if !ok {
				stream = newLocalPubSubStream()
				subscriptions[sub.stream] = stream
			}

			stream.subscribe <- sub
		case sub := <-unsubscribe:
			if stream, ok := subscriptions[sub.stream]; ok {
				stream.unsubscribe <- sub
				stream.subscribers--
				if stream.subscribers == 0 {
					stream.finish <- struct{}{}
					delete(subscriptions, sub.stream)
				}
			}
		}
	}
}

func runLocalPubSubStream(
	finish <-chan struct{},
	publish <-chan *EventWrapper,
	subscribe <-chan *subscription,
	unsubscribe <-chan *subscription,
) {
	subscriptions := make(map[chan<- *EventWrapper]*localSub)

	for {
		select {
		case <-finish:
			return
		case evt := <-publish:
			for _, sub := range subscriptions {
				sub.publish <- evt
			}
		case sub := <-subscribe:
			if _, ok := subscriptions[sub.ch]; !ok {
				subscriptions[sub.ch] = newLocalSub(sub.ch)
			}
		case sub := <-unsubscribe:
			if subscription, ok := subscriptions[sub.ch]; ok {
				subscription.finish <- struct{}{}
				delete(subscriptions, sub.ch)
			}
		}
	}
}

func runLocalSub(
	ch chan<- *EventWrapper,
	finish <-chan struct{},
	publish <-chan *EventWrapper,
) {
	queue := make([]*EventWrapper, 0)

	for {
		if len(queue) == 0 {
			select {
			case <-finish:
				return
			case evt := <-publish:
				queue = append(queue, evt)
			}
		} else {
			select {
			case <-finish:
				return
			case evt := <-publish:
				queue = append(queue, evt)
			case ch <- queue[0]:
				queue = queue[1:]
			}
		}
	}
}
