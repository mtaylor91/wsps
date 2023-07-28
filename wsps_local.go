package wsps

import (
	"encoding/json"

	"github.com/google/uuid"
)

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

type localPubSubTopic struct {
	topic       string
	finish      chan<- struct{}
	publish     chan<- *EventWrapper
	subscribe   chan<- *subscription
	unsubscribe chan<- *subscription
	subscribers int
}

type localPubSubStream struct {
	topic       string
	stream      uuid.UUID
	finish      chan<- struct{}
	publish     chan<- *EventWrapper
	subscribe   chan<- *subscription
	unsubscribe chan<- *subscription
	subscribers int
}

type localPubSubSubscription struct {
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

func newLocalPubSubTopic(topic string) *localPubSubTopic {
	finish := make(chan struct{})
	publish := make(chan *EventWrapper)
	subscribe := make(chan *subscription)
	unsubscribe := make(chan *subscription)

	go runLocalPubSubTopic(topic, finish, publish, subscribe, unsubscribe)

	return &localPubSubTopic{
		topic:       topic,
		finish:      finish,
		publish:     publish,
		subscribe:   subscribe,
		unsubscribe: unsubscribe,
		subscribers: 0,
	}
}

func newLocalPubSubStream(topic string, stream uuid.UUID) *localPubSubStream {
	finish := make(chan struct{})
	publish := make(chan *EventWrapper)
	subscribe := make(chan *subscription)
	unsubscribe := make(chan *subscription)

	go runLocalPubSubStream(topic, stream, finish, publish, subscribe, unsubscribe)

	return &localPubSubStream{
		topic:       topic,
		stream:      stream,
		finish:      finish,
		publish:     publish,
		subscribe:   subscribe,
		unsubscribe: unsubscribe,
		subscribers: 0,
	}
}

func newLocalPubSubSubscription(
	topic string,
	stream uuid.UUID,
	ch chan<- *EventWrapper,
) *localPubSubSubscription {
	finish := make(chan struct{})
	publish := make(chan *EventWrapper)

	go runLocalPubSubSubscription(topic, stream, ch, finish, publish)

	return &localPubSubSubscription{
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
	subscriptions := make(map[string]*localPubSubTopic)

	for {
		select {
		case evt := <-publish:
			if topic, ok := subscriptions[evt.Decoded.Topic]; ok {
				topic.publish <- evt
			}
		case sub := <-subscribe:
			topic, ok := subscriptions[sub.topic]
			if !ok {
				topic = newLocalPubSubTopic(sub.topic)
				subscriptions[sub.topic] = topic
			}

			topic.subscribe <- sub
			topic.subscribers++
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
	topic string,
	finish <-chan struct{},
	publish <-chan *EventWrapper,
	subscribe <-chan *subscription,
	unsubscribe <-chan *subscription,
) {
	subscriptions := make(map[uuid.UUID]*localPubSubStream)

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
				stream = newLocalPubSubStream(topic, sub.stream)
				subscriptions[sub.stream] = stream
			}

			stream.subscribe <- sub
			stream.subscribers++
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
	topic string,
	stream uuid.UUID,
	finish <-chan struct{},
	publish <-chan *EventWrapper,
	subscribe <-chan *subscription,
	unsubscribe <-chan *subscription,
) {
	subscriptions := make(map[chan<- *EventWrapper]*localPubSubSubscription)

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
				subscriptions[sub.ch] =
					newLocalPubSubSubscription(topic, stream, sub.ch)
			}
		case sub := <-unsubscribe:
			if subscription, ok := subscriptions[sub.ch]; ok {
				subscription.finish <- struct{}{}
				delete(subscriptions, sub.ch)
			}
		}
	}
}

func runLocalPubSubSubscription(
	topic string,
	stream uuid.UUID,
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
				select {
				case ch <- evt:
				default:
					queue = append(queue, evt)
				}
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
