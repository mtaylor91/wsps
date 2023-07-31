package wsps

import (
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

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

type subscription struct {
	topic  string
	stream uuid.UUID
	ch     chan<- *EventWrapper
}

type subs map[chan<- *EventWrapper]*localPubSubSubscription

type streamSubs map[uuid.UUID]subs

type topicSubs map[string]streamSubs

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

func (ps *LocalPubSub) SubscribeChannel(
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

func (ps *LocalPubSub) UnsubscribeChannel(
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

func (ps *LocalPubSub) Publish(e *Event) error {
	logrus.WithFields(logrus.Fields{
		"topic":  e.Topic,
		"stream": e.Stream,
	}).Trace("Publishing event")

	w, err := e.Wrap()
	if err != nil {
		return err
	}

	ps.publish <- w

	return nil
}

func runLocalPubSub(
	publish <-chan *EventWrapper,
	subscribe <-chan *subscription,
	unsubscribe <-chan *subscription,
) {
	topicSubs := make(topicSubs)

	for {
		select {
		case evt := <-publish:
			topic, ok := topicSubs[evt.Decoded.Topic]
			if !ok {
				continue
			}

			stream, ok := topic[evt.Decoded.Stream]
			if !ok {
				continue
			}

			for _, sub := range stream {
				sub.publish <- evt
			}
		case sub := <-subscribe:
			topic, ok := topicSubs[sub.topic]
			if !ok {
				topic = make(streamSubs)
				topicSubs[sub.topic] = topic
			}

			stream, ok := topic[sub.stream]
			if !ok {
				stream = make(subs)
				topic[sub.stream] = stream
			}

			if _, ok := stream[sub.ch]; !ok {
				stream[sub.ch] = newLocalPubSubSubscription(
					sub.topic,
					sub.stream,
					sub.ch,
				)
			}
		case sub := <-unsubscribe:
			topic, ok := topicSubs[sub.topic]
			if !ok {
				continue
			}

			stream, ok := topic[sub.stream]
			if !ok {
				continue
			}

			if s, ok := stream[sub.ch]; ok {
				close(s.finish)
				delete(stream, sub.ch)
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
