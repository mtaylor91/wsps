package wsps

import "github.com/gorilla/websocket"

type PubSubClient struct {
	dialer *websocket.Dialer
	local  *LocalPubSub
}

type PubSubConnection struct {
	topic     string
	prototype interface{}
	wsConn    *websocket.Conn
}

func NewPubSubClient(local *LocalPubSub) *PubSubClient {
	return &PubSubClient{
		dialer: &websocket.Dialer{},
		local:  local,
	}
}

func (c *PubSubClient) NewConnection(
	endpoint, topic string,
	prototype interface{},
) (*PubSubConnection, error) {
	wsConn, _, err := c.dialer.Dial(endpoint, nil)
	if err != nil {
		return nil, err
	}

	return &PubSubConnection{
		topic:     topic,
		prototype: prototype,
		wsConn:    wsConn,
	}, nil
}
