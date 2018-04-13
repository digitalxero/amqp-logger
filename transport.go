package amqp_logger

import (
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/streadway/amqp"
)

var amqpPool = struct {
	sync.RWMutex
	conns map[string]*amqp.Connection
}{conns: make(map[string]*amqp.Connection)}


type rabbitMQTransport struct {
	url string
	channel *amqp.Channel
	logger log.Logger
}

func newTransport(url string, logger log.Logger) *rabbitMQTransport {
	return &rabbitMQTransport{
		url: url,
		logger: logger,
	}
}

func (t *rabbitMQTransport) Dial() (err error) {
	var (
		conn *amqp.Connection
		channel *amqp.Channel
	)
	if conn, err = t.getAmqpConnection(); err != nil {
		return
	}

	if channel, err = conn.Channel(); err != nil {
		return
	}

	// Use fair dispatching instead of round-robin, if the worker is busy
	// the message will be dispatched to another worker.
	if err = channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	); err != nil {
		return
	}

	t.channel = channel
	return nil
}

func (t *rabbitMQTransport) Close() (err error) {
	if t.channel != nil {
		defer func(tr *rabbitMQTransport) {tr.channel = nil}(t)
		return t.channel.Close()
	}
	return nil
}

func (t *rabbitMQTransport) Publish(exchange, key string, msg amqp.Publishing) error {
	if t.channel == nil {
		if err := t.Dial(); err != nil {
			return err
		}
	}
	return t.channel.Publish(exchange, key, false, false, msg)
}

func (t *rabbitMQTransport) reconnectClosed(conn *amqp.Connection) {
	errCh := conn.NotifyClose(make(chan *amqp.Error, 1))

	go func() {
		// Block until a closed connection notification arrives
		err := <-errCh

		t.logger.Log(level.Key(), level.ErrorValue(), "msg", "Connection closed", "err", err)

		// Force recreating by removing from a map of opened connections
		delete(amqpPool.conns, t.url)
		t.channel = nil
	}()
}

func (t *rabbitMQTransport) handleBlocked(conn *amqp.Connection) {
	blockCh := conn.NotifyBlocked(make(chan amqp.Blocking))

	go func() {
		for b := range blockCh {
			if b.Active {
				t.logger.Log(level.Key(), level.InfoValue(), "msg", "Connection blocked")
			} else {
				t.logger.Log(level.Key(), level.InfoValue(), "msg", "Connection unblocked")
			}
		}
	}()
}

func (t *rabbitMQTransport) getAmqpConnection() (*amqp.Connection, error) {
	amqpPool.Lock()
	defer amqpPool.Unlock()

	if _, ok := amqpPool.conns[t.url]; !ok {
		t.logger.Log(level.Key(), level.InfoValue(), "msg", "Connecting", "url", t.url)
		conn, err := amqp.Dial(t.url)
		if err != nil {
			return nil, err
		}
		amqpPool.conns[t.url] = conn
		t.reconnectClosed(conn)
		t.handleBlocked(conn)
	}
	return amqpPool.conns[t.url], nil
}