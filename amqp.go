package amqp_logger

import (
	"bytes"
	"os"
	"io"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/streadway/amqp"
)

type AMQPLogWritter interface {
	Close() error
}

func WithAMQP(logger log.Logger, options ...Option) log.Logger {
	l := &amqpLogger{
		baseLogger: log.With(logger),
		appendLevel: true,
		amqpURL: "amqp://guest:guest@localhost:5672//",
		exchange: "amq.topic",
		routingKey: "log",
		newLogger: log.NewJSONLogger,
		contentType: "application/json",
		contentEncoding: "application/json",
	}

	for _, option := range options {
		option(l)
	}

	l.transport = newTransport(l.amqpURL, l.baseLogger)

	return l
}

func NewAMQPLogger(options ...Option) log.Logger {
	return WithAMQP(log.NewJSONLogger(os.Stdout), options...)
}

type amqpLogger struct {
	baseLogger log.Logger
	appendLevel bool
	amqpURL string
	exchange string
	routingKey string
	newLogger func(io.Writer) log.Logger
	contentType string
	contentEncoding string
	transport *rabbitMQTransport
}

func (l *amqpLogger) Log(keyvals ...interface{}) error {
	if err := l.baseLogger.Log(keyvals...); err != nil {
		return err
	}

	key := l.routingKey
	correlationId := ""
	if l.appendLevel {
		for i := 0; i < len(keyvals); i++ {
			if v, ok := keyvals[i].(level.Value); ok {
				key = key + "." + v.String()
			}
			if v, ok := keyvals[i].(string); ok && v == "correlation_id" {
				correlationId = keyvals[i+1].(string)
			}
		}
	}

	buf := &bytes.Buffer{}
	l.newLogger(buf).Log(keyvals...)
	msg := amqp.Publishing{
		ContentType: l.contentType,
		ContentEncoding: l.contentEncoding,
		Timestamp: time.Now(),
		Body: buf.Bytes(),
	}
	if correlationId != "" {
		msg.CorrelationId = correlationId
	}

	if err := l.transport.Publish(l.exchange, key, msg); err != nil {
		l.baseLogger.Log(level.Key(), level.ErrorValue(), "err", err)
		return err
	}
	return nil
}
