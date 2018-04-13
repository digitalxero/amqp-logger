package amqp_logger

import (
	"github.com/go-kit/kit/log"
)

type Option func(*amqpLogger)

func AMQPURL(url string) Option {
	return func(l *amqpLogger) { l.amqpURL = url }
}

func AMQPExchange(exchange string) Option {
	return func(l *amqpLogger) { l.exchange = exchange }
}

func AMQPRoutingKey(routingKey string) Option {
	return func(l *amqpLogger) { l.routingKey = routingKey }
}

func AppendLevel(appendLevel bool) Option {
	return func(l *amqpLogger) { l.appendLevel = appendLevel }
}

func JSONLogger() Option {
	return func(l *amqpLogger) {
		l.newLogger = log.NewJSONLogger
		l.contentType = "application/json"
		l.contentEncoding = "application/json"
	}
}

func LOGFMTLogger() Option {
	return func(l *amqpLogger) {
		l.newLogger = log.NewLogfmtLogger
		l.contentType = "text"
		l.contentEncoding = "text"
	}
}