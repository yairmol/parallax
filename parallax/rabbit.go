package parallax

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitConnectionParams struct {
	username string
	password string
	host     string
	port     uint16
	prefetch int
}

type RabbitQueueConn struct {
	conn *amqp.Connection
	ch   *amqp.Channel
	q    *amqp.Queue
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func (c *RabbitConnectionParams) formatConnectionString() string {
	return fmt.Sprintf("amqp://%v:%v@%v:%v/", c.username, c.password, c.host, c.port)
}

func rabbitParamsFromEnv() *RabbitConnectionParams {
	port, err := getEnvInt("RABBIT_PORT", "5672")
	if err != nil {
		log.Panic(err)
	}
	prefetch, err := getEnvInt("RABBIT_PREFETCH", "1")
	if err != nil {
		log.Panic(err)
	}
	return &RabbitConnectionParams{
		getEnv("RABBIT_USERNAME", "guest"),
		getEnv("RABBIT_PASSWORD", "guest"),
		getEnv("RABBIT_HOST", "localhost"),
		uint16(port),
		prefetch,
	}
}
