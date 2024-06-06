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

type RabbitClient struct {
	conn *amqp.Connection
	ch   *amqp.Channel
	q    amqp.Queue
}

func NewRabbitClient() *RabbitClient {
	return &RabbitClient{}
}

func (client *RabbitClient) Connect(url string, prefetch int) error {
	var err error
	client.conn, err = amqp.Dial(url)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	client.ch, err = client.conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open a channel: %w", err)
	}
	client.ch.Qos(prefetch, 0, false)

	return nil
}

func (client *RabbitClient) DeclareQueue(name string) error {
	var err error
	client.q, err = client.ch.QueueDeclare(
		name,  // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare a queue: %w", err)
	}
	return nil
}

func (client *RabbitClient) Publish(body []byte, queue string) error {
	err := client.ch.Publish(
		"",    // exchange
		queue, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
		})
	if err != nil {
		return fmt.Errorf("failed to publish a message: %w", err)
	}
	fmt.Println(" [x] Sent", body, "to", queue)
	return nil
}

func (client *RabbitClient) Consume() (<-chan amqp.Delivery, error) {
	msgs, err := client.ch.Consume(
		client.q.Name, // queue
		"",            // consumer
		false,         // auto-ack
		false,         // exclusive
		false,         // no-local
		false,         // no-wait
		nil,           // args
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register a consumer: %w", err)
	}
	return msgs, nil
}

func (client *RabbitClient) Close() {
	client.ch.Close()
	client.conn.Close()
}
