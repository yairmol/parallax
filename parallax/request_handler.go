package parallax

import (
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AlgoPreProcess[Request any, PreProcessResult any] func(request Request) PreProcessResult
type AlgoProcess[PreProcessResult any, AlgoResult any] func(preProcessResult PreProcessResult) AlgoResult
type AlgoPostProcess[AlgoResult any, PostProcessResult any] func(algoResult AlgoResult) PostProcessResult

type RequestHandler[PreProcessResult any, AlgoResult any, PostProcessResult any] struct {
	algoPreProcess  AlgoPreProcess[[]byte, PreProcessResult]
	algoPostProcess AlgoPostProcess[AlgoResult, PostProcessResult]

	streamer *Streamer[PreProcessResult, AlgoResult]
}

func getEnvIntOrPanic(key string, fallback string) int {
	n, err := getEnvInt(key, fallback)
	if err != nil {
		log.Panic(err)
	}
	return n
}

func getEnvFloatOrPanic(key string, fallback string) float64 {
	f, err := getEnvFloat(key, fallback)
	if err != nil {
		log.Panic(err)
	}
	return f
}

func NewRequestHandler[U any, V any, W any](
	algoPreProcess AlgoPreProcess[[]byte, U],
	algoProcess AlgoProcess[[]U, []V],
	algoPostProcess AlgoPostProcess[V, W],
) RequestHandler[U, V, W] {
	nWorkers := getEnvIntOrPanic("WORKERS", "1")
	batchSize := getEnvIntOrPanic("BATCH_SIZE", "1")
	batchTimeoutSecs := getEnvFloatOrPanic("BATCH_TIMEOUT", "1")
	batchTimeout := time.Duration(batchTimeoutSecs*1000) * time.Millisecond
	streamer := newStreamer[U, V](algoProcess, nWorkers, batchSize, batchTimeout)
	return RequestHandler[U, V, W]{
		algoPreProcess,
		algoPostProcess,
		streamer,
	}
}

func (r *RequestHandler[U, V, W]) consume(c *RabbitConnectionParams) {
	connStr := c.formatConnectionString()
	fmt.Println(connStr)
	conn, err := amqp.Dial(connStr)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	ch.Qos(5, 0, false)
	q, err := ch.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")
	qc := &RabbitQueueConn{conn, ch, &q}

	fmt.Println("starting to consume")
	msgs, err := qc.ch.Consume(
		qc.q.Name, // queue
		"",        // consumer
		false,     // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	failOnError(err, "Failed to register a consumer")

	var forever chan struct{}

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			go r.consumeMessage(qc, &d)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func (r *RequestHandler[U, V, W]) consumeMessage(qc *RabbitQueueConn, d *amqp.Delivery) {
	pre := r.algoPreProcess(d.Body)
	algoRes := r.streamer.callAlgoProcess(pre)
	r.algoPostProcess(algoRes)
	qc.ch.Ack(d.DeliveryTag, false)
}

func (r *RequestHandler[U, V, W]) StartConsuming() {
	c := rabbitParamsFromEnv()
	r.streamer.startWorkers()
	r.streamer.start()
	// qc := connectToQueue(c)
	r.consume(c)
}
