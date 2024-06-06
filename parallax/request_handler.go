package parallax

import (
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AlgoPreProcess[T any, U any] func(request T) (U, error)
type AlgoProcess[U any, V any] func(preProcessResult U) V
type AlgoPostProcess[V any, W any] func(algoResult V) (W, error)

type RequestHandler[PreProcessResult any, AlgoResult any] struct {
	algoPreProcess  AlgoPreProcess[[]byte, PreProcessResult]
	algoPostProcess AlgoPostProcess[AlgoResult, []byte]

	streamer     *Streamer[PreProcessResult, AlgoResult]
	rabbitClient *RabbitClient

	consumeQueue string
	publishQueue string
	errorQueue   string
}

func NewRequestHandler[U any, V any](
	algoPreProcess AlgoPreProcess[[]byte, U],
	algoProcess AlgoProcess[[]U, []Optional[V]],
	algoPostProcess AlgoPostProcess[V, []byte],
) RequestHandler[U, V] {
	nWorkers := getEnvIntOrPanic("WORKERS", "1")
	batchSize := getEnvIntOrPanic("BATCH_SIZE", "1")
	batchTimeoutSecs := getEnvFloatOrPanic("BATCH_TIMEOUT", "1")
	batchTimeout := time.Duration(batchTimeoutSecs*1000) * time.Millisecond
	streamer := newStreamer[U, V](algoProcess, nWorkers, batchSize, batchTimeout)
	rabbitClient := NewRabbitClient()
	consumeQueue := getEnv("CONSUME_QUEUE", "input")
	publishQueue := getEnv("PUBLISH_QUEUE", "output")
	errorQueue := getEnv("ERROR_QUEUE", "")

	return RequestHandler[U, V]{
		algoPreProcess,
		algoPostProcess,
		streamer,
		rabbitClient,
		consumeQueue,
		publishQueue,
		errorQueue,
	}
}

func (r *RequestHandler[U, V]) consume(c *RabbitConnectionParams) {
	connStr := c.formatConnectionString()
	fmt.Println(connStr)
	err := r.rabbitClient.Connect(connStr, 5)
	failOnError(err, "")
	defer r.rabbitClient.Close()

	r.rabbitClient.DeclareQueue("hello")
	failOnError(err, "")

	fmt.Println("starting to consume")
	msgs, err := r.rabbitClient.Consume()
	failOnError(err, "")

	var forever chan struct{}

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			go r.consumeMessage(&d)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func (r *RequestHandler[U, V]) publishError(err error) {
	if r.errorQueue == "" {
		return
	}
	r.rabbitClient.Publish([]byte(fmt.Sprintf("%v", err)), r.errorQueue)
}

func (r *RequestHandler[U, V]) consumeMessage(d *amqp.Delivery) {
	defer r.rabbitClient.ch.Ack(d.DeliveryTag, false)
	pre, err := r.algoPreProcess(d.Body)
	if err != nil {
		r.publishError(err)
		return
	}
	algoRes, err := r.streamer.callAlgoProcess(pre)
	if err != nil {
		r.publishError(err)
		return
	}
	response, err := r.algoPostProcess(algoRes)
	if err != nil {
		r.publishError(err)
		return
	}
	r.rabbitClient.Publish(response, "output")
}

func (r *RequestHandler[U, V]) StartConsuming() {
	c := rabbitParamsFromEnv()
	r.streamer.startWorkers()
	r.streamer.start()
	r.consume(c)
}
