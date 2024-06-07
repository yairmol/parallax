package parallax

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
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

func (r *RequestHandler[U, V]) processMessage(body []byte) ([]byte, error) {
	pre, err := r.algoPreProcess(body)
	if err != nil {
		return nil, err
	}
	algoRes, err := r.streamer.callAlgoProcess(pre)
	if err != nil {
		return nil, err
	}
	return r.algoPostProcess(algoRes)
}

func (r *RequestHandler[U, V]) consumeMessage(d *amqp.Delivery) {
	defer r.rabbitClient.ch.Ack(d.DeliveryTag, false)
	response, err := r.processMessage(d.Body)
	if err != nil {
		r.publishError(err)
	} else {
		r.rabbitClient.Publish(response, "output")
	}
}

func (r *RequestHandler[U, V]) serveOnRest() {
	router := gin.Default()
	router.POST("/run", func(c *gin.Context) {

		buf := new(bytes.Buffer)
		buf.ReadFrom(c.Request.Body)
		request := buf.String()
		response, err := r.processMessage([]byte(request))
		if err != nil {
			errMsg := gin.H{"message": fmt.Sprintf("%v", err)}
			c.JSON(http.StatusInternalServerError, errMsg)
		} else {
			c.Data(http.StatusOK, "text/plain", response)
		}
	})
	router.Run("localhost:8080")
}

func (r *RequestHandler[U, V]) StartConsuming(serveHttp bool) {
	c := rabbitParamsFromEnv()
	r.streamer.startWorkers()
	r.streamer.start()
	if serveHttp {
		go r.serveOnRest()
	}
	r.consume(c)
}
