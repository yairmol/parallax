package main

import (
	"fmt"
	"parallax/parallax"
)

// func failOnError(err error, msg string) {
// 	if err != nil {
// 		log.Panicf("%s: %s", msg, err)
// 	}
// }

// func hey() {
// 	for {
// 		time.Sleep(time.Millisecond * 10)
// 		fmt.Println("hey")
// 	}
// }

// func main() {
// 	hey()
// 	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
// 	failOnError(err, "Failed to connect to RabbitMQ")
// 	defer conn.Close()
// 	ch, err := conn.Channel()
// 	failOnError(err, "Failed to open a channel")
// 	defer ch.Close()
// 	q, err := ch.QueueDeclare(
// 		"hello", // name
// 		false,   // durable
// 		false,   // delete when unused
// 		false,   // exclusive
// 		false,   // no-wait
// 		nil,     // arguments
// 	)
// 	failOnError(err, "Failed to declare a queue")

// 	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
// 	defer cancel()

// 	messages := []string{
// 		"Hi",
// 		"Hello",
// 		"Whatsapp",
// 	}
// 	for range 100 {
// 		for _, body := range messages {
// 			err = ch.PublishWithContext(ctx,
// 				"",     // exchange
// 				q.Name, // routing key
// 				false,  // mandatory
// 				false,  // immediate
// 				amqp.Publishing{
// 					ContentType: "text/plain",
// 					Body:        []byte(body),
// 				})
// 			failOnError(err, "Failed to publish a message")
// 			log.Printf(" [x] Sent %s\n", body)
// 		}
// 	}
// }

type PreResult string
type AlgoResult string
type PostResult string

func algoPreProcess(bytes []byte) PreResult {
	fmt.Printf("In PreProcess with %v\n", bytes)
	return PreResult(bytes)
}

func algoProcess(pre PreResult) AlgoResult {
	fmt.Printf("In algo with %v\n", pre)
	return AlgoResult(pre)
}

func algoPostProcess(pre AlgoResult) PostResult {
	fmt.Printf("In PostProcess with %v\n", pre)
	return PostResult(pre)
}

func main() {
	r := parallax.NewRequestHandler(
		algoPreProcess,
		algoProcess,
		algoPostProcess,
	)
	r.StartConsuming()
}
