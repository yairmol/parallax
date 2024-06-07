package parallax

import "time"

type ParallaxConfig struct {
	nWorkers     int
	batchSize    int
	batchTimeout time.Duration
	consumeQueue string
	publishQueue string
	errorQueue   string
	serveHttp    bool
	host         string
	port         uint16
}

type StreamerConfig struct {
	nWorkers     int
	batchSize    int
	batchTimeout time.Duration
}

func ConfigFromEnv() *ParallaxConfig {
	nWorkers := getEnvIntOrPanic("WORKERS", "1")
	batchSize := getEnvIntOrPanic("BATCH_SIZE", "1")
	batchTimeoutSecs := getEnvFloatOrPanic("BATCH_TIMEOUT", "1")
	batchTimeout := time.Duration(batchTimeoutSecs*1000) * time.Millisecond
	consumeQueue := getEnv("CONSUME_QUEUE", "input")
	publishQueue := getEnv("PUBLISH_QUEUE", "output")
	errorQueue := getEnv("ERROR_QUEUE", "")
	serveHttp := isSet("SERVE_HTTP")
	host := getEnv("HOST", "localhost")
	port := uint16(getEnvIntOrPanic("PORT", "8080"))
	return &ParallaxConfig{
		nWorkers:     nWorkers,
		batchSize:    batchSize,
		batchTimeout: batchTimeout,
		consumeQueue: consumeQueue,
		publishQueue: publishQueue,
		errorQueue:   errorQueue,
		serveHttp:    serveHttp,
		host:         host,
		port:         port,
	}
}

func (c *ParallaxConfig) GetStreamerConfig() *StreamerConfig {
	return &StreamerConfig{
		nWorkers:     c.nWorkers,
		batchSize:    c.batchSize,
		batchTimeout: c.batchTimeout,
	}
}
