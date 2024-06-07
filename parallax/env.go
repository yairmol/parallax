package parallax

import (
	"log"
	"os"
	"strconv"
)

func isSet(key string) bool {
	_, present := os.LookupEnv(key)
	return present
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func getEnvInt(key string, fallback string) (int, error) {
	s := getEnv(key, fallback)
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, err
	}
	return v, nil
}

func getEnvFloat(key string, fallback string) (float64, error) {
	s := getEnv(key, fallback)
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, err
	}
	return v, nil
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
