package main

import (
	"fmt"
	"parallax/parallax"
	"time"
)

type PreResult string
type AlgoResult string
type PostResult string

func algoPreProcess(bytes []byte) (PreResult, error) {
	fmt.Printf("In PreProcess with %v\n", bytes)
	time.Sleep(100 * time.Millisecond)
	return PreResult(bytes), nil
}

func algoProcess(pre []PreResult) []parallax.Optional[AlgoResult] {
	fmt.Printf("In algo with %v\n", pre)
	res := []parallax.Optional[AlgoResult]{}
	for _, x := range pre {
		var opt parallax.Optional[AlgoResult]
		if x == PreResult("Hey") {
			opt = parallax.Optional[AlgoResult]{Err: fmt.Errorf("hey :(")}
		} else {
			opt = parallax.Optional[AlgoResult]{V: AlgoResult(x), Err: nil}
		}
		res = append(res, opt)
	}
	time.Sleep(100 * time.Millisecond)
	return res
}

func algoPostProcess(algoRes AlgoResult) ([]byte, error) {
	fmt.Printf("In PostProcess with %v\n", algoRes)
	res := []byte(algoRes)
	time.Sleep(100 * time.Millisecond)
	return res, nil
}

func main() {
	config := parallax.ConfigFromEnv()
	r := parallax.NewRequestHandler(
		config,
		algoPreProcess,
		algoProcess,
		algoPostProcess,
	)
	r.StartConsuming(true)
}
