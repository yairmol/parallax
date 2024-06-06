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

func algoProcess(pre []PreResult) ([]AlgoResult, error) {
	fmt.Printf("In algo with %v\n", pre)
	res := []AlgoResult{}
	for _, x := range pre {
		res = append(res, AlgoResult(x))
	}
	time.Sleep(100 * time.Millisecond)
	return res, nil
}

func algoPostProcess(algoRes AlgoResult) ([]byte, error) {
	fmt.Printf("In PostProcess with %v\n", algoRes)
	res := []byte(algoRes)
	time.Sleep(100 * time.Millisecond)
	return res, nil
}

func main() {
	r := parallax.NewRequestHandler(
		algoPreProcess,
		algoProcess,
		algoPostProcess,
	)
	r.StartConsuming()
}
