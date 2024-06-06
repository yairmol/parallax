package main

import (
	"fmt"
	"parallax/parallax"
)

type PreResult string
type AlgoResult string
type PostResult string

func algoPreProcess(bytes []byte) PreResult {
	fmt.Printf("In PreProcess with %v\n", bytes)
	return PreResult(bytes)
}

func algoProcess(pre []PreResult) []AlgoResult {
	fmt.Printf("In algo with %v\n", pre)
	res := []AlgoResult{}
	for _, x := range pre {
		res = append(res, AlgoResult(x))
	}
	return res
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
