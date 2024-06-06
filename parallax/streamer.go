package parallax

import (
	"fmt"
	"sync"
	"time"
)

type Pair[T any, U any] struct {
	first  T
	second U
}

type Streamer[T any, U any] struct {
	inChannel      chan Pair[int, T]
	batchesChannel chan []Pair[int, T]
	outChannels    map[int]chan U

	algoProcess AlgoProcess[[]T, []U]

	nWorkers     int
	batchSize    int
	batchTimeout time.Duration
	// batchLatency time.Duration

	indexMutex   sync.Mutex
	currentIndex int
}

func newStreamer[T, U any](
	algoProcess AlgoProcess[[]T, []U],
	nWorkers int,
	batchSize int,
	batchTimeout time.Duration,
) *Streamer[T, U] {
	return &Streamer[T, U]{
		inChannel:      make(chan Pair[int, T]),
		outChannels:    make(map[int]chan U),
		batchesChannel: make(chan []Pair[int, T]),
		algoProcess:    algoProcess,
		nWorkers:       nWorkers,
		batchSize:      batchSize,
		batchTimeout:   batchTimeout,
	}

}

func (s *Streamer[T, U]) nextIndex() int {
	s.indexMutex.Lock()
	defer s.indexMutex.Unlock()
	s.currentIndex += 1
	return s.currentIndex
}

func (s *Streamer[T, U]) callAlgoProcess(m T) U {
	i := s.nextIndex()
	outChan := make(chan U)
	s.outChannels[i] = outChan
	p := Pair[int, T]{i, m}
	s.inChannel <- p
	res := <-outChan
	return res
}

func (s *Streamer[T, U]) worker(workerId int) {
	fmt.Printf("starting worker %v\n", workerId)
	for {
		batchPairs := <-s.batchesChannel
		fmt.Printf("received batch %v\n", batchPairs)
		batch := []T{}
		batchIndices := []int{}
		for _, p := range batchPairs {
			batchIndices = append(batchIndices, p.first)
			batch = append(batch, p.second)
		}
		batchRes := s.algoProcess(batch)
		for i, ind := range batchIndices {
			s.outChannels[ind] <- batchRes[i]
			close(s.outChannels[ind])
		}
	}
}

func (s *Streamer[T, U]) startWorkers() {
	for workerId := range s.nWorkers {
		go s.worker(workerId)
	}
}

func (s *Streamer[T, U]) start() {
	go s.stream()
}

func (s *Streamer[T, U]) stream() {
	batch := []Pair[int, T]{}
	batchStart := time.Now()
	for {
		timePassed := time.Since(batchStart)
		timeout := s.batchTimeout - timePassed
		select {
		case nextInp := <-s.inChannel:
			if len(batch) == 0 {
				batchStart = time.Now()
			}
			batch = append(batch, nextInp)
			if len(batch) == s.batchSize {
				fmt.Printf("sending batch with %v items\n", len(batch))
				s.batchesChannel <- batch
				batch = batch[:0]
			}
		case <-time.After(timeout):
			if len(batch) > 0 {
				fmt.Printf("sending batch with %v items\n", len(batch))
				s.batchesChannel <- batch
				batch = batch[:0]
			}
			batchStart = time.Now()
		}
	}
}
