package parallax

import (
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

	mapMutex     sync.Mutex
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

func (s *Streamer[T, U]) setOutputChannel(i int, outChan chan U) {
	s.mapMutex.Lock()
	defer s.mapMutex.Unlock()
	s.outChannels[i] = outChan
}

func (s *Streamer[T, U]) removeOutputChannel(i int) {
	s.mapMutex.Lock()
	defer s.mapMutex.Unlock()
	delete(s.outChannels, i)
}

func (s *Streamer[T, U]) callAlgoProcess(m T) (U, error) {
	i := s.nextIndex()
	outChan := make(chan U)
	s.setOutputChannel(i, outChan)
	p := Pair[int, T]{i, m}
	s.inChannel <- p
	res := <-outChan
	s.removeOutputChannel(i)
	return res, nil
}

func (s *Streamer[T, U]) getOutputChannel(i int) chan U {
	s.mapMutex.Lock()
	defer s.mapMutex.Unlock()
	return s.outChannels[i]
}

func (s *Streamer[T, U]) worker(workerId int) {
	for {
		batchPairs := <-s.batchesChannel
		batch := []T{}
		batchIndices := []int{}
		for _, p := range batchPairs {
			batchIndices = append(batchIndices, p.first)
			batch = append(batch, p.second)
		}
		batchRes, err := s.algoProcess(batch)
		if err != nil {
			continue
		}
		for i, ind := range batchIndices {
			outChan := s.getOutputChannel(ind)
			outChan <- batchRes[i]
			close(outChan)
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
	batch := make([]Pair[int, T], 0)
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
				s.batchesChannel <- batch
				batch = make([]Pair[int, T], 0)
			}
		case <-time.After(timeout):
			if len(batch) > 0 {
				s.batchesChannel <- batch
				batch = batch[:0]
			}
			batchStart = time.Now()
		}
	}
}
