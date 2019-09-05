package jobqueue

import (
	"fmt"
)

type job interface {
	Process() error
}

type worker struct {
	ID            int
	WorkerChannel chan chan job
	Channel       chan job
	End           chan struct{}
}

func (w *worker) start() {
	go func() {
		for {
			// Worker will post to worker channel if it is idling
			w.WorkerChannel <- w.Channel
			select {
			case <-w.End:
				return
			case job := <-w.Channel:
				fmt.Println("Worker ID", w.ID)
				job.Process()
			}
		}
	}()
}

func (w *worker) stop() {
	w.End <- struct{}{}
}

type dispatcher struct {
	Work chan job
	End  chan struct{}
}

func newDispatcher(workerCount int) dispatcher {
	var workers []worker
	input := make(chan job)
	end := make(chan struct{})
	jobDispatcher := dispatcher{Work: input, End: end}
	workerChannel := make(chan chan job)

	for i := 0; i < workerCount; i++ {
		fmt.Println(fmt.Sprintf("Starting worker: %d", i))
		newWorker := worker{
			ID:            i,
			Channel:       make(chan job),
			WorkerChannel: workerChannel,
			End:           make(chan struct{}),
		}
		newWorker.start()
		workers = append(workers, newWorker)
	}

	go func() {
		for {
			select {
			case <-end:
				for _, w := range workers {
					w.stop()
				}
				close(workerChannel)
				close(input)
				close(end)
				return
			case work := <-input:
				// Check which worker is available when there is work to do
				worker := <-workerChannel
				worker <- work

				// TODO Check if there is error reported
			}
		}
	}()

	return jobDispatcher
}
