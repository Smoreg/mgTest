package main

import (
	"context"
	"sync"
	"time"
)

// Dispatcher simple jobs dispatcherdddddddddddddddddddd
type Dispatcher struct {
	workerPool chan chan Job // chans from workers ready to new jobs
	jobQueue   chan Job
	result     chan JobResult // chan for workers results

	workersCtx context.Context
	ctxCancel  context.CancelFunc // stop workers
	workersWg  sync.WaitGroup

	maxWorkers     int
	currentWorkers int
}

// NewDispatcher creates, and returns a new Dispatcher object.
func NewDispatcher(jobQueue chan Job, result chan JobResult, maxWorkers int) *Dispatcher {
	workerPool := make(chan chan Job, maxWorkers)
	ctx, cancel := context.WithCancel(context.Background())
	return &Dispatcher{
		jobQueue:   jobQueue,
		result:     result,
		maxWorkers: maxWorkers,
		workerPool: workerPool,
		workersCtx: ctx,
		ctxCancel:  cancel,
		workersWg:  sync.WaitGroup{},
	}
}

func (d *Dispatcher) dispatch() {

	for job := range d.jobQueue {
		if d.currentWorkers < d.maxWorkers && len(d.workerPool) == 0 {
			d.currentWorkers++
			worker := NewWorker(d.workersCtx, d.workerPool, &d.workersWg)
			d.workersWg.Add(1)
			worker.start()
		}
		workerJobQueue := <-d.workerPool
		workerJobQueue <- job
	}

}

// Run dispatcher
func (d *Dispatcher) Run() {
	go d.dispatch()
}

// Stop dispatcher
func (d *Dispatcher) Stop() {
	close(d.jobQueue)
	d.ctxCancel()
	waitTimeout(&d.workersWg, waitGroupTimeout)
}

// Worker for
type Worker struct {
	jobQueue   chan Job
	workerPool chan chan Job
	workersCtx context.Context
	wg         *sync.WaitGroup
}

// NewWorker creates takes a numeric id and a channel w/ worker pool.
func NewWorker(ctx context.Context, workerPool chan chan Job, wg *sync.WaitGroup) (w Worker) {
	return Worker{
		workersCtx: ctx,
		jobQueue:   make(chan Job),
		workerPool: workerPool,
		wg:         wg,
	}
}

func (w Worker) start() {
	go func() {
		for {
			w.workerPool <- w.jobQueue

			select {
			case job := <-w.jobQueue:
				job.Run()
			case <-w.workersCtx.Done():
				w.wg.Done()
				return
			}
		}
	}()
}

// waitTimeout waitGroup released after timeout
func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}
