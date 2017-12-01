package main

import (
	"io"
	"bufio"
	"os"
	"fmt"
	"sync"
	"flag"
	"strings"
	"net/http"
	"errors"
	"time"
)

// dispatcherBufferSize saves from necessary switching between goroutines
const (
	dispatcherBufferSize = 1000
	waitGroupTimeout     = 10 * time.Second
)

// BadOptionError returns then options arg was wrong
type BadOptionError struct {
	msg string
}

// Error return err msg
func (boe BadOptionError) Error() string {
	return boe.msg
}

// StartLineCounter starts dispatcher, sent jobs to it, and aggregate results
// sourceReader - reader with sources (url, file path etc.) separated by "\n"
func StartLineCounter(sourceReader io.Reader, resultWriter io.Writer, sourceType string, maxWorkers int) (err error) {

	var sourceProcess sourceProcessType // Every job process source with sourceProcess before count target string ("Go")

	switch sourceType {
	case "url":
		sourceProcess = urlProcess
	case "file":
		sourceProcess = fileProcess
	default:
		return BadOptionError{"BadSource"}
	}

	if maxWorkers <= 0 {
		return BadOptionError{"BadMaxWorkers"}
	}
	// DISPATCHER -------------------
	jobsQueue := make(chan Job, dispatcherBufferSize)
	resultQueue := make(chan JobResult, dispatcherBufferSize)
	shutdown := make(chan bool)

	var jobsWg sync.WaitGroup

	dispatcherInstance := NewDispatcher(jobsQueue, resultQueue, maxWorkers)
	dispatcherInstance.Run()

	// JOBS -------------------------
	reader := bufio.NewReader(sourceReader)

	go func() {
		sum := 0 // total count
		for r := range resultQueue {
			jr, ok := r.Result().(matchCountingJobResult)
			if !ok {
				jr.source = "Unknown"
				jr.err = errors.New("bad result type")
			}
			if jr.err == nil {
				fmt.Fprintf(resultWriter, "Count for %v : %v \n", jr.source, jr.count)
				sum += jr.count
			} else {
				fmt.Fprintf(resultWriter, "Error for %v : %v \n", jr.source, jr.err)
			}
			jobsWg.Done()
		}
		fmt.Fprintf(resultWriter, "Total : %v\n", sum)
		close(shutdown)
	}()

	sourceSlice := make(chan string) // chan for sources like urls or filepaths

	go func() {
		for { // read sources
			source, errReader := reader.ReadString('\n')
			if errReader == io.EOF {
				close(sourceSlice)
				return
			} else if errReader != nil {
				err = errReader
				dispatcherInstance.Stop()
				close(sourceSlice)
				return
			}
			sourceSlice <- strings.Trim(source, "\n")
		}
	}()

	for source := range sourceSlice {
		jobsWg.Add(1)
		jobsQueue <- newJob(source, sourceProcess, resultQueue)
	}

	close(jobsQueue)
	jobsWg.Wait()

	close(resultQueue)
	<-shutdown
	return err
}

func urlProcess(source string) (ur io.ReadCloser, err error) {
	resp, err := http.Get(source)
	if err != nil {
		return
	}
	return resp.Body, nil
}

func fileProcess(source string) (ur io.ReadCloser, err error) {
	file, err := os.Open(source)
	if err != nil {
		return
	}
	return file, nil
}

func main() {
	var sourceType string
	var maxWorkers int

	flag.StringVar(&sourceType, "type", "", "'file' or 'url'")
	flag.IntVar(&maxWorkers, "maxWorkers", 5, "max number of workers")
	flag.Parse()

	err := StartLineCounter(os.Stdin, os.Stdout, sourceType, maxWorkers)

	if err != nil {

		if _, ok := err.(BadOptionError); ok {
			switch err.Error() {
			case "BadSource":
				fmt.Println("Please use option -type= 'file' or 'url'")
			case "BadMaxWorkers":
				fmt.Printf("Please use option -type= 'file' or 'url', not %v", sourceType)
			default:
				fmt.Printf("Unpredictabke option error: %v", err.Error())
			}
		} else {
			fmt.Println(err.Error())
		}
	}
}
