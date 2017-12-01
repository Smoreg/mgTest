package main

import (
	"bufio"
	"io"
	"strings"
)

func newJob(source string, sourceProcess sourceProcessType, result chan JobResult) matchCountingJob {
	return matchCountingJob{
		source:       source,
		sourceGetter: sourceProcess,
		result:       result,
	}
}

type sourceProcessType func(string) (io.ReadCloser, error)

// Job general job interface
type Job interface {
	Run()
}

// JobResult general job result interface
type JobResult interface {
	Result() interface{}
	Error() error
}

type matchCountingJob struct {
	source       string
	sourceGetter sourceProcessType
	result       chan JobResult
}

// Run
func (j matchCountingJob) Run() {
	reader, err := j.sourceGetter(j.source)
	if err != nil {
		j.result <- matchCountingJobResult{source: j.source, err: err}
		return
	}
	defer reader.Close()
	j.result <- j.read(reader)
}

func (j matchCountingJob) read(reader io.Reader) (jr matchCountingJobResult) {
	jr.source = j.source
	bufioReader := bufio.NewReader(reader)
	for {
		line, errReader := bufioReader.ReadBytes('\n')

		if errReader == io.EOF {
			return
		} else if errReader != nil {
			jr.err = errReader
			return
		}

		if match := strings.Contains(string(line), "Go"); match {
			jr.count ++
		}
	}
}

type matchCountingJobResult struct {
	source string
	count  int
	err    error
}

// Result returns self in interface
func (m matchCountingJobResult) Result() interface{} {
	return m
}

// Result returns err
func (m matchCountingJobResult) Error() error {
	return m.err
}
