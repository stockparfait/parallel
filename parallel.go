// Copyright 2022 Stock Parfait

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package parallel implements job processing using parallel workers.
package parallel

import (
	"context"
	"sync"

	"google.golang.org/api/iterator"
)

type contextKey int

const (
	parallelKey contextKey = iota
)

// TestSerialize forces the number of workers in Map to be 1, thereby running
// jobs serially and in the strict FIFO order. This helps make tests
// deterministic.
func TestSerialize(ctx context.Context) context.Context {
	return context.WithValue(ctx, parallelKey, true)
}

func isSerialized(ctx context.Context) bool {
	v, ok := ctx.Value(parallelKey).(bool)
	return ok && v
}

// Job is a unit of work to be done, which returns a result and an error.
//
// Note: a Job instance cannot return iterator.Done as an error; it is reserved
// for iterators, and will break their functionality.
type Job = func() (interface{}, error)

// JobsIter is a generator of work units for executing in Map. The iterator
// complies with Google Cloud iterator guidelines: it either returns a work unit
// as a callable function, or an error. When the error is iterator.Done, it's
// the end of the sequence (and not really an error).
type JobsIter interface {
	Next() (Job, error)
}

// ResultsIter is an iterator over jobs results merged from parallel runs.
//
// Note, that Next() may return a non-nil error from a job, but only
// iterator.Done means all the jobs have finished.
//
// Also, when error is iterator.Done, a non-nil value can be a non-job related
// error, e.g. from JobsIter.
type ResultsIter interface {
	Next() (interface{}, error)
}

// Result of a Job as a single value, including the error.
type Result struct {
	Value interface{}
	Error error
}

type mapIter struct {
	ctx     context.Context // potentially cancelable context
	workers int             // maximum number of parallel jobs allowed
	jobs    int             // number of jobs currently running
	it      JobsIter        // job iterator
	resCh   chan Result     // workers send their results to this channel
	done    error           // non-nil error returned by the job iterator
	mux     sync.Mutex      // to make Next() go routine safe
}

var _ ResultsIter = &mapIter{}

// Map runs multiple jobs in parallel on a given number of workers, 0=unlimited,
// collects their results and returns as an iterator. The order of results is
// not guaranteed, unless the number of workers is 1.
//
// Canceling the supplied context immediately stops queuing new jobs, but the
// jobs that already started will finish and their results will be returned.
// Therefore, it is important to flush the iterator after canceling the context
// to release all the resources.
//
// No job is started by this method itself. Jobs begin to run on the first
// Next() call on the result iterator, which is go routine safe.
//
// If the supplied JobsIter returns an error other than iterator.Done, the
// Next() call returns iterator.Done and returns the original error as a value.
// It is, therefore, important to check the value field when the ResultsIter
// completes.
//
// Example usage:
//
//   m := Map(context.Background(), 2, jobsIter)
//   for {
//     v, err := m.Next()
//     if err == iterator.Done {
//       break
//     }
//     // Process v and err.
//   }
func Map(ctx context.Context, workers int, it JobsIter) ResultsIter {
	if isSerialized(ctx) {
		workers = 1
	}
	return &mapIter{
		ctx:     ctx,
		workers: workers,
		resCh:   make(chan Result),
		it:      it,
	}
}

// startJobs starts as many jobs as possible given the number of workers.
func (m *mapIter) startJobs() {
	if m.done != nil {
		return
	}
	for ; m.workers <= 0 || m.jobs < m.workers; m.jobs++ {
		select {
		case <-m.ctx.Done():
			m.done = iterator.Done
			return
		default:
		}
		j, err := m.it.Next()
		if err != nil {
			m.done = err
			return
		}
		go func() {
			var r Result
			r.Value, r.Error = j()
			m.resCh <- r
		}()
	}
}

// Next implements ResultsIter. It runs jobs in parallel up to the number of
// workers, blocks till at least one finishes (if any), and returns its result.
// When no more jobs are left, return iterator.Done error. Go routine safe.
func (m *mapIter) Next() (interface{}, error) {
	m.mux.Lock()
	defer m.mux.Unlock()

	m.startJobs()
	if m.jobs == 0 {
		err := m.done
		if err == iterator.Done {
			err = nil
		}
		return err, iterator.Done
	}
	r := <-m.resCh
	m.jobs--
	m.startJobs()
	return r.Value, r.Error
}

// jobs is an implementation of JobsIter from a slice of Job.
type jobs struct {
	jobs []Job
	i    int
}

var _ JobsIter = &jobs{}

func (j *jobs) Next() (Job, error) {
	if j.i >= len(j.jobs) {
		return nil, iterator.Done
	}
	i := j.i
	j.i++
	return j.jobs[i], nil
}

// Jobs creates a JobsIter from a slice of Job.
func Jobs(js []Job) JobsIter {
	return &jobs{jobs: js}
}

// Results collects all the results from ResultsIter into the slice of Result.
func Results(it ResultsIter) (res []Result, err error) {
	for {
		var r Result
		r.Value, r.Error = it.Next()
		if r.Error == iterator.Done {
			if e, ok := r.Value.(error); ok {
				err = e
			}
			break
		}
		res = append(res, r)
	}
	return
}

// MapSlice is a convenience method around Map. It runs a slice of jobs in
// parallel, waits for them to finish, and returns the results as a slice.
func MapSlice(ctx context.Context, workers int, jobs []Job) []Result {
	// Jobs() never returns non-nil error, it's safe to ignore it.
	res, _ := Results(Map(ctx, workers, Jobs(jobs)))
	return res
}

// Failed returns the sub-slice of only failed results, whose error is not nil.
// Checking if len(Failed(results)) == 0 is a convenient way to verify that all
// jobs succeeded.
func Failed(res []Result) (failed []Result) {
	for _, r := range res {
		if r.Error != nil {
			failed = append(failed, r)
		}
	}
	return
}
