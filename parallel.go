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

// Job is a unit of work to be done, which returns a result.
type Job[T any] func() T

type doneErr struct{}

func (*doneErr) Error() string {
	return "parallel.Done: no more items in iterator"
}

// Done is an error value indicating that an iterator has no more items.
var Done error = &doneErr{}

// JobsIter is a generator of work units for executing in Map. The iterator is
// inspired by the Google Cloud iterator guidelines: it either returns a work
// unit as Job instance, or terminates with a non-nil error. When the error is
// Done, it's the end of the sequence (and not really an error).
type JobsIter[T any] interface {
	Next() (Job[T], error)
}

// ResultsIter is an iterator over jobs results merged from parallel runs. Like
// JobsIter, it terminates normally with error Done, but may also terminate with
// a different error, e.g. due to JobsIter's error.
type ResultsIter[T any] interface {
	Next() (T, error)
}

type mapIter[T any] struct {
	ctx     context.Context // potentially cancelable context
	workers int             // maximum number of parallel jobs allowed
	jobs    int             // number of jobs currently running
	it      JobsIter[T]     // job iterator
	resCh   chan T          // workers send their results to this channel
	done    error           // non-nil error returned by the JobsIter
	mux     sync.Mutex      // to make Next() go routine safe
}

var _ ResultsIter[int] = &mapIter[int]{}

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
// Example usage:
//
//	m := Map(context.Background(), 2, jobsIter)
//	for {
//	  v, err := m.Next()
//	  if err == Done {
//	    break
//	  }
//	  // Process v and err.
//	}
func Map[T any](ctx context.Context, workers int, it JobsIter[T]) ResultsIter[T] {
	if isSerialized(ctx) {
		workers = 1
	}
	return &mapIter[T]{
		ctx:     ctx,
		workers: workers,
		resCh:   make(chan T),
		it:      it,
	}
}

// startJobs starts as many jobs as possible given the number of workers.
func (m *mapIter[T]) startJobs() {
	if m.done != nil {
		return
	}
	for ; m.workers <= 0 || m.jobs < m.workers; m.jobs++ {
		select {
		case <-m.ctx.Done():
			m.done = Done
			return
		default:
		}
		j, err := m.it.Next()
		if err != nil {
			m.done = err
			return
		}
		go func() { m.resCh <- j() }()
	}
}

// Next implements ResultsIter. It runs jobs in parallel up to the number of
// workers, blocks till at least one finishes (if any), and returns its result.
// When no more jobs are left, return Done error. Go routine safe.
func (m *mapIter[T]) Next() (T, error) {
	m.mux.Lock()
	defer m.mux.Unlock()

	m.startJobs()
	if m.jobs == 0 {
		if m.done == nil {
			m.done = Done
		}
		var zero T
		return zero, m.done
	}
	r := <-m.resCh
	m.jobs--
	m.startJobs()
	return r, nil
}

// jobs is an implementation of JobsIter from a slice of Job.
type jobs[T any] struct {
	jobs []Job[T]
	i    int
}

var _ JobsIter[int] = &jobs[int]{}

func (j *jobs[T]) Next() (Job[T], error) {
	if j.i >= len(j.jobs) {
		return nil, Done
	}
	i := j.i
	j.i++
	return j.jobs[i], nil
}

// Jobs creates a JobsIter from a slice of Job.
func Jobs[T any](js []Job[T]) JobsIter[T] {
	return &jobs[T]{jobs: js}
}

// Results collects all the results from ResultsIter into a slice.
func Results[T any](it ResultsIter[T]) ([]T, error) {
	res := []T{}
	for {
		r, err := it.Next()
		if err != nil {
			if err == Done {
				err = nil
			}
			return res, err
		}
		res = append(res, r)
	}
}

// MapSlice is a convenience method around Map. It runs a slice of jobs in
// parallel, waits for them to finish, and returns the results in a slice.
func MapSlice[T any](ctx context.Context, workers int, jobs []Job[T]) []T {
	// Jobs() never returns non-nil error, it's safe to ignore it.
	res, _ := Results(Map(ctx, workers, Jobs(jobs)))
	return res
}
