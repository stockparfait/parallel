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

package parallel

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"google.golang.org/api/iterator"

	. "github.com/smartystreets/goconvey/convey"
)

var mux sync.Mutex

type jobsIterErr struct {
	jobs []Job
	i    int
	err  error
}

func (j *jobsIterErr) Next() (Job, error) {
	i := j.i
	if i < len(j.jobs) {
		j.i++
		return j.jobs[i], nil
	}
	return nil, j.err
}

func jobsErr(jobs []Job, err error) JobsIter {
	if err == nil {
		err = iterator.Done
	}
	return &jobsIterErr{jobs: jobs, err: err}
}

func TestParallel(t *testing.T) {
	t.Parallel()
	Convey("Map works", t, func() {
		ctx := context.Background()
		var running, maxRunning int
		var sequence []int

		start := func(i int) {
			mux.Lock()
			sequence = append(sequence, i)
			running++
			if running > maxRunning {
				maxRunning = running
			}
			mux.Unlock()
		}

		end := func() {
			mux.Lock()
			running--
			mux.Unlock()
		}

		job := func(i int, err error) Job {
			return func() (interface{}, error) {
				start(i)
				time.Sleep(1 * time.Millisecond)
				end()
				return i, err
			}
		}

		jobs := func(n int) (jobs []Job) {
			for i := 0; i < n; i++ {
				jobs = append(jobs, job(i, nil))
			}
			return
		}

		expectedResults := func(n int) (res []Result) {
			for i := 0; i < n; i++ {
				res = append(res, Result{Value: i})
			}
			return
		}

		Convey("with limited workers", func() {
			res := MapSlice(ctx, 5, jobs(15))
			So(len(res), ShouldEqual, 15)
			So(len(Failed(res)), ShouldEqual, 0)
			So(len(sequence), ShouldEqual, 15)
			So(maxRunning, ShouldEqual, 5)
		})

		Convey("serialized", func() {
			res := MapSlice(TestSerialize(ctx), 5, jobs(15))
			So(res, ShouldResemble, expectedResults(15))
			So(maxRunning, ShouldEqual, 1)
			So(len(sequence), ShouldEqual, 15)
		})

		Convey("with a failing job", func() {
			err := fmt.Errorf("test error")
			jobs := []Job{job(0, nil), job(1, err)}
			res := MapSlice(ctx, 0, jobs)
			failed := Failed(res)
			So(len(failed), ShouldEqual, 1)
			So(failed[0].Error, ShouldEqual, err)
			So(len(sequence), ShouldEqual, 2)
			So(len(res), ShouldEqual, 2)
		})

		Convey("with no jobs", func() {
			res := MapSlice(ctx, 0, nil)
			So(len(res), ShouldEqual, 0)
		})

		Convey("canceling context stops enqueuing jobs", func() {
			cc, cancel := context.WithCancel(ctx)
			m := Map(cc, 3, Jobs(jobs(15)))
			_, err := m.Next()
			So(err, ShouldBeNil)
			cancel() // 3 more still in flight
			_, err = m.Next()
			So(err, ShouldBeNil)
			_, err = m.Next()
			So(err, ShouldBeNil)
			_, err = m.Next()
			So(err, ShouldBeNil)
			v, err := m.Next()
			So(err, ShouldEqual, iterator.Done)
			So(v, ShouldBeNil) // check for JobsIter error
			So(len(sequence), ShouldEqual, 4)
		})

		Convey("error from JobsIter propagates through Results", func() {
			testErr := fmt.Errorf("test error")
			res, err := Results(Map(ctx, 3, jobsErr(jobs(5), testErr)))
			So(len(res), ShouldEqual, 5)
			So(err, ShouldEqual, testErr)
		})
	})
}
