# Parallel processing utilities

This package implements processing units of work (`Job`) in parallel using a
specified number of workers. In the most general case (`Map` and `NewMap`), the
user supplies an iterator of job units (`JobIter`), which are processed in
parallel and the results are returned as an iterator of the results
(`ResultsIter`).

For simpler situations when all jobs can be created ahead of time, use `Jobs` to
convert a slice of jobs into an iterator. If all the results can be collected
before further processing, use `MapSlice`.

## Installation

```
go get github.com/stockparfait/parallel
```

## Example usage

```go
package main

import (
	"context"
	"fmt"

	"github.com/stockparfait/parallel"
)

func job(i int) parallel.Job {
	return func() (interface{}, error) {
		return i + 1, nil
	}
}

func main() {
	ctx := context.Background()
	jobs := []parallel.Job{job(5), job(10)}
	for _, r := range parallel.MapSlice(ctx, 2, jobs) {
		if r.Error != nil {
			fmt.Printf("job failed: %s\n", r.Error.Error())
			return
		}
		fmt.Printf("result = %d\n", r.Value.(int))
	}
}
```

should print something like:

```
result = 6
result = 11
```

## Development

Clone and initialize the repository, run tests:

```sh
git clone git@github.com:stockparfait/parallel.git
cd parallel
make init
make test
```
