// Package poolmap plays fast and loose with types to provide a simple interface
// for parallel operations on order-agnostic slices.
package poolmap

import (
	"io/ioutil"
	"log"
	"time"

	"gopkg.in/cheggaaa/pb.v1"
)

// An Operation processes an element of the `from` argument to Map.
type Operation func(interface{}) (interface{}, error)

// Map uses LIM goroutines to run OP on every element in FROM. It returns the
// mapped outputs and errors. If SILENT, Map does not log errors or progress.
//
// NOTE: Map will not maintain order because of parallelism unless LIM = 1.
func Map(from []interface{}, op Operation, lim int, silent bool) ([]interface{}, []error) {
	if silent {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}
	defer timeTrack(time.Now(), "Slicemap")

	// Initialize workpool.
	jobs := make(chan interface{}, len(from))
	results := make(chan interface{}, len(from))
	errors := make(chan error, len(from))
	for w := 0; w < lim; w++ {
		go func() {
			for j := range jobs {
				r, err := op(j)
				results <- r
				errors <- err
			}
		}()
	}

	// Feed jobs.
	for i := 0; i < len(from); i++ {
		jobs <- from[i]
	}
	close(jobs)

	// Pull values.
	out := make([]interface{}, len(from))
	err := make([]error, len(from))
	bar := pb.New(len(from))
	bar.NotPrint = silent
	bar.Start()
	for i := 0; i < len(from); i++ {
		out[i], err[i] = <-results, <-errors
		if err[i] != nil {
			log.Print("Mapping error: ", err[i])
		}
		bar.Increment()
	}
	bar.Finish()
	return out, err
}

// timeTrack is a timing util.
func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
}
