// Package poolmap plays fast and loose with types to provide a simple interface
// for parallel operations on order-agnostic slices.
package poolmap

import (
	"log"
	"time"

	"gopkg.in/cheggaaa/pb.v1"
)

// timeTrack is a timing util.
func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
}

// An Operation processes an element of the `from` argument to Map.
type Operation func(interface{}) (interface{}, error)

// Map uses LIM goroutines to run OP on every element in FROM. It returns the
// mapped outputs and errors.
//
// NOTE: Map will not maintain order because of parallelism unless LIM = 1.
func Map(from []interface{}, op Operation, lim int) ([]interface{}, []error) {
	defer timeTrack(time.Now(), "Slicemap time")
	jobs := make(chan interface{}, len(from))
	results := make(chan interface{}, len(from))
	errors := make(chan error, len(from))
	// Start workpool.
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
	bar := pb.StartNew(len(from))
	for i := 0; i < len(from); i++ {
		out[i], err[i] = <-results, <-errors
		if err[i] != nil {
			log.Print("ERROR:", err)
		}
		bar.Increment()
	}
	bar.FinishPrint("Done mapping.")
	return out, err
}

// MapNoLogging serves the same purpose as Map but does not log anything.
func MapNoLogging(from []interface{}, op Operation, lim int) ([]interface{}, []error) {
	jobs := make(chan interface{}, len(from))
	results := make(chan interface{}, len(from))
	errors := make(chan error, len(from))
	// Workpool
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
	for i := 0; i < len(from); i++ {
		out[i], err[i] = <-results, <-errors
	}
	return out, err
}
