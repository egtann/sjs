package sjs

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
)

type Job struct {
	ID        int
	CreatedAt time.Time

	// Name describes the type of job, for instance, "calculateInvoice" or
	// "sendInvoice". Workers will subscribe to jobs from that server.
	Name JobName

	// LastRun indicates when the job last started. If a job has never been
	// run, this is nil.
	LastRun *time.Time

	// RunEveryInSeconds describes the interval on which to run the job.
	RunEveryInSeconds int

	// TimeoutInSeconds is the max length of time that a specific job's
	// execution is allowed before it's canceled. If nil, the job may run
	// forever.
	TimeoutInSeconds *int

	// JobStatus indicates whether a job is complete, paused, or running.
	JobStatus JobStatus

	// PayloadData included every time sjs notifies a worker.
	PayloadData []byte
}

// JobResult represents the result of a particular job. Any job will have 1 or
// more JobResults from prior runs.
type JobResult struct {
	JobID     int
	Succeeded bool
	StartedAt time.Time
	EndedAt   time.Time

	// ErrMessage is nil if the job succeeded.
	ErrMessage *string
}

type JobName string

type JobStatus string

const (
	JobStatusComplete = "complete"
	JobStatusPaused   = "paused"
	JobStatusRunning  = "running"
)

// Valid reports whether a job is valid or not. If invalid, this reports an
// error describing the validation issue.
func (j *Job) Valid() error {
	if j == nil {
		return errors.New("Job cannot be nil")
	}
	if j.Name == "" {
		return errors.New("Name cannot be empty")
	}
	if j.RunEveryInSeconds < 1 {
		return errors.New("RunEveryInSeconds must be >= 1 second")
	}
	return nil
}

// Schedule a job to run in a goroutine.
func Schedule(
	db DataStorage,
	workerMap *WorkerMap,
	j *Job,
	errCh chan<- error,
) {
	go schedule(db, workerMap, j, errCh)
}

func schedule(
	db DataStorage,
	workerMap *WorkerMap,
	j *Job,
	errCh chan<- error,
) {
	dur := time.Duration(j.RunEveryInSeconds) * time.Second
	for start := range time.Tick(dur) {
		ctx := context.Background()
		var cancel context.CancelFunc
		if j.TimeoutInSeconds != nil {
			t := time.Duration(*j.TimeoutInSeconds) * time.Second
			ctx, cancel = context.WithTimeout(ctx, t)
		}
		err := run(ctx, workerMap, j)
		result := &JobResult{
			JobID:     j.ID,
			Succeeded: err == nil,
			StartedAt: start,
			EndedAt:   time.Now(),
		}
		if err != nil {
			errCh <- errors.Wrap(err, "run")

			// Update our JobResult
			errMsg := err.Error()
			result.ErrMessage = &errMsg

			// Don't return or continue in this error handling. We
			// want to record the failed job result below and
			// cancel the context to free up resources
		}
		if err = db.CreateJobResult(ctx, result); err != nil {
			errCh <- errors.Wrap(err, "create job result")
		}
		if j.TimeoutInSeconds != nil {
			cancel()
		}
	}
}

// run a job. If no workers are available with that capability, then report an
// error.
func run(ctx context.Context, m *WorkerMap, j *Job) error {
	worker := m.GetWorkerForJobName(j.Name)
	if worker == nil {
		return fmt.Errorf("no workers capable of %s", j.Name)
	}
	err := worker.Run(ctx, j)
	return errors.Wrap(err, "run job")
}
