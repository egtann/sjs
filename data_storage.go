package sjs

import "context"

type DataStorage interface {
	// GetJobForID should report a Missing error if a job by that ID does
	// not exist.
	GetJobForID(context.Context, int) (*Job, error)

	// GetOrCreateJob reports the created ID and an error, if any.
	GetOrCreateJob(context.Context, *Job) (int, error)

	// GetActiveJobs reports all incomplete, non-paused jobs.
	GetActiveJobs(context.Context) ([]*Job, error)

	// CreateJobResult tracks successes and failures.
	CreateJobResult(context.Context, *JobResult) error

	UpdateJob(context.Context, *Job) error
}
