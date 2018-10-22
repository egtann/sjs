package sjs

type DataStorage interface {
	// GetJobForID should report a Missing error if a job by that ID does
	// not exist.
	GetJobForID(context.Context, int) (*Job, error)

	// CreateJob reports the created ID and an error, if any.
	CreateJob(context.Context, *Job) (int, error)

	// GetActiveJobs reports all incomplete, non-paused jobs.
	GetActiveJobs(context.Context) ([]*Job, error)
}
