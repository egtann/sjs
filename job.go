package sjs

import (
	"context"
	"fmt"
	"strings"
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

	// RunEvery describes the interval on which to run the job.
	RunEvery int

	// RunEveryPeriod can be "second" or "day of month".
	RunEveryPeriod JobPeriod

	// TimeoutInSeconds is the max length of time that a specific job's
	// execution is allowed before it's canceled. If nil, the job may run
	// forever.
	TimeoutInSeconds *int

	// PayloadData included every time sjs notifies a worker.
	PayloadData []byte

	// JobStatus indicates whether the job is running or paused.
	JobStatus JobStatus
}

// JobPeriod determines how often the job should be run. Second indicates that
// the job should run every X seconds. Day of month indicates that the job
// should run on every X day of the month, such as the Jan 1st, Feb 1st, Mar
// 1st, etc.
type JobPeriod string

const (
	JobPeriodSecond     JobPeriod = "second"
	JobPeriodDayOfMonth           = "dayOfMonth"
)

// JobData is sent when registering worker capabilities. This enables the
// creation of jobs with that. Using a zero value for TimeoutInSeconds is
// treated as no timeout. When created, jobs default to running.
type JobData struct {
	Name             JobName
	RunEvery         int
	RunEveryPeriod   JobPeriod
	TimeoutInSeconds int
	JobStatus        JobStatus
	PayloadData      []byte
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
	JobStatusPaused  = "paused"
	JobStatusRunning = "running"
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
	if j.RunEvery < 1 {
		return errors.New("RunEvery must be >= 1 second")
	}
	switch j.JobStatus {
	case JobStatusRunning, JobStatusPaused:
		// Do nothing
	default:
		return fmt.Errorf("invalid job status: %s", j.JobStatus)
	}
	return nil
}

// Schedule a job to run.
func Schedule(
	ctx context.Context,
	db DataStorage,
	workerMap *WorkerMap,
	j *Job,
	errCh *OptErr,
) {
	// Since the job is new, we update the job in the database.
	err := db.UpdateJob(ctx, j)
	if err != nil {
		errCh.Send(errors.Wrap(err, "updating job"))
		// Keep going; we can still run the timer correctly even if
		// updating the job failed
	}

	// Now we have a timer that we need to listen to
	go func() {
		for {
			workerMap.mu.RLock()
			wg := workerMap.data[j.Name]
			workerMap.mu.RUnlock()

			select {
			case start := <-wg.ticker.C:
				jobTick(db, workerMap, j, start, errCh)
			case <-wg.doneCh:
				wg.ticker.Stop()
				return
			}
		}
	}()
}

func jobTick(
	db DataStorage,
	workerMap *WorkerMap,
	j *Job,
	start time.Time,
	errCh *OptErr,
) {
	monthly := j.RunEveryPeriod == JobPeriodDayOfMonth
	if monthly && start.Day() != j.RunEvery {
		return
	}
	err := scheduleJobWithTimeout(workerMap, j)
	if err != nil {
		errCh.Send(errors.Wrap(err, "schedule"))
		// Keep going; we want to record the job result.
	}
	err = recordJobResult(db, j, start, err)
	if err != nil {
		errCh.Send(errors.Wrap(err, "record"))
		return
	}
}

func recordJobResult(
	db DataStorage,
	j *Job,
	start time.Time,
	err error,
) error {
	result := &JobResult{
		JobID:     j.ID,
		Succeeded: err == nil,
		StartedAt: start,
		EndedAt:   time.Now(),
	}
	if err != nil {
		// Update our JobResult
		errMsg := err.Error()
		result.ErrMessage = &errMsg

		// Don't return in this error handling. We want to record the
		// failed job result below
	}
	if err = db.CreateJobResult(context.Background(), result); err != nil {
		return errors.Wrap(err, "create job result")
	}
	return nil
}

func scheduleJobWithTimeout(workerMap *WorkerMap, j *Job) error {
	ctx := context.Background()
	var cancel context.CancelFunc
	if j.TimeoutInSeconds == nil {
		ctx, cancel = context.WithCancel(ctx)
	} else {
		t := time.Duration(*j.TimeoutInSeconds) * time.Second
		ctx, cancel = context.WithTimeout(ctx, t)
	}
	defer cancel()
	err := run(ctx, workerMap, j)
	return errors.Wrap(err, "run")
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

// JobFromData converts JobData to a job and validates the job, reporting any
// errors.
func JobFromData(jd *JobData) (*Job, error) {
	jd.Name = JobName(strings.TrimSpace(string(jd.Name)))
	var timeoutSecs *int
	if jd.TimeoutInSeconds != 0 {
		timeoutSecs = &jd.TimeoutInSeconds
	}
	job := &Job{
		Name:             jd.Name,
		RunEvery:         jd.RunEvery,
		RunEveryPeriod:   jd.RunEveryPeriod,
		PayloadData:      jd.PayloadData,
		JobStatus:        JobStatusRunning,
		TimeoutInSeconds: timeoutSecs,
	}
	err := job.Valid()
	return job, errors.Wrapf(err, "invalid job %s", job.Name)
}

func jobDuration(j *Job) time.Duration {
	switch j.RunEveryPeriod {
	case JobPeriodSecond:
		return time.Duration(j.RunEvery) * time.Second
	case JobPeriodDayOfMonth:
		return 24 * time.Hour
	}
	s := fmt.Sprintf("unknown job RunEveryPeriod: %s", j.RunEveryPeriod)
	panic(s)
}
