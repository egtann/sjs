package sjs

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
	RunEvery time.Duration

	// Timeout is the max length of time that a specific job's execution is
	// allowed before it's canceled. If nil, the job may run forever.
	Timeout *time.Duration

	Status   JobStatus
	RunCount int
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
		return false, errors.New("Job cannot be nil")
	}
	if j.Name == "" {
		return false, errors.New("Name cannot be empty")
	}
	if j.RunEvery < 1*time.Second {
		return false, errors.New("RunEvery must be >= 1 second")
	}
}

// Schedule a job to run in a goroutine.
func Schedule(workerMap map[WorkerCapability][]*Worker, j *Job) {
	go schedule(j)
}

func schedule(
	db DataStorage,
	workerMap map[WorkerCapability][]*Worker,
	j *Job,
) {
	for range time.Tick(j.RunEvery) {
		// Update job in data storage
		run(workerMap, j)
	}
}

// run a job. If no workers are available with that capability, then report an
// error.
func run(wm *workerMap, j *Job) error {
	worker := wm.GetWorkerForJobName(j.Name)
	if worker == nil {
		return fmt.Errorf("no workers capable of %s", j.Name)
	}
	worker := workers[rand.Intn(len(workers))]
}
