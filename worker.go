package sjs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type Worker struct {
	NotifyURL     string
	Jobs          []*Job
	APIKey        string
	lastHeartbeat time.Time
}

// WorkerMap maps job names to workers. It is thread-safe.
type WorkerMap struct {
	mu sync.RWMutex

	// data maps jobs to groups of workers performing that job
	data map[JobName]*workerGroup

	// set maps workers to NotifyURLs, ensuring we don't create duplicate
	// workers for any URL
	set map[string]*Worker
}

// Workers returns a slice of all workers active for the job server.
func (m *WorkerMap) Workers() []*Worker {
	workers := []*Worker{}
	for _, w := range m.set {
		workers = append(workers, w)
	}
	return workers
}

type workerGroup struct {
	workers []*Worker
	job     *Job
	ticker  *time.Ticker
	doneCh  chan bool

	// modIdx tracks the last index used in the worker slice. This is
	// incremented and never decremented, so use modulo to find the current
	// index.
	modIdx int
}

func NewWorkerMap() *WorkerMap {
	return &WorkerMap{
		data: map[JobName]*workerGroup{},
		set:  map[string]*Worker{},
	}
}

func (m *WorkerMap) AddWorker(
	ctx context.Context,
	lg *OptLogger,
	w *Worker,
	errCh *OptErr,
) {
	m.mu.Lock()
	defer m.mu.Unlock()

	w.lastHeartbeat = time.Now()
	if _, exist := m.set[w.NotifyURL]; exist {
		// We already have this worker. Nothing else to do
		return
	}
	m.set[w.NotifyURL] = w
	for _, j := range w.Jobs {
		if _, exist := m.data[j.Name]; exist {
			// There's nothing else we need to do. A ticker is
			// already running.
			return
		}
		wg := newWorkerGroup(j)
		wg.workers = append(wg.workers, w)
		m.data[j.Name] = wg
		Schedule(ctx, m, j, errCh)
		lg.Printf("added job %s (run every %d %s)", j.Name, j.RunEvery, j.RunEveryPeriod)
	}
}

func (m *WorkerMap) GetWorkerForJobName(name JobName) *Worker {
	m.mu.Lock()
	defer m.mu.Unlock()

	wg, ok := m.data[name]
	if !ok || len(wg.workers) == 0 {
		return nil
	}
	wg.modIdx++
	return wg.workers[wg.modIdx%len(wg.workers)]
}

// GetOrCreateWorkerForNotifyURL does not add the worker to the WorkerMap. That
// happens when the worker is fully assembled by the calling function. This is
// threadsafe and never returns nil.
func (m *WorkerMap) GetOrCreateWorkerForNotifyURL(ul string) *Worker {
	m.mu.RLock()
	defer m.mu.RUnlock()

	worker, exist := m.set[ul]
	if exist {
		return worker
	}
	worker = &Worker{NotifyURL: ul}
	return worker
}

func (m *WorkerMap) PurgeWorkersEvery(dur time.Duration) {
	go func() {
		for t := range time.Tick(dur) {
			m.purgeWorkers(t)
		}
	}()
}

func (m *WorkerMap) purgeWorkers(t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Find orphan candidates (jobs that may have no more workers) and a
	// new list of purged workers.
	newWorkers := []*Worker{}
	orphanCandidates := map[JobName]*Job{}
	for k := range m.set {
		worker := m.set[k]
		if worker.lastHeartbeat.Add(30 * time.Second).After(t) {
			newWorkers = append(newWorkers, worker)
			continue
		}
		for _, j := range worker.Jobs {
			orphanCandidates[j.Name] = j
		}
	}

	// Assemble our new worker groups
	set := map[string]*Worker{}
	workerData := map[JobName]*workerGroup{}
	clears := map[JobName]struct{}{}
	for _, w := range newWorkers {
		set[w.NotifyURL] = w
		for _, j := range w.Jobs {
			// We want to re-use our existing workgroup ticker and
			// done channel, but clear out workers. There are some
			// conditions in which workerGroup will be nil.
			wg := m.data[j.Name]
			if wg == nil {
				continue
			}
			if _, ok := clears[j.Name]; !ok {
				wg.workers = []*Worker{}
				clears[j.Name] = struct{}{}
			}
			wg.workers = append(wg.workers, w)
			workerData[j.Name] = wg
		}
	}

	// Halt ticking and free up resources from orphaned jobs.
	for jobName := range orphanCandidates {
		if _, exist := workerData[jobName]; !exist {
			m.data[jobName].doneCh <- true
		}
	}
	m.data = workerData
	m.set = set
}

func (w *Worker) Run(ctx context.Context, j *Job) error {
	byt, err := json.Marshal(j)
	if err != nil {
		return errors.Wrap(err, "marshal job")
	}
	req, err := http.NewRequest("POST", w.NotifyURL, bytes.NewReader(byt))
	if err != nil {
		return errors.Wrap(err, "new request")
	}
	req = req.WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", w.APIKey)
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return errors.Wrap(err, "post")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("worker responded %d: %s", resp.StatusCode,
			w.NotifyURL)
	}
	return nil
}

func newWorkerGroup(j *Job) *workerGroup {
	return &workerGroup{
		job:    j,
		ticker: time.NewTicker(jobDuration(j)),
		doneCh: make(chan bool),
	}
}
