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
	Capabilities  []JobName
	lastHeartbeat time.Time
}

// WorkerMap maps job names to workers. It is concurrent-safe.
type WorkerMap struct {
	workers []*Worker
	data    map[JobName]*workerGroup
	mu      sync.Mutex
}

type workerGroup struct {
	workers []*Worker

	// modIdx tracks the last index used in the worker slice. This is
	// incremented and never decremented, so use modulo to find the current
	// index.
	modIdx int
}

func NewWorkerMap() *WorkerMap {
	return &WorkerMap{data: map[JobName]*workerGroup{}}
}

// addWorkerToMap in a non-concurrent way. It's assumed that the caller has
// guarded access.
func (m *WorkerMap) addWorkerToMap(w *Worker) {
	for _, jobName := range w.Capabilities {
		wg, ok := m.data[jobName]
		if !ok {
			wg = &workerGroup{}
			m.data[jobName] = wg
		}
		wg.workers = append(wg.workers, w)
	}
}

func (m *WorkerMap) AddWorker(w *Worker) {
	m.mu.Lock()
	defer m.mu.Unlock()

	w.lastHeartbeat = time.Now()
	m.workers = append(m.workers, w)
	m.addWorkerToMap(w)
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

func (m *WorkerMap) PurgeWorkersEvery(dur time.Duration) {
	go func() {
		for t := range time.Tick(dur) {
			m.purgeWorkers(t)
		}
	}()
}

func (m *WorkerMap) purgeWorkers(t time.Time) {
	newWorkers := []*Worker{}
	for _, worker := range m.workers {
		if worker.lastHeartbeat.Add(30 * time.Second).After(t) {
			newWorkers = append(newWorkers, worker)
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	m.workers = newWorkers
	m.data = map[JobName]*workerGroup{}
	for _, w := range m.workers {
		m.addWorkerToMap(w)
	}
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
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return errors.Wrap(err, "post")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("worker responded %d", resp.StatusCode)
	}
	return nil
}
