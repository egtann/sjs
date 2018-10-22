package sjs

type Worker struct {
	NotifyURL    string
	Capabilities []JobName
}

// WorkerMap maps job names to workers. It is concurrent-safe.
type WorkerMap struct {
	data map[JobName]workerGroup
	mu   sync.Mutex
}

type workerGroup struct {
	workers []*Worker

	// modIdx tracks the last index used in the worker slice. This is
	// incremented and never decremented, so use modulo to find the current
	// index.
	modIdx int
}

func NewWorkerMap() *WorkerMap {
	return &WorkerMap{data: map[JobName]workerGroup{}}
}

func (m *WorkerMap) AddWorker(w *Worker) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, jobName := range w.Capabilities {
		m.data[jobName] = append(m.data[jobData], w)
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
	return wg.workers[workers.modIdx%len(wg.workers)]
}
