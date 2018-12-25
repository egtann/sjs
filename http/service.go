package http

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/egtann/sjs"
	"github.com/justinas/alice"
	"github.com/pkg/errors"
)

type Service struct {
	Mux *http.ServeMux

	workerData *sjs.WorkerMap
	log        *sjs.OptLogger
	sjsURL     string
	apiKey     string
	errCh      *sjs.OptErr
}

// NewService prepares the endpoints and starts the jobs. The error channel is
// optional; if the channel is not nil, the server will send errors encountered
// when running jobs. Jobs are distributed to workers round-robin.
func NewService(apiKey string) (*Service, error) {
	srv := &Service{
		apiKey:     apiKey,
		workerData: sjs.NewWorkerMap(),
		errCh:      &sjs.OptErr{},
	}

	// Assemble our mux and middleware
	chain := alice.New().Append(
		removeTrailingSlash,
		setJSONContentType,
		srv.isLoggedIn,
	)
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("OK"))
	})
	mux.Handle("/workers", chain.Then(http.HandlerFunc(srv.handleWorkers)))
	srv.Mux = mux

	// Remove workers from rotation when they stop sending heartbeats.
	srv.workerData.PurgeWorkersEvery(15 * time.Second)
	return srv, nil
}

// WithVersion adds a "/version" endpoint to track the version of the server
// for automated deployment tooling. This does not create a new Service or mux;
// WithVersion has side-effects.
func (s *Service) WithVersion(version []byte) *Service {
	s.Mux.HandleFunc("/version", func(w http.ResponseWriter, r *http.Request) {
		s.log.Printf("GET /version")
		w.Write(version)
	})
	return s
}

func (s *Service) WithLogger(log sjs.Logger) *Service {
	s.log = &sjs.OptLogger{Log: log}
	return s
}

// Err is a convenience wrapper for handling errors.
func (s *Service) Err() <-chan error {
	s.errCh.RLock()

	if s.errCh.C == nil {
		s.errCh.RUnlock()
		s.errCh.Lock()
		s.errCh.C = make(chan error, 1)
		s.errCh.Unlock()
	} else {
		s.errCh.RUnlock()
	}
	return s.errCh.C
}

func (s *Service) handleWorkers(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		s.getWorkers(w, r)
	case "POST":
		s.addWorker(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *Service) getWorkers(w http.ResponseWriter, r *http.Request) {
	byt, err := json.Marshal(s.workerData.Workers())
	if err != nil {
		err = errors.Wrap(err, "marshal workers")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write(byt)
}

// addWorker notifies the job service of a new worker and its capabilities.
// This is the heartbeat URL. Combining the two prevents us from needing to
// maintain worker state. If this service goes down and is restarted, workers
// are added again in seconds automatically due to this heartbeat.
func (s *Service) addWorker(w http.ResponseWriter, r *http.Request) {
	heartbeat := sjs.Heartbeat{}
	if err := json.NewDecoder(r.Body).Decode(&heartbeat); err != nil {
		msg := fmt.Sprintf("decode json: %s", err.Error())
		s.log.Printf("bad heartbeat: %s", msg)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}
	worker := s.workerData.GetOrCreateWorkerForNotifyURL(heartbeat.NotifyURL)
	_, err := url.Parse(worker.NotifyURL)
	if err != nil {
		s := fmt.Sprintf("invalid NotifyURL: %s", err.Error())
		http.Error(w, s, http.StatusBadRequest)
		return
	}
	for _, jd := range heartbeat.Jobs {
		job, err := sjs.JobFromData(jd)
		if err != nil {
			err = errors.Wrap(err, "job from data")
			http.Error(w, err.Error(), http.StatusBadRequest)
			s.log.Printf("bad heartbeat: bad job: %s", err.Error())
			return
		}
		worker.Jobs = append(worker.Jobs, job)
	}
	if len(worker.Jobs) == 0 {
		msg := "worker must have jobs. call WithJobs() on client."
		s.log.Printf("bad heartbeat: no jobs: %+v", worker)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}
	worker.APIKey = s.apiKey
	s.workerData.AddWorker(r.Context(), s.log, worker, s.errCh)
	w.WriteHeader(http.StatusOK)
}

func setJSONContentType(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		next.ServeHTTP(w, r)
	})
}

func removeTrailingSlash(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.URL.Path = strings.TrimSuffix(r.URL.Path, "/")
		next.ServeHTTP(w, r)
	})
}

func (srv *Service) isLoggedIn(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := []byte(r.Header.Get("X-API-Key"))
		result := subtle.ConstantTimeCompare([]byte(srv.apiKey), key)
		if result != 1 {
			http.NotFound(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// shiftPath splits off the first component of p, which will be cleaned of
// relative components before processing. head will never contain a slash and
// tail will always be a rooted path without trailing slash.
//
// From: https://blog.merovius.de/2017/06/18/how-not-to-use-an-http-router.html
func shiftPath(p string) (head, tail string) {
	p = path.Clean("/" + p)
	i := strings.Index(p[1:], "/") + 1
	if i <= 0 {
		return p[1:], "/"
	}
	return p[1:i], p[i:]
}
