package http

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/egtann/sjs"
	"github.com/justinas/alice"
	"github.com/pkg/errors"
)

type Service struct {
	Mux *http.ServeMux

	db      sjs.DataStorage
	workers *sjs.WorkerMap
	sjsURL  string
	apiKey  string
	errCh   chan error // Nil unless *service.Err() is called
}

// NewService prepares the endpoints and starts the jobs. The error channel is
// optional; if the channel is not nil, the server will send errors encountered
// when running jobs. Jobs are distributed to workers round-robin.
func NewService(
	db sjs.DataStorage,
	apiKey string,
	version []byte,
) (*Service, error) {
	srv := &Service{
		db:      db,
		apiKey:  apiKey,
		workers: sjs.NewWorkerMap(),
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
	mux.HandleFunc("/version", func(w http.ResponseWriter, r *http.Request) {
		w.Write(version)
	})
	mux.Handle("/jobs", chain.Then(http.HandlerFunc(srv.handleJobs)))
	mux.Handle("/servers", chain.Then(http.HandlerFunc(srv.handleServices)))
	srv.Mux = mux

	// Fetch and start running active jobs
	jobs, err := db.GetActiveJobs(context.Background())
	if err != nil {
		return nil, errors.Wrap(err, "get active jobs")
	}
	for _, job := range jobs {
		sjs.Schedule(db, srv.workers, job, srv.errCh)
	}

	// Remove workers from rotation when they stop sending heartbeats.
	srv.workers.PurgeWorkersEvery(30 * time.Second)
	return srv, nil
}

// Err is a convenience wrapper for handling errors.
func (s *Service) Err() <-chan error {
	if s.errCh == nil {
		s.errCh = make(chan error, 1)
	}
	return s.errCh
}

func (s *Service) handleJobs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		s.handleGetJobs(w, r)
	case "POST":
		s.postJob(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *Service) handleServices(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		s.addService(w, r)
	default:
		http.NotFound(w, r)
	}
}

// addService notifies the job service of a new endpoint and its capabilities.
func (s *Service) addService(w http.ResponseWriter, r *http.Request) {
	worker := &sjs.Worker{}
	if err := json.NewDecoder(r.Body).Decode(worker); err != nil {
		s := fmt.Sprintf("decode json: %s", err.Error())
		http.Error(w, s, http.StatusBadRequest)
		return
	}
	ul, err := url.Parse(worker.NotifyURL)
	if err != nil {
		s := fmt.Sprintf("invalid NotifyURL: %s", err.Error())
		http.Error(w, s, http.StatusBadRequest)
		return
	}
	if ul.Scheme == "" || ul.Host == "" || ul.Path == "" {
		s := fmt.Sprintf("invalid NotifyURL: %s", err.Error())
		http.Error(w, s, http.StatusBadRequest)
		return
	}
	if len(worker.Capabilities) == 0 {
		s := "worker must have capabilities"
		http.Error(w, s, http.StatusBadRequest)
		return
	}
	s.workers.AddWorker(worker)
	w.WriteHeader(http.StatusOK)
}

func (s *Service) handleGetJobs(w http.ResponseWriter, r *http.Request) {
	var head string
	head, r.URL.Path = shiftPath(r.URL.Path) // jobs
	if r.URL.Path == "/" {
		s.getJobs(w, r)
		return
	}
	head, r.URL.Path = shiftPath(r.URL.Path) // id
	jobID, err := strconv.Atoi(head)
	if err != nil {
		s := fmt.Sprintf("parse job id: %s", err.Error())
		http.Error(w, s, http.StatusBadRequest)
		return
	}
	s.getJob(w, r, jobID)
}

func (s *Service) getJobs(w http.ResponseWriter, r *http.Request) {
	// TODO - do we need this?
}

func (s *Service) getJob(w http.ResponseWriter, r *http.Request, id int) {
	ctx := r.Context()
	job, err := s.db.GetJobForID(ctx, id)
	if sjs.IsMissing(err) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		s := fmt.Sprintf("get job for id: %s", err.Error())
		http.Error(w, s, http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(job)
}

func (s *Service) postJob(w http.ResponseWriter, r *http.Request) {
	job := &sjs.Job{}
	err := json.NewDecoder(r.Body).Decode(job)
	if err != nil {
		s := fmt.Sprintf("decode job: %s", err.Error())
		http.Error(w, s, http.StatusBadRequest)
		return
	}
	if err = job.Valid(); err != nil {
		s := fmt.Sprintf("invalid job: %s", err.Error())
		http.Error(w, s, http.StatusBadRequest)
		return
	}
	job.ID, err = s.db.GetOrCreateJob(r.Context(), job)
	if err != nil {
		s := fmt.Sprintf("create job: %s", err.Error())
		http.Error(w, s, http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusCreated)
	resp := struct{ ID int }{ID: job.ID}
	json.NewEncoder(w).Encode(resp)
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
