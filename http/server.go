package http

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/egtann/sjs"
	"github.com/justinas/alice"
	"github.com/pkg/errors"
)

type Server struct {
	mux     *http.ServeMux
	db      sjs.DataStorage
	log     sjs.Logger
	client  *http.Client
	apiKey  string
	workers sjs.WorkerMap
}

// NewServer prepares the endpoints and starts the jobs. The error channel is
// optional; if the channel is not nil, the server will send errors encountered
// when running jobs. Jobs are distributed to workers round-robin.
func NewServer(
	log sjs.Logger,
	db sjs.DataStorage,
	apiKey string,
	version []byte,
	errCh chan<- error,
) (*Server, error) {
	srv := &Service{
		log:     log,
		db:      db,
		apiKey:  apiKey,
		client:  &http.Client{Timeout: 10 * time.Second},
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
		log.Printf("health checked\n")
		w.Write([]byte("OK"))
	})
	mux.HandleFunc("/version", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("version checked\n")
		w.Write(version)
	})
	mux.Handle("/jobs", chain.Then(http.HandlerFunc(srv.handleJobs)))
	mux.Handle("/servers", chain.Then(http.HandlerFunc(srv.handleServers)))
	srv.mux = mux

	// Fetch and start running active jobs
	jobs, err := db.GetActiveJobs(context.Background())
	if err != nil {
		return nil, errors.Wrap(err, "get active jobs")
	}
	for _, job := range jobs {
		sjs.Schedule(job)
	}
	return srv, nil
}

func (s *Server) handleJobs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		s.handleGetJobs(w, r)
	case "POST":
		s.postJob(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) handleServers(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		s.addServer(w, r)
	default:
		http.NotFound(w, r)
	}
}

// addServer notifies the job service of a new endpoint and its capabilities.
func (s *Server) addServer(w http.ResponseWriter, r *http.Request) {
	data := sjs.Worker{}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		err = errors.Wrap(err, "decode json")
		http.Error(w, err, http.StatusBadRequest)
		return
	}
	ul, err := url.Parse(w.NotifyURL)
	if err != nil {
		err = errors.Wrap(err, "invalid NotifyURL")
		http.Error(w, err, http.StatusBadRequest)
		return
	}
	if ul.Scheme == "" || ul.Host == "" || ul.Path == "" {
		err = errors.New("invalid NotifyURL")
		http.Error(w, err, http.StatusBadRequest)
		return
	}
	if len(w.Capabilities) == 0 {
		err = errors.New("worker must have capabilities")
		http.Error(w, err, http.StatusBadRequest)
		return
	}
	s.workers.AddWorker(w)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleGetJobs(w http.ResponseWriter, r *http.Request) {
	var head string
	head, r.URL.Path = shiftPath(r.URL.Path) // jobs
	if r.URL.Path == "/" {
		s.getJobs(w, r)
		return
	}
	head, r.URL.Path = shiftPath(r.URL.Path) // id
	jobID, err := strconv.Atoi(head)
	if err != nil {
		err = errors.Wrap(err, "parse job id")
		http.Error(w, err, http.StatusBadRequest)
		return
	}
	s.getJobForID(w, r, jobID)
}

func (s *Server) getJobs(w http.ResponseWriter, r *http.Request) {
	// TODO - do we need this?
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request, id int) {
	ctx := r.Context()
	job, err := s.db.GetJobForID(ctx, id)
	if sjs.IsMissing(err) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		err = errors.Wrap(err, "get job for id")
		http.Error(w, err, http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(job)
}

func (s *Server) postJob(w http.ResponseWriter, r *http.Request) {
	data := struct {
		Name              string
		RunEveryInSeconds int
		TimeoutInSeconds  *int
	}{}
	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		err = errors.Wrap(err, "decode job data")
		http.Error(w, err, http.StatusBadRequest)
		return
	}
	job := &Job{
		Name:             data.Name,
		RunEvery:         data.RunEveryInSeconds * time.Duration,
		TimeoutInSeconds: data.TimeoutInSeconds * time.Duration,
	}
	if err = job.Valid(); err != nil {
		err = errors.Wrap(err, "invalid job")
		http.Error(w, err, http.StatusBadRequest)
		return
	}
	job.ID, err = s.db.CreateJob(r.Context(), job)
	if err != nil {
		err = errors.Wrap(err, "create job")
		http.Error(w, err, http.StatusBadRequest)
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
