package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/egtann/sjs"
	"github.com/hashicorp/go-cleanhttp"
	"github.com/pkg/errors"
)

// Client communicates with SJS server through client.Notify, which continually
// sends heartbeats.
type Client struct {
	Jobs []*sjs.JobData

	client    *http.Client
	apiKey    string
	selfURL   string
	serverURL string
	host      string
	role      string
	errCh     *sjs.OptErr
}

func NewClient(selfURL, sjsURL, apiKey, host, role string) *Client {
	httpClient := cleanhttp.DefaultClient()
	httpClient.Timeout = 10 * time.Second
	return &Client{
		selfURL:   selfURL,
		serverURL: sjsURL,
		apiKey:    apiKey,
		client:    httpClient,
		errCh:     &sjs.OptErr{},
	}
}

// WithJobs appends jobs that can be performed by the client.
func (c *Client) WithJobs(jobData ...*sjs.JobData) *Client {
	// Typically Err() is called before this but on a
	// goroutine which often has not completed yet. Sleep
	// to give that a chance to run first
	time.Sleep(100 * time.Millisecond)

	for _, jd := range jobData {
		if _, err := sjs.JobFromData(jd); err != nil {
			c.errCh.Send(err)
			continue
		}
		c.Jobs = append(c.Jobs, jd)
	}
	return c
}

// Heartbeat notifies sjs of some calling server's existence and capabilities.
// This automatically performs a heartbeat on a regular interval with the job
// server.
func (c *Client) Heartbeat() {
	go func() {
		err := c.notify()
		if err != nil {
			c.errCh.Send(errors.Wrap(err, "notify"))
		}
		for range time.Tick(15 * time.Second) {
			err = c.notify()
			if err != nil {
				c.errCh.Send(errors.Wrap(err, "notify"))
			}
		}
	}()
}

func (c *Client) notify() error {
	heartbeat := &sjs.Heartbeat{
		NotifyURL: c.selfURL,
		Jobs:      c.Jobs,
	}
	body, err := json.Marshal(heartbeat)
	if err != nil {
		return errors.Wrap(err, "marshal worker")
	}
	rdr := bytes.NewReader(body)
	req, err := http.NewRequest("POST", c.serverURL+"/workers", rdr)
	if err != nil {
		return errors.Wrap(err, "new request")
	}
	req.Header.Set("X-API-Key", c.apiKey)
	req.Header.Set("X-Host", c.host)
	req.Header.Set("X-Role", c.role)
	resp, err := c.client.Do(req)
	if err != nil {
		return errors.Wrap(err, "do")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		byt, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return errors.Wrap(err, "read resp body")
		}
		return fmt.Errorf("%s responded with %d: %s", c.serverURL,
			resp.StatusCode, string(byt))
	}
	return nil
}

// Err is a convenience wrapper for handling errors.
func (c *Client) Err() <-chan error {
	c.errCh.RLock()
	if c.errCh.C == nil {
		c.errCh.RUnlock()
		c.errCh.Lock()
		c.errCh.C = make(chan error, 1)
		c.errCh.Unlock()
	} else {
		c.errCh.RUnlock()
	}
	return c.errCh.C
}
