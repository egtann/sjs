package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/egtann/sjs"
	"github.com/pkg/errors"
)

// Client communicates with SJS server through client.Notify, which continually
// sends heartbeats.
type Client struct {
	Capabilities []sjs.JobName

	client *http.Client
	url    string
	apiKey string

	// errCh is nil until Err() is called.
	errCh chan error
}

func NewClient(
	sjsURL, apiKey string,
	capabilities []sjs.JobName,
) *Client {
	return &Client{
		Capabilities: capabilities,
		url:          sjsURL + "/servers",
		apiKey:       apiKey,
		client:       &http.Client{Timeout: 10 * time.Second},
	}
}

// Notify sjs of some calling server's existence and capabilities. This
// function is called by the client and automatically performs a heartbeat with
// the job server.
func (c *Client) Notify() {
	go func() {
		err := c.notify()
		if err != nil && c.errCh != nil {
			c.errCh <- errors.Wrap(err, "notify")
		}
		for range time.Tick(15 * time.Second) {
			err = c.notify()
			if err != nil && c.errCh != nil {
				c.errCh <- errors.Wrap(err, "notify")
			}
		}
	}()
}

func (c *Client) notify() error {
	worker := &sjs.Worker{
		NotifyURL:    c.url,
		Capabilities: c.Capabilities,
	}
	body, err := json.Marshal(worker)
	if err != nil && c.errCh != nil {
		return errors.Wrap(err, "marshal worker")
	}
	req, err := http.NewRequest("POST", c.url, bytes.NewReader(body))
	if err != nil {
		return errors.Wrap(err, "new request")
	}
	req.Header.Set("X-API-Key", c.apiKey)
	resp, err := c.client.Do(req)
	if err != nil {
		return errors.Wrap(err, "do")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s responded with %d", c.url, resp.StatusCode)
	}
	return nil
}

// Err is a convenience wrapper for handling errors.
func (c *Client) Err() <-chan error {
	if c.errCh == nil {
		c.errCh = make(chan error, 1)
	}
	return c.errCh
}
