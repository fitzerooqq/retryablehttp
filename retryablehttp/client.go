package retryablehttp

import (
	"io"
	"math/rand"
	"net/http"
	"time"
)

const DefaultRetryCount = 3

type Client struct {
	HTTPClient  *http.Client
	RetryCount  int
	BackoffBase time.Duration
}

func NewClient() *Client {
	return &Client{
		HTTPClient:  http.DefaultClient,
		RetryCount:  DefaultRetryCount,
		BackoffBase: 500 * time.Millisecond,
	}
}

func (c *Client) Do(req *http.Request) (*http.Response, error) {
	if c.HTTPClient == nil {
		c.HTTPClient = http.DefaultClient
	}
	retries := c.RetryCount
	if retries <= 0 {
		retries = DefaultRetryCount
	}

	var resp *http.Response
	var err error

	for attempt := 0; attempt <= retries; attempt++ {
		resp, err = c.HTTPClient.Do(req)
		if err != nil {
			if attempt < retries {
				time.Sleep(c.backoff(attempt))
				continue
			}
			return nil, err
		}

		if resp.StatusCode < 500 {
			return resp, nil
		}

		if attempt < retries {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			time.Sleep(c.backoff(attempt))
			continue
		}

		return resp, nil
	}

	return resp, err
}

func (c *Client) backoff(attempt int) time.Duration {
	base := c.BackoffBase
	if base <= 0 {
		base = 500 * time.Millisecond
	}
	delay := base * (1 << attempt)
	jitter := time.Duration(rand.Int63n(int64(base / 2)))
	return delay + jitter
}
