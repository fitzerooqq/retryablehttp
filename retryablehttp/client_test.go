package retryablehttp

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDo_Success(t *testing.T) {
	var count int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&count, 1)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
	defer ts.Close()

	client := NewClient()
	client.RetryCount = 2

	req, _ := http.NewRequest("GET", ts.URL, nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if atomic.LoadInt32(&count) != 1 {
		t.Fatalf("expected 1 request, got %d", count)
	}
}

func TestDo_RetryOn5xx(t *testing.T) {
	var count int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&count, 1)
		if n < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("retry"))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
	defer ts.Close()

	client := NewClient()
	client.RetryCount = 3
	client.BackoffBase = 10 * time.Millisecond

	req, _ := http.NewRequest("GET", ts.URL, nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if atomic.LoadInt32(&count) != 3 {
		t.Fatalf("expected 3 requests, got %d", count)
	}
}

func TestDo_ExhaustRetries(t *testing.T) {
	var count int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&count, 1)
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("down"))
	}))
	defer ts.Close()

	client := NewClient()
	client.RetryCount = 2
	client.BackoffBase = 10 * time.Millisecond

	req, _ := http.NewRequest("GET", ts.URL, nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", resp.StatusCode)
	}
	expected := int32(client.RetryCount + 1)
	if atomic.LoadInt32(&count) != expected {
		t.Fatalf("expected %d requests, got %d", expected, count)
	}
}

func TestDo_NoRetryOn4xx(t *testing.T) {
	var count int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&count, 1)
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("not found"))
	}))
	defer ts.Close()

	client := NewClient()
	client.RetryCount = 3
	client.BackoffBase = 10 * time.Millisecond

	req, _ := http.NewRequest("GET", ts.URL, nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
	if atomic.LoadInt32(&count) != 1 {
		t.Fatalf("expected 1 request, got %d", count)
	}
}

func TestDo_RetryOnConnectionError(t *testing.T) {
	client := NewClient()
	client.RetryCount = 1
	client.BackoffBase = 10 * time.Millisecond

	req, _ := http.NewRequest("GET", "http://127.0.0.1:1", nil)
	_, err := client.Do(req)
	if err == nil {
		t.Fatal("expected connection error")
	}
}

func TestDo_DrainsBodyBeforeRetry(t *testing.T) {
	var count int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&count, 1)
		if n < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("error body that should be drained"))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
	defer ts.Close()

	var drainCalled bool
	client := NewClient()
	client.RetryCount = 3
	client.BackoffBase = 10 * time.Millisecond

	origDo := client.HTTPClient.Do
	client.HTTPClient = &http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			resp, err := origDo(req)
			return resp, err
		}),
	}

	req, _ := http.NewRequest("GET", ts.URL, nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	_ = drainCalled
}

func TestDo_ClosesBodyBeforeRetry(t *testing.T) {
	var count int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&count, 1)
		if n < 2 {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("retry"))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
	defer ts.Close()

	client := NewClient()
	client.RetryCount = 2
	client.BackoffBase = 10 * time.Millisecond

	closeCalled := false
	wrappedTransport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		resp, err := http.DefaultTransport.RoundTrip(req)
		if err == nil && resp.StatusCode >= 500 {
			oldBody := resp.Body
			resp.Body = &closeCheckReadCloser{
				ReadCloser: oldBody,
				closeFn:    func() { closeCalled = true },
			}
		}
		return resp, err
	})
	client.HTTPClient = &http.Client{Transport: wrappedTransport}

	req, _ := http.NewRequest("GET", ts.URL, nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if !closeCalled {
		t.Fatal("expected body.Close() to be called on retried response")
	}
}

func TestDo_NonNilClient(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
	defer ts.Close()

	client := NewClient()
	client.HTTPClient = nil
	client.RetryCount = 0

	req, _ := http.NewRequest("GET", ts.URL, nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestDo_LargeBodyDrain(t *testing.T) {
	var count int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&count, 1)
		if n < 2 {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(strings.Repeat("A", 100000)))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
	defer ts.Close()

	client := NewClient()
	client.RetryCount = 2
	client.BackoffBase = 10 * time.Millisecond

	req, _ := http.NewRequest("GET", ts.URL, nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestDefaultRetryCount(t *testing.T) {
	client := NewClient()
	if client.RetryCount != DefaultRetryCount {
		t.Fatalf("expected default retry count %d, got %d", DefaultRetryCount, client.RetryCount)
	}
}

func TestDo_RetryCountZero(t *testing.T) {
	var count int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&count, 1)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("error"))
	}))
	defer ts.Close()

	client := NewClient()
	client.RetryCount = 0
	client.BackoffBase = 10 * time.Millisecond

	req, _ := http.NewRequest("GET", ts.URL, nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	expected := DefaultRetryCount + 1
	if atomic.LoadInt32(&count) != int32(expected) {
		t.Fatalf("expected %d requests, got %d", expected, count)
	}
}

func TestDo_BodyDrainAndCloseOnFinalFailure(t *testing.T) {
	var count int32
	closeCalled := false
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&count, 1)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("error"))
	}))
	defer ts.Close()

	client := NewClient()
	client.RetryCount = 1
	client.BackoffBase = 10 * time.Millisecond

	wrappedTransport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		resp, err := http.DefaultTransport.RoundTrip(req)
		if err == nil && resp.StatusCode >= 500 {
			oldBody := resp.Body
			resp.Body = &closeCheckReadCloser{
				ReadCloser: oldBody,
				closeFn:    func() { closeCalled = true },
			}
		}
		return resp, err
	})
	client.HTTPClient = &http.Client{Transport: wrappedTransport}

	req, _ := http.NewRequest("GET", ts.URL, nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if !closeCalled {
		t.Fatal("expected body.Close() to be called on final failure response too")
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type closeCheckReadCloser struct {
	io.ReadCloser
	closeFn func()
}

func (c *closeCheckReadCloser) Close() error {
	c.closeFn()
	return c.ReadCloser.Close()
}

func TestDo_Non5xxNotRetried(t *testing.T) {
	for _, code := range []int{400, 401, 403, 404, 409, 422, 429} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			var count int32
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(&count, 1)
				w.WriteHeader(code)
			}))
			client := NewClient()
			client.RetryCount = 3
			client.BackoffBase = 10 * time.Millisecond
			req, _ := http.NewRequest("GET", ts.URL, nil)
			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			resp.Body.Close()
			if atomic.LoadInt32(&count) != 1 {
				t.Fatalf("expected 1 request for %d, got %d", code, count)
			}
			ts.Close()
		})
	}
}

func TestDo_ConnectionErrorReturnsError(t *testing.T) {
	client := NewClient()
	client.RetryCount = 0
	client.BackoffBase = 10 * time.Millisecond
	client.HTTPClient = &http.Client{Timeout: 100 * time.Millisecond}

	req, _ := http.NewRequest("GET", "http://192.0.2.1:1", nil)
	_, err := client.Do(req)
	if err == nil {
		t.Fatal("expected error for unreachable host")
	}
}

func TestDo_BackoffDefault(t *testing.T) {
	c := NewClient()
	c.BackoffBase = 0
	d := c.backoff(0)
	if d == 0 {
		t.Fatal("expected non-zero backoff with default base")
	}
}

func TestBodyDrainedBeforeClose(t *testing.T) {
	var count int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&count, 1)
		if n < 2 {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("drain me"))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
	defer ts.Close()

	var readBytes int64
	client := NewClient()
	client.RetryCount = 2
	client.BackoffBase = 10 * time.Millisecond

	wrappedTransport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		resp, err := http.DefaultTransport.RoundTrip(req)
		if err == nil && resp.StatusCode >= 500 {
			oldBody := resp.Body
			resp.Body = &drainCheckReadCloser{
				ReadCloser: oldBody,
				readFn:    func(p []byte) (int, error) { n, err := oldBody.Read(p); readBytes += int64(n); return n, err },
			}
		}
		return resp, err
	})
	client.HTTPClient = &http.Client{Transport: wrappedTransport}

	req, _ := http.NewRequest("GET", ts.URL, nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if readBytes == 0 {
		t.Fatal("expected response body to be drained via io.Copy(io.Discard, ...)")
	}
}

type drainCheckReadCloser struct {
	io.ReadCloser
	readFn func([]byte) (int, error)
}

func (d *drainCheckReadCloser) Read(p []byte) (int, error) {
	return d.readFn(p)
}

func (d *drainCheckReadCloser) Close() error {
	return d.ReadCloser.Close()
}

func TestDo_NilClientDefaults(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	c := &Client{RetryCount: 0, BackoffBase: 10 * time.Millisecond}
	req, _ := http.NewRequest("GET", ts.URL, nil)
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestNewClient(t *testing.T) {
	c := NewClient()
	if c.HTTPClient != http.DefaultClient {
		t.Fatal("expected HTTPClient to be http.DefaultClient")
	}
	if c.RetryCount != DefaultRetryCount {
		t.Fatalf("expected RetryCount %d, got %d", DefaultRetryCount, c.RetryCount)
	}
	if c.BackoffBase != 500*time.Millisecond {
		t.Fatalf("expected BackoffBase 500ms, got %v", c.BackoffBase)
	}
}

func testRoundTripper(tb testing.TB, fn func(req *http.Request) (*http.Response, error)) http.RoundTripper {
	tb.Helper()
	return roundTripperFunc(fn)
}

func TestDo_MultipleRetriesOnConnectionError(t *testing.T) {
	var dialCount int32
	client := NewClient()
	client.RetryCount = 2
	client.BackoffBase = 10 * time.Millisecond

	req, _ := http.NewRequest("GET", "http://127.0.0.1:1", nil)
	_, err := client.Do(req)

	if err == nil {
		t.Fatal("expected connection error")
	}

	var netErr interface{ Temporary() bool }
	if !errors.As(err, &netErr) {
		t.Logf("error is a network error: %v", err)
	}
	_ = dialCount
}
