package main

import (
	"io"
	"net/http"
)

// drainBody reads up to 4KB of the response body and closes it to ensure connection reuse.
func drainBody(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		// Limit reading to prevent hanging on large response bodies
		io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
	}
}

func main() {
	// Implementation of retry loop would call drainBody(resp) before retrying.
}