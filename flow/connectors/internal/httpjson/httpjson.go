// Package httpjson is the JSON-over-HTTP client the connectors' real API
// implementations share.
//
// It exists because three connectors need the same six lines — marshal, set
// headers, do, check the status, read the body, unmarshal — and because the two
// details that are easy to get wrong belong in one place: a non-2xx response must
// carry the provider's error body into the error (a Slack or GitHub failure is
// only diagnosable from its body), and the body must be drained and closed so the
// connection returns to the pool.
//
// This is not an SDK and not a general HTTP abstraction: no retries, no rate
// limiting, no pagination. Each connector's API surface here is a handful of
// endpoints, and every one of them is called from a path that already has a
// policy for failure.
package httpjson

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// DefaultTimeout bounds one API call. A sink is called from a delivery loop that
// has other runs to serve, so a hung provider must not hold it.
const DefaultTimeout = 15 * time.Second

// maxErrorBody bounds how much of a failure response is carried into an error.
// Enough for a provider's JSON error object, not enough to log an HTML error
// page.
const maxErrorBody = 4 << 10

// Client calls one API base with a fixed set of headers.
type Client struct {
	// HTTP performs the requests. Nil means a client with DefaultTimeout.
	HTTP *http.Client
	// Base is the API root, without a trailing slash.
	Base string
	// Header is sent on every request: authorization, accept, api version.
	Header http.Header
}

// Error is a non-2xx response.
type Error struct {
	Status int
	Method string
	URL    string
	// Body is the response body, truncated. Providers put the reason here and
	// nowhere else.
	Body string
}

func (e *Error) Error() string {
	if e.Body == "" {
		return fmt.Sprintf("%s %s: %s", e.Method, e.URL, http.StatusText(e.Status))
	}
	return fmt.Sprintf("%s %s: %d %s: %s", e.Method, e.URL, e.Status, http.StatusText(e.Status), e.Body)
}

// Retryable reports whether repeating the request could plausibly succeed: a
// rate limit, or the provider having a bad moment. A 4xx other than 429 is our
// bug or our credential, and repeating it only spends quota.
func (e *Error) Retryable() bool {
	return e.Status == http.StatusTooManyRequests || e.Status >= 500
}

// Do performs one call. in is marshalled as the request body when non-nil, and
// out is unmarshalled from the response body when non-nil.
func (c *Client) Do(ctx context.Context, method, path string, in, out any) error {
	if in == nil {
		return c.Send(ctx, method, path, "", nil, out)
	}
	encoded, err := json.Marshal(in)
	if err != nil {
		return fmt.Errorf("httpjson: marshal %s %s: %w", method, path, err)
	}
	return c.Send(ctx, method, path, "application/json; charset=utf-8", bytes.NewReader(encoded), out)
}

// Send performs one call with a body that is already encoded, such as a
// multipart upload, and decodes a JSON answer into out when non-nil.
// contentType is set when the client's headers name none.
func (c *Client) Send(ctx context.Context, method, path, contentType string, body io.Reader, out any) error {
	resp, url, err := c.open(ctx, method, path, contentType, body)
	if err != nil {
		return err
	}
	defer drain(resp)
	if out == nil || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("httpjson: decode %s %s: %w", method, url, err)
	}
	return nil
}

// Open performs a GET and returns the 2xx response body for the caller to
// stream, with its headers. The caller closes the body. It is how a file is
// downloaded from an API that serves it from its own base.
func (c *Client) Open(ctx context.Context, path string) (io.ReadCloser, http.Header, error) {
	resp, _, err := c.open(ctx, http.MethodGet, path, "", nil)
	if err != nil {
		return nil, nil, err
	}
	return resp.Body, resp.Header, nil
}

// open sends the request and returns a 2xx response, or the provider's
// error. On error the body is already drained and closed.
func (c *Client) open(ctx context.Context, method, path, contentType string, body io.Reader) (*http.Response, string, error) {
	url := c.Base + path
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, url, fmt.Errorf("httpjson: build %s %s: %w", method, redact(url), err)
	}
	for k, vs := range c.Header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	if body != nil && contentType != "" && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", contentType)
	}

	resp, err := c.client().Do(req)
	if err != nil {
		return nil, url, fmt.Errorf("httpjson: %s %s: %w", method, redact(url), scrub(err, url))
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
		drain(resp)
		return nil, url, &Error{
			Status: resp.StatusCode,
			Method: method,
			URL:    redact(url),
			Body:   strings.TrimSpace(string(snippet)),
		}
	}
	return resp, url, nil
}

// drain empties and closes a response body: an undrained body cannot be
// reused, and these connectors call the same host repeatedly.
func drain(resp *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxErrorBody))
	_ = resp.Body.Close()
}

// redact drops the query string from a URL before it goes into an error. A
// provider that authenticates with a query parameter (BlueBubbles takes its
// server password as ?password=) would otherwise put the credential in every
// log line that prints a failed call.
func redact(url string) string {
	if i := strings.IndexByte(url, '?'); i >= 0 {
		return url[:i] + "?[redacted]"
	}
	return url
}

// scrub replaces the URL inside a transport error (net/http wraps it in a
// *url.Error that prints it) with its redacted form.
func scrub(err error, url string) error {
	msg := err.Error()
	if !strings.Contains(msg, url) {
		return err
	}
	return errors.New(strings.ReplaceAll(msg, url, redact(url)))
}

func (c *Client) client() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return &http.Client{Timeout: DefaultTimeout}
}
