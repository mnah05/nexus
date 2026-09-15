package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	requestVotePath   = "/raft/request-vote"
	appendEntriesPath = "/raft/append-entries"
)

// httpTransport implements NodeTransport over HTTP/JSON. Each RPC is one POST
// whose body is the JSON-encoded args and whose response is the JSON-encoded reply.
type httpTransport struct {
	client *http.Client
}

// NewHTTPTransport returns a NodeTransport that talks to peers over HTTP.
func NewHTTPTransport(timeout time.Duration) NodeTransport {
	return &httpTransport{client: &http.Client{Timeout: timeout}}
}

// RequestVote asks a peer for its vote.
func (t *httpTransport) RequestVote(ctx context.Context, addr string, args RequestArgs) (RequestReply, error) {
	var reply RequestReply
	if err := t.post(ctx, addr, requestVotePath, args, &reply); err != nil {
		return RequestReply{}, err
	}
	return reply, nil
}

// AppendEntries sends a heartbeat or log entries to a peer.
func (t *httpTransport) AppendEntries(ctx context.Context, addr string, args AppendArgs) (AppendReply, error) {
	var reply AppendReply
	if err := t.post(ctx, addr, appendEntriesPath, args, &reply); err != nil {
		return AppendReply{}, err
	}
	return reply, nil
}

// post marshals args as JSON, POSTs it to addr+path, and decodes the response
// into out. A missing scheme defaults to http://.
func (t *httpTransport) post(ctx context.Context, addr, path string, args, out any) error {
	data, err := json.Marshal(args)
	if err != nil {
		return err
	}

	target := addr
	if !strings.HasPrefix(target, "http://") && !strings.HasPrefix(target, "https://") {
		target = "http://" + target
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("raft: %s %s returned HTTP %d: %s", addr, path, resp.StatusCode, strings.TrimSpace(string(body)))
	}

	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("raft: decode %s response: %w", path, err)
	}
	return nil
}
