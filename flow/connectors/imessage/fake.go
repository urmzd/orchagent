package imessage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
)

// Fake is a complete in-memory BlueBubbles server, not a stub.
//
// It ships in the package so an integrator, a demo or an example can run the
// whole path with no Mac. Sent messages get guids, a sent file is kept with
// its bytes, and a received attachment is downloadable once added, the way
// the server serves only attachments it has.
type Fake struct {
	mu          sync.Mutex
	messages    []FakeMessage
	attachments map[string][]byte
	seq         int
	// Err, when set, fails every call.
	Err error
}

// FakeMessage is one message the fake sent.
type FakeMessage struct {
	GUID   string
	Chat   string
	Text   string
	Method string
	File   *Attachment
}

// NewFake returns an empty fake server.
func NewFake() *Fake { return &Fake{attachments: map[string][]byte{}} }

func (f *Fake) next() string {
	f.seq++
	return fmt.Sprintf("FAKE-%06d", f.seq)
}

// SendText implements API.
func (f *Fake) SendText(_ context.Context, m Text) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Err != nil {
		return "", f.Err
	}
	if m.Chat == "" || m.Message == "" {
		return "", fmt.Errorf("imessage: fake: a text needs a chat and a message")
	}
	g := f.next()
	f.messages = append(f.messages, FakeMessage{GUID: g, Chat: m.Chat, Text: m.Message, Method: method(m.Method)})
	return g, nil
}

// SendAttachment implements API.
func (f *Fake) SendAttachment(_ context.Context, a Attachment) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Err != nil {
		return "", f.Err
	}
	if a.Chat == "" || len(a.Data) == 0 {
		return "", fmt.Errorf("imessage: fake: an attachment needs a chat and content")
	}
	a.Data = append([]byte(nil), a.Data...)
	g := f.next()
	f.messages = append(f.messages, FakeMessage{GUID: g, Chat: a.Chat, Method: method(a.Method), File: &a})
	return g, nil
}

// DownloadAttachment implements API.
func (f *Fake) DownloadAttachment(_ context.Context, guid string) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Err != nil {
		return nil, f.Err
	}
	b, ok := f.attachments[guid]
	if !ok {
		return nil, fmt.Errorf("imessage: fake: no attachment %s", guid)
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

// AddAttachment makes a received attachment downloadable.
func (f *Fake) AddAttachment(guid string, data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attachments[guid] = append([]byte(nil), data...)
}

// Messages returns every message sent, oldest first.
func (f *Fake) Messages() []FakeMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]FakeMessage(nil), f.messages...)
}
