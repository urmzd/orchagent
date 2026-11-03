package whatsapp

import (
	"context"
	"fmt"
	"sync"
	"unicode/utf8"
)

// Fake is a complete in-memory WhatsApp Cloud API, not a stub.
//
// It ships in the package so an integrator, a demo or an example can run the
// whole path (message in, run, answer out) with no Meta app and no network.
// Sent messages get wamid-shaped ids, media objects are looked up by id, an
// upload is a media object that can then be sent, and sending an audio
// message refers to an upload that must exist, the way Meta refuses an
// unknown media id.
type Fake struct {
	mu       sync.Mutex
	messages []FakeMessage
	media    map[string]Media
	uploads  map[string]Upload
	seq      int
	// Err, when set, fails every call. It is how a test drives the retry
	// path.
	Err error
}

// FakeMessage is one message the fake sent.
type FakeMessage struct {
	ID            string
	PhoneNumberID string
	To            string
	// Type is "text" or "audio".
	Type    string
	Body    string
	ReplyTo string
	// MediaID is the uploaded file an audio message sent.
	MediaID string
}

// NewFake returns an empty fake.
func NewFake() *Fake {
	return &Fake{media: map[string]Media{}, uploads: map[string]Upload{}}
}

func (f *Fake) nextID(prefix string) string {
	f.seq++
	return fmt.Sprintf("%s%06d", prefix, f.seq)
}

// SendText implements API.
func (f *Fake) SendText(_ context.Context, m Text) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Err != nil {
		return "", f.Err
	}
	if m.PhoneNumberID == "" || m.To == "" || m.Body == "" {
		return "", fmt.Errorf("whatsapp: fake: a text needs a phone number id, a recipient and a body")
	}
	if utf8.RuneCountInString(m.Body) > MaxBodyChars {
		return "", fmt.Errorf("whatsapp: fake: body is longer than WhatsApp accepts")
	}
	id := f.nextID("wamid.FAKE")
	f.messages = append(f.messages, FakeMessage{ID: id, PhoneNumberID: m.PhoneNumberID, To: m.To, Type: "text", Body: m.Body, ReplyTo: m.ReplyTo})
	return id, nil
}

// SendAudio implements API.
func (f *Fake) SendAudio(_ context.Context, m AudioMessage) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Err != nil {
		return "", f.Err
	}
	if _, ok := f.uploads[m.MediaID]; !ok {
		return "", fmt.Errorf("whatsapp: fake: media %s was never uploaded", m.MediaID)
	}
	id := f.nextID("wamid.FAKE")
	f.messages = append(f.messages, FakeMessage{ID: id, PhoneNumberID: m.PhoneNumberID, To: m.To, Type: "audio", MediaID: m.MediaID})
	return id, nil
}

// Media implements API.
func (f *Fake) Media(_ context.Context, id string) (Media, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Err != nil {
		return Media{}, f.Err
	}
	m, ok := f.media[id]
	if !ok {
		return Media{}, fmt.Errorf("whatsapp: fake: no media %s", id)
	}
	return m, nil
}

// UploadMedia implements API.
func (f *Fake) UploadMedia(_ context.Context, u Upload) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Err != nil {
		return "", f.Err
	}
	if u.MimeType == "" || len(u.Data) == 0 {
		return "", fmt.Errorf("whatsapp: fake: an upload needs a type and content")
	}
	id := f.nextID("media")
	u.Data = append([]byte(nil), u.Data...)
	f.uploads[id] = u
	return id, nil
}

// AddMedia makes a received media object readable through Media. Its URL is
// where the connector downloads the bytes from.
func (f *Fake) AddMedia(m Media) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.media[m.ID] = m
}

// Messages returns every message sent, oldest first.
func (f *Fake) Messages() []FakeMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]FakeMessage(nil), f.messages...)
}

// Uploaded returns the file uploaded under id.
func (f *Fake) Uploaded(id string) (Upload, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	u, ok := f.uploads[id]
	return u, ok
}
