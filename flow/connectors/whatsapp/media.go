package whatsapp

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/urmzd/orchagent/flow/attachment"
	"github.com/urmzd/orchagent/flow/attachment/intake"
)

// MediaHosts are the hosts Meta serves received media from (the url GET
// /{media-id} returns is on lookaside.fbsbx.com), and the only hosts the
// access token is sent to when downloading one.
var MediaHosts = []string{"lookaside.fbsbx.com", "*.fbsbx.com"}

// mediaHosts is the host policy for media downloads.
func mediaHosts() intake.Allow { return intake.Hosts(MediaHosts...) }

// mediaHeaders returns the per-hop credential policy: the access token, over
// HTTPS, to MediaHosts only. A redirect anywhere else gets no token.
func mediaHeaders(token string) func(*url.URL) http.Header {
	allow := mediaHosts()
	return func(u *url.URL) http.Header {
		if token == "" || u.Scheme != "https" || allow(u) != nil {
			return nil
		}
		return http.Header{"Authorization": {"Bearer " + token}}
	}
}

// candidate turns a received media object into an intake candidate.
// WhatsApp gives no size before the lookup, so the size cap is enforced on
// the bytes read.
func (c *Connector) candidate(m *mediaRef) []intake.Candidate {
	if c.fetcher == nil || m == nil {
		return nil
	}
	name := m.Filename
	if name == "" {
		name = defaultName(m.MimeType, m.Voice)
	}
	return []intake.Candidate{{Name: name, Alt: m.Caption, Fetch: c.fetchMedia(m.ID)}}
}

// fetchMedia looks a media object up and downloads it with the access token.
func (c *Connector) fetchMedia(id string) func(context.Context, string, intake.Meta) (attachment.Attachment, error) {
	return func(ctx context.Context, tenant string, meta intake.Meta) (attachment.Attachment, error) {
		media, err := c.cfg.API.Media(ctx, id)
		if err != nil {
			return attachment.Attachment{}, err
		}
		if meta.MaxBytes > 0 && media.Size > meta.MaxBytes {
			return attachment.Attachment{}, fmt.Errorf("whatsapp: media %s is %d bytes, over the %d byte limit", id, media.Size, meta.MaxBytes)
		}
		att, err := c.fetcher.Fetch(ctx, tenant, media.URL, meta)
		if status := (*intake.StatusError)(nil); errors.As(err, &status) && status.Refused() {
			return attachment.Attachment{}, fmt.Errorf("whatsapp: %w: %w", intake.ErrRefused, err)
		}
		return att, err
	}
}

// defaultName names a file WhatsApp sent without one, by its declared type.
// The name is a label only: intake sniffs the type from the bytes.
func defaultName(mimeType string, voice bool) string {
	mt, _, _ := strings.Cut(strings.ToLower(mimeType), ";")
	ext := map[string]string{
		"audio/ogg": ".ogg", "audio/mpeg": ".mp3", "audio/mp4": ".m4a", "audio/aac": ".aac", "audio/amr": ".amr",
		"image/jpeg": ".jpg", "image/png": ".png", "image/webp": ".webp", "video/mp4": ".mp4", "application/pdf": ".pdf",
	}[strings.TrimSpace(mt)]
	switch {
	case voice:
		return "voice-note" + ext
	case strings.HasPrefix(mt, "audio/"):
		return "audio" + ext
	case strings.HasPrefix(mt, "image/"):
		return "image" + ext
	case strings.HasPrefix(mt, "video/"):
		return "video" + ext
	}
	return "file" + ext
}
