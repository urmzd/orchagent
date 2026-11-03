package imessage

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/urmzd/orchagent/flow/attachment"
	"github.com/urmzd/orchagent/flow/attachment/intake"
	"github.com/urmzd/orchagent/flow/connectors/mention"
	"github.com/urmzd/orchagent/flow/event"
)

// hook is the body of a BlueBubbles webhook POST: {"type": ..., "data": ...}.
// Only new-message is acted on.
type hook struct {
	Type string  `json:"type"`
	Data message `json:"data"`
}

// message is a BlueBubbles message. Only what the connector acts on is
// decoded, and every field is optional, because which fields the server
// includes depends on its version and on the macOS version under it.
type message struct {
	GUID     string `json:"guid"`
	Text     string `json:"text"`
	IsFromMe bool   `json:"isFromMe"`
	Handle   *struct {
		Address string `json:"address"`
	} `json:"handle"`
	Chats []struct {
		GUID        string `json:"guid"`
		DisplayName string `json:"displayName"`
	} `json:"chats"`
	Attachments []fileRef `json:"attachments"`
	// DateCreated is milliseconds since the Unix epoch.
	DateCreated int64 `json:"dateCreated"`
	// ItemType is 0 for a message; other values are group events (renames,
	// members joining).
	ItemType int `json:"itemType"`
	// AssociatedMessageType is set on a tapback or another message that
	// refers to an earlier one rather than saying something. It is a number
	// or a string depending on the server version, so it is read loosely.
	AssociatedMessageType any  `json:"associatedMessageType"`
	IsAudioMessage        bool `json:"isAudioMessage"`
	IsSystemMessage       bool `json:"isSystemMessage"`
}

// fileRef is an attachment on a received message.
type fileRef struct {
	GUID         string `json:"guid"`
	MimeType     string `json:"mimeType"`
	TransferName string `json:"transferName"`
	TotalBytes   int64  `json:"totalBytes"`
	UTI          string `json:"uti"`
}

// audio reports an audio file: by type, or by Apple's UTI for Core Audio
// Format, which is what Messages records an audio message as.
func (f fileRef) audio() bool {
	return strings.HasPrefix(strings.ToLower(f.MimeType), "audio/") || f.UTI == "com.apple.coreaudio-format" ||
		strings.HasSuffix(strings.ToLower(f.TransferName), ".caf")
}

// received is one message translated into an event, with its files.
type received struct {
	env   event.Envelope
	files []fileRef
}

// objectReplacement is what Messages puts in a message's text where an
// attachment sits.
const objectReplacement = "￼"

// eventFrom translates a webhook into a message.posted.v1 event, or reports
// that it publishes nothing. Loop prevention lives here: a message this
// account sent (isFromMe), including every answer this connector sends, is
// never a request.
func (c *Connector) eventFrom(h hook) (received, bool) {
	if h.Type != "new-message" {
		return received{}, false
	}
	m := h.Data
	if m.IsFromMe || m.GUID == "" || m.ItemType != 0 || m.IsSystemMessage || associated(m.AssociatedMessageType) {
		return received{}, false
	}
	if m.Handle == nil || m.Handle.Address == "" || len(m.Chats) == 0 || m.Chats[0].GUID == "" {
		return received{}, false
	}
	chat := m.Chats[0].GUID
	text := strings.TrimSpace(strings.ReplaceAll(m.Text, objectReplacement, ""))
	if text == "" && (len(m.Attachments) == 0 || !c.cfg.Files.Enabled()) {
		// Nothing to read: no text, and no file this connector brings in.
		return received{}, false
	}

	audio := m.IsAudioMessage
	for _, f := range m.Attachments {
		audio = audio || f.audio()
	}
	group := Group(chat)
	mentions := c.mentions(&text)
	channel := Address(c.cfg.Server, chat)
	body := event.MessagePosted{
		Request: event.Request{Text: text, Actor: event.Actor{ID: m.Handle.Address}, Mentions: mentions},
		IMessage: &event.IMessageRef{
			Server: c.cfg.Server,
			Chat:   chat,
			Sender: m.Handle.Address,
			GUID:   m.GUID,
			Group:  group,
			Audio:  audio,
		},
	}
	at := time.Now().UTC()
	if m.DateCreated > 0 {
		at = time.UnixMilli(m.DateCreated).UTC()
	}
	return received{env: event.Envelope{
		ID:      m.GUID,
		Source:  channel.String(),
		Type:    body.Type(),
		Subject: m.Handle.Address,
		Channel: channel.String(),
		Time:    at,
		Tenant:  c.cfg.Tenant,
		Data:    body,
	}, files: m.Attachments}, true
}

// associated reports a tapback or other associated message. BlueBubbles
// sends null, 0, "" or a type name for an ordinary message depending on its
// version.
func associated(v any) bool {
	switch t := v.(type) {
	case nil:
		return false
	case float64:
		return t != 0
	case string:
		return t != "" && t != "0"
	}
	return true
}

// Group reports whether a chat guid names a group chat. Apple's guids are
// "<service>;-;<handle>" for a one-to-one chat and "<service>;+;<id>" for a
// group.
func Group(chat string) bool { return strings.Contains(chat, ";+;") }

// mentions lists Config.Agent and any agents the text names by "@name".
func (c *Connector) mentions(text *string) []string {
	var out []string
	if c.cfg.Agent != "" {
		out = append(out, c.cfg.Agent)
	}
	if c.cfg.Agents != nil {
		agents, stripped := mention.FindAll(*text, c.cfg.Agents)
		for _, a := range agents {
			if !slices.Contains(out, a) {
				out = append(out, a)
			}
		}
		if len(agents) > 0 {
			*text = stripped
		}
	}
	if c.cfg.Agent != "" {
		*text = mention.Strip(*text, c.cfg.Agent)
	}
	*text = strings.TrimSpace(*text)
	return out
}

// candidates turns a message's attachments into intake candidates, in order.
// The bytes come from the configured server through the API, never from a
// URL in the payload, so there is no fetch policy to apply.
func (c *Connector) candidates(fs []fileRef) []intake.Candidate {
	if !c.cfg.Files.Enabled() || len(fs) == 0 {
		return nil
	}
	out := make([]intake.Candidate, 0, len(fs))
	for _, f := range fs {
		if f.GUID == "" {
			continue
		}
		name := f.TransferName
		if name == "" {
			name = f.GUID
		}
		out = append(out, intake.Candidate{Name: name, Size: f.TotalBytes, Fetch: c.fetch(f.GUID)})
	}
	return out
}

func (c *Connector) fetch(guid string) func(context.Context, string, intake.Meta) (attachment.Attachment, error) {
	return func(ctx context.Context, tenant string, meta intake.Meta) (attachment.Attachment, error) {
		rc, err := c.cfg.API.DownloadAttachment(ctx, guid)
		if err != nil {
			return attachment.Attachment{}, err
		}
		defer func() { _ = rc.Close() }()
		att, err := intake.Store(ctx, c.cfg.Files.Store, tenant, rc, meta)
		if err != nil {
			return attachment.Attachment{}, fmt.Errorf("imessage: attachment %s: %w", guid, err)
		}
		return att, nil
	}
}
