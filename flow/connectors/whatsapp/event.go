package whatsapp

import (
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/urmzd/orchagent/flow/address"
	"github.com/urmzd/orchagent/flow/connectors/mention"
	"github.com/urmzd/orchagent/flow/event"
)

// notification is the body of a webhook POST. Only what the connector acts
// on is decoded: Meta adds fields without notice, and a strict decoder would
// turn that into an outage.
type notification struct {
	Object string  `json:"object"`
	Entry  []entry `json:"entry"`
}

type entry struct {
	// ID is the WhatsApp Business Account id.
	ID      string   `json:"id"`
	Changes []change `json:"changes"`
}

type change struct {
	Field string `json:"field"`
	Value value  `json:"value"`
}

type value struct {
	MessagingProduct string `json:"messaging_product"`
	Metadata         struct {
		DisplayPhoneNumber string `json:"display_phone_number"`
		PhoneNumberID      string `json:"phone_number_id"`
	} `json:"metadata"`
	Contacts []struct {
		Profile struct {
			Name string `json:"name"`
		} `json:"profile"`
		WaID string `json:"wa_id"`
	} `json:"contacts"`
	Messages []message `json:"messages"`
	// Statuses (sent, delivered, read, failed) are acknowledged and dropped.
	Statuses []any `json:"statuses"`
}

type message struct {
	From      string `json:"from"`
	ID        string `json:"id"`
	Timestamp string `json:"timestamp"`
	Type      string `json:"type"`
	Text      struct {
		Body string `json:"body"`
	} `json:"text"`
	Context struct {
		ID string `json:"id"`
	} `json:"context"`
	Audio    *mediaRef `json:"audio"`
	Image    *mediaRef `json:"image"`
	Video    *mediaRef `json:"video"`
	Document *mediaRef `json:"document"`
	Button   struct {
		Text string `json:"text"`
	} `json:"button"`
	Interactive struct {
		ButtonReply struct {
			Title string `json:"title"`
		} `json:"button_reply"`
		ListReply struct {
			Title string `json:"title"`
		} `json:"list_reply"`
	} `json:"interactive"`
}

// mediaRef is a media object on a received message. The bytes are not in
// the webhook: id is looked up with GET /{media-id}.
type mediaRef struct {
	ID       string `json:"id"`
	MimeType string `json:"mime_type"`
	SHA256   string `json:"sha256"`
	Caption  string `json:"caption"`
	Filename string `json:"filename"`
	// Voice is set on an audio message recorded as a voice note.
	Voice bool `json:"voice"`
}

// received is one message translated into an event, with the media it
// carries, which file intake brings in.
type received struct {
	env   event.Envelope
	media *mediaRef
}

// eventsFrom translates a notification into one event per message it
// carries. Everything that decides "nothing" is here, because which messages
// are ignored is a security-adjacent question: a status callback is not a
// request, and a message from the business number itself would let the mesh
// answer itself.
func (c *Connector) eventsFrom(n notification) []received {
	if n.Object != "whatsapp_business_account" {
		return nil
	}
	var out []received
	for _, en := range n.Entry {
		for _, ch := range en.Changes {
			if ch.Field != "messages" {
				continue
			}
			v := ch.Value
			number := v.Metadata.PhoneNumberID
			if number == "" || address.ValidWorkspace(number) != nil {
				continue
			}
			if c.cfg.PhoneNumberID != "" && number != c.cfg.PhoneNumberID {
				// One app secret signs every number on the app. A verified
				// delivery still has to be for this connector's number.
				continue
			}
			names := map[string]string{}
			for _, ct := range v.Contacts {
				names[ct.WaID] = ct.Profile.Name
			}
			for _, m := range v.Messages {
				if r, ok := c.eventFrom(number, digits(v.Metadata.DisplayPhoneNumber), names, m); ok {
					out = append(out, r)
				}
			}
		}
	}
	return out
}

func (c *Connector) eventFrom(number, own string, names map[string]string, m message) (received, bool) {
	if m.ID == "" || m.From == "" {
		return received{}, false
	}
	// Loop prevention: the business number never answers itself, whatever
	// a coexistence or echo setting delivers.
	if own != "" && m.From == own {
		return received{}, false
	}

	var text string
	var media *mediaRef
	switch m.Type {
	case "text":
		text = m.Text.Body
	case "button":
		text = m.Button.Text
	case "interactive":
		text = firstNonEmpty(m.Interactive.ButtonReply.Title, m.Interactive.ListReply.Title)
	case "audio":
		media = m.Audio
	case "image":
		media = m.Image
	case "video":
		media = m.Video
	case "document":
		media = m.Document
	default:
		// Reactions, stickers, locations, contacts, system and unsupported
		// messages are not requests.
		return received{}, false
	}
	if media != nil {
		if media.ID == "" {
			return received{}, false
		}
		text = media.Caption
	}
	if text == "" && (media == nil || c.fetcher == nil) {
		// Nothing to read: no text, and no file this connector brings in.
		return received{}, false
	}

	mentions := c.mentions(&text)
	channel := Address(number, m.From)
	ref := &event.WhatsAppRef{
		PhoneNumberID: number,
		From:          m.From,
		MessageID:     m.ID,
		Type:          m.Type,
		ReplyTo:       m.Context.ID,
	}
	if m.Type == "audio" && media != nil {
		ref.Voice = media.Voice
	}
	body := event.MessagePosted{
		Request:  event.Request{Text: text, Actor: event.Actor{ID: m.From, Display: names[m.From]}, Mentions: mentions},
		WhatsApp: ref,
	}
	return received{env: event.Envelope{
		ID:      m.ID,
		Source:  channel.String(),
		Type:    body.Type(),
		Subject: m.From,
		Channel: channel.String(),
		Time:    unixTime(m.Timestamp),
		Tenant:  c.cfg.Tenant,
		Data:    body,
	}, media: media}, true
}

// mentions lists the agents a message names: Config.Agent always, since the
// number fronts it, then any @name the text carries that Config.Agents
// knows, stripped from the text.
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

// unixTime reads WhatsApp's timestamp, whole seconds as a string. A missing
// or broken one is now, since an event needs a time.
func unixTime(s string) time.Time {
	if sec, err := strconv.ParseInt(s, 10, 64); err == nil && sec > 0 {
		return time.Unix(sec, 0).UTC()
	}
	return time.Now().UTC()
}

// digits keeps the digits of a phone number, which is how WhatsApp writes a
// wa_id: "+1 555-010-0000" is "15550100000".
func digits(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
