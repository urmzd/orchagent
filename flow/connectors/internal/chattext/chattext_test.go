package chattext_test

import (
	"testing"

	"github.com/urmzd/orchagent/flow/connectors/internal/chattext"
)

func TestMarkdownBecomesWhatEachAppShows(t *testing.T) {
	for _, tc := range []struct {
		name, in, whatsapp, plain string
	}{
		{"bold", "a **b** c", "a *b* c", "a b c"},
		{"underscore bold", "__b__", "*b*", "b"},
		{"strike", "~~x~~", "~x~", "x"},
		{"heading", "## Plan", "*Plan*", "Plan"},
		{"bold heading", "# **Plan**", "*Plan*", "Plan"},
		{"link", "see [docs](https://x.example/d)", "see docs (https://x.example/d)", "see docs (https://x.example/d)"},
		{"bare link", "[https://x.example](https://x.example)", "https://x.example", "https://x.example"},
		{"star item", "* one\n  * two", "- one\n  - two", "- one\n  - two"},
		{"inline code", "run `ls`", "run `ls`", "run ls"},
		{"fence kept verbatim", "```go\na **b**\n```", "```\na **b**\n```", "a **b**"},
		{"plain text untouched", "2 * 3 = 6", "2 * 3 = 6", "2 * 3 = 6"},
		{"empty", "", "", ""},
	} {
		if got := chattext.WhatsApp(tc.in); got != tc.whatsapp {
			t.Errorf("%s: WhatsApp(%q) = %q, want %q", tc.name, tc.in, got, tc.whatsapp)
		}
		if got := chattext.Plain(tc.in); got != tc.plain {
			t.Errorf("%s: Plain(%q) = %q, want %q", tc.name, tc.in, got, tc.plain)
		}
	}
}
