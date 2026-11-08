package jira_test

import (
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/urmzd/orchagent/flow/event"
)

// An account mention no bot holds is a person, by account id; a literal
// "@name" is Jira text and refers to nobody; links are read as everywhere.
func TestAJiraCommentReadsAccountMentionsAsPeople(t *testing.T) {
	body := "[~accountid:" + botAccount + "] ask [~accountid:" + human + "] and @bob about " +
		"https://github.com/acme/web/pull/3 [~accountid:" + human + "]"
	delivery := strings.Replace(commentCreated,
		`"body": "[~accountid:557058:8b4a1e0f-docs-bot] summarize what changed here, and ask [~accountid:557058:8b4a1e0f-docs-bot] to check the retry path too"`,
		`"body": "`+body+`"`, 1)
	c, tags := start(t, botConfig())
	c.ServeHTTP(httptest.NewRecorder(), deliver(t, "d-people", delivery))
	tag := receive(t, tags)

	want := []event.Reference{
		{Kind: event.ReferenceAgent, Name: "docs-bot", ID: "docs-bot"},
		{Kind: event.ReferenceLink, URL: "https://github.com/acme/web/pull/3", Address: "github://acme/web/pull/3"},
		{Kind: event.ReferencePerson, ID: human},
	}
	if !reflect.DeepEqual(tag.References, want) {
		t.Errorf("References =\n%+v\nwant\n%+v", tag.References, want)
	}
	if tag.Agent != "docs-bot" {
		t.Errorf("Mentions = %v, want docs-bot first", tag.Mentions)
	}
}
