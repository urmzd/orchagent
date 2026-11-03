// Package chattext turns the Markdown a model writes into what a phone
// messaging app shows: WhatsApp's light formatting, or plain text for
// iMessage, which has none.
//
// Models answer in Markdown whatever they are told, and a chat app shows
// Markdown as punctuation: "**Note**" arrives as two pairs of asterisks
// around a word. The conversion is deliberately small and line based: bold,
// strikethrough, headings, links and code fences, the constructs that are
// common in answers and legible once converted. Anything else passes
// through unchanged, which is what a person would see anyway.
//
// Text inside a fenced code block is never rewritten.
package chattext

import (
	"regexp"
	"strings"
)

var (
	boldStars  = regexp.MustCompile(`\*\*([^*\n]+?)\*\*`)
	boldUnders = regexp.MustCompile(`__([^_\n]+?)__`)
	strike     = regexp.MustCompile(`~~([^~\n]+?)~~`)
	link       = regexp.MustCompile(`\[([^\]\n]+)\]\(([^)\s]+)\)`)
	heading    = regexp.MustCompile(`^#{1,6}\s+(.+?)\s*#*\s*$`)
	inlineCode = regexp.MustCompile("`([^`\n]+)`")
	starItem   = regexp.MustCompile(`^(\s*)\* `)
)

// WhatsApp converts Markdown to WhatsApp formatting: *bold*, ~strike~, a
// heading as a bold line, a link as "text (url)". Code fences and inline
// code are kept, since WhatsApp renders both as monospace.
func WhatsApp(md string) string {
	return convert(md, func(line string) string {
		if m := heading.FindStringSubmatch(line); m != nil {
			return "*" + stripBold(m[1]) + "*"
		}
		line = starItem.ReplaceAllString(line, "$1- ")
		line = boldStars.ReplaceAllString(line, "*$1*")
		line = boldUnders.ReplaceAllString(line, "*$1*")
		line = strike.ReplaceAllString(line, "~$1~")
		return links(line)
	}, true)
}

// Plain converts Markdown to plain text: emphasis markers and fences are
// dropped, a heading is its words, a link is "text (url)".
func Plain(md string) string {
	return convert(md, func(line string) string {
		if m := heading.FindStringSubmatch(line); m != nil {
			line = m[1]
		}
		line = starItem.ReplaceAllString(line, "$1- ")
		line = stripBold(line)
		line = strike.ReplaceAllString(line, "$1")
		line = inlineCode.ReplaceAllString(line, "$1")
		return links(line)
	}, false)
}

func stripBold(s string) string {
	s = boldStars.ReplaceAllString(s, "$1")
	return boldUnders.ReplaceAllString(s, "$1")
}

func links(line string) string {
	return link.ReplaceAllStringFunc(line, func(m string) string {
		sub := link.FindStringSubmatch(m)
		if sub[1] == sub[2] {
			return sub[2]
		}
		return sub[1] + " (" + sub[2] + ")"
	})
}

// convert applies fn to every line outside a fenced code block. keepFences
// keeps the fence lines themselves.
func convert(md string, fn func(string) string, keepFences bool) string {
	if md == "" {
		return md
	}
	lines := strings.Split(md, "\n")
	out := lines[:0]
	fenced := false
	for _, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "```") {
			fenced = !fenced
			if keepFences {
				out = append(out, "```")
			}
			continue
		}
		if fenced {
			out = append(out, line)
			continue
		}
		out = append(out, fn(line))
	}
	return strings.Join(out, "\n")
}
