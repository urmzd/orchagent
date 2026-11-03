package signature_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/urmzd/orchagent/flow/signature"
)

// Meta signs a Graph API (WhatsApp Cloud API) webhook exactly as GitHub signs
// one: "sha256=" + hex HMAC of the raw body under the app secret. These tests
// hold the Meta verifier to every confinement property the GitHub one has.

const metaSecret = "meta-app-secret"

func metaHeader(sig string) http.Header {
	h := http.Header{}
	h.Set(signature.HeaderMetaSignature, sig)
	return h
}

func TestMetaAcceptsTheRawBodySignedUnderTheAppSecret(t *testing.T) {
	for _, body := range [][]byte{bodyJSON, bodyUnicode, bodyBinary(), {}} {
		sig := refGitHub(metaSecret, body)
		if err := signature.NewMeta(metaSecret).Verify(metaHeader(sig), body); err != nil {
			t.Fatalf("Meta.Verify(%q) = %v, want nil", body, err)
		}
	}
	upper := "sha256=" + strings.ToUpper(strings.TrimPrefix(refGitHub(metaSecret, bodyJSON), "sha256="))
	if err := signature.NewMeta(metaSecret).Verify(metaHeader(upper), bodyJSON); err != nil {
		t.Fatalf("Meta.Verify(uppercase hex) = %v, want nil", err)
	}
}

func TestMetaRejectsForgedRequests(t *testing.T) {
	valid := refGitHub(metaSecret, bodyJSON)
	for _, tc := range []struct {
		name string
		sig  string
		body []byte
	}{
		{"body tampered after signing", valid, append(append([]byte{}, bodyJSON...), ' ')},
		{"body truncated by one byte", valid, bodyJSON[:len(bodyJSON)-1]},
		{"single bit flipped in the digest", flipHexNibble(valid), bodyJSON},
		{"digest signed under a different secret", refGitHub(metaSecret+"x", bodyJSON), bodyJSON},
		{"digest signed under an empty secret", refGitHub("", bodyJSON), bodyJSON},
		{"all-zero digest", "sha256=" + zeroDigest, bodyJSON},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mustBe(t, signature.NewMeta(metaSecret).Verify(metaHeader(tc.sig), tc.body), signature.ErrSignatureMismatch)
		})
	}
}

func TestMetaRejectsMalformedHeaders(t *testing.T) {
	valid := refGitHub(metaSecret, bodyJSON)
	hexDigest := strings.TrimPrefix(valid, "sha256=")
	twice := func(second string) http.Header {
		h := http.Header{}
		h.Add(signature.HeaderMetaSignature, valid)
		h.Add(signature.HeaderMetaSignature, second)
		return h
	}
	for _, tc := range []struct {
		name   string
		header http.Header
	}{
		{"no headers at all", http.Header{}},
		{"empty signature", metaHeader("")},
		{"duplicated signature, identical values", twice(valid)},
		{"duplicated signature, smuggled second value", twice("sha256=" + zeroDigest)},
		{"missing algorithm prefix", metaHeader(hexDigest)},
		{"sha1 algorithm prefix", metaHeader("sha1=" + hexDigest)},
		{"non-hex digest", metaHeader("sha256=" + strings.Repeat("z", 64))},
		{"odd-length hex", metaHeader("sha256=" + hexDigest[:63])},
		{"truncated digest", metaHeader("sha256=" + hexDigest[:62])},
		{"over-long digest", metaHeader("sha256=" + hexDigest + "ab")},
		{"digest with trailing whitespace", metaHeader(valid + " ")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mustBe(t, signature.NewMeta(metaSecret).Verify(tc.header, bodyJSON), signature.ErrMalformed)
		})
	}
}

func TestMetaWithNoAppSecretRefusesEverything(t *testing.T) {
	sig := refGitHub("", bodyJSON)
	for name, v := range map[string]*signature.Meta{
		"nil secret":    {},
		"empty secret":  {AppSecret: []byte{}},
		"NewMeta(\"\")": signature.NewMeta(""),
	} {
		t.Run(name, func(t *testing.T) {
			mustBe(t, v.Verify(metaHeader(sig), bodyJSON), signature.ErrNoSecret)
		})
	}
}
