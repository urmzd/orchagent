package whatsapp

import "github.com/urmzd/orchagent/flow/attachment/intake"

// UseFetcher swaps the connector's media fetcher, so a test can serve media
// from loopback. The host and credential policies it would have used are
// exported below and tested on their own.
func UseFetcher(c *Connector, f *intake.Fetcher) { c.fetcher = f }

var (
	MediaHostPolicy = mediaHosts
	MediaHeaders    = mediaHeaders
)
