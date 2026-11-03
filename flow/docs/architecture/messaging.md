# WhatsApp and iMessage connectors

`pkg/connectors/whatsapp` and `pkg/connectors/imessage` bring a person's
phone into the mesh. Both are trigger, sink and actor, and both differ from
Slack in one way that shapes everything else: neither surface can edit a
message once it is sent, so neither streams. A run sends nothing while it
works, one message when it waits on a person (a question or an approval, as
plain text), and one message with the answer when it ends.

Neither is built by `flow.New` from a typed option. Construct one in Go and
pass it with `flow.WithConnector`, then route its webhook to the connector's
`http.Handler`.

## Addresses

| Connector | Channel | Example |
|-----------|---------|---------|
| whatsapp | `whatsapp://<phone-number-id>/<wa-id>` | `whatsapp://106540352242922/15551234567` |
| imessage | `imessage://<server>/<chat-guid>` | `imessage://home/iMessage%3B-%3B+15551234567` |

The WhatsApp workspace is the business number's Graph API phone number id,
and the path is the person's WhatsApp id (their number without `+`). The
iMessage workspace is a name the operator gives the BlueBubbles server, and
the path is Apple's chat guid, path escaped. `whatsapp.Address` and
`imessage.Address` build them. A one-to-one conversation is a direct
message (`event.DirectMessage`), so a member agent answers without being
named; in an iMessage group an agent is named with `@name`.

## Events

Each message publishes `message.posted.v1` with the sender as the actor and
the surface's message id as the event id (WhatsApp `wamid...`, the
BlueBubbles message guid), so a retried webhook joins the run it already
started. The body carries `whatsapp` (`event.WhatsAppRef`) or `imessage`
(`event.IMessageRef`). Status callbacks, reactions, tapbacks, group events,
and messages the business number or the Mac's own account sent are dropped:
the last is what stops the sink's answers from waking the agent again.

## Voice notes

With `Config.Files` (a blob store and caps) set, the media on a message is
downloaded, stored under the tenant and carried as attachments typed by
their bytes. A voice note is an attachment of kind `audio`
(`attachment.OfKind(req.Attachments, attachment.KindAudio)` finds it),
`WhatsAppRef.Voice` or `IMessageRef.Audio` is set, and the text is empty.
orchagent does not transcribe. A host does, either in `Config.Prepare`,
which runs after the files are in and before the event is published, or
before it accepts the run. iMessage audio is Core Audio Format
(`audio/x-caf`) unless the BlueBubbles server converts it on download.

To answer with audio, the host calls `SendAudio` on the connector:
WhatsApp uploads the file and sends an `audio` message (Ogg Opus shows as a
voice note), BlueBubbles sends it as an attachment, an audio message when
`Voice` is set and the server runs the Private API.

## WhatsApp setup

1. Create a Meta app with the WhatsApp product, and note the business
   number's phone number id and a system user access token with
   `whatsapp_business_messaging`.
2. Expose the connector at one URL for both methods:
   `GET /connectors/whatsapp` answers the subscription handshake and
   `POST /connectors/whatsapp` takes notifications.
3. In the app's WhatsApp configuration, set that URL as the callback, the
   verify token to `Config.VerifyToken`, and subscribe to `messages`.

```go
c, err := whatsapp.New(whatsapp.Config{
    API:           whatsapp.NewHTTPAPI(accessToken, http.DefaultClient),
    Verifier:      signature.NewMeta(appSecret),
    VerifyToken:   verifyToken,
    PhoneNumberID: "106540352242922",
    Agent:         "concierge",
    Tenant:        "acme",
    Files:         connectors.Files{Store: blobs},
    MediaToken:    accessToken,
})
```

Meta accepts a free-form reply only within 24 hours of the person's last
message. A run that ends later is refused by the API and the send is
retried until the run is evicted.

## iMessage setup

1. Run a BlueBubbles server on a Mac signed in to Messages, reachable from
   the host over TLS (its Cloudflare or ngrok tunnel, or your own proxy).
2. Expose `POST /connectors/imessage` and register
   `https://<host>/connectors/imessage?secret=<Config.Secret>` as a webhook
   for new messages in the server's API and webhooks settings. BlueBubbles
   signs nothing and its webhook takes only a URL, so the secret is in the
   URL. It is compared in constant time before the body is read; a gateway
   that can authenticate the server another way passes `Config.Verifier`.

```go
c, err := imessage.New(imessage.Config{
    API:    imessage.NewHTTPAPI("https://bb.example.com", serverPassword, http.DefaultClient),
    Server: "home",
    Secret: webhookSecret,
    Agent:  "concierge",
    Tenant: "acme",
    Files:  connectors.Files{Store: blobs},
})
```

Sends use AppleScript by default; set `Method: imessage.MethodPrivateAPI`
when the server has the Private API helper.

## Limitations

- Which moments were sent is kept in memory, per process. A restart, or an
  eviction after `DefaultMaxRuns` runs, forgets it, and a redelivery after
  that sends the moment again.
- `whatsapp_send_message` and `imessage_send_message` reach only the
  conversation the run answers. Messaging someone new is not offered: on
  WhatsApp it needs an approved template, and from a model it is a spam
  risk.
- The BlueBubbles client is small and follows the server's REST routes
  (`/api/v1/message/text`, `/api/v1/message/attachment`,
  `/api/v1/attachment/{guid}/download`); it reads the sent message's guid
  when the server reports one.
