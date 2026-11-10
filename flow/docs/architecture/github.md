# GitHub connector capabilities

Flow owns signature verification, normalized mentions and typed webhook
events, API context and file reads, comments and review-thread replies,
idempotent delivery, commit comparison, workflow API operations, and check-run
transport.

Application policy is supplied through `Config.Automation`, `Config.Prepare`,
`Config.Render`, and `Config.Conclusion`. These hooks do not bypass webhook
verification or repository confinement. `Prepare` receives the
`*event.Envelope` before it is published. With no hooks, the connector answers
with ordinary Markdown and exposes no `/fix` or `/check` tools.

PR auto-label activation, per-commit review policy, PR guides, signed mutation
grants, agent definitions and deployment configuration live in
[Humagen](https://github.com/urmzd/humagen/blob/main/docs/reference/github.md).
Evidence-backed Markdown presentation lives in Humagen's
`internal/presentation/github` package.

For the product setup use `HUMAGEN_GITHUB_*` with `humagen serve`.
`flow.WithGitHub` takes only transport configuration, as a `flow.GitHub`
value: the token, the webhook secret, owners, bot mappings, read
repositories, whether to publish checks, and the app's own login. Auto
labels, guides and mutation grants are Humagen's.

## What a webhook publishes

Each verified delivery becomes one typed event on the issue or pull request's
channel, `github://<owner>/<repo>/issues/<number>` or
`github://<owner>/<repo>/pull/<number>`. The type follows
`github.<event>.<action>.v1` with GitHub's own names:

| Type | Raised by |
|------|-----------|
| `github.issue_comment.created.v1` | A comment on an issue or pull request |
| `github.pull_request_review_comment.created.v1` | A review comment on a diff line |
| `github.pull_request_review.submitted.v1` | A submitted review |
| `github.issues.opened.v1`, `github.issues.labeled.v1` | A new or labeled issue |
| `github.pull_request.opened.v1`, `.ready_for_review.v1`, `.labeled.v1` | A pull request opened, marked ready, or labeled |
| `github.pull_request.review_requested.v1` | A review requested from an agent's login |
| `github.pull_request.synchronize.v1` | New commits pushed to a pull request |

When checks are on, the head commit (`head_sha`) is pinned on the body before
it is published. A bot's delivery publishes nothing, except `synchronize`,
whose id is `github:commit:<repo>:<number>:<sha>` so a redelivery joins its run.

## Who a comment asks

A comment, a review, a review comment, or a new issue or pull request that
names an agent is published with every agent it names in `Mentions`. One that
names no agent but mentions the app's own login (`Config.Login`, such as
`humagen[bot]`, mentioned as `@humagen`) is addressed to the app, the way
Slack delivers only messages that mention the app, and is published naming no
agent. The app's mention is taken out of the text in both cases. Anything else
publishes nothing, so a busy repository wakes no agent on every comment, and a
bot's comment publishes nothing whatever it mentions.

The connector chooses no agent. The channel's members decide who answers
(`pkg/membership`, `pkg/dispatch`). A login in `Bots` names its agent wherever
it is mentioned, the app's own too, so when `Bots` maps the app's login,
`@humagen what changed?` mentions that agent.

## What a comment refers to

Besides the agents in `Mentions`, a person's text (a comment, a review, a
review comment, a new issue or pull request, or the description a review
request reads) is published with what it refers to in `Request.References`,
in the order written and once each:

| Written | Reference |
|---------|-----------|
| `@docs-bot`, a login `Bots` or `Agents` names | `agent`, id the login as written |
| `@ada`, any other login | `person`, id `ada` |
| `@humagen`, the app's own login | none |
| `#12` | `place`, address `github://<owner>/<repo>/issues/12` in this repository |
| `acme/web#12` | `place`, address `github://acme/web/issues/12` |
| `https://github.com/acme/web/pull/12/files#r5` | `link`, the URL, address `github://acme/web/pull/12` |
| a file or commit permalink | `link`, the URL with its line range, no address |

GitHub serves a pull request at its issue number too, and a short reference
does not say which it is, so `#12` is addressed as an issue. A team mention
(`@acme/reviewers`), an email address and anything inside code is not a
reference. A pasted Slack permalink is addressed as its thread when
`Config.Links.SlackTeams` names its domain, and `Config.Links.Address` reads
the host's own URLs. Automation text is the host's and refers to nothing.

## Where the answer lands

The GitHub sink subscribes to `github://` channels with the run types, so a run
started by a comment is answered on the same issue or pull request: one comment
per run, edited as it streams, and a check run when checks are on. A run
started elsewhere reaches GitHub only through a subscriber that names its
channel.
