# Native GitHub feedback

`github.Config.Feedback` resolves only native handles the host recorded after
delivery. `Record` owns durable observation deduplication. Optional `Withdraw`
receives both the removal identity and original reaction identity; it must
remove only that observation's current assessment, preserving newer feedback.

`PollFeedback` advances one persisted page per call, bounded to 100 pages of
100 reactions and 10,000 tracked identities. Additions are applied in ID order
after a complete scan. A removal needs two identical complete snapshots.
Failed or partial scans never remove feedback. `DeliveryState` keeps scan
progress and known identities across connector reconstruction. A nil store
keeps this state only in process. The host controls poll scheduling and age.
Offset pagination is not a transactional snapshot of a changing collection.

Native review delivery also persists an exact suggestion's reviewed commit,
path, line range, replacement and finding identity. Its indexing runs after the
delivery transaction; a durable pending flag retries failed indexing on another
replica without posting the native review again. `SuggestionCommitAPI`
and `FileAPI` permit optional verification after a push. The accepted case is
deliberately narrow: current head has exactly one parent, that parent is the
reviewed SHA, a diff patch names the target file, and complete before/after
file contents equal exactly the suggested replacement. The current head is
rechecked before recording `suggestion_committed`. The optional `Evidence`
callback receives a system observation with the immutable commit SHA. Commit
author and committer metadata are not trusted as human identity. Hosts must
persist this separately from personal ratings and evaluation candidates.

This records exact committed text, not a claim that GitHub's suggestion UI was
used, the finding was semantically correct, or the reviewer was approved.
Merges, non-adjacent commits, extra edits in the same file, incomplete files or
diffs, and moving heads remain unverified. A later revert does not erase the
historical verified commit. Outdated/resolved comment flags are not substitutes
for commit evidence. The commit query reads only its first bounded file page;
a target omitted from that page remains unverified.

`FixPullContext` is an optional host callback supplying canonical source-session
and alert URLs after authorization. These fields are excluded from action
arguments and native API JSON. They participate in fix-request identity and are
added to the draft PR alongside the independently verified source issue/PR.

API contracts checked against GitHub's official [reaction endpoints](https://docs.github.com/en/rest/reactions/reactions),
[commit endpoint](https://docs.github.com/en/rest/commits/commits#get-a-commit),
and [review-thread fields](https://docs.github.com/en/graphql/reference/pulls#pullrequestreviewthread).

Local race coverage includes snapshot pagination/failure/restart, removal replay,
exact and rejected commit evidence, and trusted backlink injection. Live GitHub
delivery was not exercised by these tests.
