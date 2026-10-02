---
name: detail-triage
description: Triage a batch of Detail bug issues and fix PRs — measure the trend, classify each finding as an instance or a class, review each fix for defects in the fix itself, and decide what merges versus what gets fixed as a class with a guardrail. Use when asked to "triage the Detail batch", "Detail opened a batch", "why is Detail volume rising", "classify Detail findings", or when a new batch of `[Detail Bug]` issues appears.
---

# Triage a Detail Batch

Detail scans the whole repository on each pass and files one issue per finding,
usually with a paired fix PR. Merging each PR on its own closes the instance and
leaves the pattern in place for the next pass to rediscover somewhere else. This
skill exists to break that loop: measure what is actually happening, separate
instances from classes, and make sure every class fix carries something that
prevents the class from coming back.

Work through the steps in order. Step 2 gates everything after it — nothing
merges before the classification table exists.

## Step 0: Know what you may not edit

`.claude/skills/detail-rules/` and `.claude/skills/detail-create-rules/` are
managed on the Detail side and synced down via `detail rules pull`. **Never
write a rule and never edit these files by hand.** Detail scans with its own
copy of each rule, so a local edit or a hand-written rule file is invisible to
the scanner, and the next sync overwrites it anyway.

The only way to get a rule is to ask Detail to generate one through the Detail
CLI, then sync the result into this repository — see step 5.

Everything under `.claude/skills/detail-triage/` is repo-owned and yours to
maintain.

## Step 1: Measure the batch and the trend

```bash
python3 .claude/skills/detail-triage/scripts/detail-stats.py
python3 .claude/skills/detail-triage/scripts/detail-stats.py --json   # for scripting
```

The script reads every Detail-authored issue from the GitHub REST API, extracts
the "Introduced in [#N] … on DATE" attribution from each body, and reports
volume, bug age, and keyword-clustered classes per detection month. It needs no
`gh` CLI: the token comes from `GH_TOKEN` or `GITHUB_TOKEN` (set in cloud
sessions), falling back to `gh auth token`, and the repository comes from
`GH_REPO` or the `origin` remote.

Read three things together:

- **median / p90 age** — how far back into history this pass reached.
- **`<30d`** — findings against code merged in the last month.
- **`Detail PR` and `PR<=3d`** (per-batch table) — findings attributed to one of
  Detail's own fix PRs, and to any PR merged at most three days before
  detection, whoever wrote it. The same table counts Detail's fix PRs and its
  Dead Code PRs, which file no issue.

A climbing median with a flat `<30d` means Detail is working through backlog:
volume is high for a reason that no process change will fix, and the right
response is throughput. A climbing `<30d` means new code is generating findings
as fast as old code is cleaned up, and the right response is a guardrail. Say
which of the two you are looking at before proposing any remedy.

`PR<=3d` is the one that decides how this batch gets merged. When it is
high, merging fix PRs as-is is feeding the next scan, and the loop only breaks
by making class decisions *before* merging. Part of the signal is mechanical —
as Detail's merged PR count grows, its commits are increasingly the last to
touch any given line — so confirm a spike by checking whether the finding is a
defect in the logic the fix *added* rather than merely in a file it touched.
Watch the human-authored case as closely as Detail's: a class fix we merged the
day before can account for every finding in a batch while the Detail-only
column reads zero.

The class table is **keyword clustering over titles — directional, not
rigorous**. Titles overlap classes and some match none. Use it to spot
recurrence worth investigating; never quote its counts as fact. When a new
recurring pattern shows up in the unmatched titles, add it to `CLASSES` in the
script.

Compare against `references/volume-log.md`, which holds the measured history;
its batch table is the per-batch series to extend.

## Step 2: Classify every open finding

For each open issue in the batch, establish three things and put them in one
table:

| column | what it means |
|---|---|
| class | which recurring pattern it belongs to, or "isolated" |
| live? | is the defect still present in current `main`? |
| siblings | other call sites sharing the pattern, found by reading the code |

**Verify "live?" against the tree, never against the issue body.** Detail
authors a batch from a snapshot, and that snapshot can predate a merge from the
same day. A finding can be real while its quoted code and line numbers are
stale, and a reviewer trusting the body reaches the wrong conclusion in either
direction.

**Match against stated residue before hunting.** Read the residue and
open-decision sections of the last few records in `.local/`. A finding that
matches something a review already accepted is a residue recurrence, not a
surprise. Count it in the batch table. The lever for a residue recurrence is
fixing residue in the PR that left it, not a new guardrail.

Finding the siblings is the actual work of this step, and it is what turns a
list of instances into a class. Prefer `codegraph_explore` or `ast-grep` over
ripgrep here: the question is structural ("every header value stored without a
length cap"), not textual. This repository's history has clear examples:
oversized `Message-ID` (#130) and oversized `References` (#126) were one class
of uncapped header values reaching a size-limited DynamoDB item, and outbox S3
objects leaking on failed sends (#78, #81, #102, #127) were one class of
cleanup missing from error paths.

**The sibling hunt disqualifies classes as often as it confirms them, and that
is a result worth having.** Exclude test-only sites — anything in
`crates/webhook-test-support` or under `tests/` — and judge each sibling's
consequence rather than counting matches. A site that fails in the safe
direction, or only affects a log line, is not a sibling worth fixing. A class of
three where two members are harmless is not a class. Say so, and merge the
instance.

Issues Detail filed **without** a paired fix PR belong in this table too. They
are usually the ones it judged too structural to auto-fix, which makes them the
strongest class candidates in the batch, not the weakest.

### Dead Code PRs

Detail also opens Dead Code PRs (branch `detail/dead-code/…`) that file no
issue. CI runs `cargo clippy --all-targets --all-features`, so the compiler
proves most removals: a function, impl, or constant that still had a caller
would not build. Review the removals the compiler cannot see:

- **A field on a persisted or wire-format struct** — a DynamoDB item in
  `store.rs` or the mail store, an outbox send spec in S3, or an EventBridge
  detail. The webhook and sender functions read rows, stream records, and
  specs written by the previous release, and EventBridge consumers outside
  this repository read every detail we publish (`docs/events.md`,
  `docs/mailbox/events.md`, pinned by `tests/mailbox_event_schemas.rs`). A
  field no code here reads is still a contract. Keep it with
  `#[serde(default)]` and remove it a release later.
- **A `template.yaml` parameter, env var, or IAM statement** — `config.rs`
  reads the env vars, but nothing compiles the template. Check `sam validate
  --lint` and every reader before accepting.
- **Unreachable branches kept on purpose** — the `DomainEvent::Unknown`
  pass-through and the 4xx/5xx retry-protocol arms are deliberate. Check
  `CLAUDE.md` "Invariants to preserve" before accepting their deletion.

## Step 3: Decide instance versus class

**A Detail fix PR is a reviewed draft, never a merge candidate.** Green CI, a
detailed PR body, and a full test suite make a PR look finished; step 6 exists
because a fix can be actively wrong under exactly that appearance.

The policy is not a claim that Detail writes bad fixes. The point is that
post-merge blame understates the defect rate, because the scanner does not
re-find everything it introduces. Review is what closes that gap, and it is
worth doing regardless of who wrote the fix. `detail-stats.py --fix-defect-rate
YYYY-MM-DD` measures how often merged PRs are later blamed by a finding, Detail
against human.

Three or more open instances of one pattern makes this decision mandatory, not
optional. For each class pick one and record which:

- **Instance** — genuinely isolated. Merge Detail's PR once it has been through
  step 6 and is green. No surrounding refactor.
- **Class** — one change that fixes every site found in step 2. Close Detail's
  individual PRs as superseded rather than merging them, and say so in each.

**Never merge the instance PR for a finding that belongs to a class.** Merging
it closes the instance and erases the evidence that the class exists, so the
next pass rediscovers the same pattern at a different call site and the cycle
repeats.

**Settle contradictory findings together.** A batch can hold findings that pull
in opposite directions, or a fix that reverses a decision already on record —
most often here, a fix that moves a failure from one side of the
transient/permanent split to the other. Resolve the governing AWS documentation
or recorded decision once — memory, `docs/design/`, the PR that made the
decision — then dispose of the whole set against it.

A class fix that lands without a guardrail will regress, so step 4 is part of
the same PR, not a follow-up.

## Step 4: Attach a guardrail to every class fix

Pick the strongest mechanism that fits, and be honest about what it does not
cover.

1. **A type whose invalid state cannot be constructed.** The strongest option:
   the wrong thing stops compiling. Best fit whenever the class is "compared or
   stored a value that was never normalized" or "stored a value that was never
   capped". A newtype only guards if its field is private: `InboxId`
   (`crates/webhook/src/mail/mod.rs`) canonicalizes in its constructor but
   exposes `pub String`, so any caller can still build one that skips it.
2. **A clippy `disallowed_methods` or `disallowed_types` entry** in
   `clippy.toml`. Cheap and real — `-D warnings` turns it into a CI failure —
   but it matches *a named function or type*, nothing more.
3. **A behavioral test** asserting the invariant across every site, driven
   through the real router against the `webhook-test-support` fakes. Never a
   test that scans source text.

**A guardrail can under-cover its class.** A lint on a constructor cannot see a
value that was built correctly and then truncated or compared at the wrong
precision. When you pick a mechanism, write down the part of the class it does
not cover, and either add a second mechanism or state the residue in the PR.

Verify the guardrail the way the project verifies tests: break the code, confirm
CI catches it, then fix it. A guardrail never observed failing is not known to
work.

**A class fix produces the next batch's findings.** Before merging a class fix,
hunt siblings of the new type or guard: every consumer of the old value it
replaces, and every caller it did not touch.

Look past Rust handlers. When a fix adds an AWS call, check the function's IAM
policy in `template.yaml` — #81 was a cleanup path that could never succeed
because the role lacked the delete permission. When a fix changes what a stored
record or published event means, check every reader: the `/v0` API responses
(`docs/mailbox/api.md`), the EventBridge detail schemas, and `docs/`.

## Step 5: Request a Detail rule for each confirmed class

This is the step that reduces future volume. A class the scanner knows about is
reported as a rule violation on the way in, rather than rediscovered instance by
instance for months.

**Request the rule; never write one.** `detail rules create` submits a request
and *Detail* generates the rule text. Hand-editing a rule file under
`.claude/skills/detail-rules/references/` is the thing that is forbidden — those
are generated artifacts and the next sync overwrites local edits.

Use the `detail-create-rules` skill once it has been synced. The evidence is
already in the issues: `detail-stats.py --json` emits an `open_bug_ids` map from
issue number to the `bug_<uuid>` in its body.

```bash
detail rules create --description "<the invariant, stated as a rule>" \
                    --bug-ids <bug_id1,bug_id2,...>
```

Then poll with `detail rules requests show <rcr_...>`, review each result with
`detail rules show <rule_id>`, pull with `detail rules pull <rule_id>`, and
commit the synced files as `chore(detail): …`.

**Read the generated rule against the merged tree before pulling it, and read
its correct-pattern section, not only its detection section.** Detail generates
from the bug-report snapshot, which predates the batch's own fixes, so a rule
can hold up as "already correct" the exact code the batch just replaced. That
is worse than no rule: it tells the next reviewer the defect is the model.

The remedy is a fresh `detail rules create` whose description names the stale
rule and spells out what it got wrong. Hand-editing the pulled file is
forbidden (step 0) and the next sync would overwrite it, and the CLI has
`create`, `pull`, `show`, `list` and `propose` — no refine verb.

A good request describes an invariant, not an incident. "Every header value persisted
from an inbound message is capped before it reaches the DynamoDB item, because
an uncapped value makes the whole item unwritable" gives Detail something to
generate a rule from; "issue #130 was fixed wrong" does not.

When a rule for the class **already exists and did not catch it**, say so in the
description and ask for refinement rather than adding a second overlapping rule.
More overlapping coverage is rarely the lever.

The `detail-rules` skill is wired into no CI job, git hook, or Makefile target —
it only runs when someone asks, so a rule that exists still does not gate a
merge. If the goal is to stop a class at the door, changing that wiring is a
separate decision to raise with the user rather than assume.

## Step 6: Review each fix for defects in the fix itself

This is the step that earns the triage. Do not treat it as a merge checklist.

### Scope the reading first

Most of a Detail PR is tests. Split production from test additions so the batch
is tractable:

```bash
python3 .claude/skills/detail-triage/scripts/detail-stats.py --pr-diff-sizes 125-139
```

Read the production hunks. Read tests only to check that the assertion pins the
behavior the issue describes. Before reporting a test as missing, search the
PR's test files for it: a diff filtered to production paths hides them.

The split is by file path, so a PR reporting zero test lines has inline
`#[cfg(test)] mod tests` in the production file rather than no tests — treat
its production figure as an upper bound.

### The failure-mode checklist

Interrogate what the fix introduces, not just whether it addresses the report:

- **A new error path** — which side of the retry protocol does it land on? A
  4xx or permanent `ActionErrorKind` drops the message for good; a 5xx or
  transient one recruits SNS or stream redelivery. Classification belongs in
  `error.rs`, not at the call site. #96 decoded a retryable condition failure
  as permanent; #134 dropped labels on a DynamoDB throttle.
- **A new retry or redelivery path** — does each attempt re-read state, or
  capture once outside the loop? Is the action it repeats still repeat-safe?
  Is there a cap, and does the sweep honor it (#79)?
- **A new cap or bound** — what happens at exactly zero, at equality, and at
  overflow? (#91 was a TTL that wrapped for large retention values.)
- **A new write to a field another path also writes** — do the two paths share
  an invariant, and does the new one carry it? SNS delivers out of order, so a
  status write needs a guard against older events overwriting newer ones
  (#135).
- **A new guard** — does it cover every call site, or only the reported one?
  Search the codebase for the pattern, do not trust the diff's coverage.
- **A new branch or match arm** — the inverse of the guard question: does
  anything *upstream* stop it being reached? A fix that adds an arm to an
  existing match inherits every early return above it. Read the enclosing
  function from its top, not from the diff hunk.
- **A new normalizer or parser** — does it alter input it should leave alone,
  and does every path that compares against the result apply the same
  normalization (#82: ingest lowercased inbox ids, the read API did not)?
- **A new `consistent_read(true)`** — does it meet the bar in `CLAUDE.md`, with
  the reason written at the call?
- **A new resource on a failure path** — is it cleaned up on every error exit,
  and does the role have permission to clean it up?
- **A changed serde shape** — the deploy-compatibility constraint under Dead
  Code PRs (step 2) applies to any field a fix removes, renames, or tightens.

### Check the fix against the class it fixes

When the bug is a parsing, normalization, or canonicalization defect, the fix is
written in the same idiom that produced it and tends to inherit the same blind
spot. Test the fix against the governing spec's own examples — the RFC for a
mail header, the AWS-documented payload for an event — and against neighbouring
inputs in the same class.

A test whose input is derived from the code's own output cannot find a
disagreement with the real producer. When a function exists to accept external
input, require at least one fixture captured from the external producer: the
verbatim AWS examples in `crates/webhook/tests/fixtures/`, or a raw message as
SES actually delivers it.

### Reproduce, do not argue

For a suspected defect in a pure function, extract the function body into a
file in the session's scratchpad directory and run it. It takes a minute and converts "this looks wrong" into
a confirmed blocker with output to paste into the review:

```bash
rustc -O -o <scratch>/t <scratch>/t.rs && <scratch>/t   # the PR's function body, verbatim
```

A review comment saying "I think this mishandles Unicode" invites debate. One
showing the wrong output does not.

### Then the hygiene checks

- Confirm any AWS behavior the PR body asserts against the AWS documentation
  and quote it; never accept the PR body's paraphrase. Check the quote's scope
  as well as its strength.
- When trimming narrative comments, keep every spec or documentation citation.
  Cut the history, not the requirement; fix an imprecise citation rather than
  deleting it.
- Confirm new downstream calls go through the `Services` trait (`state.rs`) and
  tests use the `webhook-test-support` fakes, not ad-hoc stubs.
- Confirm no single-caller helpers were added.

### Merge mechanics

`main` uses a merge queue and requires signed commits.

Read CI status fresh with `gh pr checks <n>` at the moment you decide. Do not
reuse a run ID captured earlier in the session — runs get superseded, and a
stale failing run can hide a passing one.

Before enqueueing, check that every commit is signed. A follow-up commit Detail
pushes after its CI fails (a rustfmt fix, say) can arrive unsigned, and the
merge queue rejects the whole branch:

```bash
gh api 'repos/{owner}/{repo}/pulls/<n>/commits' \
  --jq '.[] | "\(.sha[0:8]) \(.commit.verification.verified)"'
```

Rebuild such a branch with signed commits rather than rewriting Detail's.

A queued PR's branch rejects pushes ("protected branch hook declined").
A push that lands while auto-merge is armed but before the PR enters the
queue succeeds and quietly drops it; re-arm `gh pr merge <n> --auto` after
every push. To change one, dequeue it first with the GraphQL
`dequeuePullRequest` mutation, push, then run `gh pr merge <n> --auto` with no
strategy flag: it enters the queue when its checks pass, so nothing has to
watch CI. If a green PR with auto-merge armed still has no queue entry, enqueue
it with the GraphQL `enqueuePullRequest` mutation. When a wait is unavoidable,
background `gh pr checks <n> --watch` rather than sleeping.

After the batch lands, re-run `make lint` and `make test` on `main` — branches
touching disjoint files can still break each other.

## Step 7: Record the metric

Write the batch record to `.local/detail-triage-<YYYY-MM-DD>.md`: the
classification table, the review findings, and the decisions. `.local/` is
gitignored working memory — **read the most recent record before starting**.

Give the record a **Residue** section listing everything the review accepted and
did not fix, each with the finding it would become. The next pass matches new
findings against it (step 2).

A gap the review notices is residue only after the user chooses to leave it.
"Not a regression" or "already true before this PR" is an observation, not a
decision: a gap waved through as pre-existing comes back as the next batch's
finding. Put such a gap on the decision list, and follow what it admits to
where that artifact is consumed before sizing it.

Then extend `references/volume-log.md`: add a row to the batch table, with the
counts from `detail-stats.py` and the judgement columns defined above the table,
and append a section with the monthly row, the classes, and what was decided.
The next run reads both to tell a trend from a blip.

Every few batches, read all the records in `.local/` together and fold any
lesson that has recurred into this skill. Otherwise the records are read one at
a time, and a lesson that lives only in a record is not applied.

Success is **the `<30d` count and the per-class counts falling** over successive
batches. It is not an empty issue list — while Detail is still excavating
backlog, a high total is expected and says nothing about code quality going in.
