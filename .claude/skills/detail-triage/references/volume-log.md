# Detail Volume Log

Measured history of Detail's findings, so a later run can tell a trend from a
blip. Add a row to the batch table and append one section per triage pass.
Counted figures come from `scripts/detail-stats.py`; do not hand-edit them. The
judgement columns come from the batch's record in `.local/`.

## Batch table

| column | source | meaning |
|---|---|---|
| issues, fix PRs, dead-code PRs | script | what Detail opened on the detection date |
| Detail PR | script | findings blamed on one of Detail's own PRs |
| PR≤3d | script | findings blamed on any PR merged at most 3 days before detection, ours included |
| not as written | record | PRs that needed changes, were superseded, or were closed after review |
| dispositions | record | what happened to the batch's PRs |
| residue | record | findings that match residue or an open decision in an earlier record |
| rules | record | Detail rules requested for the batch's classes, and whether synced |

| batch | issues | fix PRs | dead-code PRs | Detail PR | PR≤3d | not as written | dispositions | residue | rules |
|---|---|---|---|---|---|---|---|---|---|
| 2026-09-22 | 5 | 6 | 0 | 0 | 0 | not recorded | not recorded | not recorded | 0 |
| 2026-09-23 | 15 | 16 | 0 | 0 | 0 | not recorded | not recorded | not recorded | 0 |
| 2026-09-29 | 2 | 2 | 0 | 1 | 0 | not recorded | not recorded | not recorded | 0 |
| 2026-09-30 | 5 | 5 | 0 | 1 | 1 | not recorded | not recorded | not recorded | 0 |
| 2026-10-07 | 1 | 1 | 0 | 0 | 0 | 1 (#145 superseded) | #145 closed; class fix #146 | 0 (no prior record) | 0 requested (CLI unavailable) |
| 2026-10-14 | 2 | 2 | 0 | 0 | 0 | 2 (#152, #153 superseded) | #152 → #155; #153 → #154, then #156 | 1 (#151 continues the 10-07 address class) | 2 requested: rule_a7516916 not pulled (generated before #156 merged, stale on whitespace); refinement rule_22417fef synced |
| 2026-10-21 | 2 | 2 | 0 | 0 | 0 | 1 (#170 superseded) | #170 → #173 (class + clippy guardrail); #171 instance, pending merge after advisory fix #172 | 1 (#169 continues the 10-07 participant class) | 0 requested (CLI unavailable) |

Batches before 2026-09-22 were single findings (2026-08-12, 08-18, 09-08,
09-15) or fix PRs with no new issue; run the script for their counts. No
batch has a triage record yet, so the judgement columns start with the next
one.

## 2026-10-02 — baseline

32 issues, 2026-08-12 through 2026-09-30. No Detail rules exist for this
repository yet.

| month | n | no attr | median age | p90 age | <30d | >90d |
|-------|---|---------|-----------|---------|------|------|
| 2026-08 | 2 | 1 | 6 | 6 | 1 | 0 |
| 2026-09 | 30 | 2 | 7 | 14 | 26 | 0 |

Findings are young: a median age of a week and 26 of 30 September findings
against code under 30 days old. The repository is young too, so this is not
yet evidence for either backlog excavation or a regression trend; the next few
batches set the direction.

## 2026-10-07 — first triaged batch

| month | n | no attr | median age | p90 age | <30d | >90d |
|-------|---|---------|-----------|---------|------|------|
| 2026-10 | 1 | 0 | 21 | 21 | 1 | 0 |

One finding (#144, in code 21 days old) that turned out to be one member of a class: inbound
participant strings that don't round-trip through mail-parser on the reply path. Reproduction
found five more live members (unquoted specials → 400 or a misdirected reply, encoded control
characters and quotes, reply_all self-CC). Detail's fix #145 kept the address but still wrote
names unquoted, so it failed on a capped name containing a comma. Closed as superseded by class
fix #146, which has a behavioral round-trip guardrail. No Detail rule yet: the CLI wasn't
available in the session. One finding against a month-old repository isn't a trend in either
direction.

## 2026-10-14 — envelope addresses and reply recipients

| month | n | no attr | median age | p90 age | <30d | >90d |
|-------|---|---------|-----------|---------|------|------|
| 2026-10 | 3 | 0 | 26 | 28 | 3 | 0 |

Two findings, both in code 26–28 days old. Neither was blamed on a Detail PR or
a PR merged within three days. Volume is flat at one or two a week, which is
still too little to call backlog or regression.

- **#150 (isolated):** a reply to a CC-only or BCC-only sent message got a 400.
  Detail's #152 left BCC-only broken, merged reply-all's To/CC split into To, and
  carried an unsigned commit. It was superseded by #155: fall back from To to CC
  to BCC, with BCC kept as bcc.
- **#151 (class, continues 10-07's #144):** an address SES refuses after a 200.
  Detail's #153 was right about mail-parser mangling addresses, but its test
  pinned `üser@éxample.com` as valid, and SESv2 refuses that. #154 added SES's
  ASCII and Punycode rules. #156 then replaced the mail-parser reduction with
  structured-email-address 0.0.21, checked against the RFC 5321 text (now in
  `specs/`). Probing found a further gap: RFC 5322 quoted strings allow a tab,
  which RFC 5321's envelope grammar does not.
- `main` failed `cargo deny` on four rustls advisories; the bump to 0.23.46 went
  in with #154.

Lesson: the fix PR's own tests can pin the defect as correct. #153 asserted a
value SES documents as refused. Read a fix's accept-list against the governing
spec, not only its reject-list.

The first rule request was generated before #156 merged. It held up the
then-unmerged validator as the correct pattern and treated
`ada@ example.com` as input to reject, which RFC 5322 §3.2.3 allows. A second
request, made after the merge and naming the stale rule, produced
`rule_22417fef`. Its line references matched main, and it was the one pulled.
Request rules only after the class fix is on main.

## 2026-10-21 — inbound body parts and reply-all identity

| month | n | no attr | median age | p90 age | <30d | >90d |
|-------|---|---------|-----------|---------|------|------|
| 2026-10 | 5 | 0 | 28 | 35 | 3 | 0 |

Two findings, both in #42 code that is 35 days old, and neither recently merged. October's median age is climbing (28 days)
while `<30d` stays flat. That's the start of the backlog-excavation signature, so throughput is the response here, not a
new process guardrail.

- **#168 (class):** mail-parser's `text_bodies()` lists inline non-text parts. Detail's #170 filtered them out of
  `content.text` but left `body_preview`, which reads only `text_bodies()[0]`. With an inline image before the text, the
  preview came back empty (reproduced on #170's branch). #173 superseded it: the inbound preview is now derived from the
  filtered bodies through `mime::preview`, shared with the outbound path, and `clippy.toml` disallows mail-parser's
  unfiltered body accessors. I checked the guardrail by breaking the code.
- **#169 (instance):** reply-all compared `direct` raw against stored participants. This is the member #146's `is_inbox`
  fix missed. The sibling hunt found only harmless members (in-list cc dedup, which validation repeats, and the
  display-only thread sets), so #171 merges as written.
- `main` was red on cargo deny (RUSTSEC-2026-0382/0383); fixed in #172.

Lesson: when a fix filters an iterator, also check every other reader of the *same* unfiltered source. Here
that was mail-parser's own `body_preview` convenience, which no diff hunk showed.
