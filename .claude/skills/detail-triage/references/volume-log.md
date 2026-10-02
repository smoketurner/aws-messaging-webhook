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
