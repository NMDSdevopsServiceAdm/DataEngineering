---
name: sprint-review
description: Use when preparing a "what did we ship" summary for sprint review — phrases like "sprint review summary", "what did we achieve this sprint", "prep sprint review", "/sprint-review". Gathers merged PRs to main in a date range, cross-references matching CHANGELOG.md entries, and drafts a one-bullet-per-roadmap-item executive summary (full per-item detail available on request).
---

# Summarising a sprint for sprint review

Turns merged PRs + `CHANGELOG.md` into a sprint-review narrative for user-named, user-ordered
roadmap items, built in three passes: round 1 clusters same-feature tickets, round 2 turns
clusters into a stakeholder-facing narrative per roadmap item (with its own subheadings), round 3
compresses that into one executive bullet per item. **The default output is round 3 only** — a
flat `## Summary` list, one bullet per item, no subheadings — not round 2's subheaded detail.
Rounds 1-2 stay in working memory for follow-up requests, not re-derived.

## 1. Sprint date range

Ask for the start date in chat. End date defaults to today. Canonicalize both to `YYYY-MM-DD`
before building the `gh --search` query in step 2 — that's the format `merged:<start>..<end>`
needs.

## 2. Gather merged PRs

```
gh pr list --state merged --base main --search "merged:<start>..<end>" --json number,title,mergedAt,headRefName,url --limit 200
```

Ticket number comes from `headRefName` (`<ticket>-<slug>`) — trust the branch over the title if
they disagree (titles are free-form and can be stale). Two PRs sharing a ticket (revert/redo):
keep both, let round 1 merge them into one cluster.

If `headRefName` doesn't match the `<ticket>-<slug>` pattern at all, don't drop the PR — carry it
through as its own single-PR cluster, keyed by PR number instead of ticket number, and let rounds
2-3 treat it like any other cluster. Flag it in step 9 as having no parseable ticket number.

Drop pure housekeeping PRs (changelog/tag-cut, no functional change) entirely — not even Other.

## 3. Cross-reference CHANGELOG.md

Check `## [Unreleased]`, plus any dated release section the sprint range overlaps.

PRs are the unit being summarised; CHANGELOG only enriches wording, never introduces its own
entries — a bullet with no matching PR is dropped. Match by content/judgement, not string
matching; not every PR has a bullet (fall back to the PR title in round 1).

- **Contradiction** (bullet says the opposite of the PR title, not just different wording): trust
  the title, flag it in step 9.
- **Too-thin title** to judge confidently: still make the best match, flag it as low-confidence.
- **Multi-PR saga bullet** (one bullet edited across several PRs, per CLAUDE.md's changelog
  convention): split its content across every cluster it actually supports.

## 4. Roadmap items and order

One question: what items, and in what order? Use what's given; default order = order given.
Always append **"Other"** automatically, always last — never ask the user to define it.

## 5. Round 1 — cluster and synthesise

Before roadmap items exist. Group tickets working on the *same feature* — a split PR, a fix
following its own feature, matching implementation+validation. Tickets that merely share a
pipeline or technique are NOT one cluster; stay conservative — bundling similar-but-distinct work
is round 2's job.

One synthesised sentence per cluster, from the matched CHANGELOG bullet (fall back to PR title).
Can stay technical — this isn't audience-facing yet. Keep ticket number(s)/PR link(s) attached for
round 2.

## 6. Round 2 — assign to roadmap items, write the narrative

**Assign** each cluster by subject-matter domain, not output destination (SLV-domain work stays
under SLV even if it writes into a job role estimates table). Fix-shaped item vs. domain-shaped
item competing for the same cluster: would the issue have broken `main` (crash/outage/won't run)
if left unfixed? → fix item. An improvement to something already working (tightened check, wider
coverage) → domain item, even if titled "fix". Nothing fits clearly → Other.

**Within each item**, in step-4 order ("Other" last):

- **Deliverables**: combine 2-3 clusters/line, lightly, only when they read as one coherent step.
- **Fixes**: own category, bundled and phrased as "fixed X, Y" — never like a shipped feature.
- **Technical work** (refactors, CI, infra, perf/memory): bundle densely, ~5 clusters/line — this
  is where round 1's deliberately-narrow clusters get compressed. Split into a second line beyond
  the cap rather than one catch-all.
- **Milestone exception**: for items tracking a series of milestones (e.g. "Polars migration"),
  each *milestone* cluster gets its own line even past the cap — but per-cluster, not whole-item:
  supporting technical clusters under the same item still bundle normally.
- **Synthesise, don't concatenate**: one sentence for what a combined line achieves together —
  never "and"-join separate cluster descriptions.
  > Bad: "Limited the rolling ratio to 2yr extrapolation / 5yr interpolation, and added a
  > validation that ratios sum to 1 (1869, 1876)." Good: "Capped how long a workplace's historic
  > split carries forward before refresh, and added a check that percentages always add up to
  > 100% (1869, 1876)."
- **Plain language**: what changed and why it matters, not the mechanism — no function/class
  names, library internals, or jargon ("refactored", "broadcast", ".over()", "pointblank").
- **Flag** if several Other entries look like a broader version of a narrowly-named item.

Each entry: ticket number(s), the synthesised description, PR link(s).

## 7. Round 3 — executive summary

From round 2's narrative (not raw clusters, not individual entries). One bullet per roadmap item,
including "Other", in step-4 order. No ticket/PR references. Real compression, not a shorter
rehash — and must cover the *whole* narrative, not just its most prominent thread: check each
bullet back against every round-2 line under that item before finalising, revise if anything's
uncovered. "Other" stays generic — doesn't enumerate its contents.

> Round 2 (5 lines under "Polars migration"): migrated ONS/ASCWDS/PIR ingest; finished migrating
> job role estimates; removed the old PySpark versions now their replacements are live.
> Round 3: "Continued moving our data pipelines onto the newer platform — most of the major ones
> are now migrated, with legacy versions retired as each one lands."

This `## Summary` is the entire final output by default — round 2's per-item breakdown is not
appended below it. Round 1/round 2 stay in working memory for follow-up detail requests, not
discarded, just not shown unless asked.

## 8. Output format

Ask: chat reply only, or also saved to a file? If saved, write the round 3 `## Summary` to
`sprint-review-<start>-to-<end>.md` at the worktree root — untracked, theirs to keep/paste/delete.
Only add round 2 detail to that file if specifically asked.

## 9. Report back

Present round 3's `## Summary` only. Mention that full detail is available per roadmap item on
request — already synthesised in round 2, no need to re-gather or re-cluster.

Flag explicitly rather than silently dropping: anything under "Other" that seems surprising, a
merged PR with no parseable ticket number, a PR title that contradicted or was too thin to
confirm its matched CHANGELOG bullet (step 3), or an Other-vs-named-item scope gap (step 6).

Treat the first draft as a draft: revise and re-show on pushback about clustering, assignment,
ordering, the executive summary, or wording — don't treat one pass as final.
