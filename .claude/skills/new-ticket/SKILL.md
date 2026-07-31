---
name: new-ticket
description: Use when starting a new piece of work — "/new-ticket", "start a new ticket", "let's start on card #1234". Runs branch check → short interview → hands off to Plan Mode for the SPEC. Pairs with the `commit-push` and `open-pr` skills for the rest of the cycle.
---

# Starting a new ticket

## 1. Branch check

- `git status` must be clean before switching branches — if not, stop and flag it rather than discarding anything.
- Fetch and pull the latest `main`. If not currently on `main`, confirm before switching.

## 2. Interview

Ask two short questions, one at a time (see CLAUDE.md's Output style):

1. Trello card number?
2. One-line description of the work?

Don't try to gather full requirements here — that's what Plan Mode does next.

## 3. Derive the branch name

`<trello-number>-<short-description>`, slug hyphenated and truncated to fit the repo's 16-character limit (see "Environment & workflow" in CLAUDE.md). Create and check out the branch from latest `main`.

## 4. Hand off to Plan Mode for the SPEC

Call `EnterPlanMode` and let Claude Code's normal phased workflow (Explore → Design → Review → Final Plan → `ExitPlanMode`) do the deeper requirements interview and produce the plan. Don't build a separate SPEC format — the plan file is the SPEC.

## 5. After the plan is approved

Implement, then use `commit-push` (repeatedly, per logical change) and `open-pr` (once, at the end) to finish the cycle. There's no automatic memory save at any point — only save something to memory if it looks genuinely useful, and ask first.
