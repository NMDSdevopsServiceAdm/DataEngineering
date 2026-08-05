---
name: continue-ticket
description: Use this skill whenever the user wants to pick back up on a ticket they (or someone else) already started in this DataEngineering repo — phrases like "continue ticket 1814", "let's get back to 1793", "where did I leave off", "pick this ticket back up", "resume work on the SLV reshape", or just opening a session in an existing ticket worktree and asking "what's the state of this?". It reconstructs context from git (branch/worktree, uncommitted diffs, commit history vs. main), GitHub (open PR, draft/review/CI status), the auto-memory system (any project memory already written for this ticket), and any working SPEC.md, checks the worktree actually has a usable Python environment, then produces a concise "where you left off" summary with a suggested next step. Always reach for this skill instead of just running a couple of `git log`/`git status` commands and guessing — the point is to pull together everything that's scattered across git, GitHub, memory, and SPEC.md before answering, not just whichever one is fastest to check.
---

# Continuing a ticket

This repo (Skills for Care Workforce Intelligence Team) tracks work as numbered
tickets, each developed on its own branch in its own git worktree (see the
`new-ticket` skill for how these get set up: sibling worktree directory
`DataEngineering-<ticket>-<slug>`, branch `<ticket>-<slug>`, a working
`SPEC.md` at the worktree root, a seeded project memory file). This skill is
the mirror image: reconstructing what's already true about a ticket instead of
setting one up.

The reason this needs its own pass rather than a quick `git log` is that the
real state of a ticket is scattered across four places that can each be stale
or incomplete on their own — the working tree, GitHub, the memory file
written at some earlier point, and a SPEC.md that may not reflect later
decisions. Cross-check them against each other rather than trusting whichever
one you read first.

## 1. Identify the ticket and its worktree

If the user gave a ticket number, find its worktree and branch:

```
git worktree list
```

Match by ticket-number prefix on the branch name (`1814-slv-reshp`,
`1793-empstat`, etc.) or by the sibling directory name
(`DataEngineering-<ticket>-<slug>`). If nothing matches, don't assume the
ticket doesn't exist — check for a merged-and-removed branch first
(`git branch -a --contains main` won't help here since it's gone; instead
try `git log --all --oneline --grep="<ticket>"` and `gh pr list --state all
--search "<ticket>"`) before telling the user it's not found.

If the user didn't give a number, infer it from the current directory:

```
git rev-parse --show-toplevel
git branch --show-current
```

The current worktree's branch name and directory name (per the convention
above) tell you the ticket number and slug directly. If the current directory
is the main repo checkout (branch `main`), there's no ticket to infer — ask
which one they mean rather than guessing.

If more than one ticket plausibly matches (e.g. the user says "the SLV one"
and there are two candidate branches), ask rather than picking — this skill's
whole job is avoiding wrong assumptions about state, so don't start by
guessing which ticket "state" even refers to.

## 2. Read the working tree

From inside the ticket's worktree:

```
git status
git log main..HEAD --oneline
git diff main...HEAD --stat
git diff
```

This tells you: anything uncommitted right now, what's been committed on this
branch that isn't on `main` yet, and the shape of that diff. Note explicitly
whether the working tree is clean or has pending changes — "uncommitted work
exists" is one of the most important facts in the summary, since it's the
thing most likely to get silently lost if the user starts a fresh session
elsewhere.

Don't stop at commit titles from `--oneline` — read the full body of the most
recent handful of commits (`git show --stat <sha>` or `git log -3 -p main..HEAD`
on the tail end). Investigation tickets in this repo often show up as a short
run of commits that read like a diagnostic log ("adds diagnostic pipeline",
"OOM'd again after the fix", "prototypes fallback in diag file only") — the
titles alone tell you *that* something happened across several commits, but
the bodies are where you find out whether the last one actually resolved it
or just tried something new. This is exactly the kind of detail that makes
step 4's memory reconciliation actually work instead of rubber-stamping
whatever the memory file already claims.

Check the worktree has a working Python environment before you rely on it. A
worktree that's never had `uv sync` run in it won't have a `.venv` yet:

```
uv run python -c "import polars, boto3"
```

`uv run` auto-syncs against `uv.lock` before running, so this single command both
checks and — if needed — provisions the environment (typically ~1 minute on a
brand-new worktree on this machine, mostly linking from uv's global package
cache rather than downloading). That's non-destructive and safe to just run, so
do it and mention it in the summary if it had to provision anything — no need to
ask first. This is the one setup action this skill should take on its own; it's
a precondition for reading state accurately, not a step toward implementing the
ticket.

Check for a working `SPEC.md` at the worktree root — if present, read it in
full. It's the closest thing to a design doc for the ticket and should cover
scope, key decisions, and what's explicitly out of scope. Per this repo's
convention, `SPEC.md` is a temporary file removed at merge time, so its
absence doesn't mean the ticket is undocumented — it may just mean the memory
file (step 4) is now the primary record, or the ticket never needed one.

## 3. Check GitHub for an open PR

```
gh pr view <branch> --json state,isDraft,statusCheckRollup,reviews,title,url
```

If a PR exists, note: draft vs. ready for review, CI status, any unresolved
review comments. If no PR exists yet, that's itself a fact worth surfacing —
per this repo's standing convention, PRs are opened as **drafts by default**,
so "no PR yet" plus "commits exist on the branch" often just means the next
step is opening one, not that something was forgotten.

If `gh` isn't available or the check fails, don't block on it — note in the
summary that PR status couldn't be checked rather than silently omitting it.

## 4. Check memory for prior context

Read `MEMORY.md` in the auto-memory directory shown in your system context,
and grep it (and the memory directory itself) for the ticket number and any
slug/keywords from the branch name. If a matching project memory file exists,
read it in full — it may contain decisions, known bugs, or "still open" notes
from a previous session that aren't visible in git or GitHub at all (e.g. "fix
is written but deliberately left uncommitted for review," or "diagnostic
pipeline still needs removing before merge").

Treat the memory file as a *claim about a past state*, not current truth —
it may predate commits you just read in step 2. Reconcile the two explicitly:
if memory says "uncommitted fix for X" and the working tree is now clean with
a new commit mentioning X, the fix has likely been committed since; if memory
says "OOM fixed" but you can't find a corresponding commit or diff, flag that
mismatch rather than trusting either source blindly.

If you find memory is now stale (git state has clearly moved past what it
describes), **don't update the memory file yourself** — surface the
discrepancy in the summary (step 5) and ask the user whether to update it,
and how. Memory edits are meant to capture the user's own understanding of a
ticket, not whatever a status-check run happened to infer; auto-correcting it
risks writing something confidently wrong into a file future sessions will
treat as ground truth, with no one having actually reviewed it. This applies
even when the drift seems obvious and well-evidenced from git — showing your
reasoning and asking is cheap, an unreviewed memory edit is not.

## 5. Synthesize the "where you left off" summary

Report back using this structure:

```markdown
## Ticket <number>: <title>
**Branch:** <branch> · **Worktree:** <path>
**PR:** <link + draft/ready + CI status, or "not yet opened">

### Done
<what's committed and confirmed working>

### Uncommitted
<what's in the working tree right now, if anything — be explicit that this
exists so it doesn't get lost>

### Outstanding
<bugs found but not fixed, tests not run/passing, cleanup items (e.g.
diagnostic scaffolding to remove before merge), anything SPEC.md or memory
flagged as "still open">

### Memory discrepancy (only if step 4 found one)
<what memory claims vs. what git/GitHub actually show now, with the specific
commits/diffs backing your read — then ask directly whether to update the
memory file, and to what>

### Suggested next step
<one concrete next action, not a full plan>
```

Keep it concise — this is a status report to re-orient someone, not a full
re-derivation of the ticket's history. Link out to `SPEC.md` or the memory
file for anyone who wants the full detail rather than repeating it all here.

## 6. Apply repo conventions when suggesting the next step

A few standing rules shape what "next step" should look like, and are easy to
get wrong if you reason about the ticket in isolation:

- **PySpark vs. Polars:** if outstanding work touches a `jobs/` (PySpark) file,
  don't suggest introducing new PySpark code unless the ticket is already
  scoped that way — CLAUDE.md's migration direction is Polars for anything
  new.
- **pytest migration:** if outstanding work includes writing new tests,
  they should be pytest-style (dataclass-driven `@pytest.mark.parametrize`
  cases), not `unittest.TestCase`, unless the ticket's own memory/SPEC.md
  already committed to a different call for that file.
- **Draft PRs by default:** if "open a PR" is the suggested next step, say so
  as a draft PR, not ready-for-review, unless memory or the user has said
  otherwise for this specific ticket.
- **CLAUDE.md conflicts:** if anything in SPEC.md, memory, or the git history
  contradicts an explicit CLAUDE.md rule (not just a general principle), flag
  the conflict directly rather than silently picking a side or resolving it
  yourself.

## 7. Don't start implementing unprompted

This skill's job ends at producing the summary and a suggested next step —
not at doing that next step. Wait for the user to confirm direction before
writing code, committing, or opening/updating a PR, even if the next step
seems obvious from the summary.
