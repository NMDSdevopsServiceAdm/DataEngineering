---
name: new-ticket
description: Use this skill whenever the user wants to start work on a new ticket in this DataEngineering repo — phrases like "new ticket", "start ticket 1850", "let's kick off 1862", "set up a branch for ticket NNNN", or "I need to begin work on ticket X". It sets up the branch and git worktree following this repo's exact naming conventions, provisions the worktree's Python environment via `uv sync`, runs a scoping interview via Plan Mode, writes a working SPEC.md from the approved plan, and seeds a project memory file so the ticket's context survives across sessions. Always reach for this skill instead of improvising ad hoc `git branch`/`git worktree` commands when a ticket number is mentioned — the naming rules here (16-char branch limit, sibling worktree directory) are easy to get wrong by hand, and a fresh worktree has no `.venv` until `uv sync` creates one. Pairs with the `commit-push` and `open-pr` skills for the rest of the cycle.
---

# Starting a new ticket

This repo (Skills for Care Workforce Intelligence Team) tracks work as numbered
tickets, each developed on its own branch in its own git worktree. This skill
reproduces the pattern already established by tickets like `1793-empstat` and
`1814-slv-reshp`: a short branch name, a sibling worktree directory with its own
provisioned virtualenv, a scoping interview via Plan Mode, a temporary
`SPEC.md`, and a memory file so a later session (or a different person) can pick
the ticket back up without re-deriving context.

Work through the steps below in order. Don't skip the interview even if the
ticket sounds simple — it's what makes the seeded memory file and SPEC.md
useful later, and it's cheap compared to discovering a scope misunderstanding
mid-implementation.

## 1. Get the ticket number and a short title

Ask two short questions, one at a time (see CLAUDE.md's Output style):

1. Trello card number?
2. One-line description of the work?

Don't try to look this up from GitHub issues or any other tracker — in this
repo, ticket info always comes from the user typing it in. Don't try to gather
full requirements here — that's what step 5's Plan Mode interview does.

## 2. Derive the branch name and check the length limit

Branch names in this repo follow `<ticket-number>-<short-slug>` and must be
**16 characters or fewer in total** (this is a hard constraint seen across the
whole branch history — e.g. `1132-rm-cqc-tasks`
would actually be too long and need trimming). Build the slug by:

- Lowercasing, hyphenating, stripping filler words.
- Abbreviating aggressively if needed (`starters-leavers-vacancies` → `slv`,
  `reshape` → `reshp`, `employee-status` → `empstat`) — this repo's history
  favors short, slightly-cryptic slugs over readable-but-long ones.

Compute the full branch name and count its characters. If it already fits in
16, proceed without asking. If you had to abbreviate hard enough that the
result feels unclear, show the user the proposed branch name and confirm
before creating anything — a bad branch name is annoying to rename later
(see the `1797-a-unpivot`/`1797-b-struct` rename precedent: renaming a
branch after it's pushed means deleting and recreating the remote branch).

## 3. Create the branch and worktree

The convention is a **sibling worktree directory** next to the main repo
checkout, named to match the branch: `DataEngineering-<ticket>-<slug>`.

This must run from the **main repo checkout on the `main` branch**, not from
inside another worktree (`git worktree add` targets the repository the command
is run from, and you want the new worktree to branch off an up-to-date
`main`). Find the main checkout with `git worktree list` — it's the entry with
branch `main` — and check its status before creating anything, per this
repo's git safety protocol: uncommitted or unpushed work sitting on `main`
would be worth flagging to the user first, not silently working around.

```
git -C <main-repo-path> status
git -C <main-repo-path> fetch origin main
git -C <main-repo-path> worktree add ../DataEngineering-<ticket>-<slug> -b <ticket>-<slug> main
```

(Adjust the relative path if the main checkout's parent directory differs —
the goal is a sibling of the main checkout, matching where
`DataEngineering-1793-empstat` and `DataEngineering-1814-slv-reshp` already
sit.)

This is a local, reversible operation (new branch + new worktree directory),
so it's fine to do without extra confirmation once the branch name is settled.
Don't push the new branch to `origin` as part of this step — that happens
later, when there's actual work to push, not at ticket setup.

Note that `git worktree add` branches off whatever the **local** `main` ref
points at, which the preceding `fetch` may have just left behind `origin/main`.
Check, and fast-forward if so — a plain `merge --ff-only`, no history rewrite:

```
git -C <new-worktree-path> merge --ff-only origin/main
```

Tell the user the worktree path once it's created — that's where all
subsequent work on this ticket happens.

## 4. Provision the worktree's virtualenv

**Don't skip this.** uv keys its default virtualenv (`.venv`) to the project
*directory*, so a brand-new worktree starts with no environment of its own.
Left unprovisioned, the first `uv run ...` in that worktree still works — `uv
run` auto-syncs against the repo's `uv.lock` before running — but that means the
session's first command pays the sync cost instead of it being paid upfront.

Run from the new worktree root:

```
uv sync
```

This is fast even as a genuine first run in a brand-new worktree: `uv` resolves
against `uv.lock` (typically single-digit milliseconds — it's a lockfile read,
not a solve) and installs from its own global package cache (`%LOCALAPPDATA%\uv\cache`
on Windows), so most of the cost is linking already-downloaded packages rather
than re-downloading them. Measured ~55s for a full 119-package install into a
brand-new worktree on this machine. There's no cross-worktree pointer file or
shared venv involved — each worktree just gets its own local `.venv` — and `uv`'s
`python-preference = "only-managed"` setting (see `pyproject.toml`) means it
installs its own managed Python 3.11 build rather than depending on whatever
`pyenv`/system Python happens to be on `PATH`, so there's no equivalent of the
old pipenv/pyenv-win interpreter-mismatch failure mode to guard against.

Verify with:

```
uv run python -c "import polars, boto3"
```

Don't try to share one worktree's `.venv` with another (e.g. via uv's
`UV_PROJECT_ENVIRONMENT` env var, the direct analogue of pipenv's old
`PIPENV_CUSTOM_VENV_NAME` trick) — a per-worktree install is cheap enough on this
machine's warm cache that the coordination overhead and blast-radius risk
(deleting one worktree's env taking out another's) isn't worth it. If `uv sync`
ever becomes slow enough that this stops being true — a machine with a cold
cache, or a much larger dependency set — that's worth revisiting, but don't
pre-optimize for it now.

## 5. Run the scoping interview via Plan Mode

Before any code gets written, enter plan mode (`EnterPlanMode`) and let Claude
Code's normal phased workflow (Explore → Design → Review → Final Plan →
`ExitPlanMode`) drive the interview. Plan mode is the right container for
this: the output of this step is a design to align on, not code, and it keeps
you from accidentally starting to implement mid-interview.

Don't just run the generic plan-mode flow unprompted, though — seed it with
this repo's own checklist, using `AskUserQuestion` (or plain back-and-forth
chat, if a question doesn't fit multiple-choice well) so the design phase
actually covers:

- **Where does this land?** Which project/pipeline stage does the ticket
  touch (see the `projects/<project>/_NN_stage/{jobs,fargate,utils}` layout in
  CLAUDE.md)? This decides where new helpers belong.
- **What does "done" look like?** A one-to-two sentence scope statement.
- **What's explicitly out of scope?** Things that sound related but should be
  deferred to a follow-up ticket — this repo's history shows scope creep is a
  real risk here (e.g. ticket 1793 deliberately left `join_datasets()` as a
  placeholder rather than implementing it as part of the CSV-load ticket).
- **Any decisions already made vs. still open?** E.g. PySpark vs. Polars is
  already decided by CLAUDE.md (new work is Polars unless touching existing
  PySpark code) — but ticket-specific choices like data types, column
  placement, or which of several approaches to prototype are worth surfacing
  now rather than assuming.
- **Facts about the data that the repo can't tell you.** Anything relevant to
  this ticket that only the user knows because it isn't derivable from code —
  e.g. dataset size (row/column counts, whether it's the kind of scale that
  triggers this repo's OOM concerns), the actual structure/shape of a source
  file the ticket touches (wide vs. long, schema quirks, cardinality of a
  column that's about to become a join key or `.over()` partition), or
  upstream data-quality issues. Ask only what's actually relevant to this
  ticket — don't run through a generic checklist — but do ask, since guessing
  at data shape has caused real production issues here before (see the
  `validate_00_prepare` OOM in ticket 1814, and the join-broadcast pattern
  CLAUDE.md documents from a prior incident).

If a genuine CLAUDE.md conflict comes up during this interview (the user's
answer contradicts an explicit repo rule), flag it rather than silently
picking one — see the empstat-rates ticket, where column-name-class placement
almost went to the wrong directory before this got caught.

Exit plan mode once the interview is done and the design is confirmed, before
moving on to writing `SPEC.md`.

## 6. Write SPEC.md from the approved plan

Write a `SPEC.md` at the **new worktree's root** (not the main repo) capturing
the plan's outcome. This is a working document, not committed history —
tickets in this repo remove it as cleanup once the ticket merges (precedent:
ticket 1809). Structure it loosely as:

```markdown
# Ticket <number>: <title>

## Scope
<one-two sentence "done" statement>

## Where this lands
<project/stage, which files are likely touched>

## Key decisions
<bullets — anything settled during the interview>

## Data characteristics
<facts about the data the user supplied that aren't derivable from the repo —
row/column counts, structure/shape, cardinality, known quality issues. Omit
this section if the ticket doesn't touch a dataset directly.>

## Explicitly out of scope
<bullets — deferred items, follow-up tickets>

## Open questions
<anything still unresolved, to revisit during implementation>

---
Remove this file before merging to main.
```

## 7. Seed a memory file

Create `ticket_<number>_<slug>.md` in the auto-memory directory shown in your
system context (a personal, per-user path outside this repo — don't hardcode
a specific username's path; use whatever this session's context indicates).
Use the `project`-type memory format:

```markdown
---
name: ticket-<number>-<slug>
description: "Ticket <number> — <one-line summary>, on branch <branch-name> / worktree <path>"
metadata:
  type: project
---

Ticket <number>: <scope statement>. Branch `<branch-name>`, worktree
`<worktree-path>`. `SPEC.md` at the worktree root has the full design —
**Why:** <the driving reason/motivation from the interview, e.g. a deadline,
a bug, a dependency on another ticket>.

**How to apply:** if this ticket is picked up again, read `SPEC.md` first (or
this memory if the file's already been cleaned up post-merge), check
`git log`/`git branch` to see how far implementation got, and treat the
interview decisions above as closed — don't re-litigate them without new
information.

Key decisions: <bullets from the interview>
Explicitly out of scope: <bullets>

<Link to related tickets with [[ticket-name]] if this one shares files, data,
or scope with something already in memory — e.g. two tickets touching the
same SLV project.>
```

Then add a one-line pointer to `MEMORY.md`:

```
- [Ticket <number> <short-title>](ticket_<number>_<slug>.md) — <one-line hook>
```

## 8. Report back

Summarize for the user: branch name, worktree path, that the virtualenv is
provisioned and verified, where `SPEC.md` lives, and that the memory file's been
seeded. If the branch had to be fast-forwarded past commits the fetch pulled in,
say so.

## 9. After the plan is approved

Implement, then use `commit-push` (repeatedly, per logical change) and
`open-pr` (once, at the end) to finish the cycle.
