---
name: post-merge-cleanup
description: Use this skill whenever the user wants to tidy up after a ticket's PR has been merged in this DataEngineering repo — phrases like "PR for 1793 merged, clean it up", "close out ticket 1814", "tidy up now that this merged", "can you clean up the worktree for X", or "1731 is merged, deal with the leftovers". It verifies the PR actually merged, refreshes main, removes the ticket's git worktree and local branch (its local `.venv` goes with it automatically), flags anything that needs the user's own decision (an un-auto-deleted remote branch, a stray uncommitted SPEC.md, a ticket memory file that should be archived), and reports what it did. Always reach for this skill instead of ad hoc `git worktree remove`/`git branch -d` — the point is to check for unmerged work and unresolved shared-state changes *before* deleting anything, not to assume a merged PR means everything underneath it is safe to remove.
---

# Cleaning up after a merged PR

This repo (Skills for Care Workforce Intelligence Team) develops each ticket on
its own branch in its own sibling git worktree (`DataEngineering-<ticket>-<slug>`,
branch `<ticket>-<slug>` — see the `new-ticket` skill for how these get set
up). Once a ticket's PR merges, that worktree, branch, and any related
scratch state (`SPEC.md`, the ticket's memory file) become stale and should be
cleared out — but several of those removals are destructive or affect shared
state, so this skill checks before acting rather than assuming a green PR
means everything underneath it is disposable.

## 1. Identify the ticket, branch, and worktree

If the user gave a ticket number, find the matching worktree:

```
git worktree list
```

Match by ticket-number prefix on the branch name (`1814-slv-reshp`) or the
sibling directory name (`DataEngineering-<ticket>-<slug>`). If the user didn't
give a number, infer it from the current directory the same way `continue-ticket`
does:

```
git rev-parse --show-toplevel
git branch --show-current
```

If you can't find a matching worktree at all, check whether it was already
removed in a prior cleanup pass before telling the user nothing's there —
`git branch -a --contains main` plus `gh pr list --state merged --search
"<ticket>"` will confirm the ticket merged even if the worktree is already
gone, which just means there's nothing left to clean up.

## 2. Confirm the PR actually merged — don't assume

```
gh pr view <branch> --json state,mergedAt,baseRefName,url
```

If `state` isn't `MERGED`, stop and tell the user — this skill's whole reason
to exist is not deleting work that isn't actually landed. A closed-but-not-merged
PR, or an open PR the user is misremembering as merged, is exactly the case
this check exists to catch.

## 3. Check the worktree for anything not actually merged

Before removing anything, check the ticket's worktree per this repo's git
safety protocol:

```
git -C <worktree-path> status
git -C <worktree-path> log <base-branch>..HEAD --oneline
```

If `status` shows uncommitted changes, or the branch has commits that don't
appear in the merged PR (e.g. someone pushed a follow-up after the PR's last
review), stop and surface this to the user before deleting anything — removing
the worktree would discard it. Don't rely on the PR being merged as proof the
worktree is clean; the two can drift.

The worktree's gitignored `.venv` and `.vscode/` don't need any handling here —
they're local environment config, and being gitignored they neither show up in
`git status` nor block `git worktree remove`. Each worktree has its own local
`.venv` (uv's default, not a shared one), so there's no cross-worktree venv to
identify or account for before removing it — step 5's removal takes care of it
automatically.

## 4. Refresh the main checkout

Find the main repo checkout (the `git worktree list` entry with branch `main`)
and bring it up to date before removing the ticket's worktree:

```
git -C <main-repo-path> status
git -C <main-repo-path> fetch origin main
git -C <main-repo-path> pull --ff-only origin main
```

Check `status` first per the same safety protocol — don't fast-forward over
uncommitted work sitting on `main`, flag it instead.

## 5. Remove the ticket's worktree, local branch, and its virtualenv

`git worktree remove` must run from a *different* worktree than the one being
removed — if the user is currently inside the ticket's own worktree, this has
to run from the main checkout (or another worktree) instead. Say so explicitly
if that's the case, rather than letting the command fail confusingly.

```
git -C <main-repo-path> worktree remove <worktree-path>
git -C <main-repo-path> branch -d <ticket>-<slug>
```

Use `branch -d` (lowercase), not `-D` — it refuses to delete a branch with
unmerged commits, which is exactly the safety net this step wants given step 3
already confirmed things look merged. If `-d` refuses, that's a signal step 3
missed something; stop and re-check rather than escalating to `-D`.

This is a local, reversible-in-spirit pair of operations (the branch and
worktree only ever pointed at commits that are now on `main` anyway), so it's
fine to do without extra confirmation once steps 2–4 have passed. Since each
worktree's `.venv` is a local subdirectory rather than a venv shared across
worktrees, `git worktree remove` deletes it along with everything else — there's
no separate virtualenv cleanup step needed here, and no risk of taking out
another worktree's environment by mistake.

## 6. Check the remote branch

GitHub often auto-deletes the head branch on merge, but not always (repo
setting, or a fork-based PR). Check:

```
git ls-remote --heads origin <ticket>-<slug>
```

If it still exists, **ask the user before deleting it** — deleting a remote
branch is a shared-state change per this repo's safety rules, not a local
cleanup step, even though the branch is dead weight. Don't delete it silently
just because the local half is gone.

## 7. Check for a stray SPEC.md

Ticket worktrees carry a working `SPEC.md` that's meant to be removed before
merging (per the `new-ticket` skill's template and precedent from ticket
1809). Check whether it actually got removed:

```
git -C <main-repo-path> log --oneline -1 -- projects/**/SPEC.md SPEC.md
```

or simply check the merged PR's file list via `gh pr view <branch> --json
files`. If `SPEC.md` is present on `main` after the merge, flag it to the user
— removing it now needs its own small commit on `main`, which isn't something
to do silently as a side effect of cleanup. Offer to make that commit, but
wait for a yes.

## 8. Handle the ticket's memory file

Grep `MEMORY.md` and the memory directory (shown in your system context) for
the ticket number. If a project memory file exists for this ticket, don't
delete or rewrite it yourself — per the same reasoning `continue-ticket`
already applies to memory edits, that file may hold context worth keeping
(e.g. a bug found and fixed, a decision that'll matter if a near-identical
ticket comes up later) even after the ticket itself is closed.

Instead, tell the user it exists, summarize what it currently says, and ask
whether to:
- leave it as-is (most memories linked from `MEMORY.md` are cheap to keep),
- add a one-line "merged, see PR <url>" note to the top of the file, or
- remove it entirely (and its `MEMORY.md` pointer) if it was purely
  ticket-scoped scratch context with nothing worth keeping for the future.

## 9. Report back

Summarize what happened, structured so the user can see what's done vs. what
still needs their call:

```markdown
## Cleanup for ticket <number>: <title>
**PR:** <url> — merged into `<base-branch>`

### Done
- Removed worktree `<path>` (including its local `.venv`)
- Deleted local branch `<branch>`
- Refreshed `main` in `<main-repo-path>`

### Needs your decision
- Remote branch `<branch>` still exists on origin — delete it?
- `SPEC.md` is still present on `main` — remove it in a follow-up commit?
- Memory file `<file>` — keep, annotate, or remove?
```

Only include a "Needs your decision" line for things step 2–8 actually found;
don't pad the report with checks that came back clean.
