---
name: commit-push
description: Commit the current repo's pending changes with a descriptive message and push them to the remote, in one step. Use this whenever the user says something like "/commit-push", "commit and push this", "ship this", or otherwise clearly wants their local changes both committed AND sent to the remote in the same request — not just staged or committed locally. Running this skill is itself the user's explicit go-ahead to push, so don't re-ask for push confirmation once it's invoked. Run repeatedly through a ticket, once per logical change, not just at the end. Pairs with `new-ticket` and `open-pr`.
---

# Commit and push

The user invoked this skill because they want their pending changes committed and pushed in one
go, without being asked to confirm the push separately — that confirmation is baked into calling
this skill. Everything else about safe git usage still applies: real commits (never amend), no
skipped hooks, no force-push, and a careful eye before staging anything that might be a secret.

## 1. Gather context

Run these in parallel, in the target repo:

- `git status` (never `-uall` — it can be slow/memory-heavy on large repos)
- `git diff` — both staged and unstaged changes
- `git log` — recent commits, to confirm the `<ticket-number> - <Description>` convention below

If `git status` shows nothing to commit **other than `SPEC.md`**, say so and stop — there's
nothing to push either. (See step 3 — `SPEC.md` is a working ticket doc that stays local, never
committed.)

## 2. Draft the commit message

Parse the ticket number from the current branch name (leading digits before the first `-`), and
format the message as `<ticket-number> - <Description>` — this repo's existing log convention
(see CLAUDE.md's "Environment & workflow"): short, plain-sentence, no trailing period, explaining
*why* the change was made rather than restating the diff.

## 3. Stage deliberately

Add the specific changed files by name or path. Never `git add -A` or `git add .` — a blanket add
is how unrelated work-in-progress files or stray local config end up in a commit. After staging,
look at what's actually included (`git status`), and if anything looks like it could hold a
secret — `.env` files, credentials, tokens, keys, even under an innocuous-looking name — open it
and check before committing. If it does contain something sensitive, stop and tell the user rather
than committing it.

In this repo's git worktrees, the `.venv` directory uv creates is gitignored, so
it shouldn't reach staging at all — never force-add it (`-f`/`--force` would be
needed) and don't report it as a possible secret leak. A `.env` file, if present,
follows the same rule as any other config file per the paragraph above: check its
contents before staging rather than assuming it's safe or unsafe by name alone.

**Never stage or commit `SPEC.md`.** It's a working ticket doc (seeded by the `new-ticket` skill)
that's meant to stay local to the worktree — never part of the committed history. Leave it out of
`git add` even if it's modified or untracked, regardless of whether the user's request mentioned
it by name. If `SPEC.md` is already tracked in the repo's history (check with
`git ls-files --error-unmatch SPEC.md`), don't try to silently un-track it as part of this skill —
flag it to the user instead, since removing a tracked file is its own decision.

## 4. Before committing

- Update `CHANGELOG.md`'s `Unreleased` section if this is a substantive change (see CLAUDE.md's
  Changelog section for the format).
- Run the relevant unit tests.

## 5. Commit

Create a new commit (never `--amend` — amending can silently rewrite or drop a commit the user
didn't ask you to touch). Never pass `--no-verify` or `--no-gpg-sign`, and don't bypass a failing
hook — fix whatever it's complaining about and make a new commit instead. Pass the message via a
heredoc so multi-line formatting survives, and end it with:

```
Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
```

## 6. Decide whether to push

Check the current branch name.

- **`main` or `master`:** stop here — don't push. These branches are usually protected or shared
  in a way that makes a direct push risky (bypassing PR review, clashing with others' work). Tell
  the user the commit landed locally and that they should push it themselves, or move the work to
  a feature branch first.
- **Any other branch:** push it — no separate confirmation needed, per this skill's own invocation.
  Use `git push`, or `git push -u origin <branch>` if it has no upstream tracking branch yet. Never
  force-push (`--force`/`--force-with-lease`) — if a normal push is rejected (e.g. remote has
  diverged), stop and explain the situation to the user rather than overwriting remote history.

## 7. Confirm and report back

After pushing, check `git status` (and/or `git log`) to confirm it actually landed. Report back
concisely: the commit hash and message used, and whether/where it was pushed — or, if it was
blocked on `main`/`master`, say that plainly so the user knows the push step didn't happen.
