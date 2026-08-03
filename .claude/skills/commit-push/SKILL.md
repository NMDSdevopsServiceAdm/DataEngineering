---
name: commit-push
description: Use when committing and pushing a chunk of work — "/commit-push", "commit and push this". Run repeatedly through a ticket, once per logical change, not just at the end. Pairs with `new-ticket` and `open-pr`.
---

# Committing and pushing

1. Show `git status` (staged/unstaged/untracked). Stage specific relevant files — never a blind `git add -A`.
2. Parse the ticket number from the current branch name (leading digits before the first `-`).
3. Draft the commit message as `<ticket-number> - <Description>`, matching this repo's existing log convention — short, plain-sentence, no trailing period.
4. Before committing:
   - Update `CHANGELOG.md`'s `Unreleased` section if this is a substantive change (see CLAUDE.md's Changelog section for the format).
   - Run the relevant unit tests.
5. Commit, then push. Confirm with the user before pushing — same as any push, this isn't bypassed by the skill.
