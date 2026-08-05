---
name: open-pr
description: Use when a ticket's work is ready for review — "/open-pr", "open a PR", "raise the PR". Run once, at the end of a ticket. Pairs with `new-ticket` and `commit-push`.
---

# Opening a PR

1. Confirm the branch is pushed and up to date with remote:
   - Uncommitted changes present → run `commit-push` to commit and push them. That skill's own
     "invocation = go-ahead to push" applies as normal here, since committing pending work is
     exactly what `commit-push` is for.
   - Everything already committed but the branch is ahead of its upstream (or has no upstream
     yet) → push it directly (`git push` / `git push -u origin <branch>`), but confirm with the
     user first. Opening a PR doesn't itself imply consent to push — that implicit consent is
     specific to invoking `commit-push` for its own sake, not something this skill can borrow as
     a side effect.
2. Run a sub-agent review of the diff against `main`, following the `review-checklist` skill. This is what satisfies the "Code reviewed by AI" checklist item on the PR template — don't tick it without actually running this.
   - Any **Critical** finding: stop and resolve or discuss with the user before continuing.
   - Important/Optional findings: show them, then continue regardless.
3. Pick the template from `.github/PULL_REQUEST_TEMPLATE/`:
   - `polars_migration_template.md` if the diff contains a `# converted to polars ->` marker.
   - `standard_template.md` otherwise.
4. Populate it:
   - Trello link — fill in the ticket number (parsed from the branch name) using the template's placeholder format, e.g. `Trello ticket [#1234](add link)`, and leave `(add link)` as-is. Don't ask the user for the URL or try to populate it.
   - Description of the work.
   - Testing checklist, ticked to what was actually done.
   - "Code reviewed by AI" — ticked, since step 2 just did it.
5. Create the PR as a **draft** by default (`gh pr create --draft`) using the populated
   title/body — this repo's standing convention (see `continue-ticket`'s "Draft PRs by default"
   note). Only skip `--draft` if the user's own request made it clear they want it ready for
   review immediately. Confirm with the user before creating it either way — same as any PR,
   this isn't bypassed by the skill.
