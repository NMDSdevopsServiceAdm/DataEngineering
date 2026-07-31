---
name: open-pr
description: Use when a ticket's work is ready for review — "/open-pr", "open a PR", "raise the PR". Run once, at the end of a ticket. Pairs with `new-ticket` and `commit-push`.
---

# Opening a PR

1. Confirm the branch is pushed and up to date with remote — run `commit-push` first if not.
2. Run a sub-agent review of the diff against `main`, following the `review-checklist` skill. This is what satisfies the "Code reviewed by AI" checklist item on the PR template — don't tick it without actually running this.
   - Any **Critical** finding: stop and resolve or discuss with the user before continuing.
   - Important/Optional findings: show them, then continue regardless.
3. Pick the template from `.github/PULL_REQUEST_TEMPLATE/`:
   - `polars_migration_template.md` if the diff contains a `# converted to polars ->` marker.
   - `standard_template.md` otherwise.
4. Populate it:
   - Trello link — only the ticket number is embedded in the branch name, so confirm/paste the actual link with the user rather than guessing it.
   - Description of the work.
   - Testing checklist, ticked to what was actually done.
   - "Code reviewed by AI" — ticked, since step 2 just did it.
5. Create the PR with `gh pr create` using the populated title/body. Confirm with the user before creating it — same as any PR, this isn't bypassed by the skill.
