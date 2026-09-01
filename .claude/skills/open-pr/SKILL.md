---
name: open-pr
description: Use when a ticket's work is ready for review — "/open-pr", "open a PR", "raise the PR". Run once, at the end of a ticket. Pairs with `new-ticket` and `commit-push`.
---

# Opening a PR

1. Confirm the branch is pushed and up to date with remote — run `commit-push` first if not.
2. Clean up code comments in the diff against `main`: for each comment on a touched line, trim,
   rewrite, or remove it if it doesn't meet CLAUDE.md's bar — not brief, restates *what* instead
   of the non-obvious *why*, or references something that'll go stale (a ticket number, a PR
   number, a caller). Leave comments outside the diff alone. If this changes any files, commit and
   push the fix (`commit-push` skill) before continuing.
3. Run a sub-agent review of the diff against `main`, following the `review-checklist` skill. This is what satisfies the "Code reviewed by AI" checklist item on the PR template — don't tick it without actually running this.
   - Any **Critical** finding: stop and resolve or discuss with the user before continuing.
   - Important/Optional findings: show them, then continue regardless.
4. Pick the template from `.github/PULL_REQUEST_TEMPLATE/`:
   - `polars_migration_template.md` if the diff contains a `# converted to polars ->` marker.
   - `standard_template.md` otherwise.
5. Populate it:
   - Trello link — fill in the ticket number (parsed from the branch name) using the template's placeholder format, e.g. `Trello ticket [#1234](add link)`, and leave `(add link)` as-is. Don't ask the user for the URL or try to populate it.
   - Description of the work.
   - Testing checklist, ticked to what was actually done.
   - "Code reviewed by AI" — ticked, since step 3 just did it.
6. Create the PR with `gh pr create` using the populated title/body. Confirm with the user before creating it — same as any PR, this isn't bypassed by the skill.
