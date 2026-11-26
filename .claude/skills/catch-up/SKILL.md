---
name: catch-up
description: Use when an engineer is back from leave or has been on another project and needs to know what changed — "/catch-up", "catch me up", "what did I miss", "what's new since <date>". Takes a date range and summarises merged work for a data engineer, leading with new or changed shared code and conventions. Not the sprint-review executive summary.
---

# Catching an engineer up

For a colleague who needs to work again, not a stakeholder. Technical language is fine (function
names, columns). Lead with what could trip them up or save them time: shared code and conventions
first, then what changed by area.

## 1. Date range

Ask for the start date. End date defaults to today. Canonicalize both to `YYYY-MM-DD`.

Then, read-only (never touch the working tree):

```
git fetch origin main
git rev-list -1 --before=<start> origin/main
```

Call the result `base`. Diffs below run `base..origin/main`.

## 2. Shared code

```
git diff base..origin/main --name-status -- utils/ polars_utils/ ":(glob)projects/**/utils/**"
git diff base..origin/main -U0 -- utils/ polars_utils/ ":(glob)projects/**/utils/**"
```

- **New**: added `def` / `class`, as `path` + signature + first docstring line. Include new column
  names in `utils/column_names/`.
- **Changed or removed**: renamed, deleted or signature-changed functions and column-name fields.
  These can break the reader's own branch, so say what replaced them.

The `:(glob)` magic is required: a plain `projects/**/utils/` pathspec silently matches nothing.
Skip test files and private helpers (`_name`) unless removal or rename of a public one depends on them.

## 3. Conventions and tooling

Diff `CLAUDE.md`, `STYLEGUIDE.md`, `GUIDES.md`, `SETUP.md`, `.claude/skills/`, `pyproject.toml` over
`base..origin/main`. One line per change on what is now expected or newly available (new skill, new
rule, dependency or tooling change). Ignore pure typo or formatting edits.

## 4. What changed, by area

```
gh pr list --state merged --base main --search "merged:<start>..<end>" --json number,title,headRefName,url,body,files --limit 200
```

If the count equals `--limit`, the list may be incomplete: flag it in step 7.

- Ticket number comes from `headRefName` (`<ticket>-<slug>`); trust it over the title. No
  parseable ticket: keep the PR, key it by PR number, and flag it.
- Cluster PRs for the same ticket or feature. Drop pure housekeeping PRs (changelog or tag cuts).
- Group by project and stage, taken from the changed file paths (e.g. `_03_independent_cqc/_01_filled_posts`).
- One line per cluster: what changed, ticket, PR link(s).
- Enrich wording from matching `CHANGELOG.md` bullets (match by content, not string). PRs are the
  unit: a bullet with no matching PR is dropped; fall back to the PR title.

## 5. Decisions worth knowing

Only where a PR body states a trade-off or a rejected approach: one line with the "why". Never
invent rationale; if no body says it, omit the section.

## 6. Output

Chat only. Sections in this order, omitting any that are empty:

1. **Shared code** — New, then Changed or removed
2. **Conventions and tooling**
3. **By area** — one subheading per project/stage
4. **Decisions**

Aim for something readable in a few minutes; no roadmap items and no executive compression.

## 7. Flag, don't drop

Say so explicitly rather than silently dropping: a possibly-incomplete PR list, a PR with no
parseable ticket, a removed or renamed public function, or a PR whose title and body were too thin
to describe confidently.

Treat the first draft as a draft; revise on pushback.
