---
name: review-checklist
description: Use when reviewing a PR, diff, or set of changes within this repo — "review this PR", "review my changes", "code review". Encodes this repo's severity-tiered review structure and reviewer checklist, distinct from the generic built-in review/security-review skills.
---

# Reviewing changes in this repo

Review as a senior engineer, in this order of weight: **correctness, performance, maintainability.**

- Don't rewrite whole blocks unless the approach is fundamentally flawed.
- Don't suggest renames or restructuring unless the current version is genuinely unclear or harmful to maintainability.
- Explain impact concretely — e.g. "this skews the aggregation for X" / "this materialises the full frame before the filter, at N rows that's...", not vague style comments.
- For Polars-specific scale concerns (laziness, `.over()` vs joins, streaming coverage), see `CLAUDE.md`'s scale-constraint section, or the `over-vs-join` / `polars-streaming-check` skills if the diff touches those areas.

## Structure findings as

1. **Critical** — correctness, data integrity, major performance risk.
2. **Important** — scalability, readability, maintainability.
3. **Optional** — nice-to-haves.

If there's nothing critical, say so plainly rather than inventing improvements to fill the section.

## Also check against the reviewer checklist

Same checklist as `.github/PULL_REQUEST_TEMPLATE/*.md`'s "Reviewer checklist" section — see `pytest-pattern` skill for what "appropriate" test coverage looks like.

## Validating a change

If asked to validate before/during review, see CLAUDE.md's "Environment & workflow" section for what's in scope (tests + `terraform validate`, never deploying or running against AWS).
