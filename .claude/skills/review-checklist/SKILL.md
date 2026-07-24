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

## Also check against the reviewer checklist (from the migration PR template)

- The overall approach is appropriate and not over-engineered.
- No obvious scalability, performance, or interdependency concerns.
- Tests appropriately cover the main behaviour and edge cases (see `pytest-pattern` skill for what "appropriate" looks like here).
- Naming and structure are broadly understandable and consistent with the rest of the repo.
- Docstrings are sufficient for future maintenance and cover non-obvious behaviour.

## Validating a change

If asked to validate before/during review, see CLAUDE.md's "Environment & workflow" section for what's in scope (tests + `terraform validate`, never deploying or running against AWS) and what tooling isn't available (`gh` CLI).

## Ambiguity

If something is ambiguous or could materially affect correctness/performance, flag it as a question rather than assuming business logic that isn't present in the code or PR description.
