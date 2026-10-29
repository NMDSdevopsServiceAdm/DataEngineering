---
name: review-checklist
description: Use when reviewing a PR, diff, or set of changes within this repo — "review this PR", "review my changes", "code review". Encodes this repo's severity-tiered review structure and reviewer checklist, distinct from the generic built-in review/security-review skills.
---

# Reviewing changes in this repo

Review as a senior engineer, in this order of weight: **correctness, performance, maintainability.**

- Don't rewrite whole blocks unless the approach is fundamentally flawed.
- Do suggest renames and restructuring where they make the code clearer or simpler. Fixes are cheap now, so don't hold back on small naming or structure findings; keep them in the right tier.
- Explain impact concretely — e.g. "this skews the aggregation for X" / "this materialises the full frame before the filter, at N rows that's...", not vague style comments.
- For Polars-specific scale concerns (laziness, `.over()` vs joins, streaming coverage), see `CLAUDE.md`'s scale-constraint section, or the `over-vs-join` / `polars-streaming-check` skills if the diff touches those areas.
- Flag docstrings, comments or names that refer to previous code (e.g. PySpark) as Important, and docstrings grown wordy through accretion as Optional.

## Reviewer checklist

- **Not over-engineered.** Is there a simpler way to meet the ticket? Flag unrequested options or parameters, abstractions with a single caller, helpers wrapping a simple expression, and generality nothing needs yet.
- **No scalability, performance or interdependency concerns.**
- **Tests cover the main behaviour and edge cases.** See `pytest-pattern` for what "appropriate" looks like.
- **Structure is understandable and consistent** with the surrounding code, and helpers sit at the narrowest scope that covers their use (see CLAUDE.md's Project layout).
- **No nested functions** (`def` inside a function body). Ask for a module-level function with tests, or inlining.
- **No security concerns (Critical).** Committed secrets or keys, IAM or Terraform permissions broader than needed, personal workforce data in logs, fixtures or outputs, unvalidated input passed to tools or shell commands.
- **Single-use constants are local** to the function that uses them, not module level.
- **Column names are short and follow the scheme** in CLAUDE.md's "Column names" section. Long names keep slipping through and getting renamed in later PRs, so suggest the short form now, e.g. `assessment_filled_posts_cumulative_perc_change_long_term` → `filled_posts_cum_perc_change_long`, `permanent_percentage_imputed_for_trendline` → `permanent_pct_imputed_trendline`. Look for filler words, spelled-out words (`percentage`, `cumulative`) and `pct` spelled inconsistently.
- **Docstrings on changed functions still match the code** (arguments, returns, behaviour, performance notes), are enough for future maintenance, and cover non-obvious behaviour. Stale ones are Important.

## Structure findings as

1. **Critical** — correctness, data integrity, security, major performance risk.
2. **Important** — scalability, readability, maintainability, plus the checklist items above.
3. **Optional** — nice-to-haves.

If there's nothing critical, say so plainly rather than inventing improvements to fill the section.

## Validating a change

If asked to validate before/during review, see CLAUDE.md's "Environment & workflow" section for what's in scope (tests + `terraform validate`, never deploying or running against AWS).
