---
name: polars-migration
description: Use when migrating a PySpark job or function in this repo (jobs/) to Polars (fargate/), or when asked to "migrate to polars", "convert this PySpark function", "port this job to fargate". Walks the repo's migration checklist so nothing from the PR template gets missed.
---

# Migrating a PySpark job/function to Polars

This repo is mid-migration from PySpark (`jobs/`) to Polars (`fargate/`). When migrating, follow this checklist — it mirrors `.github/PULL_REQUEST_TEMPLATE/polars_migration_template.md`.

1. **Check for existing utils before writing new ones.** Search the narrowest scope first: stage `utils/` → project `utils/` → repo-wide `projects/utils/`. Only add a new util if nothing suitable exists at any of those scopes.
2. **Confirm correct placement** for any new/moved util, using the scope table:
   | Used by | Location |
   |---|---|
   | One job/dataset only | `projects/<project>/<stage>/utils/*.py` |
   | Multiple jobs/datasets in a project | `projects/<project>/utils/*.py` |
   | Multiple projects | `projects/utils/utils.py` |
3. **Use `LazyFrame`** for test data, not eager `DataFrame`.
4. **Add or port unit tests** in pytest style — see the `pytest-pattern` skill for the repo's dataclass+parametrize convention.
5. **Add Google-style docstrings**, including non-obvious performance notes (why something stays lazy, why a `.collect()` happens where it does).
   - You have **free rein to improve the function's name, docstring, and test names/structure** as part of the conversion — don't feel bound to preserve old PySpark-era naming just for continuity. This is scoped to the function(s) actually being migrated, not an excuse to rename untouched neighbours.
6. **Tag the old PySpark function** with a comment pointing at its replacement:
   ```python
   # converted to polars -> path/to/new_fargate_file.py
   ```
7. **Follow the S3/Athena naming convention** when saving output:
   `{dataset}_{sub_dataset_if_relevant}_{order_of_process_if_relevant}_{brief_description}` (e.g. `ind_cqc_02_cleaned`).
8. **Update `CHANGELOG.md`** under `## [Unreleased]`, one bullet per piece of work (not per edit), in the matching subsection (`Added`/`Changed`/`Improved`/`Fixed`).
9. **Remind the user of the PR-process steps that aren't yours to do**: request AI code review, move the Trello ticket to the PR column.

Scope discipline: only migrate what's being asked. Don't mass-rewrite untouched PySpark files just because you're nearby, and don't refactor beyond what the migration needs (see the repo's general no-premature-abstraction guidance).

For scale/memory concerns specific to the new Polars code (laziness, `.over()` vs joins, streaming-engine coverage), see `CLAUDE.md` directly, or the `polars-streaming-check` / `over-vs-join` skills if the migrated logic touches those areas.
