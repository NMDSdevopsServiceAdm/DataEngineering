---
name: analytics-library
description: Use when asked to analyse pipeline outputs, build a SageMaker notebook or ad hoc analysis script, or find an existing metric function to reuse ("is there a function for…", "what do we already have for…"). Points at the repo's existing metric functions; add a snippet only for what doesn't exist. Pairs with `pytest-pattern`.
---

# Building analyses from existing repo functions

Notebooks and ad hoc scripts should call the repo's own metric functions, not rewrite them. Check this list (add an entry when a metric function lands in the repo), then grep the repo before writing anything new. Pass column names as parameters, using the `utils/column_names` classes.

- Jumpiness (mean absolute change between consecutive dates): `mean_period_to_period_change` (`projects/_03_independent_cqc/utils/model_evaluation_utils.py`)

## Rules

1. **Import, don't copy.** Import from a repo checkout on `sys.path`; if there isn't one, inline a copy and prove it matches the repo's on synthetic data. Check a module's imports first: some pull in boto3 or pipeline modules.
2. **Keep PySpark out.** Assume notebooks have only polars and boto3. Never import `utils/utils.py`, `utils/cleaning_utils.py` or `utils/validation/validation_utils.py` (they import PySpark and pydeequ).
3. **Build outside the repo.** Notebooks, scripts and outputs are never committed; the repo is public, so checks use synthetic data. There is no AWS access here: verify end to end on synthetic data and the user runs it on SageMaker.
4. **Loading data is the notebook's job.** Use LazyFrames. `polars_utils.utils.scan_parquet` treats any `str` as an S3 URI (needs credentials); pass local files as `Path`.
5. **Charts are built by hand** in Excel from exported tables.
6. **Snippets only for gaps.** If no equivalent exists anywhere in the repo, and only after the user agrees, add one function per file in `snippets/` beside this file (inside the repo, unlike notebooks). Follow `snippets/placeholder_example.py` and its test `tests/skills/test_placeholder_example.py`: Google docstring, LazyFrame in/out, column names as parameters (new classes in `utils/column_names` rebuild all Docker images on dev pushes, so ask first). Write the test first in `tests/skills/test_<snippet>.py` and add a CHANGELOG bullet. When the first real snippet lands, delete the placeholder and its test and point this rule at the real ones.
