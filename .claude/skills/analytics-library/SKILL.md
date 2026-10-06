---
name: analytics-library
description: Use when asked to analyse pipeline outputs, build a SageMaker notebook or ad hoc analysis script, or reuse or add an analysis snippet ("is there a snippet for…", "add this to the analytics library"). Snippets live in `snippets/` beside this file. Pairs with `pytest-pattern`.
---

# Building analyses from the snippet library

Snippets are reusable analysis functions that get copied into notebooks and scripts analysing pipeline outputs. They are not an importable package.

1. **Reuse first.** List `snippets/` and read the module docstrings: one function per file, requirements noted at the top.
2. **Use existing repo metrics, don't rewrite them** (e.g. `projects/_03_independent_cqc/utils/model_evaluation_utils.py`). Import from a repo checkout on `sys.path`; if there isn't one, inline a copy and prove it matches the repo's on synthetic data.
3. **Keep PySpark out.** Notebooks have polars and boto3 only. Never import `utils/utils.py`, `utils/cleaning_utils.py` or `utils/validation/validation_utils.py` (PySpark + pydeequ; they also raise without `SPARK_VERSION`).
4. **Build outside the repo.** Notebooks, scripts and outputs are never committed; the repo is public, so tests and checks use synthetic data. There is no AWS access here: verify end to end on synthetic data and the user runs it on SageMaker.
5. **Loading data is the notebook's job.** Snippets take and return LazyFrames. `polars_utils.utils.scan_parquet` treats any `str` as an S3 URI (needs credentials); pass local files as `Path`.
6. **Charts are built by hand** in Excel from exported tables; programmatic charts proved fragile.
7. **Adding a snippet: only after the user agrees.** One function per file in `snippets/`, Google docstring, LazyFrame in/out, column names as parameters (reuse `utils/column_names` classes; a new class there rebuilds every Docker image on dev pushes, so ask first). Write the test first in `tests/skills/test_<snippet>.py` and add a CHANGELOG bullet.
