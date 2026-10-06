---
name: analytics-library
description: Use when asked to analyse pipeline outputs, build a SageMaker notebook or ad hoc analysis script, or find an existing metric function to reuse ("is there a function for…", "what do we already have for rates, shares or growth"). Points at the repo's existing metric functions; add a snippet only for what doesn't exist. Pairs with `pytest-pattern`.
---

# Building analyses from existing repo functions

Notebooks and ad hoc scripts should call the repo's own metric functions, not rewrite them. Check this list, then grep the repo before writing anything new. Most expect the pipeline's column names (`utils/column_names`).

- Share of a total: `percentage_share` (`polars_utils/expressions.py`); row-wise `percentage_share_horizontal` (`projects/_03_independent_cqc/utils/cleaning_utils.py`)
- Turnover, starter and vacancy rates: `create_slv_rate_columns` (`projects/_03_independent_cqc/_03_starters_leavers_vacancies/fargate/utils/clean_utils.py`)
- Period-on-period and cumulative % change: `calc_perc_change_between_rows`, `calc_perc_change_cumulative_from_given_period_onwards` (`projects/_99_publication/monthly_tracker_filled_posts/fargate/utils/clean_utils.py`)
- Jumpiness and group-total scoring (R², weighted % error): `mean_period_to_period_change`, `aggregate_totals_by_group`, `score_group_totals` (`projects/_03_independent_cqc/utils/model_evaluation_utils.py`)
- Group-share scoring: `aggregate_shares_by_group`, `score_group_shares` (`projects/_03_independent_cqc/utils/model_metrics_utils.py`)
- Rolling averages and short-term imputation: `add_rolling_average_percentages`, `add_short_term_imputed_percentages` (`projects/_03_independent_cqc/_02_employment_status/fargate/utils/impute_utils.py`)
- Coverage: `calculate_la_coverage_monthly`, `calculate_coverage_monthly_change` (`projects/_02_sfc_internal/_02_cqc_coverage/fargate/utils/utils.py`)

## Rules

1. **Import, don't copy.** Import from a repo checkout on `sys.path`; if there isn't one, inline a copy and prove it matches the repo's on synthetic data. Check a module's imports first: some pull in boto3 or pipeline modules.
2. **Keep PySpark out.** Notebooks have polars and boto3 only. Never import `utils/utils.py`, `utils/cleaning_utils.py` or `utils/validation/validation_utils.py` (PySpark + pydeequ; they also raise without `SPARK_VERSION`).
3. **Build outside the repo.** Notebooks, scripts and outputs are never committed; the repo is public, so checks use synthetic data. There is no AWS access here: verify end to end on synthetic data and the user runs it on SageMaker.
4. **Loading data is the notebook's job.** Use LazyFrames. `polars_utils.utils.scan_parquet` treats any `str` as an S3 URI (needs credentials); pass local files as `Path`.
5. **Charts are built by hand** in Excel from exported tables; programmatic charts proved fragile.
6. **Snippets only for gaps.** If no equivalent exists anywhere in the repo, and only after the user agrees, add one function per file in `snippets/` beside this file, following `snippets/placeholder_example.py` (delete it when the first real snippet lands): Google docstring, LazyFrame in/out, column names as parameters (a new class in `utils/column_names` rebuilds every Docker image on dev pushes, so ask first). Write the test first in `tests/skills/test_<snippet>.py` and add a CHANGELOG bullet.
