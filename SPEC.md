# Ticket 1978: Implement has_column_data_since_date()

## Scope

Implement `has_column_data_since_date(column_name: str, from_date: date,
column_alias: str) -> pl.Expr` in
`projects/_08_publication/_01_job_role_estimates/fargate/utils/clean_utils.py`
(the capacity-tracker "has data" filter stubbed by ticket 1922, later
revised from a two-column OR into this single-column form). For a given
`column_name` and `from_date`, a location's flag is `True` if that column
has no null values, for that location, among rows dated on or after
`from_date` — and the location must have at least one row on or after
`from_date` too (guards against vacuous truth for a location with no data at
all since the cutoff, e.g. one that deregistered earlier). Aliased to the
caller-supplied `column_alias`.

`_02_clean_pub_data.py::main()` calls this six times — once per (CT column ×
cutoff date) combination, for the two source columns
(`ct_care_home_total_employed_imputed`, `ct_non_res_care_workers_employed_imputed`)
and three cutoff dates (1 Apr 2021 / 2025 / 2026) — each aliased to its own
dedicated output column. There is no combining of the two columns; each gets
its own independent flag per era.

## Where this lands

- `utils/column_names/publication_columns.py` — six explicit fields:
  `ct_care_home_has_data_2021/_2025/_2026` and
  `ct_non_res_has_data_2021/_2025/_2026`.
- `projects/_08_publication/_01_job_role_estimates/fargate/utils/clean_utils.py`
  — `has_column_data_since_date(column_name, from_date, column_alias)`, the
  sole function for this filter (no wrapper/OR function).
- `projects/_08_publication/_01_job_role_estimates/fargate/_02_clean_pub_data.py`
  — imports `clean_utils`, `date`, `IndCqcColumns as IndCQC`, and
  `PublicationColumns as Pub`; six `.with_columns(...)` calls, one per
  column/date pair.
- `projects/_08_publication/_01_job_role_estimates/tests/fargate/utils/test_clean_utils.py`
  — `TestHasColumnDataSinceDate`.
- `projects/_08_publication/_01_job_role_estimates/tests/fargate/test_02_clean_pub_data.py`
  — `TestMain` mocks `clean_utils.has_column_data_since_date` and asserts 6
  calls / a 6-argument `.with_columns()`.

## Key decisions

- Fixed cutoff (`from_date`), not per-row continuity derived from each row's
  own date — the resulting flag is constant per location, not per row.
- No OR / no combining function — `has_column_data_since_date` is called
  directly from the job, once per (column, date) pair. The earlier
  `add_ct_filter_has_ct_data` wrapper (which OR'd the two columns together)
  was removed once this was decided.
- No-data guard: a location with zero rows on/after `from_date` must resolve
  `False`, not vacuously `True` — requires "no nulls in window" AND "at
  least one row in window".
- `column_name` and `column_alias` are both explicit arguments — keeps
  column-name string construction out of the function body, per CLAUDE.md's
  rule that column names live in `utils/column_names` classes.
- Implementation shape:
  ```python
  in_window = pl.col(IndCQC.cqc_location_import_date) >= from_date
  has_row_in_window = in_window.any().over(IndCQC.location_id)
  no_nulls_in_window = ~(
      (in_window & pl.col(column_name).is_null()).any().over(IndCQC.location_id)
  )
  return (has_row_in_window & no_nulls_in_window).alias(column_alias)
  ```
  `location_id` and `cqc_location_import_date` are both present on the merged
  lf per `_01_merge_pub_data.py`'s column lists (`JOB_ROLE_ESTIMATES_ARCHIVE_COLUMNS`
  / `JOB_ROLE_METADATA_ARCHIVE_COLUMNS`). This `.over()` is a per-location
  boolean broadcast (`any()` over the location group), not a windowed/ordered
  cumulative function.
  - This is a genuine `.over()` case, which per CLAUDE.md's streaming
    checklist falls back to the in-memory engine — expected here, not a bug,
    since this stage operates on already-aggregated archive data (job role
    estimates × metadata), not raw pipeline-scale rows. Worth a line in the
    PR description.
- Qualifies as "genuinely complex, non-obvious business logic" per CLAUDE.md's
  Polars style guidance — worth its own function and dedicated unit tests,
  rather than inlining.
- Per CLAUDE.md's opportunistic-upgrade scoping: only this one function's
  docstring and its own test get upgraded. The other two filter stubs and the
  aggregation/percentage-change stubs in the same two files stay untouched.

## Test approach

Dataclass + `@pytest.mark.parametrize` convention (`pytest-pattern` skill),
each case a small multi-row table (`test_data`/`expected_data`,
`orient="row"`), modelled on `TestRemoveRepeatedValuesOverTime` in
`tests/test_polars_utils/test_cleaning_utils.py` — a single row can't
exercise a per-location aggregate check. Cases, each with its own
`column_name`, `from_date`, and `column_alias`:
- No nulls in the checked column since the cutoff → `True`.
- A null in the checked column since the cutoff → `False`.
- Zero rows since the cutoff at all (deregistered earlier) → `False` despite
  no nulls being found (the no-data guard).
- A non-null value before `from_date` at a location with no rows after it →
  still `False`.
- `column_name` is honoured — a case using the non-res column confirms the
  check targets the argument, not a hardcoded column.
- `column_alias` is honoured — a case using a non-default alias.

No new `unittest_data/*_test_file_data.py` file — inline test frames (small,
single-purpose; no `unittest_data/` directory exists yet for this project).

## Explicitly out of scope

- `add_ct_filter_consistent_service()`, `add_ct_filter_dispersion_filter()`,
  and the aggregation/row-adding/percentage-change placeholders — remain
  `pass`-stub placeholders.
- No new `unittest_data` fixture file.

## Open questions

None outstanding.

---
Remove this file before merging to main.
