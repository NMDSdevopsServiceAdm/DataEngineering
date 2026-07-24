# SPEC: Trim `employee_status_rates.csv` to only the columns/rows the pipeline uses

## Context

Ticket 1793 added a load step for `employee_status_rates.csv` (a temporary, manually-maintained
reference file — see `CHANGELOG.md` and prior implementation notes) into the SLV merge job's
`main()` at
`projects/_07_workforce_characteristics/_01_starters_leavers_vacancies/fargate/_01_merge.py`.

The real CSV has 15 columns, but the merge job only ever keeps 8: `service`, `weighting_year`
(filter-only, dropped immediately after), `weighting_job_role`, and the five `emp_stat_*` rates.
The other 7 raw columns (`permanent`, `temporary`, `bank_or_pool`, `agency`, `other`,
`filled_posts`, `weighting_date`) and all non-current-year rows exist in the file purely to be
discarded on every run.

A colleague pointed out this is wasteful for a file that's explicitly a throwaway stopgap: it
should just be produced pre-trimmed (right columns, right rows already) rather than shipped wide
and filtered down every load. This change updates the code to assume the CSV it receives is
**already** trimmed to exactly the shape the pipeline needs — and removes the logic that used to
do that trimming, since it becomes dead weight once the file itself is pre-trimmed.

**Scope: code-only.** This task changes `_01_merge.py`'s schema/logic and
`EmployeeStatusRatesColumns` to match the new assumed file shape. Producing and re-uploading the
actual smaller CSV to S3 (`s3://sfc-main-datasets/domain=workforce_characteristics/dataset=empstat_rates/employee_status_rates.csv`)
is a separate, out-of-band step handled outside this codebase change.

## Current state (baseline, `_01_merge.py` on `1793-empstat`, commit `df2b65a86`)

```python
target_weighting_year = "2025/26"

employee_status_rates_schema = pl.Schema(
    [
        (EmpStatRates.service, pl.Categorical()),
        (EmpStatRates.weighting_year, pl.Categorical()),
        (EmpStatRates.weighting_job_role, pl.Categorical()),
        (EmpStatRates.permanent, pl.String),
        (EmpStatRates.temporary, pl.String),
        (EmpStatRates.bank_or_pool, pl.String),
        (EmpStatRates.agency, pl.String),
        (EmpStatRates.other, pl.String),
        (EmpStatRates.filled_posts, pl.String),
        (EmpStatRates.weighting_date, pl.String),
        (EmpStatRates.emp_stat_perm, pl.Float32),
        (EmpStatRates.emp_stat_temp, pl.Float32),
        (EmpStatRates.emp_stat_bank_or_pool, pl.Float32),
        (EmpStatRates.emp_stat_agency, pl.Float32),
        (EmpStatRates.emp_stat_other, pl.Float32),
    ]
)
employee_status_rates_output_columns = [
    EmpStatRates.service,
    EmpStatRates.weighting_year,
    EmpStatRates.weighting_job_role,
    EmpStatRates.emp_stat_perm,
    EmpStatRates.emp_stat_temp,
    EmpStatRates.emp_stat_bank_or_pool,
    EmpStatRates.emp_stat_agency,
    EmpStatRates.emp_stat_other,
]

employee_status_rates_lf = (
    pl.scan_csv(employee_status_rates_source, schema=employee_status_rates_schema)
    .select(employee_status_rates_output_columns)
    .filter(~pl.all_horizontal(pl.all().is_null()))
    .filter(pl.col(EmpStatRates.weighting_year) == target_weighting_year)
    .drop(EmpStatRates.weighting_year)
)
```

Note: `employee_status_rates_lf` is currently unused downstream — `join_datasets()` and
`apply_employment_status_magic_numbers()` in `merge_utils.py` remain empty placeholders, and
`sink_to_parquet` still writes `job_role_estimates_lf`. That stays unchanged; this task doesn't
wire the join in.

## Decisions from interview

1. **Scope is code-only.** No CSV regeneration/upload as part of this task.
2. **`weighting_year` is removed entirely** — from the assumed CSV shape, the schema, and
   `EmployeeStatusRatesColumns`. It's redundant once every row in the file is guaranteed to be the
   current year; there's nothing left to filter on or drop.
3. **The blank-row filter stays** (`~pl.all_horizontal(pl.all().is_null())`). Unlike the year
   filter, this guards against a spreadsheet/CSV-export artifact (trailing blank lines), not a
   business rule — it's unrelated to the "pre-trimmed to the right columns/rows" assumption and
   stays regardless.
4. **No safety net beyond what `scan_csv` already gives you.** No added assertions on row count,
   distinct years, etc. `scan_csv(schema=...)` already raises `SchemaError` on a column **count**
   mismatch, which is the only validation this file gets. Consistent with this ticket's existing
   "temporary file, don't over-engineer" stance.
5. **`.select()` is dropped.** Once the schema declares exactly the 7 wanted columns, selecting
   them again is a no-op. The load collapses to: declare a 7-column schema, `scan_csv`, filter
   blank rows.
6. **`EmployeeStatusRatesColumns` is trimmed to 7 fields** (`service`, `weighting_job_role`,
   `emp_stat_perm`, `emp_stat_temp`, `emp_stat_bank_or_pool`, `emp_stat_agency`,
   `emp_stat_other`). Confirmed via grep that nothing else in the repo references the fields being
   removed (`weighting_year`, `permanent`, `temporary`, `bank_or_pool`, `agency`, `other`,
   `filled_posts`, `weighting_date`) — safe to delete outright, matching the precedent already set
   in this ticket (a dead `employee_status_rates_schema` dict in test fixtures was deleted rather
   than kept "for documentation").
7. **Accepted risk: raw counts are gone for good (for now).** Dropping
   `permanent`/`temporary`/`bank_or_pool`/`agency`/`other`/`filled_posts`/`weighting_date` means if
   the still-unimplemented `apply_employment_status_magic_numbers()` placeholder ever needs raw
   counts rather than precomputed rates, those columns will need to be re-added to the file and
   schema at that point. Accepted explicitly — consistent with not designing for hypothetical
   future requirements on a file that's a stopgap anyway.
8. **Column-order risk — confirmed empirically.** Verified using `polars==1.38.1` (via a scratch
   venv against the Pipfile-pinned version, run outside plan mode): `scan_csv(schema=...)` matches
   the schema to the file **positionally**, not by name. A CSV with header `weighting_job_role,service`
   and row `Care worker,Care home`, loaded against `pl.Schema([("service", ...), ("weighting_job_role", ...)])`,
   produced `{"service": "Care worker", "weighting_job_role": "Care home"}` — silently swapped, no
   error, no warning. The schema's declared names simply override whatever the header text says;
   only column **count** is validated (matching the earlier `SchemaError` wording from this
   ticket's original implementation). If the trimmed CSV's column order ever drifts from the
   schema's declared order, data will silently load into wrong-named columns. The code comment
   flagging this in `_01_merge.py` is load-bearing, not speculative — keep it.
9. **Test coverage stays fully mocked** — no real-CSV fixture test added back for the blank-row
   filter, consistent with the explicit decision made during the earlier "inline into `main()`"
   follow-up for this same ticket. The only test remains `test_main_runs`'s wiring check
   (`scan_csv` called once with `schema=ANY`).
10. **Final column order confirmed**: 7 columns — `service`, `weighting_job_role`,
    `emp_stat_perm`, `emp_stat_temp`, `emp_stat_bank_or_pool`, `emp_stat_agency`,
    `emp_stat_other` — same relative order as today's output columns, minus `weighting_year`.

## Target implementation

`utils/column_names/employee_status_rates_columns.py`:

```python
from dataclasses import dataclass


@dataclass
class EmployeeStatusRatesColumns:
    service: str = "service"
    weighting_job_role: str = "weighting_job_role"
    emp_stat_perm: str = "emp_stat_perm"
    emp_stat_temp: str = "emp_stat_temp"
    emp_stat_bank_or_pool: str = "emp_stat_bank_or_pool"
    emp_stat_agency: str = "emp_stat_agency"
    emp_stat_other: str = "emp_stat_other"
```

`_01_merge.py` (inside `main()`, replacing the current block):

```python
# The source CSV is expected to already be trimmed to exactly these columns, in this
# order, and to only the current weighting year's rows — scan_csv's schema is matched
# positionally, not by name, so a reordered file would silently load into the wrong
# columns with no error.
employee_status_rates_schema = pl.Schema(
    [
        (EmpStatRates.service, pl.Categorical()),
        (EmpStatRates.weighting_job_role, pl.Categorical()),
        (EmpStatRates.emp_stat_perm, pl.Float32),
        (EmpStatRates.emp_stat_temp, pl.Float32),
        (EmpStatRates.emp_stat_bank_or_pool, pl.Float32),
        (EmpStatRates.emp_stat_agency, pl.Float32),
        (EmpStatRates.emp_stat_other, pl.Float32),
    ]
)

employee_status_rates_lf = pl.scan_csv(
    employee_status_rates_source, schema=employee_status_rates_schema
).filter(~pl.all_horizontal(pl.all().is_null()))
```

Removed entirely: `target_weighting_year`, `employee_status_rates_output_columns`, the
`.select(...)`, `.filter(weighting_year == target_weighting_year)`, and `.drop(weighting_year)`
calls.

## Other updates

- **`test_01_merge.py`**: no behavioural change needed — `scan_csv_mock.assert_called_once_with(self.EMPLOYEE_STATUS_RATES_SOURCE, schema=ANY)`
  already tolerates any schema shape. Verify it still passes after the change; no new fixtures.
- **`CHANGELOG.md`**: update the existing `[Unreleased]/Added` bullet for this feature in place
  (per repo convention of reflecting final delivered state, not adding a new bullet) — drop the
  "filtered to the current weighting year" phrasing since filtering is no longer performed by this
  code; the CSV is now expected to arrive pre-filtered.
- **No Terraform changes** — S3 path, crawler exclusion, and CI sync step are all unaffected by
  this change; only the file's internal contents/shape change (out-of-band, not part of this task).
- **No changes to `merge_utils.py`** — `join_datasets()` / `apply_employment_status_magic_numbers()`
  remain untouched placeholders, out of scope.

## Verification

1. Run the existing test suite for this file:
   ```
   pytest projects/_07_workforce_characteristics/_01_starters_leavers_vacancies/tests/fargate/test_01_merge.py
   ```
   Confirm `test_main_runs` still passes unmodified.
2. Once in an environment with `polars` available, empirically confirm the column-order-matching
   behaviour flagged in decision 8 (e.g. run `scan_csv` with an explicit schema against a small CSV
   whose header order deliberately differs from the schema's declared order, and check whether it
   errors, correctly reorders by name, or silently mismaps data) — update the code comment/this
   spec if the actual behaviour differs from what's assumed here.
3. No new CSV fixture is required in-repo (none exists currently — the real file is manually
   staged directly in S3), but if you have access to a real trimmed sample, sanity-check it against
   the new 7-column schema before uploading.
