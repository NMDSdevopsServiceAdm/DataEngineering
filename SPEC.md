# SPEC: Production implementation of the SLV job-role reshape (1814)

## Context

Ticket 1797 designed how to reshape `_00_prepare.py`'s 148 wide `jrNN{emp,strt,stop,vacy}`
SLV job-role columns into a long-format table: one row per `establishment_id` ×
`ascwds_workplace_import_date` × `job_role_code`, with `employees`/`starters`/
`leavers`/`vacancies` metric columns. Job-role codes are discovered dynamically
from the schema at runtime, not hardcoded, so this stays robust to ASC-WDS
adding/retiring codes.

Two prototype branches were built and deployed at real production scale
(`sfc-*-datasets/domain=ASCWDS/dataset=workplace_cleaned`, ~6.6M rows × 210 cols):
Candidate A (`1797-a-unpivot`, four `.unpivot()` calls joined back together) OOM'd
on the 60 GiB Fargate task; Candidate B (`1797-b-struct`, `pl.struct()` per
job-role code → `concat_list` → `explode` → `unnest`) succeeded at ~14 GB peak
RSS. That decision is closed — Candidate B is the technique used here.

A production-readiness design was interviewed and agreed afterwards, but never
implemented — `main` only had the bare placeholder stub added by ticket 1798.
**This ticket (1814) is that implementation**, built on branch `1814-slv-reshp`
in a separate git worktree (`../DataEngineering-1814-slv-reshp`), not a
continuation of `1797-b-struct` (which stays untouched as the historical
prototype record).

One thing the earlier interview didn't anticipate: candidate B's `_00_prepare.py`
was written *before* ticket 1798 added commented-out placeholders for three
unrelated future tickets (`reduce_to_published_roles` [1796],
`convert_job_role_strings_to_number_only` [1795], `apply_categorical_labels`
[1794]) — candidate B has no trace of them. This spec reconciles that (see
Decisions).

## Decisions

| Topic | Decision |
|---|---|
| Branch / worktree | `1814-slv-reshp` (14 chars, under the repo's 16-char limit), in a sibling worktree directory, matching the convention used for the earlier `-candidate-b` worktree |
| 1796 / 1794 placeholders | Left untouched in `_00_prepare.py` — `reduce_to_published_roles()` and `apply_categorical_labels()` stay commented, belong to their own tickets |
| 1795 placeholder | **Removed.** `discover_job_role_codes()` already strips leading zeros and emits bare-number `job_role_code` values (`"1"`, `"45"`), which already accomplishes what `convert_job_role_strings_to_number_only()` was for. The placeholder function and its call/comment were deleted — ticket 1795 is likely closeable as a no-op; whoever reviews it should confirm against this reshape's output. |
| Prototype branch cleanup | `1797-a-unpivot` and `1797-b-struct` are now superseded and safe to delete, but that wasn't done as part of this ticket — a separate, explicit decision. |
| Test fixture scope | `polars_slv_test_data.py` / `polars_slv_test_schemas.py` were populated scoped tightly to what `_00_prepare`'s own tests need (wide SLV input rows + long-format expected output) — not designed for hypothetical reuse by `_01_merge`/`_02_clean`/`_03_impute`/`_04_estimate`. |
| Int16 downcast location | Inside `convert_job_role_columns_to_rows()`, cast on the `pl.struct()` fields themselves (before `concat_list`/`explode`) — shrinks the intermediate list-of-structs column too, not just the final sink. Safe because upstream bounding (`BoundingExpressions.slv_lower_bound`/`slv_upper_bound` in `clean_workplace_utils.py`) already constrains these metrics to `[1, 998]`. |
| Job-role label (1794) vs reshape (1814) boundary | **Resolved — no change needed in 1814.** See "Job-role labelling: benchmark result" below. |

## Job-role labelling: benchmark result

`apply_categorical_labels()` (ticket 1794, staying a placeholder) is a generic
`replace_strict()`-based label-join, already used elsewhere for the
*worker-level* `main_job_role_clean` column via `MainJobRoleID`/
`MainJobRoleLabels` (`utils/column_values/categorical_column_values.py`). The
SLV `jrNN` codes use the same ASC-WDS numeric ID scheme, so that mapping is
directly reusable once 1794 is picked up — just not yet wired to `job_role_code`.

Initial concern: running `apply_categorical_labels()` post-explode touches ~370M
long-format rows instead of ~6.6M wide rows. A theoretically "free" alternative
was floated — inject the label as a second literal field in
`convert_job_role_columns_to_rows()`'s per-code `pl.struct()` (~50 distinct
codes), before `concat_list`/`explode`.

**Local benchmark (synthetic frame, 1M wide rows × 50 job-role codes → 50M
long-format rows, single process per candidate, `psutil` peak_wset):**

| | Post-explode `replace_strict` | Struct-literal injection |
|---|---|---|
| Collect time | 33.1s | 39.6s |
| Peak RSS | 7,371 MB | 7,680 MB |

The struct-literal approach was **not** free — it was slightly slower and used
slightly more memory. A `pl.lit()` placed inside a per-code struct still gets
broadcast to every row once exploded, so it costs the same per-row
materialisation as a post-explode `replace_strict()`, plus it widens the
intermediate struct/list column before the explode itself. `replace_strict()` is
an in-place expression (not a join), so per this repo's own established memory
model, running it over more rows post-explode is a compute-time cost, not an OOM
risk — and the benchmark shows it isn't even the slower option.

**Conclusion: leave `apply_categorical_labels()` exactly as the untouched 1794
placeholder.** No labelling code was added to 1814's reshape.

**Caveat — not yet confirmed at production scale.** This benchmark used a local
synthetic frame; the environment this was built in has no AWS credentials, so
the production-scale deployment comparison planned as the deciding check
(deploying both candidate implementations to the real Fargate task and
comparing peak RSS/run time, the same way Candidate A vs B was originally
settled) has **not been run**. Prototype benchmark scripts with logging
(`bench_post_explode_label.py` / `bench_struct_literal_label.py`) were used
locally but are not part of this repo — whoever has deployment access should
re-run an equivalent comparison at production scale before treating this as
fully closed.

## Files changed

- **`projects/_07_workforce_characteristics/_01_starters_leavers_vacancies/fargate/utils/prepare_utils.py`**
  Added `discover_job_role_codes()` and `convert_job_role_columns_to_rows()`
  (ported from `1797-b-struct`, stripped of `peak_rss_kb()` and all prototype
  instrumentation). Added a new guard in `discover_job_role_codes()`: raises
  `ValueError` when zero SLV columns are found (previously silently returned an
  empty list). Applied the `Int16` downcast per the decision above. Removed
  `convert_job_role_strings_to_number_only()`.

- **`projects/_07_workforce_characteristics/_01_starters_leavers_vacancies/fargate/_00_prepare.py`**
  Kept the existing structure (commented `reduce_to_published_roles()` /
  `apply_categorical_labels()` calls untouched); replaced the
  `pivot_job_role_cols_to_rows()` placeholder with the real
  `discover_job_role_codes()` + narrowed re-scan + `convert_job_role_columns_to_rows()`
  flow, stripped of all instrumentation/logging.

- **`utils/column_names/cleaned_data_files/ascwds_workplace_job_roles.py`** (new)
  `AscwdsWorkplaceJobRolesColumns` dataclass, ported from `1797-b-struct`.

- **`projects/_07_workforce_characteristics/_01_starters_leavers_vacancies/fargate/validate_00_prepare.py`**
  Added `rows_distinct` on the grain columns, `col_vals_not_null` on grain
  columns, `col_vals_between(1, 998, na_pass=True)` on the 4 metric columns.
  Changed `expected_row_count` to be derived independently — a cheap lazy
  `utils.scan_parquet(compare_path).collect_schema()` call (schema/metadata
  only, no data materialised) feeds `discover_job_role_codes()`, rather than
  counting distinct `job_role_code` values from the `source_df` being validated
  (self-referential in the candidate-B version).

- **`projects/_07_workforce_characteristics/_01_starters_leavers_vacancies/fargate/_01_merge.py`**
  Carried over only the column-selection fix from `1797-b-struct`: scans with
  `selected_columns=workplace_columns` (the new `AWPJobRoles` columns) directly,
  instead of the old post-scan `.select(index_cols, expr.is_slv_job_role_column())`.
  The three stub `mUtils` placeholder calls are untouched.

- **Tests**: ported and adjusted `test_prepare_utils.py` (added a zero-columns
  guard test and an Int16-downcast test to candidate B's cases) and
  `test_00_prepare.py` (kept as a single `test_main_runs`-shaped test, updated to
  assert the real calls, not candidate B's 3-way split). Migrated
  `test_01_merge.py` from `unittest.TestCase` to pytest style. Populated
  `polars_slv_test_data.py`/`polars_slv_test_schemas.py` with `Data`/`Schemas`
  classes, replacing inline-built LazyFrames. Updated `test_validate_00_prepare.py`
  to mock the new `utils.scan_parquet` schema-discovery call and assert all 4
  validation checks are present in the report.

- **`CHANGELOG.md`**: one `### Added` bullet under `[Unreleased]`.

## Testing / Verification

- `pipenv run pytest projects/_07_workforce_characteristics` — 30 passed.
- `discover_job_role_codes()`'s zero-columns guard is covered by
  `test_raises_when_no_slv_columns_are_found`.
- Local synthetic-frame benchmark for the labelling question — see above; not
  yet confirmed at production scale (no AWS credentials in this environment).
- **Not yet done**: a real production-scale deployment run (the same style of
  check used for Candidate A vs B — peak RSS, run time) to confirm this
  production version performs at least as well as Candidate B's ~14 GB
  measurement now that instrumentation is stripped and the `Int16` downcast is
  applied. Needs to be run by whoever has access to the actual Fargate task and
  S3 data.

## Changelog note on this file

Per this repo's existing convention for working specs (see ticket 1809's
`SPEC.md`, added in `3d272a889` and removed in `e7a7e40f9` once merged), this
file may be removed as a cleanup commit once 1814 merges — that's a call for
whoever merges it, not done as part of this ticket.

---

# Addendum: `validate_00_prepare.py` OOM at production scale, and its fix

## Context

The manual production deploy of 1814 (see main spec above) hit an **immediate OOM** in
`validate_00_prepare.py` on the same 60 GiB Fargate task the reshape+sink step handles comfortably.
A control deploy with `validate_00_prepare` removed had every other job succeed, isolating the
problem specifically to this script.

## Diagnostic process

A temporary, throwaway Step Functions state machine (`Ind-CQC-SLV-1814-oom-diag`, auto-discovered via
`terraform/pipeline/step-functions/dynamic/`) chained four standalone diagnostic scripts
(`diag_01_read_parquet_only.py` → `diag_04_full_instrumented.py`, under the project's `fargate/`
folder, each writing `psutil`/`resource`-based peak-RSS checkpoints straight to S3 via
`fargate/utils/diag_helpers.py`, to survive an OOM-kill that would otherwise lose all stdout). Each
stage has its own `Catch`, redirecting to a distinct terminal `Succeed` state on failure rather than
erroring out, so the whole battery self-stops cleanly at the first experiment that reproduces the
OOM.

**Confirmed results:**
- `diag_01` (bare `utils.read_parquet()`, no pointblank at all): **succeeded**, peak RSS **15.36 GB**
  (`diag_01_after_read` checkpoint) — ~45 GB of real headroom under the 60 GiB ceiling. The eager
  full-table materialization was never the bottleneck.
- `diag_02` (same read + only `.rows_distinct(GRAIN_COLUMNS)`): **OOM'd**, never reaching its
  `after_interrogate` checkpoint. Adding just this one check exhausts the remaining headroom.
- Execution correctly stopped at `"Stopped After Diag 2 - Rows Distinct Only"`; `diag_03`/`diag_04`
  never needed to run.

## Root cause, confirmed at the code level

`pointblank`'s `Interrogator.rows_distinct()` (`pointblank/_interrogation.py:783-804`):
```python
count_tbl = tbl.group_by(columns_subset).agg(nw.len().alias("pb_count_"))
tbl = tbl.join(count_tbl, on=columns_subset, how="left")   # <-- broadcast join
tbl = tbl.with_columns(pb_is_good_=nw.col("pb_count_") == 1).drop("pb_count_")
```
This **joins the per-group count back onto every one of the ~370M original rows** — the "join that
broadcasts a computed value back onto every row" anti-pattern this repo's own CLAUDE.md already
documents from a prior production incident, producing a full second, wider copy of the whole table.

Then in `RowsDistinct.test()` (`pointblank/_interrogation.py:1391-1400`):
```python
results_list = nw.from_native(self.test_unit_res)["pb_is_good_"].to_list()
return _threshold_check(failing_test_units=results_list.count(False), ...)
```
The per-row `pb_is_good_` boolean column (~370M values) is converted to a **Python list** just to
count `False` values in pure Python — a large, unnecessary allocation purely to compute a single
number. Combined with the base ~370M-row frame already confirmed in memory (~15.4 GB), these two
extra allocations account for the OOM.

## Decisions

| Topic | Decision |
|---|---|
| Chosen fix | Replace `.rows_distinct(GRAIN_COLUMNS)` with a custom `.specially()` validator, `has_no_duplicate_grain_rows()`, built on `pl.DataFrame.is_duplicated()` — no join at all, unlike pointblank's built-in check or a `group_by()`+semi-join alternative (both considered, documented below as retained alternatives, not chosen). |
| `is_duplicated()` vs. `group_by()`+semi-join | `is_duplicated()` has **zero precedent** in this codebase, vs. 11 files using `.group_by(...)` and one existing `how="semi"` join. Went with `is_duplicated()` anyway per explicit user steer, accepting the lack of in-repo track record for its leaner, single-pass approach. |
| Side-effect placement | The S3 write (and stale-file cleanup) happens **inside** the validator itself, not split out into `main()`. Simpler, one pass — but makes this the first side-effecting custom validator in `polars_utils/validation/actions.py` (the existing ones, `is_unique_count_equal()` / `make_col_has_fewer_nulls_validator()`, are pure functions of the DataFrame). Tests mock `boto3`/`write_to_parquet` accordingly, following the existing pattern in `TestReportOnFail`. |
| Fix scope | Scoped to `validate_00_prepare.py` only. The identical `.rows_distinct()` pattern exists in **23 other `validate_XX.py` scripts** across the repo (grep-confirmed) — currently safe at their row-count scale, but the same latent risk. Explicitly **not** touched by this ticket; flagged here as a follow-up concern for a separate ticket. |
| Extract cap | Capped at **1,000 duplicate rows** written to S3 (`max_rows_to_extract` parameter, default `1000`), bounding the worst case if the reshape ever has a systemic bug duplicating a large fraction of the ~370M rows — the extract itself should never become a second OOM/slow-write risk. |
| Extract semantics | Includes **all instances** of a duplicated grain-tuple (native `is_duplicated()` behavior) — e.g. a grain appearing 3 times yields all 3 rows in the extract, not just the 2 "extra" ones beyond the first occurrence. |
| Null handling | Rows with null grain columns are included in the duplicate check using Polars' default equality semantics (two nulls in the same grain column count as a match). Deliberate overlap with the separate `col_vals_not_null` check (which still independently flags nulls) — not treated as a gap. |
| Stale reports | On a passing run, the check **deletes** any previously-written `duplicate_grain_rows.parquet` from a prior failing run (`s3_client.delete_object`, safe/idempotent even if no such object exists), so the reports folder never retains a misleading stale artifact. |
| Production-scale confirmation | `diag_02_rows_distinct_only.py` gets updated to use the new `has_no_duplicate_grain_rows()` logic, so one more production-scale deploy of the temporary diagnostic pipeline can confirm peak RSS is safe **before** the fix is applied to the real `validate_00_prepare.py`. |

## Implementation

New helper in `polars_utils/validation/actions.py`, alongside the existing `is_unique_count_equal()`
/ `make_col_has_fewer_nulls_validator()` (`boto3` and `Callable` are already imported in this
module):

```python
DEFAULT_MAX_DUPLICATE_ROWS_TO_EXTRACT = 1000


def has_no_duplicate_grain_rows(
    columns: list[str],
    bucket_name: str,
    reports_path: str,
    max_rows_to_extract: int = DEFAULT_MAX_DUPLICATE_ROWS_TO_EXTRACT,
) -> Callable[[pl.DataFrame], bool]:
    """Creates a validation function which checks that no row is duplicated across `columns`.

    Writes up to `max_rows_to_extract` duplicated rows directly to S3 on
    failure - since pointblank's get_data_extracts() does not support custom
    (specially) validations - bounding the worst case if a systemic bug
    duplicates a large fraction of rows. Deletes any stale extract left by a
    prior failing run once the check passes again.

    Uses is_duplicated() rather than pointblank's own rows_distinct(): that
    built-in check joins its per-group count back onto every original row and
    converts a full per-row boolean column to a Python list, which is what
    OOM'd at ~370M rows (see ticket 1814's isolation experiments, above).

    Args:
        columns (list[str]): the columns whose combination must be unique per row.
        bucket_name (str): the bucket to write the duplicate-row extract to (or
            clean up from, on a passing run).
        reports_path (str): the folder (relative to the bucket) to write the
            extract under.
        max_rows_to_extract (int): the maximum number of duplicate rows to
            write out on failure. Defaults to 1000.

    Returns:
        Callable[[pl.DataFrame], bool]: the inner function pointblank's
        `.specially()` invokes with the validated DataFrame.
    """
    extract_key = f"{reports_path.strip('/')}/duplicate_grain_rows.parquet"

    def inner_callable(df: pl.DataFrame) -> bool:
        is_dup = df.select(columns).is_duplicated()
        has_duplicates = is_dup.any()

        s3_client = boto3.client("s3")
        if has_duplicates:
            dup_rows = df.filter(is_dup).head(max_rows_to_extract)
            utils.write_to_parquet(
                dup_rows, f"s3://{bucket_name}/{extract_key}", append=False
            )
        else:
            s3_client.delete_object(Bucket=bucket_name, Key=extract_key)

        return not has_duplicates

    return inner_callable
```

In `validate_00_prepare.py`, replace:
```python
.rows_distinct(
    GRAIN_COLUMNS,
    brief="Grain should be unique per establishment, import date and job role",
)
```
with:
```python
.specially(
    vl.has_no_duplicate_grain_rows(GRAIN_COLUMNS, bucket_name, reports_path),
    brief="Grain should be unique per establishment, import date and job role",
)
```
`row_count_match`, `col_vals_not_null`, and `col_vals_between` are left unchanged — they were never
implicated (the eager frame they run against is already confirmed to have headroom), and
`col_vals_not_null`/`col_vals_between` are simple elementwise boolean ops, not group-by/join
operations. `.specially()` returning a single boolean counts as exactly one test unit
(`pointblank/validate.py:9244+`), so `GLOBAL_THRESHOLDS = pb.Thresholds(error=1)` still triggers
`assert_below_threshold` on a genuine duplicate, consistent with how the existing custom validators
already behave in production.

## Test plan

`has_no_duplicate_grain_rows()` (new tests in `tests/test_polars_utils/test_validation_actions.py`,
mocking `boto3.client` / `polars_utils.utils.write_to_parquet` per the existing `TestReportOnFail`
pattern):
- No duplicates → returns `True`; `write_to_parquet` not called; `delete_object` **is** called
  (stale-file cleanup).
- Duplicates present → returns `False`; `write_to_parquet` called with all instances of the
  duplicated rows.
- Duplicates exceeding `max_rows_to_extract` → extract truncated to the cap (test by passing a small
  override, e.g. `max_rows_to_extract=2`, against a small fixture — avoids needing a 1000+-row
  fixture).
- Two rows sharing a null in a grain column → treated as duplicates (default Polars equality
  semantics) — explicit test case.
- A row with a null grain value that appears only once → not flagged as a duplicate.

`validate_00_prepare.py` (update `tests/fargate/test_validate_00_prepare.py`): assert the assembled
validation now includes a `.specially()` step wired to `has_no_duplicate_grain_rows()` in place of
`.rows_distinct()`.

## Verification

1. Update `diag_02_rows_distinct_only.py` to use `has_no_duplicate_grain_rows()` in place of
   `.rows_distinct()`, matching the real fix exactly.
2. Redeploy the temporary `Ind-CQC-SLV-1814-oom-diag` state machine (same branch/workspace) and
   confirm `diag_02` now completes, with peak RSS comfortably under 60 GiB (expected: close to
   `diag_01`'s ~15.4 GB, plus a modest increment — nowhere near the ~370M-row-sized allocations the
   old `rows_distinct()` required).
3. Apply the same change to `validate_00_prepare.py`, run the updated unit tests, then redeploy the
   real pipeline end-to-end and confirm `validate_00_prepare.py` completes successfully.
4. Once confirmed, remove the temporary diagnostic scaffolding — `diag_0N_*.py` scripts,
   `fargate/utils/diag_helpers.py`, their `Dockerfile` `COPY` lines, and
   `terraform/pipeline/step-functions/dynamic/Ind-CQC-SLV-1814-oom-diag.json` — before merging 1814
   to `main`. None of it is meant to reach `main`.

## Alternatives considered but not chosen (retained for later)

Two other candidate replacements for `.rows_distinct()` were designed and rejected only in favour of
the one above — not because they're flawed, kept here in case they're wanted later:

**Option 1 alone (bypassing pointblank's `.specially()` entirely)** — the same `is_duplicated()` logic,
but run as a standalone check in `validate_00_prepare.py`'s `main()`, raising `AssertionError`
directly instead of going through pointblank's report/threshold framework:
```python
is_dup = source_df.select(GRAIN_COLUMNS).is_duplicated()
if is_dup.any():
    dup_rows = source_df.filter(is_dup)
    utils.write_to_parquet(
        dup_rows, f"s3://{bucket_name}/{reports_path}duplicate_grain_rows.parquet", append=False
    )
    raise AssertionError(f"Found {dup_rows.height} rows with duplicate grain in {source_path}")
```

**Option 2 (`group_by()` + semi-join, either standalone or wrapped in `.specially()`)** — mirrors the
`group_by`-and-count approach pointblank's own `rows_distinct()` uses internally, but stops there
instead of joining the count back onto every original row, using a `semi` join (row-filtering, no new
columns attached) for the extract instead of pointblank's `how="left"` (attaches a column to every
row):
```python
dup_groups = source_df.group_by(GRAIN_COLUMNS).len().filter(pl.col("len") > 1)
if dup_groups.height > 0:
    dup_rows = source_df.join(dup_groups.select(GRAIN_COLUMNS), on=GRAIN_COLUMNS, how="semi")
    utils.write_to_parquet(
        dup_rows, f"s3://{bucket_name}/{reports_path}duplicate_grain_rows.parquet", append=False
    )
    raise AssertionError(f"Found {dup_groups.height} duplicate grain groups in {source_path}")
```
This option is built entirely from primitives with existing precedent in this codebase
(`.group_by(...)` in 11 files; `how="semi"` in
`projects/_03_independent_cqc/_02_clean/fargate/utils/clean_ascwds_filled_post_outliers/null_grouped_providers.py:396-398`),
making it the lower-risk pick if `is_duplicated()` ever turns out to misbehave at scale and this needs
revisiting.
