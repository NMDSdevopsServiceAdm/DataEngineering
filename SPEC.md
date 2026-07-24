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
