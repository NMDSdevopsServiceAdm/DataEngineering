# Ticket 1859: Job role rolling ratio bake-off

## Scope

Emit a small Tableau-chartable dataset comparing six variants of the upfront fill regime that feeds
the job role rolling ratio, so we can decide which one produces a trendline sound enough to
extrapolate against. Production `_03_impute` is not changed.

## Where this lands

`projects/_03_independent_cqc/_07_estimate_filled_posts_by_job_role/`

- `fargate/bakeoff_rolling_ratio.py` — entrypoint
- `fargate/utils/bakeoff_utils.py` — logic (local dataclass for throwaway column names)
- `tests/fargate/utils/test_bakeoff_utils.py`
- `terraform/pipeline/step-functions/dynamic/Ind-CQC-Filled-Post-Estimates-By-Role.json` — parallel branch

## Key decisions

- **Six variants.** `base` (today's production: post-weighted, positional uncapped interpolate,
  indefinite fill) then five unweighted + `interpolate_by` + 730d-capped variants differing only in
  edge fill: `indefinite`, `none`, `fill_6m` (±183d), `fill_12m` (±365d), `fill_24m` (±730d).
  `base → indefinite` isolates weighting/cap; `indefinite → the rest` isolates the fill.
- **Unweighted, not post-weighted.** The consumer is `estimate_filled_posts × ratio` per location, so
  the estimand is a per-workplace share. Post-weighting biases small workplaces toward large ones'
  composition, and the open-ended `NR 100 plus` bucket doesn't prevent that.
- **All limits are day-based**, not row-based — the date axis is monthly for ~3 FYs back and
  quarterly before that, so `.forward_fill(limit=n)` would mean different calendar spans per era.
- **Fill is symmetric and flat** (not size-based). Job role mix is stable regardless of headcount, so
  the size-based reasoning behind `SIZE_BASED_FORWARD_FILL_DAYS` doesn't transfer.
- **Fill is edge-only** — outside [first known, last known] per (location, role). This is what
  production already does, since `.interpolate()` fills every interior null.
- Diagnostics matter as much as the ratios: base provenance counts, plus `rolling_ratio_change_pp`
  (the number nominal extrapolation actually adds to a last known value).

## Data characteristics

- Grain is one row per (location, import date, job role); 37 roles; ~30k locations; ~160 import dates.
  Roughly 180M rows. Fargate task is 8 vCPU / 60 GB and OOM is a live concern.
- Counts are explicit `0`, not null, for roles a submitting workplace doesn't employ. Nulling in
  `_02_clean` is always all-37-roles-together.
- **Invariant:** for any (location, date), all 37 ratios are non-null or all 37 are null. This is what
  makes unweighted shares self-normalise to 1 and must be asserted in the output.
- Roughly 10% of workplaces change their data in a given import date; ~50% have submitted at some
  point. Indefinite ffill/bfill is what currently gets coverage to ~50% in every month.
- The rolling ratio output is only ~71k rows before crossing with variants/windows — tiny.

## Explicitly out of scope

- The nominal extrapolation itself, and the renormalise-to-1 follow-up.
- Negative clipping.
- Any change to production `_03_impute` or `_04_estimate`.
- Generalising `model_imputation` / `split_dataset_for_imputation` off their hardcoded
  `.over(location_id)` and `care_home` assumptions — needed for the real implementation, not for this.

## Open questions

- Whether the 730-day interpolation cap is right. Deliberately not made a variant; the
  `null_interior_rows` diagnostic measures what raising it would buy, and we decide from the charts.
- Whether the 12-way size-group split starves strata — the `_by_service` output is there to show it.

---
Remove this file before merging to main.
