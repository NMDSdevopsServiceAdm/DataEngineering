# Ticket 2166: filled posts experiments, round 1

SageMaker only, no code change. Upload the three notebooks.

| Notebook | Use |
|---|---|
| `checks.ipynb` | Run once. Month contiguity per feature dataset; chart of locations whose dormancy data starts in 2022. |
| `experiment.ipynb` | One run per `VARIANT` (settings cell lists the three modes). Clones branch `2166-fp-exp-1` into `repo-2166` (deleting the old `repo`) and records the commit. |
| `compare.ipynb` | Baseline vs variant per fold, the three rules, KEEP/REJECT. Also `n_iter_`, zero coefficients, reproduction check. |

## Run order
1. `checks.ipynb`. If months are not contiguous, `time_months_*` differ from the index for care homes too.
2. `alpha_0p001_ch` and `alpha_0p001_nr`: the baseline spec re-run. `compare.ipynb` must say "reproduces" before trusting anything else.
3. The rest of the variants (below).
4. Winners: run `VARIANT = "none"`, `RUN_LABEL = "baseline_seed43"`, `BASELINE_RUN_LABEL = None`, `FOLD_SEED = 43` once, then each winner with `RUN_LABEL = f"{VARIANT}_seed43"`, `BASELINE_RUN_LABEL = "baseline_seed43"`. Set `SUFFIX = "_seed43"` in `compare.ipynb`.
5. For a non-res winner, look once at the combined series chart (section 8, `non_res_combined_model`).

## Variants
- Time: `time_months_ch`, `time_months_nr`, `time_months_nr_nocubic`, `time_index_nr_nocubic` (control)
- Dormancy: `dormant_cap24_nr`, `dormant_cap36_nr`, `dormant_cap48_nr`
- Time registered: `time_registered_cap120_nr`
- Penalty: `maxiter10000_nr` first, then `alpha_{0p0001,0p001,0p01,0p1}_{ch,nr}`

## Assumptions to confirm
- `DATA_BUCKET = "sfc-2167-model-eval-datasets"` is a guess at the name.
- Months elapsed counts from 2020-01 (`ORIGIN`).
- `ever_dormant` is row-level (as of that date); months since dormant is 0 when never dormant.
- "No worse" trend uses the absolute pooled slope, per service.
- Each run refits everything (all three models, jumpiness, charts), so a run is as slow as the baseline.
