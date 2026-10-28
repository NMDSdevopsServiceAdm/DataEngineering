# Categorical rolling average experiments

Scores variants of a categorical's rolling average (grouping splits × window lengths) on held-out known values. First used for employment status (ticket 2260). SageMaker only; nothing here runs in the pipeline.

| File | Use |
|---|---|
| `experiment.ipynb` | Test A (held-out workplaces) for every variant, then test B (hide each workplace's last 12 months) for the top few plus current. Writes one result file per variant to S3. |
| `compare.ipynb` | Loads the results and applies the win rule against the current setup. |
| `experiment_utils.py` | Variants, size band, fallback hierarchy, hold-out and scoring helpers. No repo tests. |

Scoring lives in `projects/_03_independent_cqc/utils/model_evaluation_utils.py` (`score_shares`, `calculate_coverage`, `mean_period_to_period_change`, `assign_location_folds`), with tests. The rolling average takes `group_columns` and `rolling_period` in the employment status `impute_utils.py`.

## Run order
0. Run `notebooks/sagemaker/sagemaker_setup.ipynb` (upload it once, set `BRANCH`). It clones the repo to `~/SageMaker/DataEngineering` or updates it to the latest push. The notebooks import `experiment_utils.py` from that clone. Save As the two notebooks out of the clone first: it is reset on every setup run. To keep a finished experiment, rename the clone folder (such as `Emp_Stat_rolling_avg`) and set `REPO_DIR` to it.
1. Set `DATA_BUCKET` (`sfc-<branch name>-datasets`) and `RESULTS_PREFIX` in `experiment.ipynb`. Trial first: `VARIANT_NAMES = ["current_6mo"]`, `RUN_LABEL = "trial"`. Then `VARIANT_NAMES = None` with a new `RUN_LABEL`. Rerun to resume; finished variants are skipped.
2. Run `compare.ipynb` with the same `RESULTS_PREFIX` and `RUN_LABEL`.
3. If a variant wins, change the pipeline's rolling average period and/or group columns and update the tests. If none wins, no pipeline change.

## How it works
- 24 variants: 6 splits × windows of 3, 6, 12 and 24 months. A split lists group columns finest to coarsest; the finest with at least 5 workplaces contributing on the row's date is used, else the next coarser, else the job role pooled. Every row gets a value.
- Size band (sum of job role estimates per workplace and date): <10, 10-49, 50-149, 150+, unknown.
- The data is tens of millions of rows, so rolling averages are built from per-cell totals (service, region, size band, job role, date, fold). Only known rows and a fixed random sample of workplaces (`SAMPLE_PERCENT`) are held in memory. Per-workplace jumpiness and all of test B's imputation use the sample. Results match a row-level run exactly.
- **Test A:** held-out workplaces (5 folds) add nothing to any average, then are scored on their own known shares.
- **Test B:** each workplace's last 12 months are hidden and the pipeline's imputation re-run. Each variant's averages are applied to the sample, imputed, and scored on the hidden values.
- Scores: MAE (pp), aggregate MAE (mean vs mean known share per service × region × role × date, pp), Brier, top-1 accuracy, skill against a pooled-mean baseline, coverage, and jumpiness (workplace and service × region × role aggregate).
- Win rule (`compare.ipynb`): at least 1pp lower MAE and no jumpier, or at least 0.5pp lower and clearly smoother; no worse on test B; coverage 1.0.

## Reusing for another categorical (such as gender)
1. Change only the "Settings for this categorical" block at the top of `experiment_utils.py`:
   - the location, date and grouping columns, and `SIZE_POSTS` for the size band;
   - `KNOWN`, `TREND`, `ROLLING`, `IMPUTED` share columns (same status order);
   - `add_trendline` and `add_imputed`, the pipeline steps for that categorical.
2. Edit `SPLITS` if the grouping dimensions differ.
3. Set `CLEANED_DATASET` and the paths in the notebook's parameters.
4. The cell-based rolling average repeats the employment status `add_rolling_avg_pcts` (each workplace counts once, window sums of totals, nearest average where there are no contributors). A categorical with a different rolling definition needs `rolling_averages` changed.
5. Check the result against a row-level run on a small sample before trusting it at scale.

## Result for employment status (ticket 2260)
No variant won; the pipeline is unchanged. The best on test A (`service_region_size_role_3mo`) improved MAE by 0.06pp (3.54 vs 3.60) and was about twice as jumpy; on test B current was best (1.27pp vs 1.34 to 1.37). Coarser splits and longer windows are smoother but slightly less accurate. Grouping and window barely matter: known workplaces are dominated by their own history. A gender run should start with a smaller grid.

## Assumptions
- The cleaned employment status data has region but no CSSR, so the aggregate jumpiness uses region (`AGGREGATE_GEOGRAPHY`).
- "Contributing workplaces" is counted on the row's own date, not across the window.
- A tied top share counts as the first status.

Left for a follow-up: workplace age or time-open features, and any real model.
