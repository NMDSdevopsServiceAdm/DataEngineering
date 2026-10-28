# Ticket 2260: employment status rolling average tuning

SageMaker only. Nothing here runs in the pipeline.

| File | Use |
|---|---|
| `experiment.ipynb` | Scores every split and window (test A), then the best plus current on a temporal hide (test B). Clones branch `2260-empstat-avg`. |
| `compare.ipynb` | Loads the results and applies the win rule against the current setup. |
| `experiment_utils.py` | Variants, size band, fallback hierarchy, hold-out and scoring helpers. No repo tests. |

## Run order
1. Set `DATA_BUCKET` and `RESULTS_PREFIX` in `experiment.ipynb`, then run top to bottom. One result file per variant is written under `RESULTS_PREFIX/RUN_LABEL/`; rerun to resume.
2. Run `compare.ipynb` with the same `RESULTS_PREFIX` and `RUN_LABEL`. Paste its win rule table and winners into the PR.
3. If a variant wins, change `ROLLING_AVERAGE_PERIOD` and/or the group columns in `_03_impute.py` and update the tests. If none wins, no pipeline change.

## Variants
6 splits by 4 windows (3, 6, 12, 24 months). Each split lists group columns finest to coarsest; the finest with at least 5 workplaces contributing on the row's date is used, else the next coarser, else the job role pooled.

- `current`: service, region, job role (no fallback; the 6 month window is the current setup)
- `current_with_fallback`: the same, falling back to service, job role, then job role
- `service_role`, `region_role`
- `service_size_role`: adds workplace size band
- `service_region_size_role`

Size bands (sum of job role estimates per workplace and date): <10, 10-49, 50-149, 150+, unknown.

## Assumptions
- The cleaned data has region but no CSSR, so the aggregate jumpiness check is service by region by job role. Set `AGGREGATE_GEOGRAPHY` in `experiment_utils.py` if CSSR is added.
- "Contributing workplaces" is counted on the row's own date, not across the window.
- Test A removes held-out workplaces' trendline shares from the averages, and scores them against their known (not imputed) shares.
- A tied top share counts as the first status.
- The data is tens of millions of rows, so rolling averages are built from per-cell totals (service, region, size band, job role, date, fold) and only known rows and a fixed random sample of workplaces (`SAMPLE_PERCENT`, default 10) are held in memory. Jumpiness per workplace and all of test B's imputation use that sample. Test A scores match a row-level run exactly.

Left for a follow-up ticket: workplace age or time-open features, and any real model.
