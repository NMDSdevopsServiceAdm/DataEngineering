# Ticket 2011: turnover rate rolling average

SageMaker only. Nothing here runs in the pipeline. Does not use `score_shares`.

| File | Use |
|---|---|
| `experiment.ipynb` | Scores every split, window and fill cap (test A), then the top 3 with both methods on a temporal hide (test B). Clones branch `2011-leavers-imp`. |
| `compare.ipynb` | Loads the results and picks the setup with the lowest test B aggregate error. |
| `experiment_utils.py` | Variants, size band, hold-out and hide helpers, scoring. Tests in `test_experiment_utils.py`. |

## Run order
1. Set `BRANCH` and `RESULTS_PREFIX` in `experiment.ipynb`, then run top to bottom. One result file per variant is written under `RESULTS_PREFIX/RUN_LABEL/`; rerun to resume.
2. Run `compare.ipynb` with the same `RESULTS_PREFIX` and `RUN_LABEL`. Set `WINNER_NAME` in section 6 of `experiment.ipynb` and run it for the mean-of-rates baseline.
3. Set `ROLLING_AVERAGE_PERIOD`, `ROLLING_AVERAGE_GROUP_LEVELS`, `EXTRAPOLATION_PERIOD` and `EXTRAPOLATION_METHOD` in `_03_impute.py` from the winner, and paste the compare tables into the PR.

## Variants
6 splits by 5 windows (3, 6, 12, 24, 36 months) by 2 fill caps (12 months, 2 years) by 2 methods (nominal, ratio). Each split lists group columns finest to coarsest; the finest with at least 5 contributing workplaces on the row's date is used, else the next coarser. A contributor has a rate and at least 1 estimated employee.

- `current`: service, region, job role, falling back to service then job role
- `service_role`, `region_role`, `role` (pooled)
- `service_size_role`, `service_region_size_role`: add workplace size band

Size bands (sum of job role estimates per workplace and date): <10, 10-49, 50-149, 150+, unknown.

## Scoring
- **Aggregate error and bias (headline):** a service by CSSR by job role by date cell's rate is its leavers over its workers, for prediction and known. Cell differences are weighted by the cell's workers.
- **Workplace MAE and bias, jumpiness:** reported alongside. Jumpiness is the mean absolute change in the predicted rate between dates, per workplace and per aggregate.
- **Test A:** held-out workplaces' rates are removed before the trendline fill and averages, then scored on their known rates. The method does not affect the rolling average, so test A runs once per split, window and cap.
- **Test B:** each workplace's last 12 months with a known rate are hidden, the pipeline imputation runs, and the imputed rate is scored on the hidden values.
- Rows with a null rate or under 1 worker are not scored. The rate is not clipped above.
