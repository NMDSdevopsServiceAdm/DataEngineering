# Employment status model comparison (ticket 2262)

Compares models of the 5 employment status shares against the pipeline's rolling average. Experiment only: no pipeline change.

## Notebooks

Run in order on the `data-engineering-notebook` SageMaker instance (run `sagemaker_setup.ipynb` first; set `BRANCH` to `main`). Edit only each parameters cell.

| Notebook | Does |
| --- | --- |
| `01_dataset_and_baselines.ipynb` | Builds the modelling dataset (size band, dormancy level, as-of CQC rating, time registered, 5 folds), scores the as-is and fold-safe baselines, and **gates on reproducing 2260** (3.60pp held-out, 1.27pp hiding the last 12 months). |
| `02_feature_charts.ipynb` | Residual (share minus rolling average) by feature. Soft gate: stop and ask only if every feature is flat. |
| `03_models_and_scoring.ipynb` | Multinomial logit and Lasso (each feature alone, then all), gradient boosting as a ceiling, scoring and the win rule. |

Merged repo modules are imported from the clone; functions only on the unmerged 2260 and 2103 branches are copied into the notebooks.

## Decisions

- **Win:** at least 1pp lower held-out headline error than as-is, better in all 5 folds, and no worse on trend or the hidden-months guard.
- **Headline:** service x region x role x financial year, worker-weighted. CSSR is an extra table.
- **Jumpiness** is reported, never pass/fail; highlighted only when it degrades notably against the gain or as a tie-breaker.
- One pooled model across roles. Trained on `_pct_imputed`, scored on known shares only. The rolling average is a feature (not fold-safe), so as-is is the like-for-like baseline.

## Results

Not run yet. Add the verdict, which features helped and any jumpiness flags here after running notebook 3.
