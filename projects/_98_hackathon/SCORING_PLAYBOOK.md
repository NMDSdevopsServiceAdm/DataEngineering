# Scoring your worker turnover predictions

Score your predictions with `score_predictions`. Every team uses the same function, so scores are comparable. It reads the held-back answers itself: you never need to open them.

## 1. Set up

TODO: write once this moves to the separate locked-down AWS account (how students get the function onto the instance and import `score_predictions`, and the answers bucket in `DEFAULT_ANSWERS_SOURCE`).

## 2. Build your predictions

One row per worker in the predict dataset, with exactly these three columns (any other column is rejected):

| Column | Meaning |
| --- | --- |
| `establishmentid` | Establishment id from the predict dataset |
| `workerid` | Worker id from the predict dataset |
| `predicted_probability` | Your probability that the worker leaves, from 0 to 1 |

Every worker needs exactly one row, with no nulls or NaNs. A polars DataFrame, polars LazyFrame or pandas DataFrame all work.

## 3. Score

```python
scores = score_predictions(predictions_df)
```

If the predictions are invalid, `score_predictions` raises a `ValueError` saying what is wrong, how many rows are affected and some example workers. Fix the predictions and run it again.

## 4. Read the scores

| Score | Better is | What it tells you |
| --- | --- | --- |
| `worker_auc` | Higher (0.5 is chance, 1 is perfect) | How well you rank leavers above stayers |
| `worker_brier` | Lower (0 is perfect) | How well calibrated your probabilities are |
| `workplace_mae` | Lower (0 is perfect) | How far each establishment's predicted leave rate is from its observed rate, averaged equally across establishments with at least 10 scored workers |

`n_workers` is the number of workers scored and `n_workplaces` the number of establishments used in `workplace_mae`. Only workers valid for analysis with a known outcome are scored, but you still need predictions for every worker in the predict dataset. Establishments with fewer than 10 scored workers are left out of `workplace_mae` because their observed leave rates are mostly noise, but their workers still count towards `worker_auc` and `worker_brier`.

The workplace leave rate is your summed probabilities divided by headcount, so calibrated probabilities matter as much as ranking.

## Common errors

| Message starts with | Fix |
| --- | --- |
| `Predictions are missing required columns` | Rename or add the three columns above |
| `Predictions have unexpected columns` | Drop everything except the three columns |
| `Predictions have N null worker keys` | Fill or drop rows with a missing id |
| `Predictions have N null, NaN or non-numeric probabilities` | Fill or fix them |
| `Predictions have N probabilities outside 0 to 1` | Clip or rescale to probabilities |
| `Predictions have N duplicated workers` | Keep one row per worker |
| `Predictions are missing N workers from the predict dataset` | Predict for every worker |
| `Predictions have N workers not in the predict dataset` | Remove them |
| `Answers have N duplicated workers` | Not your fault: report it to the organisers |
