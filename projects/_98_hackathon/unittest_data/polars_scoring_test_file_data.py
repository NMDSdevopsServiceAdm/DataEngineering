from dataclasses import dataclass

import polars as pl

from projects._98_hackathon.unittest_data.polars_scoring_test_file_schemas import (
    ScoringSchemas as Schemas,
)

# (establishment_id, worker_id, is_leaver, valid_for_analysis): every test worker.
CHECK_ANSWER_ROWS = [
    ("1-001", "w1", True, True),
    ("1-001", "w2", False, True),
    ("1-002", "w3", None, False),
]

VALID_PREDICTION_ROWS = [
    ("1-001", "w1", 0.2),
    ("1-001", "w2", 0.8),
    ("1-002", "w3", 0.5),
]


@dataclass
class CheckPredictionsErrorCase:
    id: str
    rows: list[tuple]
    schema: pl.Schema
    error_match: str


@dataclass
class CalculateScoresCase:
    id: str
    answer_rows: list[tuple]
    prediction_rows: list[tuple]
    expected_scores: dict[str, float]


ONE_CLASS_ANSWER_ROWS = [
    ("1-001", "w1", False, True),
    ("1-001", "w2", False, True),
]
ONE_CLASS_PREDICTION_ROWS = [("1-001", "w1", 0.1), ("1-001", "w2", 0.2)]

# Two leavers and two stayers in one establishment.
FOUR_WORKER_ANSWER_ROWS = [
    ("1-001", "w1", True, True),
    ("1-001", "w2", False, True),
    ("1-001", "w3", True, True),
    ("1-001", "w4", False, True),
]


def four_worker_predictions(*probabilities: float) -> list[tuple]:
    """Returns predictions for the four workers in `FOUR_WORKER_ANSWER_ROWS`."""
    return [
        ("1-001", worker, probability)
        for worker, probability in zip(["w1", "w2", "w3", "w4"], probabilities)
    ]


class ScoringData:
    calculate_scores_cases = [
        CalculateScoresCase(
            id="auc_is_1_when_leavers_ranked_above_stayers",
            answer_rows=FOUR_WORKER_ANSWER_ROWS,
            prediction_rows=four_worker_predictions(0.9, 0.1, 0.8, 0.2),
            expected_scores={"worker_auc": 1.0},
        ),
        CalculateScoresCase(
            id="auc_is_half_when_probabilities_constant",
            answer_rows=FOUR_WORKER_ANSWER_ROWS,
            prediction_rows=four_worker_predictions(0.5, 0.5, 0.5, 0.5),
            expected_scores={"worker_auc": 0.5},
        ),
        CalculateScoresCase(
            id="brier_is_0_for_perfect_probabilities",
            answer_rows=FOUR_WORKER_ANSWER_ROWS,
            prediction_rows=four_worker_predictions(1.0, 0.0, 1.0, 0.0),
            expected_scores={"worker_brier": 0.0},
        ),
        CalculateScoresCase(
            id="brier_matches_hand_calculation",
            answer_rows=FOUR_WORKER_ANSWER_ROWS[:2],
            prediction_rows=[("1-001", "w1", 0.8), ("1-001", "w2", 0.2)],
            # ((1 - 0.8) ** 2 + (0 - 0.2) ** 2) / 2
            expected_scores={"worker_brier": 0.04},
        ),
        CalculateScoresCase(
            id="workplace_mae_is_0_when_summed_probability_matches_rate",
            answer_rows=FOUR_WORKER_ANSWER_ROWS
            + [("1-002", "w5", True, True), ("1-002", "w6", False, True)],
            prediction_rows=four_worker_predictions(0.5, 0.5, 0.5, 0.5)
            + [("1-002", "w5", 0.25), ("1-002", "w6", 0.75)],
            # Both establishments have an observed rate of 0.5 and summed
            # probability of 0.5 per worker.
            expected_scores={"workplace_mae": 0.0},
        ),
        CalculateScoresCase(
            id="workplace_mae_averages_error_across_workplaces",
            answer_rows=FOUR_WORKER_ANSWER_ROWS
            + [("1-002", "w5", False, True), ("1-002", "w6", False, True)],
            prediction_rows=four_worker_predictions(0.25, 0.25, 0.25, 0.25)
            + [("1-002", "w5", 0.5), ("1-002", "w6", 0.5)],
            # 1-001: |0.25 - 0.5| = 0.25. 1-002: |0.5 - 0| = 0.5.
            expected_scores={"workplace_mae": 0.375},
        ),
        CalculateScoresCase(
            id="excludes_workers_not_valid_for_analysis",
            answer_rows=FOUR_WORKER_ANSWER_ROWS[:2]
            + [("1-002", "w3", True, False), ("1-002", "w4", False, False)],
            prediction_rows=[
                ("1-001", "w1", 0.9),
                ("1-001", "w2", 0.1),
                ("1-002", "w3", 0.0),
                ("1-002", "w4", 1.0),
            ],
            expected_scores={"worker_auc": 1.0, "n_workers": 2, "n_workplaces": 1},
        ),
        CalculateScoresCase(
            id="excludes_workers_with_null_is_leaver",
            answer_rows=FOUR_WORKER_ANSWER_ROWS[:2]
            + [("1-002", "w3", None, True), ("1-002", "w4", None, True)],
            prediction_rows=[
                ("1-001", "w1", 0.9),
                ("1-001", "w2", 0.1),
                ("1-002", "w3", 0.0),
                ("1-002", "w4", 1.0),
            ],
            expected_scores={"worker_auc": 1.0, "n_workers": 2, "n_workplaces": 1},
        ),
    ]

    check_predictions_error_cases = [
        CheckPredictionsErrorCase(
            id="raises_when_required_column_missing",
            rows=[("1-001", "w1"), ("1-001", "w2"), ("1-002", "w3")],
            schema=Schemas.predictions_without_probability_schema,
            error_match="missing required columns",
        ),
        CheckPredictionsErrorCase(
            id="raises_when_unexpected_column_present",
            rows=[(e, w, p, 1) for e, w, p in VALID_PREDICTION_ROWS],
            schema=Schemas.predictions_with_extra_column_schema,
            error_match="unexpected columns",
        ),
        CheckPredictionsErrorCase(
            id="raises_when_key_is_null",
            rows=[("1-001", "w1", 0.2), ("1-001", None, 0.8), ("1-002", "w3", 0.5)],
            schema=Schemas.predictions_schema,
            error_match="1 null worker keys",
        ),
        CheckPredictionsErrorCase(
            id="raises_when_probability_is_null",
            rows=[("1-001", "w1", 0.2), ("1-001", "w2", None), ("1-002", "w3", 0.5)],
            schema=Schemas.predictions_schema,
            error_match="1 null or NaN probabilities",
        ),
        CheckPredictionsErrorCase(
            id="raises_when_probability_is_nan",
            rows=[
                ("1-001", "w1", 0.2),
                ("1-001", "w2", float("nan")),
                ("1-002", "w3", 0.5),
            ],
            schema=Schemas.predictions_schema,
            error_match="1 null or NaN probabilities",
        ),
        CheckPredictionsErrorCase(
            id="raises_when_probability_below_0",
            rows=[("1-001", "w1", 0.2), ("1-001", "w2", -0.1), ("1-002", "w3", 0.5)],
            schema=Schemas.predictions_schema,
            error_match="1 probabilities outside 0 to 1",
        ),
        CheckPredictionsErrorCase(
            id="raises_when_probability_above_1",
            rows=[("1-001", "w1", 0.2), ("1-001", "w2", 1.1), ("1-002", "w3", 0.5)],
            schema=Schemas.predictions_schema,
            error_match="1 probabilities outside 0 to 1",
        ),
        CheckPredictionsErrorCase(
            id="raises_when_worker_appears_twice",
            rows=VALID_PREDICTION_ROWS + [("1-001", "w1", 0.4)],
            schema=Schemas.predictions_schema,
            error_match="1 duplicated workers",
        ),
        CheckPredictionsErrorCase(
            id="raises_when_test_worker_missing",
            rows=VALID_PREDICTION_ROWS[:2],
            schema=Schemas.predictions_schema,
            error_match="missing 1 test workers",
        ),
        CheckPredictionsErrorCase(
            id="raises_when_worker_not_in_test_set",
            rows=VALID_PREDICTION_ROWS + [("1-009", "w9", 0.5)],
            schema=Schemas.predictions_schema,
            error_match="1 workers not in the test set",
        ),
    ]
