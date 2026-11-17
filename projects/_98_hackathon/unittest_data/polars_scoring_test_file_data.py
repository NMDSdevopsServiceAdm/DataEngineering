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


def build_workplace(
    establishment_id: str,
    leaver_probability: float,
    stayer_probability: float,
    n_leavers: int = 5,
    n_stayers: int = 5,
    valid_for_analysis: bool = True,
) -> tuple[list[tuple], list[tuple]]:
    """Returns answer and prediction rows for one workplace.

    Worker ids repeat across workplaces ("l0", "s0", ...). Leavers all get
    `leaver_probability` and stayers all get `stayer_probability`.
    """
    workers = [("l", True, leaver_probability, n_leavers)] + [
        ("s", False, stayer_probability, n_stayers)
    ]
    answer_rows = []
    prediction_rows = []
    for prefix, is_leaver, probability, count in workers:
        for number in range(count):
            worker_id = f"{prefix}{number}"
            answer_rows.append(
                (establishment_id, worker_id, is_leaver, valid_for_analysis)
            )
            prediction_rows.append((establishment_id, worker_id, probability))
    return answer_rows, prediction_rows


# Five leavers and five stayers in one establishment.
ONE_WORKPLACE_ANSWER_ROWS, ONE_WORKPLACE_PREDICTION_ROWS = build_workplace(
    "1-001", 0.9, 0.1
)

ONE_CLASS_ANSWER_ROWS = [
    ("1-001", "w1", False, True),
    ("1-001", "w2", False, True),
]
ONE_CLASS_PREDICTION_ROWS = [("1-001", "w1", 0.1), ("1-001", "w2", 0.2)]

# Workplaces of 10 workers are the smallest included in the workplace error.
# 1-001: predicted and observed leave rate both 0.5, so no workplace error.
RATE_MATCHED_ANSWERS, RATE_MATCHED_PREDICTIONS = build_workplace("1-001", 0.5, 0.5)

NINE_WORKER_ANSWERS, NINE_WORKER_PREDICTIONS = build_workplace(
    "1-002", 0.0, 0.0, n_leavers=4, n_stayers=5
)
TEN_WORKER_ANSWERS, TEN_WORKER_PREDICTIONS = build_workplace("1-002", 0.0, 0.0)

# One of ten workers is not valid for analysis, so 9 are scored.
ONE_INVALID_ANSWERS, ONE_INVALID_PREDICTIONS = build_workplace("1-002", 0.0, 0.0)
ONE_INVALID_ANSWERS[0] = (*ONE_INVALID_ANSWERS[0][:3], False)

NOT_VALID_ANSWERS, NOT_VALID_PREDICTIONS = build_workplace(
    "1-002", 0.0, 1.0, valid_for_analysis=False
)

UNKNOWN_LEAVER_ANSWERS, UNKNOWN_LEAVER_PREDICTIONS = build_workplace("1-002", 0.0, 1.0)
UNKNOWN_LEAVER_ANSWERS = [
    (establishment, worker, None, valid)
    for establishment, worker, _, valid in UNKNOWN_LEAVER_ANSWERS
]

SECOND_WORKPLACE_ANSWERS, SECOND_WORKPLACE_PREDICTIONS = build_workplace(
    "1-002", 0.8, 0.2
)


class ScoringData:
    calculate_scores_cases = [
        CalculateScoresCase(
            id="auc_is_1_when_leavers_ranked_above_stayers",
            answer_rows=ONE_WORKPLACE_ANSWER_ROWS,
            prediction_rows=ONE_WORKPLACE_PREDICTION_ROWS,
            expected_scores={"worker_auc": 1.0, "n_workers": 10, "n_workplaces": 1},
        ),
        CalculateScoresCase(
            id="auc_is_half_when_probabilities_constant",
            answer_rows=RATE_MATCHED_ANSWERS,
            prediction_rows=RATE_MATCHED_PREDICTIONS,
            expected_scores={"worker_auc": 0.5},
        ),
        CalculateScoresCase(
            id="brier_is_0_for_perfect_probabilities",
            answer_rows=ONE_WORKPLACE_ANSWER_ROWS,
            prediction_rows=build_workplace("1-001", 1.0, 0.0)[1],
            expected_scores={"worker_brier": 0.0},
        ),
        CalculateScoresCase(
            id="brier_matches_hand_calculation",
            answer_rows=ONE_WORKPLACE_ANSWER_ROWS,
            prediction_rows=build_workplace("1-001", 0.8, 0.2)[1],
            # Every worker is 0.2 from their outcome: 0.2 ** 2.
            expected_scores={"worker_brier": 0.04},
        ),
        CalculateScoresCase(
            id="workplace_mae_is_0_when_summed_probability_matches_rate",
            answer_rows=RATE_MATCHED_ANSWERS + build_workplace("1-002", 0.25, 0.75)[0],
            prediction_rows=RATE_MATCHED_PREDICTIONS
            + build_workplace("1-002", 0.25, 0.75)[1],
            # Both establishments have an observed rate of 0.5 and summed
            # probability of 5 over 10 workers.
            expected_scores={"workplace_mae": 0.0},
        ),
        CalculateScoresCase(
            id="workplace_mae_averages_error_across_workplaces",
            answer_rows=build_workplace("1-001", 0.25, 0.25)[0]
            + build_workplace("1-002", 0.5, 0.5, n_leavers=0, n_stayers=10)[0],
            prediction_rows=build_workplace("1-001", 0.25, 0.25)[1]
            + build_workplace("1-002", 0.5, 0.5, n_leavers=0, n_stayers=10)[1],
            # 1-001: |0.25 - 0.5| = 0.25. 1-002: |0.5 - 0| = 0.5.
            expected_scores={"workplace_mae": 0.375},
        ),
        CalculateScoresCase(
            id="excludes_workers_not_valid_for_analysis",
            answer_rows=ONE_WORKPLACE_ANSWER_ROWS + NOT_VALID_ANSWERS,
            prediction_rows=ONE_WORKPLACE_PREDICTION_ROWS + NOT_VALID_PREDICTIONS,
            expected_scores={"worker_auc": 1.0, "n_workers": 10, "n_workplaces": 1},
        ),
        CalculateScoresCase(
            id="excludes_workers_with_null_is_leaver",
            answer_rows=ONE_WORKPLACE_ANSWER_ROWS + UNKNOWN_LEAVER_ANSWERS,
            prediction_rows=ONE_WORKPLACE_PREDICTION_ROWS + UNKNOWN_LEAVER_PREDICTIONS,
            expected_scores={"worker_auc": 1.0, "n_workers": 10, "n_workplaces": 1},
        ),
        CalculateScoresCase(
            id="scores_same_worker_id_in_two_establishments_separately",
            answer_rows=ONE_WORKPLACE_ANSWER_ROWS + SECOND_WORKPLACE_ANSWERS,
            prediction_rows=ONE_WORKPLACE_PREDICTION_ROWS
            + SECOND_WORKPLACE_PREDICTIONS,
            expected_scores={"worker_auc": 1.0, "n_workers": 20, "n_workplaces": 2},
        ),
        CalculateScoresCase(
            id="excludes_workplace_with_9_scored_workers_from_workplace_error",
            answer_rows=RATE_MATCHED_ANSWERS + NINE_WORKER_ANSWERS,
            prediction_rows=RATE_MATCHED_PREDICTIONS + NINE_WORKER_PREDICTIONS,
            # Worker scores still include the 9: Brier is (10 * 0.25 + 4) / 19.
            expected_scores={
                "workplace_mae": 0.0,
                "worker_brier": 6.5 / 19,
                "n_workers": 19,
                "n_workplaces": 1,
            },
        ),
        CalculateScoresCase(
            id="includes_workplace_with_exactly_10_scored_workers",
            answer_rows=RATE_MATCHED_ANSWERS + TEN_WORKER_ANSWERS,
            prediction_rows=RATE_MATCHED_PREDICTIONS + TEN_WORKER_PREDICTIONS,
            # 1-001 error 0. 1-002: |0 - 0.5| = 0.5.
            expected_scores={"workplace_mae": 0.25, "n_workers": 20, "n_workplaces": 2},
        ),
        CalculateScoresCase(
            id="counts_only_scored_workers_towards_workplace_minimum",
            answer_rows=RATE_MATCHED_ANSWERS + ONE_INVALID_ANSWERS,
            prediction_rows=RATE_MATCHED_PREDICTIONS + ONE_INVALID_PREDICTIONS,
            expected_scores={"workplace_mae": 0.0, "n_workers": 19, "n_workplaces": 1},
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
            error_match="1 null, NaN or non-numeric probabilities",
        ),
        CheckPredictionsErrorCase(
            id="raises_when_probability_is_nan",
            rows=[
                ("1-001", "w1", 0.2),
                ("1-001", "w2", float("nan")),
                ("1-002", "w3", 0.5),
            ],
            schema=Schemas.predictions_schema,
            error_match="1 null, NaN or non-numeric probabilities",
        ),
        CheckPredictionsErrorCase(
            id="raises_when_probability_is_not_numeric",
            rows=[
                ("1-001", "w1", "0.2"),
                ("1-001", "w2", "abc"),
                ("1-002", "w3", "0.5"),
            ],
            schema=Schemas.predictions_with_text_probability_schema,
            error_match="1 null, NaN or non-numeric probabilities",
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
