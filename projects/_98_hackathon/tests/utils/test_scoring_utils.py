from unittest.mock import Mock, patch

import pandas as pd
import polars as pl
import pytest

import projects._98_hackathon.utils.scoring_utils as job
from projects._98_hackathon.unittest_data.polars_scoring_test_file_data import (
    CHECK_ANSWER_ROWS,
    FOUR_WORKER_ANSWER_ROWS,
    ONE_CLASS_ANSWER_ROWS,
    ONE_CLASS_PREDICTION_ROWS,
    VALID_PREDICTION_ROWS,
    four_worker_predictions,
)
from projects._98_hackathon.unittest_data.polars_scoring_test_file_data import (
    ScoringData as Data,
)
from projects._98_hackathon.unittest_data.polars_scoring_test_file_schemas import (
    ScoringSchemas as Schemas,
)
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.hackathon_columns import HackathonColumns as HackCols

PATCH_PATH = "projects._98_hackathon.utils.scoring_utils"


class TestCheckPredictions:
    answers_df = pl.DataFrame(
        CHECK_ANSWER_ROWS, schema=Schemas.answers_schema, orient="row"
    )

    def test_accepts_valid_predictions(self):
        predictions_df = pl.DataFrame(
            VALID_PREDICTION_ROWS, schema=Schemas.predictions_schema, orient="row"
        )

        assert job.check_predictions(predictions_df, self.answers_df) is None

    def test_accepts_probability_of_exactly_0_and_1(self):
        predictions_df = pl.DataFrame(
            [("1-001", "w1", 0.0), ("1-001", "w2", 1.0), ("1-002", "w3", 0.5)],
            schema=Schemas.predictions_schema,
            orient="row",
        )

        assert job.check_predictions(predictions_df, self.answers_df) is None

    @pytest.mark.parametrize(
        "case",
        [pytest.param(case, id=case.id) for case in Data.check_predictions_error_cases],
    )
    def test_raises_value_error_with_clear_message(self, case):
        predictions_df = pl.DataFrame(case.rows, schema=case.schema, orient="row")

        with pytest.raises(ValueError, match=case.error_match):
            job.check_predictions(predictions_df, self.answers_df)


class TestCalculateScores:
    @staticmethod
    def build_frames(
        answer_rows: list[tuple], prediction_rows: list[tuple]
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        return (
            pl.DataFrame(
                prediction_rows, schema=Schemas.predictions_schema, orient="row"
            ),
            pl.DataFrame(answer_rows, schema=Schemas.answers_schema, orient="row"),
        )

    @pytest.mark.parametrize(
        "case",
        [pytest.param(case, id=case.id) for case in Data.calculate_scores_cases],
    )
    def test_returns_expected_scores(self, case):
        predictions_df, answers_df = self.build_frames(
            case.answer_rows, case.prediction_rows
        )

        returned_scores = job.calculate_scores(predictions_df, answers_df)

        for name, expected_value in case.expected_scores.items():
            assert returned_scores[name] == pytest.approx(expected_value)

    def test_is_unchanged_by_row_order(self):
        case = Data.calculate_scores_cases[5]
        predictions_df, answers_df = self.build_frames(
            case.answer_rows, case.prediction_rows
        )

        returned_scores = job.calculate_scores(predictions_df, answers_df)
        shuffled_scores = job.calculate_scores(
            predictions_df.reverse(), answers_df.reverse()
        )

        assert shuffled_scores == pytest.approx(returned_scores)

    def test_raises_when_only_one_outcome_class(self):
        predictions_df, answers_df = self.build_frames(
            ONE_CLASS_ANSWER_ROWS, ONE_CLASS_PREDICTION_ROWS
        )

        with pytest.raises(ValueError, match="both leavers and stayers"):
            job.calculate_scores(predictions_df, answers_df)

    def test_returns_expected_keys_and_counts(self):
        case = Data.calculate_scores_cases[0]
        predictions_df, answers_df = self.build_frames(
            case.answer_rows, case.prediction_rows
        )

        returned_scores = job.calculate_scores(predictions_df, answers_df)

        assert set(returned_scores) == {
            "worker_auc",
            "worker_brier",
            "workplace_mae",
            "n_workers",
            "n_workplaces",
        }
        assert returned_scores["n_workers"] == 4
        assert returned_scores["n_workplaces"] == 1


class TestScorePredictions:
    prediction_rows = four_worker_predictions(0.9, 0.1, 0.8, 0.2)

    @staticmethod
    def build_answers_lf() -> pl.LazyFrame:
        return pl.LazyFrame(
            FOUR_WORKER_ANSWER_ROWS, schema=Schemas.answers_schema, orient="row"
        )

    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_reads_answers_from_test_dataset_with_needed_columns_only(
        self, scan_parquet_mock: Mock
    ):
        scan_parquet_mock.return_value = self.build_answers_lf()
        predictions_df = pl.DataFrame(
            self.prediction_rows, schema=Schemas.predictions_schema, orient="row"
        )

        job.score_predictions(predictions_df)

        scan_parquet_mock.assert_called_once_with(
            "s3://<hackathon-bucket>/domain=98_hackathon/dataset=worker_turnover_test/",
            selected_columns=[
                AWKClean.establishment_id,
                AWKClean.worker_id,
                HackCols.is_leaver,
                HackCols.valid_for_analysis,
            ],
        )

    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_accepts_pandas_dataframe(self, scan_parquet_mock: Mock):
        scan_parquet_mock.return_value = self.build_answers_lf()
        predictions_pd = pd.DataFrame(
            self.prediction_rows,
            columns=list(Schemas.predictions_schema.names()),
        )

        returned_scores = job.score_predictions(predictions_pd)

        assert returned_scores["worker_auc"] == pytest.approx(1.0)

    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_accepts_polars_lazyframe(self, scan_parquet_mock: Mock):
        scan_parquet_mock.return_value = self.build_answers_lf()
        predictions_lf = pl.LazyFrame(
            self.prediction_rows, schema=Schemas.predictions_schema, orient="row"
        )

        returned_scores = job.score_predictions(predictions_lf)

        assert returned_scores["worker_auc"] == pytest.approx(1.0)

    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_returns_scores_for_valid_predictions(self, scan_parquet_mock: Mock):
        scan_parquet_mock.return_value = self.build_answers_lf()
        predictions_df = pl.DataFrame(
            self.prediction_rows, schema=Schemas.predictions_schema, orient="row"
        )

        returned_scores = job.score_predictions(predictions_df)

        assert returned_scores == pytest.approx(
            {
                "worker_auc": 1.0,
                "worker_brier": 0.025,  # (0.1 ** 2 + 0.1 ** 2 + 0.2 ** 2 + 0.2 ** 2) / 4
                "workplace_mae": 0.0,  # predicted 2.0 / 4 against observed 0.5
                "n_workers": 4,
                "n_workplaces": 1,
            }
        )
