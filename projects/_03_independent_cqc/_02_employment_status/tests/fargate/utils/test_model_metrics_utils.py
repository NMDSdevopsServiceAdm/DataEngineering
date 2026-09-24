from typing import Any

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._02_employment_status.fargate.utils.model_metrics_utils as job
from projects._03_independent_cqc._02_employment_status.unittest_data.polars_employment_status_model_test_data import (
    ACTUAL_SHARES,
    PREDICTED_SHARES,
)
from projects._03_independent_cqc._02_employment_status.unittest_data.polars_employment_status_model_test_data import (
    TestModelMetricsUtilsData as Data,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    ShareModelColumns as ShareModel,
)

SCHEMA_OVERRIDES = {
    **dict.fromkeys(PREDICTED_SHARES + ACTUAL_SHARES, pl.Float32),
}


def to_lf(data: dict[str, Any]) -> pl.LazyFrame:
    """Build a LazyFrame, typing the share columns as in the pipeline."""
    return pl.LazyFrame(
        data,
        schema_overrides={
            col: dtype for col, dtype in SCHEMA_OVERRIDES.items() if col in data
        },
    )


class TestAggregateSharesByCell:
    @staticmethod
    def aggregate(case) -> pl.LazyFrame:
        return job.aggregate_shares_by_cell(
            to_lf(case.input_data),
            predicted_columns=PREDICTED_SHARES[:1],
            actual_columns=ACTUAL_SHARES[:1],
            weight_column=IndCQC.estimate_filled_posts_by_job_role,
            cell_columns=[IndCQC.primary_service_type],
        )

    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.cell_shares_are_worker_weighted_test_cases
        ],
    )
    def test_cell_shares_are_worker_weighted(self, case):
        pl_testing.assert_frame_equal(
            self.aggregate(case),
            pl.LazyFrame(case.expected_data),
            check_row_order=False,
        )

    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.unknown_rows_excluded_from_cells_test_cases
        ],
    )
    def test_unknown_rows_excluded_from_cells(self, case):
        pl_testing.assert_frame_equal(
            self.aggregate(case),
            pl.LazyFrame(case.expected_data),
            check_row_order=False,
        )


class TestScoreCellShares:
    @staticmethod
    def score(case, share_count: int, by_columns: list[str] | None = None):
        return job.score_cell_shares(
            pl.LazyFrame(case.input_data),
            predicted_columns=PREDICTED_SHARES[:share_count],
            actual_columns=ACTUAL_SHARES[:share_count],
            weight_column=ShareModel.cell_weight,
            by_columns=by_columns,
        )

    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.perfect_predictions_test_cases
        ],
    )
    def test_perfect_predictions_score_one_and_zero(self, case):
        pl_testing.assert_frame_equal(
            self.score(case, share_count=2),
            pl.LazyFrame(case.expected_data),
            check_row_order=False,
        )

    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.larger_cells_weigh_more_test_cases
        ],
    )
    def test_larger_cells_weigh_more(self, case):
        pl_testing.assert_frame_equal(
            self.score(case, share_count=1), pl.LazyFrame(case.expected_data)
        )

    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.cells_missing_a_share_test_cases
        ],
    )
    def test_cells_missing_a_share_are_not_scored(self, case):
        pl_testing.assert_frame_equal(
            self.score(case, share_count=1), pl.LazyFrame(case.expected_data)
        )

    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.scores_split_by_fold_test_cases
        ],
    )
    def test_scores_split_by_fold(self, case):
        pl_testing.assert_frame_equal(
            self.score(case, share_count=1, by_columns=[ShareModel.fold]),
            pl.LazyFrame(case.expected_data),
            check_row_order=False,
        )


class TestMeanPeriodToPeriodChange:
    @staticmethod
    def mean_change(case, share_count: int) -> pl.LazyFrame:
        return job.mean_period_to_period_change(
            pl.LazyFrame(case.input_data),
            share_columns=PREDICTED_SHARES[:share_count],
            partition_columns=[IndCQC.location_id, IndCQC.published_job_role_label],
            date_column=IndCQC.cqc_location_import_date,
        )

    @pytest.mark.parametrize(
        "case",
        [pytest.param(case, id=case.id) for case in Data.steady_predictions_test_cases],
    )
    def test_steady_predictions_have_zero_change(self, case):
        pl_testing.assert_frame_equal(
            self.mean_change(case, share_count=2), pl.LazyFrame(case.expected_data)
        )

    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.change_within_location_role_test_cases
        ],
    )
    def test_change_measured_within_location_role(self, case):
        pl_testing.assert_frame_equal(
            self.mean_change(case, share_count=1), pl.LazyFrame(case.expected_data)
        )
