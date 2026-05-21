import math

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._03_impute.fargate.utils.primary_service_rate_of_change as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    ModelRateOfChangeData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    ModelRateOfChangeSchemas as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class TestModelPrimaryServiceRateOfChangeTrendline:
    def test_banded_bed_threshold_match_expected_values(self):
        assert job.BANDED_BED_THRESHOLDS == [0, 1, 15, 25, math.inf]


class TestModelPrimaryServiceRateOfChangeTrendline:
    @pytest.mark.parametrize(
        "expected_data",
        [case.as_pytest_param() for case in Data.model_roc_trendline_test_cases],
    )
    def test_trendline_matches_expected(self, expected_data):
        expected_lf = pl.LazyFrame(
            expected_data,
            Schemas.expected_model_roc_trendline_schema,
            orient="row",
        )
        input_lf = expected_lf.drop(IndCQC.ascwds_rate_of_change_trendline_model)
        returned_lf = job.model_primary_service_rate_of_change_trendline(
            input_lf,
            value_col=IndCQC.combined_ratio_and_filled_posts,
            days=3,
            out_col=IndCQC.ascwds_rate_of_change_trendline_model,
            max_days_between_submissions=5,
        )

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
            abs_tol=0.001,
            check_row_order=False,
        )


class TestCalculateRollingSums:
    @pytest.mark.parametrize(
        "input_data, expected_data",
        [case.as_pytest_param() for case in Data.calculate_rolling_sums_test_cases],
    )
    def test_calculate_rolling_sums_returns_expected_output(
        self, input_data, expected_data
    ):
        expected_lf = pl.LazyFrame(
            expected_data,
            Schemas.expected_calculate_rolling_sums_schema,
            orient="row",
        )
        input_lf = pl.LazyFrame(
            input_data,
            Schemas.calculate_rolling_sums_schema,
            orient="row",
        )
        returned_lf = job.calculate_rolling_sums(
            input_lf,
            days=3,
            group_cols=[
                IndCQC.location_id,
                IndCQC.primary_service_type,
                IndCQC.number_of_beds_banded_roc,
            ],
        )
        pl_testing.assert_frame_equal(
            returned_lf, expected_lf, check_row_order=False, check_column_order=False
        )


class TestCleanNonResidentialRateOfChange:
    @pytest.mark.parametrize(
        "expected_data",
        [
            case.as_pytest_param()
            for case in Data.clean_non_residential_rate_of_change_test_cases
        ],
    )
    def test_clean_non_residential_rate_of_change_returns_expected_output(
        self, expected_data
    ):
        expected_lf = pl.LazyFrame(
            expected_data,
            Schemas.expected_clean_non_residential_rate_of_change_schema,
            orient="row",
        )
        input_lf = expected_lf.drop(
            job.TempCol.previous_period_cleaned, job.TempCol.current_period_cleaned
        )
        returned_lf = job.clean_non_residential_rate_of_change(input_lf)
        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
            check_row_order=False,
        )


class TestCalculateTrendline:
    @pytest.mark.parametrize(
        "input_data, expected_data",
        [case.as_pytest_param() for case in Data.calculate_trendline_test_cases],
    )
    def test_calculate_trendline_returns_expected_output(
        self, input_data, expected_data
    ):
        expected_lf = pl.LazyFrame(
            expected_data,
            Schemas.expected_calculate_trendline_schema,
            orient="row",
        )
        input_lf = pl.LazyFrame(
            input_data,
            Schemas.calculate_trendline_schema,
            orient="row",
        )
        returned_lf = job.calculate_trendline(
            input_lf,
            out_col=IndCQC.ascwds_rate_of_change_trendline_model,
            group_cols=[
                IndCQC.primary_service_type,
                IndCQC.number_of_beds_banded_roc,
            ],
        )
        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
            check_row_order=False,
        )
