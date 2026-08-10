import unittest
from dataclasses import dataclass
from unittest.mock import ANY, Mock, patch

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._03_impute.fargate.impute_ind_cqc_ascwds_and_pir as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    ImputeIndCqcAscwdsAndPirData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    ImputeIndCqcAscwdsAndPirSchema as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH = (
    "projects._03_independent_cqc._03_impute.fargate.impute_ind_cqc_ascwds_and_pir"
)


class ImputeIndCqcAscwdsAndPirTests(unittest.TestCase):
    TEST_CLEANED_IND_CQC_DATA_SOURCE = "some/directory"
    TEST_DESTINATION = "some/other/directory"

    mock_data = Mock(name="data")

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.nullify_ct_values_previous_to_first_submission")
    @patch(f"{PATCH_PATH}.model_imputation")
    @patch(f"{PATCH_PATH}.merge_ascwds_and_pir_filled_post_submissions")
    @patch(f"{PATCH_PATH}.convert_pir_to_filled_posts")
    @patch(f"{PATCH_PATH}.model_primary_service_rate_of_change_trendline")
    @patch(f"{PATCH_PATH}.cUtils.calculate_filled_posts_per_bed_ratio")
    @patch(f"{PATCH_PATH}.forward_fill_latest_known_value")
    @patch(f"{PATCH_PATH}.utils.scan_parquet", return_value=mock_data)
    def test_main_runs_successfully(
        self,
        scan_parquet_mock: Mock,
        forward_fill_latest_known_value_mock: Mock,
        calculate_filled_posts_per_bed_ratio_mock: Mock,
        model_roc_trendline_mock: Mock,
        convert_pir_to_filled_posts_mock: Mock,
        merge_ascwds_and_pir_filled_post_submissions_mock: Mock,
        model_imputation_mock: Mock,
        nullify_ct_values_previous_to_first_submission_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):

        job.main(
            self.TEST_CLEANED_IND_CQC_DATA_SOURCE,
            self.TEST_DESTINATION,
        )

        scan_parquet_mock.assert_called_once()
        self.assertEqual(forward_fill_latest_known_value_mock.call_count, 2)
        calculate_filled_posts_per_bed_ratio_mock.assert_called_once()
        self.assertEqual(model_roc_trendline_mock.call_count, 2)
        convert_pir_to_filled_posts_mock.assert_called_once()
        merge_ascwds_and_pir_filled_post_submissions_mock.assert_called_once()
        self.assertEqual(model_imputation_mock.call_count, 4)
        nullify_ct_values_previous_to_first_submission_mock.assert_called_once()
        sink_to_parquet_mock.assert_called_once_with(
            ANY,
            self.TEST_DESTINATION,
        )


@dataclass
class CalculateRollingAverageTestCase:
    id: str
    rows: list
    schema: pl.Schema
    column_to_average: str
    columns_to_partition_by: list

    def as_pytest_param(self):
        return pytest.param(self, id=self.id)


calculate_rolling_average_cases = [
    CalculateRollingAverageTestCase(
        id="single_partition_column_with_duplicate_group_and_date",
        rows=Data.expected_rolling_average_rows,
        schema=Schemas.expected_rolling_average_schema,
        column_to_average=IndCQC.ascwds_filled_posts_dedup_clean,
        columns_to_partition_by=[IndCQC.location_id],
    ),
    CalculateRollingAverageTestCase(
        id="multiple_partition_columns",
        rows=Data.expected_multiple_partition_columns_rolling_average_rows,
        schema=Schemas.expected_multiple_partition_columns_rolling_average_schema,
        column_to_average=IndCQC.imputed_filled_post_model,
        columns_to_partition_by=[
            IndCQC.primary_service_type,
            IndCQC.number_of_beds_banded_for_rolling_avg,
        ],
    ),
    CalculateRollingAverageTestCase(
        id="excludes_nulls_from_the_mean",
        rows=Data.expected_rolling_average_with_null_rows,
        schema=Schemas.expected_rolling_average_with_null_schema,
        column_to_average=IndCQC.ascwds_filled_posts_dedup_clean,
        columns_to_partition_by=[IndCQC.location_id],
    ),
    CalculateRollingAverageTestCase(
        id="preserves_other_columns_not_involved_in_the_calculation",
        rows=Data.expected_rolling_average_preserves_other_columns_rows,
        schema=Schemas.expected_rolling_average_preserves_other_columns_schema,
        column_to_average=IndCQC.ascwds_filled_posts_dedup_clean,
        columns_to_partition_by=[IndCQC.location_id],
    ),
]


class TestCalculateRollingAverage:
    @pytest.mark.parametrize(
        "case", [c.as_pytest_param() for c in calculate_rolling_average_cases]
    )
    def test_calculate_rolling_average_returns_expected_values(self, case):
        expected_lf = pl.LazyFrame(case.rows, case.schema, orient="row")
        test_lf = expected_lf.drop(IndCQC.posts_rolling_average_model)

        returned_lf = job.calculate_rolling_average(
            test_lf,
            column_to_average=case.column_to_average,
            period=Data.test_rolling_average_period,
            columns_to_partition_by=case.columns_to_partition_by,
            new_column_name=IndCQC.posts_rolling_average_model,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)

    def test_calculate_rolling_average_is_independent_of_input_row_order(self):
        expected_lf = pl.LazyFrame(
            Data.expected_rolling_average_rows,
            Schemas.expected_rolling_average_schema,
            orient="row",
        )
        test_lf = expected_lf.drop(IndCQC.posts_rolling_average_model).reverse()

        returned_lf = job.calculate_rolling_average(
            test_lf,
            column_to_average=IndCQC.ascwds_filled_posts_dedup_clean,
            period=Data.test_rolling_average_period,
            columns_to_partition_by=[IndCQC.location_id],
            new_column_name=IndCQC.posts_rolling_average_model,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)
