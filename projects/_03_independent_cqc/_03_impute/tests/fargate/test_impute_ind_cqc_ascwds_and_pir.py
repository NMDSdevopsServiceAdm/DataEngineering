import unittest
from unittest.mock import ANY, Mock, patch

import polars as pl
import polars.testing as pl_testing

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
    @patch(f"{PATCH_PATH}.convert_pir_to_filled_posts")
    @patch(f"{PATCH_PATH}.utils.scan_parquet", return_value=mock_data)
    def test_main_runs_successfully(
        self,
        scan_parquet_mock: Mock,
        convert_pir_to_filled_posts_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):

        job.main(
            self.TEST_CLEANED_IND_CQC_DATA_SOURCE,
            self.TEST_DESTINATION,
        )

        scan_parquet_mock.assert_called_once()
        convert_pir_to_filled_posts_mock.assert_called_once()
        sink_to_parquet_mock.assert_called_once_with(
            ANY,
            self.TEST_DESTINATION,
            append=False,
        )


class CalculateRollingAverageTests(unittest.TestCase):
    def test_calculate_rolling_average_returns_expected_values(self):
        expected_lf = pl.LazyFrame(
            Data.expected_rolling_average_rows,
            Schemas.expected_rolling_average_schema,
            orient="row",
        )
        test_lf = expected_lf.drop(IndCQC.posts_rolling_average_model)
        returned_lf = test_lf.with_columns(
            job.calculate_rolling_average(
                column_to_average=IndCQC.ascwds_filled_posts_dedup_clean,
                period=Data.test_rolling_average_period,
                columns_to_partition_by=[IndCQC.location_id],
            ).alias(IndCQC.posts_rolling_average_model)
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)
