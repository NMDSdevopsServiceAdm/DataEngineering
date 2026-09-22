import unittest
from unittest.mock import ANY, Mock, call, patch

import polars as pl

import projects._03_independent_cqc._01_filled_posts._06_job_role_estimates.fargate._01_merge as job
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH = "projects._03_independent_cqc._01_filled_posts._06_job_role_estimates.fargate._01_merge"


class MainTests(unittest.TestCase):
    ESTIMATE_SOURCE = "some/source"
    ASCWDS_JOB_ROLE_COUNT_SOURCE = "some/other/source"
    MERGED_DATA_DESTINATION = "some/destination"
    METADATA_DESTINATION = "some/other/destination"

    mock_estimate_lf = pl.LazyFrame(
        schema=job.transformation_columns | job.metadata_columns
    )
    mock_prepared_job_role_counts_lf = pl.LazyFrame(schema=job.ascwds_columns_to_import)

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.join_estimates_to_ascwds")
    @patch(
        f"{PATCH_PATH}.utils.scan_parquet",
        side_effect=[mock_estimate_lf, mock_prepared_job_role_counts_lf],
    )
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        join_estimates_to_ascwds_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.ESTIMATE_SOURCE,
            self.ASCWDS_JOB_ROLE_COUNT_SOURCE,
            self.MERGED_DATA_DESTINATION,
            self.METADATA_DESTINATION,
        )

        self.assertEqual(scan_parquet_mock.call_count, 2)
        scan_parquet_mock.assert_has_calls(
            [
                call(self.ESTIMATE_SOURCE),
                call(self.ASCWDS_JOB_ROLE_COUNT_SOURCE),
            ]
        )

        join_estimates_to_ascwds_mock.assert_called_once()

        self.assertEqual(sink_to_parquet_mock.call_count, 2)

        sink_to_parquet_mock.assert_has_calls(
            [
                call(
                    lazy_df=ANY,
                    output_path=self.MERGED_DATA_DESTINATION,
                ),
                call(
                    lazy_df=ANY,
                    output_path=self.METADATA_DESTINATION,
                ),
            ]
        )


def test_metadata_columns_declares_care_home_as_the_care_home_enum_type():
    # care_home must stay the stricter CareHomeEnumType here, not a generic
    # Categorical - _02_employment_status/fargate/_01_merge.py joins this metadata
    # output against PIR/Capacity Tracker data whose care_home is CareHomeEnumType,
    # and a generic Categorical would raise a dtype mismatch on that join.
    assert job.metadata_columns[IndCQC.care_home] == CatColType.CareHomeEnumType
