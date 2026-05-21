import unittest
from unittest.mock import ANY, Mock, call, patch
from datetime import date

import polars as pl
import polars.testing as pltesting

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate._02_clean as job
from projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.utils import (
    CatagoricalColumnTypes as CatColType,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH = "projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate._02_clean"


class MainTests(unittest.TestCase):
    MERGED_DATA_SOURCE = "some/source"
    CLEANED_DATA_DESTINATION = "some/destination"

    mock_estimated_job_role_posts_lf = pl.LazyFrame()

    test_estimated_job_role_posts_lf = pl.LazyFrame(
        data={
            IndCQC.id_per_locationid_import_date: [1],
            IndCQC.location_id: ["loc1"],
            IndCQC.cqc_location_import_date: [date(2024, 1, 1)],
            IndCQC.primary_service_type: ["Care home with nursing"],
            IndCQC.ascwds_filled_posts_dedup_clean: [10.0],
            IndCQC.estimate_filled_posts: [10.0],
            IndCQC.estimate_filled_posts_source: ["ascwds_pir_merged"],
            IndCQC.ascwds_job_role_counts: [1],
            IndCQC.main_job_role_clean_labelled: ["care_worker"],
            IndCQC.registered_manager_names: [["Manager 1", "Manager 2"]],
        }
    )
    expected_estimated_job_role_posts_lf = pl.LazyFrame(
        data={
            IndCQC.id_per_locationid_import_date: [1],
            IndCQC.id_per_locationid_import_date_job_role: [0],
            IndCQC.location_id: ["loc1"],
            IndCQC.cqc_location_import_date: [date(2024, 1, 1)],
            IndCQC.primary_service_type: ["Care home with nursing"],
            IndCQC.estimate_filled_posts: [10.0],
            IndCQC.ascwds_job_role_counts: [1],
            IndCQC.main_job_role_clean_labelled: ["care_worker"],
            IndCQC.registered_manager_names: [["Manager 1", "Manager 2"]],
            IndCQC.job_group_dist_out_of_bounds: False,
        },
        schema={
            IndCQC.id_per_locationid_import_date: pl.UInt32,
            IndCQC.id_per_locationid_import_date_job_role: pl.UInt32,
            IndCQC.location_id: CatColType.LocationCatType,
            IndCQC.cqc_location_import_date: pl.Date,
            IndCQC.primary_service_type: CatColType.PrimaryServiceEnumType,
            IndCQC.estimate_filled_posts: pl.Float32,
            IndCQC.ascwds_job_role_counts: pl.Int16,
            IndCQC.main_job_role_clean_labelled: CatColType.JobRoleEnumType,
            IndCQC.registered_manager_names: pl.List(str),
            IndCQC.job_group_dist_out_of_bounds: pl.Boolean,
        },
    )

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.filter_job_role_group_outliers")
    @patch(f"{PATCH_PATH}.nullify_job_role_count_when_source_not_ascwds")
    @patch(
        f"{PATCH_PATH}.utils.scan_parquet",
        side_effect=[mock_estimated_job_role_posts_lf],
    )
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        nullify_job_role_count_when_source_not_ascwds_mock: Mock,
        filter_job_role_group_outliers_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(self.MERGED_DATA_SOURCE, self.CLEANED_DATA_DESTINATION)

        self.assertEqual(scan_parquet_mock.call_count, 1)
        scan_parquet_mock.assert_has_calls(
            [
                call(self.MERGED_DATA_SOURCE),
            ]
        )

        nullify_job_role_count_when_source_not_ascwds_mock.assert_called_once()
        filter_job_role_group_outliers_mock.assert_called_once()

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=ANY,
            output_path=self.CLEANED_DATA_DESTINATION,
            append=False,
        )

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(
        f"{PATCH_PATH}.utils.scan_parquet",
        side_effect=[test_estimated_job_role_posts_lf],
    )
    def test_main_runs_with_data(
        self,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(self.MERGED_DATA_SOURCE, self.CLEANED_DATA_DESTINATION)

        pltesting.assert_frame_equal(
            sink_to_parquet_mock.call_args.kwargs["lazy_df"],
            self.expected_estimated_job_role_posts_lf,
            check_column_order=False,
        )
