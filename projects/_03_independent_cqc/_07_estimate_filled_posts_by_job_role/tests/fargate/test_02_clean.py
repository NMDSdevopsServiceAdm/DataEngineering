import unittest
from unittest.mock import ANY, Mock, call, patch

import polars as pl
import polars.testing as pltesting

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate._02_clean as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    EstimateFilledPostsByJobRoleCleanData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    EstimateFilledPostsByJobRoleCleanSchemas as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH = "projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate._02_clean"


class MainTests(unittest.TestCase):
    MERGED_DATA_SOURCE = "some/source"
    MERGED_METADATA_SOURCE = "some/metadata/source"
    CLEANED_DATA_DESTINATION = "some/destination"

    mock_estimated_job_role_posts_lf = pl.LazyFrame()

    test_estimated_job_role_posts_lf = pl.LazyFrame(Data.test_data, Schemas.test_schema)
    expected_estimated_job_role_posts_lf = pl.LazyFrame(
        Data.expected_data,
        Schemas.expected_schema,
    )
    metadata_lf = pl.LazyFrame(Data.metadata, Schemas.metadata_schema)

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.cUtils.filter_job_role_group_outliers")
    @patch(f"{PATCH_PATH}.cUtils.filter_job_role_group_equal_zero")
    @patch(f"{PATCH_PATH}.add_filtering_rule_column")
    @patch(f"{PATCH_PATH}.add_job_role_groups_column")
    @patch(f"{PATCH_PATH}.cUtils.nullify_job_role_count_when_source_not_ascwds")
    @patch(
        f"{PATCH_PATH}.utils.scan_parquet",
        side_effect=[mock_estimated_job_role_posts_lf, metadata_lf],
    )
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        nullify_job_role_count_when_source_not_ascwds_mock: Mock,
        add_job_roles_groups_mock: Mock,
        add_filtering_rule_column_mock: Mock,
        filter_job_role_group_equal_zero_mock: Mock,
        filter_job_role_group_outliers_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.MERGED_DATA_SOURCE,
            self.MERGED_METADATA_SOURCE,
            self.CLEANED_DATA_DESTINATION,
        )

        self.assertEqual(scan_parquet_mock.call_count, 2)
        scan_parquet_mock.assert_has_calls(
            [
                call(self.MERGED_DATA_SOURCE, schema=ANY, selected_columns=ANY),
                call(self.MERGED_METADATA_SOURCE, schema=ANY, selected_columns=ANY),
            ],
        )

        nullify_job_role_count_when_source_not_ascwds_mock.assert_called_once()
        add_job_roles_groups_mock.assert_called_once()
        add_filtering_rule_column_mock.assert_called_once()
        filter_job_role_group_equal_zero_mock.assert_called_once()
        filter_job_role_group_outliers_mock.assert_has_calls(
            [
                call(
                    ANY,
                    id_column=IndCQC.brand_id,
                    include_direct_care_lower_bound=False,
                ),
                call(ANY, id_column=IndCQC.provider_id),
                call(ANY, id_column=IndCQC.location_id),
            ],
            any_order=True,
        )

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=ANY,
            output_path=self.CLEANED_DATA_DESTINATION,
            append=False,
        )

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(
        f"{PATCH_PATH}.utils.scan_parquet",
        side_effect=[test_estimated_job_role_posts_lf, metadata_lf],
    )
    def test_main_runs_with_data(
        self,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.MERGED_DATA_SOURCE,
            self.MERGED_METADATA_SOURCE,
            self.CLEANED_DATA_DESTINATION,
        )

        pltesting.assert_frame_equal(
            sink_to_parquet_mock.call_args.kwargs["lazy_df"],
            self.expected_estimated_job_role_posts_lf,
            check_column_order=False,
        )
