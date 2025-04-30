import unittest
from unittest.mock import ANY, call, patch, Mock

import jobs.estimate_ind_cqc_filled_posts_by_job_role as job
from tests.test_file_data import EstimateIndCQCFilledPostsByJobRoleData as Data
from tests.test_file_schemas import EstimateIndCQCFilledPostsByJobRoleSchemas as Schemas
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]
PATCH_PATH = "jobs.estimate_ind_cqc_filled_posts_by_job_role"


class EstimateIndCQCFilledPostsByJobRoleTests(unittest.TestCase):
    ESTIMATE_SOURCE = "some/source"
    ASCWDS_WORKER_SOURCE = "some/other/source"
    OUTPUT_DIR = "some/destination"

    def setUp(self):
        self.spark = utils.get_spark()

        self.test_estimated_ind_cqc_filled_posts_df = self.spark.createDataFrame(
            Data.estimated_ind_cqc_filled_posts_rows,
            Schemas.estimated_ind_cqc_filled_posts_schema,
        )
        self.test_cleaned_ascwds_worker_df = self.spark.createDataFrame(
            Data.cleaned_ascwds_worker_rows, Schemas.cleaned_ascwds_worker_schema
        )


class NumericalValuesTests(EstimateIndCQCFilledPostsByJobRoleTests):
    def setUp(self) -> None:
        super().setUp()

    def test_number_of_days_in_window_value(self):
        self.assertEqual(job.NumericalValues.number_of_days_in_rolling_sum, 185)


class MainTests(EstimateIndCQCFilledPostsByJobRoleTests):
    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(f"{PATCH_PATH}.JRutils.recalculate_total_filled_posts")
    @patch(f"{PATCH_PATH}.JRutils.recalculate_managerial_filled_posts")
    @patch(
        f"{PATCH_PATH}.JRutils.calculate_sum_and_proportion_split_of_non_rm_managerial_estimate_posts"
    )
    @patch(
        f"{PATCH_PATH}.JRutils.calculate_difference_between_estimate_and_cqc_registered_managers"
    )
    @patch(f"{PATCH_PATH}.JRutils.count_registered_manager_names")
    @patch(f"{PATCH_PATH}.JRutils.unpack_mapped_column")
    @patch(f"{PATCH_PATH}.JRutils.create_estimate_filled_posts_by_job_role_map_column")
    @patch(f"{PATCH_PATH}.FPutils.merge_columns_in_order")
    @patch(f"{PATCH_PATH}.calculate_rolling_sum_of_job_roles")
    @patch(f"{PATCH_PATH}.JRutils.transform_interpolated_job_role_ratios_to_counts")
    @patch(f"{PATCH_PATH}.model_job_role_ratio_interpolation")
    @patch(f"{PATCH_PATH}.extrapolate_job_role_ratios")
    @patch(f"{PATCH_PATH}.JRutils.apply_quality_filters_to_ascwds_job_role_data")
    @patch(f"{PATCH_PATH}.JRutils.calculate_job_group_sum_from_job_role_map_column")
    @patch(f"{PATCH_PATH}.JRutils.transform_job_role_count_map_to_ratios_map")
    @patch(
        f"{PATCH_PATH}.JRutils.remove_ascwds_job_role_count_when_estimate_filled_posts_source_not_ascwds"
    )
    @patch(f"{PATCH_PATH}.JRutils.merge_dataframes")
    @patch(f"{PATCH_PATH}.JRutils.aggregate_ascwds_worker_job_roles_per_establishment")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    def test_main_function(
        self,
        read_from_parquet_mock: Mock,
        aggregate_ascwds_worker_job_roles_per_establishment_mock: Mock,
        merge_dataframes_mock: Mock,
        remove_ascwds_job_role_count_when_estimate_filled_posts_source_not_ascwds_mock: Mock,
        transform_job_role_count_map_to_ratios_map_mock: Mock,
        calculate_job_group_sum_from_job_role_map_column_mock: Mock,
        apply_quality_filters_to_ascwds_job_role_data_mock: Mock,
        model_job_role_ratio_interpolation_mock: Mock,
        extrapolate_job_role_ratios_mock: Mock,
        transform_interpolated_job_role_ratios_to_counts_mock: Mock,
        calculate_rolling_sum_of_job_roles_mock: Mock,
        merge_columns_in_order_mock: Mock,
        create_estimate_filled_posts_by_job_role_map_column_mock: Mock,
        unpack_mapped_column_mock: Mock,
        count_registered_manager_names_mock: Mock,
        calculate_difference_between_estimate_and_cqc_registered_managers_mock: Mock,
        calculate_sum_and_proportion_split_of_non_rm_managerial_estimate_posts_mock: Mock,
        recalculate_managerial_filled_posts_mock: Mock,
        recalculate_total_filled_posts_mock: Mock,
        write_to_parquet_mock: Mock,
    ):
        read_from_parquet_mock.side_effect = [
            self.test_estimated_ind_cqc_filled_posts_df,
            self.test_cleaned_ascwds_worker_df,
        ]
        job.main(self.ESTIMATE_SOURCE, self.ASCWDS_WORKER_SOURCE, self.OUTPUT_DIR)

        self.assertEqual(read_from_parquet_mock.call_count, 2)
        read_from_parquet_mock.assert_has_calls(
            [
                call(
                    self.ESTIMATE_SOURCE,
                    selected_columns=job.estimated_ind_cqc_filled_posts_columns_to_import,
                ),
                call(
                    self.ASCWDS_WORKER_SOURCE,
                    selected_columns=job.cleaned_ascwds_worker_columns_to_import,
                ),
            ]
        )
        aggregate_ascwds_worker_job_roles_per_establishment_mock.assert_called_once()
        merge_dataframes_mock.assert_called_once()
        remove_ascwds_job_role_count_when_estimate_filled_posts_source_not_ascwds_mock.assert_called_once()
        self.assertEqual(transform_job_role_count_map_to_ratios_map_mock.call_count, 3)
        self.assertEqual(
            calculate_job_group_sum_from_job_role_map_column_mock.call_count, 2
        )
        apply_quality_filters_to_ascwds_job_role_data_mock.assert_called_once()
        model_job_role_ratio_interpolation_mock.assert_called_once()
        extrapolate_job_role_ratios_mock.assert_called_once()
        transform_interpolated_job_role_ratios_to_counts_mock.assert_called_once()
        calculate_rolling_sum_of_job_roles_mock.assert_called_once()
        merge_columns_in_order_mock.assert_called_once()
        create_estimate_filled_posts_by_job_role_map_column_mock.assert_called_once()
        unpack_mapped_column_mock.assert_called_once()
        count_registered_manager_names_mock.assert_called_once()
        calculate_difference_between_estimate_and_cqc_registered_managers_mock.assert_called_once()
        calculate_sum_and_proportion_split_of_non_rm_managerial_estimate_posts_mock.assert_called_once()
        recalculate_managerial_filled_posts_mock.assert_called_once()
        recalculate_total_filled_posts_mock.assert_called_once()
        write_to_parquet_mock.assert_called_once_with(
            ANY, self.OUTPUT_DIR, "overwrite", PartitionKeys
        )
