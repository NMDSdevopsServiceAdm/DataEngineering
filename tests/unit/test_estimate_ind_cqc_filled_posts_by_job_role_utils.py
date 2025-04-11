import unittest
from unittest.mock import patch, Mock

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.estimate_filled_posts_by_job_role_utils import utils as job
from utils.estimate_filled_posts_by_job_role_utils.models import interpolation as interp
from tests.test_file_data import EstimateIndCQCFilledPostsByJobRoleUtilsData as Data
from tests.test_file_schemas import (
    EstimateIndCQCFilledPostsByJobRoleUtilsSchemas as Schemas,
)

import pyspark.sql.functions as F


class EstimateIndCQCFilledPostsByJobRoleUtilsTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()


class AggregateAscwdsWorkerJobRolesPerEstablishmentTests(
    EstimateIndCQCFilledPostsByJobRoleUtilsTests
):
    def setUp(self) -> None:
        super().setUp()

    def test_aggregate_ascwds_worker_job_roles_per_establishment_returns_expected_columns(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.aggregate_ascwds_worker_job_roles_per_establishment_rows,
            Schemas.aggregate_ascwds_worker_with_additional_column_schema,
        )
        returned_df = job.aggregate_ascwds_worker_job_roles_per_establishment(
            test_df, Data.list_of_job_roles_for_tests
        )
        expected_df = self.spark.createDataFrame(
            [],
            Schemas.expected_aggregate_ascwds_worker_schema,
        )
        self.assertEqual(returned_df.columns, expected_df.columns)

    def test_aggregate_ascwds_worker_job_roles_per_establishment_returns_expected_data_when_all_job_roles_present(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.aggregate_ascwds_worker_job_roles_per_establishment_when_all_job_roles_present_rows,
            Schemas.aggregate_ascwds_worker_schema,
        )
        returned_df = job.aggregate_ascwds_worker_job_roles_per_establishment(
            test_df, Data.list_of_job_roles_for_tests
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_aggregate_ascwds_worker_job_roles_per_establishment_when_all_job_roles_present_rows,
            Schemas.expected_aggregate_ascwds_worker_schema,
        )
        returned_data = returned_df.sort(
            IndCQC.establishment_id, IndCQC.ascwds_worker_import_date
        ).collect()
        expected_data = expected_df.collect()

        for i in range(len(returned_data)):
            self.assertEqual(
                returned_data[i][IndCQC.ascwds_job_role_counts],
                expected_data[i][IndCQC.ascwds_job_role_counts],
                f"Returned row {i} does not match expected",
            )

    def test_aggregate_ascwds_worker_job_roles_per_establishment_returns_expected_data_when_some_job_roles_never_present(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.aggregate_ascwds_worker_job_roles_per_establishment_when_some_job_roles_never_present_rows,
            Schemas.aggregate_ascwds_worker_schema,
        )
        returned_df = job.aggregate_ascwds_worker_job_roles_per_establishment(
            test_df, Data.list_of_job_roles_for_tests
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_aggregate_ascwds_worker_job_roles_per_establishment_when_some_job_roles_never_present_rows,
            Schemas.expected_aggregate_ascwds_worker_schema,
        )
        returned_data = returned_df.collect()
        expected_data = expected_df.collect()

        self.assertEqual(
            returned_data[0][IndCQC.ascwds_job_role_counts],
            expected_data[0][IndCQC.ascwds_job_role_counts],
        )

    def test_aggregate_ascwds_worker_job_roles_per_establishment_returns_null_values_replaced_with_zeroes(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.aggregate_ascwds_worker_job_roles_per_establishment_missing_roles_replaced_with_zero_rows,
            Schemas.aggregate_ascwds_worker_schema,
        )
        returned_df = job.aggregate_ascwds_worker_job_roles_per_establishment(
            test_df, Data.list_of_job_roles_for_tests
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_aggregate_ascwds_worker_job_roles_per_establishment_missing_roles_replaced_with_zero_rows,
            Schemas.expected_aggregate_ascwds_worker_schema,
        )
        returned_data = returned_df.collect()
        expected_data = expected_df.collect()

        self.assertEqual(
            returned_data[0][IndCQC.ascwds_job_role_counts],
            expected_data[0][IndCQC.ascwds_job_role_counts],
        )

    def test_aggregate_ascwds_worker_job_roles_per_establishment_returns_expected_data_when_single_establishment_has_multiple_dates(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.aggregate_ascwds_worker_job_roles_per_establishment_when_single_establishment_has_multiple_dates_rows,
            Schemas.aggregate_ascwds_worker_schema,
        )
        returned_df = job.aggregate_ascwds_worker_job_roles_per_establishment(
            test_df, Data.list_of_job_roles_for_tests
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_aggregate_ascwds_worker_job_roles_per_establishment_when_single_establishment_has_multiple_dates_rows,
            Schemas.expected_aggregate_ascwds_worker_schema,
        )
        returned_data = returned_df.sort(
            IndCQC.establishment_id, IndCQC.ascwds_worker_import_date
        ).collect()
        expected_data = expected_df.collect()

        for i in range(len(returned_data)):
            self.assertEqual(
                returned_data[i][IndCQC.ascwds_job_role_counts],
                expected_data[i][IndCQC.ascwds_job_role_counts],
                f"Returned row {i} does not match expected",
            )

    def test_aggregate_ascwds_worker_job_roles_per_establishment_returns_expected_data_when_multiple_establishments_on_the_same_date(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.aggregate_ascwds_worker_job_roles_per_establishment_when_multiple_establishments_on_the_same_date_rows,
            Schemas.aggregate_ascwds_worker_schema,
        )
        returned_df = job.aggregate_ascwds_worker_job_roles_per_establishment(
            test_df, Data.list_of_job_roles_for_tests
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_aggregate_ascwds_worker_job_roles_per_establishment_when_multiple_establishments_on_the_same_date_rows,
            Schemas.expected_aggregate_ascwds_worker_schema,
        )
        returned_data = returned_df.sort(
            IndCQC.establishment_id, IndCQC.ascwds_worker_import_date
        ).collect()
        expected_data = expected_df.collect()

        for i in range(len(returned_data)):
            self.assertEqual(
                returned_data[i][IndCQC.ascwds_job_role_counts],
                expected_data[i][IndCQC.ascwds_job_role_counts],
                f"Returned row {i} does not match expected",
            )

    def test_aggregate_ascwds_worker_job_roles_per_establishment_returns_expected_data_when_unrecognised_role_present(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.aggregate_ascwds_worker_job_roles_per_establishment_when_unrecognised_role_present_rows,
            Schemas.aggregate_ascwds_worker_schema,
        )
        returned_df = job.aggregate_ascwds_worker_job_roles_per_establishment(
            test_df, Data.list_of_job_roles_for_tests
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_aggregate_ascwds_worker_job_roles_per_establishment_when_unrecognised_role_present_rows,
            Schemas.expected_aggregate_ascwds_worker_schema,
        )
        returned_data = returned_df.collect()
        expected_data = expected_df.collect()

        self.assertEqual(
            returned_data[0][IndCQC.ascwds_job_role_counts],
            expected_data[0][IndCQC.ascwds_job_role_counts],
        )


class CreateMapColumnTests(EstimateIndCQCFilledPostsByJobRoleUtilsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_create_map_column_when_all_columns_populated(self):
        test_df = self.spark.createDataFrame(
            Data.create_map_column_when_all_columns_populated_rows,
            Schemas.create_map_column_schema,
        )
        returned_df = job.create_map_column(
            test_df,
            Data.list_of_job_roles_for_tests,
            Schemas.test_map_column,
            drop_original_columns=False,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_create_map_column_when_all_columns_populated_rows,
            Schemas.expected_create_map_column_schema,
        )
        returned_data = returned_df.collect()
        expected_data = expected_df.collect()

        self.assertEqual(
            returned_data[0][Schemas.test_map_column],
            expected_data[0][Schemas.test_map_column],
        )

    def test_create_map_column_when_some_columns_populated(self):
        test_df = self.spark.createDataFrame(
            Data.create_map_column_when_some_columns_populated_rows,
            Schemas.create_map_column_schema,
        )
        returned_df = job.create_map_column(
            test_df,
            Data.list_of_job_roles_for_tests,
            Schemas.test_map_column,
            drop_original_columns=False,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_create_map_column_when_some_columns_populated_rows,
            Schemas.expected_create_map_column_schema,
        )
        returned_data = returned_df.collect()
        expected_data = expected_df.collect()

        self.assertEqual(
            returned_data[0][Schemas.test_map_column],
            expected_data[0][Schemas.test_map_column],
        )

    def test_create_map_column_when_no_columns_populated(self):
        test_df = self.spark.createDataFrame(
            Data.create_map_column_when_no_columns_populated_rows,
            Schemas.create_map_column_schema,
        )
        returned_df = job.create_map_column(
            test_df,
            Data.list_of_job_roles_for_tests,
            Schemas.test_map_column,
            drop_original_columns=False,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_create_map_column_when_no_columns_populated_rows,
            Schemas.expected_create_map_column_schema,
        )
        returned_data = returned_df.collect()
        expected_data = expected_df.collect()

        self.assertEqual(
            returned_data[0][Schemas.test_map_column],
            expected_data[0][Schemas.test_map_column],
        )

    def test_create_map_column_drops_columns_when_drop_original_columns_is_true(self):
        test_df = self.spark.createDataFrame(
            Data.create_map_column_when_all_columns_populated_rows,
            Schemas.create_map_column_schema,
        )
        returned_df = job.create_map_column(
            test_df,
            Data.list_of_job_roles_for_tests,
            Schemas.test_map_column,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_create_map_column_when_all_columns_populated_and_drop_columns_is_true_rows,
            Schemas.expected_create_map_column_when_drop_columns_is_true_schema,
        )
        returned_data = returned_df.collect()
        expected_data = expected_df.collect()

        self.assertEqual(
            returned_data[0][Schemas.test_map_column],
            expected_data[0][Schemas.test_map_column],
        )


class MergeDataframesTests(EstimateIndCQCFilledPostsByJobRoleUtilsTests):
    def setUp(self) -> None:
        super().setUp()

        self.estimated_filled_posts_df = self.spark.createDataFrame(
            Data.estimated_filled_posts_when_single_establishment_has_multiple_dates_rows,
            Schemas.estimated_filled_posts_schema,
        )
        aggregated_job_role_breakdown_df = self.spark.createDataFrame(
            Data.aggregated_job_role_breakdown_when_single_establishment_has_multiple_dates_rows,
            Schemas.aggregated_job_role_breakdown_df,
        )
        self.returned_df = job.merge_dataframes(
            self.estimated_filled_posts_df, aggregated_job_role_breakdown_df
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_merge_dataframse_when_single_establishment_has_multiple_dates_rows,
            Schemas.merged_job_role_estimate_schema,
        )

    def test_merge_dataframes_returns_expected_columns(
        self,
    ):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_merge_dataframes_returns_same_row_count_as_original_estimated_filled_posts_df(
        self,
    ):
        self.assertEqual(
            self.returned_df.count(), self.estimated_filled_posts_df.count()
        )

    def test_merge_dataframes_when_single_establishment_has_multiple_dates(
        self,
    ):
        returned_data = self.returned_df.sort(
            IndCQC.establishment_id, IndCQC.ascwds_workplace_import_date
        ).collect()
        expected_data = self.expected_df.collect()

        for i in range(len(returned_data)):
            self.assertEqual(
                returned_data[i],
                expected_data[i],
                f"Returned row {i} does not match expected",
            )

    def test_merge_dataframes_when_multiple_establishments_on_the_same_date(
        self,
    ):
        estimated_filled_posts_df = self.spark.createDataFrame(
            Data.estimated_filled_posts_when_multiple_establishments_on_the_same_date_rows,
            Schemas.estimated_filled_posts_schema,
        )
        aggregated_job_role_breakdown_df = self.spark.createDataFrame(
            Data.aggregated_job_role_breakdown_when_multiple_establishments_on_the_same_date_rows,
            Schemas.aggregated_job_role_breakdown_df,
        )
        returned_df = job.merge_dataframes(
            estimated_filled_posts_df, aggregated_job_role_breakdown_df
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_merge_dataframse_when_multiple_establishments_on_the_same_date_rows,
            Schemas.merged_job_role_estimate_schema,
        )
        returned_data = returned_df.sort(
            IndCQC.establishment_id, IndCQC.ascwds_workplace_import_date
        ).collect()
        expected_data = expected_df.collect()

        for i in range(len(returned_data)):
            self.assertEqual(
                returned_data[i],
                expected_data[i],
                f"Returned row {i} does not match expected",
            )

    def test_merge_dataframes_when_multiple_establishments_do_not_match(
        self,
    ):
        estimated_filled_posts_df = self.spark.createDataFrame(
            Data.estimated_filled_posts_when_establishments_do_not_match_rows,
            Schemas.estimated_filled_posts_schema,
        )
        aggregated_job_role_breakdown_df = self.spark.createDataFrame(
            Data.aggregated_job_role_breakdown_when_establishments_do_not_match_rows,
            Schemas.aggregated_job_role_breakdown_df,
        )
        returned_df = job.merge_dataframes(
            estimated_filled_posts_df, aggregated_job_role_breakdown_df
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_merge_dataframse_when_establishments_do_not_match_rows,
            Schemas.merged_job_role_estimate_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())


class TransformJobRoleCountsMapToRatiosMap(
    EstimateIndCQCFilledPostsByJobRoleUtilsTests
):
    def setUp(self) -> None:
        super().setUp()

        self.test_df = self.spark.createDataFrame(
            Data.create_total_from_values_in_map_column_when_all_count_values_above_zero_rows,
            Schemas.create_total_from_values_in_map_column_schema,
        )
        self.returned_df = job.transform_job_role_count_map_to_ratios_map(
            self.test_df,
            IndCQC.ascwds_job_role_counts,
            IndCQC.ascwds_job_role_ratios,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_create_ratios_from_counts_when_all_count_values_above_zero_rows,
            Schemas.expected_ascwds_job_role_count_map_to_ratios_map_schema,
        )

        self.columns_added_by_function = [
            column
            for column in self.returned_df.columns
            if column not in self.test_df.columns
        ]

    @patch(
        "utils.estimate_filled_posts_by_job_role_utils.utils.create_ratios_map_from_count_map_and_total"
    )
    @patch(
        "utils.estimate_filled_posts_by_job_role_utils.utils.calculate_total_sum_of_values_in_a_map_column"
    )
    def test_transform_job_role_count_map_to_ratios_map_function(
        self,
        calculate_total_sum_of_values_in_a_map_column: Mock,
        create_ratios_map_from_count_map_and_total_mock: Mock,
    ):
        job.transform_job_role_count_map_to_ratios_map(
            self.test_df,
            IndCQC.ascwds_job_role_counts,
            IndCQC.ascwds_job_role_ratios,
        )
        calculate_total_sum_of_values_in_a_map_column.assert_called_once()
        create_ratios_map_from_count_map_and_total_mock.assert_called_once()

    def test_transform_job_role_count_map_to_ratios_map_only_adds_one_column(self):
        self.assertEqual(len(self.columns_added_by_function), 1)

    def test_transform_job_role_count_map_to_ratios_map_adds_correctly_named_column(
        self,
    ):
        self.assertEqual(
            self.columns_added_by_function[0], IndCQC.ascwds_job_role_ratios
        )


class CreateTotalFromValuesInMapColumn(EstimateIndCQCFilledPostsByJobRoleUtilsTests):
    def setUp(self) -> None:
        super().setUp()

        self.test_df = self.spark.createDataFrame(
            Data.create_total_from_values_in_map_column_when_all_count_values_above_zero_rows,
            Schemas.create_total_from_values_in_map_column_schema,
        )
        self.returned_df = job.calculate_total_sum_of_values_in_a_map_column(
            self.test_df,
            IndCQC.ascwds_job_role_counts,
            Data.temp_total_count_of_worker_records,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_create_total_from_values_in_map_column_when_all_count_values_above_zero_rows,
            Schemas.expected_create_total_from_values_in_map_column_schema,
        )

        self.added_columns = [
            column
            for column in self.returned_df.columns
            if column not in self.test_df.columns
        ]

    def test_create_total_from_values_in_map_adds_one_column(
        self,
    ):
        self.assertEqual(len(self.added_columns), 1)

    def test_create_total_from_values_in_map_gives_new_column_expected_name(
        self,
    ):
        self.assertEqual(
            self.added_columns[0],
            Data.temp_total_count_of_worker_records,
        )

    def test_create_total_from_values_in_map_returns_expected_value_when_all_count_values_above_zero(
        self,
    ):
        self.assertEqual(self.returned_df.collect(), self.expected_df.collect())

    def test_create_total_from_values_in_map_returns_null_when_all_count_values_are_null(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.create_total_from_values_in_map_column_when_all_count_values_are_null_rows,
            Schemas.create_total_from_values_in_map_column_schema,
        )
        returned_df = job.calculate_total_sum_of_values_in_a_map_column(
            test_df,
            IndCQC.ascwds_job_role_counts,
            Data.temp_total_count_of_worker_records,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_create_total_from_values_in_map_column_when_all_count_values_are_null_rows,
            Schemas.expected_create_total_from_values_in_map_column_schema,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_create_total_from_values_in_map_returns_null_when_count_column_is_null(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.create_total_from_values_in_map_column_when_count_column_is_null_rows,
            Schemas.create_total_from_values_in_map_column_schema,
        )
        returned_df = job.calculate_total_sum_of_values_in_a_map_column(
            test_df,
            IndCQC.ascwds_job_role_counts,
            Data.temp_total_count_of_worker_records,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_create_total_from_values_in_map_column_when_count_column_is_null_rows,
            Schemas.expected_create_total_from_values_in_map_column_schema,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())


class CreateRatiosMapFromCountMapAndTotal(EstimateIndCQCFilledPostsByJobRoleUtilsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_create_ratios_from_counts_returns_expected_ratios_when_all_count_values_above_zero(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.create_ratios_from_counts_when_all_count_values_above_zero_rows,
            Schemas.ascwds_job_role_count_map_to_ratios_map_schema,
        )
        returned_df = job.create_ratios_map_from_count_map_and_total(
            test_df,
            IndCQC.ascwds_job_role_counts,
            Data.temp_total_count_of_worker_records,
            IndCQC.ascwds_job_role_ratios,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_create_ratios_from_counts_when_all_count_values_above_zero_rows,
            Schemas.expected_ascwds_job_role_count_map_to_ratios_map_schema,
        )

        returned_data = returned_df.collect()
        expected_data = expected_df.collect()

        returned_ratio_dict: dict = returned_data[0][IndCQC.ascwds_job_role_ratios]
        expected_ratio_dict: dict = expected_data[0][IndCQC.ascwds_job_role_ratios]

        for i in list(expected_ratio_dict.keys()):
            self.assertAlmostEqual(
                returned_ratio_dict[i],
                expected_ratio_dict[i],
                places=3,
                msg=f"Dict element {i} does not match",
            )

    def test_create_ratios_from_counts_returns_nulls_when_all_count_values_are_null(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.create_ratios_from_counts_when_all_count_values_are_null_rows,
            Schemas.ascwds_job_role_count_map_to_ratios_map_schema,
        )
        returned_df = job.create_ratios_map_from_count_map_and_total(
            test_df,
            IndCQC.ascwds_job_role_counts,
            Data.temp_total_count_of_worker_records,
            IndCQC.ascwds_job_role_ratios,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_create_ratios_from_counts_when_all_count_values_are_null_rows,
            Schemas.expected_ascwds_job_role_count_map_to_ratios_map_schema,
        )

        returned_data = returned_df.collect()
        expected_data = expected_df.collect()

        self.assertEqual(
            returned_data[0][IndCQC.ascwds_job_role_ratios],
            expected_data[0][IndCQC.ascwds_job_role_ratios],
        )

    def test_create_ratios_from_counts_returns_null_when_count_map_column_is_null(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.create_ratios_from_counts_when_count_map_column_is_null_rows,
            Schemas.ascwds_job_role_count_map_to_ratios_map_schema,
        )
        returned_df = job.create_ratios_map_from_count_map_and_total(
            test_df,
            IndCQC.ascwds_job_role_counts,
            Data.temp_total_count_of_worker_records,
            IndCQC.ascwds_job_role_ratios,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_create_ratios_from_counts_when_count_map_column_is_null_rows,
            Schemas.expected_ascwds_job_role_count_map_to_ratios_map_schema,
        )

        returned_data = returned_df.collect()
        expected_data = expected_df.collect()

        self.assertEqual(
            returned_data[0][IndCQC.ascwds_job_role_ratios],
            expected_data[0][IndCQC.ascwds_job_role_ratios],
        )

    def test_create_ratios_from_counts_returns_expected_ratios_given_multiple_establishments(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.create_ratios_from_counts_at_multiple_establishments_rows,
            Schemas.ascwds_job_role_count_map_to_ratios_map_schema,
        )
        returned_df = job.create_ratios_map_from_count_map_and_total(
            test_df,
            IndCQC.ascwds_job_role_counts,
            Data.temp_total_count_of_worker_records,
            IndCQC.ascwds_job_role_ratios,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_create_ratios_from_counts_at_multiple_establishments_rows,
            Schemas.expected_ascwds_job_role_count_map_to_ratios_map_schema,
        )

        returned_data = returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.sort(IndCQC.location_id).collect()

        self.assertEqual(
            returned_data[0][IndCQC.ascwds_job_role_ratios],
            expected_data[0][IndCQC.ascwds_job_role_ratios],
        )


class CreateEstimateFilledPostsByJobRoleMapColumn(
    EstimateIndCQCFilledPostsByJobRoleUtilsTests
):
    def setUp(self) -> None:
        super().setUp()

        self.test_df = self.spark.createDataFrame(
            Data.create_estimate_filled_posts_by_job_role_map_column_when_all_job_role_ratios_populated_rows,
            Schemas.create_estimate_filled_posts_by_job_role_map_column_schema,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_create_estimate_filled_posts_by_job_role_map_column_when_all_job_role_ratios_populated_rows,
            Schemas.expected_create_estimate_filled_posts_by_job_role_map_column_schema,
        )
        self.returned_df = job.create_estimate_filled_posts_by_job_role_map_column(
            self.test_df
        )

        self.new_columns_added = [
            column
            for column in self.returned_df.columns
            if column not in self.test_df.columns
        ]

    def test_create_estimate_filled_posts_by_job_role_map_column_adds_one_column(
        self,
    ):
        self.assertEqual(len(self.new_columns_added), 1)

    def test_create_estimate_filled_posts_by_job_role_map_column_returns_expected_estimates_when_all_job_role_ratios_populated(
        self,
    ):
        self.assertEqual(self.returned_df.collect(), self.expected_df.collect())

    def test_create_estimate_filled_posts_by_job_role_map_column_returns_null_when_job_role_ratio_column_is_null(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.create_estimate_filled_posts_by_job_role_map_column_when_job_role_ratio_column_is_null_rows,
            Schemas.create_estimate_filled_posts_by_job_role_map_column_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_create_estimate_filled_posts_by_job_role_map_column_when_job_role_ratio_column_is_null_rows,
            Schemas.expected_create_estimate_filled_posts_by_job_role_map_column_schema,
        )
        returned_df = job.create_estimate_filled_posts_by_job_role_map_column(test_df)

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_create_estimate_filled_posts_by_job_role_map_column_returns_null_when_estimate_filled_posts_is_null(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.create_estimate_filled_posts_by_job_role_map_column_when_estimate_filled_posts_is_null_rows,
            Schemas.create_estimate_filled_posts_by_job_role_map_column_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_create_estimate_filled_posts_by_job_role_map_column_when_estimate_filled_posts_is_null_rows,
            Schemas.expected_create_estimate_filled_posts_by_job_role_map_column_schema,
        )
        returned_df = job.create_estimate_filled_posts_by_job_role_map_column(test_df)

        self.assertEqual(returned_df.collect(), expected_df.collect())


class RemoveAscwdsJobRoleCountWhenFilledPostsSourceNotAscwds(
    EstimateIndCQCFilledPostsByJobRoleUtilsTests
):
    def setUp(self) -> None:
        super().setUp()

        self.test_df = self.spark.createDataFrame(
            Data.remove_ascwds_job_role_count_when_estimate_filled_posts_source_not_ascwds_rows,
            Schemas.remove_ascwds_job_role_count_when_estimate_filled_posts_source_not_ascwds_schema,
        )
        self.returned_df = job.remove_ascwds_job_role_count_when_estimate_filled_posts_source_not_ascwds(
            self.test_df
        )
        self.returned_data = self.returned_df.sort(IndCQC.location_id).collect()

    def test_remove_ascwds_job_role_count_when_estimate_filled_posts_source_not_ascwds_returns_null_as_expected(
        self,
    ):
        expected_df = self.spark.createDataFrame(
            Data.expected_remove_ascwds_job_role_count_when_estimate_filled_posts_source_not_ascwds_rows,
            Schemas.remove_ascwds_job_role_count_when_estimate_filled_posts_source_not_ascwds_schema,
        )
        expected_data = expected_df.sort(IndCQC.location_id).collect()

        self.assertEqual(self.returned_data, expected_data)


class CountRegisteredManagerNamesTests(EstimateIndCQCFilledPostsByJobRoleUtilsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_count_registered_manager_names_when_location_has_one_registered_manager(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.count_registered_manager_names_when_location_has_one_registered_manager_rows,
            Schemas.count_registered_manager_names_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_count_registered_manager_names_when_location_has_one_registered_manager_rows,
            Schemas.expected_count_registered_manager_names_schema,
        )

        returned_df = job.count_registered_manager_names(test_df)

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_count_registered_manager_names_when_location_has_two_registered_managers(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.count_registered_manager_names_when_location_has_two_registered_managers_rows,
            Schemas.count_registered_manager_names_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_count_registered_manager_names_when_location_has_two_registered_managers_rows,
            Schemas.expected_count_registered_manager_names_schema,
        )

        returned_df = job.count_registered_manager_names(test_df)

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_count_registered_manager_names_returns_zero_when_location_has_null_registered_manager(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.count_registered_manager_names_when_location_has_null_registered_manager_rows,
            Schemas.count_registered_manager_names_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_count_registered_manager_names_when_location_has_null_registered_manager_rows,
            Schemas.expected_count_registered_manager_names_schema,
        )

        returned_df = job.count_registered_manager_names(test_df)

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_count_registered_manager_names_returns_zero_when_location_has_empty_list(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.count_registered_manager_names_when_location_has_empty_list_rows,
            Schemas.count_registered_manager_names_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_count_registered_manager_names_when_location_has_empty_list_rows,
            Schemas.expected_count_registered_manager_names_schema,
        )

        returned_df = job.count_registered_manager_names(test_df)

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_count_registered_manager_names_when_two_locations_have_different_number_of_registered_managers(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.count_registered_manager_names_when_two_locations_have_different_number_of_registered_managers_rows,
            Schemas.count_registered_manager_names_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_count_registered_manager_names_when_two_locations_have_different_number_of_registered_managers_rows,
            Schemas.expected_count_registered_manager_names_schema,
        )

        returned_df = job.count_registered_manager_names(test_df)

        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )

    def test_count_registered_manager_names_when_location_has_different_number_of_registered_managers_at_different_import_dates(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.count_registered_manager_names_when_a_location_has_different_number_of_registered_managers_at_different_import_dates_rows,
            Schemas.count_registered_manager_names_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_count_registered_manager_names_when_a_location_has_different_number_of_registered_managers_at_different_import_dates_rows,
            Schemas.expected_count_registered_manager_names_schema,
        )

        returned_df = job.count_registered_manager_names(test_df)

        self.assertEqual(
            returned_df.sort(IndCQC.cqc_location_import_date).collect(),
            expected_df.collect(),
        )


class InterpolateJobRoleRatio(EstimateIndCQCFilledPostsByJobRoleUtilsTests):
    def setUp(self) -> None:
        super().setUp()

        self.test_col_list = Data.list_of_job_roles_for_tests

        self.test_df = self.spark.createDataFrame(
            Data.interpolate_job_role_ratios_data,
            Schemas.interpolate_job_role_ratios_schema,
        )

        self.expected_df = self.spark.createDataFrame(
            Data.expected_interpolate_job_role_ratios_data,
            Schemas.expected_interpolate_job_role_ratios_schema,
        )
        self.return_df = interp.model_job_role_ratio_interpolation(
            self.test_df, self.test_col_list
        )

        self.new_columns_added = [
            column
            for column in self.return_df.columns
            if column not in self.test_df.columns
        ]

    def test_model_interpolation_adds_one_column(
        self,
    ):
        self.assertEqual(len(self.new_columns_added), 1)

    def test_model_interpolation_does_not_change_the_number_of_rows(
        self,
    ):
        self.assertEqual(self.test_df.count(), self.return_df.count())

    def test_model_interpolation_when_data_includes_nulls_which_cannot_be_interpolated_return_dataframe_with_no_incorrect_population_of_values(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.interpolate_job_role_ratios_with_null_records_which_cannot_be_interpolated_data,
            Schemas.interpolate_job_role_ratios_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_interpolate_job_role_ratios_with_null_records_which_cannot_be_interpolated_data,
            Schemas.expected_interpolate_job_role_ratios_schema,
        )

        return_df = interp.model_job_role_ratio_interpolation(
            test_df, self.test_col_list
        )

        self.assertEqual(
            expected_df.select(
                IndCQC.location_id,
                IndCQC.unix_time,
                IndCQC.ascwds_job_role_ratios,
                IndCQC.ascwds_job_role_ratios_interpolated,
            )
            .sort(IndCQC.location_id, IndCQC.unix_time)
            .collect(),
            return_df.select(
                IndCQC.location_id,
                IndCQC.unix_time,
                IndCQC.ascwds_job_role_ratios,
                IndCQC.ascwds_job_role_ratios_interpolated,
            )
            .sort(IndCQC.location_id, IndCQC.unix_time)
            .collect(),
        )


class PivotJobRoleColumn(EstimateIndCQCFilledPostsByJobRoleUtilsTests):
    def setUp(self) -> None:
        super().setUp()

        test_with_identical_grouping_columns_df = self.spark.createDataFrame(
            Data.pivot_job_role_column_returns_expected_pivot_rows,
            Schemas.pivot_job_role_column_schema,
        )
        self.returned_identical_grouping_column_df = job.pivot_job_role_column(
            test_with_identical_grouping_columns_df,
            [IndCQC.unix_time, IndCQC.primary_service_type],
            IndCQC.ascwds_job_role_ratios_interpolated,
        )
        self.expected_identical_grouping_column_df = self.spark.createDataFrame(
            Data.expected_pivot_job_role_column_returns_expected_pivot_rows,
            Schemas.expected_pivot_job_role_column_schema,
        )

        test_with_multiple_grouping_columns_df = self.spark.createDataFrame(
            Data.pivot_job_role_column_with_multiple_grouping_column_options_rows,
            Schemas.pivot_job_role_column_schema,
        )
        self.returned_multiple_grouping_column_df = job.pivot_job_role_column(
            test_with_multiple_grouping_columns_df,
            [IndCQC.unix_time, IndCQC.primary_service_type],
            IndCQC.ascwds_job_role_ratios_interpolated,
        )
        self.expected_multiple_grouping_column_df = self.spark.createDataFrame(
            Data.expected_pivot_job_role_column_with_multiple_grouping_column_options_rows,
            Schemas.expected_pivot_job_role_column_two_job_roles_schema,
        )

    def test_pivot_job_role_column_returns_expected_pivoted_column_names(self):
        self.assertEqual(
            self.returned_identical_grouping_column_df.columns,
            self.expected_identical_grouping_column_df.columns,
        )

    def test_pivot_job_role_column_returns_expected_row_values_when_grouping_columns_are_identical(
        self,
    ):
        self.assertEqual(
            self.returned_identical_grouping_column_df.collect(),
            self.expected_identical_grouping_column_df.collect(),
        )

    def test_pivot_job_role_column_returns_expected_row_count_when_grouping_columns_are_identical(
        self,
    ):
        self.assertEqual(self.returned_identical_grouping_column_df.count(), 1)

    def test_pivot_job_role_column_returns_expected_values_when_multiple_grouping_columns_present(
        self,
    ):
        self.assertEqual(
            self.returned_identical_grouping_column_df.sort(
                IndCQC.unix_time, IndCQC.primary_service_type
            ).collect(),
            self.expected_identical_grouping_column_df.collect(),
        )

    def test_pivot_job_role_column_returns_expected_row_count_when_multiple_grouping_columns_present(
        self,
    ):
        self.assertEqual(
            self.returned_multiple_grouping_column_df.count(),
            self.expected_multiple_grouping_column_df.count(),
        )

    def test_pivot_job_role_column_returns_first_aggregated_value(self):
        test_df = self.spark.createDataFrame(
            Data.pivot_job_role_column_returns_first_aggregation_column_value_rows,
            Schemas.pivot_job_role_column_schema,
        )
        returned_df = job.pivot_job_role_column(
            test_df,
            [IndCQC.unix_time, IndCQC.primary_service_type],
            IndCQC.ascwds_job_role_ratios_interpolated,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_pivot_job_role_column_returns_first_aggregation_column_value_rows,
            Schemas.expected_pivot_job_role_column_two_job_roles_schema,
        )

        self.assertEqual(
            returned_df.sort(IndCQC.unix_time).collect(),
            expected_df.collect(),
        )


class ConvertMapWithAllNullValuesToNull(EstimateIndCQCFilledPostsByJobRoleUtilsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_convert_map_with_all_null_values_to_null_when_map_has_no_null_returns_identical_dataframe(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.convert_map_with_all_null_values_to_null_map_has_no_nulls_data,
            Schemas.convert_map_with_all_null_values_to_null_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_convert_map_with_all_null_values_to_null_map_has_no_nulls_data,
            Schemas.expected_convert_map_with_all_null_values_to_null_schema,
        )

        returned_df = job.convert_map_with_all_null_values_to_null(test_df)

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_convert_map_with_all_null_values_to_null_when_map_has_all_null_returns_null(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.convert_map_with_all_null_values_to_null_map_has_all_nulls,
            Schemas.convert_map_with_all_null_values_to_null_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_convert_map_with_all_null_values_to_null_map_has_all_nulls,
            Schemas.expected_convert_map_with_all_null_values_to_null_schema,
        )

        returned_df = job.convert_map_with_all_null_values_to_null(test_df)

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_convert_map_with_all_null_values_to_null_when_map_has_all_null_and_all_non_null_records_returns_identical_record_and_null(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.convert_map_with_all_null_values_to_null_when_map_has_all_null_and_all_non_null_records_data,
            Schemas.convert_map_with_all_null_values_to_null_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_convert_map_with_all_null_values_to_null_when_map_has_all_null_and_all_non_null_records_data,
            Schemas.expected_convert_map_with_all_null_values_to_null_schema,
        )

        returned_df = job.convert_map_with_all_null_values_to_null(test_df)

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_convert_map_with_all_null_values_to_null_when_map_has_some_null_returns_identical_dataframe(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.convert_map_with_all_null_values_to_null_when_map_has_some_nulls_data,
            Schemas.convert_map_with_all_null_values_to_null_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_convert_map_with_all_null_values_to_null_when_map_has_some_nulls_data,
            Schemas.expected_convert_map_with_all_null_values_to_null_schema,
        )

        returned_df = job.convert_map_with_all_null_values_to_null(test_df)

        self.assertEqual(returned_df.collect(), expected_df.collect())


class UnpackingMappedColumnsTest(EstimateIndCQCFilledPostsByJobRoleUtilsTests):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.unpacked_mapped_column_with_one_record_data,
            Schemas.unpacked_mapped_column_schema,
        )
        self.returned_df = job.unpack_mapped_column(
            test_df, IndCQC.estimate_filled_posts_by_job_role
        )

        self.expected_df = self.spark.createDataFrame(
            Data.expected_unpacked_mapped_column_with_one_record_data,
            Schemas.expected_unpacked_mapped_column_schema,
        )

    def test_unpack_mapped_column_returns_expected_columns(self):
        self.assertEqual(
            sorted(self.returned_df.columns), sorted(self.expected_df.columns)
        )

    def test_unpack_mapped_column_returns_expected_values_in_each_column(self):
        self.assertEqual(
            self.returned_df.collect(),
            self.expected_df.collect(),
        )

    def test_unpack_mapped_column_when_rows_have_map_items_in_differing_orders_returns_expected_values(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.unpacked_mapped_column_with_map_items_in_different_orders_data,
            Schemas.unpacked_mapped_column_schema,
        )
        returned_df = job.unpack_mapped_column(
            test_df, IndCQC.estimate_filled_posts_by_job_role
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_unpacked_mapped_column_with_map_items_in_different_orders_data,
            Schemas.expected_unpacked_mapped_column_schema,
        )

        returned_data = returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.collect()

        self.assertEqual(returned_data, expected_data)

    def test_unpack_mapped_column_with_null_values_returns_expected_values(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.unpacked_mapped_column_with_null_values_data,
            Schemas.unpacked_mapped_column_schema,
        )
        returned_df = job.unpack_mapped_column(
            test_df, IndCQC.estimate_filled_posts_by_job_role
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_unpacked_mapped_column_with_null_values_data,
            Schemas.expected_unpacked_mapped_column_schema,
        )

        returned_data = returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.collect()

        self.assertEqual(returned_data, expected_data)


class CalculateDifferenceBetweenEstimatedAndCqcRegisteredManagers(
    EstimateIndCQCFilledPostsByJobRoleUtilsTests
):
    def setUp(self) -> None:
        super().setUp()

        self.test_df = self.spark.createDataFrame(
            Data.estimate_and_cqc_registered_manager_rows,
            Schemas.estimate_and_cqc_registered_manager_schema,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_estimate_and_cqc_registered_manager_rows,
            Schemas.expected_estimate_and_cqc_registered_manager_schema,
        )
        self.returned_df = (
            job.calculate_difference_between_estimate_and_cqc_registered_managers(
                self.test_df
            )
        )

        self.new_columns_added = [
            column
            for column in self.returned_df.columns
            if column not in self.test_df.columns
        ]

    def test_calculate_difference_between_estimate_and_cqc_registered_managers_adds_expected_column(
        self,
    ):
        self.assertEqual(len(self.new_columns_added), 1)
        self.assertEqual(
            self.new_columns_added[0],
            IndCQC.difference_between_estimate_and_cqc_registered_managers,
        )

    def test_calculate_difference_between_estimate_and_cqc_registered_managers_returns_expected_values(
        self,
    ):
        expected_data = self.expected_df.collect()
        returned_data = self.returned_df.collect()
        self.assertEqual(expected_data, returned_data)


class CreateJobGroupCounts(EstimateIndCQCFilledPostsByJobRoleUtilsTests):
    def setUp(self) -> None:
        super().setUp()

        self.test_df = self.spark.createDataFrame(
            Data.sum_job_group_counts_from_job_role_count_map_rows,
            Schemas.sum_job_group_counts_from_job_role_count_map_schema,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_sum_job_group_counts_from_job_role_count_map_rows,
            Schemas.expected_sum_job_group_counts_from_job_role_count_map_schema,
        )
        self.returned_df = job.calculate_job_group_sum_from_job_role_map_column(
            self.test_df, IndCQC.ascwds_job_role_counts, IndCQC.ascwds_job_group_counts
        )

        self.new_columns_added = [
            column
            for column in self.returned_df.columns
            if column not in self.test_df.columns
        ]

        self.returned_df_for_patch_create_map_column = self.spark.createDataFrame(
            Data.sum_job_group_counts_from_job_role_count_map_for_patching_create_map_column_rows,
            Schemas.sum_job_group_counts_from_job_role_count_map_for_patching_create_map_column_schema,
        )

    @patch("utils.estimate_filled_posts_by_job_role_utils.utils.create_map_column")
    def test_sum_job_group_counts_from_job_role_count_map_calls_premade_functionality(
        self, create_map_column_mock: Mock
    ):
        create_map_column_mock.return_value = (
            self.returned_df_for_patch_create_map_column
        )
        job.calculate_job_group_sum_from_job_role_map_column(
            self.test_df, IndCQC.ascwds_job_role_counts, IndCQC.ascwds_job_group_counts
        )
        create_map_column_mock.assert_called_once()

    def test_sum_job_group_counts_from_job_role_count_map_adds_1_expected_column(
        self,
    ):
        self.assertEqual(len(self.new_columns_added), 1)
        self.assertEqual(self.new_columns_added[0], IndCQC.ascwds_job_group_counts)

    def test_sum_job_group_counts_from_job_role_count_map_does_not_change_row_count(
        self,
    ):
        self.assertEqual(self.test_df.count(), self.returned_df.count())

    def test_sum_job_group_counts_from_job_role_count_map_returns_expected_values(
        self,
    ):
        expected_data = self.expected_df.sort(
            IndCQC.location_id, IndCQC.unix_time
        ).collect()
        returned_data = self.returned_df.sort(
            IndCQC.location_id, IndCQC.unix_time
        ).collect()
        self.assertEqual(expected_data, returned_data)


class CalculateSumAndProportionSplitOfNonRmManagerialEstimatePosts(
    EstimateIndCQCFilledPostsByJobRoleUtilsTests
):
    def setUp(self) -> None:
        super().setUp()

        self.test_df = self.spark.createDataFrame(
            Data.non_rm_managerial_estimate_filled_posts_rows,
            Schemas.non_rm_managerial_estimate_filled_posts_schema,
        )
        self.returned_df = (
            job.calculate_sum_and_proportion_split_of_non_rm_managerial_estimate_posts(
                self.test_df
            )
        )

        self.expected_df = self.spark.createDataFrame(
            Data.expected_non_rm_managerial_estimate_filled_posts_rows,
            Schemas.expected_non_rm_managerial_estimate_filled_posts_schema,
        )

        self.new_columns_added = [
            column
            for column in self.returned_df.columns
            if column not in self.test_df.columns
        ]

    def test_calculate_sum_and_proportion_split_of_non_rm_managerial_estimate_posts_adds_two_columns(
        self,
    ):
        self.assertEqual(len(self.new_columns_added), 2)

    def test_calculate_sum_and_proportion_split_of_non_rm_managerial_estimate_posts_mapped_column_returns_expected_values(
        self,
    ):
        expected_data = (
            self.expected_df.withColumn(
                IndCQC.proportion_of_non_rm_managerial_estimated_filled_posts_by_role,
                F.map_from_entries(
                    F.sort_array(
                        F.map_entries(
                            IndCQC.proportion_of_non_rm_managerial_estimated_filled_posts_by_role
                        )
                    )
                ),
            )
            .sort(IndCQC.location_id)
            .collect()
        )
        returned_data = self.returned_df.sort(IndCQC.location_id).collect()

        for iterable in range(len(expected_data)):
            returned_ratio_dict = returned_data[iterable][
                IndCQC.proportion_of_non_rm_managerial_estimated_filled_posts_by_role
            ]
            expected_ratio_dict = expected_data[iterable][
                IndCQC.proportion_of_non_rm_managerial_estimated_filled_posts_by_role
            ]

            if isinstance(returned_ratio_dict, dict) and isinstance(
                expected_ratio_dict, dict
            ):
                for i in list(expected_ratio_dict.keys()):
                    self.assertAlmostEqual(
                        returned_ratio_dict[i],
                        expected_ratio_dict[i],
                        places=3,
                        msg=f"Dict element {i} does not match",
                    )

            else:
                expected_ratio_value = expected_data[iterable][
                    IndCQC.proportion_of_non_rm_managerial_estimated_filled_posts_by_role
                ]

                returned_ratio_value = returned_data[iterable][
                    IndCQC.proportion_of_non_rm_managerial_estimated_filled_posts_by_role
                ]

                self.assertEqual(expected_ratio_value, returned_ratio_value)

    def test_calculate_sum_and_proportion_split_of_non_rm_managerial_estimate_posts_non_mapped_columns_returns_expected_values(
        self,
    ):
        selected_cols = [
            col
            for col in self.expected_df.columns
            if col
            != IndCQC.proportion_of_non_rm_managerial_estimated_filled_posts_by_role
        ]

        expected_df = self.expected_df.select(*selected_cols)
        returned_df = self.returned_df.select(*selected_cols)

        self.assertEqual(
            expected_df.sort(IndCQC.location_id).collect(),
            returned_df.sort(IndCQC.location_id).collect(),
        )
