import unittest
from unittest.mock import ANY, call, patch, Mock

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.estimate_filled_posts_by_job_role_utils import utils as job
from tests.test_file_data import EstimateIndCQCFilledPostsByJobRoleUtilsData as Data
from tests.test_file_schemas import (
    EstimateIndCQCFilledPostsByJobRoleUtilsSchemas as Schemas,
)


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
        returned_df = test_df.withColumn(
            Schemas.test_map_column,
            job.create_map_column(Data.list_of_job_roles_for_tests),
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
        returned_df = test_df.withColumn(
            Schemas.test_map_column,
            job.create_map_column(Data.list_of_job_roles_for_tests),
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
        returned_df = test_df.withColumn(
            Schemas.test_map_column,
            job.create_map_column(Data.list_of_job_roles_for_tests),
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


class MergeAscwdsAndPrimaryServiceJobRoleRatioColumns(
    EstimateIndCQCFilledPostsByJobRoleUtilsTests
):
    def setUp(self) -> None:
        super().setUp()

        self.test_df = self.spark.createDataFrame(
            Data.merge_job_role_ratio_columns_when_only_ascwds_known,
            Schemas.merge_ascwds_and_primary_service_job_role_ratio_columns,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_merge_job_role_ratio_columns_when_only_ascwds_known,
            Schemas.expected_merge_ascwds_and_primary_service_job_role_ratio_columns,
        )
        self.returned_df = job.merge_job_role_ratio_columns(
            self.test_df,
            Data.list_of_job_role_ratio_columns_to_be_merged,
            IndCQC.ascwds_job_role_ratios_merged,
            IndCQC.ascwds_job_role_ratios_merged_source,
        )

        self.new_columns_added = [
            column
            for column in self.returned_df.columns
            if column not in self.test_df.columns
        ]

    def test_merge_job_role_ratio_columns_adds_two_columns(
        self,
    ):
        self.assertEqual(len(self.new_columns_added), 2)

    def test_merge_job_role_ratio_columns_adds_a_column_called_ascwds_job_role_ratios_merged(
        self,
    ):
        self.assertEqual(
            self.new_columns_added[0], IndCQC.ascwds_job_role_ratios_merged
        )

    def test_merge_job_role_ratio_columns_adds_a_column_called_ascwds_job_role_ratios_merged_source(
        self,
    ):
        self.assertEqual(
            self.new_columns_added[1], IndCQC.ascwds_job_role_ratios_merged_source
        )

    def test_merge_job_role_ratio_columns_returns_ascwds_ratios_when_only_ascwds_known(
        self,
    ):
        self.assertEqual(self.returned_df.collect(), self.expected_df.collect())

    def test_merge_job_role_ratio_columns_returns_primary_service_ratios_when_only_ratios_by_primary_service_populated(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.merge_job_role_ratio_columns_when_only_primary_service_known,
            Schemas.merge_ascwds_and_primary_service_job_role_ratio_columns,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_merge_job_role_ratio_columns_when_only_primary_service_known,
            Schemas.expected_merge_ascwds_and_primary_service_job_role_ratio_columns,
        )
        returned_df = job.merge_job_role_ratio_columns(
            test_df,
            Data.list_of_job_role_ratio_columns_to_be_merged,
            IndCQC.ascwds_job_role_ratios_merged,
            IndCQC.ascwds_job_role_ratios_merged_source,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_merge_job_role_ratio_columns_returns_ascwds_ratios_when_both_ratios_populated(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.merge_job_role_ratio_columns_when_both_ratio_columns_populated,
            Schemas.merge_ascwds_and_primary_service_job_role_ratio_columns,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_merge_job_role_ratio_columns_when_both_ratio_columns_populated,
            Schemas.expected_merge_ascwds_and_primary_service_job_role_ratio_columns,
        )
        returned_df = job.merge_job_role_ratio_columns(
            test_df,
            Data.list_of_job_role_ratio_columns_to_be_merged,
            IndCQC.ascwds_job_role_ratios_merged,
            IndCQC.ascwds_job_role_ratios_merged_source,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_merge_job_role_ratio_columns_returns_null_when_both_ratio_columns_are_null(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.merge_job_role_ratio_columns_when_both_null,
            Schemas.merge_ascwds_and_primary_service_job_role_ratio_columns,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_merge_job_role_ratio_columns_when_both_null,
            Schemas.expected_merge_ascwds_and_primary_service_job_role_ratio_columns,
        )
        returned_df = job.merge_job_role_ratio_columns(
            test_df,
            Data.list_of_job_role_ratio_columns_to_be_merged,
            IndCQC.ascwds_job_role_ratios_merged,
            IndCQC.ascwds_job_role_ratios_merged_source,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_merge_job_role_ratio_columns_returns_ascwds_at_both_locations_when_both_ratios_populated_at_both_locations(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.merge_job_role_ratio_columns_at_multiple_locations_when_both_ratio_columns_populated_at_both_locations,
            Schemas.merge_ascwds_and_primary_service_job_role_ratio_columns,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_merge_job_role_ratio_columns_at_multiple_locations_when_both_ratio_columns_populated_at_both_locations,
            Schemas.expected_merge_ascwds_and_primary_service_job_role_ratio_columns,
        )
        returned_df = job.merge_job_role_ratio_columns(
            test_df,
            Data.list_of_job_role_ratio_columns_to_be_merged,
            IndCQC.ascwds_job_role_ratios_merged,
            IndCQC.ascwds_job_role_ratios_merged_source,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())


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


class SumJobRoleCountSplitByServiceTests(EstimateIndCQCFilledPostsByJobRoleUtilsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_sum_job_role_count_split_by_service_when_multiple_entries_in_partition_column(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.sum_job_role_count_split_by_service_with_multiple_service_types_data,
            Schemas.sum_job_role_split_by_service_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_sum_job_role_split_by_service_with_multiple_service_types_data,
            Schemas.expected_sum_job_role_split_by_service_schema,
        )

        return_df = job.sum_job_role_count_split_by_service(
            test_df, Data.list_of_job_roles_for_tests
        )

        self.assertEqual(
            expected_df.sort(IndCQC.primary_service_type).collect(),
            return_df.select(
                IndCQC.establishment_id,
                IndCQC.ascwds_worker_import_date,
                IndCQC.ascwds_job_role_counts,
                IndCQC.primary_service_type,
                IndCQC.ascwds_job_role_counts_by_primary_service,
            )
            .sort(IndCQC.primary_service_type)
            .collect(),
        )

    def test_sum_job_role_count_split_by_service_when_one_entry_in_each_partition(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.sum_job_role_count_split_by_service_with_one_service_type_data,
            Schemas.sum_job_role_split_by_service_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_sum_job_role_count_split_by_service_with_one_service_type_data,
            Schemas.expected_sum_job_role_split_by_service_schema,
        )

        return_df = job.sum_job_role_count_split_by_service(
            test_df, Data.list_of_job_roles_for_tests
        )

        self.assertEqual(
            expected_df.sort(IndCQC.primary_service_type).collect(),
            return_df.select(
                IndCQC.establishment_id,
                IndCQC.ascwds_worker_import_date,
                IndCQC.ascwds_job_role_counts,
                IndCQC.primary_service_type,
                IndCQC.ascwds_job_role_counts_by_primary_service,
            )
            .sort(IndCQC.primary_service_type)
            .collect(),
        )
