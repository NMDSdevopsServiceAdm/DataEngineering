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


class InterpolateJobRoleRatio(EstimateIndCQCFilledPostsByJobRoleUtilsTests):
    def setUp(self) -> None:
        super().setUp()

        self.test_df = self.spark.createDataFrame(
            Data.interpolate_job_role_ratios_data,
            Schemas.interpolate_job_role_ratios_schema,
        )

        self.expected_df = self.spark.createDataFrame(
            Data.expected_interpolate_job_role_ratios_data,
            Schemas.expected_interpolate_job_role_ratios_schema,
        )
        self.return_df = interp.model_job_role_ratio_interpolation(self.test_df)

        self.new_columns_added = [
            column
            for column in self.return_df.columns
            if column not in self.test_df.columns
        ]

    def test_model_interpolation_adds_one_column(
        self,
    ):
        self.assertEqual(len(self.new_columns_added), 1)

    def test_model_interpolation_count_of_rows(
        self,
    ):
        self.assertEqual(self.test_df.count(), self.return_df.count())

    def test_model_interpolation_when_one_record_of_null_values_in_between_populated_records_return_dataframe_with_interpolated_values(
        self,
    ):
        self.assertEqual(
            self.expected_df.select(
                IndCQC.location_id,
                IndCQC.unix_time,
                IndCQC.ascwds_job_role_ratios,
                IndCQC.ascwds_job_role_ratios_interpolated,
            )
            .sort(IndCQC.location_id, IndCQC.unix_time)
            .collect(),
            self.return_df.select(
                IndCQC.location_id,
                IndCQC.unix_time,
                IndCQC.ascwds_job_role_ratios,
                IndCQC.ascwds_job_role_ratios_interpolated,
            )
            .sort(IndCQC.location_id, IndCQC.unix_time)
            .collect(),
        )

    def test_example(self,):

        test_df = self.spark.createDataFrame(
            Data.interpolate_example_data,
            Schemas.interpolate_job_role_ratios_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_interpolate_example_data,
            Schemas.expected_interpolate_job_role_ratios_schema,
        )

        returned_df = interp.model_job_role_ratio_interpolation(test_df)

        self.assertEqual(
            expected_df.select(
                IndCQC.location_id,
                IndCQC.unix_time,
                IndCQC.ascwds_job_role_ratios,
                IndCQC.ascwds_job_role_ratios_interpolated,
            )
            .sort(IndCQC.location_id, IndCQC.unix_time)
            .collect(),
            returned_df.select(
                IndCQC.location_id,
                IndCQC.unix_time,
                IndCQC.ascwds_job_role_ratios,
                IndCQC.ascwds_job_role_ratios_interpolated,
            )
            .sort(IndCQC.location_id, IndCQC.unix_time)
            .collect(),
        )

    def test_model_interpolation_when_two_records_of_null_values_in_between_populated_records_return_dataframe_with_interpolated_values(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.interpolate_job_role_ratios_with_two_records_with_nulls_data,
            Schemas.interpolate_job_role_ratios_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_interpolate_job_role_ratios_with_two_records_with_nulls_data,
            Schemas.expected_interpolate_job_role_ratios_schema,
        )

        return_df = interp.model_job_role_ratio_interpolation(test_df)

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

    def test_model_interpolation_when_two_paritions_in_dataframe_return_dataframe_with_interpolated_values(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.interpolate_job_role_ratios_with_two_partitions_data,
            Schemas.interpolate_job_role_ratios_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_interpolate_job_role_ratios_with_two_partitions_data,
            Schemas.expected_interpolate_job_role_ratios_schema,
        )

        return_df = interp.model_job_role_ratio_interpolation(test_df)

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

    def test_model_interpolation_when_three_record_of_null_values_in_between_populated_records_return_dataframe_with_interpolated_values(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.interpolate_job_role_ratios_with_three_records_with_nulls_data,
            Schemas.interpolate_job_role_ratios_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_interpolate_job_role_ratios_with_three_records_with_nulls_data,
            Schemas.expected_interpolate_job_role_ratios_schema,
        )

        return_df = interp.model_job_role_ratio_interpolation(test_df)

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

        return_df = interp.model_job_role_ratio_interpolation(test_df)

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

    def test_model_interpolation_when_data_includes_empty_record_which_cannot_be_interpolated_return_dataframe_with_no_incorrect_population_of_values(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.interpolate_job_role_ratios_with_an_empty_record_which_cannot_be_interpolated_data,
            Schemas.interpolate_job_role_ratios_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_interpolate_job_role_ratios_with_an_empty_record_which_cannot_be_interpolated_data,
            Schemas.expected_interpolate_job_role_ratios_schema,
        )

        return_df = interp.model_job_role_ratio_interpolation(test_df)

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

    def test_model_interpolation_when_data_includes_sequential_null_values_return_dataframe(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.interpolate_job_role_ratios_with_an_sequential_none_records_data,
            Schemas.interpolate_job_role_ratios_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_interpolate_job_role_ratios_with_an_sequential_none_records_data,
            Schemas.expected_interpolate_job_role_ratios_schema,
        )

        return_df = interp.model_job_role_ratio_interpolation(test_df)

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


class PivotInterpolatedJobRolesMappedColumn(
    EstimateIndCQCFilledPostsByJobRoleUtilsTests
):
    def setUp(self) -> None:
        super().setUp()

    def test_pivot_interpolated_job_role_returns_pivoted_job_role_labels(self):
        test_df = self.spark.createDataFrame(
            Data.pivot_interpolated_job_role_ratios_data,
            Schemas.pivot_interpolated_job_role_ratios_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_pivot_interpolated_job_role_ratios_data,
            Schemas.expected_pivot_interpolated_job_role_ratios_schema,
        )

        returned_df = job.pivot_interpolated_job_role_ratios(test_df)

        self.assertEqual(
            expected_df.orderBy(IndCQC.location_id, IndCQC.unix_time).collect(),
            returned_df.orderBy(IndCQC.location_id, IndCQC.unix_time).collect(),
        )

    def test_pivot_interpolated_job_role_when_unix_time_is_different_returns_pivoted_job_role_labels(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.pivot_interpolated_job_role_ratios_with_different_unix_time_data,
            Schemas.pivot_interpolated_job_role_ratios_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_pivot_interpolated_job_role_ratios_with_different_unix_time_data,
            Schemas.expected_pivot_interpolated_job_role_ratios_schema,
        )

        returned_df = job.pivot_interpolated_job_role_ratios(test_df)

        self.assertEqual(
            expected_df.orderBy(IndCQC.location_id, IndCQC.unix_time).collect(),
            returned_df.orderBy(IndCQC.location_id, IndCQC.unix_time).collect(),
        )

    def test_pivot_interpolated_job_role_ratios_when_unix_time_is_different_and_null_interpolated_values_returns_pivoted_job_role_labels(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.pivot_interpolated_job_role_ratios_with_different_unix_time_and_null_interpolated_values_data,
            Schemas.pivot_interpolated_job_role_ratios_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_pivot_interpolated_job_role_ratios_with_different_unix_time_and_null_interpolated_values_data,
            Schemas.expected_pivot_interpolated_job_role_ratios_schema,
        )

        returned_df = job.pivot_interpolated_job_role_ratios(test_df)

        self.assertEqual(
            expected_df.orderBy(IndCQC.location_id, IndCQC.unix_time).collect(),
            returned_df.orderBy(IndCQC.location_id, IndCQC.unix_time).collect(),
        )

    def test_pivot_interpolated_job_role_ratios_when_location_id_is_different_returns_pivoted_job_role_labels(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.pivot_interpolated_job_role_ratios_with_different_location_id_data,
            Schemas.pivot_interpolated_job_role_ratios_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_pivot_interpolated_job_role_ratios_with_different_location_id_data,
            Schemas.expected_pivot_interpolated_job_role_ratios_schema,
        )

        returned_df = job.pivot_interpolated_job_role_ratios(test_df)

        self.assertEqual(
            expected_df.orderBy(IndCQC.location_id, IndCQC.unix_time).collect(),
            returned_df.orderBy(IndCQC.location_id, IndCQC.unix_time).collect(),
        )

    def test_pivot_interpolated_job_role_ratios_when_location_id_is_different_and_null_interpreted_values_returns_pivoted_job_role_labels(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.pivot_interpolated_job_role_ratios_with_different_location_id_and_null_interpolated_values_data,
            Schemas.pivot_interpolated_job_role_ratios_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_pivot_interpolated_job_role_ratios_with_different_location_id_and_null_interpolated_values_data,
            Schemas.expected_pivot_interpolated_job_role_ratios_schema,
        )

        returned_df = job.pivot_interpolated_job_role_ratios(test_df)

        self.assertEqual(
            expected_df.orderBy(IndCQC.location_id, IndCQC.unix_time).collect(),
            returned_df.orderBy(IndCQC.location_id, IndCQC.unix_time).collect(),
        )

    def test(self,):

        test_df = self.spark.createDataFrame(
            Data.pivot_example_data,
            Schemas.pivot_interpolated_job_role_ratios_schema,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_pivot_example_data,
            Schemas.expected_pivot_interpolated_job_role_ratios_schema,
        )

        returned_df = job.pivot_interpolated_job_role_ratios(test_df)

        expected_df.show(truncate=False)
        returned_df.show(truncate=False)

        self.assertEqual(
            expected_df.orderBy(IndCQC.location_id, IndCQC.unix_time).collect(),
            returned_df.orderBy(IndCQC.location_id, IndCQC.unix_time).collect(),
        )


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
