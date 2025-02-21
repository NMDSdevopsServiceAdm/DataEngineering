import unittest

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


class CountJobRoleSplitByServiceTypeTests(EstimateIndCQCFilledPostsByJobRoleUtilsTests):

    def setUp(self) -> None:
        super().setUp()

    def sample_test(
        self,
    ):
        
        input_df = self.spark.createDataFrame(
            Data.count_job_role_split_by_service_data,
            Schemas.count_job_role_split_by_service_schema,
        )

        expected_output_df = self.spark.createDataFrame(
            Data.expected_count_job_role_split_by_service_data,
            Schemas.expected_count_role_split_by_service_schema,
        )

        output_df = job.sum_job_role_count_split_by_service(
            input_df, Data.list_of_job_roles_for_tests
        )

        self.assertEqual(
            output_df.sort(IndCQC.establishment_id).collect(),
            expected_output_df.sort(IndCQC.establishment_id).collect(),
        )
