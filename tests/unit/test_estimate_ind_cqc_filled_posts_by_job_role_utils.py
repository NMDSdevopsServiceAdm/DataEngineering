import unittest

from utils import utils
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.estimate_filled_posts_by_job_role_utils import utils as job
from tests.test_file_data import EstimateFilledPostsByJobRoleData as Data
from tests.test_file_schemas import EstimateFilledPostsByJobRoleSchema as Schemas


class EstimateFilledPostsByJobRoleTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()


class CountRegisteredManagerNamesTests(EstimateFilledPostsByJobRoleTests):
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


class CountJobRolesPerEstablishmentTests(EstimateFilledPostsByJobRoleTests):
    def setUp(self) -> None:
        super().setUp()

    def test_count_job_role_per_establishment_as_columns_returns_expected_number_of_columns_when_called(
        self,
    ):
        test_workplace_with_three_distinct_job_role_df = self.spark.createDataFrame(
            Data.workplace_with_three_distinct_job_role,
            Schemas.ascwds_worker_schema,
        )
        returned_df = job.count_job_role_per_establishment_as_columns(
            test_workplace_with_three_distinct_job_role_df, Data.list_of_job_roles
        )
        self.assertEqual(
            len(returned_df.columns),
            len(test_workplace_with_three_distinct_job_role_df.columns) + 3,
        )

    def test_count_job_role_per_establishment_as_columns_returns_correct_counts_when_workplace_has_three_distinct_job_roles(
        self,
    ):
        test_workplace_with_three_distinct_job_role_df = self.spark.createDataFrame(
            Data.workplace_with_three_distinct_job_role,
            Schemas.ascwds_worker_schema,
        )
        expected_workplace_with_three_distinct_job_role_df = self.spark.createDataFrame(
            Data.expected_workplace_with_three_distinct_job_role,
            Schemas.ascwds_worker_with_columns_per_count_of_job_role_per_establishment,
        )

        returned_df = job.count_job_role_per_establishment_as_columns(
            test_workplace_with_three_distinct_job_role_df, Data.list_of_job_roles
        )

        self.assertEqual(
            returned_df.sort(AWKClean.establishment_id).collect(),
            expected_workplace_with_three_distinct_job_role_df.collect(),
        )

    def test_count_job_role_per_establishment_as_columns_returns_correct_counts_when_has_none_job_role(
        self,
    ):
        test_workplace_with_none_job_role_df = self.spark.createDataFrame(
            Data.workplace_with_none_job_role,
            Schemas.ascwds_worker_schema,
        )
        expected_workplace_with_none_job_role_df = self.spark.createDataFrame(
            Data.expected_workplace_with_none_job_role,
            Schemas.ascwds_worker_with_columns_per_count_of_job_role_per_establishment,
        )
        returned_df = job.count_job_role_per_establishment_as_columns(
            test_workplace_with_none_job_role_df, Data.list_of_job_roles
        )
        self.assertEqual(
            returned_df.sort(AWKClean.establishment_id).collect(),
            expected_workplace_with_none_job_role_df.collect(),
        )

    def test_count_job_role_per_establishment_as_columns_returns_correct_counts_when_workplace_has_different_import_date(
        self,
    ):
        test_workplace_with_different_import_date_df = self.spark.createDataFrame(
            Data.workplace_with_different_import_date,
            Schemas.ascwds_worker_schema,
        )
        expected_workplace_with_different_import_date_df = self.spark.createDataFrame(
            Data.expected_workplace_with_different_import_date,
            Schemas.ascwds_worker_with_columns_per_count_of_job_role_per_establishment,
        )
        returned_df = job.count_job_role_per_establishment_as_columns(
            test_workplace_with_different_import_date_df, Data.list_of_job_roles
        )
        self.assertEqual(
            returned_df.sort(
                AWKClean.establishment_id, AWKClean.ascwds_worker_import_date
            ).collect(),
            expected_workplace_with_different_import_date_df.sort(
                AWKClean.establishment_id, AWKClean.ascwds_worker_import_date
            ).collect(),
        )

    def test_count_job_role_per_establishment_as_columns_returns_correct_counts_when_workplace_has_different_establishmentid(
        self,
    ):
        test_workplace_with_different_establishmentid_df = self.spark.createDataFrame(
            Data.workplace_with_different_establishmentid,
            Schemas.ascwds_worker_schema,
        )
        expected_workplace_with_different_establishmentid_df = self.spark.createDataFrame(
            Data.expected_workplace_with_different_establishmentid,
            Schemas.ascwds_worker_with_columns_per_count_of_job_role_per_establishment,
        )
        returned_df = job.count_job_role_per_establishment_as_columns(
            test_workplace_with_different_establishmentid_df, Data.list_of_job_roles
        )
        self.assertEqual(
            returned_df.sort(AWKClean.establishment_id).collect(),
            expected_workplace_with_different_establishmentid_df.collect(),
        )

    def test_count_job_role_per_establishment_as_columns_returns_correct_counts_when_workplace_has_three_job_roles_with_two_being_distinct(
        self,
    ):
        test_workplace_three_jobs_roles_with_two_being_distinct_df = (
            self.spark.createDataFrame(
                Data.workplace_three_jobs_roles_with_two_being_distinct,
                Schemas.ascwds_worker_schema,
            )
        )
        expected_workplace_three_jobs_roles_with_two_being_distinct_df = self.spark.createDataFrame(
            Data.exptected_workplace_three_job_roles_with_two_being_distinct,
            Schemas.ascwds_worker_with_columns_per_count_of_job_role_per_establishment,
        )
        returned_df = job.count_job_role_per_establishment_as_columns(
            test_workplace_three_jobs_roles_with_two_being_distinct_df,
            Data.list_of_job_roles,
        )
        self.assertEqual(
            returned_df.sort(AWKClean.establishment_id).collect(),
            expected_workplace_three_jobs_roles_with_two_being_distinct_df.collect(),
        )

    def test_merge_dataframes_returns_ind_cqc_estimate_filled_posts_with_job_role_counts_when_one_workplace_match(
        self,
    ):
        test_left_table_df = self.spark.createDataFrame(
            Data.ind_cqc_estimated_filled_posts_by_job_role,
            Schemas.IndCQCEstimateFilledPostsByJobRoleSchema,
        )

        test_workplace_with_one_record_matching_df = self.spark.createDataFrame(
            Data.workplace_with_one_record_matching,
            Schemas.ascwds_worker_with_columns_per_count_of_job_role_per_establishment,
        )

        expected_workplace_with_one_record_matching_df = self.spark.createDataFrame(
            Data.expected_workplace_with_one_record_matching,
            Schemas.merged_job_role_estimate_schema,
        )

        returned_df = job.merge_dataframes(
            test_left_table_df, test_workplace_with_one_record_matching_df
        )

        self.assertEqual(
            returned_df.sort(AWKClean.establishment_id).collect(),
            expected_workplace_with_one_record_matching_df.sort(
                AWKClean.establishment_id
            ).collect(),
        )

    def test_merge_dataframes_returns_ind_cqc_estimate_filled_posts_with_job_role_counts_when_all_workplaces_match(
        self,
    ):
        test_left_table_df = self.spark.createDataFrame(
            Data.ind_cqc_estimated_filled_posts_by_job_role,
            Schemas.IndCQCEstimateFilledPostsByJobRoleSchema,
        )

        test_workplace_with_no_records_matching_df = self.spark.createDataFrame(
            Data.workplace_with_all_records_matching,
            Schemas.ascwds_worker_with_columns_per_count_of_job_role_per_establishment,
        )

        expected_workplace_with_all_records_matching_df = self.spark.createDataFrame(
            Data.expected_workplace_with_all_records_matching,
            Schemas.merged_job_role_estimate_schema,
        )

        returned_df = job.merge_dataframes(
            test_left_table_df, test_workplace_with_no_records_matching_df
        )

        self.assertEqual(
            returned_df.sort(AWKClean.establishment_id).collect(),
            expected_workplace_with_all_records_matching_df.sort(
                AWKClean.establishment_id
            ).collect(),
        )

    def test_merge_dataframes_returns_ind_cqc_estimate_filled_posts_with_job_role_counts_when_no_workplaces_match(
        self,
    ):
        test_left_table_df = self.spark.createDataFrame(
            Data.ind_cqc_estimated_filled_posts_by_job_role,
            Schemas.IndCQCEstimateFilledPostsByJobRoleSchema,
        )

        test_workplace_with_no_records_matching_df = self.spark.createDataFrame(
            Data.workplace_with_no_records_matching,
            Schemas.ascwds_worker_with_columns_per_count_of_job_role_per_establishment,
        )

        expected_workplace_with_no_records_matching_df = self.spark.createDataFrame(
            Data.expected_workplace_with_no_records_matching,
            Schemas.merged_job_role_estimate_schema,
        )

        returned_df = job.merge_dataframes(
            test_left_table_df, test_workplace_with_no_records_matching_df
        )

        self.assertEqual(
            returned_df.sort(AWKClean.establishment_id).collect(),
            expected_workplace_with_no_records_matching_df.sort(
                AWKClean.establishment_id
            ).collect(),
        )