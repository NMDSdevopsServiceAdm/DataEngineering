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
