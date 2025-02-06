import unittest

from tests.test_file_data import RegisteredManagerNamesCountData as Data
from tests.test_file_schemas import RegisteredManagerNamesCountSchema as Schemas

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.estimate_filled_posts_by_job_utils.ascwds_mapped_column_job_role_count.count_registered_manager_names import (
    count_registered_manager_names,
)


class CountRegisteredManagerNamesTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

    def test_count_registered_manager_names_when_location_has_one_registered_manager(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.location_with_one_registered_manager,
            Schemas.location_with_list_of_names,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_location_with_one_registered_manager,
            Schemas.location_with_list_of_names_and_count_of_names,
        )

        returned_df = count_registered_manager_names(test_df)

        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )

    def test_count_registered_manager_names_when_location_has_two_registered_managers(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.location_with_two_registered_managers,
            Schemas.location_with_list_of_names,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_location_with_two_registered_managers,
            Schemas.location_with_list_of_names_and_count_of_names,
        )

        returned_df = count_registered_manager_names(test_df)

        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )

    def test_count_registered_manager_names_when_location_has_null_registered_manager(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.location_with_null_registered_manager,
            Schemas.location_with_list_of_names,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_location_with_null_registered_manager,
            Schemas.location_with_list_of_names_and_count_of_names,
        )

        returned_df = count_registered_manager_names(test_df)

        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )

    def test_count_registered_manager_names_when_location_has_only_nulls_element_in_list(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.location_with_only_null_element_in_registered_manager_names_list,
            Schemas.location_with_list_of_names,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_location_with_only_null_element_in_registered_manager_names_list,
            Schemas.location_with_list_of_names_and_count_of_names,
        )

        returned_df = count_registered_manager_names(test_df)

        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )

    def test_count_registered_manager_names_when_location_has_null_and_named_registered_manager(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.location_with_null_elements_in_list_as_well_as_registered_manager_names,
            Schemas.location_with_list_of_names,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_location_with_null_elements_in_list_as_well_as_registered_manager_names,
            Schemas.location_with_list_of_names_and_count_of_names,
        )

        returned_df = count_registered_manager_names(test_df)

        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )

    def test_count_registered_manager_names_when_two_locations_have_different_number_of_registered_managers(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.two_locations_with_different_number_of_registered_managers,
            Schemas.location_with_list_of_names,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_two_locations_with_different_number_of_registered_managers,
            Schemas.location_with_list_of_names_and_count_of_names,
        )

        returned_df = count_registered_manager_names(test_df)

        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )

    def test_count_registered_manager_names_when_location_has_different_registered_manager_count_at_different_import_dates(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.location_with_different_number_of_registered_managers_at_different_import_dates,
            Schemas.location_with_list_of_names,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_location_with_different_number_of_registered_managers_at_different_import_dates,
            Schemas.location_with_list_of_names_and_count_of_names,
        )

        returned_df = count_registered_manager_names(test_df)

        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )
