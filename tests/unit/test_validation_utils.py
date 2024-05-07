import unittest

from unittest.mock import Mock, patch

import utils.validation.validation_utils as job

from tests.test_file_data import ValidationUtils as Data

from tests.test_file_schemas import ValidationUtils as Schemas

from utils import utils


class ValidateUtilsTests(unittest.TestCase):

    def setUp(self) -> None:
        self.spark = utils.get_spark()

    def tearDown(self) -> None:
        if self.spark.sparkContext._gateway:
            self.spark.sparkContext._gateway.shutdown_callback_server()


class ValidateDatasetTests(ValidateUtilsTests):
    def setUp(self) -> None:
        return super().setUp()

    def test_validate_dataset(self):
        pass


class AddChecksToRunTests(ValidateUtilsTests):
    def setUp(self) -> None:
        return super().setUp()

    def test_add_checks_to_run(self):
        pass


class CreateCheckTests(ValidateUtilsTests):
    def setUp(self) -> None:
        return super().setUp()

    def test_create_check(self):
        pass


class CheckForColumnCompletenessTests(ValidateUtilsTests):
    def setUp(self) -> None:
        super().setUp()
        self.one_column_rule = Data.one_complete_column_rule
        self.two_column_rule = Data.two_complete_columns_rule

    def test_create_check_for_column_completeness_when_one_column_is_given_and_complete(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.one_complete_column_complete_rows, Schemas.one_column_schema
        )
        expected_df = self.spark.createDataFrame(
            Data.one_complete_column_result_complete_rows, Schemas.validation_schema
        )
        returned_df = job.validate_dataset(test_df, self.one_column_rule)

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_create_check_for_column_completeness_when_one_column_is_given_and_incomplete(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.one_complete_column_incomplete_rows, Schemas.one_column_schema
        )
        expected_df = self.spark.createDataFrame(
            Data.one_complete_column_result_incomplete_rows, Schemas.validation_schema
        )
        returned_df = job.validate_dataset(test_df, self.one_column_rule)

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_create_check_for_column_completeness_when_two_columns_are_given_and_both_are_complete(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.two_complete_columns_both_complete_rows, Schemas.two_column_schema
        )
        expected_df = self.spark.createDataFrame(
            Data.two_complete_columns_result_both_complete_rows,
            Schemas.validation_schema,
        )
        returned_df = job.validate_dataset(test_df, self.two_column_rule)

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_create_check_for_column_completeness_when_two_columns_are_given_and_one_is_incomplete(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.two_complete_columns_one_incomplete_rows, Schemas.two_column_schema
        )
        expected_df = self.spark.createDataFrame(
            Data.two_complete_columns_result_one_incomplete_rows,
            Schemas.validation_schema,
        )
        returned_df = job.validate_dataset(test_df, self.two_column_rule)

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_create_check_for_column_completeness_when_two_columns_are_given_and_both_are_incomplete(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.two_complete_columns_both_incomplete_rows, Schemas.two_column_schema
        )
        expected_df = self.spark.createDataFrame(
            Data.two_complete_columns_result_both_incomplete_rows,
            Schemas.validation_schema,
        )
        returned_df = job.validate_dataset(test_df, self.two_column_rule)

        self.assertEqual(returned_df.collect(), expected_df.collect())


class CheckOfUniquenessOfTwoIndexColumns(ValidateUtilsTests):
    def setUp(self) -> None:
        super().setUp()
        self.unique_columns_rule = Data.unique_index_columns_rule

    def test_create_check_of_uniqueness_of_two_index_columns_returns_success_when_columns_are_unique(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.unique_index_columns_success_rows, Schemas.index_column_schema
        )
        expected_df = self.spark.createDataFrame(
            Data.unique_index_columns_result_success_rows, Schemas.validation_schema
        )
        returned_df = job.validate_dataset(test_df, self.unique_columns_rule)
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_create_check_of_uniqueness_of_two_index_columns_returns_failure_when_columns_are_not_unique(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.unique_index_columns_not_unique_rows, Schemas.index_column_schema
        )
        expected_df = self.spark.createDataFrame(
            Data.unique_index_columns_result_not_unique_rows, Schemas.validation_schema
        )
        returned_df = job.validate_dataset(test_df, self.unique_columns_rule)

        self.assertEqual(returned_df.collect(), expected_df.collect())


class CheckOfSizeOfDataset(ValidateUtilsTests):
    def setUp(self) -> None:
        super().setUp()
        self.size_of_dataset_rule = Data.size_of_dataset_rule

    def test_create_check_of_size_of_dataset_returns_success_with_valid_data(self):
        test_df = self.spark.createDataFrame(
            Data.size_of_dataset_success_rows, Schemas.size_of_dataset_schema
        )
        expected_df = self.spark.createDataFrame(
            Data.size_of_dataset_result_success_rows, Schemas.validation_schema
        )
        returned_df = job.validate_dataset(test_df, self.size_of_dataset_rule)
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_create_check_of_size_of_dataset_returns_failure_when_rows_are_missing(
        self,
    ):
        missing_rows_df = self.spark.createDataFrame(
            Data.size_of_dataset_missing_rows, Schemas.size_of_dataset_schema
        )
        expected_df = self.spark.createDataFrame(
            Data.size_of_dataset_result_missing_rows, Schemas.validation_schema
        )
        returned_df = job.validate_dataset(missing_rows_df, self.size_of_dataset_rule)
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_create_check_of_size_of_dataset_returns_failure_when_extra_rows_are_present(
        self,
    ):
        extra_rows_df = self.spark.createDataFrame(
            Data.size_of_dataset_extra_rows, Schemas.size_of_dataset_schema
        )
        expected_df = self.spark.createDataFrame(
            Data.size_of_dataset_result_extra_rows, Schemas.validation_schema
        )
        returned_df = job.validate_dataset(extra_rows_df, self.size_of_dataset_rule)
        self.assertEqual(returned_df.collect(), expected_df.collect())


if __name__ == "__main__":
    unittest.main(warnings="ignore")
