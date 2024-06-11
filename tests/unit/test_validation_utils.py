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
        super().setUp()
        self.rules = Data.multiple_rules
        self.unknown_rules = Data.unknown_rules
        self.test_df = self.spark.createDataFrame(
            Data.multiple_rules_rows, Schemas.multiple_rules_schema
        )
        self.expected_df = self.spark.createDataFrame(
            Data.multiple_rules_results_rows, Schemas.validation_schema
        )

    def test_validate_dataset_can_run_checks_with_multiple_rules(self):
        self.returned_df = job.validate_dataset(self.test_df, self.rules)
        self.assertEqual(self.returned_df.collect(), self.expected_df.collect())

    def test_validate_dataset_raises_error_with_unknown_rules(self):
        with self.assertRaises(ValueError) as context:
            job.validate_dataset(self.test_df, self.unknown_rules)

        self.assertTrue("Unknown rule to check" in str(context.exception))


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


class CheckOfMinValues(ValidateUtilsTests):
    def setUp(self) -> None:
        super().setUp()
        self.min_values_rule = Data.min_values_rule
        self.min_values_multiple_columns_rule = Data.min_values_multiple_columns_rule

    def test_create_check_of_min_values_returns_success_when_values_are_above_minimum(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.min_values_above_minimum_rows, Schemas.min_values_schema
        )
        expected_df = self.spark.createDataFrame(
            Data.min_values_result_success_rows, Schemas.validation_schema
        )
        returned_df = job.validate_dataset(test_df, self.min_values_rule)
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_create_check_of_min_values_returns_success_when_lowest_value_equals_minimum(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.min_values_equal_minimum_rows, Schemas.min_values_schema
        )
        expected_df = self.spark.createDataFrame(
            Data.min_values_result_success_rows, Schemas.validation_schema
        )
        returned_df = job.validate_dataset(test_df, self.min_values_rule)
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_create_check_of_min_values_returns_failure_when_lowest_value_is_below_minimum(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.min_values_below_minimum_rows, Schemas.min_values_schema
        )
        expected_df = self.spark.createDataFrame(
            Data.min_values_result_below_minimum_rows, Schemas.validation_schema
        )
        returned_df = job.validate_dataset(test_df, self.min_values_rule)
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_create_check_of_min_values_returns_correct_results_when_multiple_columns_supplied(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.min_values_multiple_columns_rows,
            Schemas.min_values_multiple_columns_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.min_values_result_multiple_columns_rows, Schemas.validation_schema
        )
        returned_df = job.validate_dataset(
            test_df, self.min_values_multiple_columns_rule
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())


class CheckOfMaxValues(ValidateUtilsTests):
    def setUp(self) -> None:
        super().setUp()
        self.max_values_rule = Data.max_values_rule
        self.max_values_multiple_columns_rule = Data.max_values_multiple_columns_rule

    def test_create_check_of_max_values_returns_success_when_values_are_below_maximum(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.max_values_below_maximum_rows, Schemas.max_values_schema
        )
        expected_df = self.spark.createDataFrame(
            Data.max_values_result_success_rows, Schemas.validation_schema
        )
        returned_df = job.validate_dataset(test_df, self.max_values_rule)
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_create_check_of_max_values_returns_success_when_highest_value_equals_maximum(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.max_values_equal_maximum_rows, Schemas.max_values_schema
        )
        expected_df = self.spark.createDataFrame(
            Data.max_values_result_success_rows, Schemas.validation_schema
        )
        returned_df = job.validate_dataset(test_df, self.max_values_rule)
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_create_check_of_max_values_returns_failure_when_highest_value_is_above_maximum(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.max_values_above_maximum_rows, Schemas.max_values_schema
        )
        expected_df = self.spark.createDataFrame(
            Data.max_values_result_above_maximum_rows, Schemas.validation_schema
        )
        returned_df = job.validate_dataset(test_df, self.max_values_rule)
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_create_check_of_max_values_returns_correct_results_when_multiple_columns_supplied(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.max_values_multiple_columns_rows,
            Schemas.max_values_multiple_columns_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.max_values_result_multiple_columns_rows, Schemas.validation_schema
        )
        returned_df = job.validate_dataset(
            test_df, self.max_values_multiple_columns_rule
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())


class CheckCategoricalValues(ValidateUtilsTests):
    def setUp(self) -> None:
        super().setUp()
        self.categorical_values_rule = Data.categorical_values_rule

    def test_create_check_of_categorical_values_in_columns_returns_success_when_all_values_are_present(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.categorical_values_all_present_rows, Schemas.categorical_values_schema
        )
        expected_df = self.spark.createDataFrame(
            Data.categorical_values_result_success_rows, Schemas.validation_schema
        )
        returned_df = job.validate_dataset(test_df, self.categorical_values_rule)
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_create_check_of_categorical_values_in_columns_returns_success_when_some_values_are_present(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.categorical_values_some_present_rows, Schemas.categorical_values_schema
        )
        expected_df = self.spark.createDataFrame(
            Data.categorical_values_result_success_rows, Schemas.validation_schema
        )
        returned_df = job.validate_dataset(test_df, self.categorical_values_rule)
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_create_check_of_categorical_values_in_columns_returns_failure_when_additional_values_are_present(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.categorical_values_extra_rows,
            Schemas.categorical_values_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.categorical_values_result_failure_rows, Schemas.validation_schema
        )
        returned_df = job.validate_dataset(test_df, self.categorical_values_rule)
        self.assertEqual(returned_df.collect(), expected_df.collect())


class CheckOfNumberOfDistinctValuesInColumns(ValidateUtilsTests):
    def setUp(self) -> None:
        super().setUp()
        self.distinct_values_rule = Data.distinct_values_rule
        self.distinct_values_multiple_columns_rule = (
            Data.distinct_values_multiple_columns_rule
        )

    def test_create_check_of_number_of_distinct_values_returns_success_when_column_has_correct_number_of_distinct_values(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.distinct_values_success_rows, Schemas.distinct_values_schema
        )
        expected_df = self.spark.createDataFrame(
            Data.distinct_values_result_success_rows, Schemas.validation_schema
        )
        returned_df = job.validate_dataset(test_df, self.distinct_values_rule)
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_create_check_of_number_of_distinct_values_returns_failure_when_column_has_fewer_distinct_values_than_expected(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.fewer_distinct_values_rows, Schemas.distinct_values_schema
        )
        expected_df = self.spark.createDataFrame(
            Data.fewer_distinct_values_result_rows, Schemas.validation_schema
        )
        returned_df = job.validate_dataset(test_df, self.distinct_values_rule)
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_create_check_of_number_of_distinct_values_returns_failure_when_column_has_more_distinct_values_than_expected(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.more_distinct_values_rows, Schemas.distinct_values_schema
        )
        expected_df = self.spark.createDataFrame(
            Data.more_distinct_values_result_rows, Schemas.validation_schema
        )
        returned_df = job.validate_dataset(test_df, self.distinct_values_rule)
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_create_check_of_number_of_distinct_values_returns_correct_results_when_multiple_columns_supplied(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.distinct_values_multiple_columns_rows,
            Schemas.distinct_values_multiple_columns_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.distinct_values_result_multiple_columns_rows, Schemas.validation_schema
        )
        returned_df = job.validate_dataset(
            test_df, self.distinct_values_multiple_columns_rule
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())


class CheckMaxLengthOfString(ValidateUtilsTests):
    def setUp(self) -> None:
        super().setUp()
        self.max_length_of_string_rule = Data.max_length_of_string_rule
        self.max_length_of_string_multiple_columns_rule = (
            Data.max_length_of_string_multiple_columns_rule
        )

    def test_create_check_of_max_length_of_string_returns_success_when_values_are_below_maximum(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.max_length_of_string_below_maximum_rows,
            Schemas.max_length_of_string_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.max_length_of_string_result_success_rows, Schemas.validation_schema
        )
        returned_df = job.validate_dataset(test_df, self.max_length_of_string_rule)
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_create_check_of_max_length_of_string_returns_success_when_highest_value_equals_maximum(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.max_length_of_string_equal_maximum_rows,
            Schemas.max_length_of_string_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.max_length_of_string_result_success_rows, Schemas.validation_schema
        )
        returned_df = job.validate_dataset(test_df, self.max_length_of_string_rule)
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_create_check_of_max_length_of_string_returns_failure_when_highest_value_is_above_maximum(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.max_length_of_string_above_maximum_rows,
            Schemas.max_length_of_string_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.max_length_of_string_result_above_maximum_rows,
            Schemas.validation_schema,
        )
        returned_df = job.validate_dataset(test_df, self.max_length_of_string_rule)
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_create_check_of_max_length_of_string_returns_correct_results_when_multiple_columns_supplied(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.max_length_of_string_multiple_columns_rows,
            Schemas.max_length_of_string_multiple_columns_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.max_length_of_string_result_multiple_columns_rows,
            Schemas.validation_schema,
        )
        returned_df = job.validate_dataset(
            test_df, self.max_length_of_string_multiple_columns_rule
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())


if __name__ == "__main__":
    unittest.main(warnings="ignore")
