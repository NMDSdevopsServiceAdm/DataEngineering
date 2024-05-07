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


class CreateCheckForColumnCompletenessTests(ValidateUtilsTests):
    def setUp(self) -> None:
        return super().setUp()

    def test_create_check_for_column_completeness(self):
        pass


class CreateCheckOfUniquenessOfTwoIndexColumns(ValidateUtilsTests):
    def setUp(self) -> None:
        return super().setUp()

    def test_create_check_of_uniqueness_of_two_index_columns(self):
        pass


class CreateCheckOfSizeOfDataset(ValidateUtilsTests):
    def setUp(self) -> None:
        super().setUp()
        self.size_of_dataset_rule = Data.size_of_dataset_rule
        self.test_df = self.spark.createDataFrame(
            Data.size_of_dataset_success_rows, Schemas.size_of_dataset_schema
        )
        self.expected_df = self.spark.createDataFrame(
            Data.size_of_dataset_result_success_rows, Schemas.validation_schema
        )

    def test_create_check_of_size_of_dataset(self):
        returned_df = job.validate_dataset(self.test_df, self.size_of_dataset_rule)
        returned_df.show(truncate=False)
        self.assertEqual(returned_df.collect(), self.expected_df.collect())


if __name__ == "__main__":
    unittest.main(warnings="ignore")
