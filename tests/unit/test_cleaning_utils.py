import unittest

from utils import utils

import utils.cleaning_utils as job

from tests.test_file_schemas import CleaningUtilsSchemas as Schemas
from tests.test_file_data import CleaningUtilsData as Data

from utils.column_names.raw_data_files.ascwds_worker_columns import (
    AscwdsWorkerColumns as AWK,
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

gender_labels: str = "gender_labels"
nationality_labels: str = "nationality_labels"


class TestCleaningUtils(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()
        self.test_worker_df = self.spark.createDataFrame(
            Data.worker_rows, schema=Schemas.worker_schema
        )
        self.replace_labels_df = self.spark.createDataFrame(
            Data.replace_labels_rows, schema=Schemas.replace_labels_schema
        )
        self.label_df = self.spark.createDataFrame(Data.gender, Schemas.labels_schema)
        self.label_dict = {AWK.gender: Data.gender, AWK.nationality: Data.nationality}
        self.expected_df_with_new_columns = self.spark.createDataFrame(
            Data.expected_rows_with_new_columns,
            Schemas.expected_schema_with_new_columns,
        )
        self.expected_df_without_new_columns = self.spark.createDataFrame(
            Data.expected_rows_without_new_columns, Schemas.worker_schema
        )

    def test_apply_categorical_labels_completes(self):
        returned_df = job.apply_categorical_labels(
            self.test_worker_df,
            self.spark,
            self.label_dict,
            [AWK.gender, AWK.nationality],
            add_as_new_column=True,
        )

        self.assertIsNotNone(returned_df)

    def test_apply_categorical_labels_adds_a_new_column_when_given_one_column_and_new_column_is_set_to_true(
        self,
    ):
        returned_df = job.apply_categorical_labels(
            self.test_worker_df,
            self.spark,
            self.label_dict,
            [AWK.gender],
            add_as_new_column=True,
        )

        expected_columns = len(self.test_worker_df.columns) + 1

        self.assertEqual(len(returned_df.columns), expected_columns)

    def test_apply_categorical_labels_adds_two_new_columns_when_given_two_columns_and_new_column_is_set_to_true(
        self,
    ):
        returned_df = job.apply_categorical_labels(
            self.test_worker_df,
            self.spark,
            self.label_dict,
            [AWK.gender, AWK.nationality],
            add_as_new_column=True,
        )

        expected_columns = len(self.test_worker_df.columns) + 2

        self.assertEqual(len(returned_df.columns), expected_columns)

    def test_apply_categorical_labels_adds_new_columns_with_replaced_values_when_new_column_is_set_to_true(
        self,
    ):
        returned_df = job.apply_categorical_labels(
            self.test_worker_df,
            self.spark,
            self.label_dict,
            [AWK.gender, AWK.nationality],
            add_as_new_column=True,
        )
        returned_data = returned_df.sort(AWK.worker_id).collect()
        expected_data = self.expected_df_with_new_columns.sort(AWK.worker_id).collect()
        self.assertEqual(returned_data, expected_data)

    def test_apply_categorical_labels_does_not_add_a_new_column_when_given_one_column_and_new_column_is_set_to_false(
        self,
    ):
        returned_df = job.apply_categorical_labels(
            self.test_worker_df,
            self.spark,
            self.label_dict,
            [AWK.gender],
            add_as_new_column=False,
        )

        expected_columns = len(self.test_worker_df.columns)

        self.assertEqual(len(returned_df.columns), expected_columns)

    def test_apply_categorical_labels_does_not_add_new_columns_when_given_two_columns_and_new_column_is_set_to_false(
        self,
    ):
        returned_df = job.apply_categorical_labels(
            self.test_worker_df,
            self.spark,
            self.label_dict,
            [AWK.gender, AWK.nationality],
            add_as_new_column=False,
        )

        expected_columns = len(self.test_worker_df.columns)

        self.assertEqual(len(returned_df.columns), expected_columns)

    def test_apply_categorical_labels_replaces_values_when_new_column_is_set_to_false(
        self,
    ):
        returned_df = job.apply_categorical_labels(
            self.test_worker_df,
            self.spark,
            self.label_dict,
            [AWK.gender, AWK.nationality],
            add_as_new_column=False,
        )
        returned_data = returned_df.sort(AWK.worker_id).collect()
        expected_data = self.expected_df_without_new_columns.sort(
            AWK.worker_id
        ).collect()
        self.assertEqual(returned_data, expected_data)

    def test_apply_categorical_labels_adds_a_new_column_when_given_one_column_and_new_column_is_undefined(
        self,
    ):
        returned_df = job.apply_categorical_labels(
            self.test_worker_df, self.spark, self.label_dict, [AWK.gender]
        )

        expected_columns = len(self.test_worker_df.columns) + 1

        self.assertEqual(len(returned_df.columns), expected_columns)

    def test_apply_categorical_labels_adds_two_new_columns_when_given_two_columns_and_new_column_is_undefined(
        self,
    ):
        returned_df = job.apply_categorical_labels(
            self.test_worker_df,
            self.spark,
            self.label_dict,
            [AWK.gender, AWK.nationality],
        )

        expected_columns = len(self.test_worker_df.columns) + 2

        self.assertEqual(len(returned_df.columns), expected_columns)

    def test_apply_categorical_labels_adds_new_columns_with_replaced_values_when_new_column_is_undefined(
        self,
    ):
        returned_df = job.apply_categorical_labels(
            self.test_worker_df,
            self.spark,
            self.label_dict,
            [AWK.gender, AWK.nationality],
        )
        returned_data = returned_df.sort(AWK.worker_id).collect()
        expected_data = self.expected_df_with_new_columns.sort(AWK.worker_id).collect()
        self.assertEqual(returned_data, expected_data)

    def test_replace_labels_replaces_values_in_situe_when_new_column_name_is_null(self):
        returned_df = job.replace_labels(
            self.replace_labels_df,
            self.label_df,
            AWK.gender,
        )
        returned_data = returned_df.sort(AWK.worker_id).collect()

        expected_df = self.spark.createDataFrame(
            Data.expected_rows_replace_labels_in_situe, Schemas.replace_labels_schema
        )
        expected_data = expected_df.sort(AWK.worker_id).collect()
        self.assertEqual(returned_data, expected_data)

    def test_replace_labels_replaces_values_in_new_column_when_new_column_name_is_supplied(
        self,
    ):
        returned_df = job.replace_labels(
            self.replace_labels_df,
            self.label_df,
            AWK.gender,
            new_column_name=gender_labels,
        )
        returned_data = returned_df.sort(AWK.worker_id).collect()
        expected_df = self.spark.createDataFrame(
            Data.expected_rows_replace_labels_with_new_column,
            Schemas.expected_schema_replace_labels_with_new_columns,
        )
        expected_data = expected_df.sort(AWK.worker_id).collect()
        self.assertEqual(returned_data, expected_data)
