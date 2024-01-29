import unittest

from utils import utils

import utils.cleaning_utils as job

from tests.test_file_schemas import CleaningUtilsSchemas as Schemas
from tests.test_file_data import CleaningUtilsData as Data

from utils.column_names.raw_data_files.ascwds_worker_columns import (
    AscwdsWorkerColumns as AWK,
)


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
        self.expected_categorical_labels = {
            "gender_labels": ["male", "male", "female", "female", None, "female"],
            "nationality_labels": [
                "British",
                "French",
                "Spanish",
                "Portuguese",
                "Portuguese",
                None,
            ],
        }

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

        self.assertEqual(
            returned_data[0]["gender_labels"],
            self.expected_categorical_labels["gender_labels"][0],
        )
        self.assertEqual(
            returned_data[1]["gender_labels"],
            self.expected_categorical_labels["gender_labels"][1],
        )
        self.assertEqual(
            returned_data[2]["gender_labels"],
            self.expected_categorical_labels["gender_labels"][2],
        )
        self.assertEqual(
            returned_data[3]["gender_labels"],
            self.expected_categorical_labels["gender_labels"][3],
        )
        self.assertEqual(
            returned_data[4]["gender_labels"],
            self.expected_categorical_labels["gender_labels"][4],
        )
        self.assertEqual(
            returned_data[5]["gender_labels"],
            self.expected_categorical_labels["gender_labels"][5],
        )

        self.assertEqual(
            returned_data[0]["nationality_labels"],
            self.expected_categorical_labels["nationality_labels"][0],
        )
        self.assertEqual(
            returned_data[1]["nationality_labels"],
            self.expected_categorical_labels["nationality_labels"][1],
        )
        self.assertEqual(
            returned_data[2]["nationality_labels"],
            self.expected_categorical_labels["nationality_labels"][2],
        )
        self.assertEqual(
            returned_data[3]["nationality_labels"],
            self.expected_categorical_labels["nationality_labels"][3],
        )
        self.assertEqual(
            returned_data[4]["nationality_labels"],
            self.expected_categorical_labels["nationality_labels"][4],
        )
        self.assertEqual(
            returned_data[5]["nationality_labels"],
            self.expected_categorical_labels["nationality_labels"][5],
        )

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

        self.assertEqual(
            returned_data[0]["gender"],
            self.expected_categorical_labels["gender_labels"][0],
        )
        self.assertEqual(
            returned_data[1]["gender"],
            self.expected_categorical_labels["gender_labels"][1],
        )
        self.assertEqual(
            returned_data[2]["gender"],
            self.expected_categorical_labels["gender_labels"][2],
        )
        self.assertEqual(
            returned_data[3]["gender"],
            self.expected_categorical_labels["gender_labels"][3],
        )
        self.assertEqual(
            returned_data[4]["gender"],
            self.expected_categorical_labels["gender_labels"][4],
        )
        self.assertEqual(
            returned_data[5]["gender"],
            self.expected_categorical_labels["gender_labels"][5],
        )

        self.assertEqual(
            returned_data[0]["nationality"],
            self.expected_categorical_labels["nationality_labels"][0],
        )
        self.assertEqual(
            returned_data[1]["nationality"],
            self.expected_categorical_labels["nationality_labels"][1],
        )
        self.assertEqual(
            returned_data[2]["nationality"],
            self.expected_categorical_labels["nationality_labels"][2],
        )
        self.assertEqual(
            returned_data[3]["nationality"],
            self.expected_categorical_labels["nationality_labels"][3],
        )
        self.assertEqual(
            returned_data[4]["nationality"],
            self.expected_categorical_labels["nationality_labels"][4],
        )
        self.assertEqual(
            returned_data[5]["nationality"],
            self.expected_categorical_labels["nationality_labels"][5],
        )

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

        self.assertEqual(
            returned_data[0]["gender_labels"],
            self.expected_categorical_labels["gender_labels"][0],
        )
        self.assertEqual(
            returned_data[1]["gender_labels"],
            self.expected_categorical_labels["gender_labels"][1],
        )
        self.assertEqual(
            returned_data[2]["gender_labels"],
            self.expected_categorical_labels["gender_labels"][2],
        )
        self.assertEqual(
            returned_data[3]["gender_labels"],
            self.expected_categorical_labels["gender_labels"][3],
        )
        self.assertEqual(
            returned_data[4]["gender_labels"],
            self.expected_categorical_labels["gender_labels"][4],
        )
        self.assertEqual(
            returned_data[5]["gender_labels"],
            self.expected_categorical_labels["gender_labels"][5],
        )

        self.assertEqual(
            returned_data[0]["nationality_labels"],
            self.expected_categorical_labels["nationality_labels"][0],
        )
        self.assertEqual(
            returned_data[1]["nationality_labels"],
            self.expected_categorical_labels["nationality_labels"][1],
        )
        self.assertEqual(
            returned_data[2]["nationality_labels"],
            self.expected_categorical_labels["nationality_labels"][2],
        )
        self.assertEqual(
            returned_data[3]["nationality_labels"],
            self.expected_categorical_labels["nationality_labels"][3],
        )
        self.assertEqual(
            returned_data[4]["nationality_labels"],
            self.expected_categorical_labels["nationality_labels"][4],
        )
        self.assertEqual(
            returned_data[5]["nationality_labels"],
            self.expected_categorical_labels["nationality_labels"][5],
        )

    def test_replace_labels_replaces_values_in_situe_when_new_column_name_is_null(self):
        returned_df = job.replace_labels(
            self.replace_labels_df,
            self.label_df,
            AWK.gender,
        )
        returned_data = returned_df.collect()

        expected_columns = {
            "gender": ["male", "female", None, None, "female"],
        }

        expected_columns_count = len(self.replace_labels_df.columns)

        self.assertEqual(len(returned_df.columns), expected_columns_count)

        self.assertEqual(returned_data[0]["gender"], expected_columns["gender"][0])
        self.assertEqual(returned_data[1]["gender"], expected_columns["gender"][1])
        self.assertEqual(returned_data[2]["gender"], expected_columns["gender"][2])
        self.assertEqual(returned_data[3]["gender"], expected_columns["gender"][3])
        self.assertEqual(returned_data[4]["gender"], expected_columns["gender"][4])

    def test_replace_labels_replaces_values_in_new_column_when_new_column_name_is_supplied(
        self,
    ):
        returned_df = job.replace_labels(
            self.replace_labels_df,
            self.label_df,
            AWK.gender,
            new_column_name="gender_labels",
        )
        returned_data = returned_df.collect()

        expected_columns = {
            "gender": ["1", "2", None, None, "2"],
            "gender_labels": ["male", "female", None, None, "female"],
        }
        expected_columns_count = len(self.replace_labels_df.columns) + 1

        self.assertEqual(len(returned_df.columns), expected_columns_count)

        self.assertEqual(returned_data[0]["gender"], expected_columns["gender"][0])
        self.assertEqual(returned_data[1]["gender"], expected_columns["gender"][1])
        self.assertEqual(returned_data[2]["gender"], expected_columns["gender"][2])
        self.assertEqual(returned_data[3]["gender"], expected_columns["gender"][3])
        self.assertEqual(returned_data[4]["gender"], expected_columns["gender"][4])

        self.assertEqual(
            returned_data[0]["gender_labels"], expected_columns["gender_labels"][0]
        )
        self.assertEqual(
            returned_data[1]["gender_labels"], expected_columns["gender_labels"][1]
        )
        self.assertEqual(
            returned_data[2]["gender_labels"], expected_columns["gender_labels"][2]
        )
        self.assertEqual(
            returned_data[3]["gender_labels"], expected_columns["gender_labels"][3]
        )
        self.assertEqual(
            returned_data[4]["gender_labels"], expected_columns["gender_labels"][4]
        )
