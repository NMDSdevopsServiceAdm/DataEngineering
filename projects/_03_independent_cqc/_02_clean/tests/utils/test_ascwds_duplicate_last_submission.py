import unittest

import projects._03_independent_cqc._02_clean.utils.ascwds_duplicate_last_submission as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    AscwdsDuplicateLastSubmission as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    AscwdsDuplicateLastSubmission as Schemas,
)
from utils import utils


class AscwdsDuplicateLastSubmissionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = utils.get_spark()


class GetImportDateOfLastKnownValueTests(AscwdsDuplicateLastSubmissionTests):
    def test_duplicate_latest_known_value_into_following_two_rows_returns_expected_values_when_last_known_value_is_before_latest_import(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.duplicate_latest_known_value_into_following_two_rows_when_last_known_value_is_before_latest_import_rows,
            Schemas.duplicate_latest_known_value_into_following_two_rows_schema,
        )
        returned_df = job.duplicate_latest_known_value_into_following_two_rows(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_duplicate_latest_known_value_into_following_two_rows_when_last_known_value_is_before_latest_import_rows,
            Schemas.duplicate_latest_known_value_into_following_two_rows_schema,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_duplicate_latest_known_value_into_following_two_rows_returns_expected_values_when_last_known_value_is_near_latest_import(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.duplicate_latest_known_value_into_following_two_rows_when_last_known_value_is_near_latest_import_rows,
            Schemas.duplicate_latest_known_value_into_following_two_rows_schema,
        )
        returned_df = job.duplicate_latest_known_value_into_following_two_rows(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_duplicate_latest_known_value_into_following_two_rows_when_last_known_value_is_near_latest_import_rows,
            Schemas.duplicate_latest_known_value_into_following_two_rows_schema,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())
