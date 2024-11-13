import unittest

import jobs.ingest_ascwds_dataset as job
from tests.test_file_data import IngestASCWDSData as Data
from tests.test_file_schemas import IngestASCWDSData as Schemas
from utils import utils


class IngestASCWDSDatasetTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()


class FilterTestAccountsTests(IngestASCWDSDatasetTests):
    def setUp(self) -> None:
        super().setUp()

    def test_filter_test_accounts(self):
        columns = [
            "locationid",
            "orgid",
            "location_feature",
        ]
        rows = [
            ("1-000000001", "310", "Definitely a feature"),
            ("1-000000002", "2452", "Not important"),
            ("1-000000003", "308", "Test input"),
            ("1-000000004", "1234", "Something else"),
            ("1-000000005", "31138", "A new test account"),
        ]

        df = self.spark.createDataFrame(rows, columns)

        df = job.filter_test_accounts(df)
        self.assertEqual(df.count(), 1)

        df = df.collect()
        self.assertEqual(df[0]["location_feature"], "Something else")
        self.assertEqual(df[0]["locationid"], "1-000000004")

    def test_filter_test_accounts_without_orgid_doesnt_filter_rows(self):
        columns = [
            "locationid",
            "location_feature",
        ]
        rows = [
            ("1-000000001", "Definitely a feature"),
            ("1-000000002", "Not important"),
            ("1-000000003", "Test input"),
            ("1-000000004", "Something else"),
        ]

        df = self.spark.createDataFrame(rows, columns)

        df = job.filter_test_accounts(df)
        self.assertEqual(df.count(), 4)


class RemoveWhiteSpaceFromNmdsidTests(IngestASCWDSDatasetTests):
    def setUp(self) -> None:
        super().setUp()

    def test_remove_white_space_from_nmdsid(self):
        columns = [
            "locationid",
            "nmdsid",
        ]
        rows = [
            ("1-000000001", "A123  "),
            ("1-000000002", "A1234 "),
            ("1-000000003", "A12345"),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = job.remove_white_space_from_nmdsid(df)
        self.assertEqual(df.count(), 3)

        df = df.sort("locationid").collect()
        self.assertEqual(df[0]["nmdsid"], "A123")
        self.assertEqual(df[1]["nmdsid"], "A1234")
        self.assertEqual(df[2]["nmdsid"], "A12345")


class RaiseErrorIfMainjridIncludesUnknownValuesTests(IngestASCWDSDatasetTests):
    def setUp(self) -> None:
        super().setUp()

        self.ascwds_without_mainjrid_df = self.spark.createDataFrame(
            Data.raise_mainjrid_error_col_not_present_rows,
            Schemas.raise_mainjrid_error_when_mainjrid_not_in_df_schema,
        )
        self.ascwds_with_known_mainjrid_df = self.spark.createDataFrame(
            Data.raise_mainjrid_error_with_known_value_rows,
            Schemas.raise_mainjrid_error_when_mainjrid_in_df_schema,
        )

    def test_error_not_raised_if_mainjrid_column_not_present(
        self,
    ):
        try:
            job.raise_error_if_mainjrid_includes_unknown_values(
                self.ascwds_without_mainjrid_df
            )
        except ValueError:
            self.fail(
                "raise_error_if_mainjrid_includes_unknown_values() raised ValueError unexpectedly"
            )

    def test_returns_original_df_if_mainjrid_column_not_present(
        self,
    ):
        returned_df = job.raise_error_if_mainjrid_includes_unknown_values(
            self.ascwds_without_mainjrid_df
        )
        self.assertEqual(
            returned_df.collect(), self.ascwds_without_mainjrid_df.collect()
        )

    def test_error_not_raised_if_mainjrid_present_and_all_values_known(
        self,
    ):
        try:
            job.raise_error_if_mainjrid_includes_unknown_values(
                self.ascwds_with_known_mainjrid_df
            )
        except ValueError:
            self.fail(
                "raise_error_if_mainjrid_includes_unknown_values() raised ValueError unexpectedly"
            )

    def test_returns_original_df_if_mainjrid_present_and_all_values_known(
        self,
    ):
        returned_df = job.raise_error_if_mainjrid_includes_unknown_values(
            self.ascwds_with_known_mainjrid_df
        )
        self.assertEqual(
            returned_df.collect(), self.ascwds_with_known_mainjrid_df.collect()
        )

    def test_raises_error_if_mainjrid_includes_unknown_values(self):
        test_df = self.spark.createDataFrame(
            Data.raise_mainjrid_error_with_unknown_value_rows,
            Schemas.raise_mainjrid_error_when_mainjrid_in_df_schema,
        )

        with self.assertRaises(ValueError) as context:
            job.raise_error_if_mainjrid_includes_unknown_values(test_df)

        self.assertIn(
            "Error: this file contains 1 unknown mainjrid record(s)",
            str(context.exception),
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
