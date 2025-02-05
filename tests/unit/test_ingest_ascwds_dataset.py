import unittest

import jobs.ingest_ascwds_dataset as job
from tests.test_file_data import IngestASCWDSData as Data
from tests.test_file_schemas import IngestASCWDSData as Schemas
from utils import utils


class IngestASCWDSDatasetTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()


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
