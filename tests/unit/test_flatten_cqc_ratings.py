import unittest
from unittest.mock import ANY, Mock, patch
import pyspark.sql.functions as F

from utils import utils

import jobs.flatten_cqc_ratings as job

from tests.test_file_data import FlattenCQCRatings as Data
from tests.test_file_schemas import FlattenCQCRatings as Schema

from utils.cqc_ratings_utils.cqc_ratings_values import (
    CQCRatingsValues,
)


class FlattenCQCRatingsTests(unittest.TestCase):
    TEST_LOCATIONS_SOURCE = "some/directory"
    TEST_WORKPLACE_SOURCE = "some/directory"
    TEST_CQC_RATINGS_DESTINATION = "some/other/directory"
    TEST_BENCHMARK_RATINGS_DESTINATION = "some/other/directory"

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_cqc_locations_df = self.spark.createDataFrame(
            Data.test_cqc_locations_rows, schema=Schema.test_cqc_locations_schema
        )
        self.test_ascwds_df = self.spark.createDataFrame(
            Data.test_ascwds_workplace_rows, schema=Schema.test_ascwds_workplace_schema
        )


class MainTests(FlattenCQCRatingsTests):
    def setUp(self) -> None:
        super().setUp()

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main(self, read_from_parquet_patch: Mock, write_to_parquet_patch: Mock):
        read_from_parquet_patch.side_effect = [
            self.test_cqc_locations_df,
            self.test_ascwds_df,
        ]
        job.main(
            self.TEST_LOCATIONS_SOURCE,
            self.TEST_WORKPLACE_SOURCE,
            self.TEST_CQC_RATINGS_DESTINATION,
            self.TEST_BENCHMARK_RATINGS_DESTINATION,
        )
        self.assertEqual(read_from_parquet_patch.call_count, 2)
        write_to_parquet_patch.assert_called_once_with(
            ANY,
            self.TEST_CQC_RATINGS_DESTINATION,
            mode="overwrite",
        )


class FilterToMonthlyImportDate(FlattenCQCRatingsTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_cqc_df = self.spark.createDataFrame(
            Data.filter_to_monthly_import_date_rows,
            Schema.filter_to_monthly_import_date_schema,
        )
        self.test_cqc_when_not_first_of_month_df = self.spark.createDataFrame(
            Data.filter_to_monthly_import_date_when_not_first_of_month_rows,
            Schema.filter_to_monthly_import_date_schema,
        )
        self.expected_data = self.spark.createDataFrame(
            Data.expected_filter_to_monthly_import_date_rows,
            Schema.filter_to_monthly_import_date_schema,
        ).collect()

    def test_filter_to_monthly_import_date_returns_correct_rows_when_most_recent_data_is_the_first_of_the_month(
        self,
    ):
        returned_data = job.filter_to_monthly_import_date(self.test_cqc_df).collect()
        self.assertEqual(returned_data, self.expected_data)

    def test_filter_to_monthly_import_date_returns_correct_rows_when_most_recent_data_is_not_the_first_of_the_month(
        self,
    ):
        returned_data = job.filter_to_monthly_import_date(
            self.test_cqc_when_not_first_of_month_df
        ).collect()
        self.assertEqual(returned_data, self.expected_data)


class FlattenCurrentRatings(FlattenCQCRatingsTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_cqc_current_ratings_df = self.spark.createDataFrame(
            Data.flatten_current_ratings_rows, Schema.flatten_current_ratings_schema
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_flatten_ratings_rows,
            Schema.expected_flatten_ratings_schema,
        )
        self.returned_df = job.flatten_current_ratings(self.test_cqc_current_ratings_df)

    def test_flatten_current_ratings_returns_correct_columns(self):
        returned_columns = len(self.returned_df.columns)
        expected_columns = len(self.expected_df.columns)
        self.assertEqual(returned_columns, expected_columns)

    def test_flatten_current_ratings_returns_correct_rows(self):
        returned_rows = self.returned_df.count()
        expected_rows = self.expected_df.count()
        self.assertEqual(returned_rows, expected_rows)

    def test_flatten_current_ratings_returns_correct_values(self):
        returned_data = self.returned_df.collect()
        expected_data = self.expected_df.collect()
        self.assertEqual(returned_data, expected_data)


class FlattenHistoricRatings(FlattenCQCRatingsTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_cqc_historic_ratings_df = self.spark.createDataFrame(
            Data.flatten_historic_ratings_rows, Schema.flatten_historic_ratings_schema
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_flatten_ratings_rows,
            Schema.expected_flatten_ratings_schema,
        )
        self.returned_df = job.flatten_historic_ratings(
            self.test_cqc_historic_ratings_df
        )

    def test_flatten_historic_ratings_returns_correct_columns(self):
        returned_columns = len(self.returned_df.columns)
        expected_columns = len(self.expected_df.columns)
        self.assertEqual(returned_columns, expected_columns)

    def test_flatten_historic_ratings_returns_correct_rows(self):
        returned_rows = self.returned_df.count()
        expected_rows = self.expected_df.count()
        self.assertEqual(returned_rows, expected_rows)

    def test_flatten_historic_ratings_returns_correct_values(self):
        returned_data = self.returned_df.collect()
        expected_data = self.expected_df.collect()
        self.assertEqual(returned_data, expected_data)


class RecodeUnknownToNull(FlattenCQCRatingsTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_current_ratings_df = self.spark.createDataFrame(
            Data.recode_unknown_to_null_rows,
            Schema.expected_flatten_ratings_schema,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_recode_unknown_to_null_rows,
            Schema.expected_flatten_ratings_schema,
        )
        self.returned_df = job.recode_unknown_codes_to_null(
            self.test_current_ratings_df
        )

    def test_recode_unknown_codes_to_null_returns_correct_values(self):
        returned_data = self.returned_df.collect()
        expected_data = self.expected_df.collect()
        self.assertEqual(returned_data, expected_data)


class AddCurrentOrHistoricColumn(FlattenCQCRatingsTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_ratings_df = self.spark.createDataFrame(
            Data.add_current_or_historic_rows, Schema.add_current_or_historic_schema
        )
        self.expected_current_df = self.spark.createDataFrame(
            Data.expected_add_current_rows,
            Schema.expected_add_current_or_historic_schema,
        )
        self.expected_historic_df = self.spark.createDataFrame(
            Data.expected_add_historic_rows,
            Schema.expected_add_current_or_historic_schema,
        )
        self.returned_current_df = job.add_current_or_historic_column(
            self.test_ratings_df, CQCRatingsValues.current
        )
        self.returned_historic_df = job.add_current_or_historic_column(
            self.test_ratings_df, CQCRatingsValues.historic
        )

    def test_add_current_or_historic_column_returns_correct_values_when_passed_current(
        self,
    ):
        returned_data = self.returned_current_df.collect()
        expected_data = self.expected_current_df.collect()
        self.assertEqual(returned_data, expected_data)

    def test_add_current_or_historic_column_returns_correct_values_when_passed_historic(
        self,
    ):
        returned_data = self.returned_historic_df.collect()
        expected_data = self.expected_historic_df.collect()
        self.assertEqual(returned_data, expected_data)


class RemoveBlankRows(FlattenCQCRatingsTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_ratings_df = self.spark.createDataFrame(
            Data.remove_blank_rows_rows,
            Schema.remove_blank_rows_schema,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_remove_blank_rows_rows,
            Schema.remove_blank_rows_schema,
        )
        self.returned_df = job.remove_blank_and_duplicate_rows(self.test_ratings_df)

    def test_remove_blank_rows_returns_correct_values(self):
        returned_data = self.returned_df.collect()
        expected_data = self.expected_df.collect()
        self.assertEqual(returned_data, expected_data)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
