import unittest
from unittest.mock import ANY, Mock, patch
import pyspark.sql.functions as F

from utils import utils

import jobs.flatten_cqc_ratings as job

from tests.test_file_data import FlattenCQCRatings as Data
from tests.test_file_schemas import FlattenCQCRatings as Schema


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
        write_to_parquet_patch.assert_called_once_with(
            ANY,
            self.TEST_CQC_RATINGS_DESTINATION,
            mode="overwrite",
        )

class FilterToMonthlyImportDate(FlattenCQCRatingsTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_cqc_df = self.spark.createDataFrame(Data.filter_to_monthly_import_date_rows, Schema.filter_to_monthly_import_date_schema)
        self.test_cqc_when_not_first_of_month_df = self.spark.createDataFrame(Data.filter_to_monthly_import_date_when_not_first_of_month_rows, Schema.filter_to_monthly_import_date_schema)
        self.expected_data = self.spark.createDataFrame(Data.expected_filter_to_monthly_import_date_rows, Schema.filter_to_monthly_import_date_schema).collect()
    
    def test_filter_to_monthly_import_date_returns_correct_rows_when_most_recent_data_is_the_first_of_the_month(self):
        returned_data = job.filter_to_monthly_import_date(self.test_cqc_df).collect()
        self.assertEqual(returned_data, self.expected_data)

    def test_filter_to_monthly_import_date_returns_correct_rows_when_most_recent_data_is_not_the_first_of_the_month(self):
        returned_data = job.filter_to_monthly_import_date(self.test_cqc_when_not_first_of_month_df).collect()
        self.assertEqual(returned_data, self.expected_data)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
