import unittest
import warnings
from datetime import date
import re
import os
from unittest.mock import patch

from pyspark.sql import SparkSession

from tests.test_file_generator import generate_prepared_locations_preclean_file_parquet
from tests.test_helpers import remove_file_path
import jobs.prepare_locations_cleaned as job


class PrepareLocationsCleanedTests(unittest.TestCase):
    PREPARED_LOCATIONS_DIR = "tests/test_data/tmp/prepared_locations/"
    PREPARED_LOCATIONS_CLEANED_DIR = "tests/test_data/tmp/prepared_locations_cleaned/"

    def setUp(self):
        self.spark = SparkSession.builder.appName(
            "test_prepare_locations_cleaned"
        ).getOrCreate()
        generate_prepared_locations_preclean_file_parquet(self.PREPARED_LOCATIONS_DIR)
        warnings.filterwarnings("ignore", category=ResourceWarning)

    def tearDown(self):
        remove_file_path(self.PREPARED_LOCATIONS_DIR)
        remove_file_path(self.PREPARED_LOCATIONS_CLEANED_DIR)

    @patch("utils.utils.get_s3_sub_folders_for_path")
    @patch("jobs.prepare_locations_cleaned.date")
    def test_main_partitions_data_based_on_todays_date(
        self, mock_date, mock_get_s3_folders
    ):
        mock_get_s3_folders.return_value = ["1.0.0"]
        mock_date.today.return_value = date(2022, 6, 29)
        mock_date.side_effect = lambda *args, **kw: date(*args, **kw)
        generate_prepared_locations_preclean_file_parquet(self.PREPARED_LOCATIONS_DIR)

        job.main(
            self.PREPARED_LOCATIONS_DIR,
            self.PREPARED_LOCATIONS_CLEANED_DIR,
        )

        first_partitions = os.listdir(self.PREPARED_LOCATIONS_CLEANED_DIR)
        year_partition = next(
            re.match("^run_year=([0-9]{4})$", path)
            for path in first_partitions
            if re.match("^run_year=([0-9]{4})$", path)
        )

        self.assertIsNotNone(year_partition)
        self.assertEqual(year_partition.groups()[0], "2022")

        second_partitions = os.listdir(
            f"{self.PREPARED_LOCATIONS_CLEANED_DIR}/{year_partition.string}/"
        )
        month_partition = next(
            re.match("^run_month=([0-9]{2})$", path) for path in second_partitions
        )
        self.assertIsNotNone(month_partition)
        self.assertEqual(month_partition.groups()[0], "06")

        third_partitions = os.listdir(
            f"{self.PREPARED_LOCATIONS_CLEANED_DIR}/{year_partition.string}/{month_partition.string}/"
        )
        day_partition = next(
            re.match("^run_day=([0-9]{2})$", path) for path in third_partitions
        )
        self.assertIsNotNone(day_partition)
        self.assertEqual(day_partition.groups()[0], "29")

    def test_remove_unwanted_data(self):
        columns = [
            "locationid",
            "cqc_sector",
            "registration_status",
        ]
        rows = [
            ("1-000000001", "Local authority", "Registered"),
            ("1-000000002", "Independent", "Registered"),
            ("1-000000003", "Independent", "Deregistered"),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = job.remove_unwanted_data(df)
        self.assertEqual(df.count(), 1)

        df = df.collect()
        self.assertEqual(df[0]["locationid"], "1-000000002")


if __name__ == "__main__":
    unittest.main(warnings="ignore")
