import unittest
import warnings
from datetime import date
import re
import os
from unittest.mock import patch

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    IntegerType,
)

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

    def test_replace_zero_beds_with_null(self):
        columns = [
            "locationid",
            "number_of_beds",
        ]
        rows = [
            ("1-000000001", None),
            ("1-000000002", 0),
            ("1-000000003", 1),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = job.replace_zero_beds_with_null(df)
        self.assertEqual(df.count(), 3)

        df = df.collect()
        self.assertEqual(df[0]["number_of_beds"], None)
        self.assertEqual(df[1]["number_of_beds"], None)
        self.assertEqual(df[2]["number_of_beds"], 1)

    def test_populate_missing_carehome_number_of_beds(self):
        schema = StructType(
            [
                StructField("locationId", StringType(), True),
                StructField("snapshot_date", StringType(), True),
                StructField("carehome", StringType(), True),
                StructField("number_of_beds", IntegerType(), True),
            ]
        )

        input_rows = [
            ("1-000000001", "2023-01-01", "Y", None),
            ("1-000000002", "2023-01-01", "N", None),
            ("1-000000003", "2023-01-01", "Y", 1),
            ("1-000000003", "2023-02-01", "Y", None),
            ("1-000000003", "2023-03-01", "Y", 1),
        ]
        input_df = self.spark.createDataFrame(input_rows, schema=schema)

        df = job.populate_missing_carehome_number_of_beds(input_df)
        self.assertEqual(df.count(), 5)

        df = df.sort("locationid", "snapshot_date").collect()
        self.assertEqual(df[0]["number_of_beds"], None)
        self.assertEqual(df[1]["number_of_beds"], None)
        self.assertEqual(df[2]["number_of_beds"], 1)
        self.assertEqual(df[3]["number_of_beds"], 1)
        self.assertEqual(df[4]["number_of_beds"], 1)

    def test_filter_to_carehomes_with_known_beds(self):
        columns = [
            "locationid",
            "carehome",
            "number_of_beds",
        ]
        rows = [
            ("1-000000001", "Y", None),
            ("1-000000002", "N", None),
            ("1-000000003", "Y", 1),
            ("1-000000004", "N", 1),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = job.filter_to_carehomes_with_known_beds(df)
        self.assertEqual(df.count(), 1)

        df = df.collect()
        self.assertEqual(df[0]["locationid"], "1-000000003")

    def test_average_beds_per_location(self):
        columns = [
            "locationid",
            "number_of_beds",
        ]
        rows = [
            ("1-000000001", 1),
            ("1-000000002", 2),
            ("1-000000002", 3),
            ("1-000000003", 2),
            ("1-000000003", 3),
            ("1-000000003", 4),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = job.average_beds_per_location(df)
        self.assertEqual(df.count(), 3)

        df = df.sort("locationid").collect()
        self.assertEqual(df[0]["avg_beds"], 1)
        self.assertEqual(df[1]["avg_beds"], 2)
        self.assertEqual(df[2]["avg_beds"], 3)

    def test_replace_null_beds_with_average(self):
        columns = ["locationid", "number_of_beds", "avg_beds"]
        rows = [
            ("1-000000001", None, None),
            ("1-000000002", None, 1),
            ("1-000000003", 2, 2),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = job.replace_null_beds_with_average(df)
        self.assertEqual(df.count(), 3)

        df = df.collect()
        self.assertEqual(df[0]["number_of_beds"], None)
        self.assertEqual(df[1]["number_of_beds"], 1)
        self.assertEqual(df[2]["number_of_beds"], 2)

    def test_replace_null_beds_with_average_doesnt_change_known_beds(self):
        columns = ["locationid", "number_of_beds", "avg_beds"]
        rows = [
            ("1-000000001", 1, 2),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = job.replace_null_beds_with_average(df)
        self.assertEqual(df.count(), 1)

        df = df.collect()
        self.assertEqual(df[0]["number_of_beds"], 1)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
