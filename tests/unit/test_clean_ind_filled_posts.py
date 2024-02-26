import unittest
import warnings
from datetime import date
from unittest.mock import Mock, patch

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

import jobs.clean_ind_cqc_filled_posts as job
from tests.test_file_generator import \
    generate_ind_cqc_filled_posts_file_parquet
from utils import utils


class CleanIndFilledPostsTests(unittest.TestCase):
    IND_FILELD_POSTS_DIR = "tests/test_data/tmp/ind_filled_posts/"
    IND_FILELD_POSTS_CLEANED_DIR = "tests/test_data/tmp/ind_filled_posts_cleaned/"

    def setUp(self):
        self.spark = utils.get_spark()
        self.test_df = generate_ind_cqc_filled_posts_file_parquet()
        warnings.filterwarnings("ignore", category=ResourceWarning)

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    @patch("utils.utils.get_s3_sub_folders_for_path")
    @patch("jobs.prepare_locations_cleaned.date")
    def test_main_partitions_data_based_on_todays_date(
        self,
        mock_date,
        mock_get_s3_folders,
        mock_read_from_parquet,
        mock_write_to_parquet: Mock,
    ):
        mock_read_from_parquet.return_value = self.test_df
        mock_get_s3_folders.return_value = ["1.0.0"]
        mock_date.today.return_value = date(2022, 6, 29)
        mock_date.side_effect = lambda *args, **kw: date(*args, **kw)

        job.main(
            self.IND_FILELD_POSTS_DIR,
            self.IND_FILELD_POSTS_CLEANED_DIR,
        )

        cleaned_df: DataFrame = mock_write_to_parquet.call_args[0][0]

        year_partition = cleaned_df.select("year")

        self.assertIsNotNone(year_partition)
        self.assertEqual(year_partition.collect()[0][0], "2022")

        month_partition = cleaned_df.select("month")

        self.assertIsNotNone(month_partition)
        self.assertEqual(month_partition.collect()[0][0], "06")

        day_partition = cleaned_df.select("day")

        self.assertIsNotNone(day_partition)
        self.assertEqual(day_partition.collect()[0][0], "29")

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
            ("1-000000004", "2023-01-01", "Y", 1),
            ("1-000000004", "2023-02-01", "Y", 3),
        ]
        input_df = self.spark.createDataFrame(input_rows, schema=schema)

        df = job.populate_missing_carehome_number_of_beds(input_df)
        self.assertEqual(df.count(), 7)

        df = df.sort("locationid", "snapshot_date").collect()
        self.assertEqual(df[0]["number_of_beds"], None)
        self.assertEqual(df[1]["number_of_beds"], None)
        self.assertEqual(df[2]["number_of_beds"], 1)
        self.assertEqual(df[3]["number_of_beds"], 1)
        self.assertEqual(df[4]["number_of_beds"], 1)
        self.assertEqual(df[5]["number_of_beds"], 1)
        self.assertEqual(df[6]["number_of_beds"], 3)

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
