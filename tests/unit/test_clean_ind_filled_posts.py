import unittest
import warnings
from datetime import date
from unittest.mock import ANY, Mock, patch

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, DateType

import jobs.clean_ind_cqc_filled_posts as job
from tests.test_file_generator import generate_ind_cqc_filled_posts_file_parquet
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
    IndCqcColumns,
)


class CleanIndFilledPostsTests(unittest.TestCase):
    IND_FILELD_POSTS_DIR = "input_dir"
    IND_FILELD_POSTS_CLEANED_DIR = "output_dir"
    partition_keys = [
        Keys.year,
        Keys.month,
        Keys.day,
        Keys.import_date,
    ]

    def setUp(self):
        self.spark = utils.get_spark()
        self.test_df = generate_ind_cqc_filled_posts_file_parquet()
        warnings.filterwarnings("ignore", category=ResourceWarning)

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main(
        self,
        read_from_parquet_mock,
        write_to_parquet_mock: Mock,
    ):
        read_from_parquet_mock.return_value = self.test_df

        job.main(
            self.IND_FILELD_POSTS_DIR,
            self.IND_FILELD_POSTS_CLEANED_DIR,
        )

        write_to_parquet_mock.assert_called_once_with(
            ANY,
            self.IND_FILELD_POSTS_CLEANED_DIR,
            mode=ANY,
            partitionKeys=self.partition_keys,
        )

    def test_replace_zero_beds_with_null(self):
        columns = [
            IndCqcColumns.location_id,
            IndCqcColumns.number_of_beds,
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
        self.assertEqual(df[0][IndCqcColumns.number_of_beds], None)
        self.assertEqual(df[1][IndCqcColumns.number_of_beds], None)
        self.assertEqual(df[2][IndCqcColumns.number_of_beds], 1)

    def test_populate_missing_care_home_number_of_beds(self):
        schema = StructType(
            [
                StructField(IndCqcColumns.location_id, StringType(), True),
                StructField(IndCqcColumns.cqc_location_import_date, DateType(), True),
                StructField(IndCqcColumns.care_home, StringType(), True),
                StructField(IndCqcColumns.number_of_beds, IntegerType(), True),
            ]
        )

        input_rows = [
            ("1-000000001", date(2023, 1, 1), "Y", None),
            ("1-000000002", date(2023, 1, 1), "N", None),
            ("1-000000003", date(2023, 1, 1), "Y", 1),
            ("1-000000003", date(2023, 2, 1), "Y", None),
            ("1-000000003", date(2023, 3, 1), "Y", 1),
            ("1-000000004", date(2023, 1, 1), "Y", 1),
            ("1-000000004", date(2023, 2, 1), "Y", 3),
        ]
        input_df = self.spark.createDataFrame(input_rows, schema=schema)

        df = job.populate_missing_care_home_number_of_beds(input_df)
        self.assertEqual(df.count(), 7)

        df = df.sort(
            IndCqcColumns.location_id, IndCqcColumns.cqc_location_import_date
        ).collect()
        self.assertEqual(df[0][IndCqcColumns.number_of_beds], None)
        self.assertEqual(df[1][IndCqcColumns.number_of_beds], None)
        self.assertEqual(df[2][IndCqcColumns.number_of_beds], 1)
        self.assertEqual(df[3][IndCqcColumns.number_of_beds], 1)
        self.assertEqual(df[4][IndCqcColumns.number_of_beds], 1)
        self.assertEqual(df[5][IndCqcColumns.number_of_beds], 1)
        self.assertEqual(df[6][IndCqcColumns.number_of_beds], 3)

    def test_filter_to_care_homes_with_known_beds(self):
        columns = [
            IndCqcColumns.location_id,
            IndCqcColumns.care_home,
            IndCqcColumns.number_of_beds,
        ]
        rows = [
            ("1-000000001", "Y", None),
            ("1-000000002", "N", None),
            ("1-000000003", "Y", 1),
            ("1-000000004", "N", 1),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = job.filter_to_care_homes_with_known_beds(df)
        self.assertEqual(df.count(), 1)

        df = df.collect()
        self.assertEqual(df[0][IndCqcColumns.location_id], "1-000000003")

    def test_average_beds_per_location(self):
        columns = [
            IndCqcColumns.location_id,
            IndCqcColumns.number_of_beds,
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

        df = df.sort(IndCqcColumns.location_id).collect()
        self.assertEqual(df[0]["avg_beds"], 1)
        self.assertEqual(df[1]["avg_beds"], 2)
        self.assertEqual(df[2]["avg_beds"], 3)

    def test_replace_null_beds_with_average(self):
        columns = [IndCqcColumns.location_id, IndCqcColumns.number_of_beds, "avg_beds"]
        rows = [
            ("1-000000001", None, None),
            ("1-000000002", None, 1),
            ("1-000000003", 2, 2),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = job.replace_null_beds_with_average(df)
        self.assertEqual(df.count(), 3)

        df = df.collect()
        self.assertEqual(df[0][IndCqcColumns.number_of_beds], None)
        self.assertEqual(df[1][IndCqcColumns.number_of_beds], 1)
        self.assertEqual(df[2][IndCqcColumns.number_of_beds], 2)

    def test_replace_null_beds_with_average_doesnt_change_known_beds(self):
        columns = [IndCqcColumns.location_id, IndCqcColumns.number_of_beds, "avg_beds"]
        rows = [
            ("1-000000001", 1, 2),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = job.replace_null_beds_with_average(df)
        self.assertEqual(df.count(), 1)

        df = df.collect()
        self.assertEqual(df[0][IndCqcColumns.number_of_beds], 1)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
