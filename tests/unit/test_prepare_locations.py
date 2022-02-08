import datetime
import shutil
import unittest
from pathlib import Path

from jobs import format_fields, prepare_locations
from pyspark.sql import SparkSession
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType)


class PrepareLocationsTests(unittest.TestCase):

    calculate_jobs_schema = StructType(
        [
            StructField('locationid', StringType(), False),
            StructField(
                'totalstaff', IntegerType(), True),
            StructField(
                'wkrrecs', IntegerType(), True),
            StructField(
                'numberofbeds', IntegerType(), True),
            StructField(
                'jobcount', DoubleType(), True)
        ]
    )

    def setUp(self):
        self.spark = SparkSession.builder \
            .appName("test_prepare_locations") \
            .getOrCreate()

    def test_format_date(self):
        columns = ["locationid", "import_date"]
        rows = [
            ("1-000000001", "20221201"),
            ("1-000000002", "20220328")
        ]
        df = self.spark.createDataFrame(rows, columns)

        formatted_df = prepare_locations.format_date(
            df, "import_date", "new_formatted_date")

        self.assertEqual(formatted_df.count(), 2)
        self.assertEqual(formatted_df.select("new_formatted_date").rdd.flatMap(lambda x: x).collect(), [
            datetime.date(2022, 12, 1), datetime.date(2022, 3, 28)])

    def test_remove_duplicates(self):
        columns = ["locationid", "ascwds_workplace_import_date"]
        rows = [
            ("1-000000001", "01-12-2022"),
            ("1-000000001", "01-12-2022"),
            ("1-000000001", "01-12-2021"),
            ("1-000000001", "01-12-2021"),
            ("1-000000002", "01-12-2022"),
            ("1-000000002", "01-12-2021")
        ]
        df = self.spark.createDataFrame(rows, columns)

        filtered_df = prepare_locations.remove_duplicates(df)
        self.assertEqual(filtered_df.count(), 4)
        self.assertEqual(filtered_df.select("locationid").rdd.flatMap(lambda x: x).collect(), [
                         "1-000000001", "1-000000001", "1-000000002", "1-000000002"])

    def test_filter_nulls(self):
        columns = ["locationid", "wkrrecs", "totalstaff"]
        rows = [
            ("1-000000001", None, 20),
            ("1-000000002", 500, 500),
            ("1-000000003", 100, None),
            ("1-000000004", None, None),
            ("1-000000005", 25, 75),
            (None, 1, 0),
        ]
        df = self.spark.createDataFrame(rows, columns)

        filtered_df = prepare_locations.filter_nulls(df)
        self.assertEqual(filtered_df.count(), 4)
        self.assertEqual(filtered_df.select("locationid").rdd.flatMap(lambda x: x).collect(), [
                         "1-000000001", "1-000000002", "1-000000003", "1-000000005"])

    def test_clean(self):

        columns = ["locationid", "wkrrecs", "totalstaff"]
        rows = [
            ("1-000000001", None, "0"),
            ("1-000000002", "500", "500"),
            ("1-000000003", "100", "-1"),
            ("1-000000004", None, "0"),
            ("1-000000005", "25", "75"),
            (None, "1", "0"),
        ]
        df = self.spark.createDataFrame(rows, columns)

        cleaned_df = prepare_locations.clean(df)
        cleaned_df_list = cleaned_df.collect()
        self.assertEqual(cleaned_df.count(), 6)
        self.assertEqual(cleaned_df_list[0]["totalstaff"], None)
        self.assertEqual(cleaned_df_list[1]["totalstaff"], 500)

    def test_calculate_jobcount_totalstaff_equal_wkrrecs(self):
        columns = ["locationid", "wkrrecs",
                   "totalstaff", "numberofbeds", "jobcount"]
        rows = [
            ("1-000000001", 20, 20, 25, None),
        ]
        df = self.spark.createDataFrame(
            data=rows, schema=self.calculate_jobs_schema)

        df = prepare_locations.calculate_jobcount_totalstaff_equal_wkrrecs(df)
        self.assertEqual(df.count(), 1)

        df = df.collect()
        self.assertEqual(df[0]["jobcount"], 20)

    def test_calculate_jobcount_coalesce_totalstaff_wkrrecs(self):
        rows = [
            ("1-000000001", None, 50, 15, None),
        ]
        df = self.spark.createDataFrame(
            data=rows, schema=self.calculate_jobs_schema)

        df = prepare_locations.calculate_jobcount_coalesce_totalstaff_wkrrecs(
            df)
        self.assertEqual(df.count(), 1)

        df = df.collect()
        self.assertEqual(df[0]["jobcount"], 50)

    def test_calculate_jobcount_abs_difference_within_range(self):
        rows = [
            ("1-000000008", 10, 12, 15, None),
            ("1-000000001", 100, 109, 80, None),
        ]
        df = self.spark.createDataFrame(
            data=rows, schema=self.calculate_jobs_schema)

        df = prepare_locations.calculate_jobcount_abs_difference_within_range(
            df)
        self.assertEqual(df.count(), 2)

        df = df.collect()
        self.assertEqual(df[0]["jobcount"], 11)
        self.assertEqual(df[1]["jobcount"], 104.5)

    def test_calculate_jobcount_handle_tiny_values(self):
        rows = [
            ("1-000000008", 2, 53, 26, None),
        ]
        df = self.spark.createDataFrame(
            data=rows, schema=self.calculate_jobs_schema)

        df = prepare_locations.calculate_jobcount_handle_tiny_values(
            df)
        self.assertEqual(df.count(), 1)

        df = df.collect()
        self.assertEqual(df[0]["jobcount"], 53)

    def test_calculate_jobcount(self):
        columns = ["locationid", "wkrrecs", "totalstaff", "numberofbeds"]
        rows = [
            ("1-000000001", None, 0, 0),  # Both 0: Return 0
            # Both 500: Return 500
            ("1-000000002", 500, 500, 490),
            # Only know wkrrecs: Return wkrrecs (100)
            ("1-000000003", 100, None, 10),
            # Only know totalstaff: Return totalstaf (10)
            ("1-000000004", None, 10, 12),
            # None of the rules apply: Return None
            ("1-000000005", 25, 75, 40),
            # None of the rules apply: Return None
            ("1-000000006", 30, 60, 40),
            # None of the rules apply: Return None
            ("1-000000007", 600, 900, 150),
            # Absolute difference is within 10%: Return Average
            ("1-000000008", 10, 12, None),
            # Either totalstaff or wkrrecs < 3: return max
            ("1-000000009", 1, 23, None),
            # Utilise bedcount estimate - Average
            ("1-000000010", 90, 102, 85),
            # Utilise bedcount estimate - Wkrrecs
            ("1-000000011", 90, 102, 95),
            # Utilise bedcount estimate - Totalstaff
            ("1-000000012", 90, 102, 80)
        ]
        df = self.spark.createDataFrame(rows, columns)

        jobcount_df = prepare_locations.calculate_jobcount(df)
        jobcount_df_list = jobcount_df.collect()

        self.assertEqual(jobcount_df_list[0]["jobcount"], 0.0)
        self.assertEqual(jobcount_df_list[1]["jobcount"], 500.0)
        self.assertEqual(jobcount_df_list[2]["jobcount"], 100.0)
        self.assertEqual(jobcount_df_list[3]["jobcount"], 10.0)
        self.assertEqual(jobcount_df_list[4]["jobcount"], None)
        self.assertEqual(jobcount_df_list[5]["jobcount"], None)
        self.assertEqual(jobcount_df_list[6]["jobcount"], None)
        self.assertEqual(jobcount_df_list[7]["jobcount"], 11.0)
        self.assertEqual(jobcount_df_list[8]["jobcount"], 23.0)
        self.assertEqual(jobcount_df_list[9]["jobcount"], 96.0)
        self.assertEqual(jobcount_df_list[10]["jobcount"], 102.0)
        self.assertEqual(jobcount_df_list[11]["jobcount"], 90.0)


if __name__ == '__main__':
    unittest.main()
