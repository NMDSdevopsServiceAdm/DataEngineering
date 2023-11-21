import unittest
import warnings
import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)


import jobs.ingest_capacity_tracker_data as job


class TestJobCountAbsDiffInRange(unittest.TestCase):
    calculate_jobs_schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("Last_Updated_UTC", StringType(), True),
        ]
    )

    def setUp(self):
        self.spark = SparkSession.builder.appName(
            "test_ingest_capacity_tracker_data"
        ).getOrCreate()

        warnings.simplefilter("ignore", ResourceWarning)

    def test_add_column_with_formatted_dates_care_homes(self):
        rows = [
            ("1", "08 Mar 2022 12:03"),
            ("2", "09 Feb 2022 15:23"),
        ]
        df = self.spark.createDataFrame(data=rows, schema=self.calculate_jobs_schema)

        df = job.add_column_with_formatted_dates(
            df, "Last_Updated_UTC", "Last_Updated_UTC_formatted", "dd MMM yyyy"
        )
        self.assertEqual(df.count(), 2)

        df = df.collect()
        self.assertEqual(df[0]["Last_Updated_UTC_formatted"], datetime.date(2022, 3, 8))
        self.assertEqual(df[1]["Last_Updated_UTC_formatted"], datetime.date(2022, 2, 9))

    def test_add_column_with_formatted_dates_non_res(self):
        rows = [
            ("1", "08/03/2022 12:03"),
            ("2", "09/02/2022 15:23"),
        ]
        df = self.spark.createDataFrame(data=rows, schema=self.calculate_jobs_schema)

        df = job.add_column_with_formatted_dates(
            df, "Last_Updated_UTC", "Last_Updated_UTC_formatted", "dd/MM/yyyy "
        )
        self.assertEqual(df.count(), 2)

        df = df.collect()
        self.assertEqual(df[0]["Last_Updated_UTC_formatted"], datetime.date(2022, 3, 8))
        self.assertEqual(df[1]["Last_Updated_UTC_formatted"], datetime.date(2022, 2, 9))
