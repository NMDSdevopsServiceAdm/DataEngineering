import unittest
import warnings

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

from utils.prepare_locations_utils.job_calculator.calculate_jobcount_coalesce_totalstaff_wkrrecs import \
    calculate_jobcount_coalesce_totalstaff_wkrrecs


class TestJobCountCoalesceWorkerRecords(unittest.TestCase):
    calculate_jobs_schema = StructType(
        [
            StructField("locationid", StringType(), False),
            StructField("total_staff", IntegerType(), True),
            StructField("worker_record_count", IntegerType(), True),
            StructField("number_of_beds", IntegerType(), True),
            StructField("job_count", DoubleType(), True),
        ]
    )

    def setUp(self):
        self.spark = SparkSession.builder.appName(
            "test_calculate_coalesce"
        ).getOrCreate()

        warnings.simplefilter("ignore", ResourceWarning)

    def test_calculate_jobcount_coalesce_totalstaff_wkrrecs(self):
        rows = [
            ("1-000000001", None, 20, 25, None),
            ("1-000000002", 30, None, 25, None),
            ("1-000000002", 35, 40, 25, None),
        ]
        df = self.spark.createDataFrame(data=rows, schema=self.calculate_jobs_schema)

        df = calculate_jobcount_coalesce_totalstaff_wkrrecs(df)
        self.assertEqual(df.count(), 3)

        df = df.collect()
        self.assertEqual(df[0]["job_count"], 20)
        self.assertEqual(df[1]["job_count"], 30)
        self.assertEqual(df[2]["job_count"], None)
