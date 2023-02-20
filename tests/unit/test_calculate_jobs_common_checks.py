import unittest
import warnings

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

from utils.prepare_locations_utils.job_calculator.common_checks import (
    column_value_is_less_than_min_abs_difference_between_total_staff_and_worker_record_count,
    selected_column_is_null,
    selected_column_is_not_null,
    selected_ascwds_job_count_is_at_least_the_min_permitted,
    job_count_from_ascwds_is_not_populated,
)


# noinspection PyTypeChecker
class TestJobCountTotalStaffEqualWorkerRecords(unittest.TestCase):
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
            "test_calculate_common_checks"
        ).getOrCreate()

        warnings.simplefilter("ignore", ResourceWarning)

    def test_job_count_from_ascwds_is_not_populated(self):
        rows = [("1-000000001", 2, 2, 3, None), ("1-000000002", 2, 2, 3, 1.5)]
        df = self.spark.createDataFrame(data=rows, schema=self.calculate_jobs_schema)
        result = df.withColumn(
            "result",
            F.when(
                (job_count_from_ascwds_is_not_populated("job_count")), True
            ).otherwise(False),
        )

        result_df = result.collect()
        self.assertEqual(result_df[0]["result"], True)
        self.assertEqual(result_df[1]["result"], False)

    def test_selected_column_is_null(self):

        rows = [("1-000000001", None, 2, 3, 1.5), ("1-000000002", 2, 2, 3, 1.5)]
        df = self.spark.createDataFrame(data=rows, schema=self.calculate_jobs_schema)
        result = df.withColumn(
            "result",
            F.when((selected_column_is_null("total_staff")), True).otherwise(False),
        )

        result_df = result.collect()
        self.assertEqual(result_df[0]["result"], True)
        self.assertEqual(result_df[1]["result"], False)

    def test_selected_column_is_not_null(self):

        rows = [("1-000000001", None, 2, 3, 1.5), ("1-000000002", 2, 2, 3, 1.5)]
        df = self.spark.createDataFrame(data=rows, schema=self.calculate_jobs_schema)
        result = df.withColumn(
            "result",
            F.when((selected_column_is_not_null("total_staff")), True).otherwise(False),
        )

        result_df = result.collect()
        self.assertEqual(result_df[0]["result"], False)
        self.assertEqual(result_df[1]["result"], True)

    def test_selected_ascwds_job_count_is_at_least_the_min_permitted(self):

        rows = [("1-000000001", 3, 2, 3, 1.5)]
        df = self.spark.createDataFrame(data=rows, schema=self.calculate_jobs_schema)
        result = df.withColumn(
            "result",
            F.when(
                (
                    selected_ascwds_job_count_is_at_least_the_min_permitted(
                        "total_staff"
                    )
                ),
                True,
            ).otherwise(False),
        )

        result_df = result.collect()
        self.assertEqual(result_df[0]["result"], True)

    def test_selected_ascwds_job_count_is_less_than_the_least_min_permitted(self):

        rows = [("1-000000001", 2, 3, 3, 1.5)]
        df = self.spark.createDataFrame(data=rows, schema=self.calculate_jobs_schema)
        result = df.withColumn(
            "result",
            F.when(
                (
                    selected_ascwds_job_count_is_at_least_the_min_permitted(
                        "total_staff"
                    )
                ),
                True,
            ).otherwise(False),
        )
        result.show()

        result_df = result.collect()
        self.assertEqual(result_df[0]["result"], False)

    def test_selected_ascwds_job_count_returns_false_when_null(self):

        rows = [("1-000000002", None, 2, 3, 1.5)]
        df = self.spark.createDataFrame(data=rows, schema=self.calculate_jobs_schema)
        result = df.withColumn(
            "result",
            F.when(
                (
                    selected_ascwds_job_count_is_at_least_the_min_permitted(
                        "total_staff"
                    )
                ),
                True,
            ).otherwise(False),
        )

        result_df = result.collect()
        self.assertEqual(result_df[0]["result"], False)

    def test_column_value_is_less_than_min_abs_difference_between_total_staff_and_worker_record_count_resolves_to_true(
        self,
    ):
        rows = [("1-000000001", 1, 2, None, 1.5)]
        df = self.spark.createDataFrame(data=rows, schema=self.calculate_jobs_schema)
        min_diff_val = 3

        result = df.withColumn(
            "result",
            F.when(
                (
                    column_value_is_less_than_min_abs_difference_between_total_staff_and_worker_record_count(
                        "job_count", min_diff_val
                    )
                ),
                True,
            ).otherwise(False),
        )

        result_df = result.collect()
        self.assertEqual(result_df[0]["result"], True)

    def test_column_value_is_less_than_min_abs_difference_between_total_staff_and_worker_record_count_resolves_to_false(
        self,
    ):
        rows = [("1-000000001", 1, 2, None, 3.0)]
        df = self.spark.createDataFrame(data=rows, schema=self.calculate_jobs_schema)
        min_diff_val = 3

        result = df.withColumn(
            "result",
            F.when(
                (
                    column_value_is_less_than_min_abs_difference_between_total_staff_and_worker_record_count(
                        "job_count", min_diff_val
                    )
                ),
                True,
            ).otherwise(False),
        )

        result_df = result.collect()
        self.assertEqual(result_df[0]["result"], False)
