import unittest
import warnings

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
import utils.ind_cqc_filled_posts_utils.ascwds_filled_posts_calculator.utils as job


class TestAscwdsFilledPostsCalculatorCommonChecks(unittest.TestCase):
    common_checks_rows = [("1-000000001", 9, 2, None), ("1-000000002", 2, 2, 2.0)]
    common_checks_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.total_staff_bounded, IntegerType(), True),
            StructField(IndCQC.worker_records_bounded, IntegerType(), True),
            StructField(IndCQC.ascwds_filled_posts, DoubleType(), True),
        ]
    )

    def setUp(self):
        self.spark = utils.get_spark()
        self.df = self.spark.createDataFrame(
            data=self.common_checks_rows, schema=self.common_checks_schema
        )

        warnings.filterwarnings("ignore", category=ResourceWarning)

    def test_ascwds_filled_posts_is_null(self):
        result = self.df.withColumn(
            "result",
            F.when((job.ascwds_filled_posts_is_null()), True).otherwise(False),
        )

        result_df = result.sort(IndCQC.location_id).collect()
        self.assertEqual(result_df[0]["result"], True)
        self.assertEqual(result_df[1]["result"], False)

    def test_selected_column_is_not_null(self):
        result = self.df.withColumn(
            "result",
            F.when(
                (job.selected_column_is_not_null(IndCQC.ascwds_filled_posts)), True
            ).otherwise(False),
        )

        result_df = result.sort(IndCQC.location_id).collect()
        self.assertEqual(result_df[0]["result"], False)
        self.assertEqual(result_df[1]["result"], True)

    def test_selected_column_is_at_least_the_min_permitted_value(self):
        result = self.df.withColumn(
            "result",
            F.when(
                (
                    job.selected_column_is_at_least_the_min_permitted_value(
                        IndCQC.total_staff_bounded
                    )
                ),
                True,
            ).otherwise(False),
        )

        result_df = result.sort(IndCQC.location_id).collect()
        self.assertEqual(result_df[0]["result"], True)
        self.assertEqual(result_df[1]["result"], False)

    def test_absolute_difference_between_total_staff_and_worker_records_below_cut_off(
        self,
    ):
        result = self.df.withColumn(
            "result",
            F.when(
                (
                    job.absolute_difference_between_total_staff_and_worker_records_below_cut_off()
                ),
                True,
            ).otherwise(False),
        )

        result_df = result.sort(IndCQC.location_id).collect()
        self.assertEqual(result_df[0]["result"], False)
        self.assertEqual(result_df[1]["result"], True)

    def test_percentage_difference_between_total_staff_and_worker_records_below_cut_off(
        self,
    ):
        result = self.df.withColumn(
            "result",
            F.when(
                (
                    job.percentage_difference_between_total_staff_and_worker_records_below_cut_off()
                ),
                True,
            ).otherwise(False),
        )

        result_df = result.sort(IndCQC.location_id).collect()
        self.assertEqual(result_df[0]["result"], False)
        self.assertEqual(result_df[1]["result"], True)

    def test_two_cols_are_equal_and_at_least_minimum_permitted_value(self):
        rows = [
            ("1-000000001", 9, 2, None),
            ("1-000000002", 2, 2, 2.0),
            ("1-000000003", 8, 8, 8.0),
        ]
        df = self.spark.createDataFrame(data=rows, schema=self.common_checks_schema)

        result = df.withColumn(
            "result",
            F.when(
                (
                    job.two_cols_are_equal_and_at_least_minimum_permitted_value(
                        IndCQC.total_staff_bounded, IndCQC.worker_records_bounded
                    )
                ),
                True,
            ).otherwise(False),
        )

        result_df = result.sort(IndCQC.location_id).collect()
        self.assertEqual(result_df[0]["result"], False)
        self.assertEqual(result_df[1]["result"], False)
        self.assertEqual(result_df[2]["result"], True)

    def test_absolute_difference_between_two_columns(self):
        result = self.df.withColumn(
            "result",
            job.absolute_difference_between_two_columns(
                IndCQC.total_staff_bounded, IndCQC.worker_records_bounded
            ),
        )

        result_df = result.sort(IndCQC.location_id).collect()
        self.assertEqual(result_df[0]["result"], 7)
        self.assertEqual(result_df[1]["result"], 0)

    def test_average_of_two_columns(self):
        result = self.df.withColumn(
            "result",
            job.average_of_two_columns(
                IndCQC.total_staff_bounded, IndCQC.worker_records_bounded
            ),
        )

        result_df = result.sort(IndCQC.location_id).collect()
        self.assertEqual(result_df[0]["result"], 5.5)
        self.assertEqual(result_df[1]["result"], 2.0)
