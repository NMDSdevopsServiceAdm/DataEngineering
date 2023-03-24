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
from dataclasses import dataclass

from utils.estimate_job_count.calculate_rolling_average import (
    calculate_rolling_averages,
)


@dataclass
class TestRollingAverage:
    MODEL_NAME = "model_name"
    ROLLING_AVERAGE_TIME_PERIOD = "3 days"
    ROLLING_AVERAGE_WINDOW_SLIDE = "1 days"
    DAYS: int = 1


class TestCalcRollingAvg(unittest.TestCase):
    column_schema = StructType(
        [
            StructField("snapshot_date", StringType(), False),
            StructField("job_count", IntegerType(), True),
        ]
    )
    # fmt: off
    rows = [
        ("2023-01-04", 15),
        ("2023-01-03", 5),
        ("2023-01-02", 10),
        ("2023-01-01", 15),
        ("2023-01-25", None),
    ]
    # fmt: on

    def setUp(self):
        self.spark = SparkSession.builder.appName("test_calculate_rolling_average").getOrCreate()
        df = self.spark.createDataFrame(self.rows, schema=self.column_schema)

        self.rolling_avg_df = calculate_rolling_averages(
            df,
            TestRollingAverage.MODEL_NAME,
            TestRollingAverage.ROLLING_AVERAGE_TIME_PERIOD,
            TestRollingAverage.ROLLING_AVERAGE_WINDOW_SLIDE,
            TestRollingAverage.DAYS,
        )

        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)

    def test_calc_rolling_average_returns_average(self):

        df = self.rolling_avg_df.orderBy("snapshot_date").collect()
        self.assertEqual(df[0]["model_name"], 15)
        self.assertEqual(df[1]["model_name"], 12.5)
        self.assertEqual(df[2]["model_name"], 10)
        self.assertEqual(df[3]["model_name"], 10)
        self.assertEqual(df[4]["model_name"], 10)
