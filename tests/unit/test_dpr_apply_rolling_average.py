import unittest
import warnings

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
)

import utils.direct_payments_utils.estimate_direct_payments.apply_rolling_average as job
from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
    DirectPaymentColumnValues as Values,
)
from utils.direct_payments_utils.direct_payments_configuration import (
    DirectPaymentConfiguration as Config,
)


class TestApplyRollingAverage(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test_apply_rolling_average").getOrCreate()

        warnings.simplefilter("ignore", ResourceWarning)

    def test_apply_rolling_average_completes(self):
        rows = [
            ("area_1", 2021, 0.25),
            ("area_1", 2020, 0.45),
            ("area_1", 2019, 0.46),
            ("area_1", 2018, 0.51),
            ("area_1", 2017, 0.37),
            ("area_1", 2016, 0.42),
            ("area_2", 2021, 0.62),
            ("area_2", 2020, 0.80),
            ("area_2", 2019, 0.54),
            ("area_2", 2018, 0.74),
            ("area_2", 2017, 0.65),
            ("area_2", 2016, 0.62),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
                StructField(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.apply_rolling_average(df)
        self.assertEqual(df.count(), output_df.count())
