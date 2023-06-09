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

import utils.prepare_direct_payments_utils.prepare_during_year_data as job
from utils.prepare_direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


class TestDetermineAreasIncludingCarers(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName(
            "test_areas_including_carers"
        ).getOrCreate()

        warnings.simplefilter("ignore", ResourceWarning)

    def test_determine_areas_including_carers_on_adass_completes(self):
        rows = [
            ("area_1", 2021, 300.0, 50.0, 200.0, 5.0, None, 100.0, 50.0),
            ("area_2", 2021, 300.0, 50.0, 100.0, 10.0, None, 100.0, 25.0),
            ("area_3", 2020, 300.0, 50.0, 200.0, 5.0, None, 100.0, 50.0),
            ("area_4", 2020, 300.0, 50.0, 200.0, 5.0, None, 100.0, 50.0),
            ("area_5", 2019, 300.0, 50.0, 200.0, 5.0, None, 100.0, 50.0),
            ("area_6", 2019, 300.0, 50.0, 200.0, 5.0, None, 100.0, 50.0),
        ]
        test_schema = StructType(
            [
                StructField(DP.LA_AREA, StringType(), False),
                StructField(DP.YEAR, IntegerType(), True),
                StructField(DP.SERVICE_USER_DPRS_DURING_YEAR, FloatType(), True),
                StructField(DP.CARER_DPRS_DURING_YEAR, FloatType(), True),
                StructField(DP.SERVICE_USER_DPRS_AT_YEAR_END, FloatType(), True),
                StructField(DP.CARER_DPRS_AT_YEAR_END, FloatType(), True),
                StructField(DP.IMD_SCORE, FloatType(), True),
                StructField(DP.DPRS_ADASS, FloatType(), True),
                StructField(DP.DPRS_EMPLOYING_STAFF_ADASS, FloatType(), True),
            ]
        )
        df = self.spark.createDataFrame(rows, schema=test_schema)
        output_df = job.prepare_during_year_data(df)
        self.assertEqual(df.count(), output_df.count())
