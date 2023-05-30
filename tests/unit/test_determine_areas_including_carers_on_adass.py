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

from utils.prepare_direct_payments_utils.determine_areas_including_carers_on_adass import (
    determine_areas_including_carers_on_adass,
)
from utils.prepare_direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


class TestDetermineAreasIncludingCarers(unittest.TestCase):
    determine_areas_including_carers_schema = StructType(
        [
            StructField(DP.LA_AREA, StringType(), False),
            StructField(DP.YEAR, IntegerType(), True),
            StructField(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, FloatType(), True),
            StructField(DP.SERVICE_USER_DPRS_DURING_YEAR, FloatType(), True),
            StructField(DP.CARER_DPRS_DURING_YEAR, FloatType(), True),
        ]
    )

    def setUp(self):
        self.spark = SparkSession.builder.appName("test_areas_including_carers").getOrCreate()

        warnings.simplefilter("ignore", ResourceWarning)

    @unittest.skip("template")
    def test_example(self):
        rows = []
        df = self.spark.createDataFrame(rows, schema=self.determine_areas_including_carers_schema)
