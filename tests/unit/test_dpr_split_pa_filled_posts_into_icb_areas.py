import unittest
import warnings

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
)

from utils import utils
from tests.test_file_data import CreateListFromRowsOfICBs as TestData
from tests.test_file_schemas import CreateListFromRowsOfICBs as TestSchema


class SplitPAFilledPostsIntoICBAreas(unittest.TestCase):
    TEST_SOURCE = "some/other/directory"
    TEST_DESTINATION = "some/other/directory"

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_sample_rows = self.spark.createDataFrame(
            TestData.sample_rows, schema=TestSchema.sample_schema
        )
        self.test_location_df = self.spark.createDataFrame(
            TestData.expected_rows, TestSchema.expected_schema
        )
