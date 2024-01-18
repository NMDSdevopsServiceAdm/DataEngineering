import unittest
import warnings

from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, FloatType, StructField, StructType, StringType, IntegerType

from schemas import cqc_pir_csv_schema as job

class TestNewSchema(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName(
            "test"
        ).getOrCreate()

        warnings.simplefilter("ignore", ResourceWarning)
    def tearDown(self):
        pass

    def test_schema(self):
        old_schema = job.PIR_CSV_OLD

        new_schema = job.PIR_CSV
        old_df = self.spark.createDataFrame([], old_schema)
        new_df = self.spark.createDataFrame([], new_schema)
        old_columns = old_df.columns
        new_columns = new_df.columns
        number_of_cols = len(old_columns)
        for col in range(number_of_cols):
            self.assertEqual(old_columns[col], new_columns[col])
        self.assertEqual(old_schema, new_schema)
