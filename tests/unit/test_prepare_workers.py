import unittest
from pyspark.sql import SparkSession
from jobs import prepare_workers


class PrepareWorkersTests(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName(
            "sfc_data_engineering_test_prepare_workers"
        ).getOrCreate()

    def test_main(self):
        result = prepare_workers.main("source", "destination")
        self.assertTrue(result)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
