import unittest
from pyspark.sql import SparkSession
from jobs import prepare_workers
from jobs.prepare_locations import get_ascwds_workplace_df


class PrepareWorkersTests(unittest.TestCase):

    TEST_ASCWDS_WORKER_FILE = "tests/test_data/domain=ascwds"

    def setUp(self):
        self.spark = SparkSession.builder.appName("test_prepare_workers").getOrCreate()
        # generate_ascwds_worker(self.TEST_ASCWDS_WORKER_FILE)

    def test_main(self):
        result = prepare_workers.main("source", "destination")
        self.assertTrue(result)

    def test_get_dataset_worker(self):
        worker_df = prepare_workers.get_dataset_worker(self.TEST_ASCWDS_WORKER_FILE)
        self.assertEqual(worker_df.columns[0], "col_a")
        self.assertEqual(worker_df.columns[1], "col_b")
        self.assertEqual(worker_df.columns[2], "col_c")
        self.assertEqual(worker_df.columns[3], "date_col")
        self.assertEqual(worker_df.count(), 3)

if __name__ == "__main__":
    unittest.main(warnings="ignore")
