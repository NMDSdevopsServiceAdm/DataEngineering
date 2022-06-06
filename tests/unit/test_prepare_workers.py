import shutil
import unittest

from pyspark.sql import SparkSession

from jobs import prepare_workers
from tests.test_file_generator import generate_ascwds_worker_file

class PrepareWorkersTests(unittest.TestCase):

    TEST_ASCWDS_WORKER_FILE = "tests/test_data/domain=ascwds/dataset=worker"

    def setUp(self):
        self.spark = SparkSession.builder.appName("test_prepare_workers").getOrCreate()
        generate_ascwds_worker_file(self.TEST_ASCWDS_WORKER_FILE)

    def tearDown(self):
        try:
            shutil.rmtree(self.TEST_ASCWDS_WORKER_FILE)
        except OSError():
            pass  # Ignore dir does not exist

    def test_get_dataset_worker(self):
        worker_df = prepare_workers.get_dataset_worker(self.TEST_ASCWDS_WORKER_FILE)
        self.assertEqual(worker_df.columns[0], "period")
        self.assertEqual(worker_df.columns[1], "establishmentid")
        self.assertEqual(worker_df.count(), 1000)

if __name__ == "__main__":
    unittest.main(warnings="ignore")
