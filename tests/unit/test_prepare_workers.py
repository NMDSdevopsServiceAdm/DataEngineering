import shutil
import unittest

from pyspark.sql import SparkSession

from jobs import prepare_workers
from schemas.worker_schema import WORKER_SCHEMA
from tests.test_file_generator import generate_ascwds_worker_file
from utils import utils


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

    def test_get_dataset_worker_has_correct_columns(self):
        worker_df = prepare_workers.get_dataset_worker(self.TEST_ASCWDS_WORKER_FILE)
        column_names = utils.extract_column_from_schema(WORKER_SCHEMA)
        self.assertEqual(worker_df.columns, column_names)

    def test_get_dataset_worker_has_correct_rows_number(self):
        worker_df = prepare_workers.get_dataset_worker(self.TEST_ASCWDS_WORKER_FILE)
        self.assertEqual(worker_df.count(), 100)

    def test_aggregate_training_columns_returns_one_column(self):
        spark = utils.get_spark()
        column = [
            "tr01flag", "tr01latestdate", "tr01count", "tr01ac", "tr01nac", "tr01dn",
            "tr02flag", "tr02latestdate", "tr02count", "tr02ac", "tr02nac", "tr02dn",
        ]
        row = [
            (1, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0),
            (0, 0, 1, 0, 0, 0,0, 0, 1, 0, 0, 0),
        ]
        tr_df = spark.createDataFrame(row, column)
        aggregated_col = prepare_workers.aggregate_training_columns(tr_df)
        self.assertEqual(aggregated_col.columns, ['training'])
        self.assertEqual(aggregated_col.first()['training'], '{"tr01flag":1,"tr01latestdate":0,"tr01count":1,"tr01ac":0,"tr01nac":0,"tr01dn":0}')

    def test_aggregate_training_columns_return_correct_value_format(self):
        # TODO - check the values have the right format, nested dict
        pass


if __name__ == "__main__":
    unittest.main(warnings="ignore")
