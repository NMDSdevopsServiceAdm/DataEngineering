from multiprocessing import dummy
import shutil
import unittest

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

from jobs import prepare_workers
from schemas.worker_schema import WORKER_SCHEMA
from tests.test_file_generator import generate_ascwds_worker_file,  generate_training_file
from utils import utils


class PrepareWorkersTests(unittest.TestCase):

    TEST_ASCWDS_WORKER_FILE = "tests/test_data/domain=ascwds/dataset=worker"
    TEST_TRAINING_FILE = "tests/test_data/domain=ascwds/dataset=training"

    def setUp(self):
        self.spark = SparkSession.builder.appName("test_prepare_workers").getOrCreate()
        generate_ascwds_worker_file(self.TEST_ASCWDS_WORKER_FILE)
        generate_training_file(self.TEST_TRAINING_FILE)

    def tearDown(self):
        try:
            shutil.rmtree(self.TEST_ASCWDS_WORKER_FILE)
            shutil.rmtree(self.TEST_TRAINING_FILE)
        except OSError():
            pass  # Ignore dir does not exist

    def test_get_dataset_worker_has_correct_columns(self):
        worker_df = prepare_workers.get_dataset_worker(self.TEST_ASCWDS_WORKER_FILE)
        column_names = utils.extract_column_from_schema(WORKER_SCHEMA)
        self.assertEqual(worker_df.columns, column_names)

    def test_get_dataset_worker_has_correct_rows_number(self):
        worker_df = prepare_workers.get_dataset_worker(self.TEST_ASCWDS_WORKER_FILE)
        self.assertEqual(worker_df.count(), 100)

    def test_get_training_aggregated_columns_returns_one_column(self):
        spark = utils.get_spark()
        tr_df = spark.read.parquet(self.TEST_TRAINING_FILE)

        aggregated_col = prepare_workers.get_training_aggregated_column(tr_df)

        self.assertEqual(aggregated_col.columns, ['training'])
        self.assertEqual(aggregated_col.first()['training'], '{"tr01": {"latestdate": 0, "count": 1, "ac": 0, "nac": 0, "dn": 0}}')

    def test_aggregate_training_columns_return_correct_value_format(self):
        # TODO - check the values have the right format, nested dict
        pass

    def test_replace_training_columns(self):
        spark = utils.get_spark()
        df = spark.read.parquet(self.TEST_ASCWDS_WORKER_FILE)
        # df = df.withColumn("dummy", lit(3))
        # dummy_col = df.select("dummy")
        training_cols = utils.extract_col_with_pattern("^tr\d\d[a-z]", WORKER_SCHEMA)
        df = prepare_workers.replace_training_columns(df)
        # self.assertIn("training", df.columns)
        self.assertNotIn(training_cols, df.columns)

if __name__ == "__main__":
    unittest.main(warnings="ignore")
