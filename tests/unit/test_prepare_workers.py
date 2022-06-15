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

    def test_get_aggregated_training_column_adds_json_training_col(self):
        spark = utils.get_spark()
        df = spark.read.parquet(self.TEST_ASCWDS_WORKER_FILE)

        training_columns = utils.extract_col_with_pattern("^tr\d\d[a-z]", WORKER_SCHEMA)
        df = prepare_workers.add_aggregated_training_column(
            df, training_columns
        )
        training_types_flag = utils.extract_col_with_pattern("^tr\d\dflag$", WORKER_SCHEMA)
        training_types_flag.remove("tr01flag")

        self.assertEqual(df.columns[-1], "training")
        self.assertEqual(
            df.first()["training"],
            '{"tr01": {"latestdate": 0, "count": 1, "ac": 0, "nac": 0, "dn": 0}}',
        )
        for training in training_types_flag:
            self.assertEqual(df.first()[training], 0)

    def test_replace_training_columns(self):
        spark = utils.get_spark()
        df = spark.read.parquet(self.TEST_ASCWDS_WORKER_FILE)
        training_cols = utils.extract_col_with_pattern("^tr\d\d[a-z]", WORKER_SCHEMA)

        df = prepare_workers.replace_training_columns(df)

        self.assertEqual("training", df.columns[-1])
        self.assertNotIn(training_cols, df.columns)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
