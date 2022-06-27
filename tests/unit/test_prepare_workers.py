import shutil
from tkinter import W
import unittest
import warnings
import json

from pyspark.sql import SparkSession

from jobs import prepare_workers
from schemas.worker_schema import WORKER_SCHEMA
from tests.test_file_generator import generate_ascwds_worker_file
from utils import utils


class PrepareWorkersTests(unittest.TestCase):

    TEST_ASCWDS_WORKER_FILE = "tests/test_data/domain=ascwds/dataset=worker"

    def setUp(self):
        self.spark = SparkSession.builder.appName("test_prepare_workers").getOrCreate()
        self.test_df = generate_ascwds_worker_file(self.TEST_ASCWDS_WORKER_FILE)
        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)

    def tearDown(self):
        try:
            shutil.rmtree(self.TEST_ASCWDS_WORKER_FILE)
        except OSError():
            pass  # Ignore dir does not exist

    def test_main_adds_aggregated_columns(self):
        df = prepare_workers.main(self.TEST_ASCWDS_WORKER_FILE)

        aggregated_cols = ["training", "job_role", "qualifications"]
        training_json = json.loads(df.first()["training"])
        extracted_date = training_json["tr01"]["latestdate"]

        for col in aggregated_cols:
            self.assertIn(col, df.columns)
        self.assertNotIn("tr01latestdate", df.columns)
        self.assertNotIn("jr01flag", df.columns)
        self.assertNotIn("ql15year3", df.columns)
        self.assertEqual(len(df.columns), 77)
        self.assertEqual(extracted_date, "2017-06-15")

    def test_get_dataset_worker_has_correct_columns(self):
        worker_df = prepare_workers.get_dataset_worker(self.TEST_ASCWDS_WORKER_FILE)
        column_names = utils.extract_column_from_schema(WORKER_SCHEMA)

        self.assertEqual(worker_df.columns, column_names)

    def test_get_dataset_worker_has_correct_rows_number(self):
        worker_df = prepare_workers.get_dataset_worker(self.TEST_ASCWDS_WORKER_FILE)

        self.assertEqual(worker_df.count(), 50)

    def test_replace_columns_after_aggregation(self):
        training_cols = utils.extract_col_with_pattern("^tr\d\d[a-z]", WORKER_SCHEMA)
        # TODO - make it run with all the other patterns and columns
        pattern = "^tr\d\d[a-z]"
        df = prepare_workers.replace_columns_with_aggregated_column(
            self.test_df, "training", pattern, prepare_workers.get_training_into_json
        )

        self.assertEqual("training", df.columns[-1])
        for training_col in training_cols:
            self.assertNotIn(training_col, df.columns)

    def test_get_training_into_json(self):
        training_columns = utils.extract_col_with_pattern("^tr\d\d[a-z]", WORKER_SCHEMA)
        df = prepare_workers.add_aggregated_column(
            self.test_df,
            "training",
            training_columns,
            prepare_workers.get_training_into_json,
        )
        training_types_flag = utils.extract_col_with_pattern(
            "^tr\d\dflag$", WORKER_SCHEMA
        )
        training_types_flag.remove("tr01flag")

        self.assertEqual(df.columns[-1], "training")
        self.assertEqual(
            df.first()["training"],
            '{"tr01": {"latestdate": "2017-06-15", "count": 1, "ac": 0, "nac": 0, "dn": 0}}',
        )
        for training in training_types_flag:
            self.assertEqual(df.first()[training], 0)

    def test_get_job_role_into_json(self):
        jr_columns = utils.extract_col_with_pattern("^jr\d\d[a-z]", WORKER_SCHEMA)
        df = prepare_workers.add_aggregated_column(
            self.test_df, "job_role", jr_columns, prepare_workers.get_job_role_into_json
        )
        jr_types_flag = utils.extract_col_with_pattern("^jr\d\d*[a-z]\d", WORKER_SCHEMA)

        self.assertEqual(df.columns[-1], "job_role")
        self.assertEqual(
            df.first()["job_role"],
            '["jr01flag", "jr03flag", "jr16cat1"]',
        )
        for jr in jr_types_flag:
            self.assertEqual(df.first()[jr], 0)

    def test_get_qualification_into_json(self):
        qualification_columns = utils.extract_col_with_pattern(
            "^ql\d{1,3}.", WORKER_SCHEMA
        )
        df = prepare_workers.add_aggregated_column(
            self.test_df,
            "qualification",
            qualification_columns,
            prepare_workers.get_qualification_into_json,
        )

        self.assertEqual(df.columns[-1], "qualification")
        self.assertEqual(
            df.first()["qualification"],
            '{"ql01achq2": {"value": 1, "year": 2009}, "ql34achqe": {"value": 1, "year": 2010}, "ql37achq": {"value": 3, "year": 2021}, "ql313app": {"value": 1, "year": 2013}}',
        )
        self.assertEqual(df.first()["ql02achq3"], 0)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
