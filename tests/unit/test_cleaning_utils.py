import unittest
import warnings

from pyspark.sql import SparkSession

from utils import utils

import utils.cleaning_utils as job
from utils.column_names.raw_data_files.ascwds_worker_columns import (
    AscwdsWorkerColumns as AWK,
)
from tests.test_file_schemas import CleaningUtilsSchemas as Schemas
from tests.test_file_data import CleaningUtilsData as Data


class TestCleaningUtils(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()
        self.test_worker_df = self.spark.createDataFrame(
            Data.worker_rows, schema=Schemas.worker_schema
        )
        self.label_dicts = {"gender_labels": Data.gender_labels, 
                            "nationality_labels": Data.nationality_labels}


    def test_apply_categorical_labels_completes_with_data_frame_of_correct_size(self):
        returned_df = job.apply_categorical_labels(self.test_worker_df, self.label_dicts, new_column=True)
        
        expected_rows = self.test_worker_df.count()
        expected_columns = len(self.test_worker_df.columns) + len(self.label_dicts)

        self.assertEqual(returned_df.count(), expected_rows)
        self.assertEqual(len(returned_df.columns), expected_columns)