import unittest

from utils import utils

import utils.cleaning_utils as job

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


    def test_apply_categorical_labels_completes(self):
        returned_df = job.apply_categorical_labels(self.test_worker_df, self.label_dicts, new_column=True)

        self.assertIsNotNone(returned_df)