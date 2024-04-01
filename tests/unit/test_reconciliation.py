import unittest
import warnings
from unittest.mock import ANY, Mock, patch

from pyspark.sql import functions as F
from datetime import date

import jobs.reconciliation as job
from utils import utils

from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)

from tests.test_file_data import ReconciliationData as Data
from tests.test_file_schemas import ReconciliationSchema as Schemas


class ReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

        warnings.simplefilter("ignore", ResourceWarning)

    def test_collect_dates_to_use(self):
        input_df = self.spark.createDataFrame(
            Data.dates_to_use_rows, Schemas.dates_to_use_schema
        )

        first_of_most_recent_month, first_of_previous_month = job.collect_dates_to_use(
            input_df
        )

        self.assertEqual(first_of_most_recent_month, date(2024, 3, 1))
        self.assertEqual(first_of_previous_month, date(2024, 2, 1))
