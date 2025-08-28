import unittest


import projects._01_ingest.cqc_api.utils.utils as job
from projects._01_ingest.unittest_data.ingest_test_file_data import (
    CQCLocationsData as Data,
)
from projects._01_ingest.unittest_data.ingest_test_file_schemas import (
    CQCLocationsSchema as Schemas,
)

from utils import utils
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_values.categorical_column_values import Specialisms


class TestClassifySpecialisms(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()
        self.test_df = self.spark.createDataFrame(
            Data.classify_specialisms_rows, Schemas.classify_specialisms_schema
        )
        self.returned_df = job.classify_specialisms(
            self.test_df,
            Specialisms.dementia,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_classify_specialisms_rows,
            Schemas.expected_classify_specialisms_schema,
        )

    def test_classify_specialisms_returns_expected_value(
        self,
    ):
        self.assertEqual(self.returned_df.collect(), self.expected_df.collect())

    def test_classify_specialisms_adds_expected_columns(
        self,
    ):
        returned_columns = self.returned_df.columns.sort()
        expected_columns = self.expected_df.columns.sort()
        self.assertEqual(returned_columns, expected_columns)
