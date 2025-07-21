import unittest
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)
from utils.column_values.categorical_column_values import Specialisms
import projects._01_ingest.cqc_api.utils.utils as job
from projects._01_ingest.unittest_data.ingest_test_file_data import (
    CQCLocationsData as Data,
)
from projects._01_ingest.unittest_data.ingest_test_file_schemas import (
    CQCLocationsSchema as Schemas,
)


class TestClassifySpecialisms(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

    def test_classify_specialisms(self):
        test_df = self.spark.createDataFrame(
            Data.classify_specialisms_rows, Schemas.classify_specialisms_schema
        )
        returned_df = job.classify_specialisms(
            test_df,
            IndCQC.specialist_generalist_other_dementia,
            Specialisms.dementia,
        )
        returned_df = job.classify_specialisms(
            returned_df,
            IndCQC.specialist_generalist_other_lda,
            Specialisms.learning_disabilities,
        )
        returned_df = job.classify_specialisms(
            returned_df,
            IndCQC.specialist_generalist_other_mh,
            Specialisms.mental_health,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_classify_specialisms_rows,
            Schemas.expected_classify_specialisms_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())
