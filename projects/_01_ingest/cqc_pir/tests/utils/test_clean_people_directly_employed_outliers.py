import unittest

import projects._01_ingest.cqc_pir.utils.clean_people_directly_employed_outliers as job
from projects._01_ingest.unittest_data.ingest_test_file_data import (
    CleanPeopleDirectlyEmployedData as Data,
)
from projects._01_ingest.unittest_data.ingest_test_file_schemas import (
    CleanPeopleDirectlyEmployedSchema as Schemas,
)
from utils import utils
from utils.column_names.cleaned_data_files.cqc_pir_cleaned import (
    CqcPIRCleanedColumns as PIRCleanCols,
)

PATCH_PATH: str = (
    "projects._01_ingest.cqc_pir.utils.clean_people_directly_employed_outliers"
)


class CleanPeopleDirectlyEmployedTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = utils.get_spark()


class MainTests(CleanPeopleDirectlyEmployedTests):
    def setUp(self) -> None:
        super().setUp()

        self.test_df = self.spark.createDataFrame(
            Data.clean_people_directly_employed_outliers_rows,
            Schemas.clean_people_directly_employed_outliers_schema,
        )
        self.returned_df = job.clean_people_directly_employed_outliers(self.test_df)

    def test_main_adds_cleaned_column(self):
        self.assertIn(
            PIRCleanCols.pir_people_directly_employed_cleaned, self.returned_df.columns
        )

    def test_main_returns_original_number_of_rows(self):
        self.assertEqual(self.returned_df.count(), self.test_df.count())
