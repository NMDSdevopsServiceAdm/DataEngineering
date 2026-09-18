import warnings

import projects._02_sfc_internal.cqc_coverage.utils.reconciliation_utils as job
from projects._02_sfc_internal.unittest_data.sfc_test_file_data import (
    ReconciliationUtilsData as Data,
)
from projects._02_sfc_internal.unittest_data.sfc_test_file_schemas import (
    ReconciliationUtilsSchema as Schemas,
)
from tests.base_test import SparkBaseTest
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)


class AddParentsOrSinglesAndSubsColToDf(SparkBaseTest):
    def setUp(self) -> None:
        warnings.simplefilter("ignore", ResourceWarning)

        self.input_df = self.spark.createDataFrame(
            Data.parents_or_singles_and_subs_rows,
            Schemas.parents_or_singles_and_subs_schema,
        )

        self.returned_df = job.add_parents_or_singles_and_subs_col_to_df(self.input_df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_parents_or_singles_and_subs_rows,
            Schemas.expected_parents_or_singles_and_subs_schema,
        )

    def test_parents_or_singles_and_subs_column_adds_a_column(self):
        returned_column_count = len(self.returned_df.columns)
        expected_column_count = len(self.input_df.columns) + 1
        self.assertEqual(returned_column_count, expected_column_count)

    def test_parents_or_singles_and_subs_column_adds_correct_labels(self):
        returned_data = self.returned_df.sort(AWPClean.establishment_id).collect()
        expected_data = self.expected_df.sort(AWPClean.establishment_id).collect()

        self.assertEqual(returned_data, expected_data)
