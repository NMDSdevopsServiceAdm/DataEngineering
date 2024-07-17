import unittest
from unittest.mock import patch, Mock, ANY, call

import jobs.diagnostics_on_known_filled_posts as job
from tests.test_file_schemas import (
    DiagnosticsOnKnownFilledPostsSchemas as Schemas,
)
from tests.test_file_data import (
    DiagnosticsOnKnownFilledPostsData as Data,
)
from utils import utils
from utils.column_values.categorical_column_values import CareHome
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
    IndCqcColumns as IndCQC,
)


class DiagnosticsOnKnownFilledPostsTests(unittest.TestCase):
    ESTIMATED_FILLED_POSTS_SOURCE = "some/directory"
    DIAGNOSTICS_DESTINATION = "some/other/directory"
    SUMMARY_DIAGNOSTICS_DESTINATION = "another/directory"
    partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

    def setUp(self):
        self.spark = utils.get_spark()
        self.estimate_jobs_df = self.spark.createDataFrame(
            Data.estimate_filled_posts_rows,
            Schemas.estimate_filled_posts_schema,
        )


class MainTests(DiagnosticsOnKnownFilledPostsTests):
    def setUp(self) -> None:
        super().setUp()

    @patch("utils.utils.read_from_parquet")
    def test_main_runs(self, read_from_parquet_patch: Mock):
        read_from_parquet_patch.return_value = self.estimate_jobs_df

        job.main(
            self.ESTIMATED_FILLED_POSTS_SOURCE,
            self.DIAGNOSTICS_DESTINATION,
            self.SUMMARY_DIAGNOSTICS_DESTINATION,
        )

        self.assertEqual(read_from_parquet_patch.call_count, 1)


class FilterToKnownValuesTests(DiagnosticsOnKnownFilledPostsTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_column = IndCQC.ascwds_filled_posts_clean
        self.test_df = self.spark.createDataFrame(
            Data.filter_to_known_values_rows, Schemas.filter_to_known_values_schema
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_filter_to_known_values_rows,
            Schemas.filter_to_known_values_schema,
        )
        self.returned_df = job.filter_to_known_values(self.test_df, self.test_column)

    def test_filter_to_known_values_removes_null_values_from_specified_column(self):
        self.assertEqual(self.returned_df.collect(), self.expected_df.collect())

    def test_filter_to_known_values_does_not_remove_any_columns(self):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)


class RestructureDataframeToColumnWiseTests(DiagnosticsOnKnownFilledPostsTests):
    def setUp(self) -> None:
        super().setUp()


class CreateWindowForModelAndServiceSplitsTests(DiagnosticsOnKnownFilledPostsTests):
    def setUp(self) -> None:
        super().setUp()


class CalculateDistributionMetricsTests(DiagnosticsOnKnownFilledPostsTests):
    def setUp(self) -> None:
        super().setUp()


class CalculateResidualsTests(DiagnosticsOnKnownFilledPostsTests):
    def setUp(self) -> None:
        super().setUp()


class CalculateAggregateResidualsTests(DiagnosticsOnKnownFilledPostsTests):
    def setUp(self) -> None:
        super().setUp()


if __name__ == "__main__":
    unittest.main(warnings="ignore")
