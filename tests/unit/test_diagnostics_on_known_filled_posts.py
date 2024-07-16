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
            Data.estimate_jobs_rows,
            Schemas.estimate_jobs,
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


class FilterToKnownDataPointsTests(DiagnosticsOnKnownFilledPostsTests):
    def setUp(self) -> None:
        super().setUp()


if __name__ == "__main__":
    unittest.main(warnings="ignore")
