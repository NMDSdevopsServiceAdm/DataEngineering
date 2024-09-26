import unittest
from unittest.mock import patch, Mock

import jobs.diagnostics_on_known_filled_posts as job
from tests.test_file_schemas import (
    DiagnosticsOnKnownFilledPostsSchemas as Schemas,
)
from tests.test_file_data import (
    DiagnosticsOnKnownFilledPostsData as Data,
)
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)


class DiagnosticsOnKnownFilledPostsTests(unittest.TestCase):
    ESTIMATED_FILLED_POSTS_SOURCE = "some/directory"
    DIAGNOSTICS_DESTINATION = "some/other/directory"
    SUMMARY_DIAGNOSTICS_DESTINATION = "another/directory"
    CHARTS_DESTINATION = "yet/another/directory"
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

    @patch("utils.diagnostics_utils.create_charts_for_diagnostics.boto3.resource")
    @patch("utils.diagnostics_utils.create_charts_for_diagnostics.PdfPages")
    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main_runs(
        self,
        read_from_parquet_patch: Mock,
        write_to_parquet_patch: Mock,
        pdf_pages_mock: Mock,
        boto_resource_mock: Mock,
    ):
        read_from_parquet_patch.return_value = self.estimate_jobs_df

        job.main(
            self.ESTIMATED_FILLED_POSTS_SOURCE,
            self.DIAGNOSTICS_DESTINATION,
            self.SUMMARY_DIAGNOSTICS_DESTINATION,
            self.CHARTS_DESTINATION,
        )

        self.assertEqual(read_from_parquet_patch.call_count, 1)
        pdf_pages_mock.assert_called_once()
        boto_resource_mock.assert_called_once()
        self.assertEqual(write_to_parquet_patch.call_count, 2)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
