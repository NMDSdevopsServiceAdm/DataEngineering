import unittest
from unittest.mock import patch, Mock, ANY, call

import jobs.create_job_estimates_diagnostics as job
from tests.test_file_schemas import (
    CreateJobEstimatesDiagnosticsSchemas as Schemas,
)
from tests.test_file_data import (
    CreateJobEstimatesDiagnosticsData as Data,
)
from utils import utils
from utils.column_values.categorical_column_values import CareHome
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
    IndCqcColumns as IndCQC,
)
from utils.diagnostics_utils.diagnostics_meta_data import (
    ResidualsRequired,
)


class DiagnosticsOnKnownValuesTests(unittest.TestCase):
    ESTIMATED_FILLED_POSTS_SOURCE = "some/directory"
    DIAGNOSTICS_DESTINATION = "some/directory"
    RESIDUALS_DESTINATION = "some/directory"
    partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

    def setUp(self):
        self.spark = utils.get_spark()
        self.estimate_jobs_df = self.spark.createDataFrame(
            Data.estimate_jobs_rows,
            schema=Schemas.estimate_jobs,
        )


class MainTests(DiagnosticsOnKnownValuesTests):
    def setUp(self) -> None:
        super().setUp()

    @patch("utils.utils.read_from_parquet")
    def test_main_runs(self, read_from_parquet_patch: Mock):
        read_from_parquet_patch.return_value = self.estimate_jobs_df

        job.main(
            self.ESTIMATED_FILLED_POSTS_SOURCE,
            self.DIAGNOSTICS_DESTINATION,
            self.RESIDUALS_DESTINATION,
        )

        self.assertEqual(read_from_parquet_patch.call_count, 1)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
