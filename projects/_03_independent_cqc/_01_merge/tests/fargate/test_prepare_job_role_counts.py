import unittest
from unittest.mock import ANY, Mock, patch

import polars as pl
import polars.testing as pl_testing

import projects._03_independent_cqc._01_merge.fargate.prepare_job_role_counts as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    PrepareJobRoleCountsUtilsData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    PrepareJobRoleCountsUtilsSchemas as Schemas,
)

PATCH_PATH = "projects._03_independent_cqc._01_merge.fargate.prepare_job_role_counts"


class MainTests(unittest.TestCase):
    CLEANED_ASCWDS_WORKER_SOURCE = "some/source"
    PREPARED_JOB_ROLE_COUNTS_DESTINATION = "some/destination"

    mock_ascwds_worker_data = Mock(name="ascwds_worker_data")

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.JRUtils.aggregate_ascwds_worker_job_roles_per_establishment")
    @patch(f"{PATCH_PATH}.utils.scan_parquet", return_value=mock_ascwds_worker_data)
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        aggregate_ascwds_worker_job_roles_per_establishment_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.CLEANED_ASCWDS_WORKER_SOURCE,
            self.PREPARED_JOB_ROLE_COUNTS_DESTINATION,
        )

        scan_parquet_mock.assert_called_once_with(
            source=self.CLEANED_ASCWDS_WORKER_SOURCE,
            selected_columns=job.cleaned_ascwds_worker_columns_to_import,
        )

        aggregate_ascwds_worker_job_roles_per_establishment_mock.assert_called_once()

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=ANY,
            output_path=self.PREPARED_JOB_ROLE_COUNTS_DESTINATION,
            append=False,
        )


class IsCQCLocationTests(unittest.TestCase):
    def test_is_cqc_location_correctly_identifies_only_cqc_locations(
        self,
    ):
        test_lf = pl.LazyFrame(
            data=Data.filter_to_cqc_locations_rows,
            schema=Schemas.filter_to_cqc_locations_schema,
        )
        returned_lf = test_lf.filter(job.is_cqc_location)
        expected_lf = pl.LazyFrame(
            data=Data.expected_filter_to_cqc_locations_rows,
            schema=Schemas.filter_to_cqc_locations_schema,
        )
        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
        )
