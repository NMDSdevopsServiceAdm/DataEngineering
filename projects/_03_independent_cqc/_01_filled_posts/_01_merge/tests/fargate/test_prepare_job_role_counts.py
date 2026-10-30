import unittest
from dataclasses import dataclass
from unittest.mock import ANY, Mock, patch

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._01_filled_posts._01_merge.fargate.prepare_job_role_counts as job
from polars_utils.column_types import CategoricalColumnTypes
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)

PATCH_PATH = "projects._03_independent_cqc._01_filled_posts._01_merge.fargate.prepare_job_role_counts"


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
        )


@dataclass
class IsCqcLocationTestCase:
    id: str
    location_ids: list[str | None]
    expected_location_ids: list[str]

    def as_pytest_param(self):
        return pytest.param(self.location_ids, self.expected_location_ids, id=self.id)


is_cqc_location_cases = [
    IsCqcLocationTestCase(
        id="keeps_non_empty_location_id",
        location_ids=["1-001"],
        expected_location_ids=["1-001"],
    ),
    IsCqcLocationTestCase(
        id="drops_empty_location_id",
        location_ids=[""],
        expected_location_ids=[],
    ),
    IsCqcLocationTestCase(
        id="drops_null_location_id",
        location_ids=[None],
        expected_location_ids=[],
    ),
]


class TestIsCqcLocation:
    @pytest.mark.parametrize(
        "location_ids, expected_location_ids",
        [case.as_pytest_param() for case in is_cqc_location_cases],
    )
    def test_filters_to_workers_with_a_categorical_location_id(
        self, location_ids, expected_location_ids
    ):
        schema = {AWKClean.location_id: CategoricalColumnTypes.LocationCatType}
        input_lf = pl.LazyFrame({AWKClean.location_id: location_ids}, schema=schema)

        returned_lf = input_lf.filter(job.is_cqc_location)

        expected_lf = pl.LazyFrame(
            {AWKClean.location_id: expected_location_ids}, schema=schema
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf)
