import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._02_employment_status.fargate.utils.prepare_worker_utils as job
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from projects._03_independent_cqc._02_employment_status.unittest_data.polars_employment_status_test_data import (
    TestPrepareWorkerUtilsData as Data,
)
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class TestCollapseJobRolesToPublishedLabels:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.collapse_job_roles_to_published_labels_test_cases
        ],
    )
    def test_collapses_job_roles_as_expected(self, case):
        test_lf = pl.LazyFrame(case.input_data)
        expected_lf = pl.LazyFrame(case.expected_data).with_columns(
            pl.col(IndCQC.published_job_role_label).cast(
                CatColType.PublishedJobRoleLabelCatType
            )
        )

        returned_lf = job.collapse_job_roles_to_published_labels(test_lf)

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
            check_row_order=False,
            check_column_order=False,
        )


class TestAggregateEmploymentStatusData:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.aggregate_employment_status_data_test_cases
        ],
    )
    def test_aggregates_employment_status_data(self, case):
        test_lf = pl.LazyFrame(case.input_data)
        expected_lf = pl.LazyFrame(
            case.expected_data,
            schema_overrides={EmpStatus.employment_status_count: pl.UInt32},
        )

        returned_lf = job.aggregate_employment_status_data(test_lf)

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
            check_row_order=False,
            check_column_order=False,
        )


class TestReshapeEmploymentStatusData:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.reshape_employment_status_data_test_cases
        ],
    )
    def test_reshapes_employment_status_data(self, case):
        emplstat_count_cols = [
            EmpStatus.employment_status_count,
            *job.EMPLOYMENT_STATUS_LABEL_TO_COLUMN.values(),
        ]
        test_lf = pl.LazyFrame(
            case.input_data,
            schema_overrides={EmpStatus.employment_status_count: pl.UInt32},
        )
        expected_lf = pl.LazyFrame(
            case.expected_data,
            schema_overrides={
                column: pl.UInt32
                for column in emplstat_count_cols
                if column in case.expected_data
            },
        ).with_columns(
            pl.col(AWKClean.location_id).cast(CatColType.LocationCatType),
            pl.col(AWKClean.establishment_id).cast(CatColType.EstablishmentCatType),
        )

        returned_lf = job.reshape_employment_status_data(test_lf)

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
            check_row_order=False,
            check_column_order=False,
        )
