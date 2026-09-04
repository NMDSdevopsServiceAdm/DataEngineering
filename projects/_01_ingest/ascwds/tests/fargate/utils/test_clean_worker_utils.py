import polars as pl
import polars.testing as pl_testing
import pytest

import projects._01_ingest.ascwds.fargate.utils.clean_worker_utils as job
from projects._01_ingest.unittest_data.polars_ingest_test_file_data import (
    TestCleanAscwdsWorkerUtilsData as Data,
)
from projects._01_ingest.unittest_data.polars_ingest_test_file_schema import (
    TestCleanAscwdsWorkerUtilsSchemas as Schemas,
)
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)


class TestRemoveWorkersWithoutWorkplaces:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.remove_workers_without_workplaces_test_cases
        ],
    )
    def test_removes_workers_without_matching_workplace(self, case):
        worker_lf = pl.LazyFrame(
            case.worker_data, schema=Schemas.worker_schema, orient="row"
        )
        workplace_lf = pl.LazyFrame(
            case.workplace_data, schema=Schemas.workplace_schema, orient="row"
        )
        expected_lf = pl.LazyFrame(
            case.expected_data,
            schema=Schemas.expected_remove_workers_without_workplaces_schema,
            orient="row",
        )

        returned_lf = job.remove_workers_without_workplaces(worker_lf, workplace_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)

    def test_keeps_workers_with_matching_workplace(self):
        case = Data.remove_workers_without_workplaces_test_cases[1]
        worker_lf = pl.LazyFrame(
            case.worker_data, schema=Schemas.worker_schema, orient="row"
        )
        workplace_lf = pl.LazyFrame(
            case.workplace_data, schema=Schemas.workplace_schema, orient="row"
        )

        returned_lf = job.remove_workers_without_workplaces(worker_lf, workplace_lf)

        assert (
            returned_lf.select(pl.len()).collect().item()
            == worker_lf.select(pl.len()).collect().item()
        )


class TestRemapMainjridCodes:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.remap_mainjrid_codes_test_cases
        ],
    )
    def test_function_returns_expected_values(self, case):
        input_lf = pl.LazyFrame(
            case.input_data, schema=Schemas.remap_mainjrid_codes_schema, orient="row"
        )
        expected_lf = pl.LazyFrame(
            case.expected_data,
            schema=Schemas.remap_mainjrid_codes_schema,
            orient="row",
        )

        returned_lf = job.remap_mainjrid_codes(input_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestImputeNotKnownJobRoles:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.impute_not_known_job_roles_test_cases
        ],
    )
    def test_function_returns_expected_values(self, case):
        input_lf = pl.LazyFrame(
            case.input_data,
            schema=Schemas.impute_not_known_job_roles_schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            case.expected_data,
            schema=Schemas.impute_not_known_job_roles_schema,
            orient="row",
        )

        returned_lf = job.impute_not_known_job_roles(input_lf)

        pl_testing.assert_frame_equal(
            returned_lf.sort(AWKClean.worker_id, AWKClean.ascwds_worker_import_date),
            expected_lf.sort(AWKClean.worker_id, AWKClean.ascwds_worker_import_date),
        )


class TestCreateCleanMainJobRoleColumn:
    case = Data.create_clean_main_job_role_column_case
    input_lf = pl.LazyFrame(
        case.input_data,
        schema=Schemas.create_clean_main_job_role_column_schema,
        orient="row",
    )
    labels_lf = pl.LazyFrame(
        case.labels_data, schema=Schemas.data_labels_schema, orient="row"
    )
    expected_lf = pl.LazyFrame(
        case.expected_data,
        schema=Schemas.expected_create_clean_main_job_role_column_schema,
        orient="row",
    )
    returned_lf = job.create_clean_main_job_role_column(input_lf, labels_lf)

    def test_returns_expected_main_job_role_clean_values(self):
        pl_testing.assert_frame_equal(
            self.returned_lf.sort(
                AWKClean.worker_id, AWKClean.ascwds_worker_import_date
            ).select(AWKClean.worker_id, AWKClean.main_job_role_clean),
            self.expected_lf.sort(
                AWKClean.worker_id, AWKClean.ascwds_worker_import_date
            ).select(AWKClean.worker_id, AWKClean.main_job_role_clean),
        )

    def test_returns_expected_main_job_role_clean_labelled_values(self):
        pl_testing.assert_frame_equal(
            self.returned_lf.sort(
                AWKClean.worker_id, AWKClean.ascwds_worker_import_date
            ).select(AWKClean.worker_id, AWKClean.main_job_role_clean_labelled),
            self.expected_lf.sort(
                AWKClean.worker_id, AWKClean.ascwds_worker_import_date
            ).select(AWKClean.worker_id, AWKClean.main_job_role_clean_labelled),
        )

    def test_filters_out_unresolvable_job_role_rows(self):
        returned_worker_ids = (
            self.returned_lf.select(AWKClean.worker_id).collect().to_series().to_list()
        )

        assert "102" not in returned_worker_ids
