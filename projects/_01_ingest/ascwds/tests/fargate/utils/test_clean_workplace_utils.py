from datetime import date

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._01_ingest.ascwds.fargate.utils.clean_workplace_utils as job
from projects._01_ingest.unittest_data.polars_ingest_test_file_data import (
    TestCleanAscwdsWorkplaceUtilsData as Data,
)
from projects._01_ingest.unittest_data.polars_ingest_test_file_schema import (
    TestCleanAscwdsWorkplaceUtilsSchemas as Schemas,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.raw_data_files.ascwds_worker_columns import (
    AscwdsWorkerColumns as AWKRaw,
)


class TestValidWorkplaceFilter:
    def test_valid_workplace_filter_returns_correct_rows(self):
        input_lf = pl.LazyFrame(
            {
                AWPClean.organisation_id: [
                    "305",  # test account
                    "123",
                    "1234",
                    "28470",  # test account
                ],
                AWPClean.establishment_id: [
                    "1",
                    "48904",  # duplicate
                    "12",
                    "50640",  # duplicate
                ],
            }
        )
        expected_lf = pl.LazyFrame(
            {
                AWPClean.organisation_id: ["1234"],
                AWPClean.establishment_id: ["12"],
            }
        )

        returned_lf = input_lf.filter(job.valid_workplace_filter())

        pl_testing.assert_frame_equal(expected_lf, returned_lf)


class TestRemoveRowsWithDuplicateLocationIds:
    def test_remove_duplicate_location_ids_within_import_date(self):
        test_schema = [
            AWPClean.establishment_id,
            AWPClean.location_id,
            AWPClean.ascwds_workplace_import_date,
        ]
        test_lf = pl.LazyFrame(
            [
                ("1", "1-001", date(2026, 1, 1)),  # duplicate location_id
                ("2", "1-001", date(2026, 1, 1)),  # duplicate location_id
                ("3", "1-003", date(2026, 1, 1)),  # keep - unique
                ("4", None, date(2026, 1, 1)),  # keep - null location_id
                ("5", None, date(2026, 1, 1)),  # keep - null location_id
                ("6", "1-006", date(2026, 1, 1)),  # keep - unique within import date
                ("7", "1-006", date(2026, 1, 2)),  # keep - unique within import date
            ],
            schema=test_schema,
            orient="row",
        )
        returned_lf = job.remove_rows_with_duplicate_location_ids(test_lf)
        expected_lf = pl.LazyFrame(
            [
                ("3", "1-003", date(2026, 1, 1)),
                ("4", None, date(2026, 1, 1)),
                ("5", None, date(2026, 1, 1)),
                ("6", "1-006", date(2026, 1, 1)),
                ("7", "1-006", date(2026, 1, 2)),
            ],
            schema=test_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestPurgeWorkplaceDataExpressions:
    exprs = job.PurgeWorkplaceDataExpressions()

    @pytest.mark.parametrize(
        "case",
        [pytest.param(case, id=case.id) for case in Data.purge_date_test_cases],
    )
    def test_purge_date(self, case):

        expected_lf = pl.LazyFrame(
            case.expected_data, Schemas.purge_date_exprs_schema, orient="row"
        )
        test_lf = expected_lf.drop(AWPClean.purge_date)

        returned_lf = test_lf.with_columns(self.exprs.purge_date)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.data_last_amended_date_test_cases
        ],
    )
    def test_data_last_amended_date(self, case):
        expected_lf = pl.LazyFrame(
            case.expected_data, Schemas.last_amended_date_exprs_schema, orient="row"
        )
        test_lf = expected_lf.drop(AWPClean.data_last_amended_date)

        returned_lf = test_lf.with_columns(self.exprs.data_last_amended_date)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.workplace_last_active_date_test_cases
        ],
    )
    def test_workplace_last_active_date(self, case):
        expected_lf = pl.LazyFrame(
            case.expected_data,
            Schemas.workplace_last_active_date_exprs_schema,
            orient="row",
        )
        test_lf = expected_lf.drop(AWPClean.workplace_last_active_date)
        returned_lf = test_lf.with_columns(self.exprs.workplace_last_active_date)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestAddMasterUpdateDateOrg:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.add_master_update_date_org_test_cases
        ],
    )
    def test_function_returns_expected_values(self, case):
        expected_lf = pl.LazyFrame(
            case.expected_data, Schemas.master_upd_date_org_schema, orient="row"
        )
        test_lf = expected_lf.drop(AWPClean.master_update_date_org)
        returned_lf = job.add_master_update_date_org(test_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


class TestCreatePurgedLfsForReconciliationAndData:
    @pytest.mark.parametrize(
        "case",
        [pytest.param(case, id=case.id) for case in Data.create_purged_lfs_test_cases],
    )
    def test_function_returns_expected_values(self, case):
        test_lf = pl.LazyFrame(case.test_data, Schemas.test_schema, orient="row")

        expected_workplace_lf = (
            pl.LazyFrame(
                case.expected_workplace_data, Schemas.test_schema, orient="row"
            )
            if case.expected_workplace_data is not None
            else pl.LazyFrame(schema=Schemas.test_schema)
        )
        expected_recon_lf = (
            pl.LazyFrame(case.expected_recon_data, Schemas.test_schema, orient="row")
            if case.expected_recon_data is not None
            else pl.LazyFrame(schema=Schemas.test_schema)
        )

        workplace_lf, recon_lf = job.create_purged_lfs_for_reconciliation_and_data(
            test_lf
        )

        pl_testing.assert_frame_equal(
            workplace_lf.select(Schemas.test_schema.keys()),
            expected_workplace_lf,
            check_row_order=False,
        )
        pl_testing.assert_frame_equal(
            recon_lf.select(Schemas.test_schema.keys()),
            expected_recon_lf,
            check_row_order=False,
        )

    def test_returns_lazy_frames(self):
        test_lf = pl.LazyFrame(
            [("org1", date(2024, 6, 1), date(2024, 5, 1), "No", date(2024, 4, 1))],
            schema=Schemas.test_schema,
            orient="row",
        )
        workplace_lf, recon_lf = job.create_purged_lfs_for_reconciliation_and_data(
            test_lf
        )

        assert isinstance(workplace_lf, pl.LazyFrame)
        assert isinstance(recon_lf, pl.LazyFrame)


class TestApplyDataCorrections:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.apply_data_corrections_test_cases
        ],
    )
    def test_function_returns_expected_values(self, case):
        test_lf = pl.LazyFrame(
            case.test_data, Schemas.apply_data_corrections_schema, orient="row"
        )

        expected_workplace_lf = pl.LazyFrame(
            case.expected_data,
            Schemas.apply_data_corrections_schema,
            orient="row",
        )

        returned_lf = job.apply_data_corrections(test_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_workplace_lf)


class TestCreateSlvSchema:
    returned_schema_default = job.create_slv_schema([9, 10])
    returned_schema_inc_index = job.create_slv_schema([9, 10], incl_index=True)

    def test_raises_value_error_when_given_empty_list(self):
        with pytest.raises(ValueError) as context:
            job.create_slv_schema([])

        assert str(context.value) == "Given job role list be populated. Got []"

    def test_returns_a_polars_schema(self):
        assert type(self.returned_schema_default) == pl.Schema

    def test_returns_schema_for_requested_job_roles_when_inc_index_is_default(self):
        expected_schema = pl.Schema(
            {
                AWPClean.job_role_09_employees: pl.Int32,
                AWPClean.job_role_09_starters: pl.Int32,
                AWPClean.job_role_09_leavers: pl.Int32,
                AWPClean.job_role_09_vacancies: pl.Int32,
                AWPClean.job_role_10_employees: pl.Int32,
                AWPClean.job_role_10_starters: pl.Int32,
                AWPClean.job_role_10_leavers: pl.Int32,
                AWPClean.job_role_10_vacancies: pl.Int32,
            }
        )

        assert self.returned_schema_default == expected_schema

    def test_returns_schema_with_index_cols_and_requested_job_roles_when_inc_index_is_true(
        self,
    ):
        expected_schema = pl.Schema(
            {
                AWPClean.establishment_id: pl.String,
                AWPClean.import_date: pl.String,
                AWPClean.job_role_09_employees: pl.String,
                AWPClean.job_role_09_starters: pl.String,
                AWPClean.job_role_09_leavers: pl.String,
                AWPClean.job_role_09_vacancies: pl.String,
                AWPClean.job_role_10_employees: pl.String,
                AWPClean.job_role_10_starters: pl.String,
                AWPClean.job_role_10_leavers: pl.String,
                AWPClean.job_role_10_vacancies: pl.String,
            }
        )

        assert self.returned_schema_inc_index == expected_schema


class TestCheckJobRolesList:
    def test_function_raises_value_error(self):
        test_worker_lf = pl.LazyFrame(
            {
                AWKRaw.main_job_role_id: ["-1", "1", "2", "3"],
                "Another_column": ["a", "b", "c", "d"],
            }
        )
        test_jr_num_list = [1, 2]

        with pytest.raises(ValueError) as context:
            job.check_job_roles_list(test_worker_lf, test_jr_num_list)

        assert (
            str(context.value)
            == "Given job role list ([1, 2]) must match equivalent from raw worker data ([1, 2, 3])."
        )
