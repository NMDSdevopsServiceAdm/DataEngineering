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


class TestCreatePurgeDateColumns:
    def test_function_returns_expected_values(self):
        expected_lf = pl.LazyFrame(Data.expected_create_purge_date_columns_rows)
        test_lf = expected_lf.drop(
            AWPClean.master_update_date_org,
            AWPClean.purge_date,
            AWPClean.data_last_amended_date,
            AWPClean.workplace_last_active_date,
        )

        returned_lf = job.create_purge_date_columns(test_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


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


class TestBoundingExpressions:
    def test_expression_bounds(self):
        exprs = job.BoundingExpressions()
        assert exprs.filled_posts_lower_bound == 1
        assert exprs.slv_lower_bound == 1
        assert exprs.slv_upper_bound == 998

    def test_filled_posts_expression_bounds_values_to_valid_range(self):
        exprs = job.BoundingExpressions()
        test_lf = pl.LazyFrame(
            {
                AWPClean.total_staff: [0, 5, None],
                AWPClean.worker_records: [1, 0, 2],
            }
        )
        expected_lf = pl.LazyFrame(
            {
                AWPClean.total_staff: [0, 5, None],
                AWPClean.worker_records: [1, 0, 2],
                f"{AWPClean.total_staff}_bounded": [None, 5, None],
                f"{AWPClean.worker_records}_bounded": [1, None, 2],
            }
        )

        returned_lf = test_lf.with_columns(exprs.filled_posts_expr)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_slv_expression_bounds_values_to_valid_range(self):
        exprs = job.BoundingExpressions()
        test_lf = pl.LazyFrame(
            {
                AWPClean.job_role_01_employees: [0, 10, 1000, 500],
                AWPClean.job_role_01_starters: [1, 2, 999, 1000],
                AWPClean.job_role_01_leavers: [-1, 500, 998, 999],
                AWPClean.job_role_01_vacancies: [0, 998, 999, 500],
                AWPClean.job_role_01_temporary: [0, 1, 2, 3],  # Not an SLV column
            }
        )
        expected_lf = pl.LazyFrame(
            {
                AWPClean.job_role_01_employees: [None, 10, None, 500],
                AWPClean.job_role_01_starters: [1, 2, None, None],
                AWPClean.job_role_01_leavers: [None, 500, 998, None],
                AWPClean.job_role_01_vacancies: [None, 998, None, 500],
                AWPClean.job_role_01_temporary: [0, 1, 2, 3],
            }
        )

        returned_lf = test_lf.with_columns(exprs.slv_expr)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestFixLegacyJobRoles:
    test_lf = pl.LazyFrame(
        {
            "jr22emp": 1,
            "jr22strt": 2,
            "jr27emp": 3,
            "jr27strt": 4,
        }
    )

    expected_lf = pl.LazyFrame(
        {
            "jr27emp": 4,
            "jr27strt": 6,
        }
    )

    def test_fix_legacy_job_roles(self):
        returned_lf = job.fix_legacy_job_roles(self.test_lf)

        pl_testing.assert_frame_equal(returned_lf, self.expected_lf)
        pass
