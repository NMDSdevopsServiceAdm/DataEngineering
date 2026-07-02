from datetime import date

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._01_ingest.ascwds.fargate.utils.clean_workplace_utils as job
from projects._01_ingest.unittest_data.polars_ingest_test_file_data import (
    TestCreatePurgedLfsForReconciliationAndData as Data,
)
from projects._01_ingest.unittest_data.polars_ingest_test_file_schema import (
    TestCreatePurgedLfsForReconciliationAndDataSchemas as Schemas,
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


class TestCleanWorkplaceDataExpressions:
    exprs = job.CleanWorkplaceDataExpressions()

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
