from dataclasses import dataclass
from datetime import date

import polars as pl
import polars.testing as pl_testing

import projects._01_ingest.ascwds.fargate.utils.clean_workplace_utils as job
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)


@dataclass
class Schemas:
    test_schema = {
        AWPClean.organisation_id: pl.String,
        AWPClean.ascwds_workplace_import_date: pl.Date,
        AWPClean.master_update_date: pl.Date,
        AWPClean.is_parent: pl.String,
        AWPClean.last_logged_in_date: pl.Date,
    }
    master_upd_date_org_schema = {
        **test_schema,
        AWPClean.master_update_date_org: pl.Date,
    }
    purge_date_exprs_schema = {
        **master_upd_date_org_schema,
        AWPClean.purge_date: pl.Date,
        AWPClean.data_last_amended_date: pl.Date,
        AWPClean.workplace_last_active_date: pl.Date,
    }
    purge_date_exprs_schema = {
        **master_upd_date_org_schema,
        AWPClean.purge_date: pl.Date,
        AWPClean.data_last_amended_date: pl.Date,
        AWPClean.workplace_last_active_date: pl.Date,
    }
    purge_date_exprs_schema = {
        **master_upd_date_org_schema,
        AWPClean.purge_date: pl.Date,
        AWPClean.data_last_amended_date: pl.Date,
        AWPClean.workplace_last_active_date: pl.Date,
    }


class TestCleanWorkplaceDataExpressions:
    exprs = job.CleanWorkplaceDataExpressions()

    def test_purge_date_subtracts_correct_months(self):
        test_lf = pl.LazyFrame(
            [("org1", date(2024, 6, 1), date(2024, 5, 1), "No", date(2024, 4, 1))],
            schema=Schemas.base,
            orient="row",
        )
        expected_lf = test_lf.with_columns(
            pl.lit(date(2022, 6, 1)).cast(pl.Date).alias(AWPClean.purge_date)
        )
        returned_lf = test_lf.with_columns(self.exprs.purge_date)
        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_data_last_amended_date_uses_org_max_for_parent(self):
        test_lf = pl.LazyFrame(
            [
                ("org1",date(2024, 6, 1),date(2020, 1, 1),"Yes",date(2024, 4, 1),date(2024, 5, 1))
            ], # fmt: skip
            schema=Schemas.with_org_max,
            orient="row",
        )
        expected_lf = test_lf.with_columns(
            pl.lit(date(2024, 5, 1))
            .cast(pl.Date)
            .alias(AWPClean.data_last_amended_date)
        )
        returned_lf = test_lf.with_columns(self.exprs.data_last_amended_date)
        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_data_last_amended_date_uses_own_date_for_non_parent(self):
        test_lf = pl.LazyFrame(
            [
                ("org1",date(2024, 6, 1),date(2024, 3, 1),"No",date(2024, 4, 1),date(2024, 5, 1))
            ], # fmt: skip
            schema=Schemas.with_org_max,
            orient="row",
        )
        expected_lf = test_lf.with_columns(
            pl.lit(date(2024, 3, 1))
            .cast(pl.Date)
            .alias(AWPClean.data_last_amended_date)
        )
        returned_lf = test_lf.with_columns(self.exprs.data_last_amended_date)
        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_workplace_last_active_date(self):
        test_lf = pl.LazyFrame(
            [
                ("org1",date(2024, 6, 1),date(2024, 3, 1),"No",date(2024, 4, 1),date(2024, 5, 1))
            ], # fmt: skip
            schema=Schemas.with_org_max,
            orient="row",
        )
        expected_lf = test_lf.with_columns(
            pl.lit(date(2024, 3, 1))
            .cast(pl.Date)
            .alias(AWPClean.workplace_last_active_date)
        )
        returned_lf = test_lf.with_columns(self.exprs.workplace_last_active_date)
        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestAddMasterUpdateDateOrg:
    def test_org_max_is_maximum_within_org_and_import_date(self):
        test_lf = pl.LazyFrame(
            [
                ("org1", date(2024, 6, 1), date(2024, 1, 1), "No", date(2024, 1, 1)),
                ("org1", date(2024, 6, 1), date(2024, 5, 1), "No", date(2024, 1, 1)),
                ("org2", date(2024, 6, 1), date(2024, 3, 1), "No", date(2024, 1, 1)),
            ],
            schema=Schemas.base,
            orient="row",
        )
        expected_lf = test_lf.with_columns(
            pl.Series(
                AWPClean.master_update_date_org,
                [date(2024, 5, 1), date(2024, 5, 1), date(2024, 3, 1)],
                dtype=pl.Date,
            )
        )
        returned_lf = job.add_master_update_date_org(test_lf)
        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)

    def test_org_max_is_scoped_per_import_date(self):
        test_lf = pl.LazyFrame(
            [
                ("org1", date(2024, 1, 1), date(2024, 1, 1), "No", date(2024, 1, 1)),
                ("org1", date(2024, 6, 1), date(2024, 5, 1), "No", date(2024, 1, 1)),
            ],
            schema=Schemas.base,
            orient="row",
        )
        expected_lf = test_lf.with_columns(
            pl.Series(
                AWPClean.master_update_date_org,
                [date(2024, 1, 1), date(2024, 5, 1)],
                dtype=pl.Date,
            )
        )
        returned_lf = job.add_master_update_date_org(test_lf)
        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)

    def test_single_record_org_uses_own_date(self):
        test_lf = pl.LazyFrame(
            [("org1", date(2024, 6, 1), date(2024, 3, 1), "No", date(2024, 1, 1))],
            schema=Schemas.base,
            orient="row",
        )
        expected_lf = test_lf.with_columns(
            pl.lit(date(2024, 3, 1))
            .cast(pl.Date)
            .alias(AWPClean.master_update_date_org)
        )
        returned_lf = job.add_master_update_date_org(test_lf)
        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestCreatePurgedLfsForReconciliationAndData:
    def test_recent_record_retained_in_both_outputs(self):
        test_lf = pl.LazyFrame(
            [("org1", date(2024, 6, 1), date(2024, 5, 1), "No", date(2024, 4, 1))],
            schema=Schemas.base,
            orient="row",
        )
        workplace_lf, recon_lf = job.create_purged_lfs_for_reconciliation_and_data(
            test_lf
        )
        pl_testing.assert_frame_equal(
            workplace_lf.select(Schemas.base.keys()),
            test_lf,
        )
        pl_testing.assert_frame_equal(
            recon_lf.select(Schemas.base.keys()),
            test_lf,
        )

    def test_stale_record_removed_from_both_outputs(self):
        test_lf = pl.LazyFrame(
            [("org1", date(2024, 6, 1), date(2020, 1, 1), "No", date(2020, 1, 1))],
            schema=Schemas.base,
            orient="row",
        )
        workplace_lf, recon_lf = job.create_purged_lfs_for_reconciliation_and_data(
            test_lf
        )
        pl_testing.assert_frame_equal(
            workplace_lf.select(Schemas.base.keys()),
            pl.LazyFrame(schema=Schemas.base),
        )
        pl_testing.assert_frame_equal(
            recon_lf.select(Schemas.base.keys()),
            pl.LazyFrame(schema=Schemas.base),
        )

    def test_stale_by_update_but_recent_login_retained_in_recon_only(self):
        test_lf = pl.LazyFrame(
            [("org1", date(2024, 6, 1), date(2020, 1, 1), "No", date(2024, 5, 1))],
            schema=Schemas.base,
            orient="row",
        )
        workplace_lf, recon_lf = job.create_purged_lfs_for_reconciliation_and_data(
            test_lf
        )
        pl_testing.assert_frame_equal(
            workplace_lf.select(Schemas.base.keys()),
            pl.LazyFrame(schema=Schemas.base),
        )
        pl_testing.assert_frame_equal(
            recon_lf.select(Schemas.base.keys()),
            test_lf,
        )

    def test_parent_retained_when_sibling_has_recent_update(self):
        test_lf = pl.LazyFrame(
            [
                ("org1", date(2024, 6, 1), date(2020, 1, 1), "Yes", date(2020, 1, 1)),
                ("org1", date(2024, 6, 1), date(2024, 5, 1), "No", date(2020, 1, 1)),
            ],
            schema=Schemas.base,
            orient="row",
        )
        workplace_lf, _ = job.create_purged_lfs_for_reconciliation_and_data(test_lf)
        pl_testing.assert_frame_equal(
            workplace_lf.select(Schemas.base.keys()),
            test_lf,
            check_row_order=False,
        )

    def test_parent_removed_when_all_siblings_stale(self):
        test_lf = pl.LazyFrame(
            [
                ("org1", date(2024, 6, 1), date(2020, 1, 1), "Yes", date(2020, 1, 1)),
                ("org1", date(2024, 6, 1), date(2020, 2, 1), "No", date(2020, 1, 1)),
            ],
            schema=Schemas.base,
            orient="row",
        )
        workplace_lf, _ = job.create_purged_lfs_for_reconciliation_and_data(test_lf)
        pl_testing.assert_frame_equal(
            workplace_lf.select(Schemas.base.keys()),
            pl.LazyFrame(schema=Schemas.base),
        )

    def test_returns_lazy_frames(self):
        test_lf = pl.LazyFrame(
            [("org1", date(2024, 6, 1), date(2024, 5, 1), "No", date(2024, 4, 1))],
            schema=Schemas.base,
            orient="row",
        )
        workplace_lf, recon_lf = job.create_purged_lfs_for_reconciliation_and_data(
            test_lf
        )
        assert isinstance(workplace_lf, pl.LazyFrame)
        assert isinstance(recon_lf, pl.LazyFrame)
