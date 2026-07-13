from datetime import date
from unittest.mock import ANY, Mock, patch

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

PATCH_PATH = "projects._01_ingest.ascwds.fargate.utils.clean_workplace_utils"


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
        returned_lf = test_lf.filter(job.remove_rows_with_duplicate_location_ids())
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
        expected_lf = pl.LazyFrame(
            Data.expected_create_purge_date_columns_rows,
            Schemas.create_purge_date_columns_schema,
            orient="row",
        )
        test_lf = expected_lf.drop(
            AWPClean.master_update_date_org,
            AWPClean.purge_date,
            AWPClean.data_last_amended_date,
            AWPClean.workplace_last_active_date,
        )

        returned_lf = job.create_purge_date_columns(test_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


class TestProduceAndSaveDataForReconciliation:
    TEST_LF = Mock(spec=pl.LazyFrame, name="test_lf")
    RECONCILIATION_DESTINATION = "some/other/destination"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.filter_to_maximum_value_in_column")
    def test_main_runs(
        self,
        filter_to_maximum_value_in_column_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.produce_and_save_data_for_reconciliation(
            self.TEST_LF, self.RECONCILIATION_DESTINATION
        )

        filter_to_maximum_value_in_column_mock.assert_called_once()
        sink_to_parquet_mock.assert_called_once_with(
            ANY, output_path=self.RECONCILIATION_DESTINATION
        )

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    def test_filters_to_latest_import_date_and_selects_expected_columns(
        self, sink_to_parquet_mock: Mock
    ):
        test_lf = pl.LazyFrame(
            Data.data_for_reconciliation_rows,
            Schemas.produce_and_save_data_for_reconciliation_schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            Data.expected_data_for_reconciliation_rows,
            Schemas.expected_produce_and_save_data_for_reconciliation_schema,
            orient="row",
        )

        job.produce_and_save_data_for_reconciliation(
            test_lf, self.RECONCILIATION_DESTINATION
        )

        passed_lf = sink_to_parquet_mock.call_args.args[0]

        pl_testing.assert_frame_equal(passed_lf, expected_lf)


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
