import json
import unittest
from datetime import date
from unittest.mock import Mock, patch

import polars as pl

import projects._01_ingest.ascwds.fargate.validate_clean_ascwds_workplace as job
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as ASCWPClean,
)

PATCH_PATH = "projects._01_ingest.ascwds.fargate.validate_clean_ascwds_workplace"


class ValidateCleanASCWDSWorkplaceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.source_df = pl.DataFrame(
            data={
                ASCWPClean.organisation_id: "",
                ASCWPClean.period: "",
                ASCWPClean.establishment_id: "",
                ASCWPClean.establishment_id_from_nmds: "",
                ASCWPClean.parent_id: "",
                ASCWPClean.nmds_id: "",
                ASCWPClean.establishment_created_date: date(2000, 1, 1),
                ASCWPClean.establishment_updated_date: date(2000, 1, 1),
                ASCWPClean.master_update_date: date(2000, 1, 1),
                ASCWPClean.last_logged_in: date(2000, 1, 1),
                ASCWPClean.la_permission: "",
                ASCWPClean.is_bulk_uploader: "",
                ASCWPClean.is_parent: "",
                ASCWPClean.parent_permission: "",
                ASCWPClean.registration_type: "",
                ASCWPClean.provider_id: "",
                ASCWPClean.location_id: "",
                ASCWPClean.establishment_type: "",
                ASCWPClean.establishment_name: "",
                ASCWPClean.address: "",
                ASCWPClean.postcode: "",
                ASCWPClean.region_id: "",
                ASCWPClean.total_staff: 1,
                ASCWPClean.worker_records: 1,
                ASCWPClean.total_starters: "",
                ASCWPClean.total_leavers: "",
                ASCWPClean.total_vacancies: "",
                ASCWPClean.main_service_id: "",
                ASCWPClean.version: "",
                ASCWPClean.ascwds_workplace_import_date: date(2000, 1, 1),
                ASCWPClean.master_update_date_org: date(2000, 1, 1),
                ASCWPClean.purge_date: date(2000, 1, 1),
                ASCWPClean.data_last_amended_date: date(2000, 1, 1),
                ASCWPClean.workplace_last_active_date: date(2000, 1, 1),
                ASCWPClean.total_staff_bounded: 1,
                ASCWPClean.worker_records_bounded: 1,
                ASCWPClean.import_date: "",
            },
            schema={
                ASCWPClean.organisation_id: pl.String,
                ASCWPClean.period: pl.String,
                ASCWPClean.establishment_id: pl.String,
                ASCWPClean.establishment_id_from_nmds: pl.String,
                ASCWPClean.parent_id: pl.String,
                ASCWPClean.nmds_id: pl.String,
                ASCWPClean.establishment_created_date: pl.Date,
                ASCWPClean.establishment_updated_date: pl.Date,
                ASCWPClean.master_update_date: pl.Date,
                ASCWPClean.last_logged_in: pl.Date,
                ASCWPClean.la_permission: pl.String,
                ASCWPClean.is_bulk_uploader: pl.String,
                ASCWPClean.is_parent: pl.String,
                ASCWPClean.parent_permission: pl.String,
                ASCWPClean.registration_type: pl.String,
                ASCWPClean.provider_id: pl.String,
                ASCWPClean.location_id: pl.String,
                ASCWPClean.establishment_type: pl.String,
                ASCWPClean.establishment_name: pl.String,
                ASCWPClean.address: pl.String,
                ASCWPClean.postcode: pl.String,
                ASCWPClean.region_id: pl.String,
                ASCWPClean.total_staff: pl.Int32,
                ASCWPClean.worker_records: pl.Int32,
                ASCWPClean.total_starters: pl.String,
                ASCWPClean.total_leavers: pl.String,
                ASCWPClean.total_vacancies: pl.String,
                ASCWPClean.main_service_id: pl.String,
                ASCWPClean.version: pl.String,
                ASCWPClean.ascwds_workplace_import_date: pl.Date,
                ASCWPClean.master_update_date_org: pl.Date,
                ASCWPClean.purge_date: pl.Date,
                ASCWPClean.data_last_amended_date: pl.Date,
                ASCWPClean.workplace_last_active_date: pl.Date,
                ASCWPClean.total_staff_bounded: pl.Int32,
                ASCWPClean.worker_records_bounded: pl.Int32,
                ASCWPClean.import_date: pl.String,
            },
        )

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_runs(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.return_value = self.source_df
        job.main("bucket", "my/source/", "my/reports/")

        mock_read_parquet.assert_called_once_with(source="s3://bucket/my/source/")
        mock_write_reports.assert_called_once()

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_report_includes_expected_validations(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.return_value = self.source_df

        job.main("bucket", "my/source/", "my/reports/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())

        assertion_types_present = {item["assertion_type"] for item in report_json}

        expected_assertions = {
            "col_schema_match",
            "col_vals_not_null",
            "rows_distinct",
            "col_vals_between",
            "col_vals_in_set",
            "specially",
        }

        for assertion in expected_assertions:
            self.assertIn(
                assertion,
                assertion_types_present,
                f"{assertion} not found in validation report",
            )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
