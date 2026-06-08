import json
import unittest
from datetime import date
from unittest.mock import Mock, call, patch

import polars as pl

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.validate_01_merge_metadata as job
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns

PATCH_PATH = "projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.validate_01_merge_metadata"


class ValidateJobRoleEstimatesTests(unittest.TestCase):
    def setUp(self) -> None:
        source_schema = {
            IndCqcColumns.location_id: pl.String,
            IndCqcColumns.cqc_location_import_date: pl.Date,
            IndCqcColumns.id_per_locationid_import_date: pl.UInt32,
            IndCqcColumns.name: pl.String,
            IndCqcColumns.provider_id: pl.String,
            IndCqcColumns.services_offered: pl.List(pl.String),
            IndCqcColumns.care_home: pl.String,
            IndCqcColumns.primary_service_type_second_level: pl.String,
            IndCqcColumns.imputed_registration_date: pl.Date,
            IndCqcColumns.ascwds_workplace_import_date: pl.Date,
            IndCqcColumns.ascwds_filtering_rule: pl.String,
            IndCqcColumns.current_ons_import_date: pl.Date,
            IndCqcColumns.current_cssr: pl.String,
            IndCqcColumns.current_region: pl.String,
            IndCqcColumns.current_icb: pl.String,
            IndCqcColumns.current_rural_urban_indicator_2011: pl.String,
            IndCqcColumns.current_lsoa21: pl.String,
            IndCqcColumns.current_msoa21: pl.String,
            IndCqcColumns.estimate_filled_posts_source: pl.String,
            IndCqcColumns.estimate_filled_posts: pl.Float64,
            IndCqcColumns.ascwds_filled_posts_dedup_clean: pl.Float64,
            IndCqcColumns.ascwds_pir_merged: pl.Float64,
            IndCqcColumns.number_of_beds: pl.Float64,
            IndCqcColumns.worker_records_bounded: pl.Float64,
            IndCqcColumns.dormancy: pl.String,
            IndCqcColumns.ascwds_job_role_counts: pl.Float64,
        }
        source_rows = [
            ("1-001", date(2025, 1, 1), 1, "name1", "1-001", ["Service A"], "Y", "something", date(2023, 1, 1), date(2022, 1, 1), "FilteringRule A", date(2023, 1, 2), "CSSR A", "Region A", "ICB A", "RUI A", "LSOA A", "MSOA A", "Estimate Source A", 10.0, 5.0, 7.0, 50.0, 100.0, "Not Dormant", 8.0),
            ("2-002", date(2025, 1, 2), 2, "name2", "2-002", ["Service B"], "N", "something", date(2023, 1, 2), date(2022, 1, 2), "FilteringRule B", date(2023, 1, 2), "CSSR B", "Region B", "ICB B", "RUI B", "LSOA B", "MSOA B", "Estimate Source B", 15.0, 10.0, 12.0, 60.0, 120.0, "Dormant", 13.0),
        ]  # fmt: skip
        self.source_df = pl.DataFrame(source_rows, source_schema, orient="row")
        self.compare_df = self.source_df.select(
            [
                IndCqcColumns.location_id,
                IndCqcColumns.cqc_location_import_date,
            ]
        )

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_runs(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.side_effect = [self.source_df, self.compare_df]
        job.main("bucket", "my/source/", "my/compare/", "my/reports/")

        self.assertEqual(mock_read_parquet.call_count, 2)
        mock_read_parquet.assert_has_calls(
            [
                call(
                    source="s3://bucket/my/source/",
                    selected_columns=job.VALIDATION_COLS_TO_IMPORT,
                ),
                call(
                    source="s3://bucket/my/compare/",
                    selected_columns=job.IND_CQC_ESTIMATES_COLS_TO_IMPORT,
                ),
            ]
        )
        mock_write_reports.assert_called_once()

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_key_validation_report_includes_expected_validations(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.side_effect = [self.source_df, self.compare_df]

        job.main("my/source/", "my/compare/", "bucket", "my/reports/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())

        assertion_types_present = {item["assertion_type"] for item in report_json}

        expected_assertions = {
            "col_schema_match",
            "row_count_match",
            "rows_distinct",
            "col_vals_not_null",
            "col_vals_in_set",
            "specially",
            "col_vals_ge",
            "col_vals_gt",
        }

        for assertion in expected_assertions:
            self.assertIn(
                assertion,
                assertion_types_present,
                f"{assertion} not found in validation report",
            )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
