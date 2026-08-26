import json
from unittest.mock import Mock, patch

import polars as pl

import projects._01_ingest.ons_pd.fargate.validate_postcode_directory_raw_data as job
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.raw_data_files.ons_columns import (
    OnsPostcodeDirectoryColumns as ONS,
)

PATCH_PATH = "projects._01_ingest.ons_pd.fargate.validate_postcode_directory_raw_data"


class TestMain:
    source_df = pl.DataFrame(
        {
            ONS.postcode: ["SW1A 1AA", "SW1A 1AA", "CT1 2AB", "CT1 2AB"],
            Keys.import_date: ["20260101", "20260115", "20260101", "20260115"],
            ONS.cssr: ["Westminster"] * 2 + ["Kent"] * 2,
            ONS.region: ["London"] * 4,
            ONS.sub_icb: ["North West London Sub ICB"] * 2
            + ["Kent and Medway Sub ICB"] * 2,
            ONS.icb: ["NHS North West London ICB"] * 2
            + ["NHS Kent and Medway ICB"] * 2,
            ONS.icb_region: ["London"] * 2 + ["South East"] * 2,
            ONS.lower_super_output_area_2021: ["E01000001"] * 2 + ["E01008678"] * 2,
            ONS.middle_super_output_area_2021: ["E02000001"] * 2 + ["E02001938"] * 2,
            ONS.rural_urban_indicator_2011: ["3"] * 2 + ["1"] * 2,
        }
    )

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_runs(self, mock_read_parquet: Mock, mock_write_reports: Mock):
        mock_read_parquet.return_value = self.source_df

        job.main("bucket", "my/source/", "my/reports/")

        mock_read_parquet.assert_called_once_with(
            source="s3://bucket/my/source/",
            selected_columns=job.VALIDATED_COLUMNS,
        )
        mock_write_reports.assert_called_once()

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_report_includes_expected_validations(
        self, mock_read_parquet: Mock, mock_write_reports: Mock
    ):
        mock_read_parquet.return_value = self.source_df

        job.main("bucket", "my/source/", "my/reports/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())
        assertion_types_present = {item["assertion_type"] for item in report_json}

        expected_assertions = {"col_vals_not_null", "rows_distinct"}
        for assertion in expected_assertions:
            assert (
                assertion in assertion_types_present
            ), f"{assertion} not found in validation report"

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_rows_distinct_check_fails_when_postcode_and_import_date_repeat(
        self, mock_read_parquet: Mock, mock_write_reports: Mock
    ):
        duplicate_rows_df = pl.DataFrame(
            {
                ONS.postcode: ["SW1A 1AA", "SW1A 1AA"],
                Keys.import_date: ["20260101", "20260101"],
                ONS.cssr: ["Westminster", "Westminster"],
                ONS.region: ["London", "London"],
                ONS.sub_icb: ["North West London Sub ICB", "North West London Sub ICB"],
                ONS.icb: ["NHS North West London ICB", "NHS North West London ICB"],
                ONS.icb_region: ["London", "London"],
                ONS.lower_super_output_area_2021: ["E01000001", "E01000001"],
                ONS.middle_super_output_area_2021: ["E02000001", "E02000001"],
                ONS.rural_urban_indicator_2011: ["3", "3"],
            }
        )
        mock_read_parquet.return_value = duplicate_rows_df

        job.main("bucket", "my/source/", "my/reports/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())
        rows_distinct_step = next(
            item for item in report_json if item["assertion_type"] == "rows_distinct"
        )

        assert rows_distinct_step["all_passed"] is False
