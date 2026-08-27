import json
from datetime import date
from unittest.mock import Mock, patch

import polars as pl

import projects._01_ingest.ons_pd.fargate.validate_postcode_directory_cleaned_data as job
from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
)
from utils.column_names.raw_data_files.ons_columns import (
    OnsPostcodeDirectoryColumns as ONS,
)

PATCH_PATH = (
    "projects._01_ingest.ons_pd.fargate.validate_postcode_directory_cleaned_data"
)


def build_cleaned_df(
    postcodes: list[str] = ["SW1A 1AA", "CT1 2AB"],
    cssr: list[str] = ["Barnet", "Bristol"],
    region: list[str] = ["London", "South West"],
    import_dates: list[date] | None = None,
    rui21: list[str] | None = None,
) -> pl.DataFrame:
    import_dates = import_dates or [date(2026, 1, 1)] * len(postcodes)
    rui21 = rui21 or ["Urban - Nearer to a major town or city"] * len(postcodes)
    return pl.DataFrame(
        {
            ONSClean.postcode: postcodes,
            ONSClean.contemporary_ons_import_date: import_dates,
            ONSClean.contemporary_cssr: cssr,
            ONSClean.contemporary_region: region,
            ONSClean.current_ons_import_date: import_dates,
            ONSClean.current_cssr: cssr,
            ONSClean.current_region: region,
            ONSClean.current_sub_icb: ["Sub ICB 1"] * len(postcodes),
            ONSClean.current_icb: ["ICB 1"] * len(postcodes),
            ONSClean.current_icb_region: region,
            ONSClean.current_lsoa21: ["E01000001"] * len(postcodes),
            ONSClean.current_msoa21: ["E02000001"] * len(postcodes),
            ONSClean.current_rural_urban_ind_11: ["Urban major conurbation"]
            * len(postcodes),
            ONSClean.current_rural_urban_ind_21: rui21,
        }
    )


ALL_SIX_RUI21_VALUES = [
    "Rural - Larger: Further from a major town or city",
    "Rural - Larger: Nearer to a major town or city",
    "Rural - Smaller: Further from a major town or city",
    "Rural - Smaller: Nearer to a major town or city",
    "Urban - Further from a major town or city",
    "Urban - Nearer to a major town or city",
]
SIX_POSTCODES = ["PC1", "PC2", "PC3", "PC4", "PC5", "PC6"]


class TestMain:
    cleaned_df = build_cleaned_df()
    compare_lf = pl.LazyFrame({ONS.postcode: ["SW1A 1AA", "CT1 2AB"]})

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_runs(
        self, mock_read_parquet: Mock, mock_scan_parquet: Mock, mock_write_reports: Mock
    ):
        mock_read_parquet.return_value = self.cleaned_df
        mock_scan_parquet.return_value = self.compare_lf

        job.main("bucket", "my/source/", "my/reports/", "my/compare/")

        mock_read_parquet.assert_called_once_with(source="s3://bucket/my/source/")
        mock_scan_parquet.assert_called_once_with(
            "s3://bucket/my/compare/", selected_columns=[ONS.postcode]
        )
        mock_write_reports.assert_called_once()

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_report_includes_expected_validations(
        self, mock_read_parquet: Mock, mock_scan_parquet: Mock, mock_write_reports: Mock
    ):
        mock_read_parquet.return_value = self.cleaned_df
        mock_scan_parquet.return_value = self.compare_lf

        job.main("bucket", "my/source/", "my/reports/", "my/compare/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())
        assertion_types_present = {item["assertion_type"] for item in report_json}

        expected_assertions = {
            "row_count_match",
            "col_vals_not_null",
            "rows_distinct",
            "col_vals_in_set",
            "specially",
        }
        for assertion in expected_assertions:
            assert (
                assertion in assertion_types_present
            ), f"{assertion} not found in validation report"

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_row_count_match_fails_when_cleaned_row_count_differs_from_raw(
        self, mock_read_parquet: Mock, mock_scan_parquet: Mock, mock_write_reports: Mock
    ):
        mock_read_parquet.return_value = self.cleaned_df
        mock_scan_parquet.return_value = pl.LazyFrame(
            {ONS.postcode: ["SW1A 1AA", "CT1 2AB", "AB1 2CD"]}
        )

        job.main("bucket", "my/source/", "my/reports/", "my/compare/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())
        row_count_step = next(
            item for item in report_json if item["assertion_type"] == "row_count_match"
        )

        assert row_count_step["all_passed"] is False

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_col_vals_not_null_fails_when_a_required_column_is_null(
        self, mock_read_parquet: Mock, mock_scan_parquet: Mock, mock_write_reports: Mock
    ):
        null_cssr_df = build_cleaned_df(cssr=[None, "Bristol"])
        mock_read_parquet.return_value = null_cssr_df
        mock_scan_parquet.return_value = self.compare_lf

        job.main("bucket", "my/source/", "my/reports/", "my/compare/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())
        not_null_steps = [
            item
            for item in report_json
            if item["assertion_type"] == "col_vals_not_null"
        ]

        assert any(not step["all_passed"] for step in not_null_steps)

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_rows_distinct_check_fails_when_postcode_and_import_date_repeat(
        self, mock_read_parquet: Mock, mock_scan_parquet: Mock, mock_write_reports: Mock
    ):
        duplicate_rows_df = build_cleaned_df(
            postcodes=["SW1A 1AA", "SW1A 1AA"],
            cssr=["Barnet", "Barnet"],
            region=["London", "London"],
        )
        mock_read_parquet.return_value = duplicate_rows_df
        mock_scan_parquet.return_value = self.compare_lf

        job.main("bucket", "my/source/", "my/reports/", "my/compare/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())
        rows_distinct_step = next(
            item for item in report_json if item["assertion_type"] == "rows_distinct"
        )

        assert rows_distinct_step["all_passed"] is False

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_col_vals_in_set_fails_when_region_is_not_a_known_value(
        self, mock_read_parquet: Mock, mock_scan_parquet: Mock, mock_write_reports: Mock
    ):
        invalid_region_df = build_cleaned_df(region=["Fake Region", "South West"])
        mock_read_parquet.return_value = invalid_region_df
        mock_scan_parquet.return_value = self.compare_lf

        job.main("bucket", "my/source/", "my/reports/", "my/compare/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())
        in_set_steps = [
            item for item in report_json if item["assertion_type"] == "col_vals_in_set"
        ]

        assert any(not step["all_passed"] for step in in_set_steps)

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_distinct_values_check_passes_when_every_canonical_rui21_value_is_present(
        self, mock_read_parquet: Mock, mock_scan_parquet: Mock, mock_write_reports: Mock
    ):
        all_values_df = build_cleaned_df(
            postcodes=SIX_POSTCODES,
            cssr=["Barnet"] * 6,
            region=["London"] * 6,
            rui21=ALL_SIX_RUI21_VALUES,
        )
        mock_read_parquet.return_value = all_values_df
        mock_scan_parquet.return_value = pl.LazyFrame({ONS.postcode: SIX_POSTCODES})

        job.main("bucket", "my/source/", "my/reports/", "my/compare/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())
        rui21_distinct_step = next(
            item
            for item in report_json
            if item["assertion_type"] == "specially"
            and ONSClean.current_rural_urban_ind_21 in (item["brief"] or "")
        )

        assert rui21_distinct_step["all_passed"] is True

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_distinct_values_check_fails_when_a_canonical_rui21_value_is_missing(
        self, mock_read_parquet: Mock, mock_scan_parquet: Mock, mock_write_reports: Mock
    ):
        missing_value_df = build_cleaned_df(
            postcodes=SIX_POSTCODES,
            cssr=["Barnet"] * 6,
            region=["London"] * 6,
            rui21=ALL_SIX_RUI21_VALUES[:5] + [ALL_SIX_RUI21_VALUES[0]],
        )
        mock_read_parquet.return_value = missing_value_df
        mock_scan_parquet.return_value = pl.LazyFrame({ONS.postcode: SIX_POSTCODES})

        job.main("bucket", "my/source/", "my/reports/", "my/compare/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())
        rui21_distinct_step = next(
            item
            for item in report_json
            if item["assertion_type"] == "specially"
            and ONSClean.current_rural_urban_ind_21 in (item["brief"] or "")
        )

        assert rui21_distinct_step["all_passed"] is False
