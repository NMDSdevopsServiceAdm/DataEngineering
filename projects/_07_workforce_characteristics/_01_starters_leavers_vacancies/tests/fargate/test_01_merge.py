from dataclasses import dataclass
from unittest.mock import ANY, Mock, call, patch

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate._01_merge as job
from projects._07_workforce_characteristics.unittest_data.polars_slv_test_data import (
    EMPLOYEE_STATUS_RATES_BLANK_ROW,
    EMPLOYEE_STATUS_RATES_CSV_HEADER,
    EMPLOYEE_STATUS_RATES_OTHER_YEAR_ROW,
    EMPLOYEE_STATUS_RATES_TARGET_YEAR_ROW,
    employee_status_rates_expected_rows,
)
from projects._07_workforce_characteristics.unittest_data.polars_slv_test_schemas import (
    employee_status_rates_expected_schema,
)

PATCH_PATH = "projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate._01_merge"


class TestMain:
    METADATA_SOURCE = "some/source"
    JOB_ROLE_ESTIMATES_SOURCE = "another/source"
    PREPARED_SLV_DATASET_SOURCE = "other/source"
    EMPLOYEE_STATUS_RATES_SOURCE = "employee/status/rates/source"
    MERGED_DATA_DESTINATION = "some/destination"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.mUtils.apply_employment_status_magic_numbers")
    @patch(f"{PATCH_PATH}.mUtils.join_datasets")
    @patch(f"{PATCH_PATH}.mUtils.convert_ascwds_job_role_columns_to_rows")
    @patch(f"{PATCH_PATH}.load_employee_status_rates_lf")
    @patch(f"{PATCH_PATH}.expr.is_slv_job_role_column")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        is_slv_job_role_column_mock: Mock,
        load_employee_status_rates_lf_mock: Mock,
        convert_ascwds_job_role_columns_to_rows_mock: Mock,
        join_datasets_mock: Mock,
        apply_employment_status_magic_numbers_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.METADATA_SOURCE,
            self.JOB_ROLE_ESTIMATES_SOURCE,
            self.PREPARED_SLV_DATASET_SOURCE,
            self.EMPLOYEE_STATUS_RATES_SOURCE,
            self.MERGED_DATA_DESTINATION,
        )

        assert len(scan_parquet_mock.call_args_list) == 3

        scan_calls = [
            call(source=self.METADATA_SOURCE, selected_columns=job.metadata_columns),
            call(
                source=self.JOB_ROLE_ESTIMATES_SOURCE,
                selected_columns=job.job_role_estimates_columns,
            ),
            call(self.PREPARED_SLV_DATASET_SOURCE),
        ]
        scan_parquet_mock.assert_has_calls(scan_calls)

        # TODO: Uncomment these assertions when the placeholder functions are implemented
        is_slv_job_role_column_mock.assert_called_once()
        # convert_ascwds_job_role_columns_to_rows_mock.assert_called_once()
        # join_datasets_mock.assert_called_once()
        # apply_employment_status_magic_numbers_mock.assert_called_once()

        load_employee_status_rates_lf_mock.assert_called_once_with(
            self.EMPLOYEE_STATUS_RATES_SOURCE
        )

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=ANY,
            output_path=self.MERGED_DATA_DESTINATION,
        )


@dataclass
class LoadEmployeeStatusRatesTestCase:
    id: str
    csv_rows: list[str]
    expected_data: list[tuple]


load_employee_status_rates_test_cases = [
    LoadEmployeeStatusRatesTestCase(
        id="keeps_only_target_weighting_year",
        csv_rows=[
            EMPLOYEE_STATUS_RATES_TARGET_YEAR_ROW,
            EMPLOYEE_STATUS_RATES_OTHER_YEAR_ROW,
        ],
        expected_data=employee_status_rates_expected_rows,
    ),
    LoadEmployeeStatusRatesTestCase(
        id="drops_fully_blank_rows",
        csv_rows=[
            EMPLOYEE_STATUS_RATES_TARGET_YEAR_ROW,
            EMPLOYEE_STATUS_RATES_BLANK_ROW,
        ],
        expected_data=employee_status_rates_expected_rows,
    ),
    LoadEmployeeStatusRatesTestCase(
        id="returns_empty_when_no_rows_match_target_year",
        csv_rows=[EMPLOYEE_STATUS_RATES_OTHER_YEAR_ROW],
        expected_data=[],
    ),
]


class TestLoadEmployeeStatusRatesLf:
    @pytest.mark.parametrize(
        "test_case",
        [
            pytest.param(case, id=case.id)
            for case in load_employee_status_rates_test_cases
        ],
    )
    def test_filters_and_selects_expected_rows(self, tmp_path, test_case):
        csv_path = tmp_path / "employee_status_rates.csv"
        csv_path.write_text(
            "\n".join([EMPLOYEE_STATUS_RATES_CSV_HEADER, *test_case.csv_rows]) + "\n"
        )

        returned_lf = job.load_employee_status_rates_lf(str(csv_path))
        returned_df = returned_lf.collect()

        expected_df = pl.DataFrame(
            test_case.expected_data,
            schema=employee_status_rates_expected_schema,
            orient="row",
        )

        assert returned_df.columns == list(employee_status_rates_expected_schema)
        pl_testing.assert_frame_equal(returned_df, expected_df)

    def test_output_dtypes_match_declared_schema(self, tmp_path):
        csv_path = tmp_path / "employee_status_rates.csv"
        csv_path.write_text(
            "\n".join(
                [
                    EMPLOYEE_STATUS_RATES_CSV_HEADER,
                    EMPLOYEE_STATUS_RATES_TARGET_YEAR_ROW,
                ]
            )
            + "\n"
        )

        returned_df = job.load_employee_status_rates_lf(str(csv_path)).collect()

        assert returned_df.schema == pl.Schema(employee_status_rates_expected_schema)
