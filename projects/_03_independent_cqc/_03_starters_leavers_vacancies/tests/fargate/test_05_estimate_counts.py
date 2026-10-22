from datetime import date
from unittest.mock import Mock, call, patch

import polars as pl
import pytest

import projects._03_independent_cqc._03_starters_leavers_vacancies.fargate._05_estimate_counts as job
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from projects._03_independent_cqc._02_employment_status.fargate.utils.estimate_utils import (
    ESTIMATED_COUNT_COLUMNS,
    ESTIMATED_PERCENTAGE_COLUMNS,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    StartersLeaversVacanciesColumns as SLVCols,
)

PATCH_PATH = "projects._03_independent_cqc._03_starters_leavers_vacancies.fargate._05_estimate_counts"

SLV_ESTIMATE_SOURCE = "some/slv/source"
EMPLOYMENT_STATUS_ESTIMATE_SOURCE = "some/employment_status/source"
ESTIMATED_DATA_DESTINATION = "some/destination"

IMPORT_DATE = date(2025, 1, 1)
EMPLOYMENT_STATUS_COLUMNS = [
    *ESTIMATED_PERCENTAGE_COLUMNS,
    EmpStatus.estimated_percentage_source,
    *ESTIMATED_COUNT_COLUMNS,
]


def build_slv_lf(
    location_id: str = "1-001",
    starter_rate: float | None = 0.1,
    turnover_rate: float | None = 0.2,
    vacancy_rate: float | None = 0.5,
) -> pl.LazyFrame:
    return pl.LazyFrame(
        {
            IndCQC.published_job_role_label: ["role"],
            IndCQC.location_id: [location_id],
            IndCQC.cqc_location_import_date: [IMPORT_DATE],
            SLVCols.estimated_starter_rate: [starter_rate],
            SLVCols.estimated_turnover_rate: [turnover_rate],
            SLVCols.estimated_vacancy_rate: [vacancy_rate],
            SLVCols.estimated_starter_rate_source: ["source"],
            SLVCols.estimated_turnover_rate_source: ["source"],
            SLVCols.estimated_vacancy_rate_source: ["source"],
        }
    )


def build_employment_lf(
    location_id: str = "1-001", employees: float | None = 100.0
) -> pl.LazyFrame:
    return pl.LazyFrame(
        {
            IndCQC.published_job_role_label: ["role"],
            IndCQC.location_id: [location_id],
            IndCQC.cqc_location_import_date: [IMPORT_DATE],
            EmpStatus.estimated_employees: [employees],
            **{col: [1.0] for col in EMPLOYMENT_STATUS_COLUMNS},
        }
    )


def run_main(slv_lf: pl.LazyFrame, employment_lf: pl.LazyFrame) -> pl.DataFrame:
    with (
        patch(f"{PATCH_PATH}.utils.scan_parquet", side_effect=[employment_lf, slv_lf]),
        patch(f"{PATCH_PATH}.utils.sink_to_parquet") as sink_mock,
    ):
        job.main(
            SLV_ESTIMATE_SOURCE,
            EMPLOYMENT_STATUS_ESTIMATE_SOURCE,
            ESTIMATED_DATA_DESTINATION,
        )
    assert sink_mock.call_args.kwargs["output_path"] == ESTIMATED_DATA_DESTINATION
    return sink_mock.call_args.kwargs["lazy_df"].collect()


class TestMain:
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_scans_both_sources(
        self, scan_parquet_mock: Mock, sink_to_parquet_mock: Mock
    ):
        scan_parquet_mock.side_effect = [build_employment_lf(), build_slv_lf()]

        job.main(
            SLV_ESTIMATE_SOURCE,
            EMPLOYMENT_STATUS_ESTIMATE_SOURCE,
            ESTIMATED_DATA_DESTINATION,
        )

        assert scan_parquet_mock.call_args_list == [
            call(
                EMPLOYMENT_STATUS_ESTIMATE_SOURCE,
                selected_columns=job.employment_status_estimate_columns,
            ),
            call(SLV_ESTIMATE_SOURCE, selected_columns=job.slv_estimate_columns),
        ]
        sink_to_parquet_mock.assert_called_once()

    def test_keeps_unmatched_slv_rows_with_null_counts(self):
        result = run_main(build_slv_lf("1-001"), build_employment_lf("1-002"))

        assert result.height == 1
        assert result[SLVCols.estimated_starters][0] is None

    @pytest.mark.parametrize(
        "column, expected",
        [
            (SLVCols.estimated_starters, 10.0),
            (SLVCols.estimated_leavers, 20.0),
            (SLVCols.estimated_vacancies, 100.0),
        ],
    )
    def test_calculates_estimated_counts(self, column: str, expected: float):
        result = run_main(build_slv_lf(), build_employment_lf(employees=100.0))

        assert result[column][0] == pytest.approx(expected)

    def test_estimated_counts_are_null_when_rate_is_null(self):
        slv_lf = build_slv_lf(starter_rate=None, turnover_rate=None, vacancy_rate=None)

        result = run_main(slv_lf, build_employment_lf())

        assert result[SLVCols.estimated_starters][0] is None
        assert result[SLVCols.estimated_leavers][0] is None
        assert result[SLVCols.estimated_vacancies][0] is None
