from datetime import date
from unittest.mock import Mock, patch

import polars as pl
import pytest

import projects._03_independent_cqc._03_starters_leavers_vacancies.fargate._03_impute as job
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    StartersLeaversVacanciesColumns as SLVCols,
)

PATCH_PATH = (
    "projects._03_independent_cqc._03_starters_leavers_vacancies.fargate._03_impute"
)


class TestMain:
    CLEANED_DATA_SOURCE = "some/source"
    IMPUTED_DATA_DESTINATION = "some/destination"

    @pytest.fixture
    def cleaned_lf(self) -> pl.LazyFrame:
        return pl.LazyFrame(
            {
                IndCQC.location_id: ["1-001"] * 3 + ["1-002"],
                IndCQC.published_job_role_label: ["role"] * 4,
                IndCQC.primary_service_type: ["service"] * 4,
                IndCQC.current_region: ["region"] * 4,
                IndCQC.cqc_location_import_date: [
                    date(2024, 1, 1),
                    date(2024, 2, 1),
                    date(2024, 3, 1),
                    date(2024, 1, 1),
                ],
                EmpStatus.estimated_employees: [10.0, 10.0, 10.0, 20.0],
                SLVCols.ascwds_turnover_rate: [0.2, None, None, 0.1],
            },
            schema_overrides={
                EmpStatus.estimated_employees: pl.Float32,
                SLVCols.ascwds_turnover_rate: pl.Float32,
            },
        )

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_impute_main_adds_imputed_rate_and_keeps_row_count(
        self,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
        cleaned_lf: pl.LazyFrame,
    ):
        scan_parquet_mock.return_value = cleaned_lf

        job.main(self.CLEANED_DATA_SOURCE, self.IMPUTED_DATA_DESTINATION)

        scan_parquet_mock.assert_called_once_with(self.CLEANED_DATA_SOURCE)
        sink_to_parquet_mock.assert_called_once()
        assert (
            sink_to_parquet_mock.call_args.kwargs["output_path"]
            == self.IMPUTED_DATA_DESTINATION
        )

        returned_df = sink_to_parquet_mock.call_args.kwargs["lazy_df"].collect()
        assert returned_df.height == cleaned_lf.collect().height
        assert {
            SLVCols.ascwds_turnover_rate_imputed_trendline,
            SLVCols.ascwds_turnover_rate_rolling_avg,
            SLVCols.ascwds_turnover_rate_imputed,
        }.issubset(returned_df.columns)
        assert returned_df[SLVCols.ascwds_turnover_rate_imputed].null_count() == 0
