from unittest.mock import patch

import polars as pl
import pytest

import projects._03_independent_cqc._03_starters_leavers_vacancies.fargate._04_estimate as job
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    StartersLeaversVacanciesColumns as SLVCols,
)

PATCH_PATH = (
    "projects._03_independent_cqc._03_starters_leavers_vacancies.fargate._04_estimate"
)

VARIABLES = [
    (
        SLVCols.starter_rate,
        SLVCols.estimated_starter_rate,
        SLVCols.estimated_starter_rate_source,
        CatColType.StarterRateEstimateSourceEnumType,
    ),
    (
        SLVCols.turnover_rate,
        SLVCols.estimated_turnover_rate,
        SLVCols.estimated_turnover_rate_source,
        CatColType.TurnoverRateEstimateSourceEnumType,
    ),
    (
        SLVCols.vacancy_rate,
        SLVCols.estimated_vacancy_rate,
        SLVCols.estimated_vacancy_rate_source,
        CatColType.VacancyRateEstimateSourceEnumType,
    ),
]


class TestMain:
    IMPUTED_DATA_SOURCE = "some/source"
    ESTIMATED_DATA_DESTINATION = "some/destination"

    @pytest.fixture
    def imputed_lf(self) -> pl.LazyFrame:
        """Two rows: the first has every rate populated, the second has none."""
        return pl.LazyFrame(
            {
                IndCQC.location_id: ["1-001", "1-002"],
                SLVCols.starter_rate: [0.1, None],
                SLVCols.turnover_rate: [0.2, None],
                SLVCols.vacancy_rate: [0.3, None],
            },
            schema_overrides={
                SLVCols.starter_rate: pl.Float64,
                SLVCols.turnover_rate: pl.Float64,
                SLVCols.vacancy_rate: pl.Float64,
            },
        )

    @pytest.fixture
    def run_main(self, imputed_lf):
        """Runs main with mocked IO; returns (scan mock, sink mock)."""
        with (
            patch(
                f"{PATCH_PATH}.utils.scan_parquet", return_value=imputed_lf
            ) as scan_mock,
            patch(f"{PATCH_PATH}.utils.sink_to_parquet") as sink_mock,
        ):
            job.main(self.IMPUTED_DATA_SOURCE, self.ESTIMATED_DATA_DESTINATION)
        return scan_mock, sink_mock

    @pytest.fixture
    def result_df(self, run_main) -> pl.DataFrame:
        _, sink_mock = run_main
        return sink_mock.call_args.kwargs["lazy_df"].collect()

    def test_main_reads_source_and_writes_destination(self, run_main):
        scan_mock, sink_mock = run_main

        scan_mock.assert_called_once_with(self.IMPUTED_DATA_SOURCE)
        assert sink_mock.call_args.kwargs["output_path"] == (
            self.ESTIMATED_DATA_DESTINATION
        )

    @pytest.mark.parametrize("rate, estimated, source, source_dtype", VARIABLES)
    def test_main_estimated_rate_equals_input_rate_when_populated_and_null_otherwise(
        self, result_df, rate, estimated, source, source_dtype
    ):
        assert result_df[estimated].to_list() == [
            pytest.approx(result_df[rate][0]),
            None,
        ]

    def test_main_estimated_rates_are_float32(self, result_df):
        for _, estimated, _, _ in VARIABLES:
            assert result_df[estimated].dtype == pl.Float32

    @pytest.mark.parametrize("rate, estimated, source, source_dtype", VARIABLES)
    def test_main_source_is_input_column_name_when_populated_and_null_otherwise(
        self, result_df, rate, estimated, source, source_dtype
    ):
        assert result_df[source].cast(pl.String).to_list() == [rate, None]

    @pytest.mark.parametrize("rate, estimated, source, source_dtype", VARIABLES)
    def test_main_source_columns_are_enum_dtype(
        self, result_df, rate, estimated, source, source_dtype
    ):
        assert result_df[source].dtype == source_dtype

    def test_main_keeps_existing_columns_and_row_count(self, imputed_lf, result_df):
        assert set(imputed_lf.collect_schema().names()) <= set(result_df.columns)
        assert result_df.height == imputed_lf.collect().height
