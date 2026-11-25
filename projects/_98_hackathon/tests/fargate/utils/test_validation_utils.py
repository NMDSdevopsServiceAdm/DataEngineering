from datetime import date

import polars as pl

import projects._98_hackathon.fargate.utils.validation_utils as vUtils
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.hackathon_columns import HackathonColumns as HackCols
from utils.column_names.hackathon_columns import (
    HackathonOutputColumns as OutCols,
)


class TestHasNoHiddenColumns:
    def test_returns_true_when_no_hidden_columns_present(self):
        column_names = [AWKClean.worker_id, HackCols.valid_for_analysis]

        assert vUtils.has_no_hidden_columns(column_names) is True

    def test_returns_false_when_is_leaver_present(self):
        column_names = [AWKClean.worker_id, HackCols.is_leaver]

        assert vUtils.has_no_hidden_columns(column_names) is False

    def test_returns_false_when_turnover_rate_present(self):
        column_names = [AWKClean.worker_id, HackCols.turnover_rate]

        assert vUtils.has_no_hidden_columns(column_names) is False


class TestHasOneImportDatePerMonth:
    @staticmethod
    def build_df(import_dates: list[date]) -> pl.DataFrame:
        return pl.DataFrame(
            {OutCols.snapshot_date: import_dates},
            schema={OutCols.snapshot_date: pl.Date},
        )

    def test_returns_true_when_one_import_date_per_month(self):
        df = self.build_df(
            [date(2024, 1, 1), date(2024, 1, 1), date(2024, 2, 1), date(2025, 1, 1)]
        )

        assert vUtils.has_one_import_date_per_month(df) is True

    def test_returns_false_when_two_import_dates_in_one_month(self):
        df = self.build_df([date(2024, 1, 1), date(2024, 1, 15), date(2025, 1, 1)])

        assert vUtils.has_one_import_date_per_month(df) is False
