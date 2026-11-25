from datetime import date

import polars as pl

import projects._98_hackathon.fargate.utils.validation_utils as vUtils
from utils.column_names.hackathon_columns import HackathonOutputColumns as OutCols


class TestHasNoHiddenColumns:
    def test_returns_true_when_no_hidden_columns_present(self):
        column_names = [OutCols.workerid, OutCols.age]

        assert vUtils.has_no_hidden_columns(column_names) is True

    def test_returns_false_when_is_leaver_present(self):
        column_names = [OutCols.workerid, OutCols.is_leaver]

        assert vUtils.has_no_hidden_columns(column_names) is False

    def test_returns_false_when_turnover_rate_present(self):
        column_names = [OutCols.workerid, OutCols.turnover_rate]

        assert vUtils.has_no_hidden_columns(column_names) is False


class TestHasOneSnapshotDatePerMonth:
    @staticmethod
    def build_df(snapshot_dates: list[date]) -> pl.DataFrame:
        return pl.DataFrame(
            {OutCols.snapshot_date: snapshot_dates},
            schema={OutCols.snapshot_date: pl.Date},
        )

    def test_returns_true_when_one_snapshot_date_per_month(self):
        df = self.build_df(
            [date(2024, 1, 1), date(2024, 1, 1), date(2024, 2, 1), date(2025, 1, 1)]
        )

        assert vUtils.has_one_snapshot_date_per_month(df) is True

    def test_returns_false_when_two_snapshot_dates_in_one_month(self):
        df = self.build_df([date(2024, 1, 1), date(2024, 1, 15), date(2025, 1, 1)])

        assert vUtils.has_one_snapshot_date_per_month(df) is False
