from datetime import date

import polars as pl
import pytest

import experiments.rate_rolling_average.experiment_utils as x
from projects._03_independent_cqc.utils.model_evaluation_utils import (
    assign_location_folds,
)


def scoring_lf(rows: list[tuple[str, date, float, float | None, float | None]]):
    """Rows of (cell, date, workers, predicted, truth); one workplace per row."""
    return pl.LazyFrame(
        {
            x.LOCATION: [str(i) for i in range(len(rows))],
            x.SERVICE: ["service"] * len(rows),
            x.CSSR: [row[0] for row in rows],
            x.ROLE: ["role"] * len(rows),
            x.DATE: [row[1] for row in rows],
            x.WORKERS: [row[2] for row in rows],
            x.PREDICTED: [row[3] for row in rows],
            x.TRUTH: [row[4] for row in rows],
        },
        schema_overrides={
            x.WORKERS: pl.Float32,
            x.PREDICTED: pl.Float32,
            x.TRUTH: pl.Float32,
        },
    )


JAN = date(2024, 1, 1)


class TestScoreAggregate:
    def test_score_aggregate_error_weights_by_workers(self):
        lf = scoring_lf(
            [
                ("big", JAN, 100, 0.3, 0.2),
                ("small", JAN, 1, 0.7, 0.2),
            ]
        )

        scores = x.score_aggregate(lf)

        assert scores[x.AGGREGATE_ERROR] == pytest.approx(
            (100 * 0.1 + 1 * 0.5) / 101, abs=1e-5
        )

    def test_score_aggregate_error_compares_cell_rates_not_workplaces(self):
        lf = scoring_lf(
            [
                ("cell", JAN, 10, 0.5, 0.0),
                ("cell", JAN, 10, 0.0, 0.5),
            ]
        )

        scores = x.score_aggregate(lf)

        assert scores[x.AGGREGATE_ERROR] == pytest.approx(0, abs=1e-6)

    def test_score_aggregate_bias_is_signed(self):
        lf = scoring_lf([("a", JAN, 10, 0.1, 0.3)])

        scores = x.score_aggregate(lf)

        assert scores[x.AGGREGATE_BIAS] == pytest.approx(-0.2, abs=1e-6)

    def test_score_aggregate_ignores_rows_without_truth_or_workers(self):
        lf = scoring_lf(
            [
                ("a", JAN, 10, 0.2, 0.2),
                ("a", JAN, 10, 0.9, None),
                ("a", JAN, 0, 0.9, 0.0),
            ]
        )

        scores = x.score_aggregate(lf)

        assert scores[x.AGGREGATE_ERROR] == pytest.approx(0, abs=1e-6)

    def test_score_workplace_reports_mae_and_bias(self):
        lf = scoring_lf([("a", JAN, 10, 0.3, 0.2), ("a", JAN, 10, 0.1, 0.4)])

        scores = x.score_workplace(lf)

        assert scores[x.WORKPLACE_MAE] == pytest.approx(0.2, abs=1e-6)
        assert scores[x.WORKPLACE_BIAS] == pytest.approx(-0.1, abs=1e-6)

    def test_score_jumpiness_is_mean_change_between_dates(self):
        lf = scoring_lf(
            [
                ("a", JAN, 10, 0.1, None),
                ("a", date(2024, 2, 1), 10, 0.3, None),
                ("a", date(2024, 3, 1), 10, 0.2, None),
            ]
        )

        scores = x.score_jumpiness(lf)

        assert scores[x.JUMPINESS_AGGREGATE] == pytest.approx(0.15, abs=1e-6)


class TestHideLastMonths:
    def test_hide_last_months_nulls_rates_after_cutoff_per_location(self):
        lf = pl.LazyFrame(
            {
                x.LOCATION: ["a"] * 3 + ["b"] * 2,
                x.DATE: [
                    date(2023, 1, 1),
                    date(2023, 12, 1),
                    date(2024, 1, 1),
                    date(2022, 1, 1),
                    date(2022, 6, 1),
                ],
                x.RATE: [0.1, 0.2, 0.3, 0.4, 0.5],
            },
            schema_overrides={x.RATE: pl.Float32},
        )

        returned_df = x.hide_last_months(lf, months=3).collect()

        assert returned_df[x.HIDDEN].to_list() == [False, True, True, False, True]
        assert returned_df[x.RATE].to_list() == [
            pytest.approx(0.1),
            None,
            None,
            pytest.approx(0.4),
            None,
        ]
        assert returned_df[x.RATE_TRUTH].to_list() == pytest.approx(
            [0.1, 0.2, 0.3, 0.4, 0.5]
        )

    def test_hide_last_months_only_flags_rows_with_a_known_rate(self):
        lf = pl.LazyFrame(
            {
                x.LOCATION: ["a"] * 3,
                x.DATE: [date(2023, 1, 1), date(2023, 2, 1), date(2024, 1, 1)],
                x.RATE: [0.1, 0.2, None],
            },
            schema_overrides={x.RATE: pl.Float32},
        )

        returned_df = x.hide_last_months(lf, months=1).collect()

        assert returned_df[x.HIDDEN].to_list() == [False, True, False]


class TestHoldOutFold:
    def test_location_folds_hold_out_whole_locations(self):
        lf = pl.LazyFrame(
            {
                x.LOCATION: ["a", "a", "b", "b", "c", "c", "d", "d"],
                x.DATE: [JAN, date(2024, 2, 1)] * 4,
                x.TREND: [0.1] * 8,
            },
            schema_overrides={x.TREND: pl.Float32},
        )
        folded_lf = assign_location_folds(lf, x.LOCATION, n_folds=2, seed=1)

        returned_df = x.hold_out_fold(folded_lf, fold=0).collect()

        held_out_df = returned_df.filter(pl.col(x.FOLD) == 0)
        other_df = returned_df.filter(pl.col(x.FOLD) != 0)
        assert held_out_df[x.TREND].null_count() == held_out_df.height > 0
        assert other_df[x.TREND].null_count() == 0
        assert (
            returned_df.group_by(x.LOCATION)
            .agg(pl.col(x.FOLD).n_unique())[x.FOLD]
            .to_list()
            == [1] * 4
        )


def test_as_mean_of_rates_gives_every_contributor_one_worker():
    lf = pl.LazyFrame(
        {x.WORKERS: [100.0, 0.5, None, 5.0]}, schema_overrides={x.WORKERS: pl.Float32}
    )

    returned = x.as_mean_of_rates(lf).collect()[x.WORKERS].to_list()

    assert returned == [1.0, 0.5, None, 1.0]
