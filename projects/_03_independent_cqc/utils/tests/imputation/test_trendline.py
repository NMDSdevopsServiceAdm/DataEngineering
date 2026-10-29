from datetime import date

import polars as pl
import pytest

import projects._03_independent_cqc.utils.imputation.trendline as job
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

KNOWN = "known_rate"
TREND = "known_rate_imputed_trendline"
GROUPS = [IndCQC.location_id]
EXTRAPOLATION_PERIOD = "12mo"
INTERPOLATION_CAP_PERIOD = "5y"


def fill(dates: list[date], rates: list[float | None], location_id: str = "1-001"):
    lf = pl.LazyFrame(
        {
            IndCQC.location_id: [location_id] * len(dates),
            IndCQC.cqc_location_import_date: dates,
            KNOWN: rates,
        },
        schema_overrides={KNOWN: pl.Float32},
    )
    returned_df = job.add_imputed_trendline(
        lf,
        known_columns=[KNOWN],
        trendline_columns=[TREND],
        group_columns=GROUPS,
        extrapolation_period=EXTRAPOLATION_PERIOD,
        interpolation_cap_period=INTERPOLATION_CAP_PERIOD,
    ).collect()
    return returned_df.sort(IndCQC.cqc_location_import_date)[TREND].to_list()


def test_fill_carries_last_rate_forward_within_cap():
    dates = [date(2024, 1, 1), date(2024, 7, 1), date(2025, 1, 1)]

    returned = fill(dates, [0.2, None, None])

    assert returned == pytest.approx([0.2, 0.2, 0.2])


def test_fill_carries_first_rate_backward_within_cap():
    dates = [date(2024, 1, 1), date(2024, 7, 1), date(2025, 1, 1)]

    returned = fill(dates, [None, None, 0.2])

    assert returned == pytest.approx([0.2, 0.2, 0.2])


def test_fill_leaves_null_beyond_cap():
    dates = [date(2024, 1, 1), date(2025, 1, 1), date(2025, 2, 1)]

    returned = fill(dates, [0.2, None, None])

    assert returned == pytest.approx([0.2, 0.2, None])


def test_fill_interpolates_between_known_rates():
    dates = [date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)]

    returned = fill(dates, [0.1, None, 0.3])

    assert returned[1] == pytest.approx(0.2, abs=0.01)


def test_fill_leaves_gap_wider_than_interpolation_cap_null():
    dates = [date(2018, 1, 1), date(2021, 1, 1), date(2024, 1, 1)]

    returned = fill(dates, [0.1, None, 0.3])

    assert returned[1] is None


def test_fill_keeps_known_rates():
    dates = [date(2024, 1, 1), date(2024, 2, 1)]

    returned = fill(dates, [0.1, 0.4])

    assert returned == pytest.approx([0.1, 0.4])


def test_fill_does_not_cross_groups():
    lf = pl.LazyFrame(
        {
            IndCQC.location_id: ["1-001", "1-002"],
            IndCQC.cqc_location_import_date: [date(2024, 1, 1), date(2024, 2, 1)],
            KNOWN: [0.2, None],
        },
        schema_overrides={KNOWN: pl.Float32},
    )

    returned_df = job.add_imputed_trendline(
        lf,
        known_columns=[KNOWN],
        trendline_columns=[TREND],
        group_columns=GROUPS,
        extrapolation_period=EXTRAPOLATION_PERIOD,
        interpolation_cap_period=INTERPOLATION_CAP_PERIOD,
    ).collect()

    assert returned_df.sort(IndCQC.location_id)[TREND].to_list() == [
        pytest.approx(0.2),
        None,
    ]


def test_fill_returns_float32_and_only_adds_trendline_columns():
    lf = pl.LazyFrame(
        {
            IndCQC.location_id: ["1-001"],
            IndCQC.cqc_location_import_date: [date(2024, 1, 1)],
            KNOWN: [0.2],
        },
        schema_overrides={KNOWN: pl.Float32},
    )

    returned_lf = job.add_imputed_trendline(
        lf,
        known_columns=[KNOWN],
        trendline_columns=[TREND],
        group_columns=GROUPS,
        extrapolation_period=EXTRAPOLATION_PERIOD,
        interpolation_cap_period=INTERPOLATION_CAP_PERIOD,
    )

    schema = returned_lf.collect_schema()
    assert schema.names() == [*lf.collect_schema().names(), TREND]
    assert schema[TREND] == pl.Float32
