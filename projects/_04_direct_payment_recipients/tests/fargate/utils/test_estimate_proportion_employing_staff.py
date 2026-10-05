import polars as pl
import pytest

import projects._04_direct_payment_recipients.fargate.utils.prepare_dpr_utils.estimate_proportion_employing_staff as job
from projects._04_direct_payment_recipients.direct_payments_config_polars import (
    DirectPaymentConfiguration as Config,
)
from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)

CARERS_PCT = Config.CARERS_EMPLOYING_PERCENTAGE

INPUT_SCHEMA = {
    DP.YEAR: pl.String,
    DP.DPRS_ADASS: pl.Float64,
    DP.DPRS_EMPLOYING_STAFF_ADASS: pl.Float64,
    DP.SERVICE_USER_DPRS_AT_YEAR_END: pl.Float64,
    DP.CARER_DPRS_AT_YEAR_END: pl.Float64,
    DP.PROPORTION_IMPORTED: pl.Float64,
}


def run(adass=150.0, employing=75.0, su=100.0, carers=50.0, imported=None) -> dict:
    """Runs one row through the function; defaults make total DPRs the closer base."""
    lf = pl.LazyFrame(
        [("2021", adass, employing, su, carers, imported)],
        schema=INPUT_SCHEMA,
        orient="row",
    )
    return job.estimate_proportion_employing_staff(lf).collect().row(0, named=True)


def test_year_is_cast_to_integer():
    returned_df = job.estimate_proportion_employing_staff(
        pl.LazyFrame(
            [("2021", 1.0, 1.0, 1.0, 1.0, None)], schema=INPUT_SCHEMA, orient="row"
        )
    ).collect()

    assert returned_df.schema[DP.YEAR_AS_INTEGER] == pl.Int32
    assert returned_df[DP.YEAR_AS_INTEGER].to_list() == [2021]


def test_proportion_is_employing_staff_over_dprs_adass():
    # Employing 30 of 120 ADASS DPRs is 0.25; SU only closer, so
    # (0.25 * SU + carers * pct) / SU.
    returned = run(adass=120.0, employing=30.0, su=110.0, carers=50.0)

    expected = (0.25 * 110.0 + 50.0 * CARERS_PCT) / 110.0
    assert returned[DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF] == pytest.approx(
        expected
    )


def test_total_dprs_is_service_users_plus_carers():
    # Proportion 0.5 with ADASS equal to SU + carers (150), so total is closer.
    returned = run(adass=150.0, employing=75.0, su=100.0, carers=50.0)

    assert returned[DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF] == pytest.approx(
        0.75
    )


@pytest.mark.parametrize(
    "adass, expected",
    [
        pytest.param(140.0, 0.75, id="total_closer"),
        pytest.param(110.0, 0.5 + 50.0 * CARERS_PCT / 100.0, id="su_closer"),
        pytest.param(125.0, 0.75, id="tie_chooses_total"),
    ],
)
def test_closer_base_is_total_su_or_total_on_tie_or_null(adass, expected):
    # SU 100 and total 150; proportion fixed at 0.5. Null differences also choose
    # total but the null propagates to the proportion, so they are not observable here.
    returned = run(adass=adass, employing=adass / 2, su=100.0, carers=50.0)

    assert returned[DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF] == pytest.approx(
        expected
    )


def test_proportion_if_total_closer():
    # 0.4 * 150 / 100
    returned = run(adass=150.0, employing=60.0, su=100.0, carers=50.0)

    assert returned[DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF] == pytest.approx(
        0.6
    )


def test_proportion_if_su_closer_includes_carers_at_fixed_percentage():
    # 0.4 proportion; (0.4 * 100 + 50 * pct) / 100
    returned = run(adass=100.0, employing=40.0, su=100.0, carers=50.0)

    expected = (0.4 * 100.0 + 50.0 * CARERS_PCT) / 100.0
    assert returned[DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF] == pytest.approx(
        expected
    )


@pytest.mark.parametrize(
    "adass, employing, su, carers, expected",
    [
        pytest.param(150.0, 75.0, 100.0, 50.0, 0.75, id="below_threshold_kept"),
        pytest.param(200.0, 100.0, 100.0, 100.0, 0.5 + CARERS_PCT, id="at_threshold"),
        pytest.param(
            150.0, 120.0, 100.0, 50.0, 0.8 + 0.5 * CARERS_PCT, id="above_threshold"
        ),
        pytest.param(150.0, None, 100.0, 50.0, None, id="null"),
    ],
)
def test_allocated_falls_back_to_su_formula_when_at_or_above_threshold_or_null(
    adass, employing, su, carers, expected
):
    # Total is the closer base in every case.
    returned = run(adass=adass, employing=employing, su=su, carers=carers)

    assert returned[DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF] == pytest.approx(
        expected
    )


def test_imported_proportion_takes_precedence_when_present():
    returned = run(imported=0.123)

    assert returned[DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF] == 0.123


def test_returns_input_columns_plus_proportion_and_year_as_integer():
    returned_df = job.estimate_proportion_employing_staff(
        pl.LazyFrame(
            [("2021", 1.0, 1.0, 1.0, 1.0, None)], schema=INPUT_SCHEMA, orient="row"
        )
    ).collect()

    assert returned_df.columns == [
        *INPUT_SCHEMA,
        DP.YEAR_AS_INTEGER,
        DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
    ]
