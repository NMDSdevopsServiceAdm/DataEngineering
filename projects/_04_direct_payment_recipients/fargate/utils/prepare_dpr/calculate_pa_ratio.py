import polars as pl

from projects._04_direct_payment_recipients.direct_payments_config import (
    DIRECT_PAYMENTS_MISSING_PA_RATIOS,
    DirectPaymentConfiguration as Config,
)
from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


def calculate_pa_ratio(survey_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Calculates the rolling average of staff per survey response for each year.

    Survey totals outside 1 to 9 staff (inclusive) are excluded, then averaged
    per year. Historic ratios fill years the survey does not have. Years are
    reduced by one to match external data, then averaged over a window of
    years (not rows), so gap years shrink the window.

    Args:
        survey_lf (pl.LazyFrame): Survey data with year and recoded
            total staff columns.

    Returns:
        pl.LazyFrame: A LazyFrame with columns 'year_as_integer' and
            'ratio_rolling_average'.
    """
    # Staff bounds for plausible survey responses.
    min_staff = 1.0
    max_staff = 9.0

    historic_ratio_lf = pl.LazyFrame(
        {
            DP.year_as_integer: list(DIRECT_PAYMENTS_MISSING_PA_RATIOS),
            DP.historic_ratio: list(DIRECT_PAYMENTS_MISSING_PA_RATIOS.values()),
        },
        schema={DP.year_as_integer: pl.Int32, DP.historic_ratio: pl.Float32},
    )

    survey_average_lf = (
        survey_lf.select(
            pl.col(DP.year).cast(pl.Int32).alias(DP.year_as_integer),
            DP.total_staff_recoded,
        )
        .filter(pl.col(DP.total_staff_recoded).is_between(min_staff, max_staff))
        .group_by(DP.year_as_integer)
        .agg(pl.col(DP.total_staff_recoded).mean().alias(DP.average_staff))
    )

    pa_ratio_lf = survey_average_lf.join(
        historic_ratio_lf, on=DP.year_as_integer, how="full", coalesce=True
    ).select(
        (pl.col(DP.year_as_integer) - 1).alias(DP.year_as_integer),
        pl.coalesce(DP.average_staff, DP.historic_ratio).alias(DP.average_staff),
    )

    return (
        pa_ratio_lf.sort(DP.year_as_integer)
        .with_columns(
            pl.col(DP.average_staff)
            .rolling_mean_by(
                DP.year_as_integer,
                window_size=f"{Config.NUMBER_OF_YEARS_ROLLING_AVERAGE}i",
                closed="right",
                min_samples=1,
            )
            .alias(DP.ratio_rolling_average)
        )
        .select(DP.year_as_integer, DP.ratio_rolling_average)
    )
