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
        pl.LazyFrame: A LazyFrame with columns 'YEAR_AS_INTEGER' and
            'RATIO_ROLLING_AVERAGE'.
    """
    # Staff bounds for plausible survey responses.
    min_staff = 1.0
    max_staff = 9.0

    historic_ratio_lf = pl.LazyFrame(
        {
            DP.YEAR_AS_INTEGER: list(DIRECT_PAYMENTS_MISSING_PA_RATIOS),
            DP.HISTORIC_RATIO: list(DIRECT_PAYMENTS_MISSING_PA_RATIOS.values()),
        },
        schema={DP.YEAR_AS_INTEGER: pl.Int32, DP.HISTORIC_RATIO: pl.Float64},
    )

    survey_average_lf = (
        survey_lf.select(
            pl.col(DP.YEAR).cast(pl.Int32).alias(DP.YEAR_AS_INTEGER),
            DP.TOTAL_STAFF_RECODED,
        )
        .filter(pl.col(DP.TOTAL_STAFF_RECODED).is_between(min_staff, max_staff))
        .group_by(DP.YEAR_AS_INTEGER)
        .agg(pl.col(DP.TOTAL_STAFF_RECODED).mean().alias(DP.AVERAGE_STAFF))
    )

    pa_ratio_lf = survey_average_lf.join(
        historic_ratio_lf, on=DP.YEAR_AS_INTEGER, how="full", coalesce=True
    ).select(
        (pl.col(DP.YEAR_AS_INTEGER) - 1).alias(DP.YEAR_AS_INTEGER),
        pl.coalesce(DP.AVERAGE_STAFF, DP.HISTORIC_RATIO).alias(DP.AVERAGE_STAFF),
    )

    return (
        pa_ratio_lf.sort(DP.YEAR_AS_INTEGER)
        .with_columns(
            pl.col(DP.AVERAGE_STAFF)
            .rolling_mean_by(
                DP.YEAR_AS_INTEGER,
                window_size=f"{Config.NUMBER_OF_YEARS_ROLLING_AVERAGE}i",
                closed="right",
                min_samples=1,
            )
            .alias(DP.RATIO_ROLLING_AVERAGE)
        )
        .select(DP.YEAR_AS_INTEGER, DP.RATIO_ROLLING_AVERAGE)
    )
