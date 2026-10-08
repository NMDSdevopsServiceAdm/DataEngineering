import polars as pl

from projects._04_direct_payment_recipients.direct_payments_config import (
    DirectPaymentConfiguration as Config,
)
from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


def remove_outliers(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Null unreliable proportions of service users employing staff.

    Removal rules are cumulative. The final retain rule overrides all of them.
    LA stats use the original values, including any removed by earlier rules.
    Uses `.over()` as there are only ~150 LAs.

    Args:
        lf (pl.LazyFrame): Data with one row per LA and year.

    Returns:
        pl.LazyFrame: Input with flagged proportions set to null.
    """
    value = pl.col(DP.proportion_employing_staff)
    year = pl.col(DP.year_as_integer)

    is_remove = (
        # Proportions must be between 0 and 1.
        (value < 0)
        | (value > 1)
        # An LA's only known value is implausibly extreme.
        | ((value.count().over(DP.la_area) == 1) & ((value > 0.85) | (value < 0.15)))
        # Value is too far from the LA mean.
        | (
            (value - value.mean().over(DP.la_area)).abs()
            >= Config.ADASS_PROPORTION_OUTLIER_THRESHOLD
        )
        # 2022 is extreme and jumps by more than 0.3 from 2021.
        | (
            (year == 2022)
            & ((value > 0.9) | (value < 0.1))
            & (
                (value - value.filter(year == 2021).first().over(DP.la_area)).abs()
                > 0.3
            )
        )
    ).fill_null(False)

    # Latest year with a known value is kept if it is plausible.
    is_retain = (
        (year == year.filter(value.is_not_null()).max().over(DP.la_area))
        & (value > 0.25)
        & (value < 0.75)
    ).fill_null(False)

    return lf.with_columns(
        pl.when(is_remove & ~is_retain)
        .then(None)
        .otherwise(value)
        .alias(value.meta.output_name())
    )
