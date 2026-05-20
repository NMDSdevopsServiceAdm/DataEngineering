import polars as pl

from projects._04_direct_payment_recipients.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)
from utils.column_values.categorical_column_values import ContemporaryCSSR


def merge_cornwall_and_isles_of_scilly(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Merge Cornwall and Isles of Scilly into one area.

    Both areas are merged into cornwall_and_isles_of_scilly whereby:
        - service user values are summed.
        - proportion of DPR employing staff, historic proportion and filled
          posts per employer are taken from Cornwall only.

    Args:
        lf (pl.LazyFrame): LazyFrame containing direct payments data with
            separate rows for Cornwall and Isles of Scilly.

    Returns:
        pl.LazyFrame: LazyFrame with Cornwall and Isles of Scilly merged into
            one area.
    """
    cornwall = "Cornwall"
    isles_of_scilly = "Isles of Scilly"

    merged_lf = (
        lf.filter(
            (pl.col(DP.LA_AREA) == cornwall) | (pl.col(DP.LA_AREA) == isles_of_scilly)
        )
        .group_by([DP.YEAR_AS_INTEGER])
        .agg(
            pl.when(pl.col(DP.SERVICE_USER_DPRS_DURING_YEAR).count() > 0).then(
                pl.sum(DP.SERVICE_USER_DPRS_DURING_YEAR)
            ),
            pl.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF)
            .filter(pl.col(DP.LA_AREA) == cornwall)
            .first()
            .alias(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF),
            pl.col(DP.HISTORIC_SERVICE_USERS_EMPLOYING_STAFF_ESTIMATE)
            .filter(pl.col(DP.LA_AREA) == cornwall)
            .first()
            .alias(DP.HISTORIC_SERVICE_USERS_EMPLOYING_STAFF_ESTIMATE),
            pl.when(pl.col(DP.TOTAL_DPRS_DURING_YEAR).count() > 0).then(
                pl.sum(DP.TOTAL_DPRS_DURING_YEAR)
            ),
            pl.col(DP.FILLED_POSTS_PER_EMPLOYER)
            .filter(pl.col(DP.LA_AREA) == cornwall)
            .first()
            .alias(DP.FILLED_POSTS_PER_EMPLOYER),
        )
        .with_columns(
            pl.lit(ContemporaryCSSR.cornwall_and_isles_of_scilly).alias(DP.LA_AREA)
        )
        .select(DP.LA_AREA, pl.all().exclude(DP.LA_AREA))
    )

    lf = lf.filter(
        (pl.col(DP.LA_AREA) != cornwall) & (pl.col(DP.LA_AREA) != isles_of_scilly)
    )

    return pl.concat(
        [lf, merged_lf],
        how="vertical",
    )
