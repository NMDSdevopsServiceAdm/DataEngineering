import polars as pl

from utils.column_names.direct_payments_column_names import (
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
            (pl.col(DP.la_area) == cornwall) | (pl.col(DP.la_area) == isles_of_scilly)
        )
        # polars_streaming: groupby-agg workaround; could be replaced with .over() grouped aggregations when window functions support streaming
        .group_by([DP.year_as_integer])
        .agg(
            pl.when(pl.col(DP.service_user_dprs_during_year).count() > 0).then(
                pl.sum(DP.service_user_dprs_during_year)
            ),
            pl.col(DP.proportion_employing_staff)
            .filter(pl.col(DP.la_area) == cornwall)
            .first()
            .alias(DP.proportion_employing_staff),
            pl.col(DP.historic_service_users_employing_staff_estimate)
            .filter(pl.col(DP.la_area) == cornwall)
            .first()
            .alias(DP.historic_service_users_employing_staff_estimate),
            pl.when(pl.col(DP.total_dprs_during_year).count() > 0).then(
                pl.sum(DP.total_dprs_during_year)
            ),
            pl.col(DP.filled_posts_per_employer)
            .filter(pl.col(DP.la_area) == cornwall)
            .first()
            .alias(DP.filled_posts_per_employer),
        )
        .with_columns(
            pl.lit(ContemporaryCSSR.cornwall_and_isles_of_scilly).alias(DP.la_area)
        )
        .select(DP.la_area, pl.all().exclude(DP.la_area))
    )

    lf = lf.filter(
        (pl.col(DP.la_area) != cornwall) & (pl.col(DP.la_area) != isles_of_scilly)
    )

    return pl.concat(
        [lf, merged_lf],
        how="vertical",
    )
