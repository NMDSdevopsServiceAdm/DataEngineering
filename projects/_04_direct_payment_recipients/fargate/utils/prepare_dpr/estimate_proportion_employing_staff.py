import polars as pl

from projects._04_direct_payment_recipients.direct_payments_config import (
    DirectPaymentConfiguration as Config,
)
from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)
from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnValues as Values,
)


def estimate_proportion_employing_staff(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Estimates the proportion of service users employing staff from ADASS counts.

    The ADASS proportion of DPRs employing staff is scaled to whichever base is
    closer to the ADASS DPR count: total DPRs (service users plus carers) or
    service-user-only DPRs. Ties and nulls choose total DPRs. Proportions
    reaching the threshold, or null, fall back to the service-user-only
    formula. An imported proportion takes precedence when present.

    Args:
        lf (pl.LazyFrame): DPR data with year, ADASS DPR counts, service user
            and carer DPRs at year end, and the imported proportion.

    Returns:
        pl.LazyFrame: The input columns plus 'year_as_integer' and
            'proportion_employing_staff'.
    """
    input_columns = lf.collect_schema().names()

    difference_to_total = (
        pl.col(DP.dprs_adass) - pl.col(DP.total_dprs_at_year_end)
    ).abs()
    difference_to_su_only = (
        pl.col(DP.dprs_adass) - pl.col(DP.service_user_dprs_at_year_end)
    ).abs()

    lf = lf.with_columns(
        pl.col(DP.year).cast(pl.Int32).alias(DP.year_as_integer),
        (pl.col(DP.dprs_employing_staff_adass) / pl.col(DP.dprs_adass)).alias(
            DP.proportion_dpr_employing_staff
        ),
        (
            pl.col(DP.service_user_dprs_at_year_end) + pl.col(DP.carer_dprs_at_year_end)
        ).alias(DP.total_dprs_at_year_end),
    )

    lf = lf.with_columns(
        pl.when(difference_to_total < difference_to_su_only)
        .then(pl.lit(Values.total_dprs))
        .when(difference_to_total > difference_to_su_only)
        .then(pl.lit(Values.su_only_dprs))
        .otherwise(pl.lit(Values.total_dprs))
        .alias(DP.closer_base),
        (
            pl.col(DP.proportion_dpr_employing_staff)
            * pl.col(DP.total_dprs_at_year_end)
            / pl.col(DP.service_user_dprs_at_year_end)
        ).alias(DP.proportion_if_total_dpr_closer),
        (
            (
                pl.col(DP.proportion_dpr_employing_staff)
                * pl.col(DP.service_user_dprs_at_year_end)
                + pl.col(DP.carer_dprs_at_year_end) * Config.CARERS_EMPLOYING_PERCENTAGE
            )
            / pl.col(DP.service_user_dprs_at_year_end)
        ).alias(DP.proportion_if_service_user_dpr_closer),
    )

    lf = lf.with_columns(
        pl.when(pl.col(DP.closer_base) == Values.su_only_dprs)
        .then(pl.col(DP.proportion_if_service_user_dpr_closer))
        .otherwise(pl.col(DP.proportion_if_total_dpr_closer))
        .alias(DP.proportion_allocated)
    )

    lf = lf.with_columns(
        pl.when(
            pl.col(DP.proportion_allocated)
            < Config.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_THRESHOLD
        )
        .then(pl.col(DP.proportion_allocated))
        .otherwise(pl.col(DP.proportion_if_service_user_dpr_closer))
        .alias(DP.proportion_allocated)
    )

    return lf.select(
        *input_columns,
        DP.year_as_integer,
        pl.coalesce(DP.proportion_imported, DP.proportion_allocated).alias(
            DP.proportion_employing_staff
        ),
    )
