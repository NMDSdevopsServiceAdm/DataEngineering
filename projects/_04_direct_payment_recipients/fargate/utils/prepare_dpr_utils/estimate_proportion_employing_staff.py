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
            'proportion_of_service_users_employing_staff'.
    """
    input_columns = lf.collect_schema().names()

    difference_to_total = (
        pl.col(DP.DPRS_ADASS) - pl.col(DP.TOTAL_DPRS_AT_YEAR_END)
    ).abs()
    difference_to_su_only = (
        pl.col(DP.DPRS_ADASS) - pl.col(DP.SERVICE_USER_DPRS_AT_YEAR_END)
    ).abs()

    lf = lf.with_columns(
        pl.col(DP.YEAR).cast(pl.Int32).alias(DP.YEAR_AS_INTEGER),
        (pl.col(DP.DPRS_EMPLOYING_STAFF_ADASS) / pl.col(DP.DPRS_ADASS)).alias(
            DP.PROPORTION_OF_DPR_EMPLOYING_STAFF
        ),
        (
            pl.col(DP.SERVICE_USER_DPRS_AT_YEAR_END) + pl.col(DP.CARER_DPRS_AT_YEAR_END)
        ).alias(DP.TOTAL_DPRS_AT_YEAR_END),
    )

    lf = lf.with_columns(
        pl.when(difference_to_total < difference_to_su_only)
        .then(pl.lit(Values.TOTAL_DPRS))
        .when(difference_to_total > difference_to_su_only)
        .then(pl.lit(Values.SU_ONLY_DPRS))
        .otherwise(pl.lit(Values.TOTAL_DPRS))
        .alias(DP.CLOSER_BASE),
        (
            pl.col(DP.PROPORTION_OF_DPR_EMPLOYING_STAFF)
            * pl.col(DP.TOTAL_DPRS_AT_YEAR_END)
            / pl.col(DP.SERVICE_USER_DPRS_AT_YEAR_END)
        ).alias(DP.PROPORTION_IF_TOTAL_DPR_CLOSER),
        (
            (
                pl.col(DP.PROPORTION_OF_DPR_EMPLOYING_STAFF)
                * pl.col(DP.SERVICE_USER_DPRS_AT_YEAR_END)
                + pl.col(DP.CARER_DPRS_AT_YEAR_END) * Config.CARERS_EMPLOYING_PERCENTAGE
            )
            / pl.col(DP.SERVICE_USER_DPRS_AT_YEAR_END)
        ).alias(DP.PROPORTION_IF_SERVICE_USER_DPR_CLOSER),
    )

    lf = lf.with_columns(
        pl.when(pl.col(DP.CLOSER_BASE) == Values.SU_ONLY_DPRS)
        .then(pl.col(DP.PROPORTION_IF_SERVICE_USER_DPR_CLOSER))
        .otherwise(pl.col(DP.PROPORTION_IF_TOTAL_DPR_CLOSER))
        .alias(DP.PROPORTION_ALLOCATED)
    )

    lf = lf.with_columns(
        pl.when(
            pl.col(DP.PROPORTION_ALLOCATED)
            < Config.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_THRESHOLD
        )
        .then(pl.col(DP.PROPORTION_ALLOCATED))
        .otherwise(pl.col(DP.PROPORTION_IF_SERVICE_USER_DPR_CLOSER))
        .alias(DP.PROPORTION_ALLOCATED)
    )

    return lf.select(
        *input_columns,
        DP.YEAR_AS_INTEGER,
        pl.coalesce(DP.PROPORTION_IMPORTED, DP.PROPORTION_ALLOCATED).alias(
            DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
        ),
    )
