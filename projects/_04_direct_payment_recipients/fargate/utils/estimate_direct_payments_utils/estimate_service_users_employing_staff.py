import polars as pl

from polars_utils.utils import coalesce_with_source_labels
from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)
from projects._04_direct_payment_recipients.fargate.utils.estimate_direct_payments_utils.calculate_rolling_mean import (
    calculate_rolling_mean,
)
from projects._04_direct_payment_recipients.fargate.utils.models.extrapolation_ratio import (
    model_extrapolation,
)
from projects._04_direct_payment_recipients.fargate.utils.models.interpolation import (
    model_interpolation,
)
from projects._04_direct_payment_recipients.fargate.utils.models.mean_imputation import (
    model_using_mean,
)


def calculate_estimated_service_users_employing_staff(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Calculate estimated service users employing staff using a hierarchy of
    methods:
        1. Use known proportions where available.
        2. Use extrapolated and interpolated proportions from known proportions.
        4. Use mean imputation to estimate any remaining missing proportions.
        5. Use interpolation to estimate any remaining missing proportions after
           mean imputation, as mean imputation creates new known data points
           which can be used for interpolation.
        6. Calculate rolling average to smooth final estimates.
        7. Calculate estimated service users employing staff by applying the rolling
           average proportions to the count of service user DPRS during the year.

    Args:
        lf (pl.LazyFrame): LazyFrame containing direct payments data with columns:
            - LA_AREA
            - YEAR_AS_INTEGER
            - SERVICE_USER_DPRS_DURING_YEAR
            - PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF

    Returns:
        pl.LazyFrame: LazyFrame with additional columns:
            - ESTIMATE_USING_MEAN
            - ESTIMATE_USING_EXTRAPOLATION_RATIO
            - ESTIMATE_USING_INTERPOLATION
            - ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            - ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_SOURCE
            - ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            - ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF
    """

    lf = model_using_mean(lf)
    lf = lf.with_columns(
        pl.coalesce(
            [DP.ESTIMATE_USING_MEAN, DP.HISTORIC_SERVICE_USERS_EMPLOYING_STAFF_ESTIMATE]
        ).alias(DP.ESTIMATE_USING_MEAN)
    )

    lf = model_extrapolation(lf)

    lf = model_interpolation(lf, DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF)

    lf = lf.with_columns(
        coalesce_with_source_labels(
            cols=[
                DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
                DP.ESTIMATE_USING_EXTRAPOLATION_RATIO,
                DP.ESTIMATE_USING_INTERPOLATION,
                DP.ESTIMATE_USING_MEAN,
            ],
            name=DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
        )
    )

    # Drop the interpolation column and re-run interpolation to fill any remaining nulls.
    # This is to populate year = 2014 where there are no values for proportion of service users employing staff.
    # Mean imputation populates 2013, proportions are known in 2015 and later, so interpolation is being used to estimate 2014.
    lf = lf.drop(DP.ESTIMATE_USING_INTERPOLATION)
    lf = model_interpolation(
        lf, DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
    )

    source_update_exprs = (
        pl.when(
            (
                pl.col(
                    DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
                ).is_null()
                & pl.col(DP.ESTIMATE_USING_INTERPOLATION).is_not_null()
            )
        )
        .then(pl.lit(DP.ESTIMATE_USING_INTERPOLATION))
        .otherwise(
            pl.col(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_SOURCE)
        )
        .alias(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_SOURCE)
    )
    estimate_update_expr = pl.coalesce(
        [
            DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
            DP.ESTIMATE_USING_INTERPOLATION,
        ]
    ).alias(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF)
    lf = lf.with_columns(estimate_update_expr, source_update_exprs)

    lf = calculate_rolling_mean(lf)

    lf = lf.with_columns(
        (
            pl.col(DP.SERVICE_USER_DPRS_DURING_YEAR)
            * pl.col(
                DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            )
        ).alias(DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF)
    )

    return lf
