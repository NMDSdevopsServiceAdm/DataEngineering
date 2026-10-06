import polars as pl

from polars_utils.utils import coalesce_with_source_labels
from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)
from projects._04_direct_payment_recipients.fargate.utils.estimate_dpr.calculate_rolling_mean import (
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
            - la_area
            - year_as_integer
            - service_user_dprs_during_year
            - proportion_employing_staff

    Returns:
        pl.LazyFrame: LazyFrame with additional columns:
            - estimate_using_mean
            - estimate_using_extrapolation_ratio
            - estimate_using_interpolation
            - imputed_proportion_employing_staff
            - imputed_proportion_employing_staff_source
            - rolling_average_proportion_employing_staff
            - estimated_service_users_employing_staff
    """

    lf = model_using_mean(lf)
    lf = lf.with_columns(
        pl.coalesce(
            [DP.estimate_using_mean, DP.historic_service_users_employing_staff_estimate]
        ).alias(DP.estimate_using_mean)
    )

    lf = model_extrapolation(lf)

    lf = model_interpolation(lf, DP.proportion_employing_staff)

    lf = lf.with_columns(
        coalesce_with_source_labels(
            cols=[
                DP.proportion_employing_staff,
                DP.estimate_using_extrapolation_ratio,
                DP.estimate_using_interpolation,
                DP.estimate_using_mean,
            ],
            name=DP.imputed_proportion_employing_staff,
        )
    )

    # Drop the interpolation column and re-run interpolation to fill any remaining nulls.
    # This is to populate year = 2014 where there are no values for proportion of service users employing staff.
    # Mean imputation populates 2013, proportions are known in 2015 and later, so interpolation is being used to estimate 2014.
    lf = lf.drop(DP.estimate_using_interpolation)
    lf = model_interpolation(lf, DP.imputed_proportion_employing_staff)

    source_update_exprs = (
        pl.when(
            (
                pl.col(DP.imputed_proportion_employing_staff).is_null()
                & pl.col(DP.estimate_using_interpolation).is_not_null()
            )
        )
        .then(pl.lit(DP.estimate_using_interpolation))
        .otherwise(pl.col(DP.imputed_proportion_employing_staff_source))
        .alias(DP.imputed_proportion_employing_staff_source)
    )
    estimate_update_expr = pl.coalesce(
        [
            DP.imputed_proportion_employing_staff,
            DP.estimate_using_interpolation,
        ]
    ).alias(DP.imputed_proportion_employing_staff)
    lf = lf.with_columns(estimate_update_expr, source_update_exprs)

    lf = calculate_rolling_mean(lf)

    lf = lf.with_columns(
        (
            pl.col(DP.service_user_dprs_during_year)
            * pl.col(DP.rolling_average_proportion_employing_staff)
        ).alias(DP.estimated_service_users_employing_staff)
    )

    return lf
