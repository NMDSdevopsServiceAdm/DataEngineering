import polars as pl

from projects._04_direct_payment_recipients.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)
from projects._04_direct_payment_recipients.fargate.utils.models.interpolation import (
    model_interpolation,
)
from projects._04_direct_payment_recipients.fargate.utils.models.mean_imputation import (
    model_using_mean,
)


def calculate_estimated_service_users_employing_staff(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Doc string here
    """

    lf = lf.with_columns(
        pl.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF).alias(
            DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
        )
    )

    # lf = expolation_function(lf)

    lf = model_interpolation(lf)

    lf = lf.with_columns(
        pl.coalesce(
            [
                DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
                DP.ESTIMATE_USING_EXTRAPOLATION_RATIO,
                DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
            ]
        ).alias(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF)
    )

    lf = model_using_mean(lf)

    lf = lf.with_columns(
        pl.coalesce(
            [DP.ESTIMATE_USING_MEAN, DP.HISTORIC_SERVICE_USERS_EMPLOYING_STAFF_ESTIMATE]
        ).alias(DP.ESTIMATE_USING_MEAN)
    )

    lf = lf.with_columns(
        pl.coalesce(
            [
                DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
                DP.ESTIMATE_USING_MEAN,
            ]
        ).alias(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF)
    )

    # Why does the interpolation happen again after the mean is applied?
    # Area with 1 known value gets extrpolated, area with more than 1 gets interpolated, then no known values gets the mean.

    return lf
