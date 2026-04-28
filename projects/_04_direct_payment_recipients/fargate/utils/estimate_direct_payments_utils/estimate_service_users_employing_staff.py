import polars as pl

from polars_utils.utils import coalesce_with_source_labels
from projects._04_direct_payment_recipients.direct_payments_column_names import (
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
    Doc string here
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

    lf = lf.drop(DP.ESTIMATE_USING_INTERPOLATION)
    lf = model_interpolation(
        lf, DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
    )

    lf = lf.with_columns(
        pl.coalesce(
            [
                DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
                DP.ESTIMATE_USING_INTERPOLATION,
            ]
        ).alias(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF)
    ).with_columns(
        pl.when(
            pl.col(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF).is_null()
        )
        .then(pl.lit(DP.ESTIMATE_USING_INTERPOLATION))
        .otherwise(
            pl.col("estimated_proportion_of_service_users_employing_staff_source")
        )
        .alias("estimated_proportion_of_service_users_employing_staff_source")
    )

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
