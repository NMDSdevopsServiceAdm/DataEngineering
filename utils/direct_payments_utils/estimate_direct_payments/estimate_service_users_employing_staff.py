import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
    DirectPaymentColumnValues as Values,
)

from utils.direct_payments_utils.direct_payments_configuration import (
    DirectPaymentConfiguration as Config,
)
from utils.direct_payments_utils.estimate_direct_payments.models.extrapolation_ratio import (
    model_extrapolation,
)
from utils.direct_payments_utils.estimate_direct_payments.models.interpolation import (
    model_interpolation,
)
from utils.direct_payments_utils.estimate_direct_payments.models.mean_imputation import (
    model_using_mean,
)


def estimate_service_users_employing_staff(
    direct_payments_df: DataFrame,
) -> DataFrame:
    direct_payments_df = estimate_missing_data_for_service_users_employing_staff(
        direct_payments_df
    )
    direct_payments_df = calculate_estimated_number_of_service_users_employing_staff(
        direct_payments_df
    )

    return direct_payments_df


def calculate_estimated_number_of_service_users_employing_staff(
    direct_payments_df: DataFrame,
) -> DataFrame:
    direct_payments_df = direct_payments_df.withColumn(
        DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF,
        F.col(DP.SERVICE_USER_DPRS_DURING_YEAR)
        * F.col(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF),
    )
    return direct_payments_df


def estimate_missing_data_for_service_users_employing_staff(
    direct_payments_df: DataFrame,
) -> DataFrame:
    direct_payments_df = add_column_with_year_as_integer(direct_payments_df)

    direct_payments_df = model_using_mean(direct_payments_df)
    # TODO: Copy historic estimates into mean estimate column in preparation for extrapolation step

    direct_payments_df = model_extrapolation(direct_payments_df)

    direct_payments_df = model_interpolation(direct_payments_df)

    direct_payments_df = apply_models(direct_payments_df)
    # TODO: Interpolate remaining values for 2016 and 2017

    return direct_payments_df


def apply_models(direct_payments_df: DataFrame) -> DataFrame:
    direct_payments_df = direct_payments_df.withColumn(
        DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
        F.col(DP.ESTIMATE_USING_INTERPOLATION),
    )
    direct_payments_df = direct_payments_df.withColumn(
        DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
        F.when(
            F.col(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF).isNotNull(),
            F.col(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF),
        ).otherwise(F.col(DP.ESTIMATE_USING_EXTRAPOLATION_RATIO)),
    )
    direct_payments_df = direct_payments_df.withColumn(
        DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
        F.when(
            F.col(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF).isNotNull(),
            F.col(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF),
        ).otherwise(F.col(DP.ESTIMATE_USING_MEAN)),
    )
    return direct_payments_df


def add_column_with_year_as_integer(
    direct_payments_df: DataFrame,
) -> DataFrame:
    direct_payments_df = direct_payments_df.withColumn(
        DP.YEAR_AS_INTEGER,
        F.col(DP.YEAR).cast("int"),
    )
    return direct_payments_df
