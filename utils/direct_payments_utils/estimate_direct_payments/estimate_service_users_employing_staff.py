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

    direct_payments_df = calculate_known_values_for_service_users_employing_staff(direct_payments_df)

    direct_payments_df = estimate_missing_data_for_service_users_employing_staff(direct_payments_df)
    return direct_payments_df


def calculate_known_values_for_service_users_employing_staff(
    direct_payments_df: DataFrame,
) -> DataFrame:
    direct_payments_df = direct_payments_df.withColumn(
        DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF,
        F.col(DP.SERVICE_USER_DPRS_DURING_YEAR) * F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF),
    )
    return direct_payments_df


def estimate_missing_data_for_service_users_employing_staff(
    direct_payments_df: DataFrame,
) -> DataFrame:

    # If we have never known the percentage, use the mean model
    # mean model
    # Calculate mean %
    # Calculate mean % * SALT
    # Save in column
    direct_payments_df = model_using_mean(direct_payments_df)

    # If we have gaps at the beginning, ratio back from last known year
    # ratio model
    # calculate ratio backwards from each year
    direct_payments_df = model_extrapolation(direct_payments_df)

    # If we have gaps between dates, interpolate
    # interpolation model
    # calculate even points on line - may want to use Gary's code for this
    direct_payments_df = model_interpolation(direct_payments_df)

    # select which column to use
    direct_payments_df = apply_models(direct_payments_df)

    return direct_payments_df


def apply_models(direct_payments_df: DataFrame) -> DataFrame:
    direct_payments_df = direct_payments_df.withColumn(
        DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, F.col(DP.ESTIMATE_USING_INTERPOLATION)
    )
    direct_payments_df = direct_payments_df.withColumn(
        DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
        F.when(
            F.col(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF).isNull(),
            F.col(DP.ESTIMATE_USING_EXTRAPOLATION_RATIO),
        ).otherwise(F.col(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF)),
    )
    direct_payments_df = direct_payments_df.withColumn(
        DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
        F.when(
            F.col(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF).isNull(), F.col(DP.ESTIMATE_USING_MEAN)
        ).otherwise(F.col(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF)),
    )
    return direct_payments_df
