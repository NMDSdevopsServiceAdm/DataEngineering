from pyspark.sql import DataFrame, functions as F

from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)
from projects._04_direct_payment_recipients.utils._03_estimate_direct_payment_utils.models.extrapolation_ratio import (
    model_extrapolation,
)
from projects._04_direct_payment_recipients.utils._03_estimate_direct_payment_utils.models.interpolation import (
    model_interpolation,
)
from projects._04_direct_payment_recipients.utils._03_estimate_direct_payment_utils.models.mean_imputation import (
    model_using_mean,
)
from projects._04_direct_payment_recipients.utils._03_estimate_direct_payment_utils.apply_rolling_average import (
    apply_rolling_average,
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
        * F.col(
            DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
        ),
    )
    return direct_payments_df


def estimate_missing_data_for_service_users_employing_staff(
    direct_payments_df: DataFrame,
) -> DataFrame:
    direct_payments_df = model_using_mean(direct_payments_df)
    direct_payments_df = merge_in_historical_estimates_with_estimate_using_mean(
        direct_payments_df
    )
    direct_payments_df = apply_known_values(direct_payments_df)
    direct_payments_df = model_extrapolation(direct_payments_df)
    direct_payments_df = apply_extrapolated_values(direct_payments_df)
    direct_payments_df = model_interpolation(direct_payments_df)
    direct_payments_df = apply_interpolated_values(direct_payments_df)
    direct_payments_df = direct_payments_df.drop(DP.ESTIMATE_USING_INTERPOLATION)
    direct_payments_df = apply_mean_estimates(direct_payments_df)
    direct_payments_df = model_interpolation(direct_payments_df)
    direct_payments_df = apply_interpolated_values(direct_payments_df)
    direct_payments_df = apply_rolling_average(direct_payments_df)
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


def merge_in_historical_estimates_with_estimate_using_mean(
    direct_payments_df: DataFrame,
) -> DataFrame:
    direct_payments_df = direct_payments_df.withColumn(
        DP.ESTIMATE_USING_MEAN,
        F.when(
            F.col(DP.ESTIMATE_USING_MEAN).isNotNull(), F.col(DP.ESTIMATE_USING_MEAN)
        ).otherwise(F.col(DP.HISTORIC_SERVICE_USERS_EMPLOYING_STAFF_ESTIMATE)),
    )
    return direct_payments_df


def apply_known_values(direct_payments_df: DataFrame) -> DataFrame:
    direct_payments_df = direct_payments_df.withColumn(
        DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
        F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF),
    )
    return direct_payments_df


def apply_extrapolated_values(direct_payments_df: DataFrame) -> DataFrame:
    direct_payments_df = direct_payments_df.withColumn(
        DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
        F.when(
            F.col(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF).isNotNull(),
            F.col(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF),
        ).otherwise(F.col(DP.ESTIMATE_USING_EXTRAPOLATION_RATIO)),
    )
    return direct_payments_df


def apply_interpolated_values(direct_payments_df: DataFrame) -> DataFrame:
    direct_payments_df = direct_payments_df.withColumn(
        DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
        F.when(
            F.col(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF).isNotNull(),
            F.col(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF),
        ).otherwise(F.col(DP.ESTIMATE_USING_INTERPOLATION)),
    )
    return direct_payments_df


def apply_mean_estimates(direct_payments_df: DataFrame) -> DataFrame:
    direct_payments_df = direct_payments_df.withColumn(
        DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
        F.when(
            F.col(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF).isNotNull(),
            F.col(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF),
        ).otherwise(F.col(DP.ESTIMATE_USING_MEAN)),
    )
    return direct_payments_df
