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
    direct_payments_df = estimate_missing_data_for_service_users_employing_staff(direct_payments_df)
    direct_payments_df = calculate_estimated_number_of_service_users_employing_staff(direct_payments_df)
    # TODO: remove rows where LA didn't exist for a particular year
    return direct_payments_df


def calculate_estimated_number_of_service_users_employing_staff(
    direct_payments_df: DataFrame,
) -> DataFrame:
    direct_payments_df = direct_payments_df.withColumn(
        DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF,
        F.col(DP.SERVICE_USER_DPRS_DURING_YEAR) * F.col(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF),
    )
    return direct_payments_df


def estimate_missing_data_for_service_users_employing_staff(
    direct_payments_df: DataFrame,
) -> DataFrame:

    direct_payments_df = model_using_mean(direct_payments_df)
    # TODO: Copy historic estimates into mean estimate column in preparation for extrapolation step
    direct_payments_df = merge_in_historical_estimates_with_estimate_using_mean(direct_payments_df)
    # Apply know values
    direct_payments_df = apply_known_values(direct_payments_df)
    direct_payments_df = model_extrapolation(direct_payments_df)
    # Apply extrapolation
    direct_payments_df = apply_extrapolated_values(direct_payments_df)
    direct_payments_df = model_interpolation(direct_payments_df)

    direct_payments_df = apply_interpolated_values(direct_payments_df)
    direct_payments_df = direct_payments_df.drop(DP.ESTIMATE_USING_INTERPOLATION)
    direct_payments_df = apply_mean_estimates(direct_payments_df)
    direct_payments_df = model_interpolation(direct_payments_df)

    direct_payments_df = apply_interpolated_values(direct_payments_df)
    # direct_payments_df = apply_models(direct_payments_df)
    # TODO: Interpolate remaining values for 2016 and 2017 - split apply models into functions to apply each model and then include after models
    # will need to use estimated % - applying extrapolation before modelling interpolation should fix 2016 and 2017 gaps
    # mean imputation still needs to be last option for those missing all data

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
        F.when(F.col(DP.ESTIMATE_USING_MEAN).isNotNull(), F.col(DP.ESTIMATE_USING_MEAN)).otherwise(
            F.col(DP.HISTORIC_SERVICE_USERS_EMPLOYING_STAFF_ESTIMATE)
        ),
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
