import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


def prepare_during_year_data(direct_payments_df: DataFrame) -> DataFrame:
    direct_payments_df = estimate_missing_salt_data(direct_payments_df)
    direct_payments_df = calculate_total_dprs_during_year(direct_payments_df)
    return direct_payments_df


def calculate_total_dprs_during_year(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        DP.TOTAL_DPRS_DURING_YEAR,
        F.col(DP.SERVICE_USER_DPRS_DURING_YEAR) + F.col(DP.CARER_DPRS_DURING_YEAR),
    )
    return df


def estimate_missing_salt_data(df: DataFrame) -> DataFrame:
    # Hackney is the only LA missing SALT data. The values in this function are based on a regression in excel of previous data points for Hackney in the SALT data.
    missing_service_user_dprs: float = 580.5
    missing_carer_dprs: float = 140.85

    df = df.withColumn(
        DP.SERVICE_USER_DPRS_DURING_YEAR,
        F.when(
            F.col(DP.SERVICE_USER_DPRS_DURING_YEAR).isNotNull(),
            F.col(DP.SERVICE_USER_DPRS_DURING_YEAR),
        ).when(
            (F.col(DP.SERVICE_USER_DPRS_DURING_YEAR).isNull())
            & (F.col(DP.LA_AREA) == "Hackney")
            & (F.col(DP.YEAR_AS_INTEGER) == 2022),
            F.lit(missing_service_user_dprs),
        ),
    )

    df = df.withColumn(
        DP.CARER_DPRS_DURING_YEAR,
        F.when(
            F.col(DP.CARER_DPRS_DURING_YEAR).isNotNull(),
            F.col(DP.CARER_DPRS_DURING_YEAR),
        ).when(
            (F.col(DP.CARER_DPRS_DURING_YEAR).isNull())
            & (F.col(DP.LA_AREA) == "Hackney")
            & (F.col(DP.YEAR_AS_INTEGER) == 2022),
            F.lit(missing_carer_dprs),
        ),
    )

    return df
