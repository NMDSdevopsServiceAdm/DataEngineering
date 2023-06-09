import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from utils.prepare_direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


def prepare_during_year_data(direct_payments_df: DataFrame) -> DataFrame:
    direct_payments_df = calculate_total_dprs_during_year(direct_payments_df)
    return direct_payments_df


def calculate_total_dprs_during_year(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        DP.TOTAL_DPRS_DURING_YEAR,
        F.col(DP.SERVICE_USER_DPRS_DURING_YEAR) + F.col(DP.CARER_DPRS_DURING_YEAR),
    )
    return df
