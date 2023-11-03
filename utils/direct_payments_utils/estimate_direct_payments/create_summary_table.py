from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


def create_summary_table(
    direct_payments_df: DataFrame,
) -> DataFrame:
    summary_direct_payments_df = direct_payments_df.groupBy(DP.YEAR).agg(
        F.sum(DP.TOTAL_DPRS_DURING_YEAR).cast("float").alias(DP.TOTAL_DPRS),
        F.avg(DP.ESTIMATED_PROPORTION_OF_DPR_WHO_ARE_SERVICE_USERS)
        .cast("float")
        .alias(DP.PROPORTION_OF_SERVICE_USER_DPRS),
        F.sum(DP.SERVICE_USER_DPRS_DURING_YEAR)
        .cast("float")
        .alias(DP.SERVICE_USER_DPRS),
        F.avg(DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF)
        .cast("float")
        .alias(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_FINAL),
        F.sum(DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF)
        .cast("float")
        .alias(DP.SERVICE_USERS_EMPLOYING_STAFF),
        F.sum(DP.ESTIMATED_SERVICE_USERS_WITH_SELF_EMPLOYED_STAFF)
        .cast("float")
        .alias(DP.SERVICE_USERS_WITH_SELF_EMPLOYED_STAFF),
        F.sum(DP.ESTIMATED_CARERS_EMPLOYING_STAFF)
        .cast("float")
        .alias(DP.CARERS_EMPLOYING_STAFF),
        F.sum(DP.ESTIMATED_TOTAL_DPR_EMPLOYING_STAFF)
        .cast("float")
        .alias(DP.TOTAL_DPRS_EMPLOYING_STAFF),
        F.sum(DP.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS)
        .cast("float")
        .alias(DP.TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS),
        F.avg(DP.ESTIMATED_PROPORTION_OF_TOTAL_DPR_EMPLOYING_STAFF)
        .cast("float")
        .alias(DP.PROPORTION_OF_TOTAL_DPRS_EMPLOYING_STAFF),
    )
    return summary_direct_payments_df
