from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


def create_summary_table(
    direct_payments_df: DataFrame,
) -> DataFrame:
    # TODO
    summary_direct_payments_df = direct_payments_df.groupBy(DP.YEAR).agg(
        # total direct payment recipients during year
        F.sum(DP.TOTAL_DPRS_DURING_YEAR).cast("float").alias("total_dprs"),
        # estimated % of dprs that are service users
        F.avg(DP.ESTIMATED_PROPORTION_OF_DPR_WHO_ARE_SERVICE_USERS)
        .cast("float")
        .alias("proportion_of_service_user_dprs"),
        # service user dprs during year
        F.sum(DP.SERVICE_USER_DPRS_DURING_YEAR).cast("float").alias("service_user_dprs"),
        # change in service users [to calculate]
        # % of change [to calculate]
        # estimated % of services users employing staff
        F.avg(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF)
        .cast("float")
        .alias("estimated_proportion_of_service_users_employing_staff"),
        # estimated number of service users employing staff
        F.sum(DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF)
        .cast("float")
        .alias("estimated_service_users_employing_staff"),
        # estimated number of service users with self employed pas
        F.sum(DP.ESTIMATED_SERVICE_USERS_WITH_SELF_EMPLOYED_STAFF)
        .cast("float")
        .alias("estimated_service_users_with_self_employed_staff"),
        # estimated number of carers employing staff
        F.sum(DP.ESTIMATED_CARERS_EMPLOYING_STAFF).cast("float").alias("estimated_carers_employing_staff"),
        # estimated total dprs employing staff
        F.sum(DP.ESTIMATED_TOTAL_DPR_EMPLOYING_STAFF).cast("float").alias("estimated_total_dprs_employing_staff"),
        # estimated PA filled posts
        F.sum(DP.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS)
        .cast("float")
        .alias("estimated_total_personal_assistant_filled_posts"),
        # estimated % of total dprs employing staff
        F.avg(DP.ESTIMATED_PROPORTION_OF_TOTAL_DPR_EMPLOYING_STAFF)
        .cast("float")
        .alias("estimated_proportion_of_total_dprs_employing_staff"),
    )
    return summary_direct_payments_df
