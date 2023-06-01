import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    IntegerType,
    StringType,
    FloatType,
)

from utils.prepare_direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


def determine_areas_including_carers_on_adass(direct_payments_df: DataFrame) -> DataFrame:
    # TODO
    # filter to most recent year
    most_recent_direct_payments_df = filter_to_most_recent_year(direct_payments_df)
    # calculate_total_dprs_during_year()
    # calculate_dprs_employing_staff()
    # calculate_carers_employing_staff()
    # calculate_total_employing_staff_including_carers()
    # determine_if_survey_base_is_close_to_ascof_base
    # alocate_method()
    # calculate_proportion_of_su_employing_staff
    # rejoin to table
    return direct_payments_df


def filter_to_most_recent_year(df: DataFrame) -> DataFrame:
    most_recent_year = F.max(DP.YEAR)
    df = df.where(DP.YEAR == most_recent_year)
    return df
