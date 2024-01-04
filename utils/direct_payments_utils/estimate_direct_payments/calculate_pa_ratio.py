import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window

from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)

from utils.direct_payments_utils.direct_payments_configuration import (
    DirectPaymentConfiguration as Config,
)

def calculate_pa_ratio(survey_df: DataFrame) -> DataFrame:
    survey_df = exclude_outliers(survey_df)
    average_survey_df = calculate_average_ratios(survey_df)
    pa_ratio_df = estimate_ratios(average_survey_df)
    return pa_ratio_df

def exclude_outliers(survey_df: DataFrame) -> DataFrame:
    survey_df = survey_df.where((survey_df[DP.TOTAL_STAFF_RECODED] < 10.0) & (survey_df[DP.TOTAL_STAFF_RECODED] > 0.0) )
    return survey_df

def calculate_average_ratios(survey_df: DataFrame) -> DataFrame:
    w = Window.partitionBy(DP.YEAR_AS_INTEGER)
    survey_df = survey_df.withColumn(DP.AVERAGE_STAFF, F.avg(DP.TOTAL_STAFF_RECODED).over(w))
    average_survey_df = survey_df.select(DP.YEAR_AS_INTEGER, DP.AVERAGE_STAFF).dropDuplicates()
    return average_survey_df

def estimate_ratios(average_survey_df: DataFrame) -> DataFrame:
    pa_ratio_df = average_survey_df
    return pa_ratio_df