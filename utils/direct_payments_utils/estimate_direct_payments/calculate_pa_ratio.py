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
    
    return survey_df

def calculate_average_ratios(survey_df: DataFrame) -> DataFrame:
    average_survey_df = survey_df
    return average_survey_df

def estimate_ratios(average_survey_df: DataFrame) -> DataFrame:
    pa_ratio_df = average_survey_df
    return pa_ratio_df