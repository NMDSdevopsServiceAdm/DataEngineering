from datetime import datetime, date

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window, SparkSession
from pyspark.sql.types import ArrayType, LongType, IntegerType

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

def estimate_ratios(average_survey_df: DataFrame, spark: SparkSession) -> DataFrame:
    known_ratios_df = create_dataframe_including_all_years(average_survey_df, spark)
    # calculate mean
    
    # interpolate
    # extrapolate
    # apply rolling avg


    pa_ratio_df = 
    return pa_ratio_df




def create_year_range(
    min_year: int, max_year: int, step_size_in_years: int = 1
) -> int:
    years = [min_year]
    number_of_years = max_year - min_year
    for year in range(number_of_years):
        years.append(min_year + year + 1)
    return years

def create_dataframe_including_all_years(average_survey_df: DataFrame, spark: SparkSession) -> DataFrame:
    current_year = date.today().year
    last_year_estimated = current_year - 1
    years = create_year_range(Config.FIRST_YEAR, last_year_estimated)
    years_df: DataFrame = spark.createDataFrame(years, IntegerType())
    years_df = years_df.withColumnRenamed("value", DP.YEAR_AS_INTEGER)
    years_df = years_df.join(average_survey_df, DP.YEAR_AS_INTEGER, how="left")
    return years_df