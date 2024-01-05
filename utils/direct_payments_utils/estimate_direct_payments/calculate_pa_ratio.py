import sys
from datetime import datetime, date

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window, SparkSession
from pyspark.sql.types import ArrayType, LongType, IntegerType, FloatType

from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
    DirectPaymentColumnValues as Values,
)

from utils.direct_payments_utils.direct_payments_configuration import (
    DirectPaymentConfiguration as Config,
)
from utils.direct_payments_utils.estimate_direct_payments.models.interpolation import (
    interpolation_calculation,
)


def calculate_pa_ratio(survey_df: DataFrame, spark: SparkSession) -> DataFrame:
    survey_df = exclude_outliers(survey_df)
    average_survey_df = calculate_average_ratios(survey_df)
    pa_ratio_df = estimate_ratios(average_survey_df, spark)
    pa_ratio_df = apply_rolling_average(pa_ratio_df)
    pa_ratio_df = pa_ratio_df.select(DP.YEAR_AS_INTEGER, DP.RATIO_ROLLING_AVERAGE)
    return pa_ratio_df


def exclude_outliers(survey_df: DataFrame) -> DataFrame:
    survey_df = survey_df.where(
        (survey_df[DP.TOTAL_STAFF_RECODED] < 10.0)
        & (survey_df[DP.TOTAL_STAFF_RECODED] > 0.0)
    )
    return survey_df


def calculate_average_ratios(survey_df: DataFrame) -> DataFrame:
    w = Window.partitionBy(DP.YEAR_AS_INTEGER)
    survey_df = survey_df.withColumn(
        DP.AVERAGE_STAFF, F.avg(DP.TOTAL_STAFF_RECODED).over(w)
    )
    average_survey_df = survey_df.select(
        DP.YEAR_AS_INTEGER, DP.AVERAGE_STAFF
    ).dropDuplicates()
    return average_survey_df


def estimate_ratios(average_survey_df: DataFrame, spark: SparkSession) -> DataFrame:
    ratios_df = create_dataframe_including_all_years(average_survey_df, spark)
    ratios_df = impute_values_backwards(ratios_df)
    ratios_df = interpolate_values(ratios_df)
    return ratios_df


def create_year_range(min_year: int, max_year: int, step_size_in_years: int = 1) -> int:
    years = [min_year]
    number_of_years = max_year - min_year
    for year in range(number_of_years):
        years.append(min_year + year + 1)
    return years


def create_dataframe_including_all_years(
    average_survey_df: DataFrame, spark: SparkSession
) -> DataFrame:
    current_year = date.today().year
    last_year_estimated = current_year - 1
    years = create_year_range(Config.FIRST_YEAR, last_year_estimated)
    years_df: DataFrame = spark.createDataFrame(years, IntegerType())
    years_df = years_df.withColumnRenamed("value", DP.YEAR_AS_INTEGER)
    years_df = years_df.join(average_survey_df, DP.YEAR_AS_INTEGER, how="left")
    return years_df


def get_first_known_year(df: DataFrame) -> int:
    known_rows_df = df.where(df[DP.AVERAGE_STAFF].isNotNull())
    first_known_year = known_rows_df.agg(
        F.min(known_rows_df[DP.YEAR_AS_INTEGER])
    ).collect()[0][0]
    return first_known_year


def get_first_known_ratio(df: DataFrame, first_known_year: int) -> float:
    known_rows_df = df.where(df[DP.AVERAGE_STAFF].isNotNull())
    first_known_ratio_df = known_rows_df.where(
        df[DP.YEAR_AS_INTEGER] == first_known_year
    )
    first_known_ratio = first_known_ratio_df.collect()[0][DP.AVERAGE_STAFF]
    return first_known_ratio


def impute_values_backwards(ratios_df: DataFrame) -> DataFrame:
    first_known_year = get_first_known_year(ratios_df)
    first_known_ratio = get_first_known_ratio(ratios_df, first_known_year)
    ratios_df = ratios_df.withColumn(
        DP.AVERAGE_STAFF,
        F.when(
            ratios_df[DP.YEAR_AS_INTEGER] < first_known_year, F.lit(first_known_ratio)
        ).otherwise(ratios_df[DP.AVERAGE_STAFF]),
    )
    return ratios_df


def get_value_in_new_column(
    df: DataFrame, column_name: str, new_column_name: str, direction: str
) -> DataFrame:
    if direction == Values.NEXT:
        w = Window.partitionBy().orderBy(DP.YEAR_AS_INTEGER).rowsBetween(0, sys.maxsize)
        df = df.withColumn(
            new_column_name,
            F.first(F.col(column_name), ignorenulls=True).over(w),
        )
    elif direction == Values.PREVIOUS:
        w = (
            Window.partitionBy()
            .orderBy(DP.YEAR_AS_INTEGER)
            .rowsBetween(-sys.maxsize, 0)
        )
        df = df.withColumn(
            new_column_name,
            F.last(F.col(column_name), ignorenulls=True).over(w),
        )
    return df


def add_column_with_year_for_known_data(
    df: DataFrame,
) -> DataFrame:
    df = df.withColumn(
        DP.AVERAGE_STAFF_YEAR_KNOWN,
        F.when(
            (F.col(DP.AVERAGE_STAFF).isNotNull()),
            F.col(DP.YEAR_AS_INTEGER),
        ).otherwise(F.lit(None)),
    )
    return df


def interpolate_missing_ratios(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        DP.RATIO_DIFFERENCE, df[DP.NEXT_AVERAGE_STAFF] - df[DP.PREVIOUS_AVERAGE_STAFF]
    )
    df = df.withColumn(
        DP.YEAR_DIFFERENCE,
        df[DP.NEXT_AVERAGE_STAFF_YEAR_KNOWN] - df[DP.PREVIOUS_AVERAGE_STAFF_YEAR_KNOWN],
    )
    df = df.withColumn(
        DP.INTERPOLATION_MODEL, df[DP.RATIO_DIFFERENCE] / df[DP.YEAR_DIFFERENCE]
    )
    df = df.withColumn(
        DP.INTERPOLATED_RATIO,
        F.when(
            df[DP.PREVIOUS_AVERAGE_STAFF_YEAR_KNOWN]
            == df[DP.NEXT_AVERAGE_STAFF_YEAR_KNOWN],
            df[DP.AVERAGE_STAFF],
        ).otherwise(
            df[DP.PREVIOUS_AVERAGE_STAFF]
            + df[DP.INTERPOLATION_MODEL]
            * (df[DP.YEAR_AS_INTEGER] - df[DP.PREVIOUS_AVERAGE_STAFF_YEAR_KNOWN])
        ),
    )

    df = df.withColumn(
        DP.AVERAGE_STAFF,
        F.when(df[DP.AVERAGE_STAFF].isNull(), df[DP.INTERPOLATED_RATIO]).otherwise(
            df[DP.AVERAGE_STAFF]
        ),
    )
    df_with_interpolated_values = df.drop(
        DP.RATIO_DIFFERENCE, DP.YEAR_DIFFERENCE, DP.INTERPOLATION_MODEL
    )
    return df_with_interpolated_values


def interpolate_values(ratios_df: DataFrame) -> DataFrame:
    ratios_df = add_column_with_year_for_known_data(ratios_df)
    ratios_df = get_value_in_new_column(
        ratios_df, DP.AVERAGE_STAFF, DP.PREVIOUS_AVERAGE_STAFF, Values.PREVIOUS
    )
    ratios_df = get_value_in_new_column(
        ratios_df,
        DP.AVERAGE_STAFF_YEAR_KNOWN,
        DP.PREVIOUS_AVERAGE_STAFF_YEAR_KNOWN,
        Values.PREVIOUS,
    )
    ratios_df = get_value_in_new_column(
        ratios_df, DP.AVERAGE_STAFF, DP.NEXT_AVERAGE_STAFF, Values.NEXT
    )
    ratios_df = get_value_in_new_column(
        ratios_df,
        DP.AVERAGE_STAFF_YEAR_KNOWN,
        DP.NEXT_AVERAGE_STAFF_YEAR_KNOWN,
        Values.NEXT,
    )
    ratios_df = interpolate_missing_ratios(ratios_df)
    return ratios_df


def apply_rolling_average(df: DataFrame) -> DataFrame:
    range = Config.NUMBER_OF_YEARS_ROLLING_AVERAGE - 1
    w = (
        Window.partitionBy()
        .orderBy(F.col(DP.YEAR_AS_INTEGER).cast("long"))
        .rangeBetween(-(range), 0)
    )
    df = df.withColumn(DP.COUNT, F.lit(1))
    df = df.withColumn(DP.COUNT_OF_YEARS, F.sum(df[DP.COUNT]).over(w))
    df = df.withColumn(DP.SUM_OF_RATIOS, F.sum(df[DP.AVERAGE_STAFF]).over(w))
    df = df.withColumn(
        DP.RATIO_ROLLING_AVERAGE, df[DP.SUM_OF_RATIOS] / df[DP.COUNT_OF_YEARS]
    )
    df.show()
    return df
