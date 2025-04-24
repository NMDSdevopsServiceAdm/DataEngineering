from pyspark.sql import DataFrame, Window, functions as F

from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)
from projects._04_direct_payment_recipients.direct_payments_configuration import (
    DirectPaymentConfiguration as Config,
    DirectPaymentsMissingPARatios as HistoricRatios,
    DirectPaymentsOutlierThresholds as Thresholds,
)
from utils import utils


def calculate_pa_ratio(survey_df: DataFrame) -> DataFrame:
    survey_df = survey_df.withColumnRenamed(DP.YEAR, DP.YEAR_AS_INTEGER)
    survey_df = exclude_outliers(survey_df)
    average_survey_df = calculate_average_ratios(survey_df)
    pa_ratio_df = add_in_missing_historic_ratios(average_survey_df)
    pa_ratio_df = reduce_year_by_one_to_match_external_data(pa_ratio_df)
    pa_ratio_df = apply_rolling_average(pa_ratio_df)
    pa_ratio_df = pa_ratio_df.select(DP.YEAR_AS_INTEGER, DP.RATIO_ROLLING_AVERAGE)
    return pa_ratio_df


def exclude_outliers(survey_df: DataFrame) -> DataFrame:
    survey_df = survey_df.where(
        (survey_df[DP.TOTAL_STAFF_RECODED] <= Thresholds.MAX_PAS)
        & (survey_df[DP.TOTAL_STAFF_RECODED] >= Thresholds.MIN_PAS)
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


def add_in_missing_historic_ratios(df: DataFrame) -> DataFrame:
    spark = utils.get_spark()
    historic_ratios_df = spark.createDataFrame(
        HistoricRatios.ratios, HistoricRatios.schema
    )
    df = df.join(historic_ratios_df, DP.YEAR_AS_INTEGER, how="outer")
    df = df.withColumn(
        DP.AVERAGE_STAFF,
        F.when(df[DP.AVERAGE_STAFF].isNull(), df[DP.HISTORIC_RATIO]).otherwise(
            df[DP.AVERAGE_STAFF]
        ),
    )
    return df


def reduce_year_by_one_to_match_external_data(df: DataFrame) -> DataFrame:
    df = df.withColumn(DP.YEAR_AS_INTEGER, df[DP.YEAR_AS_INTEGER] - 1)
    return df


def apply_rolling_average(df: DataFrame) -> DataFrame:
    range = Config.NUMBER_OF_YEARS_ROLLING_AVERAGE - 1
    w = (
        Window.partitionBy(F.lit(0))
        .orderBy(F.col(DP.YEAR_AS_INTEGER).cast("long"))
        .rangeBetween(-(range), 0)
    )
    df = df.withColumn(DP.COUNT, F.lit(1))
    df = df.withColumn(DP.COUNT_OF_YEARS, F.sum(df[DP.COUNT]).over(w))
    df = df.withColumn(DP.SUM_OF_RATIOS, F.sum(df[DP.AVERAGE_STAFF]).over(w))
    df = df.withColumn(
        DP.RATIO_ROLLING_AVERAGE, df[DP.SUM_OF_RATIOS] / df[DP.COUNT_OF_YEARS]
    )
    return df
