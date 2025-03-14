from pyspark.sql import DataFrame, functions as F, Window

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc


def primary_service_rate_of_change_trendline(
    df: DataFrame,
    rate_of_change_column_name: str,
    trendline_column_name: str,
) -> DataFrame:
    """
    Computes the cumulative rate of change since the first period in the DataFrame from the single period rates of change.

    The cumulative rate of change is calculated by multiplying sequential rates of change over time.
    Given a rate of change sequence:
        - Period 1 to 2: 'a'
        - Period 2 to 3: 'b'
        - Period 3 to 4: 'c'
    The cumulative rate of change is:
        - Period 1: 1.0 (no change)
        - Period 2: 'a'
        - Period 3: 'a * b'
        - Period 4: 'a * b * c', etc.

    Args:
        df (DataFrame): Input DataFrame.
        rate_of_change_column_name (str): Column name containing the single period rate of change values.
        trendline_column_name (str): The name of the new column to store the cumulative rate of change.

    Returns:
        DataFrame: The input DataFrame with an additional column containing the cumulative rate of change.
    """
    w = Window.partitionBy(IndCqc.primary_service_type).orderBy(IndCqc.unix_time)

    deduped_df = deduplicate_dataframe(df, rate_of_change_column_name)

    rate_of_change_trendline_df = deduped_df.withColumn(
        trendline_column_name,
        F.exp(F.sum(F.log(rate_of_change_column_name)).over(w)),
    ).drop(rate_of_change_column_name)

    df = df.join(
        rate_of_change_trendline_df,
        [IndCqc.primary_service_type, IndCqc.unix_time],
        "left",
    )

    return df


def deduplicate_dataframe(df: DataFrame, rate_of_change_column_name: str) -> DataFrame:
    """
    Selects primary service type, unix time and single period rate of change then deduplicates the DataFrame based on primary service type and unix time.

    Args:
        df (DataFrame): The input DataFrame.
        rate_of_change_column_name (str): Column name containing the single period rate of change values.

    Returns:
        DataFrame: The deduplicated DataFrame.
    """
    df = df.select(
        IndCqc.primary_service_type,
        IndCqc.unix_time,
        rate_of_change_column_name,
    ).dropDuplicates([IndCqc.primary_service_type, IndCqc.unix_time])

    return df
