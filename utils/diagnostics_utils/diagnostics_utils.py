from pyspark.sql import DataFrame, functions as F, Window
from pyspark.sql.types import (
    DateType,
    FloatType,
    StringType,
    StructField,
    StructType,
)

from utils import utils
from utils.column_values.categorical_columns_by_dataset import (
    DiagnosticOnKnownFilledPostsCategoricalValues as CatValues,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
    PartitionKeys as Keys,
)


def filter_to_known_values(df: DataFrame, column: str) -> DataFrame:
    """
    Removes rows which have a null value in a given column.

    Args:
        df (DataFrame): A dataframe containing the given column.
        column (str): A column in the dataframe to filter on.

    Returns:
        DataFrame: A dataframe with rows with an null value in the given column removed.
    """
    return df.filter(df[column].isNotNull())


def restructure_dataframe_to_column_wise(
    df: DataFrame, column_for_comparison: str, list_of_models: list
) -> DataFrame:
    """
    Reshapes the dataframe so that all estimated values are in one column.

    This function reshapes the dataframe to make it easier to calculate the aggregations
    using a window function. Model values that were previously in separate columns for
    each model are joined into a single column with a corresponding column describing which
    model they are from. This means that the unique index of the dataframe with now be the
    combination of location id, cqc location import date and estimate source.

    Args:
        df (DataFrame): A dataframe of estimates with each model's values in a different column.
        column_for_comparison (str): The column of values to use for calculating residuals.
        list_of_models (list): A list of strings of column names corresponding to the models to include.

    Returns:
        DataFrame: A dataframe of estimates with each model's values in a single column and a column of corresponding model names.
    """
    reshaped_df = create_empty_reshaped_dataframe(column_for_comparison)
    for model in list_of_models:
        model_df = df.select(
            IndCQC.location_id,
            IndCQC.cqc_location_import_date,
            IndCQC.primary_service_type,
            column_for_comparison,
            model,
            Keys.year,
            Keys.month,
            Keys.day,
            Keys.import_date,
        )
        model_df = model_df.withColumn(IndCQC.estimate_source, F.lit(model))
        model_df = model_df.withColumnRenamed(model, IndCQC.estimate_value)
        reshaped_df = reshaped_df.unionByName(model_df)
    return reshaped_df


def create_list_of_models() -> list:
    """
    Creates a list of models to include in the reshaping of the dataframe.

    This function creates a list of the column names of models which will be used to reshape
    the dataframe.

    Returns:
        list: A list of strings of column names corresponding to the models to include.
    """
    list_of_models = (
        CatValues.estimate_filled_posts_source_column_values.categorical_values
    )
    list_of_models = list_of_models + [IndCQC.estimate_filled_posts]
    return list_of_models


def create_empty_reshaped_dataframe(column_for_comparison: str) -> DataFrame:
    """
    Creates an empty dataframe to define it's structure.

    This function creates a new, empty dataframe which uses a predefined schema with columns for the estimate's source and value.

    Args:
        column_for_comparison (str): The column of values to use for calculating residuals.

    Returns:
        DataFrame: An empty dataframe with the predefined schema.
    """
    spark = utils.get_spark()
    reshaped_df_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.cqc_location_import_date, DateType(), False),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(
                column_for_comparison,
                FloatType(),
                True,
            ),
            StructField(IndCQC.estimate_source, FloatType(), True),
            StructField(
                IndCQC.estimate_value,
                FloatType(),
                True,
            ),
            StructField(Keys.year, StringType(), True),
            StructField(Keys.month, StringType(), True),
            StructField(Keys.day, StringType(), True),
            StructField(Keys.import_date, StringType(), True),
        ]
    )
    reshaped_df = spark.createDataFrame([], reshaped_df_schema)
    return reshaped_df


def create_window_for_model_and_service_splits() -> Window:
    """
    Creates a window partitioned by model and service type.

    Returns:
        Window: A window partitioned by model and service type, with rows
        between set to include all rows in each partition.
    """
    window = Window.partitionBy(
        [IndCQC.estimate_source, IndCQC.primary_service_type]
    ).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    return window


def calculate_distribution_metrics(df: DataFrame, window: Window) -> DataFrame:
    """
    Adds columns with distribution metrics.

    This function adds four columns to the dataset containing the mean, standard
    deviation, kurtosis, and skewness. These are aggregated over the given window.

    Args:
        df (DataFrame): A dataframe with primary_service_type, estimate_source and estimate_value.
        window (Window): A window for aggregating the metrics.

    Returns:
        DataFrame: A dataframe with four additional columns containing distribution metrics aggregated over the given window.
    """
    df = calculate_mean_over_window(df, window)
    df = calculate_standard_deviation_over_window(df, window)
    df = calculate_kurtosis_over_window(df, window)
    df = calculate_skewness_over_window(df, window)
    return df


def calculate_mean_over_window(df: DataFrame, window: Window) -> DataFrame:
    """
    Adds column with the mean.

    This function adds a columns to the dataset containing the mean, aggregated over the given window.

    Args:
        df (DataFrame): A dataframe with primary_service_type, estimate_source and estimate_value.
        window (Window): A window for aggregating the metrics.

    Returns:
        DataFrame: A dataframe with an additional columns containing the mean aggregated over the given window.
    """
    df = df.withColumn(
        IndCQC.distribution_mean, F.mean(df[IndCQC.estimate_value]).over(window)
    )
    return df


def calculate_standard_deviation_over_window(
    df: DataFrame, window: Window
) -> DataFrame:
    """
    Adds column with the standard deviation.

    This function adds a columns to the dataset containing the standard deviation, aggregated over the given window.

    Args:
        df (DataFrame): A dataframe with primary_service_type, estimate_source and estimate_value.
        window (Window): A window for aggregating the metrics.

    Returns:
        DataFrame: A dataframe with an additional columns containing the standard deviation aggregated over the given window.
    """
    df = df.withColumn(
        IndCQC.distribution_standard_deviation,
        F.stddev(df[IndCQC.estimate_value]).over(window),
    )
    return df


def calculate_kurtosis_over_window(df: DataFrame, window: Window) -> DataFrame:
    """
    Adds column with the kurtosis.

    This function adds a columns to the dataset containing the kurtosis, aggregated over the given window.

    Args:
        df (DataFrame): A dataframe with primary_service_type, estimate_source and estimate_value.
        window (Window): A window for aggregating the metrics.

    Returns:
        DataFrame: A dataframe with an additional columns containing the kurtosis aggregated over the given window.
    """
    df = df.withColumn(
        IndCQC.distribution_kurtosis, F.kurtosis(df[IndCQC.estimate_value]).over(window)
    )
    return df


def calculate_skewness_over_window(df: DataFrame, window: Window) -> DataFrame:
    """
    Adds column with the skewness.

    This function adds a columns to the dataset containing the skewness, aggregated over the given window.

    Args:
        df (DataFrame): A dataframe with primary_service_type, estimate_source and estimate_value.
        window (Window): A window for aggregating the metrics.

    Returns:
        DataFrame: A dataframe with an additional columns containing the skewness aggregated over the given window.
    """
    df = df.withColumn(
        IndCQC.distribution_skewness, F.skewness(df[IndCQC.estimate_value]).over(window)
    )
    return df


def calculate_residuals(df: DataFrame, column_for_comparison: str) -> DataFrame:
    """
    Adds columns with residuals.

    This function adds two columns to the dataset containing the absolute residual
    and the percentage residual.

    Args:
        df (DataFrame): A dataframe with ascwds_filled_posts_dedup_clean and estimate_value.
        column_for_comparison (str): The column of values to use for calculating residuals.

    Returns:
        DataFrame: A dataframe with two additional columns containing residuals.
    """
    df = calculate_residual(df, column_for_comparison)
    df = calculate_absolute_residual(df)
    df = calculate_percentage_residual(df, column_for_comparison)
    df = calculate_standardised_residual(df, column_for_comparison)
    return df


def calculate_residual(df: DataFrame, column_for_comparison: str) -> DataFrame:
    """
    Adds column with the residual.

    This function adds a columns to the dataset containing the residual.

    Args:
        df (DataFrame): A dataframe with ascwds_filled_posts_deduplicated_clean and estimate_value.
        column_for_comparison (str): The column of values to use for calculating residuals.

    Returns:
        DataFrame: A dataframe with an additional column containing the residual.
    """
    df = df.withColumn(
        IndCQC.residual,
        F.col(IndCQC.estimate_value) - F.col(column_for_comparison),
    )
    return df


def calculate_absolute_residual(df: DataFrame) -> DataFrame:
    """
    Adds column with the absolute residual.

    This function adds a columns to the dataset containing the absolute residual.

    Args:
        df (DataFrame): A dataframe with a calculated residual.

    Returns:
        DataFrame: A dataframe with an additional column containing the absolute residual.
    """
    df = df.withColumn(
        IndCQC.absolute_residual,
        F.abs(F.col(IndCQC.residual)),
    )
    return df


def calculate_percentage_residual(
    df: DataFrame, column_for_comparison: str
) -> DataFrame:
    """
    Adds column with the percentage residual.

    This function adds a columns to the dataset containing the percentage residual.

    Args:
        df (DataFrame): A dataframe with a column for comparison and estimate_value.
        column_for_comparison (str): The column of values to use for calculating residuals.

    Returns:
        DataFrame: A dataframe with an additional column containing the percentage residual.
    """
    df = df.withColumn(
        IndCQC.percentage_residual,
        (F.col(IndCQC.estimate_value) - F.col(column_for_comparison))
        / F.col(IndCQC.estimate_value),
    )
    return df


def calculate_standardised_residual(
    df: DataFrame, column_for_comparison: str
) -> DataFrame:
    """
    Adds column with the standardised residual.

    This function adds a columns to the dataset containing the standardised residual.

    Args:
        df (DataFrame): A dataframe with a column for comparison and estimate_value.
        column_for_comparison (str): The column of values to use for calculating residuals.

    Returns:
        DataFrame: A dataframe with an additional column containing the standardised residual.
    """
    df = df.withColumn(
        IndCQC.standardised_residual,
        F.col(IndCQC.residual) / F.sqrt(F.col(column_for_comparison)),
    )
    return df


def calculate_aggregate_residuals(
    df: DataFrame,
    window: Window,
    absolute_value_cutoff: float,
    percentage_value_cutoff: float,
    standardised_residual_cutoff: float,
) -> DataFrame:
    df = aggregate_residuals(
        df,
        window,
        IndCQC.average_absolute_residual,
        IndCQC.absolute_residual,
        aggregation_type="mean",
    )
    df = aggregate_residuals(
        df,
        window,
        IndCQC.average_percentage_residual,
        IndCQC.percentage_residual,
        aggregation_type="mean",
    )
    df = aggregate_residuals(
        df, window, IndCQC.max_residual, IndCQC.residual, aggregation_type="max"
    )
    df = aggregate_residuals(
        df, window, IndCQC.min_residual, IndCQC.residual, aggregation_type="min"
    )
    df = calculate_percentage_of_residuals_within_cutoffs(
        df,
        window,
        IndCQC.percentage_of_residuals_within_absolute_value,
        IndCQC.absolute_residual,
        absolute_value_cutoff,
    )
    df = calculate_percentage_of_residuals_within_cutoffs(
        df,
        window,
        IndCQC.percentage_of_residuals_within_percentage_value,
        IndCQC.percentage_residual,
        percentage_value_cutoff,
    )
    df = calculate_percentage_of_residuals_within_cutoffs(
        df,
        window,
        IndCQC.percentage_of_standardised_residuals_within_limit,
        IndCQC.standardised_residual,
        standardised_residual_cutoff,
    )
    return df


def aggregate_residuals(
    df: DataFrame,
    window: Window,
    new_column_name: str,
    residual_column_name: str,
    aggregation_type: str,
) -> DataFrame:
    """
    This function adds a column containing the specified aggregation type (mean, min or max) over the given window.

    Args:
        df (DataFrame): The input DataFrame containing the required columns.
        window (Window): A window for aggregating the residuals.
        new_column_name (str): The name of the new column to be added.
        residual_column_name (str): The name of the residual column to aggregate.
        aggregation_type (str): The type of aggregation ('mean', 'min' or 'max').

    Returns:
        DataFrame: A dataframe with an additional column containing the specified aggregation type over the given window.
    """
    aggregation_types = {"mean": F.mean, "min": F.min, "max": F.max}

    if aggregation_type not in aggregation_types:
        raise ValueError(
            f"Error: The aggregation_type parameter '{aggregation_type}' was not found. Please use 'mean', 'min' or 'max'."
        )

    method = aggregation_types[aggregation_type]

    df = df.withColumn(
        new_column_name, method(F.col(residual_column_name)).over(window)
    )
    return df


def calculate_percentage_of_residuals_within_cutoffs(
    df: DataFrame,
    window: Window,
    new_column_name: str,
    residual_column_name: str,
    cutoff_value: float,
) -> DataFrame:
    """
    This function adds a column containing the percentage of residuals which are within the cutoff value, aggregated over the given window.

    Args:
        df (DataFrame): The input DataFrame containing the required columns.
        window (Window): A window for aggregating the residuals.
        new_column_name (str): The name of the new column to be added.
        residual_column_name (str): The name of the residual column to compare with cutoff values.
        cutoff_value (float): The threshold value.

    Returns:
        DataFrame: A dataframe with an additional column containing the percentage of residuals which are within the cutoff value over the given window.
    """
    df = df.withColumn(
        new_column_name,
        F.count(F.when(F.col(residual_column_name) <= cutoff_value, True)).over(window)
        / F.count(F.col(residual_column_name)).over(window),
    )
    return df


def create_summary_diagnostics_table(df: DataFrame) -> DataFrame:
    """
    Creates deduplicated dataframe of aggregated diagnostics.

    Args:
        df (DataFrame): A dataframe with aggregated diagnostics.

    Returns:
        DataFrame: A dataframe with summary diagnostics by primary_service_type and estimate_source.
    """
    summary_df = df.select(
        IndCQC.primary_service_type,
        IndCQC.estimate_source,
        IndCQC.distribution_mean,
        IndCQC.distribution_standard_deviation,
        IndCQC.distribution_kurtosis,
        IndCQC.distribution_skewness,
        IndCQC.average_absolute_residual,
        IndCQC.average_percentage_residual,
        IndCQC.max_residual,
        IndCQC.min_residual,
        IndCQC.percentage_of_residuals_within_absolute_value,
        IndCQC.percentage_of_residuals_within_percentage_value,
        IndCQC.percentage_of_standardised_residuals_within_limit,
    ).distinct()
    return summary_df
