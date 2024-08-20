import sys

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
from utils.diagnostics_utils.create_charts_for_diagnostics import (
    create_charts_for_care_home_model_diagnostics,
)

partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]
estimate_filled_posts_columns: list = [
    IndCQC.location_id,
    IndCQC.cqc_location_import_date,
    IndCQC.ascwds_filled_posts_dedup_clean,
    IndCQC.primary_service_type,
    IndCQC.rolling_average_model,
    IndCQC.care_home_model,
    IndCQC.extrapolation_care_home_model,
    IndCQC.interpolation_model_ascwds_filled_posts_dedup_clean,
    IndCQC.interpolation_model_filled_posts_per_bed_ratio,
    IndCQC.non_res_with_dormancy_model,
    IndCQC.non_res_without_dormancy_model,
    IndCQC.extrapolation_non_res_with_dormancy_model,
    IndCQC.extrapolation_rolling_average_model,
    IndCQC.estimate_filled_posts,
    Keys.year,
    Keys.month,
    Keys.day,
    Keys.import_date,
]
absolute_value_cutoff: float = 10.0
percentage_value_cutoff: float = 0.25
standardised_value_cutoff: float = 1.0


def main(
    estimate_filled_posts_source,
    diagnostics_destination,
    summary_diagnostics_destination,
    charts_destination,
):
    print("Creating diagnostics for known filled posts")

    filled_posts_df: DataFrame = utils.read_from_parquet(
        estimate_filled_posts_source, estimate_filled_posts_columns
    )
    filled_posts_df = filter_to_known_values(
        filled_posts_df, IndCQC.ascwds_filled_posts_dedup_clean
    )
    filled_posts_df = restructure_dataframe_to_column_wise(filled_posts_df)
    filled_posts_df = filter_to_known_values(filled_posts_df, IndCQC.estimate_value)

    window = create_window_for_model_and_service_splits()

    filled_posts_df = calculate_distribution_metrics(filled_posts_df, window)
    filled_posts_df = calculate_residuals(filled_posts_df)
    filled_posts_df = calculate_aggregate_residuals(filled_posts_df, window)
    summary_df = create_summary_diagnostics_table(filled_posts_df)

    create_charts_for_care_home_model_diagnostics(filled_posts_df, charts_destination)

    utils.write_to_parquet(
        filled_posts_df,
        diagnostics_destination,
        mode="overwrite",
        partitionKeys=partition_keys,
    )
    utils.write_to_parquet(
        summary_df,
        summary_diagnostics_destination,
        mode="overwrite",
        partitionKeys=[IndCQC.primary_service_type],
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


def restructure_dataframe_to_column_wise(df: DataFrame) -> DataFrame:
    """
    Reshapes the dataframe so that all estimated values are in one column.

    This function reshapes the dataframe to make it easier to calculate the aggregations
    using a window function. Model values that were previously in separate columns for
    each model are joined into a single column with a corresponding column describing which
    model they are from. This means that the unique index of the dataframe with now be the
    combination of location id, cqc location import date and estimate source.

    Args:
        df (DataFrame): A dataframe of estimates with each model's values in a different column.

    Returns:
        DataFrame: A dataframe of estimates with each model's values in a single column and a column of corresponding model names.
    """
    reshaped_df = create_empty_reshaped_dataframe()
    list_of_models = create_list_of_models()
    for model in list_of_models:
        model_df = df.select(
            IndCQC.location_id,
            IndCQC.cqc_location_import_date,
            IndCQC.primary_service_type,
            IndCQC.ascwds_filled_posts_dedup_clean,
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


def create_list_of_models():
    """
    Creates a list of models to include in the reshaping of the dataframe.

    This function creates a list of the column names of models which will be used to reshape
    the dataframe.

    Returns:
        List(str): A list of strings of column names corresponding to the models to include.
    """
    list_of_models = (
        CatValues.estimate_filled_posts_source_column_values.categorical_values
    )
    list_of_models = list_of_models + [IndCQC.estimate_filled_posts]
    return list_of_models


def create_empty_reshaped_dataframe():
    """
    Creates an empty dataframe to define it's structure.

    This function creates a new, empty dataframe which uses a predefined schema with columns for the estimate's source and value.

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
                IndCQC.ascwds_filled_posts_dedup_clean,
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
        df (DataFrame): A dataframe with primary_service_type, estimate_source
        and estimate_value.
        window (Window): A window for aggregating the metrics.

    Returns:
        DataFrame: A dataframe with four additional columns containing distribution
        metrics aggregated over the given window.
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
        df (DataFrame): A dataframe with primary_service_type, estimate_source
        and estimate_value.
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
        df (DataFrame): A dataframe with primary_service_type, estimate_source
        and estimate_value.
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
        df (DataFrame): A dataframe with primary_service_type, estimate_source
        and estimate_value.
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
        df (DataFrame): A dataframe with primary_service_type, estimate_source
        and estimate_value.
        window (Window): A window for aggregating the metrics.

    Returns:
        DataFrame: A dataframe with an additional columns containing the skewness aggregated over the given window.
    """
    df = df.withColumn(
        IndCQC.distribution_skewness, F.skewness(df[IndCQC.estimate_value]).over(window)
    )
    return df


def calculate_residuals(df: DataFrame) -> DataFrame:
    """
    Adds columns with residuals.

    This function adds two columns to the dataset containing the absolute residual
    and the percentage residual.

    Args:
        df (DataFrame): A dataframe with ascwds_filled_posts_dedup_clean and estimate_value.

    Returns:
        DataFrame: A dataframe with two additional columns containing residuals.
    """
    df = calculate_residual(df)
    df = calculate_absolute_residual(df)
    df = calculate_percentage_residual(df)
    df = calculate_standardised_residual(df)
    return df


def calculate_residual(df: DataFrame) -> DataFrame:
    """
    Adds column with the residual.

    This function adds a columns to the dataset containing the residual.

    Args:
        df (DataFrame): A dataframe with ascwds_filled_posts_deduplicated_clean and estimate_value.

    Returns:
        DataFrame: A dataframe with an additional column containing the residual.
    """
    df = df.withColumn(
        IndCQC.residual,
        F.col(IndCQC.estimate_value) - F.col(IndCQC.ascwds_filled_posts_dedup_clean),
    )
    return df


def calculate_absolute_residual(df: DataFrame) -> DataFrame:
    """
    Adds column with the absolute residual.

    This function adds a columns to the dataset containing the absolute residual.

    Args:
        df (DataFrame): A dataframe with ascwds_filled_posts_dedup_clean and estimate_value.

    Returns:
        DataFrame: A dataframe with an additional column containing the absolute residual.
    """
    df = df.withColumn(
        IndCQC.absolute_residual,
        F.abs(F.col(IndCQC.residual)),
    )
    return df


def calculate_percentage_residual(df: DataFrame) -> DataFrame:
    """
    Adds column with the percentage residual.

    This function adds a columns to the dataset containing the percentage residual.

    Args:
        df (DataFrame): A dataframe with ascwds_filled_posts_dedup_clean and estimate_value.

    Returns:
        DataFrame: A dataframe with an additional column containing the percentage residual.
    """
    df = df.withColumn(
        IndCQC.percentage_residual,
        (F.col(IndCQC.estimate_value) - F.col(IndCQC.ascwds_filled_posts_dedup_clean))
        / F.col(IndCQC.estimate_value),
    )
    return df


def calculate_standardised_residual(df: DataFrame) -> DataFrame:
    """
    Adds column with the standardised residual.

    This function adds a columns to the dataset containing the standardised residual.

    Args:
        df (DataFrame): A dataframe with ascwds_filled_posts_dedup_clean and estimate_value.

    Returns:
        DataFrame: A dataframe with an additional column containing the standardised residual.
    """
    df = df.withColumn(
        IndCQC.standardised_residual,
        F.col(IndCQC.residual) / F.sqrt(F.col(IndCQC.ascwds_filled_posts_dedup_clean)),
    )
    return df


def calculate_aggregate_residuals(df: DataFrame, window: Window) -> DataFrame:
    df = calculate_average_absolute_residual(df, window)
    df = calculate_average_percentage_residual(df, window)
    df = calculate_max_residual(df, window)
    df = calculate_min_residual(df, window)
    df = calculate_percentage_of_residuals_within_absolute_value_of_actual(df, window)
    df = calculate_percentage_of_residuals_within_percentage_value_of_actual(df, window)
    df = calculate_percentage_of_standardised_residuals_within_limit(df, window)
    return df


def calculate_average_absolute_residual(df: DataFrame, window: Window) -> DataFrame:
    """
    Adds column with the average absolute residual.

    This function adds a columns to the dataset containing the average absolute residual, aggregated over the given window.

    Args:
        df (DataFrame): A dataframe with primary_service_type, estimate_source
        and absolute_residual.
        window (Window): A window for aggregating the residuals.

    Returns:
        DataFrame: A dataframe with an additional column containing the average absolute residual aggregated over the given window.
    """
    df = df.withColumn(
        IndCQC.average_absolute_residual,
        F.mean(df[IndCQC.absolute_residual]).over(window),
    )
    return df


def calculate_average_percentage_residual(df: DataFrame, window: Window) -> DataFrame:
    """
    Adds column with the average percentage residual.

    This function adds a columns to the dataset containing the average percentage residual, aggregated over the given window.

    Args:
        df (DataFrame): A dataframe with primary_service_type, estimate_source
        and percentage_residual.
        window (Window): A window for aggregating the residuals.

    Returns:
        DataFrame: A dataframe with an additional column containing the average percentage residual aggregated over the given window.
    """
    df = df.withColumn(
        IndCQC.average_percentage_residual,
        F.mean(df[IndCQC.percentage_residual]).over(window),
    )
    return df


def calculate_max_residual(df: DataFrame, window: Window) -> DataFrame:
    """
    Adds column with the maximum residual.

    This function adds a columns to the dataset containing the maximum residual, aggregated over the given window.

    Args:
        df (DataFrame): A dataframe with primary_service_type, estimate_source
        and absolute_residual.
        window (Window): A window for aggregating the residuals.

    Returns:
        DataFrame: A dataframe with an additional column containing the maximum residual aggregated over the given window.
    """
    df = df.withColumn(
        IndCQC.max_residual,
        F.max(df[IndCQC.residual]).over(window),
    )
    return df


def calculate_min_residual(df: DataFrame, window: Window) -> DataFrame:
    """
    Adds column with the minimum residual.

    This function adds a columns to the dataset containing the minimum residual, aggregated over the given window.

    Args:
        df (DataFrame): A dataframe with primary_service_type, estimate_source
        and absolute_residual.
        window (Window): A window for aggregating the residuals.

    Returns:
        DataFrame: A dataframe with an additional column containing the minimum residual aggregated over the given window.
    """
    df = df.withColumn(
        IndCQC.min_residual,
        F.min(df[IndCQC.residual]).over(window),
    )
    return df


def calculate_percentage_of_residuals_within_absolute_value_of_actual(
    df: DataFrame, window: Window
) -> DataFrame:
    """
    Adds column with the percentage of residuals which are within an absolute value of the actual value.

    This function adds a columns to the dataset containing the percentage of residuals which are within an absolute value of the actual value, aggregated over the given window.

    Args:
        df (DataFrame): A dataframe with primary_service_type, estimate_source, ascwds_filled_posts_dedup_clean
        and absolute_residual.
        window (Window): A window for aggregating the residuals.

    Returns:
        DataFrame: A dataframe with an additional column containing the percentage of residuals which are within an absolute value of the actual value aggregated over the given window.
    """
    df = df.withColumn(
        IndCQC.percentage_of_residuals_within_absolute_value,
        F.count(
            F.when(df[IndCQC.absolute_residual] <= absolute_value_cutoff, True)
        ).over(window)
        / F.count(df[IndCQC.absolute_residual]).over(window),
    )
    return df


def calculate_percentage_of_residuals_within_percentage_value_of_actual(
    df: DataFrame, window: Window
) -> DataFrame:
    """
    Adds column with the percentage of residuals which are within a percentage value of the actual value.

    This function adds a columns to the dataset containing the percentage of residuals which are within a percentage value of the actual value, aggregated over the given window.

    Args:
        df (DataFrame): A dataframe with primary_service_type, estimate_source, ascwds_filled_posts_dedup_clean
        and percentage_residual.
        window (Window): A window for aggregating the residuals.

    Returns:
        DataFrame: A dataframe with an additional column containing the percentage of residuals which are within a percentage value of the actual value aggregated over the given window.
    """
    df = df.withColumn(
        IndCQC.percentage_of_residuals_within_percentage_value,
        F.count(
            F.when(df[IndCQC.percentage_residual] <= percentage_value_cutoff, True)
        ).over(window)
        / F.count(df[IndCQC.percentage_residual]).over(window),
    )
    return df


def calculate_percentage_of_standardised_residuals_within_limit(
    df: DataFrame, window: Window
) -> DataFrame:
    """
    Adds column with the percentage of standardised residuals which are within a predefined limit.

    This function adds a columns to the dataset containing the percentage of standardised residuals which are within a predefined limit, aggregated over the given window.

    Args:
        df (DataFrame): A dataframe with primary_service_type, estimate_source and standardised_residual.
        window (Window): A window for aggregating the residuals.

    Returns:
        DataFrame: A dataframe with an additional column containing the standardised percentage of residuals which are within a predefinied limit, aggregated over the given window.
    """
    df = df.withColumn(
        IndCQC.percentage_of_standardised_residuals_within_limit,
        F.count(
            F.when(df[IndCQC.standardised_residual] <= standardised_value_cutoff, True)
        ).over(window)
        / F.count(df[IndCQC.standardised_residual]).over(window),
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


if __name__ == "__main__":
    print("Spark job 'diagnostics_on_known_filled_posts' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        estimate_filled_posts_source,
        diagnostics_destination,
        summary_diagnostics_destination,
        charts_destination,
    ) = utils.collect_arguments(
        (
            "--estimate_filled_posts_source",
            "Source s3 directory for job_estimates",
        ),
        (
            "--diagnostics_destination",
            "A destination directory for outputting full diagnostics tables.",
        ),
        (
            "--summary_diagnostics_destination",
            "A destination directory for outputting summary diagnostics tables.",
        ),
        (
            "--charts_destination",
            "A destination bucket name for saving pdf charts of the diagnostics data.",
        ),
    )

    main(
        estimate_filled_posts_source,
        diagnostics_destination,
        summary_diagnostics_destination,
        charts_destination,
    )

    print("Spark job 'diagnostics_on_known_filled_posts' complete")
