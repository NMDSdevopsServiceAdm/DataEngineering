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
)


estimate_filled_posts_columns: list = [
    IndCQC.location_id,
    IndCQC.cqc_location_import_date,
    IndCQC.ascwds_filled_posts_clean,
    IndCQC.ascwds_filled_posts_dedup_clean,
    IndCQC.primary_service_type,
    IndCQC.rolling_average_model,
    IndCQC.care_home_model,
    IndCQC.extrapolation_care_home_model,
    IndCQC.interpolation_model,
    IndCQC.estimate_filled_posts,
]


def main(
    estimate_filled_posts_source,
    diagnostics_destination,
    summary_diagnostics_destination,
):
    print("Creating diagnostics for known filled posts")

    filled_posts_df: DataFrame = utils.read_from_parquet(
        estimate_filled_posts_source, estimate_filled_posts_columns
    )

    # filter to where ascwds clean is not null

    # reshape df so that cols are: location id, cqc_location_import date, service, ascwds_filled-posts_clean, estimate_source, estimate_value
    filled_posts_df = restructure_dataframe_to_column_wise(filled_posts_df)
    # drop rows where estimate value is missing

    # create windows for model/ service splits

    # calculate metrics for distribution (mean, sd, kurtosis, skewness)

    # calculate residuals (abs, %)

    # calculate aggregate residuals (avg abs, avg %, max, % within abs of actual, % within % of actual)

    # save tables to s3


def filter_to_known_values(df: DataFrame, column: str) -> DataFrame:
    return df


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
            IndCQC.ascwds_filled_posts_clean,
            model,
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
                IndCQC.ascwds_filled_posts_clean,
                FloatType(),
                True,
            ),
            StructField(IndCQC.estimate_source, FloatType(), True),
            StructField(IndCQC.estimate_value, FloatType(), True),
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
    df = calculate_absolute_residual(df)
    df = calculate_percentage_residual(df)
    return df


def calculate_absolute_residual(df: DataFrame) -> DataFrame:
    return df


def calculate_percentage_residual(df: DataFrame) -> DataFrame:
    return df


def calculate_aggreagte_residuals(df: DataFrame, window: Window) -> DataFrame:
    return df


if __name__ == "__main__":
    print("Spark job 'diagnostics_on_known_filled_posts' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        estimate_filled_posts_source,
        diagnostics_destination,
        summary_diagnostics_destination,
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
    )

    main(
        estimate_filled_posts_source,
        diagnostics_destination,
        summary_diagnostics_destination,
    )

    print("Spark job 'diagnostics_on_known_filled_posts' complete")
