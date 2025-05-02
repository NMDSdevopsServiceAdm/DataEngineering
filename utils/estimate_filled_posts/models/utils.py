import boto3
import re

from typing import Optional, List, Tuple
from pyspark.sql import DataFrame, functions as F
from pyspark.ml.regression import LinearRegression, LinearRegressionModel

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
from utils.column_values.categorical_column_values import CareHome, PrimaryServiceType
from utils import utils


def insert_predictions_into_pipeline(
    locations_df: DataFrame,
    predictions_df: DataFrame,
    model_column_name: str,
) -> DataFrame:
    """
    Inserts model predictions into locations dataframe.

    This function renames the model prediction column and performs a left join
    to merge it into the locations dataframe based on matching 'location_id'
    and 'cqc_location_import_date' values.

    Args:
        locations_df (DataFrame): A dataframe containing independent CQC data.
        predictions_df (DataFrame): A dataframe containing model predictions.
        model_column_name (str): The name of the column containing the model predictions.

    Returns:
        DataFrame: A dataframe with model predictions added.
    """
    predictions_df = predictions_df.select(
        IndCqc.location_id, IndCqc.cqc_location_import_date, IndCqc.prediction
    ).withColumnRenamed(IndCqc.prediction, model_column_name)

    locations_with_predictions = locations_df.join(
        predictions_df, [IndCqc.location_id, IndCqc.cqc_location_import_date], "left"
    )

    return locations_with_predictions


def set_min_value(df: DataFrame, col_name: str, min_value: float = 1.0) -> DataFrame:
    """
    The function takes the greatest value between the existing value and the specified min_value which defaults to 1.0.

    Args:
        df (DataFrame): A dataframe containing the specified column.
        col_name (str): The name of the column to set the minimum value for.
        min_value (float): The minimum value allowed in the specified column.

    Returns:
        DataFrame: A dataframe with the specified column set to greatest value of the original value or the min_value.
    """
    return df.withColumn(
        col_name,
        F.when(
            F.col(col_name).isNotNull(),
            F.greatest(F.col(col_name), F.lit(min_value)),
        ).otherwise(F.lit(None)),
    )


def combine_care_home_ratios_and_non_res_posts(
    df: DataFrame, ratio_column: str, posts_column: str, new_column_name: str
) -> DataFrame:
    """
    Creates one column which inputs the ratio value if the location is a care home and the filled post value if not.

    Args:
        df (DataFrame): The input DataFrame.
        ratio_column (str): The name of the filled posts per bed ratio column (for care homes only).
        posts_column (str): The name of the filled posts column.
        new_column_name (str): The name of the new column with combined values.

    Returns:
        DataFrame: The input DataFrame with the new column containing a single column with the relevant combined column.
    """
    df = df.withColumn(
        new_column_name,
        F.when(
            F.col(IndCqc.care_home) == CareHome.care_home,
            F.col(ratio_column),
        ).otherwise(F.col(posts_column)),
    )
    return df


def clean_number_of_beds_banded(df: DataFrame) -> DataFrame:
    """
    Cleans the number_of_beds_banded column by merging together bands which have low bases.

    Bands below 3 are grouped together for locations with the primary service 'care home with nursing'.
    Bands below 2 are grouped together for locations with the primary service 'care home only'.
    All other bands remain as they were.

    Args:
        df (DataFrame): The input DataFrame containing the 'number_of_beds_banded' column.

    Returns:
        DataFrame: The input DataFrame with the new 'number_of_beds_banded_cleaned' column added.
    """
    band_two: float = 2.0
    band_three: float = 3.0

    df = df.withColumn(
        IndCqc.number_of_beds_banded_cleaned,
        F.when(
            (
                F.col(IndCqc.primary_service_type)
                == PrimaryServiceType.care_home_with_nursing
            )
            & (F.col(IndCqc.number_of_beds_banded) < band_three),
            F.lit(band_three),
        )
        .when(
            (F.col(IndCqc.primary_service_type) == PrimaryServiceType.care_home_only)
            & (F.col(IndCqc.number_of_beds_banded) < band_two),
            F.lit(band_two),
        )
        .otherwise(F.col(IndCqc.number_of_beds_banded)),
    )
    return df


def convert_care_home_ratios_to_filled_posts_and_merge_with_filled_post_values(
    df: DataFrame,
    ratio_column: str,
    posts_column: str,
) -> DataFrame:
    """
    Multiplies the filled posts per bed ratio values by the number of beds at each care home location to create a filled posts figure.

    If the location is not a care home, the original filled posts figure is kept.

    Args:
        df (DataFrame): The input DataFrame.
        ratio_column (str): The name of the filled posts per bed ratio column (for care homes only).
        posts_column (str): The name of the filled posts column.

    Returns:
        DataFrame: The input DataFrame with the new column containing a single column with the relevant combined column.
    """
    df = df.withColumn(
        posts_column,
        F.when(
            F.col(IndCqc.care_home) == CareHome.care_home,
            F.col(ratio_column) * F.col(IndCqc.number_of_beds),
        ).otherwise(F.col(posts_column)),
    )
    return df


def train_lasso_regression_model(
    df: DataFrame, label_col: str, model_name: str
) -> LinearRegressionModel:
    """
    Train a linear regression model on the given DataFrame.

    elasticNetParam=1 means that the model will use only L1 regularization (Lasso).
    Lasso regression is an algorithm that helps to identify the most important features in a
    dataset, allowing for more effective model building.

    The regulisation parameter (regParam) controls the strength of the regularisation.
    We set this to a low value (0.001) to allow the model to have a higher degree of freedom
    to capture more complex relationships in the data.

    Args:
        df (DataFrame): Training data.
        label_col (str): Name of the label column.
        model_name (str): The name of the model.

    Returns:
        LinearRegressionModel: Trained model.
    """
    lasso_regression: int = 1
    regulisation_parameter: float = 0.001

    lr = LinearRegression(
        featuresCol=IndCqc.features,
        labelCol=label_col,
        predictionCol=model_name,
        elasticNetParam=lasso_regression,
        regParam=regulisation_parameter,
    )
    return lr.fit(df)


def get_existing_run_numbers(model_source: str) -> List[int]:
    """
    List existing model run numbers in the specified S3 location.

    Args:
        model_source (str): S3 path to models (e.g. 's3://pipeline-resources/models/prediction/1.0.0/').

    Returns:
        List[int]: Sorted list of detected run numbers.
    """
    bucket, prefix = utils.split_s3_uri(model_source)

    s3 = boto3.client("s3")
    existing = s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
    run_numbers = []
    for obj in existing:
        match = re.search(r"run=(\d+)/$", obj["Key"])
        if match:
            run_numbers.append(int(match.group(1)))
    return sorted(set(run_numbers))


def generate_run_number(model_source: str, mode: str = "load") -> int:
    """
    Compute the S3 path for saving or loading a versioned model.

    Args:
        model_source (str): Base S3 path (eg. 's3://pipeline-resources/models/prediction/1.0.0/').
        mode (str): Either "load" to get latest run, or "save" to compute next run path.

    Returns:
        int: The relevant run number.

    Raises:
        ValueError: If mode is not "load" or "save".
        FileNotFoundError: If no existing model is found in load mode.
    """
    existing_runs = get_existing_run_numbers(model_source)

    if mode == "save":
        run_number = max(existing_runs) + 1 if existing_runs else 1

    elif mode == "load":
        if not existing_runs:
            raise FileNotFoundError(f"No model found in: {model_source}")
        run_number = max(existing_runs)

    else:
        raise ValueError("mode must be 'load' or 'save'")

    return run_number


def save_model_to_s3(model: LinearRegressionModel, model_s3_location: str) -> int:
    """
    Save model to the next available versioned S3 run path.

    Args:
        model (LinearRegressionModel): The trained linear regression model.
        model_s3_location (str): Base S3 path (eg. 's3://pipeline-resources/models/prediction/1.0.0/').

    Returns:
        int: The run number of the newly saved model.
    """
    run_number = generate_run_number(model_s3_location, mode="save")
    s3_path = f"{model_s3_location}run={run_number}/"
    model.save(s3_path)
    print(f"Model saved to: {s3_path}")
    return run_number


def load_latest_model_from_s3(model_s3_location: str) -> LinearRegressionModel:
    """
    Load the most recently saved model from a versioned S3 path.

    Args:
        model_s3_location (str): Base S3 path (eg. 's3://pipeline-resources/models/prediction/1.0.0/').

    Returns:
        LinearRegressionModel: The loaded model.
    """
    run_number = generate_run_number(model_s3_location, mode="load")
    s3_path = f"{model_s3_location}run={run_number}/"
    print(f"Loading model from: {s3_path}")
    return LinearRegressionModel.load(s3_path)


def create_test_and_train_datasets(
    df: DataFrame, test_ratio: float = 0.2, seed: Optional[int] = None
) -> Tuple[DataFrame, DataFrame]:
    """
    Split the DataFrame into training and testing datasets.

    Args:
        df (DataFrame): The input DataFrame to be split.
        test_ratio (float): The proportion of the data to include in the test split.
        seed (Optional[int]): Random seed for reproducibility.

    Returns:
        Tuple[DataFrame, DataFrame]: A tuple containing the training and testing DataFrames.
    """
    return df.randomSplit([1 - test_ratio, test_ratio], seed=seed)


def generate_model_features_s3_path(s3_datasets_uri: str, model_name: str) -> str:
    """
    Generate the S3 path for the features dataset.

    Args:
        s3_datasets_uri (str): The S3 URI of the datasets bucket (e.g. s3://sfc-branch-name-datasets).
        model_name (str): The name of the model.

    Returns:
        str: The S3 path for the features dataset.
    """
    return f"{s3_datasets_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_model_features/model_name={model_name}/"


def generate_model_s3_path(
    s3_datasets_uri: str, model_name: str, model_version: str
) -> str:
    """
    Generate the S3 path for the model dataset.

    The S3 URI is edited to reflect that the models are saved in the pipeline resources buckets, not the datasets bucket.

    Args:
        s3_datasets_uri (str): The S3 URI of the datasets bucket (e.g. s3://sfc-branch-name-datasets).
        model_name (str): The name of the model.
        model_version (str): The version of the model.

    Returns:
        str: The S3 path for the model dataset.
    """
    s3_pipeline_resources: str = s3_datasets_uri[:-8] + "pipeline-resources"
    return f"{s3_pipeline_resources}/models/{model_name}/{model_version}/"


def generate_model_predictions_s3_path(s3_datasets_uri: str, model_name: str) -> str:
    """
    Generate the S3 path for the features dataset.

    Args:
        s3_datasets_uri (str): The S3 URI of the datasets bucket (e.g. s3://sfc-branch-name-datasets).
        model_name (str): The name of the model.

    Returns:
        str: The S3 path for the predictions dataset.
    """
    return f"{s3_datasets_uri}/domain=ind_cqc_filled_posts/dataset=ind_cqc_model_predictions/model_name={model_name}/"
