import polars as pl

from projects._03_independent_cqc._04_model.utils.paths import generate_predictions_path
from polars_utils import utils
from polars_utils.cleaning_utils import calculate_filled_posts_from_beds_and_ratio
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc


def enrich_with_model_predictions(
    lf: pl.LazyFrame, bucket_name: str, model_name: str
) -> pl.LazyFrame:
    """
    Loads model predictions, applies transformations and joins into the input
    LazyFrame.

    This function calls utility functions to: - load model predictions from the
    specified data bucket - convert predicted ratios to filled posts (care home
    specific) - set minimum values for predictions - join the model predictions
    into the input LazyFrame - return the updated LazyFrame.

    Args:
        lf (pl.LazyFrame): The input LazyFrame.
        bucket_name (str): the bucket (name only) in which to source the model
            predictions dataset from.
        model_name (str): The name of the model.

    Returns:
        pl.LazyFrame: The input LazyFrame with the model predictions merged in.
    """
    predictions_path = generate_predictions_path(bucket_name, model_name)

    predictions_lf = utils.scan_parquet(predictions_path)

    if model_name == IndCqc.care_home_model:
        predictions_lf = calculate_filled_posts_from_beds_and_ratio(
            predictions_lf, IndCqc.prediction, IndCqc.prediction
        )

    ind_cqc_with_predictions_lf = join_model_predictions(
        lf, predictions_lf, model_name, include_run_id=True
    )

    return ind_cqc_with_predictions_lf


def join_model_predictions(
    lf: pl.LazyFrame,
    predictions_lf: pl.LazyFrame,
    model_name: str,
    include_run_id: bool = True,
) -> pl.LazyFrame:
    """
    Prepares the predictions LazyFrame then joins them into the input LazyFrame
    on location ID and import date.

    Selects location ID and import date for joining on plus the predicted values
    and run ID, if required. The generic prediction columns are renamed to be
    model-specific so it is clear which model they relate to after the join. The
    prepared predictions LazyFrame is then left-joined into the input LazyFrame.

    Args:
        lf (pl.LazyFrame): The input LazyFrame to join predictions into.
        predictions_lf (pl.LazyFrame): The input LazyFrame containing model
            predictions.
        model_name (str): The name of the model to use for renaming
            columns.
        include_run_id (bool): Whether to include the prediction run ID
            column.

    Returns:
        pl.LazyFrame: The input LazyFrame with the model predictions joined in.
    """
    cols = [
        IndCqc.location_id,
        IndCqc.cqc_location_import_date,
        IndCqc.prediction,
    ]

    rename_map = {
        IndCqc.prediction: model_name,
    }

    if include_run_id:
        cols.append(IndCqc.prediction_run_id)
        rename_map[IndCqc.prediction_run_id] = f"{model_name}_run_id"

    prepared_predictions_lf = predictions_lf.select(cols).rename(rename_map)

    return lf.join(
        prepared_predictions_lf,
        [IndCqc.location_id, IndCqc.cqc_location_import_date],
        "left",
    )
