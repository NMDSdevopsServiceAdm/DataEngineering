from pyspark.sql import DataFrame

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)
from utils.column_values.categorical_column_values import CareHome


def combine_non_res_with_and_without_dormancy_models(
    locations_df: DataFrame,
) -> DataFrame:
    """
    Creates a combined model prediction by adjusting the 'without_dormancy' model to align with
    the 'with_dormancy' model and applying residual corrections for smoothing.

    Args:
        locations_df (DataFrame): Input DataFrame containing 'without_dormancy' and 'with_dormancy' model predictions.

    Returns:
        DataFrame: The original DataFrame with the combined model predictions joined in.
    """
    locations_df = locations_df.select(
        IndCqc.location_id,
        IndCqc.cqc_location_import_date,
        IndCqc.related_location,
        IndCqc.time_registered,
        IndCqc.non_res_without_dormancy_model,
        IndCqc.non_res_with_dormancy_model,
    )

    non_res_locations_df = utils.select_rows_with_value(
        locations_df, IndCqc.care_home, CareHome.not_care_home
    )

    # TODO - 2 - calculate and apply model ratios

    # TODO - 3 - calculate and apply residuals

    # TODO - 4 - combine model predictions

    # TODO - 5 - set_min_value and insert predictions into pipeline

    return locations_df
