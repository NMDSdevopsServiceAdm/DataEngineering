import os

os.environ["SPARK_VERSION"] = "3.5"

from projects._03_independent_cqc._04_feature_engineering.utils.helper import (
    add_squared_column,
    filter_without_dormancy_features_to_pre_2025,
    group_rural_urban_sparse_categories,
)
from projects._03_independent_cqc._04_feature_engineering.utils.value_labels import (
    RegionLabels,
    RelatedLocationLabels,
    RuralUrbanLabels,
    ServicesLabels,
    SpecialismsLabels,
)
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_values.categorical_column_values import CareHome


def main(
    ind_cqc_filled_posts_cleaned_source: str,
    with_dormancy_features_destination: str,
    without_dormancy_features_destination: str,
):
    print("Creating non res ascwds inc dormancy features dataset...")

    locations_df = utils.read_from_parquet(ind_cqc_filled_posts_cleaned_source)

    filtered_df = utils.select_rows_with_value(
        locations_df, IndCQC.care_home, CareHome.not_care_home
    )

    features_df = group_rural_urban_sparse_categories(features_df)

    # Without dormancy features

    without_dormancy_features_df = filter_without_dormancy_features_to_pre_2025(
        features_df
    )

    # With dormancy features

    with_dormancy_features_df = utils.select_rows_with_non_null_value(
        features_df, IndCQC.dormancy
    )
    with_dormancy_features_df = add_squared_column(
        with_dormancy_features_df, IndCQC.cqc_location_import_date_indexed
    )
