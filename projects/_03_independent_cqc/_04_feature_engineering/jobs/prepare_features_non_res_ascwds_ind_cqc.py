import os
import sys
from typing import List

os.environ["SPARK_VERSION"] = "3.5"

from projects._03_independent_cqc._04_feature_engineering.utils.helper import (
    add_date_index_column,
    add_squared_column,
    cap_integer_at_max_value,
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

    features_df = cap_integer_at_max_value(
        features_df,
        IndCQC.service_count,
        max_value=4,
        new_col_name=IndCQC.service_count_capped,
    )

    features_df = cap_integer_at_max_value(
        features_df,
        IndCQC.activity_count,
        max_value=3,
        new_col_name=IndCQC.activity_count_capped,
    )

    features_df = group_rural_urban_sparse_categories(features_df)

    # Without dormancy features

    without_dormancy_features_df = filter_without_dormancy_features_to_pre_2025(
        features_df
    )

    without_dormancy_features_df = add_date_index_column(without_dormancy_features_df)

    without_dormancy_features_df = cap_integer_at_max_value(
        without_dormancy_features_df,
        IndCQC.time_registered,
        max_value=48,
        new_col_name=IndCQC.time_registered_capped_at_four_years,
    )

    # With dormancy features

    with_dormancy_features_df = utils.select_rows_with_non_null_value(
        features_df, IndCQC.dormancy
    )

    with_dormancy_features_df = add_date_index_column(with_dormancy_features_df)
    with_dormancy_features_df = add_squared_column(
        with_dormancy_features_df, IndCQC.cqc_location_import_date_indexed
    )

    """ Features cannot be null, and in order to help the model learn that locations which are not dormant
    are larger than those which are, we have entered a large value (999) for locations who have either never
    been dormant, or before they first become dormant."""
    with_dormancy_features_df = with_dormancy_features_df.fillna(
        999, subset=[IndCQC.time_since_dormant]
    )

    print(
        f"Exporting vectorised_features_without_dormancy_df as parquet to {without_dormancy_features_destination}"
    )

    print(
        f"Exporting vectorised_features_with_dormancy_df as parquet to {with_dormancy_features_destination}"
    )


if __name__ == "__main__":
    print("Spark job 'prepare_features_non_res_ascwds_ind_cqc' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        ind_cqc_filled_posts_cleaned_source,
        with_dormancy_features_destination,
        without_dormancy_features_destination,
    ) = utils.collect_arguments(
        (
            "--ind_cqc_filled_posts_cleaned_source",
            "Source s3 directory for ind_cqc_filled_posts_cleaned dataset",
        ),
        (
            "--with_dormancy_features_destination",
            "A destination directory for outputting non-res ASCWDS with dormancy model features dataset",
        ),
        (
            "--without_dormancy_features_destination",
            "A destination directory for outputting non-res ASCWDS without dormancy model features dataset",
        ),
    )

    main(
        ind_cqc_filled_posts_cleaned_source,
        with_dormancy_features_destination,
        without_dormancy_features_destination,
    )

    print("Spark job 'prepare_features_non_res_ascwds_ind_cqc' complete")
