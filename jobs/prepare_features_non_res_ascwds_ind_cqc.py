import sys
from typing import List

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
    PartitionKeys as Keys,
)
from utils.column_values.categorical_column_values import CareHome
from utils.feature_engineering_resources.feature_engineering_region import (
    FeatureEngineeringValueLabelsRegion as RegionFeatures,
)
from utils.feature_engineering_resources.feature_engineering_related_location import (
    FeatureEngineeringValueLabelsRelatedLocation as RelatedLocationFeatures,
)
from utils.feature_engineering_resources.feature_engineering_rural_urban import (
    FeatureEngineeringValueLabelsRuralUrban as RuralUrbanFeatures,
)
from utils.feature_engineering_resources.feature_engineering_services import (
    FeatureEngineeringValueLabelsServices as ServicesFeatures,
)
from utils.feature_engineering_resources.feature_engineering_specialisms import (
    FeatureEngineeringValueLabelsSpecialisms as SpecialismsFeatures,
)
from utils.features.helper import (
    add_array_column_count,
    add_date_index_column,
    cap_integer_at_max_value,
    expand_encode_and_extract_features,
    filter_without_dormancy_features_to_pre_2025,
    group_rural_urban_sparse_categories,
    vectorise_dataframe,
)


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

    features_df = add_array_column_count(
        filtered_df, IndCQC.service_count, IndCQC.services_offered
    )
    features_df = cap_integer_at_max_value(
        features_df,
        IndCQC.service_count,
        max_value=4,
        new_col_name=IndCQC.service_count_capped,
    )

    features_df = add_array_column_count(
        features_df, IndCQC.activity_count, IndCQC.imputed_regulated_activities
    )
    features_df = cap_integer_at_max_value(
        features_df,
        IndCQC.activity_count,
        max_value=3,
        new_col_name=IndCQC.activity_count_capped,
    )

    features_df, service_list = expand_encode_and_extract_features(
        features_df,
        IndCQC.services_offered,
        ServicesFeatures.non_res_labels_dict,
        is_array_col=True,
    )

    features_df, specialisms_list = expand_encode_and_extract_features(
        features_df,
        IndCQC.specialisms_offered,
        SpecialismsFeatures.labels_dict,
        is_array_col=True,
    )

    features_df = group_rural_urban_sparse_categories(features_df)
    features_df, rui_indicators_list = expand_encode_and_extract_features(
        features_df,
        IndCQC.current_rural_urban_indicator_2011_for_non_res_model,
        RuralUrbanFeatures.non_res_labels_dict,
        is_array_col=False,
    )

    features_df, region_list = expand_encode_and_extract_features(
        features_df,
        IndCQC.current_region,
        RegionFeatures.labels_dict,
        is_array_col=False,
    )

    features_df, related_location = expand_encode_and_extract_features(
        features_df,
        IndCQC.related_location,
        RelatedLocationFeatures.labels_dict,
        is_array_col=False,
    )

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

    without_dormancy_feature_list: List[str] = sorted(
        [
            IndCQC.activity_count_capped,
            IndCQC.cqc_location_import_date_indexed,
            IndCQC.posts_rolling_average_model,
            IndCQC.service_count_capped,
            IndCQC.time_registered_capped_at_four_years,
        ]
        + related_location
        + service_list
        + specialisms_list
        + region_list
        + rui_indicators_list
    )

    vectorised_features_without_dormancy_df = vectorise_dataframe(
        without_dormancy_features_df,
        without_dormancy_feature_list,
    )

    print(f"number of features without dormancy: {len(without_dormancy_feature_list)}")
    print(
        f"length of features without dormancy df: {vectorised_features_without_dormancy_df.count()}"
    )

    # With dormancy features

    with_dormancy_features_df = utils.select_rows_with_non_null_value(
        features_df, IndCQC.dormancy
    )

    with_dormancy_features_df = add_date_index_column(with_dormancy_features_df)

    with_dormancy_features_df = cap_integer_at_max_value(
        with_dormancy_features_df,
        IndCQC.time_registered,
        max_value=120,
        new_col_name=IndCQC.time_registered_capped_at_ten_years,
    )

    """ Features cannot be null, and in order to help the model learn that locations which are not dormant
    are larger than those which are, we have entered a large value (999) for locations who have either never
    been dormant, or before they first become dormant."""
    with_dormancy_features_df = with_dormancy_features_df.fillna(
        999, subset=[IndCQC.time_since_dormant]
    )

    with_dormancy_feature_list: List[str] = sorted(
        [
            IndCQC.activity_count_capped,
            IndCQC.cqc_location_import_date_indexed,
            IndCQC.posts_rolling_average_model,
            IndCQC.service_count_capped,
            IndCQC.time_registered_capped_at_ten_years,
            IndCQC.time_since_dormant,
        ]
        + related_location
        + service_list
        + specialisms_list
        + region_list
        + rui_indicators_list
    )

    vectorised_features_with_dormancy_df = vectorise_dataframe(
        with_dormancy_features_df, with_dormancy_feature_list
    )

    print(f"number of features with dormancy: {len(with_dormancy_feature_list)}")
    print(
        f"length of features with dormancy df: {vectorised_features_with_dormancy_df.count()}"
    )

    print(
        f"Exporting vectorised_features_without_dormancy_df as parquet to {without_dormancy_features_destination}"
    )
    utils.write_to_parquet(
        vectorised_features_without_dormancy_df,
        without_dormancy_features_destination,
        mode="overwrite",
        partitionKeys=[Keys.year, Keys.month, Keys.day, Keys.import_date],
    )

    print(
        f"Exporting vectorised_features_with_dormancy_df as parquet to {with_dormancy_features_destination}"
    )
    utils.write_to_parquet(
        vectorised_features_with_dormancy_df,
        with_dormancy_features_destination,
        mode="overwrite",
        partitionKeys=[Keys.year, Keys.month, Keys.day, Keys.import_date],
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
