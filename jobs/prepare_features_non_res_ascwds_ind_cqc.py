import sys
from typing import List

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
    PartitionKeys as Keys,
)
from utils.column_values.categorical_column_values import CareHome
from utils.feature_engineering_resources.feature_engineering_dormancy import (
    FeatureEngineeringValueLabelsDormancy as DormancyFeatures,
)
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
    add_log_column,
    calculate_time_registered_for,
    cap_integer_at_max_value,
    expand_encode_and_extract_features,
    group_rural_urban_sparse_categories,
    vectorise_dataframe,
)


def main(
    ind_cqc_filled_posts_cleaned_source: str,
    with_dormancy_features_destination: str,
    without_dormancy_features_destination: str,
):
    print("Creating non res ascwds inc dormancy features dataset...")

    df = utils.read_from_parquet(ind_cqc_filled_posts_cleaned_source)

    df = utils.select_rows_with_value(df, IndCQC.care_home, CareHome.not_care_home)

    df, service_list = expand_encode_and_extract_features(
        df,
        IndCQC.services_offered,
        ServicesFeatures.non_res_model_labels_dict,
        is_array_col=True,
    )
    df = add_array_column_count(df, IndCQC.service_count, IndCQC.services_offered)
    df = cap_integer_at_max_value(
        df,
        IndCQC.service_count,
        max_value=4,
        new_col_name=IndCQC.service_count_capped,
    )

    df = add_array_column_count(
        df, IndCQC.activity_count, IndCQC.imputed_regulated_activities
    )
    df = cap_integer_at_max_value(
        df,
        IndCQC.activity_count,
        max_value=3,
        new_col_name=IndCQC.activity_count_capped,
    )

    df, specialisms_list = expand_encode_and_extract_features(
        df,
        IndCQC.specialisms_offered,
        SpecialismsFeatures.labels_dict,
        is_array_col=True,
    )

    df = group_rural_urban_sparse_categories(df)
    df, rui_indicators_list = expand_encode_and_extract_features(
        df,
        IndCQC.current_rural_urban_indicator_2011_for_non_res_model,
        RuralUrbanFeatures.non_res_model_labels_dict,
        is_array_col=False,
    )

    df, region_list = expand_encode_and_extract_features(
        df,
        IndCQC.current_region,
        RegionFeatures.labels_dict,
        is_array_col=False,
    )

    df, related_location = expand_encode_and_extract_features(
        df,
        IndCQC.related_location,
        RelatedLocationFeatures.labels_dict,
        is_array_col=False,
    )

    df = calculate_time_registered_for(df)

    without_dormancy_features_df = add_date_index_column(df)
    without_dormancy_features_df = cap_integer_at_max_value(
        without_dormancy_features_df,
        col_name=IndCQC.time_registered,
        max_value=48,
        new_col_name=IndCQC.time_registered_capped_at_four_years,
    )
    without_dormancy_features_df = add_log_column(
        without_dormancy_features_df,
        IndCQC.time_registered_capped_at_four_years,
        IndCQC.time_registered_capped_at_four_years_logged,
    )

    with_dormancy_features_df = utils.select_rows_with_non_null_value(
        df, IndCQC.dormancy
    )

    with_dormancy_features_df, dormancy_key = expand_encode_and_extract_features(
        with_dormancy_features_df,
        IndCQC.dormancy,
        DormancyFeatures.labels_dict,
        is_array_col=False,
    )

    with_dormancy_features_df = add_date_index_column(with_dormancy_features_df)

    with_dormancy_features_df = cap_integer_at_max_value(
        with_dormancy_features_df,
        IndCQC.time_registered,
        max_value=120,
        new_col_name=IndCQC.time_registered_capped_at_ten_years,
    )
    with_dormancy_features_df = add_log_column(
        with_dormancy_features_df,
        IndCQC.time_registered_capped_at_ten_years,
        IndCQC.time_registered_capped_at_ten_years_logged,
    )

    without_dormancy_feature_list: List[str] = sorted(
        [
            IndCQC.activity_count_capped,
            IndCQC.cqc_location_import_date_indexed,
            IndCQC.posts_rolling_average_model,
            IndCQC.service_count_capped,
            IndCQC.time_registered_capped_at_four_years_logged,
        ]
        + region_list
        + related_location
        + rui_indicators_list
        + service_list
        + specialisms_list
    )

    with_dormancy_feature_list: List[str] = sorted(
        [
            IndCQC.activity_count_capped,
            IndCQC.cqc_location_import_date_indexed,
            IndCQC.posts_rolling_average_model,
            IndCQC.service_count_capped,
            IndCQC.time_registered_capped_at_ten_years_logged,
        ]
        + dormancy_key
        + region_list
        + related_location
        + rui_indicators_list
        + service_list
        + specialisms_list
    )

    print(f"number of features without dormancy: {len(without_dormancy_feature_list)}")
    print(f"number of features with dormancy: {len(with_dormancy_feature_list)}")

    vectorised_features_without_dormancy_df = vectorise_dataframe(
        without_dormancy_features_df, without_dormancy_feature_list
    )
    vectorised_features_with_dormancy_df = vectorise_dataframe(
        with_dormancy_features_df, with_dormancy_feature_list
    )

    print(
        f"Exporting non_res_ascwds_without_dormancy_ind_cqc_features as parquet to {without_dormancy_features_destination}"
    )
    utils.write_to_parquet(
        vectorised_features_without_dormancy_df,
        without_dormancy_features_destination,
        mode="overwrite",
        partitionKeys=[Keys.year, Keys.month, Keys.day, Keys.import_date],
    )

    print(
        f"Exporting non_res_ascwds_inc_dormancy_ind_cqc_features as parquet to {with_dormancy_features_destination}"
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
            "A destination directory for outputting non-res ASCWDS inc dormancy model features dataset",
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
