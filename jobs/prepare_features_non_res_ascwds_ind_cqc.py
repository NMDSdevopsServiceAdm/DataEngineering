import sys

from pyspark.sql import DataFrame
from typing import List, Tuple

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
    calculate_time_registered_for,
    cap_integer_at_max_value,
    column_expansion_with_dict,
    convert_categorical_variable_to_binary_variables_based_on_a_dictionary,
    group_rural_urban_sparse_categories,
    lag_column_value,
    vectorise_dataframe,
)


def main(
    ind_cqc_filled_posts_cleaned_source: str,
    non_res_ascwds_inc_dormancy_ind_cqc_features_destination: str,
    non_res_ascwds_without_dormancy_ind_cqc_features_destination: str,
) -> DataFrame:
    print("Creating non res ascwds inc dormancy features dataset...")

    df = utils.read_from_parquet(ind_cqc_filled_posts_cleaned_source)

    features_df = create_general_non_res_feature_columns(df)

    model_without_dormancy_features_df = (
        create_features_specific_to_without_dormancy_model(features_df)
    )

    model_with_dormancy_features_df = create_features_specific_to_with_dormancy_model(
        features_df
    )

    without_dormancy_feature_list, with_dormancy_feature_list = create_feature_lists()

    print(f"number of features without dormancy: {len(without_dormancy_feature_list)}")
    print(f"number of features with dormancy: {len(with_dormancy_feature_list)}")

    vectorised_features_without_dormancy_df = vectorise_dataframe(
        model_without_dormancy_features_df, without_dormancy_feature_list
    )
    vectorised_features_with_dormancy_df = vectorise_dataframe(
        model_with_dormancy_features_df, with_dormancy_feature_list
    )

    print(
        f"Exporting non_res_ascwds_without_dormancy_ind_cqc_features as parquet to {non_res_ascwds_without_dormancy_ind_cqc_features_destination}"
    )
    utils.write_to_parquet(
        vectorised_features_without_dormancy_df,
        non_res_ascwds_without_dormancy_ind_cqc_features_destination,
        mode="overwrite",
        partitionKeys=[Keys.year, Keys.month, Keys.day, Keys.import_date],
    )

    print(
        f"Exporting non_res_ascwds_inc_dormancy_ind_cqc_features as parquet to {non_res_ascwds_inc_dormancy_ind_cqc_features_destination}"
    )
    utils.write_to_parquet(
        vectorised_features_with_dormancy_df,
        non_res_ascwds_inc_dormancy_ind_cqc_features_destination,
        mode="overwrite",
        partitionKeys=[Keys.year, Keys.month, Keys.day, Keys.import_date],
    )


# TODO add tests
def create_general_non_res_feature_columns(df: DataFrame) -> DataFrame:
    """
    Create features which apply to both non-res models.

    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: The output DataFrame with the added features.
    """
    df = utils.select_rows_with_value(df, IndCQC.care_home, CareHome.not_care_home)

    df = add_array_column_count(
        df, new_col_name=IndCQC.service_count, col_to_check=IndCQC.services_offered
    )
    df = cap_integer_at_max_value(
        df,
        col_name=IndCQC.service_count,
        max_value=4,
        new_col_name=IndCQC.service_count_capped,
    )

    df = add_array_column_count(
        df,
        new_col_name=IndCQC.activity_count,
        col_to_check=IndCQC.imputed_regulated_activities,
    )
    df = cap_integer_at_max_value(
        df,
        col_name=IndCQC.activity_count,
        max_value=3,
        new_col_name=IndCQC.activity_count_capped,
    )

    df = column_expansion_with_dict(
        df,
        col_name=IndCQC.services_offered,
        lookup_dict=ServicesFeatures.non_res_model_labels_dict,
    )

    df = column_expansion_with_dict(
        df,
        col_name=IndCQC.specialisms_offered,
        lookup_dict=SpecialismsFeatures.labels_dict,
    )

    df = group_rural_urban_sparse_categories(df)
    df = convert_categorical_variable_to_binary_variables_based_on_a_dictionary(
        df,
        categorical_col_name=IndCQC.current_rural_urban_indicator_2011_for_non_res_model,
        lookup_dict=RuralUrbanFeatures.non_res_model_labels_dict,
    )

    df = convert_categorical_variable_to_binary_variables_based_on_a_dictionary(
        df,
        categorical_col_name=IndCQC.current_region,
        lookup_dict=RegionFeatures.labels_dict,
    )

    df = convert_categorical_variable_to_binary_variables_based_on_a_dictionary(
        df,
        categorical_col_name=IndCQC.related_location,
        lookup_dict=RelatedLocationFeatures.labels_dict,
    )

    df = calculate_time_registered_for(df)

    df = lag_column_value(
        df, IndCQC.posts_rolling_average_model, IndCQC.posts_rolling_average_model_lag
    )

    return df


# TODO add tests
def create_features_specific_to_without_dormancy_model(df: DataFrame) -> DataFrame:
    """
    Create additional features specific to the model without dormancy.

    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: The output DataFrame with the added features.
    """
    df = add_date_index_column(df)

    df = cap_integer_at_max_value(
        df,
        col_name=IndCQC.time_registered,
        max_value=48,
        new_col_name=IndCQC.time_registered_capped_at_four_years,
    )
    return df


# TODO add tests
def create_features_specific_to_with_dormancy_model(df: DataFrame) -> DataFrame:
    """
    Create additional features specific to the model with dormancy.

    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: The output DataFrame with the added features.
    """
    df = utils.select_rows_with_non_null_value(df, IndCQC.dormancy)

    df = convert_categorical_variable_to_binary_variables_based_on_a_dictionary(
        df,
        categorical_col_name=IndCQC.dormancy,
        lookup_dict=DormancyFeatures.labels_dict,
    )

    df = add_date_index_column(df)

    df = cap_integer_at_max_value(
        df,
        col_name=IndCQC.time_registered,
        max_value=120,
        new_col_name=IndCQC.time_registered_capped_at_ten_years,
    )

    return df


# TODO add tests
def create_feature_lists() -> Tuple[List[str], List[str]]:
    """
    Create lists of features for vectorisation.

    Returns:
        Tuple[List[str], List[str]]: A tuple containing two lists of feature names.
    """
    dormancy_key = list(DormancyFeatures.labels_dict.keys())
    regions = list(RegionFeatures.labels_dict.keys())
    related_location = list(RelatedLocationFeatures.labels_dict.keys())
    rui_indicators = list(RuralUrbanFeatures.non_res_model_labels_dict.keys())
    service_keys = list(ServicesFeatures.non_res_model_labels_dict.keys())
    specialisms_keys = list(SpecialismsFeatures.labels_dict.keys())

    without_dormancy_feature_list: List[str] = sorted(
        [
            IndCQC.activity_count_capped,
            IndCQC.cqc_location_import_date_indexed,
            IndCQC.posts_rolling_average_model,
            IndCQC.posts_rolling_average_model_lag,
            IndCQC.service_count_capped,
            IndCQC.time_registered_capped_at_four_years,
        ]
        + regions
        + related_location
        + rui_indicators
        + service_keys
        + specialisms_keys
    )

    with_dormancy_feature_list: List[str] = sorted(
        [
            IndCQC.activity_count_capped,
            IndCQC.cqc_location_import_date_indexed,
            IndCQC.posts_rolling_average_model,
            IndCQC.posts_rolling_average_model_lag,
            IndCQC.service_count_capped,
            IndCQC.time_registered_capped_at_ten_years,
        ]
        + dormancy_key
        + regions
        + related_location
        + rui_indicators
        + service_keys
        + specialisms_keys
    )

    return without_dormancy_feature_list, with_dormancy_feature_list


if __name__ == "__main__":
    print("Spark job 'prepare_features_non_res_ascwds_ind_cqc' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        ind_cqc_filled_posts_cleaned_source,
        non_res_ascwds_inc_dormancy_ind_cqc_features_destination,
        non_res_ascwds_without_dormancy_ind_cqc_features_destination,
    ) = utils.collect_arguments(
        (
            "--ind_cqc_filled_posts_cleaned_source",
            "Source s3 directory for ind_cqc_filled_posts_cleaned dataset",
        ),
        (
            "--non_res_ascwds_inc_dormancy_ind_cqc_features_destination",
            "A destination directory for outputting non-res ASCWDS inc dormancy model features dataset",
        ),
        (
            "--non_res_ascwds_without_dormancy_ind_cqc_features_destination",
            "A destination directory for outputting non-res ASCWDS without dormancy model features dataset",
        ),
    )

    main(
        ind_cqc_filled_posts_cleaned_source,
        non_res_ascwds_inc_dormancy_ind_cqc_features_destination,
        non_res_ascwds_without_dormancy_ind_cqc_features_destination,
    )

    print("Spark job 'prepare_features_non_res_ascwds_ind_cqc' complete")
