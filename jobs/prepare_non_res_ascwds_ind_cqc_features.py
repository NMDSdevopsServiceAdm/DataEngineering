import sys
from typing import List

import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from utils import utils

from utils.feature_engineering_dictionaries import (
    SERVICES_LOOKUP as services_dict,
    RURAL_URBAN_INDICATOR_LOOKUP as rural_urban_indicator_dict,
    REGION_LOOKUP as ons_region_dict,
    DORMANCY_LOOKUP as dormancy_dict,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
    PartitionKeys as Keys,
)
from utils.features.helper import (
    vectorise_dataframe,
    column_expansion_with_dict,
    add_service_count_to_data,
    convert_categorical_variable_to_binary_variables_based_on_a_dictionary,
    add_date_diff_into_df,
    add_time_registered_into_df,
)


def main(
    ind_cqc_filled_posts_cleaned_source: str,
    non_res_ascwds_inc_dormancy_ind_cqc_features_destination: str,
    non_res_ascwds_without_dormancy_ind_cqc_features_destination: str,
) -> DataFrame:
    print("Creating non res ascwds inc dormancy features dataset...")

    locations_df = utils.read_from_parquet(ind_cqc_filled_posts_cleaned_source)

    non_res_locations_df = filter_df_to_non_res_only(locations_df)

    features_df = add_service_count_to_data(
        df=non_res_locations_df,
        new_col_name=IndCQC.service_count,
        col_to_check=IndCQC.services_offered,
    )

    service_keys = list(services_dict.keys())
    features_df = column_expansion_with_dict(
        df=features_df,
        col_name=IndCQC.services_offered,
        lookup_dict=services_dict,
    )

    rui_indicators = list(rural_urban_indicator_dict.keys())
    features_df = (
        convert_categorical_variable_to_binary_variables_based_on_a_dictionary(
            df=features_df,
            categorical_col_name=IndCQC.current_rural_urban_indicator_2011,
            lookup_dict=rural_urban_indicator_dict,
        )
    )

    regions = list(ons_region_dict.keys())
    features_df = (
        convert_categorical_variable_to_binary_variables_based_on_a_dictionary(
            df=features_df,
            categorical_col_name=IndCQC.current_region,
            lookup_dict=ons_region_dict,
        )
    )

    dormancy = list(dormancy_dict.keys())
    features_df = (
        convert_categorical_variable_to_binary_variables_based_on_a_dictionary(
            df=features_df,
            categorical_col_name=IndCQC.dormancy,
            lookup_dict=dormancy_dict,
        )
    )

    features_df = add_date_diff_into_df(
        df=features_df,
        new_col_name=IndCQC.date_diff,
        import_date_col=IndCQC.cqc_location_import_date,
    )

    features_df = add_time_registered_into_df(
        df=features_df,
    )

    features_with_dormancy_df, features_without_dormancy_df = split_df_on_dormancy(
        features_df
    )

    list_for_vectorisation_with_dormancy: List[str] = sorted(
        [
            IndCQC.service_count,
            IndCQC.time_registered,
            IndCQC.date_diff,
        ]
        + dormancy
        + service_keys
        + regions
        + rui_indicators
    )

    vectorised_dataframe_with_dormancy = vectorise_dataframe(
        df=features_with_dormancy_df,
        list_for_vectorisation=list_for_vectorisation_with_dormancy,
    )
    vectorised_features_with_dormancy_df = vectorised_dataframe_with_dormancy.select(
        IndCQC.location_id,
        IndCQC.cqc_location_import_date,
        IndCQC.current_region,
        IndCQC.dormancy,
        IndCQC.care_home,
        IndCQC.ascwds_filled_posts_dedup_clean,
        IndCQC.imputed_registration_date,
        IndCQC.date_diff,
        IndCQC.time_registered,
        IndCQC.features,
        Keys.year,
        Keys.month,
        Keys.day,
        Keys.import_date,
    )

    print("number of features with dormancy:")
    print(len(list_for_vectorisation_with_dormancy))
    print(
        f"length of feature with dormancy df: {vectorised_dataframe_with_dormancy.count()}"
    )

    print(
        f"Exporting as parquet to {non_res_ascwds_inc_dormancy_ind_cqc_features_destination}"
    )
    utils.write_to_parquet(
        vectorised_features_with_dormancy_df,
        non_res_ascwds_inc_dormancy_ind_cqc_features_destination,
        mode="overwrite",
        partitionKeys=[Keys.year, Keys.month, Keys.day, Keys.import_date],
    )

    list_for_vectorisation_without_dormancy: List[str] = sorted(
        [
            IndCQC.service_count,
            IndCQC.time_registered,
            IndCQC.date_diff,
        ]
        + service_keys
        + regions
        + rui_indicators
    )

    vectorised_dataframe_without_dormancy = vectorise_dataframe(
        df=features_without_dormancy_df,
        list_for_vectorisation=list_for_vectorisation_without_dormancy,
    )
    vectorised_features_without_dormancy_df = (
        vectorised_dataframe_without_dormancy.select(
            IndCQC.location_id,
            IndCQC.cqc_location_import_date,
            IndCQC.current_region,
            IndCQC.dormancy,
            IndCQC.care_home,
            IndCQC.ascwds_filled_posts_dedup_clean,
            IndCQC.imputed_registration_date,
            IndCQC.date_diff,
            IndCQC.time_registered,
            IndCQC.features,
            Keys.year,
            Keys.month,
            Keys.day,
            Keys.import_date,
        )
    )

    print("number of features without dormancy:")
    print(len(list_for_vectorisation_without_dormancy))
    print(
        f"length of feature without dormancy df: {vectorised_dataframe_without_dormancy.count()}"
    )

    print(
        f"Exporting as parquet to {non_res_ascwds_without_dormancy_ind_cqc_features_destination}"
    )
    utils.write_to_parquet(
        vectorised_features_without_dormancy_df,
        non_res_ascwds_without_dormancy_ind_cqc_features_destination,
        mode="overwrite",
        partitionKeys=[Keys.year, Keys.month, Keys.day, Keys.import_date],
    )


def filter_df_to_non_res_only(df: DataFrame) -> DataFrame:
    return df.filter(F.col(IndCQC.care_home) == "N")


def split_df_on_dormancy(df: DataFrame) -> tuple:
    with_dormancy_df = df.filter(F.col(IndCQC.dormancy).isNotNull())
    without_dormancy_df = df.filter(F.col(IndCQC.dormancy).isNull())
    return with_dormancy_df, without_dormancy_df


if __name__ == "__main__":
    print("Spark job 'prepare_non_res_ascwds_ind_cqc_features' starting...")
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

    print("Spark job 'prepare_non_res_ascwds_ind_cqc_features' complete")
