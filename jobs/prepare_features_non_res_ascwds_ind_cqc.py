import sys
from typing import List

from pyspark.sql import DataFrame

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
from utils.feature_engineering_resources.feature_engineering_rural_urban import (
    FeatureEngineeringValueLabelsRuralUrban as RuralUrbanFeatures,
)
from utils.feature_engineering_resources.feature_engineering_services import (
    FeatureEngineeringValueLabelsServices as ServicesFeatures,
)
from utils.features.helper import (
    vectorise_dataframe,
    column_expansion_with_dict,
    add_array_column_count_to_data,
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

    non_res_locations_df = utils.select_rows_with_value(
        locations_df, IndCQC.care_home, CareHome.not_care_home
    )

    features_df = add_array_column_count_to_data(
        df=non_res_locations_df,
        new_col_name=IndCQC.service_count,
        col_to_check=IndCQC.services_offered,
    )
    features_df = add_array_column_count_to_data(
        df=features_df,
        new_col_name=IndCQC.activity_count,
        col_to_check=IndCQC.regulated_activities,
    )
    features_df = add_array_column_count_to_data(
        df=features_df,
        new_col_name=IndCQC.specialism_count,
        col_to_check=IndCQC.specialisms,
    )

    service_keys = list(ServicesFeatures.labels_dict.keys())
    features_df = column_expansion_with_dict(
        df=features_df,
        col_name=IndCQC.services_offered,
        lookup_dict=ServicesFeatures.labels_dict,
    )

    rui_indicators = list(RuralUrbanFeatures.labels_dict.keys())
    features_df = (
        convert_categorical_variable_to_binary_variables_based_on_a_dictionary(
            df=features_df,
            categorical_col_name=IndCQC.current_rural_urban_indicator_2011,
            lookup_dict=RuralUrbanFeatures.labels_dict,
        )
    )

    regions = list(RegionFeatures.labels_dict.keys())
    features_df = (
        convert_categorical_variable_to_binary_variables_based_on_a_dictionary(
            df=features_df,
            categorical_col_name=IndCQC.current_region,
            lookup_dict=RegionFeatures.labels_dict,
        )
    )

    dormancy = list(DormancyFeatures.labels_dict.keys())
    features_df = (
        convert_categorical_variable_to_binary_variables_based_on_a_dictionary(
            df=features_df,
            categorical_col_name=IndCQC.dormancy,
            lookup_dict=DormancyFeatures.labels_dict,
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

    features_with_dormancy_df = utils.select_rows_with_non_null_value(
        features_df, IndCQC.dormancy
    )

    list_for_vectorisation_with_dormancy: List[str] = sorted(
        [
            IndCQC.service_count,
            IndCQC.activity_count,
            IndCQC.specialism_count,
            IndCQC.time_registered,
            IndCQC.rolling_average_model,
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
        IndCQC.current_rural_urban_indicator_2011,
        IndCQC.dormancy,
        IndCQC.service_count,
        IndCQC.activity_count,
        IndCQC.specialism_count,
        IndCQC.ascwds_filled_posts_dedup_clean,
        IndCQC.imputed_registration_date,
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
        df=features_df,
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
