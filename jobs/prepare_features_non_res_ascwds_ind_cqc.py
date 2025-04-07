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
    expand_encode_and_extract_features,
    vectorise_dataframe,
)


vectorised_features_column_list: List[str] = [
    IndCQC.location_id,
    IndCQC.cqc_location_import_date,
    IndCQC.current_region,
    IndCQC.current_rural_urban_indicator_2011,
    IndCQC.dormancy,
    IndCQC.care_home,
    IndCQC.service_count,
    IndCQC.activity_count,
    IndCQC.specialism_count,
    IndCQC.ascwds_pir_merged,
    IndCQC.ascwds_rate_of_change_trendline_model,
    IndCQC.imputed_registration_date,
    IndCQC.time_registered,
    IndCQC.features,
    Keys.year,
    Keys.month,
    Keys.day,
    Keys.import_date,
]


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

    features_df = add_array_column_count(
        df=non_res_locations_df,
        new_col_name=IndCQC.service_count,
        col_to_check=IndCQC.services_offered,
    )
    features_df = add_array_column_count(
        df=features_df,
        new_col_name=IndCQC.activity_count,
        col_to_check=IndCQC.imputed_regulated_activities,
    )
    features_df = add_array_column_count(
        df=features_df,
        new_col_name=IndCQC.specialism_count,
        col_to_check=IndCQC.imputed_specialisms,
    )

    features_df, service_list = expand_encode_and_extract_features(
        features_df,
        IndCQC.services_offered,
        ServicesFeatures.labels_dict,
        is_array_col=True,
    )

    features_df, specialisms_list = expand_encode_and_extract_features(
        features_df,
        IndCQC.specialisms_offered,
        SpecialismsFeatures.labels_dict,
        is_array_col=True,
    )

    features_df, rui_indicators_list = expand_encode_and_extract_features(
        features_df,
        IndCQC.current_rural_urban_indicator_2011,
        RuralUrbanFeatures.labels_dict,
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

    features_df, dormancy_key = expand_encode_and_extract_features(
        features_df,
        IndCQC.dormancy,
        DormancyFeatures.labels_dict,
        is_array_col=False,
    )

    features_with_known_dormancy_df = utils.select_rows_with_non_null_value(
        features_df, IndCQC.dormancy
    )

    list_for_vectorisation_without_dormancy: List[str] = sorted(
        [
            IndCQC.service_count,
            IndCQC.activity_count,
            IndCQC.specialism_count,
            IndCQC.time_registered,
            IndCQC.ascwds_rate_of_change_trendline_model,
        ]
        + service_list
        + region_list
        + rui_indicators_list
    )
    list_for_vectorisation_with_dormancy: List[str] = sorted(
        list_for_vectorisation_without_dormancy + dormancy_key
    )

    vectorised_features_without_dormancy_df = vectorise_dataframe(
        df=features_df,
        list_for_vectorisation=list_for_vectorisation_without_dormancy,
    )
    vectorised_features_with_dormancy_df = vectorise_dataframe(
        df=features_with_known_dormancy_df,
        list_for_vectorisation=list_for_vectorisation_with_dormancy,
    )

    vectorised_features_without_dormancy_df = (
        vectorised_features_without_dormancy_df.select(vectorised_features_column_list)
    )
    vectorised_features_with_dormancy_df = vectorised_features_with_dormancy_df.select(
        vectorised_features_column_list
    )

    print(
        f"number of features without dormancy: {len(list_for_vectorisation_without_dormancy)}"
    )
    print(
        f"length of features without dormancy df: {vectorised_features_without_dormancy_df.count()}"
    )

    print(
        f"number of features with dormancy: {len(list_for_vectorisation_with_dormancy)}"
    )
    print(
        f"length of features with dormancy df: {vectorised_features_with_dormancy_df.count()}"
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
