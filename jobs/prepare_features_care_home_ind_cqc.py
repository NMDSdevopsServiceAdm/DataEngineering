import sys
from typing import List

from pyspark.sql import DataFrame

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
    PartitionKeys as Keys,
)
from utils.column_values.categorical_column_values import CareHome
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
    add_array_column_count,
    column_expansion_with_dict,
    convert_categorical_variable_to_binary_variables_based_on_a_dictionary,
    vectorise_dataframe,
)


def main(
    ind_cqc_filled_posts_cleaned_source: str,
    care_home_ind_cqc_features_destination: str,
) -> DataFrame:
    print("Creating care home features dataset...")

    locations_df = utils.read_from_parquet(ind_cqc_filled_posts_cleaned_source)

    filtered_loc_data = utils.select_rows_with_value(
        locations_df, IndCQC.care_home, CareHome.care_home
    )
    filtered_loc_data = utils.select_rows_with_non_null_value(
        filtered_loc_data, IndCQC.number_of_beds
    )

    features_df = add_array_column_count(
        df=filtered_loc_data,
        new_col_name=IndCQC.service_count,
        col_to_check=IndCQC.services_offered,
    )

    service_keys = list(ServicesFeatures.care_home_labels_dict.keys())
    features_df = column_expansion_with_dict(
        df=features_df,
        col_name=IndCQC.services_offered,
        lookup_dict=ServicesFeatures.care_home_labels_dict,
    )

    rui_indicators = list(RuralUrbanFeatures.care_home_labels_dict.keys())
    features_df = (
        convert_categorical_variable_to_binary_variables_based_on_a_dictionary(
            df=features_df,
            categorical_col_name=IndCQC.current_rural_urban_indicator_2011,
            lookup_dict=RuralUrbanFeatures.care_home_labels_dict,
        )
    )

    regions = list(RegionFeatures.care_home_labels_dict.keys())
    features_df = (
        convert_categorical_variable_to_binary_variables_based_on_a_dictionary(
            df=features_df,
            categorical_col_name=IndCQC.current_region,
            lookup_dict=RegionFeatures.care_home_labels_dict,
        )
    )

    list_for_vectorisation: List[str] = sorted(
        [
            IndCQC.service_count,
            IndCQC.number_of_beds,
            IndCQC.ascwds_rate_of_change_trendline_model,
        ]
        + service_keys
        + regions
        + rui_indicators
    )

    vectorised_dataframe = vectorise_dataframe(
        df=features_df, list_for_vectorisation=list_for_vectorisation
    )
    vectorised_features_df = vectorised_dataframe.select(
        IndCQC.location_id,
        IndCQC.cqc_location_import_date,
        IndCQC.current_region,
        IndCQC.number_of_beds,
        IndCQC.pir_people_directly_employed,
        IndCQC.care_home,
        IndCQC.features,
        IndCQC.ascwds_pir_merged,
        IndCQC.filled_posts_per_bed_ratio,
        IndCQC.ascwds_rate_of_change_trendline_model,
        Keys.year,
        Keys.month,
        Keys.day,
        Keys.import_date,
    )

    print("number_of_features:")
    print(len(list_for_vectorisation))
    print(f"length of feature df: {vectorised_dataframe.count()}")

    print(f"Exporting as parquet to {care_home_ind_cqc_features_destination}")

    utils.write_to_parquet(
        vectorised_features_df,
        care_home_ind_cqc_features_destination,
        mode="overwrite",
        partitionKeys=[Keys.year, Keys.month, Keys.day, Keys.import_date],
    )


if __name__ == "__main__":
    print("Spark job 'prepare_features_care_home_ind_cqc' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        ind_cqc_filled_posts_cleaned_source,
        care_home_ind_cqc_features_destination,
    ) = utils.collect_arguments(
        (
            "--ind_cqc_filled_posts_cleaned_source",
            "Source s3 directory for ind_cqc_filled_posts_cleaned dataset",
        ),
        (
            "--care_home_ind_cqc_features_destination",
            "A destination directory for outputting care_home_features_ind_cqc_filled_posts",
        ),
    )

    main(
        ind_cqc_filled_posts_cleaned_source,
        care_home_ind_cqc_features_destination,
    )

    print("Spark job 'prepare_features_care_home_ind_cqc' complete")
