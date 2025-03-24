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
from utils.feature_engineering_resources.feature_engineering_specialisms import (
    FeatureEngineeringValueLabelsSpecialisms as SpecialismsFeatures,
)
from utils.features.helper import (
    add_array_column_count,
    cap_integer_at_max_value,
    column_expansion_with_dict,
    convert_categorical_variable_to_binary_variables_based_on_a_dictionary,
    vectorise_dataframe,
)


def main(
    ind_cqc_filled_posts_cleaned_source: str,
    care_home_ind_cqc_features_destination: str,
) -> DataFrame:
    print("Creating care home features dataset...")

    df = utils.read_from_parquet(ind_cqc_filled_posts_cleaned_source)

    df = utils.select_rows_with_value(df, IndCQC.care_home, CareHome.care_home)

    df = utils.select_rows_with_non_null_value(df, IndCQC.number_of_beds)

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
        lookup_dict=ServicesFeatures.care_home_labels_dict,
    )

    df = column_expansion_with_dict(
        df,
        col_name=IndCQC.specialisms_offered,
        lookup_dict=SpecialismsFeatures.labels_dict,
    )

    df = convert_categorical_variable_to_binary_variables_based_on_a_dictionary(
        df,
        categorical_col_name=IndCQC.current_rural_urban_indicator_2011,
        lookup_dict=RuralUrbanFeatures.care_home_labels_dict,
    )

    df = convert_categorical_variable_to_binary_variables_based_on_a_dictionary(
        df,
        categorical_col_name=IndCQC.current_region,
        lookup_dict=RegionFeatures.labels_dict,
    )

    regions = list(RegionFeatures.labels_dict.keys())
    rui_indicators = list(RuralUrbanFeatures.care_home_labels_dict.keys())
    service_keys = list(ServicesFeatures.care_home_labels_dict.keys())
    specialisms_keys = list(SpecialismsFeatures.labels_dict.keys())

    feature_list: List[str] = sorted(
        [
            IndCQC.activity_count_capped,
            IndCQC.cqc_location_import_date_indexed,
            IndCQC.number_of_beds,
            IndCQC.ratio_rolling_average_model,
            IndCQC.service_count_capped,
        ]
        + regions
        + rui_indicators
        + service_keys
        + specialisms_keys
    )

    print(f"Number of features: {len(feature_list)}")

    vectorised_features_df = vectorise_dataframe(df, feature_list)

    print(
        f"Exporting vectorised_features_df as parquet to {care_home_ind_cqc_features_destination}"
    )

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
