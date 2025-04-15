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
    add_date_index_column,
    cap_integer_at_max_value,
    expand_encode_and_extract_features,
    vectorise_dataframe,
)


def main(
    ind_cqc_filled_posts_cleaned_source: str,
    care_home_ind_cqc_features_destination: str,
) -> DataFrame:
    print("Creating care home features dataset...")

    locations_df = utils.read_from_parquet(ind_cqc_filled_posts_cleaned_source)

    filtered_df = utils.select_rows_with_value(
        locations_df, IndCQC.care_home, CareHome.care_home
    )
    filtered_df = utils.select_rows_with_non_null_value(
        filtered_df, IndCQC.number_of_beds
    )

    features_df = add_date_index_column(filtered_df)

    features_df = add_array_column_count(
        features_df, IndCQC.service_count, IndCQC.services_offered
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
        ServicesFeatures.care_home_labels_dict,
        is_array_col=True,
    )

    features_df, specialisms_list = expand_encode_and_extract_features(
        features_df,
        IndCQC.specialisms_offered,
        SpecialismsFeatures.care_home_labels_dict,
        is_array_col=True,
    )

    features_df, rui_indicators_list = expand_encode_and_extract_features(
        features_df,
        IndCQC.current_rural_urban_indicator_2011,
        RuralUrbanFeatures.care_home_labels_dict,
        is_array_col=False,
    )

    features_df, region_list = expand_encode_and_extract_features(
        features_df,
        IndCQC.current_region,
        RegionFeatures.labels_dict,
        is_array_col=False,
    )

    feature_list: List[str] = sorted(
        [
            IndCQC.activity_count_capped,
            IndCQC.cqc_location_import_date_indexed,
            IndCQC.number_of_beds,
            IndCQC.banded_bed_ratio_rolling_average_model,
            IndCQC.service_count_capped,
        ]
        + region_list
        + rui_indicators_list
        + service_list
        + specialisms_list
    )

    vectorised_features_df = vectorise_dataframe(features_df, feature_list)

    print(f"Number of features: {len(feature_list)}")
    print(f"Length of feature df: {vectorised_features_df.count()}")

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
