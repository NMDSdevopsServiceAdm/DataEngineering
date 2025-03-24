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
    expand_encode_and_extract_features,
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

    df, service_list = expand_encode_and_extract_features(
        df,
        IndCQC.services_offered,
        ServicesFeatures.care_home_labels_dict,
        is_array_col=True,
    )

    df, specialisms_list = expand_encode_and_extract_features(
        df,
        IndCQC.specialisms_offered,
        SpecialismsFeatures.labels_dict,
        is_array_col=True,
    )

    df, rui_indicators_list = expand_encode_and_extract_features(
        df,
        IndCQC.current_rural_urban_indicator_2011,
        RuralUrbanFeatures.care_home_labels_dict,
        is_array_col=False,
    )

    df, region_list = expand_encode_and_extract_features(
        df, IndCQC.current_region, RegionFeatures.labels_dict, is_array_col=False
    )

    feature_list: List[str] = sorted(
        [
            IndCQC.activity_count_capped,
            IndCQC.cqc_location_import_date_indexed,
            IndCQC.number_of_beds,
            IndCQC.ratio_rolling_average_model,
            IndCQC.service_count_capped,
        ]
        + region_list
        + rui_indicators_list
        + service_list
        + specialisms_list
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
