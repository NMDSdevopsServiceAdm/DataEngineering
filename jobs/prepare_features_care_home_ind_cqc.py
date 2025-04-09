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
    add_date_index_column,
    expand_encode_and_extract_features,
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

    features_df = add_date_index_column(filtered_loc_data)

    features_df = add_array_column_count(
        df=features_df,
        new_col_name=IndCQC.service_count,
        col_to_check=IndCQC.services_offered,
    )

    features_df, service_list = expand_encode_and_extract_features(
        features_df,
        IndCQC.services_offered,
        ServicesFeatures.labels_dict,
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

    list_for_vectorisation: List[str] = sorted(
        [
            IndCQC.service_count,
            IndCQC.number_of_beds,
            IndCQC.ascwds_rate_of_change_trendline_model,
        ]
        + service_list
        + region_list
        + rui_indicators_list
    )

    vectorised_dataframe = vectorise_dataframe(
        df=features_df, list_for_vectorisation=list_for_vectorisation
    )
    vectorised_features_df = vectorised_dataframe.select(
        IndCQC.location_id,
        IndCQC.cqc_location_import_date,
        IndCQC.cqc_location_import_date_indexed,
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
