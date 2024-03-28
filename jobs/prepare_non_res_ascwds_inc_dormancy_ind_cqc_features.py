import sys
from typing import List

import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from utils import utils

from utils.feature_engineering_dictionaries import (
    SERVICES_LOOKUP as services_dict,
    RURAL_URBAN_INDICATOR_LOOKUP as rural_urban_indicator_dict,
    REGION_LOOKUP as ons_region_dict,
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
)


def main(
    ind_cqc_filled_posts_cleaned_source: str,
    non_res_ascwds_inc_dormancy_ind_cqc_features_destination: str,
) -> DataFrame:
    print("Creating non res ascwds inc dormancy features dataset...")

    locations_df = utils.read_from_parquet(ind_cqc_filled_posts_cleaned_source)

    filtered_loc_data = filter_df_to_non_res_only(locations_df)

    filtered_data_with_employee_col = convert_col_to_integer_col(
        df=filtered_loc_data,
        col_name=IndCQC.people_directly_employed,
    )

    features_df = add_service_count_to_data(
        df=filtered_data_with_employee_col,
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

    features_df = add_date_diff_into_df(
        df=features_df,
        new_col_name=IndCQC.date_diff,
        import_date_col=IndCQC.cqc_location_import_date,
    )

    list_for_vectorisation: List[str] = sorted(
        [
            IndCQC.service_count,
            IndCQC.people_directly_employed,
            IndCQC.date_diff,
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
        IndCQC.people_directly_employed,
        IndCQC.care_home,
        IndCQC.features,
        IndCQC.ascwds_filled_posts_dedup_clean,
        Keys.year,
        Keys.month,
        Keys.day,
        Keys.import_date,
    )

    print("number_of_features:")
    print(len(list_for_vectorisation))
    print(f"length of feature df: {vectorised_dataframe.count()}")

    print(
        f"Exporting as parquet to {non_res_ascwds_inc_dormancy_ind_cqc_features_destination}"
    )
    utils.write_to_parquet(
        vectorised_features_df,
        non_res_ascwds_inc_dormancy_ind_cqc_features_destination,
        mode="overwrite",
        partitionKeys=[Keys.year, Keys.month, Keys.day, Keys.import_date],
    )


def filter_df_to_non_res_only(df: DataFrame) -> DataFrame:
    return df.filter(F.col(IndCQC.care_home) == "N")


def convert_col_to_integer_col(df, col_name):
    return df.withColumn(col_name, F.col(col_name).cast("int"))


if __name__ == "__main__":
    print(
        "Spark job 'prepare_non_res_ascwds_inc_dormancy_ind_cqc_features' starting..."
    )
    print(f"Job parameters: {sys.argv}")

    (
        ind_cqc_filled_posts_cleaned_source,
        non_res_ascwds_inc_dormancy_ind_cqc_features_destination,
    ) = utils.collect_arguments(
        (
            "--ind_cqc_filled_posts_cleaned_source",
            "Source s3 directory for ind_cqc_filled_posts_cleaned dataset",
        ),
        (
            "--non_res_ascwds_inc_dormancy_ind_cqc_features_destination",
            "A destination directory for outputting non_res_features_ind_cqc_filled_posts",
        ),
    )

    main(
        ind_cqc_filled_posts_cleaned_source,
        non_res_ascwds_inc_dormancy_ind_cqc_features_destination,
    )

    print("Spark job 'prepare_non_res_ascwds_inc_dormancy_ind_cqc_features' complete")
