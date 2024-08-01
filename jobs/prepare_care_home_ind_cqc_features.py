import sys
from typing import List

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

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
    care_home_ind_cqc_features_destination: str,
) -> DataFrame:
    print("Creating care home features dataset...")

    locations_df = utils.read_from_parquet(ind_cqc_filled_posts_cleaned_source)

    filtered_loc_data = filter_df_to_care_home_only(locations_df)

    features_df = add_service_count_to_data(
        df=filtered_loc_data,
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
    features_df = add_import_month_index_into_df(df=features_df)
    """
    features_df = add_date_diff_into_df(
        df=features_df,
        new_col_name=IndCQC.date_diff,
        import_date_col=IndCQC.cqc_location_import_date,
    )
    """

    list_for_vectorisation: List[str] = sorted(
        [
            IndCQC.service_count,
            IndCQC.number_of_beds,
            "import_month_index",
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

    print(f"Exporting as parquet to {care_home_ind_cqc_features_destination}")

    utils.write_to_parquet(
        vectorised_features_df,
        care_home_ind_cqc_features_destination,
        mode="overwrite",
        partitionKeys=[Keys.year, Keys.month, Keys.day, Keys.import_date],
    )


def filter_df_to_care_home_only(df: DataFrame) -> DataFrame:
    return df.filter(F.col(IndCQC.care_home) == "Y")


def add_import_month_index_into_df(df: DataFrame) -> DataFrame:
    min_d = df.agg(F.min("cqc_location_import_date")).first()[0]

    # we get files on the first of each month but the data refers to the previous month
    # if there's an issue they arrive a few days late, so adjusting by 5 accounts for that
    loc_df = df.withColumn(
        "adjusted_import_date", F.date_add(F.col("cqc_location_import_date"), -5)
    )

    loc_df = loc_df.withColumn(
        "import_month_index",
        F.months_between(F.col("adjusted_import_date"), F.lit(min_d)).cast("int"),
    )
    loc_df = loc_df.drop("adjusted_import_date")
    return loc_df


if __name__ == "__main__":
    print("Spark job 'prepare_care_home_ind_cqc_features' starting...")
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

    print("Spark job 'prepare_care_home_ind_cqc_features' complete")
