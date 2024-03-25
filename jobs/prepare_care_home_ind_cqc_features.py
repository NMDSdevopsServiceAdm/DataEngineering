import sys
from typing import List

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from utils import utils

from utils.feature_engineering_dictionaries import (
    SERVICES_LOOKUP as services_dict,
    RURAL_URBAN_INDICATOR_LOOKUP as rural_urban_indicator_dict,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
    PartitionKeys as Keys,
)
from utils.features.helper import (
    vectorise_dataframe,
    column_expansion_with_dict,
    add_service_count_to_data,
    add_rui_data_data_frame,
    explode_column_from_distinct_values,
    add_date_diff_into_df,
)


def main(
    ind_cqc_filled_posts_cleaned_source: str,
    care_home_ind_cqc_features_destination: str,
) -> DataFrame:
    print("Creating care home features dataset...")

    locations_df = utils.read_from_parquet(ind_cqc_filled_posts_cleaned_source)

    filtered_loc_data = filter_df_to_care_home_only(locations_df)

    data_with_service_count = add_service_count_to_data(
        df=filtered_loc_data,
        new_col_name=IndCQC.service_count,
        col_to_check=IndCQC.services_offered,
    )

    service_keys = list(services_dict.keys())
    data_with_expanded_services = column_expansion_with_dict(
        df=data_with_service_count,
        col_name=IndCQC.services_offered,
        lookup_dict=services_dict,
    )

    rui_indicators = list(rural_urban_indicator_dict.keys())
    data_with_rui = add_rui_data_data_frame(
        df=data_with_expanded_services,
        rui_col_name=IndCQC.current_rural_urban_indicator_2011,
        lookup_dict=rural_urban_indicator_dict,
    )

    distinct_regions = get_list_of_distinct_ons_regions(
        df=data_with_rui,
    )

    data_with_region_cols, regions = explode_column_from_distinct_values(
        df=data_with_rui,
        column_name=IndCQC.current_region,
        col_prefix="ons_",
        col_list_set=set(distinct_regions),
    )

    data_with_date_diff = add_date_diff_into_df(
        df=data_with_region_cols,
        new_col_name=IndCQC.date_diff,
        import_date_col=IndCQC.cqc_location_import_date,
    )

    list_for_vectorisation: List[str] = sorted(
        [
            IndCQC.service_count,
            IndCQC.number_of_beds,
            IndCQC.date_diff,
        ]
        + service_keys
        + regions
        + rui_indicators
    )

    vectorised_dataframe = vectorise_dataframe(
        df=data_with_date_diff, list_for_vectorisation=list_for_vectorisation
    )
    features_df = vectorised_dataframe.select(
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

    print("distinct_regions")
    print(distinct_regions)
    print("number_of_features:")
    print(len(list_for_vectorisation))
    print(f"length of feature df: {vectorised_dataframe.count()}")

    print(f"Exporting as parquet to {care_home_ind_cqc_features_destination}")

    utils.write_to_parquet(
        features_df,
        care_home_ind_cqc_features_destination,
        mode="overwrite",
        partitionKeys=[Keys.year, Keys.month, Keys.day, Keys.import_date],
    )

    return list_for_vectorisation


def filter_df_to_care_home_only(df: DataFrame) -> DataFrame:
    return df.filter(F.col(IndCQC.care_home) == "Y")


def get_list_of_distinct_ons_regions(df: DataFrame) -> List[str]:
    distinct_regions = df.select(IndCQC.current_region).distinct().dropna().collect()
    dis_regions_list = [str(row.current_Region) for row in distinct_regions]
    return dis_regions_list


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
