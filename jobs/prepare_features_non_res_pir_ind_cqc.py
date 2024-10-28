import sys

from pyspark.sql import DataFrame

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
    PartitionKeys as Keys,
)
from utils.column_values.categorical_column_values import CareHome
from utils.features.helper import vectorise_dataframe
from utils import utils

non_res_pir_columns = [
    IndCqc.location_id,
    IndCqc.cqc_location_import_date,
    IndCqc.care_home,
    IndCqc.people_directly_employed_dedup,
    IndCqc.imputed_non_res_people_directly_employed,
    Keys.year,
    Keys.month,
    Keys.day,
    Keys.import_date,
]


def main(
    ind_cqc_cleaned_data_source: str,
    non_res_pir_ind_cqc_features_destination: str,
) -> DataFrame:
    print("Creating non res PIR features dataset...")

    locations_df = utils.read_from_parquet(
        ind_cqc_cleaned_data_source, non_res_pir_columns
    )
    non_res_df = utils.select_rows_with_value(
        locations_df, IndCqc.care_home, CareHome.not_care_home
    )
    features_df = utils.select_rows_with_non_null_value(
        non_res_df, IndCqc.imputed_non_res_people_directly_employed
    )
    vectorised_features_df = vectorise_dataframe(
        df=features_df,
        list_for_vectorisation=[IndCqc.imputed_non_res_people_directly_employed],
    )

    print(f"Exporting as parquet to {non_res_pir_ind_cqc_features_destination}")

    utils.write_to_parquet(
        vectorised_features_df,
        non_res_pir_ind_cqc_features_destination,
        mode="overwrite",
        partitionKeys=[Keys.year, Keys.month, Keys.day, Keys.import_date],
    )


if __name__ == "__main__":
    print("Spark job 'prepare_features_non_res_pir_ind_cqc' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        ind_cqc_cleaned_data_source,
        non_res_pir_ind_cqc_features_destination,
    ) = utils.collect_arguments(
        (
            "--ind_cqc_cleaned_data_source",
            "Source s3 directory for input dataset containing imputed PIR data",
        ),
        (
            "--non_res_pir_ind_cqc_features_destination",
            "A destination directory for outputting non-res PIR features",
        ),
    )

    main(
        ind_cqc_cleaned_data_source,
        non_res_pir_ind_cqc_features_destination,
    )

    print("Spark job 'prepare_features_non_res_pir_ind_cqc' complete")
