import sys

import pyspark.sql

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


def main(
    cleaned_ind_cqc_source: str,
    care_home_features_source: str,
    non_res_features_source:str,
    care_home_model_directory:str,
    non_res_model_directory:str,
    metrics_destination:str,
    estimated_ind_cqc_destination:str,
    job_run_id,
    job_name,
) -> pyspark.sql.DataFrame:
    print("Estimating independent CQC filled posts...")

    cleaned_ind_cqc_df = utils.read_from_parquet(cleaned_ind_cqc_source)



    print(f"Exporting as parquet to {estimated_ind_cqc_destination}")

    utils.write_to_parquet(
        cleaned_ind_cqc_df,
        estimated_ind_cqc_destination,
        mode="overwrite",
        partitionKeys=PartitionKeys,
    )





if __name__ == "__main__":
    print("Spark job 'estimate_ind_cqc_filled_posts' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        cleaned_ind_cqc_source,
        care_home_features_source,
        non_res_features_source,
        care_home_model_directory,
        non_res_model_directory,
        metrics_destination,
        estimated_ind_cqc_destination,
        job_run_id,
        job_name,
    ) = utils.collect_arguments(
        (
            "--cleaned_ind_cqc_source",
            "Source s3 directory for cleaned_ind_cqc_filled_posts",
        ),
        (
            "--care_home_features_source",
            "Source s3 directory for ML features for care homes",
        ),
        (
            "--non_res_features_source",
            "Source s3 directory for ML features for non res",
        ),
        (
            "--care_home_model_directory",
            "The directory where the care home models are saved",
        ),
        (
            "--non_res_model_directory",
            "The directory where the non re models are saved",
        ),
        ("--metrics_destination", "The destination for the R2 metric data"),
        (
            "--estimated_ind_cqc_destination",
            "A destination directory for outputting estimates for filled posts",
        ),
        ("--job_run_id", "The Glue job run id"),
        ("--job_name", "The Glue job name"),
    )

    main(
        cleaned_ind_cqc_source,
        care_home_features_source,
        non_res_features_source,
        care_home_model_directory,
        non_res_model_directory,
        metrics_destination,
        estimated_ind_cqc_destination,
        job_run_id,
        job_name,
    )
