from datetime import date
import sys

import pyspark.sql
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame


from utils import utils
from utils.estimate_job_count.column_names import (
    LOCATION_ID,
    PEOPLE_DIRECTLY_EMPLOYED,
    SNAPSHOT_DATE,
    JOB_COUNT_UNFILTERED,
    JOB_COUNT,
    ESTIMATE_JOB_COUNT,
    PRIMARY_SERVICE_TYPE,
    ROLLING_AVERAGE_MODEL,
    EXTRAPOLATION_MODEL,
    CARE_HOME_MODEL,
    INTERPOLATION_MODEL,
    NON_RESIDENTIAL_MODEL,
)

CQC_ID: str = "CQC_ID"
NURSES_EMPLOYED: str = "Nurses_Employed"
CARE_WORKERS_EMPLOYED: str = "Care_Workers_Employed"
NON_CARE_WORKERS_EMPLOYED: str = "Non_Care_Workers_Employed"
AGENCY_NURSES_EMPLOYED: str = "Agency_Nurses_Employed"
AGENCY_CARE_WORKERS_EMPLOYED: str = "Agency_Care_Workers_Employed"
AGENCY_NON_CARE_WORKERS_EMPLOYED: str = "Agency_Non_Care_Workers_Employed"
CQC_CARE_WORKERS_EMPLOYED: str = "CQC_Care_Workers_Employed"

def main(
    estimate_job_counts_source,
    capacity_tracker_care_home_source,
    capacity_tracker_non_residential_source,
    pir_source,
    diagnostics_destination,
    residuals_destination,
):
    spark = (
        SparkSession.builder.appName("sfc_data_engineering_job_estimate_diagnostics")
        .getOrCreate()
    )
    print("Creating diagnostics for job estimates")

# Create dataframe with necessary columns

    job_estimates_df: DataFrame = spark.read.parquet(
        estimate_job_counts_source
    ).select(
        LOCATION_ID,
        SNAPSHOT_DATE,
        JOB_COUNT_UNFILTERED,
        JOB_COUNT,
        PRIMARY_SERVICE_TYPE,
        ROLLING_AVERAGE_MODEL,
        CARE_HOME_MODEL,
        EXTRAPOLATION_MODEL,
        INTERPOLATION_MODEL,
        NON_RESIDENTIAL_MODEL,
        ESTIMATE_JOB_COUNT,
        PEOPLE_DIRECTLY_EMPLOYED,
    )

    capacity_tracker_care_homes_df: DataFrame = spark.read.parquet(
        capacity_tracker_care_home_source
    ).select(
        CQC_ID,
        NURSES_EMPLOYED,
        CARE_WORKERS_EMPLOYED,
        NON_CARE_WORKERS_EMPLOYED,
        AGENCY_NURSES_EMPLOYED,
        AGENCY_CARE_WORKERS_EMPLOYED,
        AGENCY_NON_CARE_WORKERS_EMPLOYED,
    )

    capacity_tracker_non_residential_df: DataFrame = spark.read.parquet(
        capacity_tracker_non_residential_source
    ).select(
        CQC_ID,
        CQC_CARE_WORKERS_EMPLOYED,
    )


# Add column to split data into known/ unkown values
    # 3 categories: ASCWDS known; known externally; Unknown 
    
# Calculate residuals for each model/ service/ known value status
    # add column to split into groups for model/ service / known value
    # calculate residuals wihin each group (window function?)
    
# Calculate average residuals
    
# Create table for histograms
    # probably just involves dropping some values

# Save diagnostics to parquet - append with timestamp

# Save residuals to parquet - append with timestamp




if __name__ == "__main__":
    print("Spark job 'create_estimate_job_counts_diagnostics' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        estimate_job_counts_source,
        capacity_tracker_care_home_source,
        capacity_tracker_non_residential_source,
        pir_source,
        diagnostics_destination,
        residuals_destination,
    ) = utils.collect_arguments(
        (
            "--estimate_job_counts_source",
            "Source s3 directory for job_estimates",
        ),
        (
            "--capacity_tracker_care_home_source",
            "Source s3 directory for capacity tracker care home data",
        ),
        (
            "--capacity_tracker_non_residential_source",
            "Source s3 directory for capacity tracker non residential data",
        ),
        (
            "--pir_source",
            "Source s3 directory for PIR data",
        ),
        (
            "--diagnostics_destination",
            "A destination directory for outputting summary diagnostics tables.",
        ),
        (
            "--residuals_destination",
            "A destination directory for outputting detailed residuals tables with which to make histograms.",
        ),
    )

    main(
        estimate_job_counts_source,
        capacity_tracker_care_home_source,
        capacity_tracker_non_residential_source,
        pir_source,
        diagnostics_destination,
        residuals_destination,
    )

    print("Spark job 'create_estimate_job_counts_diagnostics' complete")
