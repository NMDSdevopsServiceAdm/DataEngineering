from datetime import date
import sys

import pyspark.sql
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql import SparkSession

from utils import utils
from utils.estimate_job_count.column_names import (
    LOCATION_ID,
    SERVICES_OFFERED,
    PEOPLE_DIRECTLY_EMPLOYED,
    NUMBER_OF_BEDS,
    SNAPSHOT_DATE,
    JOB_COUNT_UNFILTERED,
    JOB_COUNT_UNFILTERED_SOURCE,
    JOB_COUNT,
    LOCAL_AUTHORITY,
    REGISTRATION_STATUS,
    ESTIMATE_JOB_COUNT,
    ESTIMATE_JOB_COUNT_SOURCE,
    PRIMARY_SERVICE_TYPE,
    CQC_SECTOR,
)





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
    # locationid
    # snapshot date
    # job count unfiltered
    # job count
    # primary service type
    # value for each model
    # current estimated value
    # number directly employed
    
    # CT CH cqc id
    # CT CH nurses employed
    # CT CH care workers employed
    # CT CH non care workers employed
    # CT CH agency nurses employed
    # CT CH agency care workers employed
    # CT CH agency non care workers employed

    # CT NR cqc id
    # CT NR care workers employed



# Add column to split data into known/ unkown values
    # 3 categories: ASCWDS known; known externally; Unknown 
    
# Calculate residuals for each model/ service/ known value status
    # Split into 
    
# Calculate average residuals
    
# Create table for histograms

# Save diagnostics to parquet

# Save residuals to parquet




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
