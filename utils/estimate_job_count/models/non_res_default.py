import pyspark.sql

from utils.estimate_job_count.column_names import (
    ESTIMATE_JOB_COUNT,
    PRIMARY_SERVICE_TYPE,
)
from utils.prepare_locations_utils.job_calculator.job_calculator import (
    update_dataframe_with_identifying_rule,
)

import pyspark.sql.functions as F

MEAN_NON_RESIDENTIAL_JOBS = 54.09


def model_non_res_default(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Non-res : Not Historical : Not PIR : 2021 jobs = mean of known 2021 non-res jobs (54.09)
    """
    # TODO: remove magic number 54.09

    df = df.withColumn(
        ESTIMATE_JOB_COUNT,
        F.when(
            (
                F.col(ESTIMATE_JOB_COUNT).isNull()
                & (F.col(PRIMARY_SERVICE_TYPE) == "non-residential")
            ),
            MEAN_NON_RESIDENTIAL_JOBS,
        ).otherwise(F.col(ESTIMATE_JOB_COUNT)),
    )

    df = df.withColumn(
        "non_res_default_model",
        F.when(
            (F.col(PRIMARY_SERVICE_TYPE) == "non-residential"),
            MEAN_NON_RESIDENTIAL_JOBS,
        ),
    )

    df = update_dataframe_with_identifying_rule(
        df, "model_non_res_average", ESTIMATE_JOB_COUNT
    )

    return df
