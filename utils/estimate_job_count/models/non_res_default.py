from utils.estimate_job_count.column_names import (
    ESTIMATE_JOB_COUNT,
    PRIMARY_SERVICE_TYPE,
)
from utils.prepare_locations_utils.job_calculator.job_calculator import (
    update_dataframe_with_identifying_rule,
)

import pyspark.sql.functions as F


def model_non_res_default(df):
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
            54.09,
        ).otherwise(F.col(ESTIMATE_JOB_COUNT)),
    )
    df = update_dataframe_with_identifying_rule(
        df, "model_non_res_average", ESTIMATE_JOB_COUNT
    )

    return df
