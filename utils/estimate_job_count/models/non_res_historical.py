from utils.estimate_job_count.column_names import (
    ESTIMATE_JOB_COUNT,
    PRIMARY_SERVICE_TYPE,
    LAST_KNOWN_JOB_COUNT,
)
from utils.prepare_locations_utils.job_calculator.job_calculator import (
    update_dataframe_with_identifying_rule,
)
from pyspark.sql import functions as F

PROJECTION_RATIO = 1.03


def model_non_res_historical(df):
    """
    Non-res : Historical :  : 2021 jobs = Last known value *1.03
    """
    # TODO: remove magic number 1.03
    df = df.withColumn(
        ESTIMATE_JOB_COUNT,
        F.when(
            (
                F.col(ESTIMATE_JOB_COUNT).isNull()
                & (F.col(PRIMARY_SERVICE_TYPE) == "non-residential")
                & F.col(LAST_KNOWN_JOB_COUNT).isNotNull()
            ),
            F.col(LAST_KNOWN_JOB_COUNT) * PROJECTION_RATIO,
        ).otherwise(F.col(ESTIMATE_JOB_COUNT)),
    )

    df = df.withColumn(
        "model_non_res_historical",
        F.when(
            (
                (F.col(PRIMARY_SERVICE_TYPE) == "non-residential")
                & F.col(LAST_KNOWN_JOB_COUNT).isNotNull()
            ),
            F.col(LAST_KNOWN_JOB_COUNT) * PROJECTION_RATIO,
        ),
    )

    df = update_dataframe_with_identifying_rule(
        df,
        "model_non_res_ascwds_projected_forward",
        ESTIMATE_JOB_COUNT,
    )

    return df
