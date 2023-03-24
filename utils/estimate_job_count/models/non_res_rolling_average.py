import pyspark.sql
from pyspark.sql import Window
import pyspark.sql.functions as F

from utils.estimate_job_count.column_names import (
    LOCATION_ID,
    SNAPSHOT_DATE,
    JOB_COUNT,
    ESTIMATE_JOB_COUNT,
    PRIMARY_SERVICE_TYPE,
)

from utils.prepare_locations_utils.job_calculator.job_calculator import (
    update_dataframe_with_identifying_rule,
)

ROLLING_AVERAGE_TIME_PERIOD_IN_DAYS = 90


def model_non_res_rolling_average(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:

    df = update_dataframe_with_identifying_rule(df, "model_non_res_rolling_average", ESTIMATE_JOB_COUNT)

    return df.drop("snapshot_date_unix_conv")
