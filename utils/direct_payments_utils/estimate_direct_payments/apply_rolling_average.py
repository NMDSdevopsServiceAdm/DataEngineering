import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


def apply_rolling_average(direct_payments_df: DataFrame) -> DataFrame:
    # TODO
    return direct_payments_df
