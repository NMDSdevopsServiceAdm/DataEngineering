import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from utils.prepare_direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


def prepare_during_year_data(df: DataFrame) -> DataFrame:
    return df
