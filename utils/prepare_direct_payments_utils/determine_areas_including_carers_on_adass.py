import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    IntegerType,
    StringType,
    FloatType,
)

from utils.prepare_direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


def determine_areas_including_carers_on_adass(df: DataFrame) -> DataFrame:
    # TODO
    return df
