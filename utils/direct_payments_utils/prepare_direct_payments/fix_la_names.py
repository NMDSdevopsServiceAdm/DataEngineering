from pyspark.sql import (
    DataFrame,
    functions as F,
)

from utils import utils

from utils.column_names.cleaned_data_files.ons_cleaned_values import (
    OnsCleanedColumns as ONSClean,
)
from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DPColNames,
)


def change_la_names_to_match_ons_cleaned_la_names(
    direct_payments_df: DataFrame,
) -> DataFrame: ...
