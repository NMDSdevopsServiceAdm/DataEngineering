from pyspark.sql import (
    DataFrame,
)

from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DPColNames,
)
from utils.direct_payments_utils.direct_payments_configuration import (
    DirectPaymentsMisspelledLaNames as LaNames,
)


def change_la_names_to_match_ons_cleaned(
    direct_payments_df: DataFrame,
) -> DataFrame:
    direct_payments_df = direct_payments_df.replace(
        LaNames.LIST_OF_INCORRECT_LA_NAMES,
        LaNames.LIST_OF_CORRECT_LA_NAMES,
        DPColNames.LA_AREA,
    )

    return direct_payments_df
