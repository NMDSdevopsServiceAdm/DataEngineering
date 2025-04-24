from pyspark.sql import DataFrame

from projects._04_direct_payment_recipients.tests.utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DPColNames,
)
from projects._04_direct_payment_recipients.direct_payments_configuration import (
    DirectPaymentsMisspelledLaNames as LaNames,
)


def change_la_names_to_match_ons_cleaned(
    direct_payments_df: DataFrame,
) -> DataFrame:
    direct_payments_df = direct_payments_df.replace(
        LaNames.DICT_TO_CORRECT_LA_NAMES,
        None,
        DPColNames.LA_AREA,
    )

    return direct_payments_df
