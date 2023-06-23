from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    FloatType,
    IntegerType,
)

from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


DIRECT_PAYMENTS_DATA = StructType(
    fields=[
        StructField(DP.SERVICE_USER_DPRS_DURING_YEAR, FloatType(), True),
        StructField(DP.CARER_DPRS_DURING_YEAR, FloatType(), True),
        StructField(DP.SERVICE_USER_DPRS_AT_YEAR_END, FloatType(), True),
        StructField(DP.CARER_DPRS_AT_YEAR_END, FloatType(), True),
        StructField(DP.IMD_SCORE, FloatType(), True),
        StructField(DP.LA_AREA, StringType(), False),
        StructField(DP.DPRS_ADASS, FloatType(), False),
        StructField(DP.DPRS_EMPLOYING_STAFF_ADASS, FloatType(), False),
        StructField(DP.YEAR, IntegerType(), False),
        StructField(DP.PROPORTION_IMPORTED, FloatType(), True),
        StructField(
            DP.HISTORIC_SERVICE_USERS_EMPLOYING_STAFF_ESTIMATE, FloatType(), True
        ),
        StructField(DP.FILLED_POSTS_PER_EMPLOYER, FloatType(), True),
    ]
)
