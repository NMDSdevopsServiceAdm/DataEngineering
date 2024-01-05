from dataclasses import dataclass

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    FloatType,
)
from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


@dataclass
class DirectPaymentConfiguration:
    # The carer's employing percentage was calculated from a question in older surveys. As this is so close to zero it was removed as a question from more recent surveys and we use the most recent value.
    CARERS_EMPLOYING_PERCENTAGE: float = 0.0063872289536592
    PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_THRESHOLD: float = 1.0
    ADASS_PROPORTION_OUTLIER_THRESHOLD: float = 0.3
    SELF_EMPLOYED_STAFF_PER_SERVICE_USER: float = 0.0179487641096729
    NUMBER_OF_YEARS_ROLLING_AVERAGE: int = 3
    FIRST_YEAR: int = 2011


@dataclass
class DirectPaymentsOutlierThresholds:
    ONE_HUNDRED_PERCENT: float = 1.0
    ZERO_PERCENT: float = 0.0


@dataclass
class DirectPaymentsMissingPARatios:
    ratios = [
        (2011, 1.98),
        (2012, 1.98),
        (2013, 1.98),
        (2015, 2.00),
        (2016, 2.01),
        (2018, 1.96),
    ]
    schema = StructType(
        [
            StructField(DP.YEAR_AS_INTEGER, IntegerType(), True),
            StructField(
                DP.HISTORIC_RATIO,
                FloatType(),
                True,
            ),
        ]
    )
