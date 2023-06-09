from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    IntegerType,
    StringType,
    FloatType,
)

from utils import utils
from utils.prepare_direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


def main(
    direct_payments_prepared_source,
    destination,
):
    spark = SparkSession.builder.appName("sfc_data_engineering_estimate_direct_payments").getOrCreate()

    direct_payments_df: DataFrame = spark.read.parquet(direct_payments_prepared_source).select(
        DP.LA_AREA,
        DP.YEAR,
        DP.DPRS_ADASS,
        DP.DPRS_EMPLOYING_STAFF_ADASS,
        DP.SERVICE_USER_DPRS_AT_YEAR_END,
        DP.CARER_DPRS_AT_YEAR_END,
        DP.SERVICE_USER_DPRS_DURING_YEAR,
        DP.CARER_DPRS_DURING_YEAR,
        DP.IMD_SCORE,
        # TODO
    )

    # TODO

    utils.write_to_parquet(
        direct_payments_df,
        destination,
        append=True,
        partitionKeys=[DP.YEAR],
    )


if __name__ == "__main__":
    (direct_payments_prepared_source, destination,) = utils.collect_arguments(
        ("--direct_payments_prepared_source", "Source s3 directory for direct payments prepared dataset"),
        ("--destination", "A destination directory for outputting dpr data."),
    )

    main(
        direct_payments_prepared_source,
        destination,
    )
