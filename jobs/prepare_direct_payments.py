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
from utils.prepare_direct_payments_utils.determine_areas_including_carers_on_adass import (
    determine_areas_including_carers_on_adass,
)


def main(
    direct_payments_source,
    destination,
):
    spark = SparkSession.builder.appName("sfc_data_engineering_prepare_direct_payments").getOrCreate()

    direct_payments_df: DataFrame = spark.read.parquet(direct_payments_source).select(
        DP.LA_AREA,
        DP.YEAR,
        DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
        DP.SERVICE_USER_DPRS_AT_YEAR_END,
        DP.CARER_DPRS_AT_YEAR_END,
        DP.SERVICE_USER_DPRS_DURING_YEAR,
        DP.CARER_DPRS_DURING_YEAR,
        DP.IMD_SCORE,
    )

    direct_payments_df = determine_areas_including_carers_on_adass(direct_payments_df)

    # TODO

    utils.write_to_parquet(
        direct_payments_df,
        destination,
        append=True,
        partitionKeys=[DP.YEAR],
    )


if __name__ == "__main__":
    (direct_payments_source, destination,) = utils.collect_arguments(
        ("--direct_payments_source", "Source s3 directory for direct payments dataset"),
        ("--destination", "A destination directory for outputting dpr data."),
    )

    main(
        direct_payments_source,
        destination,
    )
