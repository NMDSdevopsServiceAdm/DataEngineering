from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    IntegerType,
    StringType,
    FloatType,
)

from utils import utils
from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


def main(
    direct_payments_source,
    destination,
):
    spark = SparkSession.builder.appName("sfc_data_engineering_prepare_direct_payments").getOrCreate()

    direct_payments_df: DataFrame = spark.read.parquet(direct_payments_source).select(
        DP.LA_AREA,
        DP.YEAR,
        DP.DPRS_ADASS,
        DP.DPRS_EMPLOYING_STAFF_ADASS,
        DP.SERVICE_USER_DPRS_AT_YEAR_END,
        DP.CARER_DPRS_AT_YEAR_END,
        DP.SERVICE_USER_DPRS_DURING_YEAR,
        DP.CARER_DPRS_DURING_YEAR,
        DP.IMD_SCORE,
    )
    # TODO define a function that takes a dataframe and returns a dataframe with only the columns la_area, year, and imd_score
    direct_payments_df_three_columns = select_la_area_year_imd_score(direct_payments_df)

    # TODO define a function that takes a dataframe and reutns a dataframe filtered to leeds data only
    direct_payments_df_leeds_only = filter_to_leeds_local_authority(direct_payments_df)

    # TODO define a function that takes a dataframe and returns that dataframe with a new column that is the proportion on direct payment recipients employing staff according to ADASS data
    direct_payments_df_with_proportion_employing_staff = calculate_proportion_employing_staff(direct_payments_df)


def select_la_area_year_imd_score(direct_payments_df):
    df = direct_payments_df.select(F.col(DP.LA_AREA), F.col(DP.YEAR), F.col(DP.IMD_SCORE))
    return df


def filter_to_leeds_local_authority(direct_payments_df):
    df = direct_payments_df.where(F.col(DP.LA_AREA) == "Leeds")
    return df


def calculate_proportion_employing_staff(direct_payments_df):
    df = direct_payments_df.withColumn(
        "DP_EMPLOYING_STAFF", F.col(DP.DPRS_EMPLOYING_STAFF_ADASS) / F.col(DP.DPRS_ADASS)
    )
    return df


if __name__ == "__main__":
    (
        direct_payments_source,
        destination,
    ) = utils.collect_arguments(
        ("--direct_payments_source", "Source s3 directory for direct payments dataset"),
        ("--destination", "A destination directory for outputting dpr data."),
    )

    main(
        direct_payments_source,
        destination,
    )
