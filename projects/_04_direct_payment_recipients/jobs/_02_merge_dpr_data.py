from pyspark.sql import DataFrame

from utils import utils
from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


def main(
    direct_payments_external_data_source,
    direct_payments_survey_data_source,
    destination,
):
    spark = utils.get_spark()

    direct_payments_df: DataFrame = spark.read.parquet(
        direct_payments_external_data_source
    ).select(
        DP.LA_AREA,
        DP.YEAR,
        DP.YEAR_AS_INTEGER,
        DP.SERVICE_USER_DPRS_DURING_YEAR,
        DP.CARER_DPRS_DURING_YEAR,
        DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
        DP.HISTORIC_SERVICE_USERS_EMPLOYING_STAFF_ESTIMATE,
        DP.TOTAL_DPRS_DURING_YEAR,
    )
    pa_ratio_df: DataFrame = spark.read.parquet(direct_payments_survey_data_source)

    direct_payments_df = direct_payments_df.join(
        pa_ratio_df, DP.YEAR_AS_INTEGER, how="left"
    )
    direct_payments_df = direct_payments_df.withColumnRenamed(
        DP.RATIO_ROLLING_AVERAGE, DP.FILLED_POSTS_PER_EMPLOYER
    )

    utils.write_to_parquet(
        direct_payments_df,
        destination,
        mode="overwrite",
        partitionKeys=[DP.YEAR],
    )


if __name__ == "__main__":
    (
        direct_payments_external_data_source,
        direct_payments_survey_data_source,
        destination,
    ) = utils.collect_arguments(
        (
            "--direct_payments_external_data_source",
            "Source s3 directory for direct payments prepared dataset",
        ),
        (
            "--direct_payments_survey_data_source",
            "Source s3 directory for ingested IE/PA survey data",
        ),
        ("--destination", "A destination directory for outputting dpr data."),
    )

    main(
        direct_payments_external_data_source,
        direct_payments_survey_data_source,
        destination,
    )
