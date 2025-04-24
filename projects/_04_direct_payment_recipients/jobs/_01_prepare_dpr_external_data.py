from pyspark.sql import DataFrame

from utils import utils
from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)
from utils.direct_payments_utils.prepare_direct_payments.determine_areas_including_carers_on_adass import (
    determine_areas_including_carers_on_adass,
)
from utils.direct_payments_utils.prepare_direct_payments.prepare_during_year_data import (
    prepare_during_year_data,
)
from utils.direct_payments_utils.prepare_direct_payments.remove_outliers import (
    remove_outliers,
)


def main(
    direct_payments_source,
    destination,
):
    spark = utils.get_spark()

    direct_payments_df: DataFrame = spark.read.parquet(direct_payments_source).select(
        DP.LA_AREA,
        DP.YEAR,
        DP.DPRS_ADASS,
        DP.DPRS_EMPLOYING_STAFF_ADASS,
        DP.SERVICE_USER_DPRS_AT_YEAR_END,
        DP.CARER_DPRS_AT_YEAR_END,
        DP.SERVICE_USER_DPRS_DURING_YEAR,
        DP.CARER_DPRS_DURING_YEAR,
        DP.PROPORTION_IMPORTED,
        DP.HISTORIC_SERVICE_USERS_EMPLOYING_STAFF_ESTIMATE,
        DP.FILLED_POSTS_PER_EMPLOYER,
    )

    direct_payments_df = determine_areas_including_carers_on_adass(direct_payments_df)
    direct_payments_df = remove_outliers(direct_payments_df)
    direct_payments_df = prepare_during_year_data(direct_payments_df)

    utils.write_to_parquet(
        direct_payments_df,
        destination,
        mode="overwrite",
        partitionKeys=[DP.YEAR],
    )


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
