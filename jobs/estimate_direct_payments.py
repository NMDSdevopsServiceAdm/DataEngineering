from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

from utils import utils
from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)
from utils.direct_payments_utils.estimate_direct_payments.estimate_service_users_employing_staff import (
    estimate_service_users_employing_staff,
)
from utils.direct_payments_utils.estimate_direct_payments.calculate_remaining_variables import (
    calculate_remaining_variables,
)
from utils.direct_payments_utils.estimate_direct_payments.create_summary_table import (
    create_summary_table,
)


def main(
    direct_payments_prepared_source,
    destination,
    summary_destination,
):
    spark = SparkSession.builder.appName(
        "sfc_data_engineering_estimate_direct_payments"
    ).getOrCreate()

    direct_payments_df: DataFrame = spark.read.parquet(
        direct_payments_prepared_source
    ).select(
        DP.LA_AREA,
        DP.YEAR,
        DP.YEAR_AS_INTEGER,
        DP.SERVICE_USER_DPRS_DURING_YEAR,
        DP.CARER_DPRS_DURING_YEAR,
        DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
        DP.HISTORIC_SERVICE_USERS_EMPLOYING_STAFF_ESTIMATE,
        DP.FILLED_POSTS_PER_EMPLOYER,
        DP.TOTAL_DPRS_DURING_YEAR,
    )

    direct_payments_df = estimate_service_users_employing_staff(direct_payments_df)
    direct_payments_df = calculate_remaining_variables(direct_payments_df)
    summary_direct_payments_df = create_summary_table(direct_payments_df)

    utils.write_to_parquet(
        direct_payments_df,
        destination,
        append=True,
        partitionKeys=[DP.YEAR],
    )

    utils.write_to_parquet(
        summary_direct_payments_df,
        summary_destination,
        append=True,
        partitionKeys=[DP.YEAR],
    )


if __name__ == "__main__":
    (
        direct_payments_prepared_source,
        destination,
        summary_destination,
    ) = utils.collect_arguments(
        (
            "--direct_payments_prepared_source",
            "Source s3 directory for direct payments prepared dataset",
        ),
        ("--destination", "A destination directory for outputting dpr data."),
        (
            "--summary_destination",
            "A destination directory for outputting dpr summary data.",
        ),
    )

    main(
        direct_payments_prepared_source,
        destination,
        summary_destination,
    )
