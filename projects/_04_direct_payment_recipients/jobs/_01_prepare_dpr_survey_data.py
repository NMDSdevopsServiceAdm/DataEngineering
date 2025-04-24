from pyspark.sql import DataFrame

from utils import utils
from projects._04_direct_payment_recipients.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)
from projects._04_direct_payment_recipients.utils._01_prepare_dpr_utils.calculate_pa_ratio import (
    calculate_pa_ratio,
)


def main(survey_data_source, destination):
    survey_df: DataFrame = utils.read_from_parquet(survey_data_source)

    pa_ratio_df = calculate_pa_ratio(survey_df)

    utils.write_to_parquet(
        pa_ratio_df,
        destination,
        mode="overwrite",
        partitionKeys=[DP.YEAR_AS_INTEGER],
    )


if __name__ == "__main__":
    (
        survey_data_source,
        destination,
    ) = utils.collect_arguments(
        (
            "--survey_data_source",
            "Source s3 directory for ingested IE/PA survey data",
        ),
        ("--destination", "A destination directory for outputting dpr data."),
    )

    main(
        survey_data_source,
        destination,
    )
