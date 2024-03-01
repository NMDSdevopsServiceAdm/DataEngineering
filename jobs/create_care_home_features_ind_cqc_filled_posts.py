import sys

import pyspark.sql

from utils import utils


def main(
    ind_cqc_filled_posts_cleaned_source: str,
    care_home_features_ind_cqc_filled_posts_destination: str,
) -> pyspark.sql.DataFrame:

    print("Creating care home features dataset...")

    locations_df = utils.read_from_parquet(
        ind_cqc_filled_posts_cleaned_source
    ).selectExpr(*COLUMNS_TO_IMPORT)

    print(
        f"Exporting as parquet to {care_home_features_ind_cqc_filled_posts_destination}"
    )

    utils.write_to_parquet(
        locations_df,
        care_home_features_ind_cqc_filled_posts_destination,
        mode="overwrite",
        partitionKeys=["year", "month", "day", "import_date"],
    )


if __name__ == "__main__":
    print("Spark job 'create_care_home_feature_ind_cqc_filled_posts' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        ind_cqc_filled_posts_cleaned_source,
        care_home_features_ind_cqc_filled_posts_destination,
    ) = utils.collect_arguments(
        (
            "--ind_cqc_filled_posts_cleaned_source",
            "Source s3 directory for ind_cqc_filled_posts_cleaned dataset",
        ),
        (
            "--care_home_features_ind_cqc_filled_posts_destination",
            "A destination directory for outputting care_home_features_ind_cqc_filled_posts",
        ),
    )

    main(
        ind_cqc_filled_posts_cleaned_source,
        care_home_features_ind_cqc_filled_posts_destination,
    )
