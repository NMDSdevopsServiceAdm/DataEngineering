import polars as pl

from polars_utils import cleaning_utils, utils
from projects._04_direct_payment_recipients.direct_payments_config import (
    EstimatePeriodAsDate,
)
from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
)
from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)

postcode_columns = [
    ONSClean.contemporary_ons_import_date,
    ONSClean.postcode,
    ONSClean.contemporary_cssr,
    ONSClean.contemporary_icb,
]
pa_filled_posts_columns = [
    DP.LA_AREA,
    DP.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS,
    DP.YEAR_AS_INTEGER,
]


def main(
    postcode_directory_source: str,
    pa_filled_posts_source: str,
    destination: str,
) -> None:
    postcode_lf = utils.scan_parquet(
        postcode_directory_source, selected_columns=postcode_columns
    )
    pa_filled_posts_lf = utils.scan_parquet(
        pa_filled_posts_source, selected_columns=pa_filled_posts_columns
    )

    check_for_duplicate_postcodes(postcode_lf)

    icb_proportion_lf = calculate_icb_proportions(postcode_lf)

    pa_filled_posts_lf = pa_filled_posts_lf.with_columns(
        pl.date(
            pl.col(DP.YEAR_AS_INTEGER) + 1,
            EstimatePeriodAsDate.MONTH,
            EstimatePeriodAsDate.DAY,
        ).alias(DP.ESTIMATE_PERIOD_AS_DATE)
    )

    pa_filled_posts_lf = cleaning_utils.add_aligned_date_column(
        pa_filled_posts_lf,
        postcode_lf,
        DP.ESTIMATE_PERIOD_AS_DATE,
        ONSClean.contemporary_ons_import_date,
    )

    pa_filled_posts_lf = pa_filled_posts_lf.select(
        pl.col(DP.LA_AREA).alias(ONSClean.contemporary_cssr),
        DP.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS,
        DP.YEAR_AS_INTEGER,
        ONSClean.contemporary_ons_import_date,
    )

    icb_proportion_lf = icb_proportion_lf.join(
        pa_filled_posts_lf,
        on=[ONSClean.contemporary_ons_import_date, ONSClean.contemporary_cssr],
        how="left",
    )

    icb_proportion_lf = icb_proportion_lf.select(
        ONSClean.contemporary_ons_import_date,
        ONSClean.contemporary_cssr,
        ONSClean.contemporary_icb,
        DP.PROPORTION_OF_ICB_POSTCODES_IN_LA_AREA,
        DP.YEAR_AS_INTEGER,
        (
            pl.col(DP.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS)
            * pl.col(DP.PROPORTION_OF_ICB_POSTCODES_IN_LA_AREA)
        ).alias(DP.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS_PER_HYBRID_AREA),
    )

    utils.sink_to_parquet(icb_proportion_lf, destination)


def check_for_duplicate_postcodes(postcode_lf: pl.LazyFrame) -> None:
    """
    Raises if any postcode appears more than once on the same import date.

    Collects a single boolean from a group_by count, so the full postcode
    directory is never materialised.

    Args:
        postcode_lf (pl.LazyFrame): Postcode directory.

    Raises:
        ValueError: If a postcode is duplicated within an import date.
    """
    has_duplicates = (
        postcode_lf.group_by(ONSClean.contemporary_ons_import_date, ONSClean.postcode)
        .len()
        .select((pl.col("len") > 1).any())
        .collect(engine="streaming")
        .item()
    )

    if has_duplicates:
        raise ValueError("Postcode directory has 1 or more duplicates")


def calculate_icb_proportions(postcode_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Calculates the share of each LA's postcodes that fall in each ICB.

    Args:
        postcode_lf (pl.LazyFrame): Postcode directory with import date, postcode, cssr and icb.

    Returns:
        pl.LazyFrame: One row per import date, cssr and icb with the ICB's proportion of the cssr's postcodes.
    """
    la_keys = [ONSClean.contemporary_ons_import_date, ONSClean.contemporary_cssr]

    return (
        postcode_lf.group_by(*la_keys, ONSClean.contemporary_icb)
        .agg(pl.col(ONSClean.postcode).count().alias("icb_postcodes"))
        .with_columns(
            (pl.col("icb_postcodes") / pl.col("icb_postcodes").sum().over(la_keys))
            .cast(pl.Float32)
            .alias(DP.PROPORTION_OF_ICB_POSTCODES_IN_LA_AREA)
        )
        .drop("icb_postcodes")
    )


if __name__ == "__main__":
    print("Running estimates by ICB area job")

    args = utils.get_args(
        (
            "--postcode_directory_source",
            "S3 URI to read cleaned ons postcode directory from",
        ),
        (
            "--pa_filled_posts_source",
            "S3 URI to read estimated pa filled posts split by LA area from",
        ),
        (
            "--destination",
            "S3 URI to save pa filled posts split by ICB area to",
        ),
    )

    main(
        postcode_directory_source=args.postcode_directory_source,
        pa_filled_posts_source=args.pa_filled_posts_source,
        destination=args.destination,
    )

    print("Finished estimates by ICB area job")
