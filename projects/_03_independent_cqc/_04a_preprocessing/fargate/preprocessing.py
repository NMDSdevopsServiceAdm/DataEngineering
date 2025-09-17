import polars as pl

from polars_utils.utils import write_to_parquet


def preprocess_non_res_pir(path_to_data: str, destination: str, lazy=False) -> None:
    data = pl.scan_parquet(path_to_data) if lazy else pl.read_parquet(path_to_data)
    required_columns = [
        "locationId",
        "cqc_location_import_date",
        "careHome",
        "ascwds_filled_posts_deduplicated_clean",
        "pir_people_directly_employed_deduplicated",
    ]
    data.select(*required_columns).filter(
        (
            (pl.col("ascwds_filled_posts_deduplicated_clean").is_not_null())
            & (pl.col("pir_people_directly_employed_deduplicated").is_not_null())
            & (pl.col("ascwds_filled_posts_deduplicated_clean") > 0)
            & (pl.col("pir_people_directly_employed_deduplicated") > 0)
        )
    ).with_columns(
        (
            pl.col("ascwds_filled_posts_deduplicated_clean")
            - pl.col("pir_people_directly_employed_deduplicated")
        )
        .abs()
        .alias("abs_resid"),
    ).filter(
        pl.col("abs_resid") <= 500
    ).drop(
        "abs_resid"
    ).write_parquet(
        destination
    )
