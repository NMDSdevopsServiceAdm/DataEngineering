import sys
import polars as pl
import logging
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def main_preprocessor(
    preprocessor: Callable[..., None], *args: Any, **kwargs: Any
) -> None:
    """
    Calls the selected preprocessor with the required arguments. The required arguments will likely include
    the location of the source data and the destination to write to.

    Args:
        preprocessor (Callable[..., None]): a function that carries out the required preprocessing
        *args (Any) : required arguments
        **kwargs (Any): required keyword arguments

    Raises:
        Exception: on any exception occurring within the preprocessor
    """
    try:
        preprocessor(*args, **kwargs)
    except Exception as e:
        logger.error(
            f"There was an unexpected exception while executing preprocessor {str(preprocessor)}."
        )
        logger.error(e)
        raise


def preprocess_non_res_pir(
    path_to_data: str, destination: str, lazy: bool = False
) -> None:
    """
    Preprocesses data for Non-Residential PIR model prior to training.

    The function filters null and non-negative feature columns and eliminates large residuals.

    Args:
        path_to_data (str): the S3 uri of the feature data
        destination( str): the S3 uri of the output directory
        lazy(bool, optional): whether to read the incoming data lazily or not (default is False)

    Raises:
        FileNotFoundError: if a local data source file cannot be found
        pl.exceptions.PolarsError: if there is an error reading or processing the data
    """
    try:
        data = pl.scan_parquet(path_to_data) if lazy else pl.read_parquet(path_to_data)
        required_columns = [
            "locationId",
            "cqc_location_import_date",
            "careHome",
            "ascwds_filled_posts_deduplicated_clean",
            "pir_people_directly_employed_deduplicated",
        ]
        result = (
            data.select(*required_columns)
            .filter(
                (
                    (pl.col("ascwds_filled_posts_deduplicated_clean").is_not_null())
                    & (
                        pl.col(
                            "pir_people_directly_employed_deduplicated"
                        ).is_not_null()
                    )
                    & (pl.col("ascwds_filled_posts_deduplicated_clean") > 0)
                    & (pl.col("pir_people_directly_employed_deduplicated") > 0)
                )
            )
            .with_columns(
                (
                    pl.col("ascwds_filled_posts_deduplicated_clean")
                    - pl.col("pir_people_directly_employed_deduplicated")
                )
                .abs()
                .alias("abs_resid"),
            )
            .filter(pl.col("abs_resid") <= 500)
            .drop("abs_resid")
        )
        if lazy:
            result.sink_parquet(destination)
        else:
            result.write_parquet(destination)
    except (pl.exceptions.PolarsError, FileNotFoundError) as e:
        logger.error(
            f"Polars was not able to read or process the data in {path_to_data}, or send to {destination}"
        )
        logger.error(f"Polars error: {e}")
        raise
