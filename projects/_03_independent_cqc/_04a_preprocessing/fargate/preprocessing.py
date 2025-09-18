import sys
import polars as pl
import logging
from collections.abc import Callable
from typing import Any
from polars_utils import utils
from datetime import datetime as dt
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def main_preprocessor(preprocessor: Callable[..., None], **kwargs: Any) -> None:
    """
    Calls the selected preprocessor with the required arguments. The required arguments will likely include
    the location of the source data and the destination to write to.

    Args:
        preprocessor (Callable[..., None]): a function that carries out the required preprocessing
        **kwargs (Any): required keyword arguments including (as minimum) source and destination (strings)

    Raises:
        TypeError: if source and destination are not included
        Exception: on any exception occurring within the preprocessor
    """
    required = {"source", "destination"}
    given_params = set(kwargs.keys())
    if not required.issubset(given_params):
        raise TypeError(f"preprocessor requires {required} but got {given_params}")
    if not isinstance(kwargs["source"], str) or not isinstance(
        kwargs["destination"], str
    ):
        raise TypeError(
            f"preprocessor requires string source and destination but got {kwargs['source']} and {kwargs['destination']}"
        )

    try:
        logger.info(f"Invokng {preprocessor.__name__} with kwargs: {kwargs}")
        preprocessor(**kwargs)
    except Exception as e:
        logger.error(
            f"There was an unexpected exception while executing preprocessor {str(preprocessor)}."
        )
        logger.error(e)
        raise


def preprocess_non_res_pir(source: str, destination: str, lazy: bool = False) -> None:
    """
    Preprocesses data for Non-Residential PIR model prior to training.

    The function filters null and non-negative feature columns and eliminates large residuals.

    Args:
        source (str): the S3 uri of the feature data or a local file path for testing
        destination( str): the S3 uri of the output directory
        lazy(bool, optional): whether to read the incoming data lazily or not (default is False)

    Raises:
        FileNotFoundError: if a local data source file cannot be found
        pl.exceptions.PolarsError: if there is an error reading or processing the data
    """
    try:
        now = dt.now()
        logger.info(f"Reading data from {source} - the reading method is LAZY {lazy}")
        data = pl.scan_parquet(source) if lazy else pl.read_parquet(source)
        required_columns = [
            "locationId",
            "cqc_location_import_date",
            "careHome",
            "ascwds_filled_posts_deduplicated_clean",
            "pir_people_directly_employed_deduplicated",
        ]
        logger.info("Read succeeded - processing...")
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
        uri = f"{destination}/process_datetime={now.strftime('%Y%m%dT%H%M%S')}/processed.parquet"
        logger.info(
            f"Processing succeeded. Writing to {uri} - the writing method is LAZY {lazy}"
        )
        if lazy:
            result.sink_parquet(uri)
        else:
            result.write_parquet(uri)
    except (pl.exceptions.PolarsError, FileNotFoundError) as e:
        logger.error(
            f"Polars was not able to read or process the data in {source}, or send to {destination}"
        )
        logger.error(f"Polars error: {e}")
        raise


if __name__ == "__main__":
    (processor_name, kwargs) = utils.collect_arguments(
        (
            "--processor_name",
            "The name of the processor",
        ),
        (
            "--kwargs",
            "The additional keyword arguments to pass to the processor in the format name=bill,age=42",
        ),
    )
    processor = locals()[processor_name]
    keyword_args = {
        k: utils.parse_arg_by_type(v)
        for k, v in [tuple(kwarg.split("=", 1)) for kwarg in kwargs.split(",")]
    }
    if "source" not in keyword_args or "destination" not in keyword_args:
        logger.error('The arguments "source" and "destination" are required')
        sys.exit(1)
    main_preprocessor(processor, **keyword_args)
