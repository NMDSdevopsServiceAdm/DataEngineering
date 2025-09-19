import json
import os
import sys
import polars as pl
import logging
from collections.abc import Callable
from typing import Any
from polars_utils import utils
from datetime import datetime as dt
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def main_preprocessor(preprocessor: Callable[..., str], **kwargs: Any) -> None:
    """
    Calls the selected preprocessor with the required arguments. The required arguments will likely include
    the location of the source data and the destination to write to. A callback to the StepFunction client
    is executed to signal success or failure.

    Args:
        preprocessor (Callable[..., str]): a function that carries out the required preprocessing
        **kwargs (Any): required keyword arguments including (as minimum) source and destination (strings)

    Raises:
        TypeError: if source and destination are not included
        ClientError: if there is an error calling the StepFunctions client
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
    sfn = boto3.client("stepfunctions")
    task_token = os.environ.get("TASK_TOKEN", "testtoken")

    try:
        logger.info("Getting Task Token for Step Function callback")
        logger.info(f"Invokng {preprocessor.__name__} with kwargs: {kwargs}")
        processed = preprocessor(**kwargs)
        result = {"processed_datetime": processed}
    except ClientError as e:
        logger.error("There was an error calling the StepFunction AWS service")
        logger.error(f"preprocessor error: {e}")
        sfn.send_task_failure(taskToken=task_token, error=str(e))
        raise
    except Exception as e:
        logger.error(
            f"There was an unexpected exception while executing preprocessor {preprocessor.__name__}."
        )
        logger.error(e)
        sfn.send_task_failure(taskToken=task_token, error=str(e))
        raise
    else:
        sfn.send_task_success(taskToken=task_token, output=json.dumps(result))


def preprocess_non_res_pir(source: str, destination: str, lazy: bool = False) -> str:
    """
    Preprocesses data for Non-Residential PIR model prior to training.

    The function filters null and non-negative feature columns and eliminates large residuals.

    Args:
        source (str): the S3 uri of the feature data or a local file path for testing
        destination( str): the S3 uri of the output directory
        lazy(bool, optional): whether to read the incoming data lazily or not (default is False)

    Returns:
        str: the string datetime the processing was performed in the format YYYYMMDDHHmmss.

    Raises:
        FileNotFoundError: if a local data source file cannot be found
        pl.exceptions.PolarsError: if there is an error reading or processing the data
    """
    try:
        dt_now = dt.now()
        if source[-7:] != "parquet" and source[-1] != "/":
            source = source + "/"
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
        process_datetime = dt_now.strftime("%Y%m%dT%H%M%S")
        uri = f"{destination}/process_datetime={process_datetime}/processed.parquet"
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
    return process_datetime


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
    process_datetime = main_preprocessor(processor, **keyword_args)
    result = {"process_datetime": process_datetime}
    sys.stdout.write(json.dumps(result))
    sys.exit(0)
