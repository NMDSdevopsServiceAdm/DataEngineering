import json
import os
import sys
import polars as pl
import logging
from collections.abc import Callable
from polars_utils import utils
from datetime import datetime as dt
import boto3
from botocore.exceptions import ClientError
from projects._03_independent_cqc._05_model.model_registry import (
    model_definitions,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def main_preprocessor(model_name: str, preprocessor: Callable[..., str]) -> None:
    """
    Calls the correct model preprocessor with the required arguments.

    The function retrieves the name of the preprocessing function from the model registry using the model name, along
    with any other required information. After invoking the function, a callback to the StepFunction client is
    executed to signal success or failure.

    Args:
        model_name (str): the name of a valid model
        preprocessor (Callable[..., str]): the preprocessing function that will be called (must be defined in this
            module)

    Raises:
        ValueError: if source, destination, or processor keyword arguments are not found
        ClientError: if there is an error calling the StepFunctions client
        Exception: on any exception occurring within the preprocessor
    """
    S3_SOURCE_BUCKET = os.environ.get("S3_SOURCE_BUCKET")
    sfn = boto3.client("stepfunctions")
    task_token = os.environ.get("TASK_TOKEN", "testtoken")

    try:
        validate_model_definition(model_name)

        preprocessor_kwargs = model_definitions[model_name]["preprocessor_kwargs"]

        source = (
            f's3://{S3_SOURCE_BUCKET}/{model_definitions[model_name]["source_prefix"]}'
        )
        destination = f's3://{S3_SOURCE_BUCKET}/{model_definitions[model_name]["processed_location"]}'
        preprocessor_kwargs["source"] = source
        preprocessor_kwargs["destination"] = destination

        logger.info("Getting Task Token for Step Function callback")
        logger.info(
            f"Invoking {preprocessor.__name__} with kwargs: {preprocessor_kwargs}"
        )
        processed = preprocessor(**preprocessor_kwargs)
        result = {"processed_datetime": processed}
        sfn.send_task_success(taskToken=task_token, output=json.dumps(result))
    except ClientError as e:
        logger.error("There was an error calling the StepFunction AWS service")
        logger.error(f"preprocessor error: {e}")
        sfn.send_task_failure(taskToken=task_token, error=str(e))
        raise
    except ValueError as e:
        logger.error("There was an invalid parameter included in the invocation.")
        logger.error(e)
        sfn.send_task_failure(taskToken=task_token, error=str(e))
        raise
    except Exception as e:
        logger.error(
            f"There was an unexpected exception while executing preprocessor {preprocessor.__name__}."
        )
        logger.error(e)
        sfn.send_task_failure(taskToken=task_token, error=str(e))
        raise


def validate_model_definition(model_id: str) -> None:
    if model_id not in model_definitions:
        raise ValueError(f"{model_id} not included in model_definitions")
    elif "preprocessor_kwargs" not in model_definitions[model_id]:
        raise ValueError(
            f"{model_id} preprocessor_kwargs not included in model_definitions"
        )
    elif "source_prefix" not in model_definitions[model_id]:
        raise ValueError(f"{model_id} source_prefix not included in model_definitions")
    elif "processed_location" not in model_definitions[model_id]:
        raise ValueError(
            f"{model_id} processed_location not included in model_definitions"
        )


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
    (model_id) = utils.collect_arguments(
        (
            "--model_name",
            "The name of the processor",
        ),
    )
    if "preprocessor" not in model_definitions[model_id]:
        raise ValueError(f"{model_id} preprocessor not included in model_definitions")
    preprocessor_id = model_definitions[model_id]["preprocessor"]
    if preprocessor_id not in locals():
        logger.error(
            "The processor name provided in the model definition does not match a defined processor function."
        )
        raise ValueError(f"No such preprocessor: {preprocessor_id}")
    preprocessor = locals()[preprocessor_id]
    main_preprocessor(model_id, preprocessor)
