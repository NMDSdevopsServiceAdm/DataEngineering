from datetime import datetime
import polars as pl
import logging
from typing import Union, Any, Generator
import argparse
import uuid
import boto3
from botocore.exceptions import ClientError

util_logger = logging.getLogger(__name__)
util_logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
util_logger.addHandler(logging.StreamHandler())
util_logger.handlers[0].setFormatter(formatter)


def write_to_parquet(
    df: pl.DataFrame,
    output_path: str,
    logger: logging.Logger = util_logger,
    append: bool = True,
) -> None:
    """Writes a Polars DataFrame to a Parquet file.

    If the DataFrame is empty, an informational message is logged, and no file
    is written. Otherwise, the DataFrame is written to the specified path,
    optionally partitioned by the given keys.

    Args:
        df (pl.DataFrame): The Polars DataFrame to write.
        output_path (str): The file path where the Parquet file(s) will be written.
            This must be a directory if append is set to True.
        logger (logging.Logger): An optional logger instance to use for logging messages.
            If not provided, a default logger will be used (or you can ensure
            `util_logger` is globally available).
        append (bool): Whether to append to existing files or overwrite them. Defaults to False.

    Return:
        None
    """
    if df.height == 0:
        logger.info("The provided dataframe was empty. No data was written.")
    else:
        if append:
            output_path += f"{uuid.uuid4()}.parquet"
        df.write_parquet(output_path)
        logger.info("Parquet written to {}".format(output_path))


def collect_arguments(*args: Any) -> Generator[Any, None, None]:
    """
    Creates a new parser, and for each arg in the provided args parameter returns a Namespace object, and uses vars() function to convert the namespace to a dictionary,
    where the keys are constructed from the symbolic names, and the values from the information about the object that each name references.

    Args:
        *args (Any): This is intended to be used to contain parsed arguments when run at command line, and is generally to contain keys and values as a tuple.

    Returns:
        Generator[Any, None, None]: A generator used for parsing parsed parameters.

    Examples:
    >>> single_parameter, *_ = collect_arguments(("--single_parameter","This is how you read a single parameter"))
    >>> (parameter_1, parameter_2) = collect_arguments(("--parameter_1","parameter_1 help text"),("--parameter_2","parameter_2 help text for non-required parameter", False))
    """
    parser = argparse.ArgumentParser()
    for arg in args:
        parser.add_argument(
            arg[0],
            help=arg[1],
            required=True,
        )

    parsed_args, _ = parser.parse_known_args()

    return (vars(parsed_args)[arg[0][2:]] for arg in args)


def generate_s3_datasets_dir_date_path(
    destination_prefix,
    domain,
    dataset,
    date,
    version="1.0.0",
):
    year = f"{date.year}"
    month = f"{date.month:02d}"
    day = f"{date.day:02d}"
    import_date = year + month + day
    output_dir = f"{destination_prefix}/domain={domain}/dataset={dataset}/version={version}/year={year}/month={month}/day={day}/import_date={import_date}/"
    print(f"Generated output s3 dir: {output_dir}")
    return output_dir


def send_sns_notification(
    topic_arn: str,
    subject: str,
    message: str,
    region_name: str = "eu-west-2",
) -> None:
    sns_client = boto3.client("sns", region_name=region_name)
    try:
        sns_client.publish(TopicArn=topic_arn, Subject=subject, Message=message)
    except ClientError as e:
        util_logger.error(e)
        util_logger.error(
            "There was an error writing to SNS - check your IAM permissions and that you have the right topic ARN"
        )
        raise
