from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    merge_utils,
)
from utils.file_utils import split_s3_uri


def run_workbook_name(stem: str, run_source: str) -> str:
    """
    Names a workbook after its run.

    Args:
        stem (str): what the workbook is, as the start of the file name.
        run_source (str): S3 directory of the run, partitioned by archive_date
            and run_number.

    Returns:
        str: the file name, as <stem>_<archive_date>_<run_number>.xlsx.
    """
    return (
        f"{stem}_{merge_utils.archive_date_from_source(run_source)}"
        f"_{merge_utils.run_number_from_source(run_source)}.xlsx"
    )


def upload_to_s3(local_path: Path, s3_directory: str) -> None:
    """
    Uploads a local file into an S3 directory, keeping its file name.

    Args:
        local_path (Path): file to upload.
        s3_directory (str): S3 directory to put the file in.
    """
    bucket, prefix = split_s3_uri(s3_directory)
    key = f"{prefix.rstrip('/')}/{local_path.name}"
    boto3.client("s3").upload_file(str(local_path), bucket, key)


def set_column_widths(worksheet: object, widths: list[int]) -> None:
    """
    Sets the width of a worksheet's leading columns.

    Args:
        worksheet (object): the worksheet to size.
        widths (list[int]): width of each column from the first, in characters.
    """
    for column, width in enumerate(widths):
        worksheet.set_column(column, column, width)


class LogoUnavailableError(RuntimeError):
    """The Frontpage logo could not be read from S3, or is empty."""


def get_logo_bytes(logo_uri: str) -> bytes:
    """
    Reads the Frontpage logo PNG from S3. A missing or empty logo raises, so a
    release can't go out without it.

    Args:
        logo_uri (str): S3 URI of the logo PNG.

    Returns:
        bytes: the PNG image.

    Raises:
        LogoUnavailableError: if the object cannot be read or is empty.
    """
    bucket, key = split_s3_uri(logo_uri)
    try:
        logo = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError as error:
        raise LogoUnavailableError(f"Could not read the logo at {logo_uri}") from error
    if not logo:
        raise LogoUnavailableError(f"The logo at {logo_uri} is empty")
    return logo
