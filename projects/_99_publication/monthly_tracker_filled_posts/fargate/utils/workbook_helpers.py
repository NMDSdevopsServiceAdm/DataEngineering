from pathlib import Path

import boto3

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


def _s3_key(s3_directory: str, file_name: str) -> tuple[str, str]:
    """Splits an S3 directory and a file name into the file's bucket and key."""
    bucket, prefix = split_s3_uri(s3_directory)
    return bucket, f"{prefix.rstrip('/')}/{file_name}"


def upload_to_s3(local_path: Path, s3_directory: str) -> None:
    """
    Uploads a local file into an S3 directory, keeping its file name.

    Args:
        local_path (Path): file to upload.
        s3_directory (str): S3 directory to put the file in.
    """
    bucket, key = _s3_key(s3_directory, local_path.name)
    boto3.client("s3").upload_file(str(local_path), bucket, key)


def download_from_s3(s3_directory: str, file_name: str, local_path: Path) -> None:
    """
    Downloads a named file from an S3 directory; raises ClientError if unreadable.

    Args:
        s3_directory (str): S3 directory holding the file.
        file_name (str): name of the file in that directory.
        local_path (Path): where to save the file.
    """
    bucket, key = _s3_key(s3_directory, file_name)
    boto3.client("s3").download_file(bucket, key, str(local_path))


def set_column_widths(worksheet: object, widths: list[int]) -> None:
    """
    Sets the width of a worksheet's leading columns.

    Args:
        worksheet (object): the worksheet to size.
        widths (list[int]): width of each column from the first, in characters.
    """
    for column, width in enumerate(widths):
        worksheet.set_column(column, column, width)
