import json
import logging

from re import match
from datetime import datetime

import s3fs

from utils import build_snapshot_table_from_delta, snapshots


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def main(input_uri, output_uri, snapshot_date):
    input_parse = match(
        "s3://(?P<bucket>[\w\-=.]+)/(?P<read_folder>[\w/-=.]+)", input_uri
    )

    date_int = int(
        datetime.strptime(snapshot_date, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y%m%d")
    )
    logger.info(
        f"bucket={input_parse.group('bucket')}, read_folder={input_parse.group('read_folder')}"
    )

    snapshot_df = build_snapshot_table_from_delta(
        bucket=input_parse.group("bucket"),
        read_folder=input_parse.group("read_folder"),
        timepoint=date_int,
    )

    output_uri += f"import_date={date_int}/file.parquet"

    fs = s3fs.S3FileSystem()
    with fs.open(output_uri, mode="wb") as destination:
        snapshot_df.drop(["year", "month", "day"]).write_parquet(
            destination, compression="snappy"
        )

    logger.info(f"File has been written to: {output_uri}")


def lambda_handler(event, context):
    main(event["input_uri"], event["output_uri"], event["snapshot_date"])
    logger.info(
        f"Finished processing snapshot {event['snapshot_date']}. The files can be found at {event['output_uri']}"
    )
