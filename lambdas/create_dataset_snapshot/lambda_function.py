import json
import logging
from datetime import datetime
from re import match, search, sub

from s3fs import S3FileSystem

from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.snapshots import build_snapshot_table_from_delta

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_latest_available_date(fs, input_uri):
    bucket_path = sub(r"^s3://", "", input_uri)
    all_paths = fs.ls(bucket_path)

    available_dates = []
    for path in all_paths:
        # Match only import_date=YYYYMMDD folders
        match_date = search(r"import_date=(\d{8})", path)
        if match_date:
            available_dates.append(int(match_date.group(1)))

    if not available_dates:
        raise ValueError(f"No valid date partitions found under {input_uri}")

    latest_date = max(available_dates)
    logger.info(f"Detected latest available date: {latest_date}")
    return latest_date


def lambda_handler(event, context):
    logger.info("Received event: \n" + json.dumps(event, indent=2))

    input_parse = match(
        "s3://(?P<bucket>[\w\-=.]+)/(?P<read_folder>[\w/-=.]+)", event["input_uri"]
    )

    date_int = int(
        datetime.strptime(event["snapshot_date"], "%Y-%m-%dT%H:%M:%S.%fZ").strftime(
            "%Y%m%d"
        )
    )

    fs = S3FileSystem()

    # latest_date = get_latest_available_date(fs, event["input_uri"])
    # logger.info(f"Using detected latest snapshot date: {latest_date}")

    # logger.info(
    #     f"bucket={input_parse.group('bucket')}, read_folder={input_parse.group('read_folder')}"
    # )

    snapshot_df = build_snapshot_table_from_delta(
        bucket=input_parse.group("bucket"),
        read_folder=input_parse.group("read_folder"),
        dataset=event["dataset"],
        timepoint=date_int,
    )

    output_uri = event["output_uri"] + f"import_date={date_int}/file.parquet"

    with fs.open(output_uri, mode="wb") as destination:
        snapshot_df.drop(
            [Keys.year, Keys.month, Keys.day, Keys.import_date]
        ).write_parquet(destination, compression="snappy")
    logger.info(
        f"Finished processing snapshot {event['snapshot_date']}. The files can be found at {event['output_uri']}"
    )
