import json
import logging
from datetime import datetime
from re import match, search

from s3fs import S3FileSystem

from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.snapshots import build_snapshot_table_from_delta

logger = logging.getLogger()
logger.setLevel(logging.INFO)


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
    logger.info(
        f"bucket={input_parse.group('bucket')}, read_folder={input_parse.group('read_folder')}"
    )
    fs = S3FileSystem()
    base_paths = fs.find(
        f"s3://{input_parse.group('bucket')}/{input_parse.group('read_folder')}"
    )
    bucket_prefix = f"{input_parse.group('bucket')}/{input_parse.group('read_folder')}"
    logger.info("bucket_prefix is %s", bucket_prefix)
    partitions = [
        "/".join(p.removeprefix(bucket_prefix).split("/")[:-1])
        for p in base_paths
        if "import_date=" in p
    ]
    ## For all partitions make snapshot
    logger.info("partitions found %s", partitions)
    for partition in partitions:
        logger.info("Inside partition: %s", partition)

        snapshot_df = build_snapshot_table_from_delta(
            bucket=input_parse.group("bucket"),
            read_folder=input_parse.group("read_folder"),
            dataset=event["dataset"],
            partition=partition,
        )
        timepoint = int(search(r"import_date=(\d{8})", partition).group(1))
        output_uri = event["output_uri"] + partition + "/file.parquet"

        with fs.open(output_uri, mode="wb") as destination:
            snapshot_df.drop(
                [Keys.year, Keys.month, Keys.day, Keys.import_date]
            ).write_parquet(destination, compression="snappy")
        logger.info(
            f"Finished processing snapshot {event['snapshot_date']}. The files can be found at {event['output_uri']}"
        )
