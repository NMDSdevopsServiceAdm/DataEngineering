import logging
import json

import polars as pl
import polars.testing as pl_test

from cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    logger.info("Received event: " + json.dumps(event, indent=2))
    left_df = pl.read_parquet(event["left"])
    right_df = pl.read_parquet(event["right"])

    drop_cols = event["drop_cols"].split(",")

    pl_test.assert_frame_equal(
        left_df.filter(pl.col(CQCL.deregistration_date).is_null()).drop(drop_cols),
        right_df.filter(pl.col(CQCL.deregistration_date).is_null()).drop(drop_cols),
        check_row_order=False,
    )

    logger.info("The provided datasets are equal, excluding deregistered items")
