import logging

import polars as pl
import polars.testing as pl_test

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    left_df = pl.read_parquet(event["left"])
    right_df = pl.read_parquet(event["right"])

    drop_cols = event["drop_cols"].split(",")

    pl_test.assert_frame_equal(
        left_df.filter(pl.col("deregistrationDate").is_null()).drop(drop_cols),
        right_df.filter(pl.col("deregistrationDate").is_null()).drop(drop_cols),
        check_row_order=False,
    )

    logger.info("The provided datasets are equal")
