import logging
import json

import polars as pl
import polars.testing as pl_test

from utils.column_names.raw_data_files.cqc_provider_api_columns import (
    CqcProviderApiColumns as CqcProviders,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CqcLocations,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    logger.info("Received event: \n" + json.dumps(event, indent=2))

    if event["organisation_type"] == "locations":
        deregistration_date = CqcLocations.deregistration_date
    elif event["organisation_type"] == "providers":
        deregistration_date = CqcProviders.deregistration_date
    else:
        raise ValueError(
            f"Unknown organisation type: {event['organisation_type']}. Must be either locations or providers"
        )

    left_df = pl.read_parquet(event["left"])
    right_df = pl.read_parquet(event["right"])

    drop_cols = event["drop_cols"].split(",")

    pl_test.assert_frame_equal(
        left_df.filter(pl.col(deregistration_date).is_null()).drop(drop_cols),
        right_df.filter(pl.col(deregistration_date).is_null()).drop(drop_cols),
        check_row_order=False,
    )

    logger.info("The provided datasets are equal, excluding deregistered items")
