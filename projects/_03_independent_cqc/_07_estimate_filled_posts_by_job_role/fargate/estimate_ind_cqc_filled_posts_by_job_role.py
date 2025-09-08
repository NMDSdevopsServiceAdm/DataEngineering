import logging
import sys
from argparse import ArgumentError, ArgumentTypeError

import polars as pl

from polars_utils import utils

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def main(
    estimated_ind_cqc_filled_posts_source: str,
    cleaned_ascwds_worker_source: str,
    estimated_ind_cqc_filled_posts_by_job_role_destination: str,
) -> None:
    """
    Creates estimates of filled posts split by main job role.

    Args:
        estimated_ind_cqc_filled_posts_source (str): path to the estimates ind cqc filled posts data
        cleaned_ascwds_worker_source (str): path to the cleaned worker data
        estimated_ind_cqc_filled_posts_by_job_role_destination (str): path to where to save the outputs
    """


if __name__ == "__main__":
    try:
        (
            estimated_ind_cqc_filled_posts_source,
            cleaned_ascwds_worker_source,
            estimated_ind_cqc_filled_posts_by_job_role_destination,
            *_,
        ) = utils.collect_arguments(
            (
                "--estimated_ind_cqc_filled_posts_source",
                "Source s3 directory for estimated ind cqc filled posts data",
            ),
            (
                "--cleaned_ascwds_worker_source",
                "Source s3 directory for parquet ASCWDS worker cleaned dataset",
            ),
            (
                "--estimated_ind_cqc_filled_posts_by_job_role_destination",
                "Destination s3 directory",
            ),
        )

        main(
            estimated_ind_cqc_filled_posts_source,
            cleaned_ascwds_worker_source,
            estimated_ind_cqc_filled_posts_by_job_role_destination,
        )

    except (ArgumentError, ArgumentTypeError) as e:
        logger.error(f"An error occurred parsing arguments for {sys.argv}")
        raise e
