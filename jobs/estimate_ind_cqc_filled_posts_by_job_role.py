import sys

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


def main(
    estimated_ind_cqc_filled_posts_source: str,
    estimated_ind_cqc_filled_posts_by_job_role_destination: str,
):
    """creates job role estimates

    Args:
        source: path to the estimates ind cqc filled posts data
        output_dir: path to the output directory
    """
    df_estimated_ind_cqc_filled_posts_data = utils.read_from_parquet(
        estimated_ind_cqc_filled_posts_source
    )

    utils.write_to_parquet(
        df_estimated_ind_cqc_filled_posts_data,
        estimated_ind_cqc_filled_posts_by_job_role_destination,
        "overwrite",
        PartitionKeys,
    )


if __name__ == "__main__":
    print("spark job: estimate_ind_cqc_filled_posts_by_job_role starting")
    print(f"job args: {sys.argv}")

    (
        estimated_ind_cqc_filled_posts_source,
        estimated_ind_cqc_filled_posts_by_job_role_destination,
    ) = utils.collect_arguments(
        (
            "--estimated_ind_cqc_filled_posts_source",
            "Source s3 directory for estimated ind cqc filled posts data",
        ),
        (
            "--estimated_ind_cqc_filled_posts_by_job_role_destination",
            "Destination s3 directory",
        ),
    )

    main(
        estimated_ind_cqc_filled_posts_source,
        estimated_ind_cqc_filled_posts_by_job_role_destination,
    )
