import sys
from utils import utils


def main(source: str, output_dir: str):
    """creates job role estimates

    Args:
        source: path to the estimates ind cqc filled posts data
        output_dir: path to the output directory
    """
    df_estimated_ind_cqc_filled_posts_data = utils.read_from_parquet(source)

    utils.write_to_parquet(
        df_estimated_ind_cqc_filled_posts_data,
        output_dir,
        "overwrite",
        ["year", "month", "day", "import_date"],
    )


if __name__ == "__main__":
    print("spark job: estimate_ind_cqc_filled_posts_by_job_role starting")
    print(f"job args: {sys.argv}")

    source, output_dir = utils.collect_arguments(
        (
            "--estimated_ind_cqc_filled_posts_data",
            "Source s3 directory for estimated ind cqc filled posts data",
        ),
        (
            "--output_destination",
            "Destination s3 directory",
        ),
    )

    main(source, output_dir)
