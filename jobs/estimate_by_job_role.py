import sys
from utils import utils


def main(source: str):
    df_estimated_ind_cqc_filled_posts_data = utils.read_from_parquet(source)


if __name__ == "__main__":
    print('spark job: estimate_by_job_role starting')
    print(f'job args: {sys.argv}')

    source = next(utils.collect_arguments(
        (
            "--estimated_ind_cqc_filled_posts_data",
            "Source s3 directory for estimated ind cqc filled posts data",
        ),
    ))

    main(source)
