import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.prepare_worker_utils as pWorkerUtils
from polars_utils import utils


def main(
    cleaned_ascwds_worker_source: str,
    prepared_data_destination: str,
) -> None:
    """Load the cleaned ASCWDS worker dataset and prepare it for employment
    status estimation.

    Args:
        cleaned_ascwds_worker_source (str): path to the cleaned ascwds worker data
        prepared_data_destination (str): destination for output
    """
    worker_lf = utils.scan_parquet(cleaned_ascwds_worker_source)

    employment_status_summary_lf = pWorkerUtils.aggregate_employment_status_data(
        worker_lf
    )
    employment_status_summary_lf = pWorkerUtils.reshape_employment_status_data(
        employment_status_summary_lf
    )

    utils.sink_to_parquet(
        lazy_df=employment_status_summary_lf,
        output_path=prepared_data_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--cleaned_ascwds_worker_source",
            "Source s3 directory for cleaned ascwds worker data",
        ),
        (
            "--prepared_data_destination",
            "Destination s3 directory for prepared worker data",
        ),
    )
    main(
        cleaned_ascwds_worker_source=args.cleaned_ascwds_worker_source,
        prepared_data_destination=args.prepared_data_destination,
    )
