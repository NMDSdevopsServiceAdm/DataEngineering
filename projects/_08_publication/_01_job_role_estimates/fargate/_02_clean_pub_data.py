from polars_utils import utils
from projects._08_publication._01_job_role_estimates.fargate.utils import (
    clean_utils as cUtils,
)


def main(
    merge_data_source: str,
    assessment_destination: str,
    publication_destination: str,
) -> None:
    """
    Cleans merged job role data and splits it into assessment and publication data.

    The capacity tracker filters and the assessment/publication split are
    currently placeholders and don't yet apply any real filtering or splitting.

    Args:
        merge_data_source (str): source s3 directory for merged data
        assessment_destination (str): destination s3 directory for the assessment data
        publication_destination (str): destination s3 directory for the publication data
    """
    lf = utils.scan_parquet(merge_data_source)

    lf = lf.with_columns(
        cUtils.add_ct_filter_has_ct_data(),
        cUtils.add_ct_filter_consistent_service(),
        cUtils.add_ct_filter_dispersion_filter(),
    )

    assessment_lf, publication_lf = cUtils.split_into_assessment_and_publication_data(
        lf
    )

    utils.sink_to_parquet(
        lazy_df=assessment_lf,
        output_path=assessment_destination,
    )
    utils.sink_to_parquet(
        lazy_df=publication_lf,
        output_path=publication_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--merge_data_source",
            "Source s3 directory for merged data",
        ),
        (
            "--assessment_destination",
            "Destination s3 directory for the assessment data",
        ),
        (
            "--publication_destination",
            "Destination s3 directory for the publication data",
        ),
    )
    main(
        merge_data_source=args.merge_data_source,
        assessment_destination=args.assessment_destination,
        publication_destination=args.publication_destination,
    )
