from polars_utils import utils


def main(
    slv_estimate_source: str,
    employment_status_estimate_source: str,
    estimated_data_destination: str,
) -> None:
    """
    Placeholder for deriving estimated headcount counts of starters, leavers
    and vacancies.

    Args:
        slv_estimate_source (str): path to the SLV estimated data (rates)
        employment_status_estimate_source (str): path to the employment
            status estimated data (headcount)
        estimated_data_destination (str): destination for output
    """
    slv_estimate_lf = utils.scan_parquet(slv_estimate_source)
    employment_status_estimate_lf = utils.scan_parquet(
        employment_status_estimate_source
    )

    # TODO: join slv_estimate_lf and employment_status_estimate_lf on shared keys

    # TODO: calculate estimated starters/leavers/vacancies as estimated
    # employees * estimated starter rate, estimated employees * estimated
    # turnover rate and (estimated employees * estimated vacancy rate) / (1 -
    # estimated vacancy rate)

    utils.sink_to_parquet(
        lazy_df=slv_estimate_lf,
        output_path=estimated_data_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--slv_estimate_source",
            "Source s3 directory for SLV estimated data",
        ),
        (
            "--employment_status_estimate_source",
            "Source s3 directory for employment status estimated data",
        ),
        (
            "--estimated_data_destination",
            "Destination s3 directory for estimated count data",
        ),
    )
    main(
        slv_estimate_source=args.slv_estimate_source,
        employment_status_estimate_source=args.employment_status_estimate_source,
        estimated_data_destination=args.estimated_data_destination,
    )
