import polars as pl

import projects._03_independent_cqc._02_employment_status.fargate.utils.magic_number_utils as mnUtils
from polars_utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusMagicNumberRateColumns as EmpStatRates,
)


def main(
    imputed_data_source: str,
    employment_status_rates_source: str,
    estimated_data_destination: str,
) -> None:
    """
    Creates estimates of employment status.

    Args:
        imputed_data_source (str): path to the imputed data
        employment_status_rates_source (str): path to the employment status rates csv
        estimated_data_destination (str): destination for output
    """
    imputed_data_lf = utils.scan_parquet(imputed_data_source)

    # The source CSV is expected to already be trimmed to exactly these columns, in this
    # order, and to only the current weighting year's rows — scan_csv's schema is matched
    # positionally, not by name, so a reordered file would silently load into the wrong
    # columns with no error.
    employment_status_rates_schema = pl.Schema(
        [
            (EmpStatRates.service, pl.Categorical()),
            (EmpStatRates.weighting_job_role, pl.Categorical()),
            (EmpStatRates.emp_stat_perm, pl.Float32),
            (EmpStatRates.emp_stat_temp, pl.Float32),
            (EmpStatRates.emp_stat_bank_or_pool, pl.Float32),
            (EmpStatRates.emp_stat_agency, pl.Float32),
            (EmpStatRates.emp_stat_other, pl.Float32),
        ]
    )

    employment_status_rates_lf = pl.scan_csv(
        employment_status_rates_source, schema=employment_status_rates_schema
    )

    imputed_data_lf = mnUtils.apply_employment_status_magic_numbers(
        imputed_data_lf, employment_status_rates_lf
    )

    utils.sink_to_parquet(
        lazy_df=imputed_data_lf,
        output_path=estimated_data_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--imputed_data_source",
            "Source s3 directory for imputed data",
        ),
        (
            "--employment_status_rates_source",
            "Source s3 directory for employment status rates data",
        ),
        (
            "--estimated_data_destination",
            "Destination s3 directory for estimated data",
        ),
    )
    main(
        imputed_data_source=args.imputed_data_source,
        employment_status_rates_source=args.employment_status_rates_source,
        estimated_data_destination=args.estimated_data_destination,
    )
