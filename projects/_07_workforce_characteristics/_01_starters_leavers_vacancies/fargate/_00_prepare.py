import time

from polars_utils import utils
from projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils import (
    prepare_utils,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)

INDEX_COLUMNS = [AWPClean.establishment_id, AWPClean.ascwds_workplace_import_date]

QUERY_PLAN_GRAPH_PATH = "/tmp/candidate_b_query_plan.png"


def main(
    cleaned_ascwds_workplace_source: str,
    prepared_data_destination: str,
) -> None:
    """Reshape the cleaned ASCWDS workplace dataset's job-role columns from wide to long.

    Discovers the ASC-WDS job-role codes present in the source schema at runtime,
    then converts the 4-metrics-per-code wide SLV block into one row per
    (establishment_id, ascwds_workplace_import_date, job_role_code) using
    Candidate B's struct+concat_list+explode+unnest approach. Logs run time,
    peak RSS and the streaming query plan around the reshape and sink steps, to
    compare against Candidate A's prototype.

    Args:
        cleaned_ascwds_workplace_source (str): path to the cleaned ascwds workplace data
        prepared_data_destination (str): destination for output
    """
    print(f"[prepare][B] peak RSS before scan: {prepare_utils.peak_rss_kb()} KB")

    raw_lf = utils.scan_parquet(cleaned_ascwds_workplace_source)
    job_role_columns = prepare_utils.discover_job_role_codes(raw_lf.collect_schema())

    slv_source_columns = [
        column
        for cols in job_role_columns
        for column in (cols.employees, cols.starters, cols.leavers, cols.vacancies)
    ]
    workplace_lf = utils.scan_parquet(
        cleaned_ascwds_workplace_source,
        selected_columns=[*INDEX_COLUMNS, *slv_source_columns],
    )

    reshape_start = time.perf_counter()
    job_roles_lf = prepare_utils.convert_job_role_columns_to_rows(
        workplace_lf, INDEX_COLUMNS, job_role_columns
    )
    reshape_elapsed = time.perf_counter() - reshape_start
    print(f"[prepare][B] reshape build time (lazy): {reshape_elapsed:.3f}s")
    print(
        f"[prepare][B] peak RSS after reshape built: {prepare_utils.peak_rss_kb()} KB"
    )
    print(f"[prepare][B] query plan:\n{job_roles_lf.explain(engine='streaming')}")

    try:
        job_roles_lf.show_graph(
            engine="streaming",
            plan_stage="physical",
            output_path=QUERY_PLAN_GRAPH_PATH,
        )
    except Exception as graph_error:
        print(f"[prepare][B] could not render query plan graph: {graph_error}")

    sink_start = time.perf_counter()
    utils.sink_to_parquet(
        lazy_df=job_roles_lf,
        output_path=prepared_data_destination,
    )
    sink_elapsed = time.perf_counter() - sink_start
    print(f"[prepare][B] sink time: {sink_elapsed:.3f}s")
    print(f"[prepare][B] peak RSS after sink: {prepare_utils.peak_rss_kb()} KB")


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--cleaned_ascwds_workplace_source",
            "Source s3 directory for cleaned ascwds workplace data",
        ),
        (
            "--prepared_data_destination",
            "Destination s3 directory for prepared data",
        ),
    )
    main(
        cleaned_ascwds_workplace_source=args.cleaned_ascwds_workplace_source,
        prepared_data_destination=args.prepared_data_destination,
    )
