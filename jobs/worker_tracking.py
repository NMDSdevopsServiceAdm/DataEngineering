import argparse
from time import strptime

import pyspark.sql.functions as F
from datetime import datetime, timedelta

from utils import utils


def main(
    source_ascwds_workplace,
    source_ascwds_worker,
    destination=None,
):

    spark = utils.get_spark()

    print("Creating stayer vs leaver parquet file")

    ascwds_workplace = spark.read.parquet(source_ascwds_workplace).select(
        "establishmentid",
        "mupddate",
        "import_date",
        "wkrrecs",
    )

    ascwds_worker = get_employees_with_new_identifier(source_ascwds_worker)

    (
        start_period_import_date,
        end_period_import_date,
    ) = get_start_and_end_period_import_dates(ascwds_workplace, ascwds_worker)

    ascwds_workplace = filter_workplaces(
        ascwds_workplace, start_period_import_date, end_period_import_date
    )

    start_worker_df = determine_stayer_or_leaver(
        ascwds_worker, start_period_import_date, end_period_import_date
    )

    if destination:
        print(f"Exporting as parquet to {destination}")
        utils.write_to_parquet(start_worker_df, destination)
    else:
        return start_worker_df


def max_import_date_in_two_datasets(workplace_df, worker_df):
    workplace_df = workplace_df.join(worker_df, ["import_date"], "inner")

    max_import_date = workplace_df.select(F.max(F.col("import_date")).alias("max"))

    return max_import_date.first().max


def get_start_period_import_date(workplace_df, worker_df, end_period_import_date):
    import_date = workplace_df.join(worker_df, ["import_date"], "inner")

    max_import_date_as_date = datetime.strptime(end_period_import_date, "%Y%m%d")

    start_period_import_date = max_import_date_as_date - timedelta(days=365)

    start_period_import_date = start_period_import_date.strftime("%Y%m%d")

    import_date = import_date.filter(F.col("import_date") <= start_period_import_date)

    max_import_date = import_date.select(F.max(F.col("import_date")).alias("max"))

    return max_import_date.first().max


def get_start_and_end_period_import_dates(workplace_df, worker_df):
    end_period_import_date = max_import_date_in_two_datasets(workplace_df, worker_df)

    start_period_import_date = get_start_period_import_date(
        workplace_df, worker_df, end_period_import_date
    )

    return start_period_import_date, end_period_import_date


def filter_workplaces(
    ascwds_workplace, start_period_import_date, end_period_import_date
):

    df = ascwds_workplace.filter(
        (ascwds_workplace.import_date == start_period_import_date)
        | (ascwds_workplace.import_date == end_period_import_date)
    )

    df = utils.format_import_date(df)
    df = df.withColumn("mupddate_cutoff", F.add_months(df.import_date, -6))
    df = df.filter((df.mupddate > df.mupddate_cutoff) & (df.wkrrecs >= 1))
    df = df.select("establishmentid")

    workplace_count = df.join(
        df.groupBy("establishmentid").count(), on="establishmentid"
    )

    workplaces_to_include = workplace_count.filter(workplace_count["count"] == 2)

    return workplaces_to_include


def get_employees_with_new_identifier(source_ascwds_worker):
    spark = utils.get_spark()

    worker_df = spark.read.parquet(source_ascwds_worker)

    # employees are permament (=190) or temporary (=191) employed staff ('emplsat')
    worker_df = worker_df.filter(
        (worker_df.emplstat == 190) | (worker_df.emplstat == 191)
    )

    worker_df = worker_df.withColumn(
        "establishment_worker_id",
        F.concat(F.col("establishmentid"), F.lit("_"), F.col("workerid")),
    )

    return worker_df


def determine_stayer_or_leaver(
    ascwds_worker, start_period_import_date, end_period_import_date
):
    end_worker_df = get_relevant_end_period_workers(
        ascwds_worker, end_period_import_date
    )

    start_worker_df = get_relevant_start_period_workers(
        ascwds_worker, start_period_import_date
    )

    start_worker_df = start_worker_df.join(
        end_worker_df, ["establishment_worker_id"], "left"
    )

    start_worker_df = start_worker_df.fillna("leaver", subset="stayer_or_leaver")

    return start_worker_df


def get_relevant_start_period_workers(ascwds_worker, start_period_import_date):
    start_worker_df = ascwds_worker.filter(
        ascwds_worker.import_date == start_period_import_date
    )

    return start_worker_df


def get_relevant_end_period_workers(ascwds_worker, end_period_import_date):
    end_worker_df = ascwds_worker.filter(
        ascwds_worker.import_date == end_period_import_date
    )
    end_worker_df = end_worker_df.select("establishment_worker_id")
    end_worker_df = end_worker_df.withColumn(
        "stayer_or_leaver", F.lit("still employed")
    )

    return end_worker_df


def collect_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--source_ascwds_workplace",
        help="Source s3 directory for ASCWDS workplace data.",
        required=True,
    )
    parser.add_argument(
        "--source_ascwds_worker",
        help="Source s3 directory for ASCWDS worker data.",
        required=True,
    )
    parser.add_argument(
        "--destination",
        help="A destination directory for outputting parquet file.",
        required=True,
    )

    args, unknown = parser.parse_known_args()

    return (
        args.source_ascwds_workplace,
        args.source_ascwds_worker,
        args.destination,
    )


if __name__ == "__main__":
    (
        source_ascwds_workplace,
        source_ascwds_worker,
        destination,
    ) = collect_arguments()

    main(
        source_ascwds_workplace,
        source_ascwds_worker,
        destination,
    )
