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

    ascwds_worker = spark.read.parquet(source_ascwds_worker)

    (
        start_period_import_date,
        end_period_import_date,
    ) = get_start_and_end_period_import_dates(ascwds_workplace, ascwds_worker)

    ascwds_workplace = filter_workplaces(
        ascwds_workplace, start_period_import_date, end_period_import_date
    )

    start_worker_df = get_ascwds_worker_df(
        starters_vs_leavers_df, source_start_worker_file
    )
    end_worker_df = get_ascwds_worker_df(starters_vs_leavers_df, source_end_worker_file)

    start_worker_df = determine_stayer_or_leaver(start_worker_df, end_worker_df)

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


def get_ascwds_worker_df(estab_list_df, worker_df):
    spark = utils.get_spark()

    worker_df = spark.read.parquet(worker_df)

    worker_df = worker_df.join(estab_list_df, on="establishmentid", how="inner")
    worker_df = worker_df.withColumn(
        "establishmentid_workerid",
        F.concat(F.col("establishmentid"), F.lit("_"), F.col("workerid")),
    )

    return worker_df


def determine_stayer_or_leaver(start_worker_df, end_worker_df):
    end_worker_df = end_worker_df.select("establishmentid_workerid")
    end_worker_df = end_worker_df.withColumn(
        "stayer_or_leaver", F.lit("still employed")
    )

    start_worker_df = start_worker_df.filter(
        (start_worker_df.emplstat == 190) | (start_worker_df.emplstat == 191)
    )
    start_worker_df = start_worker_df.join(
        end_worker_df, ["establishmentid_workerid"], "left"
    )
    start_worker_df = start_worker_df.fillna("leaver", subset="stayer_or_leaver")

    return start_worker_df


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
