import argparse

import pyspark.sql.functions as F

from utils import utils


def main(
    source_start_workplace_file,
    source_start_worker_file,
    source_end_workplace_file,
    source_end_worker_file,
    destination=None,
):

    print("Creating stayer vs leaver parquet file")
    start_workplace_df = updated_within_time_period(source_start_workplace_file)

    end_workplace_df = updated_within_time_period(source_end_workplace_file)

    starters_vs_leavers_df = workplaces_in_both_dfs(
        start_workplace_df, end_workplace_df
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


def updated_within_time_period(df):
    spark = utils.get_spark()

    df = spark.read.parquet(df)
    df = df.select("establishmentid", "mupddate", "import_date", "wkrrecs")
    df = utils.format_import_date(df)
    df = df.withColumn("mupddate_cutoff", F.add_months(df.import_date, -6))
    df = df.filter((df.mupddate > df.mupddate_cutoff) & (df.wkrrecs >= 1))
    df = df.select("establishmentid")

    return df


def workplaces_in_both_dfs(start_workplace_df, end_workplace_df):

    df = start_workplace_df.join(end_workplace_df, ["establishmentid"], "inner")

    return df


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
        "--source_start_workplace_file",
        help="Source s3 directory for ASCWDS workplace dataset - start of 12 month period.",
        required=True,
    )
    parser.add_argument(
        "--source_start_worker_file",
        help="Source s3 directory for ASCWDS worker dataset - start of 12 month period.",
        required=True,
    )
    parser.add_argument(
        "--source_end_workplace_file",
        help="Source s3 directory for ASCWDS workplace dataset - end of 12 month period.",
        required=True,
    )
    parser.add_argument(
        "--source_end_worker_file",
        help="Source s3 directory for ASCWDS worker dataset - end of 12 month period.",
        required=True,
    )
    parser.add_argument(
        "--destination",
        help="A destination directory for outputting parquet file.",
        required=True,
    )

    args, unknown = parser.parse_known_args()

    return (
        args.source_start_workplace_file,
        args.source_start_worker_file,
        args.source_end_workplace_file,
        args.source_end_worker_file,
        args.destination,
    )


if __name__ == "__main__":
    (
        source_start_workplace_file,
        source_start_worker_file,
        source_end_workplace_file,
        source_end_worker_file,
        destination,
    ) = collect_arguments()

    main(
        source_start_workplace_file,
        source_start_worker_file,
        source_end_workplace_file,
        source_end_worker_file,
        destination,
    )
