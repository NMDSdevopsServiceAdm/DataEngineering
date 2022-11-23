import builtins
import sys

import pyspark.sql.functions as F

from utils import utils


def main(
    workplace_source,
    cqc_location_source,
    cqc_provider_source,
    destination=None,
):

    print("Building locations prepared dataset")
    master_df = None

    last_processed_date = utils.get_max_snapshot_partitions(destination)
    if last_processed_date is not None:
        last_processed_date = f"{last_processed_date[0]}{last_processed_date[1]}{last_processed_date[2]}"
    complete_ascwds_workplace_df = get_ascwds_workplace_df(workplace_source, since_date=last_processed_date)
    complete_cqc_location_df = get_cqc_location_df(cqc_location_source, since_date=last_processed_date)
    complete_cqc_provider_df = get_cqc_provider_df(cqc_provider_source, since_date=last_processed_date)

    date_matrix = generate_closest_date_matrix(
        complete_ascwds_workplace_df,
        complete_cqc_location_df,
        complete_cqc_provider_df,
    )

    for snapshot_date_row in date_matrix:
        ascwds_workplace_df = complete_ascwds_workplace_df.filter(
            F.col("import_date") == snapshot_date_row["asc_workplace_date"]
        )
        ascwds_workplace_df = utils.format_import_date(ascwds_workplace_df)
        ascwds_workplace_df = purge_workplaces(ascwds_workplace_df)
        ascwds_workplace_df = ascwds_workplace_df.withColumnRenamed("import_date", "ascwds_workplace_import_date")

        cqc_locations_df = complete_cqc_location_df.filter(
            F.col("import_date") == snapshot_date_row["cqc_location_date"]
        )
        cqc_locations_df = utils.format_import_date(cqc_locations_df)
        cqc_locations_df = cqc_locations_df.withColumnRenamed("import_date", "cqc_locations_import_date")

        cqc_providers_df = complete_cqc_provider_df.filter(
            F.col("import_date") == snapshot_date_row["cqc_provider_date"]
        )
        cqc_providers_df = utils.format_import_date(cqc_providers_df)
        cqc_providers_df = cqc_providers_df.withColumnRenamed("import_date", "cqc_providers_import_date")

        output_df = cqc_locations_df.join(cqc_providers_df, "providerid", "left")
        output_df = output_df.join(ascwds_workplace_df, "locationid", "full")

        output_df = output_df.withColumn("snapshot_date", F.lit(snapshot_date_row["snapshot_date"]))
        output_df = output_df.withColumn("snapshot_year", F.col("snapshot_date").substr(1, 4))
        output_df = output_df.withColumn("snapshot_month", F.col("snapshot_date").substr(5, 2))
        output_df = output_df.withColumn("snapshot_day", F.col("snapshot_date").substr(7, 2))
        output_df = utils.format_import_date(output_df, fieldname="snapshot_date")

        output_df = output_df.select(
            "snapshot_date",
            "snapshot_year",
            "snapshot_month",
            "snapshot_day",
            "ascwds_workplace_import_date",
            "cqc_locations_import_date",
            "cqc_providers_import_date",
            "locationid",
            "location_type",
            "location_name",
            "organisation_type",
            "providerid",
            "provider_name",
            "orgid",
            "establishmentid",
            "registration_status",
            "registration_date",
            "deregistration_date",
            "carehome",
            "dormancy",
            "number_of_beds",
            "services_offered",
            "region",
            "postal_code",
            "constituency",
            "local_authority",
            "cqc_sector",
        )

        if destination:
            print("Exporting snapshot {} as parquet to {}".format(snapshot_date_row["snapshot_date"], destination))
            utils.write_to_parquet(
                output_df,
                destination,
                append=True,
                partitionKeys=["snapshot_year", "snapshot_month", "snapshot_day"],
            )
            if master_df is None:
                master_df = output_df
            else:
                master_df = master_df.union(output_df)
    return master_df


def get_ascwds_workplace_df(workplace_source, since_date=None):
    spark = utils.get_spark()

    print(f"Reading workplaces parquet from {workplace_source}")
    workplace_df = (
        spark.read.option("basePath", workplace_source)
        .parquet(workplace_source)
        .select(
            F.col("locationid"),
            F.col("establishmentid"),
            F.col("totalstaff").alias("total_staff"),
            F.col("wkrrecs").alias("worker_record_count"),
            F.col("import_date"),
            F.col("orgid"),
            F.col("mupddate"),
            F.col("isparent"),
        )
    )

    workplace_df = workplace_df.drop_duplicates(subset=["locationid", "import_date"])

    workplace_df = filter_out_import_dates_older_than(workplace_df, since_date)

    return workplace_df


def get_cqc_location_df(cqc_location_source, since_date=None):
    spark = utils.get_spark()

    print(f"Reading CQC locations parquet from {cqc_location_source}")
    cqc_df = (
        spark.read.option("basePath", cqc_location_source)
        .parquet(cqc_location_source)
        .select(
            F.col("locationid"),
            F.col("providerid"),
            F.col("organisationtype").alias("organisation_type"),
            F.col("type").alias("location_type"),
            F.col("name").alias("location_name"),
            F.col("registrationstatus").alias("registration_status"),
            F.col("registrationdate").alias("registration_date"),
            F.col("deregistrationdate").alias("deregistration_date"),
            F.col("dormancy"),
            F.col("numberofbeds").alias("number_of_beds"),
            F.col("region"),
            F.col("postalcode").alias("postal_code"),
            F.col("carehome"),
            F.col("constituency"),
            F.col("localauthority").alias("local_authority"),
            F.col("gacservicetypes.description").alias("services_offered"),
            F.col("import_date"),
        )
    )

    cqc_df = cqc_df.withColumn(
        "region",
        (F.when(cqc_df.region == "Yorkshire & Humberside", "Yorkshire and The Humber").otherwise(cqc_df.region)),
    )
    cqc_df = cqc_df.withColumn("dormancy", cqc_df.dormancy == "Y")

    cqc_df = cqc_df.filter("location_type=='Social Care Org'")
    cqc_df = filter_out_import_dates_older_than(cqc_df, since_date)

    return cqc_df


def filter_out_import_dates_older_than(df, date):
    if date is None:
        return df
    return df.filter(F.col("import_date") > date)


def get_cqc_provider_df(cqc_provider_source, since_date=None):
    spark = utils.get_spark()

    print(f"Reading CQC providers parquet from {cqc_provider_source}")
    cqc_provider_df = (
        spark.read.option("basePath", cqc_provider_source)
        .parquet(cqc_provider_source)
        .select(
            F.col("providerid"),
            F.col("name").alias("provider_name"),
            F.col("import_date"),
        )
    )

    # cqc_provider_df = add_cqc_sector(cqc_provider_df)

    cqc_provider_df = filter_out_import_dates_older_than(cqc_provider_df, since_date)

    return cqc_provider_df


def get_unique_import_dates(df):
    distinct_ordered_import_date_df = df.select("import_date").distinct().orderBy("import_date")
    distinct_ordered_import_date_list = [date.import_date for date in distinct_ordered_import_date_df.collect()]
    return distinct_ordered_import_date_list


def get_date_closest_to_search_date(search_date, date_list):
    try:
        closest_date = builtins.max(d for d in date_list if d <= search_date)
    except ValueError:
        # No dates in search_list provided less than search date
        return None

    return closest_date


def generate_closest_date_matrix(dataset_workplace, dataset_locations_api, dataset_providers_api):
    unique_asc_dates = get_unique_import_dates(dataset_workplace)
    unique_cqc_location_dates = get_unique_import_dates(dataset_locations_api)
    unique_cqc_provider_dates = get_unique_import_dates(dataset_providers_api)

    date_matrix = []
    for date in unique_asc_dates:
        closest_cqc_location_date = get_date_closest_to_search_date(date, unique_cqc_location_dates)
        closest_cqc_provider_date = get_date_closest_to_search_date(date, unique_cqc_provider_dates)
        date_matrix.append(
            {
                "snapshot_date": date,
                "asc_workplace_date": date,
                "cqc_location_date": closest_cqc_location_date,
                "cqc_provider_date": closest_cqc_provider_date,
            }
        )

    return date_matrix


def purge_workplaces(input_df):
    # Remove all locations that haven't been update for two years
    print("Purging ASCWDS accounts...")

    # Convert import_date to date field and remove 2 years
    input_df = input_df.withColumn("purge_date", F.add_months(F.col("import_date"), -24))

    # if the org is a parent, use the max mupddate for all locations at the org
    org_purge_df = (
        input_df.select("locationid", "orgid", "mupddate", "import_date")
        .groupBy("orgid", "import_date")
        .agg(F.max("mupddate").alias("mupddate_org"))
    )
    input_df = input_df.join(org_purge_df, ["orgid", "import_date"], "left")
    input_df = input_df.withColumn(
        "date_for_purge",
        F.when((input_df.isparent == "1"), input_df.mupddate_org).otherwise(input_df.mupddate),
    )

    # Remove ASCWDS accounts which haven't been updated in the 2 years prior to importing
    input_df = input_df.filter(input_df.purge_date < input_df.date_for_purge)

    input_df.drop("isparent", "mupddate")

    return input_df


if __name__ == "__main__":
    print("Spark job 'cqc coverage based on login date purge' starting...")
    print(f"Job parameters: {sys.argv}")
    (workplace_source, cqc_location_source, cqc_provider_source, destination,) = utils.collect_arguments(
        ("--workplace_source", "Source s3 directory for ASCWDS workplace dataset"),
        ("--cqc_location_source", "Source s3 directory for CQC locations api dataset"),
        ("--cqc_provider_source", "Source s3 directory for CQC providers api dataset"),
        ("--destination", "A destination directory for outputting cqc locations."),
    )

    main(
        workplace_source,
        cqc_location_source,
        cqc_provider_source,
        destination,
    )

    print("Spark job 'cqc coverage based on login date purge' complete")
