import builtins
import sys

import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

from utils import utils
from utils.prepare_locations_utils.job_calculator import calculate_jobcount
from utils.prepare_locations_utils.ons_postcode_aliases import OnsPostcodeDataAliases

LAST_PROCESSED_DATE_INDEX_ZERO = 0
LAST_PROCESSED_DATE_INDEX_ONE = 1
LAST_PROCESSED_DATE_INDEX_TWO = 2

START_OF_YEAR_SUBSTRING = 1
LENGTH_OF_YEAR_SUBSTRING = 4
START_OF_MONTH_SUBSTRING = 5
LENGTH_OF_MONTH_SUBSTRING = 2
START_OF_DAY_SUBSTRING = 7
LENGTH_OF_DAY_SUBSTRING = 2

MONTHS_BETWEEN_IMPORT_DATE_AND_PURGE_DATE = -24

# MIN_ABS_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT = 5
# MIN_PERCENTAGE_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT = 0.1

MIN_TOTAL_STAFF_VALUE_PERMITTED = 3
MIN_WORKER_RECORD_COUNT_PERMITTED = 3

# BEDS_IN_WORKPLACE_THRESHOLD = 0
#
# BEDS_TO_JOB_COUNT_INTERCEPT = 8.40975704621392
# BEDS_TO_JOB_COUNT_COEFFICIENT = 1.0010753137758377001

LOCATIONID_IS_IN_ASCWDS = 1
LOCATIONID_IS_NOT_IN_ASCWDS = 0


def main(
    workplace_source,
    cqc_location_source,
    cqc_provider_source,
    pir_source,
    ons_source,
    destination=None,
):
    print("Building locations prepared dataset")
    master_df = None

    last_processed_date = utils.get_max_snapshot_partitions(destination)
    if last_processed_date is not None:
        last_processed_date = f"{last_processed_date[LAST_PROCESSED_DATE_INDEX_ZERO]}{last_processed_date[LAST_PROCESSED_DATE_INDEX_ONE]}{last_processed_date[LAST_PROCESSED_DATE_INDEX_TWO]}"
    complete_ascwds_workplace_df = get_ascwds_workplace_df(
        workplace_source, since_date=last_processed_date
    )
    complete_cqc_location_df = get_cqc_location_df(
        cqc_location_source, since_date=last_processed_date
    )
    complete_cqc_provider_df = get_cqc_provider_df(
        cqc_provider_source, since_date=last_processed_date
    )
    complete_pir_df = get_pir_df(pir_source, since_date=last_processed_date)

    latest_ons_data = get_ons_df(ons_source)

    date_matrix = generate_closest_date_matrix(
        complete_ascwds_workplace_df,
        complete_cqc_location_df,
        complete_cqc_provider_df,
        complete_pir_df,
    )

    for snapshot_date_row in date_matrix:
        ascwds_workplace_df = complete_ascwds_workplace_df.filter(
            F.col("import_date") == snapshot_date_row["asc_workplace_date"]
        )
        ascwds_workplace_df = utils.format_import_date(ascwds_workplace_df)
        ascwds_workplace_df = utils.format_date_fields(
            ascwds_workplace_df,
            raw_date_format="dd/MM/yyyy",
            date_column_identifier="lastloggedin",
        )

        ascwds_coverage_df = create_coverage_df(ascwds_workplace_df)

        ascwds_workplace_df = purge_workplaces(ascwds_workplace_df)

        ascwds_workplace_with_coverage_df = ascwds_coverage_df.join(
            ascwds_workplace_df,
            ["locationid", "establishmentid", "import_date"],
            "left",
        )
        ascwds_workplace_with_coverage_df = (
            ascwds_workplace_with_coverage_df.withColumnRenamed(
                "import_date", "ascwds_workplace_import_date"
            )
        )

        cqc_locations_df = complete_cqc_location_df.filter(
            F.col("import_date") == snapshot_date_row["cqc_location_date"]
        )
        cqc_locations_df = utils.format_import_date(cqc_locations_df)
        cqc_locations_df = cqc_locations_df.withColumnRenamed(
            "import_date", "cqc_locations_import_date"
        )

        cqc_providers_df = complete_cqc_provider_df.filter(
            F.col("import_date") == snapshot_date_row["cqc_provider_date"]
        )
        cqc_providers_df = utils.format_import_date(cqc_providers_df)
        cqc_providers_df = cqc_providers_df.withColumnRenamed(
            "import_date", "cqc_providers_import_date"
        )

        pir_df = complete_pir_df.filter(
            F.col("import_date") == snapshot_date_row["pir_date"]
        )
        pir_df = utils.format_import_date(pir_df)
        pir_df = pir_df.withColumnRenamed("import_date", "cqc_pir_import_date")

        output_df = cqc_locations_df.join(cqc_providers_df, "providerid", "left")
        output_df = output_df.join(
            ascwds_workplace_with_coverage_df, "locationid", "full"
        )

        output_df = output_df.join(pir_df, "locationid", "left")
        output_df = add_geographical_data(output_df, latest_ons_data)
        output_df = calculate_jobcount(output_df)

        output_df = add_column_if_locationid_is_in_ascwds(output_df)

        output_df = output_df.withColumn(
            "snapshot_date", F.lit(snapshot_date_row["snapshot_date"])
        )
        output_df = output_df.withColumn(
            "snapshot_year",
            F.col("snapshot_date").substr(
                START_OF_YEAR_SUBSTRING, LENGTH_OF_YEAR_SUBSTRING
            ),
        )
        output_df = output_df.withColumn(
            "snapshot_month",
            F.col("snapshot_date").substr(
                START_OF_MONTH_SUBSTRING, LENGTH_OF_MONTH_SUBSTRING
            ),
        )
        output_df = output_df.withColumn(
            "snapshot_day",
            F.col("snapshot_date").substr(
                START_OF_YEAR_SUBSTRING, LENGTH_OF_MONTH_SUBSTRING
            ),
        )
        output_df = utils.format_import_date(output_df, fieldname="snapshot_date")

        output_df = output_df.select(
            "snapshot_date",
            "snapshot_year",
            "snapshot_month",
            "snapshot_day",
            "ascwds_workplace_import_date",
            "cqc_locations_import_date",
            "cqc_providers_import_date",
            "cqc_pir_import_date",
            "ons_import_date",
            "locationid",
            "location_type",
            "location_name",
            "organisation_type",
            "providerid",
            "provider_name",
            "orgid",
            "establishmentid",
            "cqc_coverage_in_ascwds",
            "registration_status",
            "registration_date",
            "deregistration_date",
            "carehome",
            "dormancy",
            "number_of_beds",
            "services_offered",
            "people_directly_employed",
            "job_count",
            "region",
            "postal_code",
            "constituency",
            "local_authority",
            "cqc_sector",
            "ons_region",
            "nhs_england_region",
            "country",
            "lsoa",
            "msoa",
            "stp",
            "clinical_commisioning_group",
            "rural_urban_indicator",
            "oslaua",
        )

        output_df.show()

        if destination:
            print(
                "Exporting snapshot {} as parquet to {}".format(
                    snapshot_date_row["snapshot_date"], destination
                )
            )
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
            F.col("parentid"),
            F.col("lastloggedin"),
        )
    )

    workplace_df = workplace_df.drop_duplicates(subset=["locationid", "import_date"])
    workplace_df = clean(workplace_df)

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
        (
            F.when(
                cqc_df.region == "Yorkshire & Humberside", "Yorkshire and The Humber"
            ).otherwise(cqc_df.region)
        ),
    )
    cqc_df = cqc_df.withColumn("dormancy", cqc_df.dormancy == "Y")

    cqc_df = cqc_df.filter("location_type=='Social Care Org'")
    cqc_df = filter_out_import_dates_older_than(cqc_df, since_date)
    cqc_df = map_illegitimate_postcodes(cqc_df)

    return cqc_df


def filter_out_import_dates_older_than(df, date):
    if date is None:
        return df
    return df.filter(F.col("import_date") > date)


def map_illegitimate_postcodes(cqc_loc_df, column="postal_code"):
    post_codes_mapping = {
        "B69 E3G": "B69 3EG",
        "UB4 0EJ.": "UB4 0EJ",
        "TS17 3TB": "TS18 3TB",
        "TF7 3BY": "TF4 3BY",
        "S26 4DW": "S26 4WD",
        "B7 5DP": "B7 5PD",
        "DE1 IBT": "DE1 1BT",
        "YO61 3FF": "YO61 3FN",
        "L20 4QC": "L20 4QG",
        "PR! 9HL": "PR1 9HL",
        "N8 5HY": "N8 7HS",
        "PA20 3AR": "PO20 3BD",
        "CRO 4TB": "CR0 4TB",
        "PA20 3BD": "PO20 3BD",
        "LE65 3LP": "LE65 2RW",
        "NG6 3DG": "NG5 2AT",
        "HU17 ORH": "HU17 0RH",
        "SY2 9JN": "SY3 9JN",
        "CA1 2XT": "CA1 2TX",
        "MK9 1HF": "MK9 1FH",
        "HP20 1SN.": "HP20 1SN",
        "CH41 1UE": "CH41 1EU",
        "WR13 3JS": "WF13 3JS",
        "B12 ODG": "B12 0DG",
        "PO8 4PY": "PO4 8PY",
        "TS20 2BI": "TS20 2BL",
        "NG10 9LA": "NN10 9LA",
        "SG1 8AL": "SG18 8AL",
        "CCT8 8SA": "CT8 8SA",
        "OX4 2XQ": "OX4 2SQ",
        "B66 2FF": "B66 2AL",
        "EC2 5UU": "EC2M 5UU",
        "HU21 0LS": "HU170LS",
        "PL7 1RP": "PL7 1RF",
        "WF12 2SE": "WF13 2SE",
        "N12 8FP": "N12 8NP",
        "ST4 4GF": "ST4 7AA",
        "BN6 4EA": "BN16 4EA",
        "B97 6DT": "B97 6AT",
    }
    map_func = F.udf(lambda row: post_codes_mapping.get(row, row))
    return cqc_loc_df.withColumn("postal_code", map_func(F.col(column)))


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

    cqc_provider_df = add_cqc_sector(cqc_provider_df)

    cqc_provider_df = filter_out_import_dates_older_than(cqc_provider_df, since_date)

    return cqc_provider_df


def get_pir_df(pir_source, since_date=None):
    spark = utils.get_spark()

    # Join PIR service users
    print(f"Reading PIR parquet from {pir_source}")
    pir_df = (
        spark.read.option("basePath", pir_source)
        .parquet(pir_source)
        .select(
            F.col("location_id").alias("locationid"),
            F.col(
                "how_many_people_are_directly_employed"
                "_and_deliver_regulated_activities_at_"
                "your_service_as_part_of_their_daily_duties"
            )
            .alias("people_directly_employed")
            .cast("integer"),
            F.col("import_date"),
        )
    )

    pir_df = pir_df.dropDuplicates(["locationid", "import_date"])

    pir_df = filter_out_import_dates_older_than(pir_df, since_date)

    return pir_df


def get_ons_df(ons_source):
    spark = utils.get_spark()

    print(f"Reading ONS data from {ons_source}")
    ons_df = spark.read.option("basePath", ons_source).parquet(ons_source)
    ons_df = utils.get_latest_partition(ons_df, partition_keys=("year", "month", "day"))
    ons_df = ons_df.select(
        ons_df.pcd.alias(OnsPostcodeDataAliases.ons_postcode),
        ons_df.rgn.alias(OnsPostcodeDataAliases.region_alias),
        ons_df.nhser.alias(OnsPostcodeDataAliases.nhs_england_region_alias),
        ons_df.ctry.alias(OnsPostcodeDataAliases.country_alias),
        ons_df.lsoa.alias(OnsPostcodeDataAliases.lsoa_alias),
        ons_df.msoa.alias(OnsPostcodeDataAliases.msoa_alias),
        ons_df.ccg.alias(OnsPostcodeDataAliases.ccg_alias),
        ons_df.ru_ind.alias(OnsPostcodeDataAliases.rural_urban_indicator_alias),
        ons_df.stp,
        ons_df.oslaua,
        ons_df.year,
        ons_df.month,
        ons_df.day,
        ons_df.import_date.alias(OnsPostcodeDataAliases.import_date_alias),
    )

    return ons_df


def get_unique_import_dates(df):
    distinct_ordered_import_date_df = (
        df.select("import_date").distinct().orderBy("import_date")
    )
    distinct_ordered_import_date_list = [
        date.import_date for date in distinct_ordered_import_date_df.collect()
    ]
    return distinct_ordered_import_date_list


def get_date_closest_to_search_date(search_date, date_list):
    try:
        closest_date = builtins.max(d for d in date_list if d <= search_date)
    except ValueError:
        # No dates in search_list provided less than search date
        return None

    return closest_date


def generate_closest_date_matrix(
    dataset_workplace, dataset_locations_api, dataset_providers_api, dataset_pir
):
    unique_asc_dates = get_unique_import_dates(dataset_workplace)
    unique_cqc_location_dates = get_unique_import_dates(dataset_locations_api)
    unique_cqc_provider_dates = get_unique_import_dates(dataset_providers_api)
    unique_pir_dates = get_unique_import_dates(dataset_pir)

    date_matrix = []
    for date in unique_asc_dates:
        closest_cqc_location_date = get_date_closest_to_search_date(
            date, unique_cqc_location_dates
        )
        closest_cqc_provider_date = get_date_closest_to_search_date(
            date, unique_cqc_provider_dates
        )
        closest_pir_date = get_date_closest_to_search_date(date, unique_pir_dates)
        date_matrix.append(
            {
                "snapshot_date": date,
                "asc_workplace_date": date,
                "cqc_location_date": closest_cqc_location_date,
                "cqc_provider_date": closest_cqc_provider_date,
                "pir_date": closest_pir_date,
            }
        )

    return date_matrix


def add_geographical_data(locations_df, ons_df):
    locations_df = locations_df.withColumn(
        "postal_code_ws_removed", F.regexp_replace(locations_df.postal_code, " ", "")
    )
    ons_df = ons_df.withColumn(
        "ons_postcode", F.regexp_replace(ons_df.ons_postcode, " ", "")
    )
    locations_df = locations_df.join(
        ons_df,
        F.upper(locations_df.postal_code_ws_removed) == F.upper(ons_df.ons_postcode),
        "left",
    )

    locations_df.drop("postal_code_ws_removed")
    locations_df.drop("ons_postcode")

    return locations_df


def clean(input_df):
    print("Cleaning...")

    # Standardise negative and 0 values as None.
    input_df = input_df.replace("0", None).replace("-1", None)

    # Cast strings to integers
    input_df = input_df.withColumn(
        "total_staff", input_df["total_staff"].cast(IntegerType())
    )

    input_df = input_df.withColumn(
        "worker_record_count", input_df["worker_record_count"].cast(IntegerType())
    )

    return input_df


def create_coverage_df(input_df):
    # Remove all locations that haven't been update for two years
    print("Purging ASCWDS accounts...")

    # Convert import_date to date field and remove 2 years
    input_df = input_df.withColumn(
        "purge_date",
        F.add_months(F.col("import_date"), MONTHS_BETWEEN_IMPORT_DATE_AND_PURGE_DATE),
    )

    # Use most recent of mupdate or lastloggedin for purge
    input_df = input_df.withColumn(
        "max_mupddate_and_lastloggedin",
        F.greatest(F.col("mupddate"), F.col("lastloggedin")),
    )

    # if the org is a parent, use the max mupddate for all locations at the org
    org_purge_df = (
        input_df.select(
            "locationid", "orgid", "max_mupddate_and_lastloggedin", "import_date"
        )
        .groupBy("orgid", "import_date")
        .agg(
            F.max("max_mupddate_and_lastloggedin").alias(
                "max_mupddate_and_lastloggedin_org"
            )
        )
    )
    input_df = input_df.join(org_purge_df, ["orgid", "import_date"], "left")
    input_df = input_df.withColumn(
        "date_for_purge",
        F.when(
            (input_df.isparent == "1"), input_df.max_mupddate_and_lastloggedin_org
        ).otherwise(input_df.max_mupddate_and_lastloggedin),
    )

    # Remove ASCWDS accounts which haven't been updated in the 2 years prior to importing
    input_df = input_df.filter(input_df.purge_date < input_df.date_for_purge)

    input_df = input_df.select("locationid", "establishmentid", "import_date")

    return input_df


def purge_workplaces(input_df):
    # Remove all locations that haven't been update for two years
    print("Purging ASCWDS accounts...")

    # Convert import_date to date field and remove 2 years
    input_df = input_df.withColumn(
        "purge_date",
        F.add_months(F.col("import_date"), MONTHS_BETWEEN_IMPORT_DATE_AND_PURGE_DATE),
    )

    # if the org is a parent, use the max mupddate for all locations at the org
    org_purge_df = (
        input_df.select("locationid", "orgid", "mupddate", "import_date")
        .groupBy("orgid", "import_date")
        .agg(F.max("mupddate").alias("mupddate_org"))
    )
    input_df = input_df.join(org_purge_df, ["orgid", "import_date"], "left")
    input_df = input_df.withColumn(
        "date_for_purge",
        F.when((input_df.isparent == "1"), input_df.mupddate_org).otherwise(
            input_df.mupddate
        ),
    )

    # Remove ASCWDS accounts which haven't been updated in the 2 years prior to importing
    input_df = input_df.filter(input_df.purge_date < input_df.date_for_purge)

    input_df = input_df.drop("isparent", "mupddate")

    return input_df


def add_cqc_sector(input_df):
    print("Adding CQC sector column...")

    # allocate service based on provider name
    input_df = input_df.withColumn(
        "cqc_sector",
        F.lower(input_df.provider_name).rlike(
            "department of community services|social & community services|scc adult social care|cheshire west and chester reablement service|council|social services|mbc|mdc|london borough|royal borough|borough of"
        )
        & ~F.lower(input_df.provider_name).rlike("the council of st monica trust"),
    )

    input_df = input_df.withColumn(
        "cqc_sector",
        F.when(input_df.cqc_sector == "false", "Independent").otherwise(
            "Local authority"
        ),
    )

    return input_df





def add_column_if_locationid_is_in_ascwds(df):
    df = df.withColumn(
        "cqc_coverage_in_ascwds",
        F.when(df.establishmentid.isNull(), LOCATIONID_IS_NOT_IN_ASCWDS).otherwise(
            LOCATIONID_IS_IN_ASCWDS
        ),
    )

    return df


if __name__ == "__main__":
    print("Spark job 'prepare_locations' starting...")
    print(f"Job parameters: {sys.argv}")
    (
        workplace_source,
        cqc_location_source,
        cqc_provider_source,
        pir_source,
        ons_source,
        destination,
    ) = utils.collect_arguments(
        ("--workplace_source", "Source s3 directory for ASCWDS workplace dataset"),
        ("--cqc_location_source", "Source s3 directory for CQC locations api dataset"),
        ("--cqc_provider_source", "Source s3 directory for CQC providers api dataset"),
        ("--pir_source", "Source s3 directory for pir dataset"),
        ("--ons_source", "Source s3 directory for ons dataset"),
        ("--destination", "A destination directory for outputting cqc locations."),
    )

    main(
        workplace_source,
        cqc_location_source,
        cqc_provider_source,
        pir_source,
        ons_source,
        destination,
    )

    print("Spark job 'prepare_locations' complete")
