import argparse

from pyspark.sql.functions import udf, col, trim, least, lit, greatest
from pyspark.sql.types import StringType


from utils import utils

ETHNICITY_DICT = {
    "white": ["31", "32", "33", "34"],
    "mixed": ["35", "36", "37", "38"],
    "black": ["39", "40", "41", "42", "43"],
    "asian": ["44", "45", "46"],
    "other": ["47", "98"],
}

MAINJRID_DICT = {
    "Senior management": ["1"],
    "Registered manager": ["4"],
    "Social worker": ["6"],
    "Other direct care": ["10", "11", "22", "23", "38"],
    "Senior care worker": ["7"],
    "Care worker": ["8"],
    "Support and outreach": ["9"],
    "Occupational therapist": ["15"],
    "Registered nurse": ["16"],
    "Allied health professional": ["17"],
    "Other managers": ["2", "3", "5", "24"],
    "Other professional": ["35", "37"],
    "All other": ["25", "26", "27", "34", "36", "39", "40", "41", "42"],
}

REGION_DICT = {
    "North East": "E12000001",
    "North West": "E12000002",
    "Yorkshire and The Humber": "E12000003",
    "East Midlands": "E12000004",
    "West Midlands": "E12000005",
    "East of England": "E12000006",
    "London": "E12000007",
    "South East": "E12000008",
    "South West": "E12000009",
}

MAGIC_SERVICE_WHITE_DICT = {
    -0.046: "Care home with nursing",
    -0.012: "Care home without nursing",
    0.0: "non-residential",
}

MAGIC_JOBROLE_WHITE_DICT = {
    0.109: "All other",
    0.022: "Care worker",
    0.08: "Other direct care",
    0.098: "Other managers",
    -0.051: "Other professional",
    0.088: "Registered manager",
    -0.114: "Registered nurse",
    0.066: "Senior care worker",
    0.081: "Senior management",
    0.045: "Support and outreach",
    0.013: "Social worker",
    0.175: "Occupational therapist",
    0.0: "Allied health professional",
}

MAGIC_REGION_WHITE_DICT = {
    0.045: "North East",
    0.021: ["North West", "Yorkshire and The Humber"],
    -0.008: "East Midlands",
    -0.031: "West Midlands",
    -0.063: "East of England",
    -0.339: "London",
    -0.071: "South East",
    0.0: "South West",
}


def main(
    job_roles_per_location_source,
    cqc_locations_prepared_source,
    ons_source,
    worker_source,
    census_source,
    destination=None,
):

    all_job_roles_df = get_all_job_roles_per_location_df(job_roles_per_location_source)

    cqc_locations_df = get_cqc_locations_df(cqc_locations_prepared_source)

    all_job_roles_df = all_job_roles_df.join(
        cqc_locations_df, all_job_roles_df.master_locationid == cqc_locations_df.locationid, "left"
    ).drop("locationid")

    ons_df = get_ons_geography_df(ons_source)

    all_job_roles_df = all_job_roles_df.join(ons_df, all_job_roles_df.postal_code == ons_df.ons_postcode, "left").drop(
        "ons_postcode"
    )

    ascwds_ethnicity_df = get_ascwds_ethnicity_df(worker_source)
    ascwds_ethnicity_df = rename_column_values(ascwds_ethnicity_df, "ethnicity", ETHNICITY_DICT)
    ascwds_ethnicity_df = ascwds_ethnicity_df.groupBy("locationid", "mainjrid").pivot("ethnicity").count()
    ascwds_ethnicity_df = ascwds_ethnicity_df.fillna(0)
    ascwds_ethnicity_df = ascwds_ethnicity_df.withColumn(
        "ethnicity_base",
        sum(
            [
                ascwds_ethnicity_df.asian,
                ascwds_ethnicity_df.black,
                ascwds_ethnicity_df.mixed,
                ascwds_ethnicity_df.other,
                ascwds_ethnicity_df.white,
            ]
        ),
    )

    all_job_roles_df = all_job_roles_df.join(
        ascwds_ethnicity_df,
        (all_job_roles_df.master_locationid == ascwds_ethnicity_df.locationid)
        & (all_job_roles_df.main_job_role == ascwds_ethnicity_df.mainjrid),
        "left",
    ).drop("locationid", "mainjrid")
    all_job_roles_df = all_job_roles_df.fillna(0)

    all_job_roles_df = rename_column_values(all_job_roles_df, "main_job_role", MAINJRID_DICT)

    all_job_roles_df = all_job_roles_df.groupBy(
        "master_locationid",
        "providerid",
        "postal_code",
        "ons_lsoa11",
        "ons_msoa11",
        "ons_region",
        "primary_service_type",
        "main_job_role",
    ).sum()

    all_job_roles_df = (
        all_job_roles_df.withColumnRenamed("sum(estimated_jobs)", "estimated_jobs")
        .withColumnRenamed("sum(asian)", "ascwds_asian")
        .withColumnRenamed("sum(black)", "ascwds_black")
        .withColumnRenamed("sum(mixed)", "ascwds_mixed")
        .withColumnRenamed("sum(other)", "ascwds_other")
        .withColumnRenamed("sum(white)", "ascwds_white")
        .withColumnRenamed("sum(ethnicity_base)", "ascwds_base")
    )

    ascwds_by_msoa_df = (
        all_job_roles_df.select(
            "ons_msoa11",
            "main_job_role",
            "ascwds_asian",
            "ascwds_black",
            "ascwds_mixed",
            "ascwds_other",
            "ascwds_white",
            "ascwds_base",
        )
        .groupBy("ons_msoa11", "main_job_role")
        .sum()
    )

    ascwds_by_msoa_df = (
        ascwds_by_msoa_df.withColumnRenamed("sum(ascwds_asian)", "ascwds_asian_msoa")
        .withColumnRenamed("sum(ascwds_black)", "ascwds_black_msoa")
        .withColumnRenamed("sum(ascwds_mixed)", "ascwds_mixed_msoa")
        .withColumnRenamed("sum(ascwds_other)", "ascwds_other_msoa")
        .withColumnRenamed("sum(ascwds_white)", "ascwds_white_msoa")
        .withColumnRenamed("sum(ascwds_base)", "ascwds_base_msoa")
    )

    all_job_roles_df = all_job_roles_df.join(ascwds_by_msoa_df, ["ons_msoa11", "main_job_role"], "left")

    lsoa_to_msoa_df = ons_df.select("ons_lsoa11", "ons_msoa11").distinct()

    census_ethnicity_lsoa_df = get_census_ethnicity_lsoa_df(census_source)

    census_ethnicity_msoa_df = (
        lsoa_to_msoa_df.join(
            census_ethnicity_lsoa_df, lsoa_to_msoa_df.ons_lsoa11 == census_ethnicity_lsoa_df.lsoa, "left"
        )
        .drop("lsoa", "ons_lsoa11")
        .dropna()
    )

    census_ethnicity_msoa_df = census_ethnicity_msoa_df.groupBy("ons_msoa11").sum()

    census_ethnicity_msoa_df = (
        census_ethnicity_msoa_df.withColumnRenamed("sum(census_asian_lsoa)", "census_asian_msoa")
        .withColumnRenamed("sum(census_black_lsoa)", "census_black_msoa")
        .withColumnRenamed("sum(census_mixed_lsoa)", "census_mixed_msoa")
        .withColumnRenamed("sum(census_other_lsoa)", "census_other_msoa")
        .withColumnRenamed("sum(census_white_lsoa)", "census_white_msoa")
        .withColumnRenamed("sum(census_base_lsoa)", "census_base_msoa")
    )

    lsoa_to_region_df = ons_df.select("ons_lsoa11", "ons_region").distinct()

    census_ethnicity_region_df = (
        lsoa_to_region_df.join(
            census_ethnicity_lsoa_df, lsoa_to_region_df.ons_lsoa11 == census_ethnicity_lsoa_df.lsoa, "left"
        )
        .drop("lsoa", "ons_lsoa11")
        .dropna()
    )

    census_ethnicity_region_df = census_ethnicity_region_df.groupBy("ons_region").sum()

    census_ethnicity_region_df = (
        census_ethnicity_region_df.withColumnRenamed("sum(census_asian_lsoa)", "census_asian_region")
        .withColumnRenamed("sum(census_black_lsoa)", "census_black_region")
        .withColumnRenamed("sum(census_mixed_lsoa)", "census_mixed_region")
        .withColumnRenamed("sum(census_other_lsoa)", "census_other_region")
        .withColumnRenamed("sum(census_white_lsoa)", "census_white_region")
        .withColumnRenamed("sum(census_base_lsoa)", "census_base_region")
    )

    all_job_roles_df = all_job_roles_df.join(
        census_ethnicity_lsoa_df, all_job_roles_df.ons_lsoa11 == census_ethnicity_lsoa_df.lsoa, "left"
    ).drop("lsoa")

    all_job_roles_df = all_job_roles_df.join(census_ethnicity_msoa_df, ["ons_msoa11"], "left")

    all_job_roles_df = all_job_roles_df.join(census_ethnicity_region_df, ["ons_region"], "left")

    all_job_roles_df = rename_column_values(all_job_roles_df, "ons_region", REGION_DICT)

    all_job_roles_df = all_job_roles_df.withColumn("ascwds_white_%", col("ascwds_white") / col("ascwds_base"))
    all_job_roles_df = all_job_roles_df.withColumn(
        "ascwds_white_msoa_%", col("ascwds_white_msoa") / col("ascwds_base_msoa")
    )
    all_job_roles_df = all_job_roles_df.withColumn(
        "census_white_lsoa_%", col("census_white_lsoa") / col("census_base_lsoa")
    )
    all_job_roles_df = all_job_roles_df.withColumn(
        "census_white_msoa_%", col("census_white_msoa") / col("census_base_msoa")
    )
    all_job_roles_df = all_job_roles_df.withColumn(
        "census_white_region_%", col("census_white_region") / col("census_base_region")
    )

    all_job_roles_df = all_job_roles_df.withColumn("ascwds_mixed_%", col("ascwds_mixed") / col("ascwds_base"))
    all_job_roles_df = all_job_roles_df.withColumn(
        "ascwds_mixed_msoa_%", col("ascwds_mixed_msoa") / col("ascwds_base_msoa")
    )
    all_job_roles_df = all_job_roles_df.withColumn(
        "census_mixed_lsoa_%", col("census_mixed_lsoa") / col("census_base_lsoa")
    )
    all_job_roles_df = all_job_roles_df.withColumn(
        "census_mixed_msoa_%", col("census_mixed_msoa") / col("census_base_msoa")
    )
    all_job_roles_df = all_job_roles_df.withColumn(
        "census_mixed_region_%", col("census_mixed_region") / col("census_base_region")
    )

    all_job_roles_df = all_job_roles_df.withColumn("ascwds_asian_%", col("ascwds_asian") / col("ascwds_base"))
    all_job_roles_df = all_job_roles_df.withColumn(
        "ascwds_asian_msoa_%", col("ascwds_asian_msoa") / col("ascwds_base_msoa")
    )
    all_job_roles_df = all_job_roles_df.withColumn(
        "census_asian_lsoa_%", col("census_asian_lsoa") / col("census_base_lsoa")
    )
    all_job_roles_df = all_job_roles_df.withColumn(
        "census_asian_msoa_%", col("census_asian_msoa") / col("census_base_msoa")
    )
    all_job_roles_df = all_job_roles_df.withColumn(
        "census_asian_region_%", col("census_asian_region") / col("census_base_region")
    )

    all_job_roles_df = all_job_roles_df.withColumn("ascwds_black_%", col("ascwds_black") / col("ascwds_base"))
    all_job_roles_df = all_job_roles_df.withColumn(
        "ascwds_black_msoa_%", col("ascwds_black_msoa") / col("ascwds_base_msoa")
    )
    all_job_roles_df = all_job_roles_df.withColumn(
        "census_black_lsoa_%", col("census_black_lsoa") / col("census_base_lsoa")
    )
    all_job_roles_df = all_job_roles_df.withColumn(
        "census_black_msoa_%", col("census_black_msoa") / col("census_base_msoa")
    )
    all_job_roles_df = all_job_roles_df.withColumn(
        "census_black_region_%", col("census_black_region") / col("census_base_region")
    )

    all_job_roles_df = all_job_roles_df.withColumn("ascwds_other_%", col("ascwds_other") / col("ascwds_base"))
    all_job_roles_df = all_job_roles_df.withColumn(
        "ascwds_other_msoa_%", col("ascwds_other_msoa") / col("ascwds_base_msoa")
    )
    all_job_roles_df = all_job_roles_df.withColumn(
        "census_other_lsoa_%", col("census_other_lsoa") / col("census_base_lsoa")
    )
    all_job_roles_df = all_job_roles_df.withColumn(
        "census_other_msoa_%", col("census_other_msoa") / col("census_base_msoa")
    )
    all_job_roles_df = all_job_roles_df.withColumn(
        "census_other_region_%", col("census_other_region") / col("census_base_region")
    )

    all_job_roles_df = all_job_roles_df.fillna(0)

    ethnicity_white_model_df = all_job_roles_df.select(
        "master_locationid",
        "primary_service_type",
        "ons_region",
        "main_job_role",
        "estimated_jobs",
        "census_white_msoa_%",
    )

    ethnicity_white_model_df = rename_column_values(
        ethnicity_white_model_df, "primary_service_type", MAGIC_SERVICE_WHITE_DICT, "magic_service"
    )
    ethnicity_white_model_df = rename_column_values(
        ethnicity_white_model_df, "ons_region", MAGIC_REGION_WHITE_DICT, "magic_region"
    )
    ethnicity_white_model_df = rename_column_values(
        ethnicity_white_model_df, "main_job_role", MAGIC_JOBROLE_WHITE_DICT, "magic_jobrole"
    )

    # magic fromula from Will
    # 0.23 + service_model_num + jobrole_model_num + region_model_num + (0.67 * census_white_msoa_%)))
    ethnicity_white_model_df = ethnicity_white_model_df.withColumn(
        "magic_white_prediction",
        least(
            lit(1),
            greatest(
                lit(0),
                0.23
                + col("magic_service")
                + col("magic_region")
                + col("magic_jobrole")
                + (0.67 * col("census_white_msoa_%")),
            ),
        ),
    )

    ethnicity_white_model_df = ethnicity_white_model_df.withColumn(
        "estimated_jobs_white", col("estimated_jobs") * col("magic_white_prediction")
    )
    ethnicity_white_model_df = ethnicity_white_model_df.withColumn(
        "estimated_jobs_bame", col("estimated_jobs") * (1 - col("magic_white_prediction"))
    )

    ethnicity_white_model_df = ethnicity_white_model_df.drop(
        "estimated_jobs",
        "census_white_msoa_%",
        "magic_service",
        "magic_region",
        "magic_jobrole",
        "magic_white_prediction",
    )

    ethnicity_for_tableau_df = ethnicity_white_model_df.selectExpr(
        "primary_service_type",
        "ons_region",
        "main_job_role",
        "stack(2, 'White', estimated_jobs_white, 'BAME', estimated_jobs_bame) as (ethnicity, estimated_jobs)",
    )

    print(f"Exporting as parquet to {destination}")
    if destination:
        utils.write_to_parquet(ethnicity_for_tableau_df, destination)

    else:
        return ethnicity_for_tableau_df


def get_all_job_roles_per_location_df(job_roles_per_location_source):
    spark = utils.get_spark()
    # filepath = "s3a://skillsforcare/job_roles_per_location/job_roles_per_location_v2.parquet"

    print(f"Reading job role jobs per location parquet from {job_roles_per_location_source}")
    job_roles_per_location_df = spark.read.parquet(job_roles_per_location_source).select(
        col("master_locationid"), col("primary_service_type"), col("main_job_role"), col("estimate_job_role_count_2021")
    )

    job_roles_per_location_df = job_roles_per_location_df.withColumnRenamed(
        "estimate_job_role_count_2021", "estimated_jobs"
    )

    return job_roles_per_location_df


def get_cqc_locations_df(cqc_locations_prepared_source):
    spark = utils.get_spark()
    print(f"Reading CQC locations parquet from {cqc_locations_prepared_source}")
    cqc_locations_df = (
        spark.read.parquet(cqc_locations_prepared_source)
        .select(col("locationid"), col("providerid"), col("postal_code"))
        .distinct()
    )

    return cqc_locations_df


def get_ons_geography_df(ons_source):
    spark = utils.get_spark()
    print(f"Reading ONS geography parquet from {ons_source}")
    ons_df = (
        spark.read.parquet(ons_source)
        .filter(col("ctry") == "E92000001")
        .select(col("pcds"), col("lsoa11"), col("msoa11"), col("rgn"))
        .distinct()
    )

    ons_df = ons_df.withColumnRenamed("pcds", "ons_postcode")
    ons_df = ons_df.withColumnRenamed("lsoa11", "ons_lsoa11")
    ons_df = ons_df.withColumnRenamed("msoa11", "ons_msoa11")
    ons_df = ons_df.withColumnRenamed("rgn", "ons_region")

    return ons_df


def get_ascwds_ethnicity_df(worker_source):
    spark = utils.get_spark()
    print(f"Reading workers parquet from {worker_source}")
    ethnicity_df = (
        spark.read.parquet(worker_source)
        .filter(col("ethnicity") > -1)
        .filter(col("ethnicity") < 99)
        .select(col("locationid"), col("mainjrid"), col("ethnicity"))
    )

    return ethnicity_df


def get_census_ethnicity_lsoa_df(census_source):
    spark = utils.get_spark()
    print(f"Reading census csv from {census_source}")

    census_df = spark.read.csv(census_source, header=True)
    # "s3://skillsforcare/ethnicity_by_super_output_area.csv"

    census_df = census_df.withColumn("lsoa", trim(col("2011 super output area - lower layer").substr(0, 10)))
    census_df = census_df.drop(col("2011 super output area - lower layer"))

    census_df = census_df.selectExpr(
        "lsoa",
        "`Asian/Asian British` as census_asian_lsoa",
        "`Black/African/Caribbean/Black British` as census_black_lsoa",
        "`Mixed/multiple ethnic group` as census_mixed_lsoa",
        "`Other ethnic group` as census_other_lsoa",
        "`White: Total` as census_white_lsoa",
        "`All categories: Ethnic group of HRP` as census_base_lsoa",
    )

    census_df = census_df.withColumn("census_asian_lsoa", census_df.census_asian_lsoa.cast("int"))
    census_df = census_df.withColumn("census_black_lsoa", census_df.census_black_lsoa.cast("int"))
    census_df = census_df.withColumn("census_mixed_lsoa", census_df.census_mixed_lsoa.cast("int"))
    census_df = census_df.withColumn("census_other_lsoa", census_df.census_other_lsoa.cast("int"))
    census_df = census_df.withColumn("census_white_lsoa", census_df.census_white_lsoa.cast("int"))
    census_df = census_df.withColumn("census_base_lsoa", census_df.census_base_lsoa.cast("int"))

    return census_df


def get_keys_from_value(dic, val):
    # Use a dictionary item to return the associated key
    return [k for k, v in dic.items() if val in v][0]


def rename_column_values(df, var_name, dic, alias=None):
    var_udf = udf(lambda x: get_keys_from_value(dic, x), StringType())

    if alias:
        df = df.withColumn(alias, var_udf(col(var_name)))
    else:
        df = df.withColumn(var_name, var_udf(col(var_name)))

    return df


def collect_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--job_roles_per_location_source",
        help="Source s3 directory for output of job_role_breakdown",
        required=True,
    )
    parser.add_argument(
        "--cqc_locations_prepared_source",
        help="Source s3 directory for CQC locations prepared dataset",
        required=True,
    )
    parser.add_argument(
        "--ons_source",
        help="Source s3 directory for ONS postcode dataset",
        required=True,
    )
    parser.add_argument(
        "--worker_source",
        help="Source s3 directory for ASCWDS worker dataset",
        required=True,
    )
    parser.add_argument(
        "--census_source",
        help="Source s3 directory for census ethnicity data by super output area",
        required=True,
    )
    parser.add_argument(
        "--destination",
        help="A destination directory for outputting ethnicity data, if not provided shall default to S3 todays date.",
        required=True,
    )

    args, unknown = parser.parse_known_args()

    return (
        args.job_roles_per_location_source,
        args.cqc_locations_prepared_source,
        args.ons_source,
        args.worker_source,
        args.census_source,
        args.destination,
    )


if __name__ == "__main__":
    (
        job_roles_per_location_source,
        cqc_locations_prepared_source,
        ons_source,
        worker_source,
        census_source,
        destination,
    ) = collect_arguments()
    main(
        job_roles_per_location_source,
        cqc_locations_prepared_source,
        ons_source,
        worker_source,
        census_source,
        destination,
    )
