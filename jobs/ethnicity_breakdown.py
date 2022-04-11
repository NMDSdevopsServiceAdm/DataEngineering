import argparse

from pyspark.sql.functions import udf, col, trim
from pyspark.sql.types import StringType


from utils import utils
from environment import constants

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


def main(worker_source, ascwds_import_date, destination=None):
    spark = utils.get_spark()

    print("Importing ASCWDS data...")
    ascwds_ethnicity_df = get_ascwds_ethnicity_df(worker_source, ascwds_import_date)

    print("Renaming ethnicity variables...")
    ascwds_ethnicity_df = rename_column_values(ascwds_ethnicity_df, "ethnicity", ETHNICITY_DICT)

    print("Group and pivot ethnicity column...")
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
    ascwds_ethnicity_df.show()

    # all_job_roles_df = spark.read.parquet(
    #     "s3a://skillsforcare/job_roles_per_location/job_roles_per_location.parquet"
    # ).selectExpr(
    #     "master_locationid", "primary_service_type", "main_job_role", "estimate_job_role_count_2021 as estimated_jobs"
    # )

    # cqc_locations_df = (
    #     spark.table("dataset_locations_prepared")
    #     .filter(col("version") == "1.0.3")
    #     .select("locationid", "providerid", "postal_code")
    #     .distinct()
    # )

    # all_job_roles_df = all_job_roles_df.join(
    #     cqc_locations_df, all_job_roles_df.master_locationid == cqc_locations_df.locationid, "left"
    # ).drop("locationid")

    # ons_df = spark.table("domain_ons")
    # ons_df = ons_df.filter(ons_df.ctry == "E92000001").selectExpr(
    #     "pcds as ons_postcode", "lsoa11 as ons_lsoa11", "msoa11 as ons_msoa11", "rgn as ons_region"
    # )

    # lsoa_to_msoa_df = ons_df.select("ons_lsoa11", "ons_msoa11").distinct()
    # lsoa_to_region_df = ons_df.select("ons_lsoa11", "ons_region").distinct()

    # all_job_roles_df = all_job_roles_df.join(ons_df, all_job_roles_df.postal_code == ons_df.ons_postcode, "left").drop(
    #     "ons_postcode"
    # )

    # all_job_roles_df = all_job_roles_df.join(
    #     ascwds_ethnicity_df,
    #     (all_job_roles_df.master_locationid == ascwds_ethnicity_df.locationid)
    #     & (all_job_roles_df.main_job_role == ascwds_ethnicity_df.mainjrid),
    #     "left",
    # ).drop("locationid", "mainjrid")
    # all_job_roles_df = all_job_roles_df.fillna(0)

    # DONE print("Renaming job role variables...")
    # DONE all_job_roles_df = rename_column_values(all_job_roles_df, "main_job_role", MAINJRID_DICT)

    # all_job_roles_df = all_job_roles_df.groupBy(
    #     "master_locationid",
    #     "providerid",
    #     "postal_code",
    #     "ons_lsoa11",
    #     "ons_msoa11",
    #     "ons_region",
    #     "primary_service_type",
    #     "main_job_role",
    # ).sum()

    # all_job_roles_df = (
    #     all_job_roles_df.withColumnRenamed("sum(estimated_jobs)", "estimated_jobs")
    #     .withColumnRenamed("sum(asian)", "ascwds_asian")
    #     .withColumnRenamed("sum(black)", "ascwds_black")
    #     .withColumnRenamed("sum(mixed)", "ascwds_mixed")
    #     .withColumnRenamed("sum(other)", "ascwds_other")
    #     .withColumnRenamed("sum(white)", "ascwds_white")
    #     .withColumnRenamed("sum(ethnicity_base)", "ascwds_base")
    # )

    # ascwds_by_msoa_df = (
    #     all_job_roles_df.select(
    #         "ons_msoa11",
    #         "main_job_role",
    #         "ascwds_asian",
    #         "ascwds_black",
    #         "ascwds_mixed",
    #         "ascwds_other",
    #         "ascwds_white",
    #         "ascwds_base",
    #     )
    #     .groupBy("ons_msoa11", "main_job_role")
    #     .sum()
    # )

    # ascwds_by_msoa_df = (
    #     ascwds_by_msoa_df.withColumnRenamed("sum(ascwds_asian)", "ascwds_asian_msoa")
    #     .withColumnRenamed("sum(ascwds_black)", "ascwds_black_msoa")
    #     .withColumnRenamed("sum(ascwds_mixed)", "ascwds_mixed_msoa")
    #     .withColumnRenamed("sum(ascwds_other)", "ascwds_other_msoa")
    #     .withColumnRenamed("sum(ascwds_white)", "ascwds_white_msoa")
    #     .withColumnRenamed("sum(ascwds_base)", "ascwds_base_msoa")
    # )

    # all_job_roles_df = all_job_roles_df.join(ascwds_by_msoa_df, ["ons_msoa11", "main_job_role"], "left")

    # census_ethnicity_lsoa_df = spark.read.csv("s3://skillsforcare/ethnicity_by_super_output_area.csv", header=True)

    # census_ethnicity_lsoa_df = census_ethnicity_lsoa_df.withColumn(
    #     "lsoa", trim(col("2011 super output area - lower layer").substr(0, 10))
    # )

    # census_ethnicity_lsoa_df = census_ethnicity_lsoa_df.drop(col("2011 super output area - lower layer"))

    # census_ethnicity_lsoa_df = census_ethnicity_lsoa_df.selectExpr(
    #     "lsoa",
    #     "`Asian/Asian British` as census_asian_lsoa",
    #     "`Black/African/Caribbean/Black British` as census_black_lsoa",
    #     "`Mixed/multiple ethnic group` as census_mixed_lsoa",
    #     "`Other ethnic group` as census_other_lsoa",
    #     "`White: Total` as census_white_lsoa",
    #     "`All categories: Ethnic group of HRP` as census_base_lsoa",
    # )

    # census_ethnicity_lsoa_df = census_ethnicity_lsoa_df.withColumn(
    #     "census_asian_lsoa", census_ethnicity_lsoa_df.census_asian_lsoa.cast("int")
    # )
    # census_ethnicity_lsoa_df = census_ethnicity_lsoa_df.withColumn(
    #     "census_black_lsoa", census_ethnicity_lsoa_df.census_black_lsoa.cast("int")
    # )
    # census_ethnicity_lsoa_df = census_ethnicity_lsoa_df.withColumn(
    #     "census_mixed_lsoa", census_ethnicity_lsoa_df.census_mixed_lsoa.cast("int")
    # )
    # census_ethnicity_lsoa_df = census_ethnicity_lsoa_df.withColumn(
    #     "census_other_lsoa", census_ethnicity_lsoa_df.census_other_lsoa.cast("int")
    # )
    # census_ethnicity_lsoa_df = census_ethnicity_lsoa_df.withColumn(
    #     "census_white_lsoa", census_ethnicity_lsoa_df.census_white_lsoa.cast("int")
    # )
    # census_ethnicity_lsoa_df = census_ethnicity_lsoa_df.withColumn(
    #     "census_base_lsoa", census_ethnicity_lsoa_df.census_base_lsoa.cast("int")
    # )

    # census_ethnicity_msoa_df = (
    #     lsoa_to_msoa_df.join(
    #         census_ethnicity_lsoa_df, lsoa_to_msoa_df.ons_lsoa11 == census_ethnicity_lsoa_df.lsoa, "left"
    #     )
    #     .drop("lsoa", "ons_lsoa11")
    #     .dropna()
    # )

    # census_ethnicity_msoa_df = census_ethnicity_msoa_df.groupBy("ons_msoa11").sum()

    # census_ethnicity_msoa_df = (
    #     census_ethnicity_msoa_df.withColumnRenamed("sum(census_asian_lsoa)", "census_asian_msoa")
    #     .withColumnRenamed("sum(census_black_lsoa)", "census_black_msoa")
    #     .withColumnRenamed("sum(census_mixed_lsoa)", "census_mixed_msoa")
    #     .withColumnRenamed("sum(census_other_lsoa)", "census_other_msoa")
    #     .withColumnRenamed("sum(census_white_lsoa)", "census_white_msoa")
    #     .withColumnRenamed("sum(census_base_lsoa)", "census_base_msoa")
    # )

    # census_ethnicity_region_df = (
    #     lsoa_to_region_df.join(
    #         census_ethnicity_lsoa_df, lsoa_to_region_df.ons_lsoa11 == census_ethnicity_lsoa_df.lsoa, "left"
    #     )
    #     .drop("lsoa", "ons_lsoa11")
    #     .dropna()
    # )

    # census_ethnicity_region_df = census_ethnicity_region_df.groupBy("ons_region").sum()

    # census_ethnicity_region_df = (
    #     census_ethnicity_region_df.withColumnRenamed("sum(census_asian_lsoa)", "census_asian_region")
    #     .withColumnRenamed("sum(census_black_lsoa)", "census_black_region")
    #     .withColumnRenamed("sum(census_mixed_lsoa)", "census_mixed_region")
    #     .withColumnRenamed("sum(census_other_lsoa)", "census_other_region")
    #     .withColumnRenamed("sum(census_white_lsoa)", "census_white_region")
    #     .withColumnRenamed("sum(census_base_lsoa)", "census_base_region")
    # )

    # all_job_roles_df = all_job_roles_df.join(
    #     census_ethnicity_lsoa_df, all_job_roles_df.ons_lsoa11 == census_ethnicity_lsoa_df.lsoa, "left"
    # ).drop("lsoa")

    # all_job_roles_df = all_job_roles_df.join(census_ethnicity_msoa_df, ["ons_msoa11"], "left")

    # all_job_roles_df = all_job_roles_df.join(census_ethnicity_region_df, ["ons_region"], "left")

    # # not fully convinced on this one but remove rows where we estimate zero jobs
    # all_job_roles_df = all_job_roles_df.filter(all_job_roles_df.estimated_jobs > 0)

    # all_job_roles_df = all_job_roles_df.sort("master_locationid", "main_job_role")

    # all_job_roles_df = all_job_roles_df.withColumn("ascwds_white_%", col("ascwds_white") / col("ascwds_base"))
    # all_job_roles_df = all_job_roles_df.withColumn(
    #     "ascwds_white_msoa_%", col("ascwds_white_msoa") / col("ascwds_base_msoa")
    # )
    # all_job_roles_df = all_job_roles_df.withColumn(
    #     "census_white_lsoa_%", col("census_white_lsoa") / col("census_base_lsoa")
    # )
    # all_job_roles_df = all_job_roles_df.withColumn(
    #     "census_white_msoa_%", col("census_white_msoa") / col("census_base_msoa")
    # )
    # all_job_roles_df = all_job_roles_df.withColumn(
    #     "census_white_region_%", col("census_white_region") / col("census_base_region")
    # )

    # all_job_roles_df = all_job_roles_df.withColumn("ascwds_mixed_%", col("ascwds_mixed") / col("ascwds_base"))
    # all_job_roles_df = all_job_roles_df.withColumn(
    #     "ascwds_mixed_msoa_%", col("ascwds_mixed_msoa") / col("ascwds_base_msoa")
    # )
    # all_job_roles_df = all_job_roles_df.withColumn(
    #     "census_mixed_lsoa_%", col("census_mixed_lsoa") / col("census_base_lsoa")
    # )
    # all_job_roles_df = all_job_roles_df.withColumn(
    #     "census_mixed_msoa_%", col("census_mixed_msoa") / col("census_base_msoa")
    # )
    # all_job_roles_df = all_job_roles_df.withColumn(
    #     "census_mixed_region_%", col("census_mixed_region") / col("census_base_region")
    # )

    # all_job_roles_df = all_job_roles_df.withColumn("ascwds_asian_%", col("ascwds_asian") / col("ascwds_base"))
    # all_job_roles_df = all_job_roles_df.withColumn(
    #     "ascwds_asian_msoa_%", col("ascwds_asian_msoa") / col("ascwds_base_msoa")
    # )
    # all_job_roles_df = all_job_roles_df.withColumn(
    #     "census_asian_lsoa_%", col("census_asian_lsoa") / col("census_base_lsoa")
    # )
    # all_job_roles_df = all_job_roles_df.withColumn(
    #     "census_asian_msoa_%", col("census_asian_msoa") / col("census_base_msoa")
    # )
    # all_job_roles_df = all_job_roles_df.withColumn(
    #     "census_asian_region_%", col("census_asian_region") / col("census_base_region")
    # )

    # all_job_roles_df = all_job_roles_df.withColumn("ascwds_black_%", col("ascwds_black") / col("ascwds_base"))
    # all_job_roles_df = all_job_roles_df.withColumn(
    #     "ascwds_black_msoa_%", col("ascwds_black_msoa") / col("ascwds_base_msoa")
    # )
    # all_job_roles_df = all_job_roles_df.withColumn(
    #     "census_black_lsoa_%", col("census_black_lsoa") / col("census_base_lsoa")
    # )
    # all_job_roles_df = all_job_roles_df.withColumn(
    #     "census_black_msoa_%", col("census_black_msoa") / col("census_base_msoa")
    # )
    # all_job_roles_df = all_job_roles_df.withColumn(
    #     "census_black_region_%", col("census_black_region") / col("census_base_region")
    # )

    # all_job_roles_df = all_job_roles_df.withColumn("ascwds_other_%", col("ascwds_other") / col("ascwds_base"))
    # all_job_roles_df = all_job_roles_df.withColumn(
    #     "ascwds_other_msoa_%", col("ascwds_other_msoa") / col("ascwds_base_msoa")
    # )
    # all_job_roles_df = all_job_roles_df.withColumn(
    #     "census_other_lsoa_%", col("census_other_lsoa") / col("census_base_lsoa")
    # )
    # all_job_roles_df = all_job_roles_df.withColumn(
    #     "census_other_msoa_%", col("census_other_msoa") / col("census_base_msoa")
    # )
    # all_job_roles_df = all_job_roles_df.withColumn(
    #     "census_other_region_%", col("census_other_region") / col("census_base_region")
    # )

    # all_job_roles_df = all_job_roles_df.fillna(0)

    # all_job_roles_df = all_job_roles_df.withColumn("main_job_role", jobRoleUDF(col("main_job_role")))

    # ethnicity_white_model_df = all_job_roles_df.select(
    #     "master_locationid", "primary_service_type", "main_job_role", "ons_region", "census_white_msoa_%"
    # )

    print(f"Exporting as parquet to {destination}")
    if destination:
        # utils.write_to_parquet(ethnicity_white_model_df, destination)
        utils.write_to_parquet(ascwds_ethnicity_df, destination)

    else:
        # return ethnicity_white_model_df
        return ascwds_ethnicity_df


def get_ascwds_ethnicity_df(worker_source, ascwds_import_date):
    spark = utils.get_spark()
    print(f"Reading workers parquet from {worker_source}")
    ethnicity_df = (
        spark.read.parquet(worker_source)
        .filter(col("import_date") == ascwds_import_date)
        .filter(col("ethnicity") > -1)
        .filter(col("ethnicity") < 99)
        .select(col("locationid"), col("mainjrid"), col("ethnicity"))
    )

    return ethnicity_df


def get_keys_from_value(dic, val):
    # Use a dictionary item to return the associated key
    return [k for k, v in dic.items() if val in v][0]


def rename_column_values(df, var_name, dic):
    var_udf = udf(lambda x: get_keys_from_value(dic, x), StringType())

    df = df.withColumn(var_name, var_udf(col(var_name)))

    return df


def collect_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--worker_source",
        help="Source s3 directory for ASCWDS worker dataset",
        required=True,
    )
    parser.add_argument(
        "--ascwds_import_date",
        help="The import date of ASCWDS data in the format yyyymmdd.",
        required=True,
    )
    parser.add_argument(
        "--destination",
        help="A destination directory for outputting ethnicity data, if not provided shall default to S3 todays date.",
        required=True,
    )

    args, unknown = parser.parse_known_args()

    return (
        args.worker_source,
        args.ascwds_import_date,
        args.destination,
    )


if __name__ == "__main__":
    (
        worker_source,
        ascwds_import_date,
        destination,
    ) = collect_arguments()
    main(worker_source, ascwds_import_date, destination)
