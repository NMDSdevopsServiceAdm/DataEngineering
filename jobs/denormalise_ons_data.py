import sys

import pyspark.sql.functions as F
from pyspark.sql.utils import AnalysisException


from utils import utils
from utils.column_names.raw_data_files.ons_columns import (
    OnsPostcodeDirectoryColumns as ColNames,
)


def main(ons_source, lookup_source, destination):
    spark = utils.get_spark()
    ons_data = get_previously_unimported_data(spark, ons_source, destination)
    field_replacement_col_info = [
        # field_name, coded_column_name, named_column_name
        (ColNames.rgn, "RGN20CD", "RGN20NM"),
        (ColNames.nhser, "NHSER19CD", "NHSER19NM"),
        (ColNames.ccg, "ccg21cd", "ccg21nm"),
        (ColNames.ctry, "ctry12cd", "ctry12nm"),
        (ColNames.imd, "lsoa11cd", "lsoa11nm"),
        (ColNames.lsoa11, "lsoa11cd", "lsoa11nm"),
        (ColNames.msoa11, "msoa11cd", "msoa11nm"),
        (ColNames.oslaua, "lad21cd", "lad21nm"),
        (ColNames.ru11ind, "RU11IND", "RU11NM"),
        (ColNames.stp, "stp21cd", "stp21nm"),
    ]
    for col_info in field_replacement_col_info:
        ons_data = replace_field_from_lookup(
            spark, lookup_source, ons_data, col_info[0], col_info[1], col_info[2]
        )

    ons_data = ons_data.withColumn(
        ColNames.lsoa, F.struct(ons_data.lsoa11.alias("year_2011"))
    )
    ons_data = ons_data.withColumn(
        ColNames.msoa, F.struct(ons_data.msoa11.alias("year_2011"))
    )
    ons_data = ons_data.withColumn(
        ColNames.ru_ind, F.struct(ons_data.ru11ind.alias("year_2011"))
    )

    ons_data.write.mode("append").partitionBy(
        ColNames.year, ColNames.month, ColNames.day, ColNames.import_date
    ).parquet(destination)


def replace_field_from_lookup(
    spark, lookup_source, ons_data, field_name, coded_column_name, named_column_name
):
    lookup = spark.read.parquet(f"{lookup_source}field={field_name}/")
    if coded_column_name == "RU11IND":
        lookup = lookup.withColumnRenamed("RU11IND", "RU11INDCD")
        coded_column_name = "RU11INDCD"
    lookup = lookup.alias(field_name)

    ons_data_with_lookup = ons_data.join(
        lookup,
        (ons_data[field_name] == lookup[coded_column_name])
        & (ons_data.import_date == lookup.import_date),
        "leftouter",
    )
    ons_data_with_lookup = ons_data_with_lookup.withColumn(
        field_name,
        F.when(
            lookup[named_column_name].isNotNull(), lookup[named_column_name]
        ).otherwise(None),
    )
    ons_data_with_lookup = remove_joined_columns(
        ons_data_with_lookup, lookup.columns, field_name
    )
    return ons_data_with_lookup


def remove_joined_columns(data, columns, alias):
    for col in columns:
        data = data.drop(F.col(f"{alias}.{col}"))
    return data


def get_previously_unimported_data(spark, source, destination):
    ons_data = spark.read.parquet(source)
    previously_imported_dates = get_previous_import_dates(spark, destination)

    if previously_imported_dates:
        ons_data = ons_data.join(
            previously_imported_dates,
            ons_data.import_date == previously_imported_dates.already_imported_date,
            "leftouter",
        ).where(previously_imported_dates.already_imported_date.isNull())
    return ons_data


def get_previous_import_dates(spark, destination):
    try:
        df = spark.read.parquet(destination)
    except AnalysisException:
        return None

    return df.select(
        F.col(ColNames.import_date).alias("already_imported_date")
    ).distinct()


if __name__ == "__main__":
    print("Spark job 'denormalise_ons_data' starting...")
    print(f"Job parameters: {sys.argv}")

    ons_source, ons_lookup_source, ons_destination = utils.collect_arguments(
        ("--ons_source", "S3 path to the ONS data"),
        ("--lookup_source", "S3 path to the ONS lookup"),
        ("--destination", "S3 path to save output data"),
    )
    main(ons_source, ons_lookup_source, ons_destination)
