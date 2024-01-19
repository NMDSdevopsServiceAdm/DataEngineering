import sys
import argparse

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType

from utils import utils, cqc_care_directory_dictionaries
from utils.ind_cqc_column_names.cqc_care_directory_columns import (
    CqcCareDirectoryColumns as CareDirCols,
)
from utils.ind_cqc_column_names.cqc_provider_api_columns import (
    CqcProviderApiColumns as ProviderApiCols,
)
from utils.ind_cqc_column_names.cqc_location_api_columns import (
    CqcLocationApiColumns as LocationApiCols,
)
from schemas import cqc_location_schema, cqc_provider_schema


def main(source, provider_destination, location_destination):
    df = get_cqc_care_directory(source)

    provider_df = convert_to_cqc_provider_api_format(df)
    export_parquet_files(provider_df, provider_destination)

    location_df = convert_to_cqc_location_api_format(df)
    export_parquet_files(location_df, location_destination)


def get_cqc_care_directory(source):
    print(f"Reading CSV from {source}")
    df = utils.read_csv(source)

    print("Formatting date fields")
    df = utils.format_date_fields(df, raw_date_format="dd/MM/yyyy")

    df = df.filter(df[CareDirCols.type] == "Social Care Org")
    df = df.withColumnRenamed(
        CareDirCols.Provider.provider_id, ProviderApiCols.provider_id
    ).withColumnRenamed(CareDirCols.location_id, LocationApiCols.location_id)

    return df


def convert_to_cqc_provider_api_format(df):
    spark = SparkSession.builder.appName("test_ingest_cqc_care_directory").getOrCreate()

    print("Create CQC provider parquet file")
    provider_df = unique_providerids_with_array_of_their_locationids(df)
    distinct_provider_info_df = get_distinct_provider_info(df)
    provider_df = provider_df.join(
        distinct_provider_info_df, ProviderApiCols.provider_id
    )

    output_provider_df = spark.createDataFrame(
        data=[], schema=cqc_provider_schema.PROVIDER_SCHEMA
    )
    output_provider_df = output_provider_df.unionByName(
        provider_df, allowMissingColumns=True
    )

    return output_provider_df


def unique_providerids_with_array_of_their_locationids(df):
    locations_at_prov_df = df.select(
        ProviderApiCols.provider_id, LocationApiCols.location_id
    )
    locations_at_prov_df = (
        locations_at_prov_df.groupby(ProviderApiCols.provider_id)
        .agg(F.collect_set(LocationApiCols.location_id))
        .withColumnRenamed("collect_set(locationId)", ProviderApiCols.location_ids)
    )

    return locations_at_prov_df


def get_distinct_provider_info(df):
    prov_info_df = df.select(
        df[ProviderApiCols.provider_id],
        df[CareDirCols.Provider.name].alias(ProviderApiCols.name),
        df[CareDirCols.Provider.phone_number].alias(ProviderApiCols.phone_number),
        df[CareDirCols.Provider.address_line_one].alias(
            ProviderApiCols.address_line_one
        ),
        df[CareDirCols.Provider.town_or_city].alias(ProviderApiCols.town_or_city),
        df[CareDirCols.Provider.county].alias(ProviderApiCols.county),
        df[CareDirCols.Provider.postcode].alias(ProviderApiCols.postcode),
    ).distinct()

    prov_info_df = prov_info_df.withColumn(
        ProviderApiCols.organisation_type, F.lit("Provider").cast(StringType())
    )
    prov_info_df = prov_info_df.withColumn(
        ProviderApiCols.registration_status, F.lit("Registered").cast(StringType())
    )

    return prov_info_df


def convert_to_cqc_location_api_format(df):
    spark = SparkSession.builder.appName("test_ingest_cqc_care_directory").getOrCreate()

    print("Create CQC location parquet file")
    location_df = get_general_location_info(df)

    reg_man_df = create_contacts_from_registered_manager_name(df)

    regulated_activities_df = convert_multiple_boolean_columns_into_single_array(
        df,
        cqc_care_directory_dictionaries.REGULATED_ACTIVITIES_DICT,
        LocationApiCols.regulated_activities,
    )
    regulated_activities_df = regulated_activities_df.join(
        reg_man_df, LocationApiCols.location_id
    )
    regulated_activities_df = convert_regulated_activities_to_struct(
        regulated_activities_df
    )

    gac_service_types_df = convert_multiple_boolean_columns_into_single_array(
        df,
        cqc_care_directory_dictionaries.GAC_SERVICE_TYPES_DICT,
        LocationApiCols.gac_service_types,
    )
    gac_service_types_df = convert_gac_service_types_to_struct(gac_service_types_df)

    specialisms_df = convert_multiple_boolean_columns_into_single_array(
        df,
        cqc_care_directory_dictionaries.SPECIALISMS_DICT,
        LocationApiCols.specialisms,
    )
    specialisms_df = convert_specialisms_to_struct(specialisms_df)

    location_df = location_df.join(regulated_activities_df, LocationApiCols.location_id)
    location_df = location_df.join(gac_service_types_df, LocationApiCols.location_id)
    location_df = location_df.join(specialisms_df, LocationApiCols.location_id)

    output_location_df = spark.createDataFrame(
        data=[], schema=cqc_location_schema.LOCATION_SCHEMA
    )
    output_location_df = output_location_df.unionByName(
        location_df, allowMissingColumns=True
    )

    return output_location_df


def get_general_location_info(df):
    loc_info_df = df.select(
        df[LocationApiCols.location_id],
        df[LocationApiCols.provider_id],
        df[CareDirCols.type],
        df[CareDirCols.name],
        df[CareDirCols.registration_date].alias(LocationApiCols.registration_date),
        df[CareDirCols.number_of_beds].alias(LocationApiCols.number_of_beds),
        df[CareDirCols.website],
        df[CareDirCols.address_line_one].alias(LocationApiCols.address_line_one),
        df[CareDirCols.town_or_city].alias(LocationApiCols.town_or_city),
        df[CareDirCols.county].alias(LocationApiCols.county),
        df[CareDirCols.region],
        df[CareDirCols.postcode].alias(LocationApiCols.postcode),
        df[CareDirCols.care_home].alias(LocationApiCols.care_home),
        df[CareDirCols.phone_number].alias(LocationApiCols.phone_number),
        df[CareDirCols.local_authority].alias(LocationApiCols.local_authority),
    ).distinct()

    loc_info_df = loc_info_df.withColumn(
        LocationApiCols.number_of_beds, F.col("numberOfBeds").cast(IntegerType())
    )
    loc_info_df = loc_info_df.withColumn(
        LocationApiCols.organisation_type, F.lit("Location")
    )
    loc_info_df = loc_info_df.withColumn(
        LocationApiCols.registration_status, F.lit("Registered")
    )

    return loc_info_df


def convert_multiple_boolean_columns_into_single_array(df, value_mapping_dict, alias):
    column_names = [LocationApiCols.location_id]
    column_names.extend(list(value_mapping_dict.keys()))

    df = df.select(*column_names)

    for new_name, column_name in value_mapping_dict.items():
        df = df.replace("Y", column_name, new_name)
        df = df.withColumn(new_name, F.split(F.col(new_name), ",").alias(new_name))

    df = df.select(
        F.col(LocationApiCols.location_id), F.array(df.columns[1:]).alias(alias)
    )

    df = df.withColumn(alias, F.expr("filter(" + alias + ", elem -> elem is not null)"))

    return df


def convert_specialisms_to_struct(df):
    df = df.withColumn(
        LocationApiCols.specialisms,
        F.expr("transform(specialisms, x-> named_struct('name',x[0]))"),
    )

    return df


def convert_gac_service_types_to_struct(df):
    df = df.withColumn(
        LocationApiCols.gac_service_types,
        F.expr(
            "transform(gacservicetypes, x-> named_struct('name',x[0], 'description',x[1]))"
        ),
    )

    return df


def create_contacts_from_registered_manager_name(df):
    df = df.select(LocationApiCols.location_id, CareDirCols.registered_manager_name)
    df = df.replace("*", None)

    df = df.withColumn(
        LocationApiCols.title,
        F.when(F.length(F.col(CareDirCols.registered_manager_name)) > 1, "M").otherwise(
            F.lit(None)
        ),
    )
    df = df.withColumn(
        LocationApiCols.given_name,
        F.split(F.col(CareDirCols.registered_manager_name), ", ").getItem(1),
    )
    df = df.withColumn(
        LocationApiCols.family_name,
        F.split(F.col(CareDirCols.registered_manager_name), ",").getItem(0),
    )
    df = df.withColumn(
        LocationApiCols.roles,
        F.when(
            F.length(F.col(CareDirCols.registered_manager_name)) > 1,
            "Registered Manager",
        ).otherwise(F.lit(None)),
    )

    df = df.select(
        LocationApiCols.location_id,
        F.struct(
            LocationApiCols.title,
            LocationApiCols.given_name,
            LocationApiCols.family_name,
            LocationApiCols.roles,
        ).alias(LocationApiCols.contacts),
    )

    df = df.select(
        LocationApiCols.location_id,
        F.array(LocationApiCols.contacts).alias(LocationApiCols.contacts),
    )

    return df


def convert_regulated_activities_to_struct(df):
    df = df.select(
        LocationApiCols.location_id,
        LocationApiCols.contacts,
        F.explode(F.col(LocationApiCols.regulated_activities)).alias(
            LocationApiCols.regulated_activities
        ),
    )

    df = df.withColumn(
        LocationApiCols.name, F.col(LocationApiCols.regulated_activities).getItem(0)
    )
    df = df.withColumn(
        LocationApiCols.code, F.col(LocationApiCols.regulated_activities).getItem(1)
    )

    df = df.select(
        LocationApiCols.location_id,
        F.struct(
            LocationApiCols.name, LocationApiCols.code, LocationApiCols.contacts
        ).alias(LocationApiCols.regulated_activities),
    )

    df = df.groupBy(LocationApiCols.location_id).agg(
        F.collect_set(LocationApiCols.regulated_activities).alias(
            LocationApiCols.regulated_activities
        )
    )

    return df


def export_parquet_files(df, destination):
    print(f"Exporting as CQC provider parquet to {destination}")
    utils.write_to_parquet(df, destination)


def collect_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source", help="A CSV file used as source input", required=True
    )
    parser.add_argument(
        "--provider_destination",
        help="A destination directory for outputting CQC Provider parquet file",
        required=True,
    )
    parser.add_argument(
        "--location_destination",
        help="A destination directory for outputting CQC Location parquet file",
        required=True,
    )

    args, _ = parser.parse_known_args()

    return args.source, args.provider_destination, args.location_destination


if __name__ == "__main__":
    print("Spark job 'ingest_cqc_care_directory' starting...")
    print(f"Job parameters: {sys.argv}")

    source, provider_destination, location_destination = collect_arguments()
    main(source, provider_destination, location_destination)

    print("Spark job 'ingest_cqc_care_directory' complete")
