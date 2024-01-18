from pyspark.sql.types import StructField, StructType, StringType, ArrayType, FloatType

from utils.ind_cqc_column_names.cqc_provider_api_columns import (
    CqcProviderApiColumns as ColNames,
)

PROVIDER_SCHEMA = StructType(
    fields=[
        StructField(ColNames.provider_id, StringType(), True),
        StructField(
            ColNames.location_ids,
            ArrayType(
                StringType(),
            ),
        ),
        StructField(ColNames.organisation_type, StringType(), True),
        StructField(ColNames.ownership_type, StringType(), True),
        StructField(ColNames.type, StringType(), True),
        StructField(ColNames.uprn, StringType(), True),
        StructField(ColNames.name, StringType(), True),
        StructField(ColNames.registration_status, StringType(), True),
        StructField(ColNames.registration_date, StringType(), True),
        StructField(ColNames.deregistration_date, StringType(), True),
        StructField(ColNames.address_line_one, StringType(), True),
        StructField(ColNames.town_or_city, StringType(), True),
        StructField(ColNames.county, StringType(), True),
        StructField(ColNames.region, StringType(), True),
        StructField(ColNames.postcode, StringType(), True),
        StructField(ColNames.latitude, FloatType(), True),
        StructField(ColNames.longitude, FloatType(), True),
        StructField(ColNames.phone_number, StringType(), True),
        StructField(ColNames.companies_house_number, StringType(), True),
        StructField(ColNames.inspection_directorate, StringType(), True),
        StructField(ColNames.constituency, StringType(), True),
        StructField(ColNames.local_authority, StringType(), True),
    ]
)

PROVIDER_SCHEMA_OLD = StructType(
    fields=[
        StructField("providerId", StringType(), True),
        StructField(
            "locationIds",
            ArrayType(
                StringType(),
            ),
        ),
        StructField("organisationType", StringType(), True),
        StructField("ownershipType", StringType(), True),
        StructField("type", StringType(), True),
        StructField("uprn", StringType(), True),
        StructField("name", StringType(), True),
        StructField("registrationStatus", StringType(), True),
        StructField("registrationDate", StringType(), True),
        StructField("deregistrationDate", StringType(), True),
        StructField("postalAddressLine1", StringType(), True),
        StructField("postalAddressTownCity", StringType(), True),
        StructField("postalAddressCounty", StringType(), True),
        StructField("region", StringType(), True),
        StructField("postalCode", StringType(), True),
        StructField("onspdLatitude", FloatType(), True),
        StructField("onspdLongitude", FloatType(), True),
        StructField("mainPhoneNumber", StringType(), True),
        StructField("companiesHouseNumber", StringType(), True),
        StructField("inspectionDirectorate", StringType(), True),
        StructField("constituency", StringType(), True),
        StructField("localAuthority", StringType(), True),
    ]
)
