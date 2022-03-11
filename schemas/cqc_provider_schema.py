from pyspark.sql.types import StructField, StructType, StringType, ArrayType, FloatType

PROVIDER_SCHEMA = StructType(
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
