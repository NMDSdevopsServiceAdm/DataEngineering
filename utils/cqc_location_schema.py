from pyspark.sql.types import *

LOCATION_SCHEMA = StructType(fields=[
    StructField('locationId', StringType(), False),
    StructField('providerId', StringType(), False),
    StructField('organisationType', StringType(), False),
    StructField('type', StringType(), False),
    StructField('name', StringType(), False),
    StructField('onspdCcgCode', StringType(), False),
    StructField('onspdCcgName', StringType(), False),
    StructField('odsCode', StringType(), True),
    StructField('uprn', StringType(), False),
    StructField('registrationStatus', StringType(), False),
    StructField('registrationDate', StringType(), False),
    StructField('dormancy', StringType(), True),
    StructField('numberOfBeds', IntegerType(), True),
    StructField('postalAddressLine1', StringType(), True),
    StructField('postalAddressTownCity', StringType(), True),
    StructField('postalAddressCounty', StringType(), True),
    StructField('region', StringType(), True),
    StructField('postalCode', StringType(), True),
    StructField('onspdLatitude', FloatType(), True),
    StructField('onspdLongitude', FloatType(), True),
    StructField('careHome', StringType(), True),
    StructField('inspectionDirectorate', StringType(), True),
    StructField('mainPhoneNumber', StringType(), True),
    StructField('constituency', StringType(), True),
    StructField('localAuthority', StringType(), True),
    StructField('lastInspection', StringType(), True),
    StructField('lastInspection', StructType([
        StructField('date', StringType(), True)
    ]), True),
    StructField('lastReport', StructType([
        StructField(
            'publicationDate', StringType(), True)
    ]), True),
    StructField(
        'relationships', ArrayType(
            StructType([
                StructField('relatedLocationId', StringType(), False),
                StructField('relatedLocationName', StringType(), False),
                StructField('type', StringType(), False),
                StructField('reason', StringType(), False),
            ])
        ), True),
    # StructField('locationTypes', ArrayType(), False),
    StructField(
        'regulatedActivities', ArrayType(
            StructType([
                StructField('name', StringType(), False),
                StructField('code', StringType(), False),
                StructField('contacts', ArrayType(
                    StructType([
                            StructField('personTitle',
                                        StringType(), False),
                            StructField('personGivenName',
                                        StringType(), False),
                            StructField('personFamilyName',
                                        StringType(), False),
                            StructField('personRoles', StringType(), False),
                            ])
                ), True),
            ])
        )
    ),
    StructField(
        'gacServiceTypes', ArrayType(
            StructType([
                StructField('name', StringType(), False),
                StructField('description', StringType(), False),
            ])
        )
    ),
    StructField(
        'inspectionCategories', ArrayType(
            StructType([
                StructField('code', StringType(), True),
                StructField('primary', StringType(), True),
                StructField('name', StringType(), True),
            ])
        ), True),
    StructField(
        'specialisms', ArrayType(
            StructType([
                StructField('name', StringType(), False),
            ])
        ), True),
    # StructField('inspectionAreas', ArrayType(), True),
    StructField('currentRatings', StructType([
        StructField('overall', StructType([
                StructField("rating", StringType(), True),
                StructField("reportDate", StringType(), True),
                StructField("reportLinkId", StringType(), True),
                StructField(
                    'keyQuestionRatings', ArrayType(
                        StructType([
                            StructField('name', StringType(), False),
                            StructField('rating', StringType(), False),
                            StructField('reportDate', StringType(), False),
                            StructField('reportLinkId', StringType(), False),
                        ])
                    )
                ),
                StructField("reportDate", StringType(), True),
                ]), True)
    ]), True),
    StructField('historicRatings', ArrayType(
        StructType([
            StructField("organisationId", StringType(), True),
            StructField("reportLinkId", StringType(), True),
            StructField("reportDate", StringType(), True),
            StructField('overall', StructType([
                StructField("rating", StringType(), True),
                StructField(
                        'keyQuestionRatings', ArrayType(
                            StructType([
                                StructField('name', StringType(), True),
                                StructField('rating', StringType(), True),
                            ]), True), True),
            ]), True),
        ]), True)
    ),
    StructField(
        'reports', ArrayType(
            StructType([
                StructField('linkId', StringType(), True),
                StructField('reportDate', StringType(), True),
                StructField('reportUri', StringType(), True),
                StructField('firstVisitDate', StringType(), True),
                StructField('reportType', StringType(), True),
            ])
        ), True
    ),
])
