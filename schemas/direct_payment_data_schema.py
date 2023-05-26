from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    FloatType,
    IntegerType,
)


DIRECT_PAYMENTS_DATA = StructType(
    fields=[
        StructField("la_area_adass", StringType(), True),
        StructField("la_area_ascof", IntegerType(), False),
        StructField("la_area_salt", FloatType(), False),
        StructField("la_area_excel", IntegerType(), False),
        StructField("la_area_imd", IntegerType(), False),
        StructField("year", FloatType(), False),
        StructField("proportion_su_employing_staff_adass", FloatType(), False),
        StructField("number_su_dpr_salt", FloatType(), False),
        StructField("number_carer_dpr_salt", IntegerType(), True),
        StructField("number_su_dpr_year_end_ascof", FloatType(), True),
        StructField("number_carer_dpr_year_end_ascof", StringType(), True),
        StructField("imd_2010", StringType(), False),
        StructField("la_area_aws", StringType(), False),
    ]
)
