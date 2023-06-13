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
        StructField("la_area_ascof", StringType(), True),
        StructField("la_area_salt", StringType(), True),
        StructField("la_area_excel", StringType(), True),
        StructField("la_area_imd", StringType(), True),
        StructField("year", IntegerType(), False),
        StructField("proportion_su_only_employing_staff", FloatType(), True),
        StructField("number_su_dpr_salt", FloatType(), True),
        StructField("number_carer_dpr_salt", FloatType(), True),
        StructField("number_su_dpr_year_end_ascof", FloatType(), True),
        StructField("number_carer_dpr_year_end_ascof", FloatType(), True),
        StructField("imd_2010", FloatType(), True),
        StructField("la_area_aws", StringType(), False),
        StructField("number_of_dprs_adass", FloatType(), False),
        StructField("number_of_dprs_who_employ_staff_adass", FloatType(), False),
    ]
)
