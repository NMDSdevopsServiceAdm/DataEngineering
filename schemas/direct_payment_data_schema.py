from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    FloatType,
    IntegerType,
)


EXTERNAL_DATA = StructType(
    fields=[
        StructField("number_su_dpr_salt", FloatType(), True),
        StructField("number_carer_dpr_salt", FloatType(), True),
        StructField("number_su_dpr_year_end_ascof", FloatType(), True),
        StructField("number_carer_dpr_year_end_ascof", FloatType(), True),
        StructField("imd_2010", FloatType(), True),
        StructField("la_area_aws", StringType(), False),
        StructField("number_of_dprs_adass", FloatType(), False),
        StructField("number_of_dprs_who_employ_staff_adass", FloatType(), False),
        StructField("year", IntegerType(), False),
        StructField("proportion_su_employing_staff_adass", FloatType(), True),
        StructField("prev_service_user_employing_staff_proportion", FloatType(), True),
        StructField("filled_posts_per_employer", FloatType(), True),
    ]
)

SURVEY_DATA = StructType(
    fields=[
        StructField("year", IntegerType(), True),
        StructField("total_staff_recoded", FloatType(), True),
    ]
)
