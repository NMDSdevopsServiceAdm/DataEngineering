from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    FloatType,
    IntegerType,
)

SPSS_JOBS_ESTIMATES = StructType(
    fields=[
        StructField("LOCATIONID", StringType(), True),
        StructField("Main_Service_Group", IntegerType(), False),
        StructField("Care_homes_beds", FloatType(), False),
        StructField("WEIGHTING_CSSR", IntegerType(), False),
        StructField("WEIGHTING_REGION", IntegerType(), False),
        StructField("totalstaff", FloatType(), False),
        StructField("wkrrecs", FloatType(), False),
        StructField("jr28work", FloatType(), False),
        StructField("Data_Used", IntegerType(), True),
        StructField("All_jobs", FloatType(), True),
        StructField("Snapshot_date", StringType(), True),
        StructField("Local_authority", StringType(), False),
        StructField("Region", StringType(), False),
        StructField("Main_service", StringType(), False),
        StructField("Data_used_string", StringType(), False),
    ]
)
