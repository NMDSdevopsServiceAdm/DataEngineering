from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    FloatType,
    IntegerType,
)


CAPACITY_TRACKER_CARE_HOMES = StructType(
    fields=[
        StructField("Local_Authority", StringType(), True),
        StructField("Location", StringType(), False),
        StructField("Parent_Organisation", StringType(), False),
        StructField("Lrf", StringType(), False),
        StructField("LocalAuthority", StringType(), False),
        StructField("Region", StringType(), False),
        StructField("ICB", StringType(), False),
        StructField("Sub_ICB", StringType(), False),
        StructField("CQC_ID", StringType(), True),
        StructField("ODS_Code", StringType(), True),
        StructField("Covid_Residents_Total", IntegerType(), True),
        StructField("Is_Accepting_Admissions", StringType(), False),
        StructField("Nurses_Employed", FloatType(), False),
        StructField("Nurses_Absent_General", FloatType(), False),
        StructField("Nurses_Absent_Covid", FloatType(), False),
        StructField("Care_Workers_Employed", FloatType(), False),
        StructField("Care_Workers_Absent_General", FloatType(), False),
        StructField("Care_Workers_Absent_Covid", FloatType(), False),
        StructField("Non_Care_Workers_Employed", FloatType(), False),
        StructField("Non_Care_Workers_Absent_General", FloatType(), False),
        StructField("Non_Care_Workers_Absent_Covid", FloatType(), False),
        StructField("Agency_Nurses_Employed", FloatType(), False),
        StructField("Agency_Care_Workers_Employed", FloatType(), False),
        StructField("Agency_Non_Care_Workers_Employed", FloatType(), False),
        StructField("Hours_Paid", IntegerType(), True),
        StructField("Hours_Overtime", IntegerType(), True),
        StructField("Hours_Agency", IntegerType(), True),
        StructField("Hours_Absense", IntegerType(), True),
        StructField("Days_Absense", IntegerType(), True),
        StructField("Last_Updated_UTC", StringType(), False),
        StructField("Last_Updated_BST", StringType(), False),
    ]
)
