import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Dynamic sourcing of environment variables based on GCP infrastructure policies
GCP_PROJECT = os.environ.get("GCP_PROJECT")
BQ_DATASET = os.environ.get("BQ_DATASET")
GCS_BUCKET = os.environ.get("GCS_BUCKET")
TEMP_GCS_BUCKET = os.environ.get("TEMP_GCS_BUCKET", GCS_BUCKET)

# Job-specific variables defined as a runtime config block
JOB_CONFIG = {
    "BHB_Projektverzeichnis": "/Projects/TMD/processing/BHB/BD_PROC",
    "BHB_Version": "RLS_BHB_nach_64_rabatt_sap",
    "BHB_Graph": "map_rpos_carmen_import",
    "BHB_Prozesstyp": "D",
    "BHB_Dateimaske": "CARMEN_B_*_pos.fix",
    "BHB_Kopfdatensatzkennung": "H",
    "BHB_Nutzdatensatzkennung": "P",
    "BHB_Endedatensatzkennung": "X"
}

spark = SparkSession.builder.appName("map_rpos_carmen_import").getOrCreate()

def write_to_bq(df, table_name, mode="append"):
    df.write.format("bigquery") \
        .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.{table_name}") \
        .option("temporaryGcsBucket", f"{TEMP_GCS_BUCKET}/tmp") \
        .mode(mode) \
        .save()

# Read raw invoice file from GCS using dynamic bucket and masks
try:
    df_raw_file = spark.read.format("csv") \
        .option("header", "false") \
        .option("delimiter", ";") \
        .load(f"gs://{GCS_BUCKET}/crs/work/{JOB_CONFIG['BHB_Dateimaske']}")
    df_raw_file.createOrReplaceTempView("vw_raw_file")
except Exception as e:
    print(f"WARNING: raw_invoice_input_file not found or empty — skipping: {e}")
    from pyspark.sql.types import StructType, StructField, StringType
    schema_raw = StructType([StructField(f"_c{i}", StringType(), True) for i in range(25)])
    df_raw_file = spark.createDataFrame([], schema_raw)
    df_raw_file.createOrReplaceTempView("vw_raw_file")

df_raw_file.cache()
df_raw_file.count()

# Read BigQuery Sources
df_ta_c_vertrag = spark.read.format("bigquery") \
    .option(