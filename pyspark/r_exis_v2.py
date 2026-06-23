# This PySpark script replaces the core logic of the legacy `r_exis_v2` shell script.
# Job: EXIS_SD_APT_NNA_DATA

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, date_format, current_timestamp, regexp_replace, concat, to_timestamp
import logging
import sys
import os
import argparse

spark = SparkSession.builder.appName("EXIS_SD_APT_NNA_DATA").getOrCreate()
spark.sparkContext.setLogLevel("INFO")

logger = logging.getLogger("r_exis_v2")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)

# Argument parsing for better parameter handling from Airflow
parser = argparse.ArgumentParser(description="PySpark script for EXIS_SD_APT_NNA_DATA export.")
parser.add_argument("--job-kennung", type=str, default="EXIS_SD_APT_NNA_DATA",
                    help="Job identifier.")
parser.add_argument("--config-path", type=str, default="",
                    help="GCS path to the configuration file (e.g., h_exis_apt_nna_daten.var equivalent).")
parser.add_argument("--source-path", type=str, default="",
                    help="GCS path to the source data. (To be implemented based on actual source system).")
parser.add_argument("--output-path", type=str, default="gs://YOUR_BUCKET_NAME/output/",
                    help="GCS path for output data.")
parser.add_argument("--execution-ts", type=str, default=datetime.now().strftime("%Y-%m-%d"),
                    help="Execution timestamp for deriving MONAT_ID (format YYYY-MM-DD).")

args = parser.parse_args()

job_kennung = args.job_kennung
config_path = args.config_path
source_path = args.source_path
output_path = args.output_path
execution_ts = args.execution_ts # Expecting YYYY-MM-DD from Airflow

logger.info(f"Starting job: {job_kennung}")

# Derive MONAT_ID from the provided execution_ts.
# Assuming execution_ts is in 'YYYY-MM-DD' format from Airflow's ds.
exec_datetime_obj = datetime.strptime(execution_ts, "%Y-%m-%d")
monat_id = exec_datetime_obj.strftime("%Y%m")
output_ts = datetime.now().strftime("%Y%m%d%H%M%S") # Use current time for unique filename

logger.info(f"Derived MONAT_ID: {monat_id}")
logger.info(f"Output timestamp (for filename): {output_ts}")

config_df = None
if config_path:
    # TODO: Implement actual parsing of h_exis_apt_nna_daten.var equivalent.
    # This might be a simple key-value file, CSV, or JSON.
    # For now, assuming it's a simple CSV for demonstration if it exists.
    try:
        config_df = spark.read.option("header", "true").csv(config_path)
        logger.info(f"Loaded configuration from {config_path}. Content: {config_df.collect()}")
    except Exception as e:
        logger.warning(f"Could not load configuration from {config_path}: {e}")
        config_df = None # Ensure config_df is None if loading fails

if source_path:
    # TODO: Implement actual data extraction logic from the "telephone system master data" source.
    # This is a placeholder. The actual source system (DB, API, etc.) needs to be identified.
    # Example: If source is CSV
    try:
        source_df = spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)
        logger.info(f"Loaded source data from {source_path}. Row count: {source_df.count()}")
    except Exception as e:
        logger.error(f"Failed to load source data from {source_path}: {e}")
        sys.exit(1)
else:
    # If no source_path is provided, create an empty DataFrame.
    # This should be replaced with actual data loading.
    logger.warning("No source_path provided. Creating an empty DataFrame as placeholder for 'telephone system master data'.")
    source_df = spark.createDataFrame([], schema="id STRING, data STRING") # Example schema

# --- Transformation Logic Placeholder ---
# Replicate the core logic of `r_exis_v2` here.
# This section needs to be filled with the actual data extraction, transformation,
# and formatting steps for the telephone system master data.
# For demonstration, we'll add a simple transformation and the MONAT_ID.
transformed_df = source_df.withColumn("processed_data", concat(col("data"), lit("_processed"))) if "data" in source_df.columns else source_df
transformed_df = transformed_df.withColumn("MONAT_ID", lit(monat_id))

# If config_df was loaded, you might want to join or use its values here
# For now, a placeholder demonstrating use:
if config_df is not None and "some_config_value" in config_df.columns:
    first_config_row = config_df.limit(1).collect()
    if first_config_row:
        config_value = first_config_row[0]["some_config_value"]
        logger.info(f"Using config value: {config_value}")
        transformed_df = transformed_df.withColumn("config_setting", lit(config_value))
# --- End Transformation Logic Placeholder ---

final_df = transformed_df

# Define final output path and filename
final_output_filename = f"DWHM_APT_NNA_Daten_{output_ts}.csv.gz"
full_output_gcs_path = os.path.join(output_path, final_output_filename)

logger.info(f"Writing final output to GCS path: {full_output_gcs_path}")

# Write the DataFrame to GCS as a compressed CSV.
# Coalesce to 1 partition to get a single output file, then rename it.
# Note: PySpark writes a directory, not a single file directly with .gz.
# The GCS connector renames the part file to the desired name.
# It's typical to write to a temp folder and then move/rename.
temp_output_dir = os.path.join(output_path, f"temp_{output_ts}")
final_df.coalesce(1).write.mode("overwrite").option("header", "true").option("compression", "gzip").csv(temp_output_dir)

# After writing, we need to handle renaming if a specific filename is required by downstream systems.
# GCS doesn't directly support renaming `part-00000.csv.gz` to the desired name `DWHM_APT_NNA_Daten_...`.
# A common pattern is to move/copy the single part file to the desired name.
# This often requires `gsutil mv` or a similar Cloud Storage client operation, which is outside PySpark's direct scope.
# For simplicity and to match common PySpark output, the file will be in a directory, e.g.,
# gs://YOUR_BUCKET_NAME/output/temp_YYYYMMDDHHMMSS/part-00000-xxxx.csv.gz
# If a specific single file name is mandatory, an Airflow task (e.g., GCSObjectRenameOperator)
# after this Dataproc job would be needed to rename/move the file.
# For now, we log the expected final file name and assume the downstream system can pick it up from the directory or
# that a subsequent Airflow task will handle the exact naming.
logger.info(f"Output written to: {temp_output_dir}/part-00000-*.csv.gz. Expected logical filename: {final_output_filename}")


logger.info(f"Job completed successfully: {job_kennung}")
spark.stop()