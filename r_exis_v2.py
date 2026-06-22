# This PySpark script is a migration target for the legacy 'r_exis_v2' executable,
# part of the EXIS_SD_APT_BESTANDS job.
# The original transformation logic needs to be manually reverse-engineered
# and implemented in the 'TRANSFORMATION LOGIC' section.

import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, date_format

def main():
    if len(sys.argv) != 4:
        print("Usage: r_exis_v2.py <gcp_project_id> <bigquery_dataset> <gcs_output_bucket>")
        sys.exit(1)

    gcp_project_id = sys.argv[1]
    bigquery_dataset = sys.argv[2]
    gcs_output_bucket = sys.argv[3]

    spark = SparkSession.builder \
        .appName("EXIS_SD_APT_BESTANDS_PySpark_Export") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.23.0") \
        .getOrCreate()

    # --- INPUT CONFIGURATION ---
    # Define your BigQuery tables. These should be the migrated versions of the
    # original SOF$TA_BPR_OPTIONEN, SOF$VI_L_OPTIONZUORDNUNG, and RPT$TA_S_D1_VERTRAG.
    table_sof_ta_bpr_optionen = f"{gcp_project_id}.{bigquery_dataset}.SOF_TA_BPR_OPTIONEN"
    table_sof_vi_l_optionzuordnung = f"{gcp_project_id}.{bigquery_dataset}.SOF_VI_L_OPTIONZUORDNUNG"
    table_rpt_ta_s_d1_vertrag = f"{gcp_project_id}.{bigquery_dataset}.RPT_TA_S_D1_VERTRAG"

    # --- READ DATA FROM BIGQUERY ---
    print(f"Reading data from BigQuery table: {table_sof_ta_bpr_optionen}")
    df_bpr_optionen = spark.read.format("bigquery").option("table", table_sof_ta_bpr_optionen).load()

    print(f"Reading data from BigQuery table: {table_sof_vi_l_optionzuordnung}")
    df_optionzuordnung = spark.read.format("bigquery").option("table", table_sof_vi_l_optionzuordnung).load()

    print(f"Reading data from BigQuery table: {table_rpt_ta_s_d1_vertrag}")
    df_vertrag = spark.read.format("bigquery").option("table", table_rpt_ta_s_d1_vertrag).load()

    # --- TRANSFORMATION LOGIC (MANUAL EFFORT REQUIRED) ---
    # This section needs to be populated with the actual data transformation
    # logic derived from reverse-engineering the original 'r_exis_v2' executable
    # and its configuration file 'h_exis_apt_bestandsdaten.var'.
    # This includes:
    # 1. Joins between df_bpr_optionen, df_optionzuordnung, and df_vertrag.
    # 2. Filtering conditions.
    # 3. Column selections and renaming.
    # 4. Aggregations or any other business rules.
    # 5. Defining the exact schema of the output CSV.

    # Placeholder for transformed data.
    # For now, we will just select all columns from one table as an example.
    # REPLACE THIS WITH YOUR ACTUAL TRANSFORMATION LOGIC.
    transformed_df = df_bpr_optionen.select("*")
    print("WARNING: Placeholder transformation logic is currently implemented.")
    print("Please replace this section with the actual logic from r_exis_v2 and h_exis_apt_bestandsdaten.var.")
    # Example:
    # transformed_df = df_bpr_optionen.join(df_optionzuordnung, "common_id", "inner") \
    #                                 .join(df_vertrag, "another_common_id", "left_outer") \
    #                                 .filter(col("some_column") > 100) \
    #                                 .select("col1", "col2", "col_from_vertrag", "...")

    # --- WRITE OUTPUT TO GCS ---
    # Generate timestamp for the output filename
    current_dt = datetime.now()
    timestamp_str = current_dt.strftime("%Y%m%d%H%M%S")
    output_filename = f"DWHM_APT_BESTANDSREPORT_{timestamp_str}.csv.gz"
    gcs_output_path = f"gs://{gcs_output_bucket}/exports/{output_filename}"

    print(f"Writing transformed data to GCS: {gcs_output_path}")
    transformed_df.write \
        .mode("overwrite") \
        .option("header", "true") \
        .option("compression", "gzip") \
        .csv(gcs_output_path)

    print("PySpark job completed successfully.")
    spark.stop()

if __name__ == "__main__":
    main()