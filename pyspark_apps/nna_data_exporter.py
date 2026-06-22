# Replaces legacy source: vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/RELEASEWECHSEL/RELEASEWECHSEL172/INSTALL_NACH_172_APT_DWHM/DW.DWH_APT_EXPORT_MONATLICH_JP/DW.DWH_EXIS_SD_APT_NNA_DATA.xml
# Job: DW.DWH_APT_EXPORT_MONATLICH_JP

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import argparse
import logging
from datetime import datetime

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="PySpark application for exporting NNA Data.")
    parser.add_argument("--monat_id", type=str, help="Monthly ID parameter (e.g., YYYYMMDD).")
    parser.add_argument("--output_gcs_bucket", type=str, help="GCS bucket for output files (e.g., gs://your-gcs-export-bucket).")
    args = parser.parse_args()

    monat_id = args.monat_id
    output_gcs_bucket = args.output_gcs_bucket

    if not monat_id:
        monat_id = datetime.now().strftime("%Y%m%d") # Fallback to current date if not provided
        logger.warning(f"monat_id not provided, using current date: {monat_id}")

    if not output_gcs_bucket:
        logger.error("Output GCS bucket not provided. Exiting.")
        exit(1)

    spark = SparkSession.builder \
        .appName(f"NNA_Data_Exporter_{monat_id}") \
        .getOrCreate()

    logger.info(f"PySpark application started for NNA Data export. MONAT_ID: {monat_id}")
    logger.info(f"Output GCS Bucket: {output_gcs_bucket}")

    # --- Oracle Database Connection Details ---
    # These details should be securely managed, e.g., via Dataproc's secure property configuration,
    # or by accessing Secret Manager from a custom Spark connector.
    # Placeholder values. REPLACE with actual, secure credentials and connection string.
    JDBC_URL = "jdbc:oracle:thin:@//your-oracle-host:1521/your-oracle-sid"
    JDBC_USER = "your_oracle_user"
    JDBC_PASSWORD = "your_oracle_password" 
    
    # The actual SQL query needs to be reverse-engineered from 'r_exis_v2' and 'h_exis_apt_nna_daten.var'.
    # The design document mentions 'd_exis_apt_nna_daten.sql' as a potential source.
    # Example placeholder SQL. REPLACE with actual query logic.
    ORACLE_TABLE_OR_QUERY = f"""
        (SELECT
            -- List all columns required for export
            APT_NNA_DATA_COL1 AS column_one,
            APT_NNA_DATA_COL2 AS column_two,
            -- Add more columns as per original logic
            '{monat_id}' AS monat_id_export,
            CURRENT_TIMESTAMP AS extract_timestamp
        FROM your_oracle_schema.EXIS_APT_NNA_DATA_SOURCE_TABLE
        WHERE DATA_MONTH_ID = '{monat_id}' -- Example filtering by MONAT_ID
        ) nna_data_export_query
    """

    try:
        # Read data from Oracle
        logger.info(f"Attempting to read data from Oracle using query derived from MONAT_ID: {monat_id}")
        df = spark.read \
            .format("jdbc") \
            .option("url", JDBC_URL) \
            .option("dbtable", ORACLE_TABLE_OR_QUERY) \
            .option("user", JDBC_USER) \
            .option("password", JDBC_PASSWORD) \
            .option("driver", "oracle.jdbc.driver.OracleDriver") \
            .load()

        logger.info(f"Successfully read {df.count()} records from Oracle.")

        # --- Transformation Logic ---
        # The design document states: "Any in-script transformations or logic within
        # r_exis_v2 will be converted to PySpark DataFrames or Dataflow transforms."
        # This is a placeholder for actual transformations.
        # Example: Ensure column names are clean for CSV export.
        df_transformed = df.select([col.alias(col.lower().replace(" ", "_")) for col in df.columns])
        
        # --- Write to GCS as compressed CSV ---
        # Filename convention: DWHM_APT_NNA_Daten_<yyyymmddhhmmss>.csv.gz
        timestamp_str = datetime.now().strftime("%Y%m%d%H%M%S")
        output_filename_prefix = f"DWHM_APT_NNA_Daten_{timestamp_str}"
        output_path = f"{output_gcs_bucket}/exports/{output_filename_prefix}"

        logger.info(f"Writing transformed data to GCS path: {output_path}")

        # Spark writes in parts. To get a single file, repartition to 1 or coalesce to 1.
        # Coalescing is generally preferred to repartition if data fits on one executor to avoid shuffle.
        df_transformed.coalesce(1).write \
            .format("csv") \
            .option("header", "true") \
            .option("compression", "gzip") \
            .save(output_path)
            
        logger.info(f"Data successfully exported to GCS: {output_path}")

    except Exception as e:
        logger.error(f"Error during NNA Data export: {e}", exc_info=True)
        raise

    finally:
        spark.stop()
        logger.info("PySpark application finished.")

if __name__ == "__main__":
    main()