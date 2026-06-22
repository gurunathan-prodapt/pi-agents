# PySpark script for EXIS_SD_APT_NNA_DATA job, migrating UC4 UNIX export to GCP.
# Replaces: Logic within r_exis_v2 executable and h_exis_apt_nna_daten.var configuration.
# Job: EXIS_SD_APT_NNA_DATA

"""
This PySpark script is a placeholder for the re-implemented data export logic
of the legacy UC4 job EXIS_SD_APT_NNA_DATA.

It connects to a source system (details to be defined after reverse engineering
the r_exis_v2 executable), extracts telephone system master data, applies
transformations, and exports the data as a gzipped CSV file to a specified
GCS location.

Key functionalities:
- Dynamic generation of month ID (YYYYMM) and timestamp for output filename.
- Placeholder for data extraction from the source database.
- Placeholder for data transformation logic.
- Writes compressed CSV output to GCS.
"""

import argparse
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="PySpark script for EXIS_SD_APT_NNA_DATA export.")
    parser.add_argument(
        "--output_gcs_path",
        type=str,
        required=True,
        help="GCS path for the output CSV file (e.g., gs://your-bucket/exports/nna_data/).",
    )
    parser.add_argument(
        "--config_gcs_path",
        type=str,
        required=False,
        help="GCS path to configuration file (e.g., gs://your-bucket/config/h_exis_apt_nna_daten.var).",
    )
    args = parser.parse_args()

    logger.info(f"Starting PySpark job with output_gcs_path: {args.output_gcs_path}")
    if args.config_gcs_path:
        logger.info(f"Config GCS path: {args.config_gcs_path}")

    spark = (
        SparkSession.builder.appName("EXIS_SD_APT_NNA_DATA_Export")
        .getOrCreate()
    )

    try:
        # 1. Date Parameter Generation (replicate UC4 logic)
        current_datetime = datetime.now()
        # Equivalent to : set &MONAT_ID = SYS_DATE('YYYYMMDD') and SUBSTR(&MONAT_ID,1,6)
        monat_id = current_datetime.strftime("%Y%m")
        # For the full timestamp in the filename
        timestamp_for_filename = current_datetime.strftime("%Y%m%d%H%M%S")

        logger.info(f"Generated MONAT_ID: {monat_id}")
        logger.info(f"Generated timestamp for filename: {timestamp_for_filename}")

        # TODO: Implement 2. Data Extraction
        # This section needs to be re-implemented based on the source database.
        # Example: connecting to a database via JDBC.
        #
        # For demonstration, creating a dummy DataFrame.
        # In a real scenario, this would involve reading from a source system.
        logger.warning("Placeholder: Data extraction logic needs to be implemented. Using dummy data.")
        data = [
            ("tel_id_1", "number_a", "address_x", "type_1", monat_id),
            ("tel_id_2", "number_b", "address_y", "type_2", monat_id),
            ("tel_id_3", "number_c", "address_z", "type_1", monat_id),
        ]
        columns = ["telephone_id", "phone_number", "address", "phone_type", "month_id"]
        df = spark.createDataFrame(data, columns)

        # TODO: Implement 3. Data Processing & Formatting
        # This section needs to translate the transformation rules from r_exis_v2.
        # Example: Adding a derived column or filtering.
        logger.warning("Placeholder: Data transformation logic needs to be implemented.")
        transformed_df = df.withColumn("export_date", lit(current_datetime.date()))


        # 4. File Compression & Output
        output_filename = f"DWHM_APT_NNA_Daten_{timestamp_for_filename}.csv.gz"
        full_output_path = f"{args.output_gcs_path.rstrip('/')}/{output_filename}"

        logger.info(f"Writing transformed data to GCS: {full_output_path}")
        transformed_df.coalesce(1).write.csv(
            path=full_output_path,
            mode="overwrite",
            header=True,
            compression="gzip",
        )
        logger.info("PySpark job completed successfully.")

    except Exception as e:
        logger.error(f"PySpark job failed with error: {e}", exc_info=True)
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()