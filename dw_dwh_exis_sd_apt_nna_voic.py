# Header
# Legacy Source: vobs/dw_source/isdwh/uc4_prod_exports/.../DW.DWH_EXIS_SD_APT_NNA_VOIC.xml
# Job: DW.DWH_APT_EXPORT_MONATLICH_JP

import argparse
from datetime import datetime
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="PySpark job for APT NNA Voice export.")
    parser.add_argument("--month_id", required=True, help="Month ID in YYYYMM format.")
    parser.add_argument("--output_path", required=True, help="GCS path for output CSV.")
    args = parser.parse_args()

    logger.info(f"Starting PySpark job with month_id: {args.month_id}, output_path: {args.output_path}")

    spark = SparkSession.builder.appName("DW_DWH_EXIS_SD_APT_NNA_VOIC").getOrCreate()

    # --- Start of placeholder for actual transformation logic ---
    logger.warning("ATTENTION: This PySpark script is a placeholder.")
    logger.warning("The actual data extraction and transformation logic for DW.DWH_EXIS_SD_APT_NNA_VOIC")
    logger.warning("needs to be implemented here based on the manual analysis of 'r_exis_v2'")
    logger.warning("and 'h_exis_apt_nna_voice.var' as outlined in the design document.")
    logger.warning("Currently, this script generates dummy data.")

    # Example: create a dummy DataFrame. Replace this with actual data extraction and transformation.
    data = [
        ("voice_id_1", "channel_x", "duration_y", args.month_id),
        ("voice_id_2", "channel_z", "duration_w", args.month_id),
    ]
    columns = ["voice_id", "channel", "duration", "month_id"]
    df = spark.createDataFrame(data, columns)

    # Simulate some simple transformation if needed, based on month_id
    df = df.withColumn("processed_date", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    # --- End of placeholder ---

    # Generate filename based on convention: DWHM_APT_NNA_Voic_<yyyymmddhhmmss>.csv.gz
    current_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    output_file_name = f"DWHM_APT_NNA_Voic_{current_timestamp}.csv.gz"
    final_output_path = f"{args.output_path}/{output_file_name}"

    logger.info(f"Writing dummy data to: {final_output_path}")
    # Coalesce to 1 to ensure a single compressed CSV file.
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(final_output_path, compression="gzip")

    logger.info("PySpark job finished successfully.")
    spark.stop()

if __name__ == "__main__":
    main()