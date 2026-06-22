"""
Legacy Source: r_exis_v2 binary executed by UC4 job EXIS_SD_APT_BESTANDS
Job: EXIS_SD_APT_BESTANDS

This PySpark script re-implements the functionality of the legacy r_exis_v2 binary.
It reads data from specified BigQuery tables (migrated from Oracle),
applies necessary transformations, and exports the result as a gzipped CSV
to a specified GCS location.
"""

import argparse
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
# Import any other necessary Spark SQL functions or types

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def parse_arguments():
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(
        description="PySpark script to export stock data from BigQuery to GCS as gzipped CSV."
    )
    parser.add_argument(
        "--output-path",
        type=str,
        required=True,
        help="GCS path where the output gzipped CSV file will be written (e.g., gs://your-bucket/path/to/output.csv.gz).",
    )
    parser.add_argument(
        "--config-file",
        type=str,
        required=True,
        help="GCS path to the configuration file (e.g., gs://your-bucket/apt/cfg/h_exis_apt_bestandsdaten.var).",
    )
    parser.add_argument(
        "--bq-project",
        type=str,
        required=True,
        help="Google Cloud Project ID where BigQuery tables are located.",
    )
    parser.add_argument(
        "--bq-dataset",
        type=str,
        required=True,
        help="BigQuery dataset where the source Oracle tables are landed (e.g., raw_oracle_data).",
    )
    return parser.parse_args()

def read_config_file(spark, config_file_path):
    """
    Reads and parses the configuration file.
    TODO: Implement actual parsing logic based on the format of h_exis_apt_bestandsdaten.var.
    For now, it's a placeholder function.
    """
    logger.info(f"Attempting to read configuration from: {config_file_path}")
    try:
        # Example: if it's a simple key-value file
        # with open(config_file_path, 'r') as f:
        #     config_content = f.read()
        #     # Parse config_content
        #     config_data = {} # populate this dict
        # return config_data

        # If the file is on GCS, you might need to use Spark's utility to read it
        # or gcsfs library if running locally/on a driver with GCS access.
        # For simplicity in Dataproc, Spark can read small text files directly.
        config_rdd = spark.sparkContext.textFile(config_file_path)
        config_lines = config_rdd.collect()
        config_dict = {}
        for line in config_lines:
            if '=' in line:
                key, value = line.split('=', 1)
                config_dict[key.strip()] = value.strip()
        logger.info(f"Configuration file {config_file_path} read. Sample: {list(config_dict.keys())[:5]}")
        return config_dict
    except Exception as e:
        logger.error(f"Failed to read or parse config file {config_file_path}: {e}")
        # Depending on criticality, you might want to re-raise or return empty config
        return {}


def main():
    args = parse_arguments()

    spark = (
        SparkSession.builder.appName("EXIS_SD_APT_BESTANDS_PySpark_Export")
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.28.0")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )

    spark.conf.set("parentProject", args.bq_project) # For BigQuery connector

    logger.info(f"Starting PySpark job for EXIS_SD_APT_BESTANDS at {current_timestamp()}")
    logger.info(f"Output path: {args.output_path}")
    logger.info(f"Config file: {args.config_file}")
    logger.info(f"BigQuery Project: {args.bq_project}, Dataset: {args.bq_dataset}")

    # Read and parse configuration file
    config_parameters = read_config_file(spark, args.config_file)
    logger.info(f"Loaded config parameters: {config_parameters}")

    # Define BigQuery table full paths
    bq_table_options = f"{args.bq_project}.{args.bq_dataset}.SOF_TA_BPR_OPTIONEN"
    bq_table_option_zuordnung = f"{args.bq_project}.{args.bq_dataset}.SOF_VI_L_OPTIONZUORDNUNG"
    bq_table_vertrag = f"{args.bq_project}.{args.bq_dataset}.RPT_TA_S_D1_VERTRAG"

    try:
        # Read data from BigQuery tables
        logger.info(f"Reading from BigQuery table: {bq_table_options}")
        df_options = spark.read.format("bigquery").option("table", bq_table_options).load()
        logger.info(f"Reading from BigQuery table: {bq_table_option_zuordnung}")
        df_option_zuordnung = spark.read.format("bigquery").option("table", bq_table_option_zuordnung).load()
        logger.info(f"Reading from BigQuery table: {bq_table_vertrag}")
        df_vertrag = spark.read.format("bigquery").option("table", bq_table_vertrag).load()

        # TODO: Implement the transformation logic here.
        # This section needs to replicate the exact business logic of the original
        # 'r_exis_v2' binary, including joins, filters, aggregations, and column selections.
        # Use df_options, df_option_zuordnung, and df_vertrag as input DataFrames.
        # The 'config_parameters' dictionary might contain values that influence this logic.

        # Example placeholder transformation:
        # This is a MINIMAL placeholder. REPLACE with actual logic!
        logger.warning("Placeholder transformation logic is being used. REPLACE with actual business logic.")
        transformed_df = df_options.alias("t1").join(
            df_option_zuordnung.alias("t2"),
            df_options["some_id"] == df_option_zuordnung["another_id"], # Replace with actual join condition
            "inner"
        ).join(
            df_vertrag.alias("t3"),
            df_option_zuordnung["key"] == df_vertrag["foreign_key"], # Replace with actual join condition
            "inner"
        ).select(
            df_options["column1"].alias("OutputColumn1"),
            df_option_zuordnung["column2"].alias("OutputColumn2"),
            df_vertrag["column3"].alias("OutputColumn3"),
            lit("some_static_value").alias("AdditionalColumn"), # Example of adding a new column
            current_timestamp().alias("processing_timestamp")
        ).distinct() # Example: remove duplicates

        # Ensure the schema and data types match the expected CSV output.
        # Perform necessary type casting if required.

        logger.info(f"Transformed DataFrame schema: {transformed_df.printSchema()}")
        logger.info(f"Transformed DataFrame count: {transformed_df.count()}")

        # Write the transformed data to GCS as a gzipped CSV
        logger.info(f"Writing transformed data to GCS: {args.output_path}")
        transformed_df.write.format("csv").option("header", "true").option("compression", "gzip").save(args.output_path)
        logger.info("Successfully exported data to GCS.")

    except Exception as e:
        logger.error(f"An error occurred during PySpark execution: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()