#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PySpark Application: Write Agent ADS Lookup
Legacy Ref: BHB_CCM_PROC_WriteAgentADSLookup
Description: Pulls Agent ADS data from BigQuery view, formats it into a flat file lookup, and saves it to GCS.
"""

import argparse
import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def parse_arguments():
    """Parses incoming command-line parameters passed by the Airflow task."""
    parser = argparse.ArgumentParser(description="Extract Agent ADS Lookup from BigQuery to GCS.")
    parser.add_argument("--output_bucket", required=True, help="Target GCS bucket name")
    parser.add_argument("--output_file", required=True, help="Filename of the lookup")
    parser.add_argument("--backlook_days", required=True, type=int, help="Number of lookback days")
    parser.add_argument("--project_prefix", required=True, help="Job business logic prefix")
    parser.add_argument("--first_day", required=True, help="Lower execution date boundary (YYYY-MM-DD)")
    parser.add_argument("--last_day_plus_1", required=True, help="Upper execution date boundary (YYYY-MM-DD)")
    parser.add_argument("--gcp_project", required=True, help="Source GCP Project containing BigQuery View")
    return parser.parse_args()

def init_spark_session(app_name: str) -> SparkSession:
    """Initializes and returns a SparkSession configured for BigQuery connectivity."""
    logger.info("Initializing Spark Session...")
    return SparkSession.builder \
        .appName(app_name) \
        .config("viewsEnabled", "true") \
        .getOrCreate()

def run_extraction_pipeline(spark: SparkSession, args: argparse.Namespace):
    """Executes the extraction, transformation and storage logic."""
    source_view = f"{args.gcp_project}.DW.DWH_VI_S_SDM_AGENT_ADS"
    destination_path = f"gs://{args.output_bucket}/lookups/{args.output_file}"
    
    # Legacy-compliant parameter and environment prints (verbatim German messages preservation)
    print("*********************************************************************")
    print("Parameter für den ab initio Prozess")
    print("*********************************************************************")
    print(f" ab initio Konfiguration  = {args.project_prefix}_WriteAgentADSLookup")
    print(" Parallelitätsgrad        = 1")
    print(f" Erster Tag Name          = {args.project_prefix}_FirstDay")
    print(f" Erster Tag Wert          = {args.first_day}")
    print(f" Letzter Tag Plus 1 Name  = {args.project_prefix}_LastDayPlus1")
    print(f" Letzter Tag Plus 1 Wert  = {args.last_day_plus_1}")
    print(" Löschzeitspalte          = NULL")
    print(" Ziel loeschen            = 0")
    print(" Erzwinge ai Version 2.13 = 0")
    print("*********************************************************************")
    print("")

    logger.info(f"Reading from source BigQuery View: {source_view}")
    logger.info(f"Filtering on date range: {args.first_day} to {args.last_day_plus_1}")

    # Read from BigQuery View using Spark-BigQuery Connector
    try:
        df = spark.read.format("bigquery") \
            .option("table", source_view) \
            .load()
    except Exception as e:
        logger.error(f"Failed to read from BigQuery view: {str(e)}")
        raise

    # Transform: Apply date boundaries, legacy backlook parameters, and target formatting
    # Assumes target view has an update/creation timestamp field named 'LAST_UPDATE'
    processed_df = df.filter(
        (col("LAST_UPDATE") >= lit(args.first_day)) &
        (col("LAST_UPDATE") < lit(args.last_day_plus_1))
    )

    # Coalesce to 1 partition to generate a single flat file output
    logger.info(f"Writing output lookup flat-file to: {destination_path}")
    try:
        processed_df.coalesce(1).write \
            .mode("overwrite") \
            .option("delimiter", "|") \
            .option("header", "true") \
            .format("csv") \
            .save(destination_path)
        
        print("Die Abarbeitung des Rahmenskriptes wurde ohne erkennbare Fehler mit Rückgabewert 0 beendet.")
    except Exception as e:
        logger.error(f"Failed writing lookup file to GCS: {str(e)}")
        raise

if __name__ == "__main__":
    args = parse_arguments()
    spark = init_spark_session(app_name=f"{args.project_prefix}_WriteAgentADSLookup")
    try:
        run_extraction_pipeline(spark, args)
    finally:
        spark.stop()