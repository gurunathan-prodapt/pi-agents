#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PySpark replacement for legacy Ab Initio GDE graph map_rpos_carmen_import.mp.
Ingests, enriches, validates, and routes Carmen RPOS data to BigQuery targets.
"""

import os
import sys
import re
import datetime
import logging
from google.cloud import bigquery
from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, when, lit, regexp_replace, to_date, last_day, current_timestamp

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ENVIRONMENT-SPECIFIC VALUES (Global & Job-Specific per migration policy)
GCP_PROJECT = os.environ.get("GCP_PROJECT")
BQ_DATASET = os.environ.get("BQ_DATASET")
GCS_BUCKET = os.environ.get("GCS_BUCKET")

# Job Specific parameters
BHB_PROJEKTVERZEICHNIS = os.environ.get("BHB_Projektverzeichnis", "/Projects/TMD/processing/BHB/BD_PROC")
BHB_VERSION = os.environ.get("BHB_Version", "RLS_BHB_nach_64_rabatt_sap")
BHB_GRAPH = os.environ.get("BHB_Graph", "map_rpos_carmen_import")
BHB_PROZESSTYP = os.environ.get("BHB_Prozesstyp", "D")
BHB_QUELLVERZEICHNIS = os.environ.get("BHB_Quellverzeichnis", "crs/work/")
BHB_ZIELVERZEICHNIS = os.environ.get("BHB_Zielverzeichnis", "crs/store/")
BHB_DATEIMASKE = os.environ.get("BHB_Dateimaske", "CARMEN_B_*_pos.fix")
BHB_KOPFDATENSATZKENNUNG = os.environ.get("BHB_Kopfdatensatzkennung", "H")
BHB_NUTZDATENSATZKENNUNG = os.environ.get("BHB_Nutzdatensatzkennung", "P")
BHB_ENDEDATENSATZKENNUNG = os.environ.get("BHB_Endedatensatzkennung", "X")

BHB_DATEINAME = os.environ.get("BHB_Dateiname")
BHB_EINTRAGSNR = os.environ.get("BHB_Eintragsnr")

# Check for critical environment values
if not all([GCP_PROJECT, BQ_DATASET, GCS_BUCKET]):
    logging.error("Initialization Failed: Missing global environment configurations (GCP_PROJECT, BQ_DATASET, GCS_BUCKET).")
    sys.exit(1)

def get_bq_client():
    return bigquery.Client(project=GCP_PROJECT)

def get_gcs_client():
    return storage.Client(project=GCP_PROJECT)

def get_matching_file_from_gcs():
    """Finds first file in GCS matching the mask if no explicit filename is provided."""
    storage_client = get_gcs_client()
    bucket = storage_client.bucket(GCS_BUCKET)
    
    # List files in source folder
    blobs = bucket.list_blobs(prefix=BHB_QUELLVERZEICHNIS)
    
    # Compile regex from mask (e.g., CARMEN_B_*_pos.fix)
    regex_pattern = BHB_DATEIMASKE.replace(".", "\\.").replace("*", ".*").replace("?", ".")
    r = re.compile(regex_pattern)
    
    for blob in blobs:
        filename = os.path.basename(blob.name)
        if r.search(filename):
            logging.info(f"Found matching file in GCS: gs://{GCS_BUCKET}/{blob.name}")
            return f"gs://{GCS_BUCKET}/{blob.name}"
            
    return None

def execute_pre_load_deletes(bq_client, staging_table):
    """
    Executes target table clean-up deletions before inserting new data
    to maintain target ledger integrity and avoid duplicates on rerun.
    """
    targets = {
        "DWH$TA_F_GPOS_FACT_CARM": ["rechnung_id", "rechnung_datum", "standardvertrags_id", "vertrags_id"],
        "DWH$TA_F_RPOS_CARM": ["rechnung_id", "rechnung_datum", "standardvertrags_id", "vertrags_id"],
        "DWH$TA_F_RPOS_FACT_CARM": ["rechnung_id", "rechnung_datum", "standardvertrags_id", "vertrags_id"],
        "DWH$TA_F_RPOS_RESELLING_CARM": ["rechnung_id", "rechnung_datum", "standardvertrags_id", "vertrags_id"],
        "DWH$TA_T_RPOS_CARM": ["debitor_id", "rechnung_datum", "rechnung_id"]
    }
    
    for target_table, keys in targets.items():
        target_full_id = f"{GCP_PROJECT}.{BQ_DATASET}.{target_table}"
        logging.info(f"Enforcing clean-up deletions on target: {target_full_id}")
        
        conditions = []
        for k in keys:
            if k == "rechnung_datum":
                conditions.append("DATE(T.rechnung_datum) = PARSE_DATE('%Y%m%d', S.rechnung_datum)")
            else:
                conditions.append(f"T.{k} = S.{k}")
        join_conditions = " AND ".join(conditions)
        
        query = f"""
        MERGE `{target_full_id}` T
        USING `{staging_table}` S
        ON {join_conditions}
        WHEN MATCHED THEN DELETE
        """
        try:
            query_job = bq_client.query(query)
            query_job.result()
            logging.info(f"Successfully deleted matching records in target table: {target_table}")
        except Exception as e:
            logging.error(f"Failed pre-load deletes on target {target_table}: {e}")
            raise e

def audit_reconciliation_logs(bq_client, parsed_count, file_name, end_record_info):
    """Updates audit log and job metric tables."""
    logging.info("Registering job execution status in audit ledgers...")
    
    reconciliation_table = f"{GCP_PROJECT}.{BQ_DATASET}.DWH$TA_K_RECH_ABSGRP"
    meldungen_table = f"{GCP_PROJECT}.{BQ_DATASET}.DWH$TA_K_MELDUNGEN"
    
    monats_id = end_record_info.get("monats_id", datetime.datetime.now().strftime("%Y%m"))
    abs_grp = end_record_info.get("abs_grp", "BHB_G")
    rechnung_datum_str = end_record_info.get("rechnung_datum", datetime.datetime.now().strftime("%Y%m%d"))
    rechnungsteil = "P"
    ladedatum_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        rechnung_datum = datetime.datetime.strptime(rechnung_datum_str, "%Y%m%d").strftime("%Y-%m-%d")
    except Exception:
        rechnung_datum = datetime.datetime.now().strftime("%Y-%m-%d")
        
    merge_reconcile = f"""
    MERGE `{reconciliation_table}` T
    USING (SELECT '{monats_id}' as monats_id, '{abs_grp}' as abs_grp, '{file_name}' as dateiname) S
    ON T.monats_id = S.monats_id AND T.abs_grp = S.abs_grp AND T.dateiname = S.dateiname
    WHEN MATCHED THEN
      UPDATE SET rechnung_datum = DATE('{rechnung_datum}'), ladedatum = TIMESTAMP('{ladedatum_str}')
    WHEN NOT MATCHED THEN
      INSERT (monats_id, abs_grp, dateiname, rechnung_datum, rechnungsteil, ladedatum)
      VALUES ('{monats_id}', '{abs_grp}', '{file_name}', DATE('{rechnung_datum}'), '{rechnungsteil}', TIMESTAMP('{ladedatum_str}'))
    """
    
    eintragsnr = BHB_EINTRAGSNR or "1"
    
    update_meldungen = f"""
    UPDATE `{meldungen_table}`
    SET anzahl_ds_eof = {parsed_count},
        dateiname = '{file_name}',
        enderecord_text = '{end_record_info.get("inhalt", "")}',
        zusatzinfo = 'Processed via PySpark Map RPOS Carmen Pipeline'
    WHERE entrynr = {eintragsnr}
    """
    
    try:
        bq_client.query(merge_reconcile).result()
        logging.info("DWH$TA_K_RECH_ABSGRP log updated successfully.")
        
        bq_client.query(update_meldungen).result()
        logging.info("DWH$TA_K_MELDUNGEN log updated successfully.")
    except Exception as e:
        logging.warning(f"Non-critical audit logging failed: {e}")

def archive_processed_file(file_path):
    """Archives the processed file in GCS."""
    logging.info(f"Archiving raw ingestion file: {file_path}")
    gcs_client = get_gcs_client()
    
    match = re.match(r"gs://([^/]+)/(.*)", file_path)
    if not match:
        return
    
    bucket_name, blob_name = match.groups()
    bucket = gcs_client.bucket(bucket_name)
    source_blob = bucket.blob(blob_name)
    
    archive_blob_name = blob_name.replace(BHB_QUELLVERZEICHNIS, BHB_ZIELVERZEICHNIS)
    
    try:
        bucket.copy_blob(source_blob, bucket, archive_blob_name)
        source_blob.delete()
        logging.info(f"Successfully archived source file to gs://{bucket_name}/{archive_blob_name}")
    except Exception as e:
        logging.error(f"Failed to archive processed file: {e}")

def main():
    # Spark Session initialization
    spark = SparkSession.builder \
        .appName("map_rpos_carmen_import") \
        .getOrCreate()
        
    # Input file determination
    target_file = BHB_DATEINAME
    if not target_file:
        target_file = get_matching_file_from_gcs()
        
    if not target_file:
        logging.info("No matching source files found in GCS. Exiting successfully.")
        spark.stop()
        sys.exit(0)
        
    if not target_file.startswith("gs://"):
        target_file = f"gs://{GCS_BUCKET}/{BHB_QUELLVERZEICHNIS.lstrip('/')}{target_file}"
        
    logging.info(f"Processing source file: {target_file}")
    
    # Read entire text file to extract end record and process data
    raw_df = spark.read.text(target_file)
    
    # 1. Parse end record (X) for metadata logging
    end_record_info = {}
    end_record_row = raw_df.filter(col("value").startswith(BHB_ENDEDATENSATZKENNUNG)).first()
    if end_record_row:
        line_val = end_record_row["value"]
        parts = line_val.split(";")
        if len(parts) >= 5:
            stichtag = parts[2].strip()
            bemerkung = parts[1].strip()
            try:
                dt = datetime.datetime.strptime(stichtag, "%Y%m%d")
                first_day = dt.replace(day=1)
                prev_month_dt = first_day - datetime.timedelta(days=1)
                monats_id = prev_month_dt.strftime("%Y%m")
            except Exception:
                monats_id = datetime.datetime.now().strftime("%Y%m")
                
            end_record_info = {
                "monats_id": monats_id,
                "abs_grp": bemerkung[9:14] if len(bemerkung) >= 14 else "BHB_G",
                "rechnung_datum": stichtag,
                "inhalt": parts[4].strip()
            }
            
    # 2. Parse payload records (P)
    data_df = raw_df.filter(col("value").startswith(BHB_NUTZDATENSATZKENNUNG))
    split_col = split(col("value"), ";")
    
    parsed_df = data_df.select(
        split_col.getItem(1).alias("monats_id"),
        split_col.getItem(2).alias("debitor_id"),
        when(split_col.getItem(3) == "", lit("#")).otherwise(split_col.getItem(3)).alias("kontier_grp_id"),
        split_col.getItem(4).alias("rechnung_id"),
        split_col.getItem(5).alias("rechnung_datum"),
        when((split_col.getItem(6) == "") | (split_col.getItem(6) == "#"), lit(0.0)).otherwise(split_col.getItem(6).cast("double")).alias("standardvertrags_id"),
        when((split_col.getItem(7) == "") | (split_col.getItem(7) == "#"), lit(0.0)).otherwise(split_col.getItem(7).cast("double")).alias("vertrags_id"),
        split_col.getItem(8).alias("rech_leistung_id_carm"),
        regexp_replace(split_col.getItem(9), ",", ".").cast("double").alias("rechpos_brutto_eur"),
        regexp_replace(split_col.getItem(10), ",", ".").cast("double").alias("rechpos_netto_eur"),
        regexp_replace(split_col.getItem(11), ",", ".").cast("double").alias("rechpos_mwst_eur"),
        when(split_col.getItem(12) == "", lit("#")).otherwise(split_col.getItem(12)).alias("abs_grp"),
        when(split_col.getItem(13) == "", lit("#")).otherwise(split_col.getItem(13)).alias("pooling"),
        when(split_col.getItem(14) == "", lit(0.0)).otherwise(split_col.getItem(14).cast("double")).alias("rechnungvertrag_id"),
        when(split_col.getItem(15) == "", lit("#")).otherwise(split_col.getItem(15)).alias("prob_vertrag_id"),
        when(split_col.getItem(16) == "", lit("#")).otherwise(split_col.getItem(16)).alias("prob_provider_kenn"),
        when(split_col.getItem(17) == "", lit(0.0)).otherwise(split_col.getItem(17).cast("double")).alias("anz_leistungen"),
        when(split_col.getItem(18) == "", lit(0.0)).otherwise(split_col.getItem(18).cast("double")).alias("anz_tickets"),
        when(split_col.getItem(19) == "", lit("#")).otherwise(split_col.getItem(19)).alias("rpos_geschaftsform_kenn"),
        when(split_col.getItem(20) == "", lit("#")).otherwise(split_col.getItem(20)).alias("vas_kenn"),
        when(split_col.getItem(21) == "", lit(0.0)).otherwise(split_col.getItem(21).cast("double")).alias("verkauftes_basisprodukt_id")
    )
    
    # Record count check
    total_records = parsed_df.count()
    if total_records == 0:
        logging.info("No nutzdaten records to process. Exiting successfully.")
        spark.stop()
        sys.exit(0)
        
    # Write parsed records to BigQuery Staging
    staging_table_name = f"DWH_TA_STAGE_RPOS_CARM"
    staging_table_full = f"{GCP_PROJECT}.{BQ_DATASET}.{staging_table_name}"
    
    logging.info(f"Writing parsed records to staging table: {staging_table_full}")
    parsed_df.write.format("bigquery") \
        .option("table", staging_table_full) \
        .option("temporaryGcsBucket", GCS_BUCKET) \
        .mode("overwrite") \
        .save()
        
    # Execute pre-load deletions using BQ Python Client
    bq_client = get_bq_client()
    execute_pre_load_deletes(bq_client, staging_table_full)
    
    # Load contract master dataset from BigQuery for lookup joins
    contract_df = spark.read.format("bigquery") \
        .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.DWH$TA_C_VERTRAG") \
        .load()
        
    filtered_contract = contract_df.filter(col("gueltig_bis") >= "2005-04-01")
    
    # Calculate month last day
    parsed_with_date_df = parsed_df.withColumn(
        "month_last_day",
        last_day(to_date(col("monats_id"), "yyyyMM"))
    )
    
    # Join with contract master
    join_cond = (parsed_with_date_df["vertrags_id"] == filtered_contract["vertrag_id_carmen"]) & \
                ((filtered_contract["gueltig_von"].isNull()) | (parsed_with_date_df["month_last_day"] > filtered_contract["gueltig_von"])) & \
                ((filtered_contract["gueltig_bis"].isNull()) | (parsed_with_date_df["month_last_day"] <= filtered_contract["gueltig_bis"]))
                
    enriched_df = parsed_with_date_df.join(filtered_contract, join_cond, "left")
    
    # Decode logic
    enriched_df = enriched_df.withColumn(
        "decoded_geschaftsform",
        when((col("rpos_geschaftsform_kenn") == "F") & (col("vas_kenn") == "P30002"), lit("G"))
        .otherwise(col("rpos_geschaftsform_kenn"))
    ).withColumn(
        "rahmenvertrag",
        col("rahmenvertrag_id")
    ).withColumn(
        "ladedatum",
        current_timestamp()
    )
    
    # Drop intermediate columns to avoid target schema issues
    enriched_df = enriched_df.drop("month_last_day", "vertrag_id_carmen", "gueltig_von", "gueltig_bis")
    
    # Route and write outputs
    routes = {
        "DWH$TA_F_RPOS_FACT_CARM": enriched_df.filter(col("decoded_geschaftsform") == "F").drop("decoded_geschaftsform"),
        "DWH$TA_F_GPOS_FACT_CARM": enriched_df.filter(col("decoded_geschaftsform") == "G").drop("decoded_geschaftsform"),
        "DWH$TA_F_RPOS_RESELLING_CARM": enriched_df.filter(col("decoded_geschaftsform") == "R").drop("decoded_geschaftsform"),
        "DWH$TA_F_RPOS_CARM": enriched_df.drop("decoded_geschaftsform")
    }
    
    for table_name, df in routes.items():
        df_count = df.count()
        logging.info(f"Target {table_name}: {df_count} records to load.")
        if df_count > 0:
            target_full_id = f"{GCP_PROJECT}.{BQ_DATASET}.{table_name}"
            df.write.format("bigquery") \
                .option("table", target_full_id) \
                .option("temporaryGcsBucket", GCS_BUCKET) \
                .mode("append") \
                .save()
                
    # Route for DWH$TA_T_RPOS_CARM (temporary data)
    temp_data_df = enriched_df.filter(
        ((col("rech_leistung_id_carm") == "RABATT") & (col("vertrags_id") == 0)) |
        (col("pooling") == "P")
    ).select(
        col("debitor_id"),
        col("rechnung_id"),
        col("rechnung_datum"),
        col("standardvertrags_id"),
        col("rech_leistung_id_carm"),
        col("vertrags_id"),
        lit("1900-01-01 00:00:00").cast("timestamp").alias("bearbeitung_datum")
    )
    
    temp_count = temp_data_df.count()
    logging.info(f"Target DWH$TA_T_RPOS_CARM: {temp_count} records to load.")
    if temp_count > 0:
        temp_target_full_id = f"{GCP_PROJECT}.{BQ_DATASET}.DWH$TA_T_RPOS_CARM"
        temp_data_df.write.format("bigquery") \
            .option("table", temp_target_full_id) \
            .option("temporaryGcsBucket", GCS_BUCKET) \
            .mode("append") \
            .save()
            
    # Audit log registration
    audit_reconciliation_logs(bq_client, total_records, os.path.basename(target_file), end_record_info)
    
    # Cleanup GCS processed file by archiving it
    archive_processed_file(target_file)
    
    # Delete temporary staging table
    bq_client.delete_table(staging_table_full, not_found_ok=True)
    
    logging.info("PySpark pipeline execution completed successfully.")
    spark.stop()

if __name__ == "__main__":
    main()