#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Target File: pyspark/DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/abinitio/umsatz_konsolidierung.py
PySpark application converting the legacy Ab Initio graph (umsatz_konsolidierung.mp).
"""

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

def main():
    # ---------------------------------------------------------
    # 1. Spark Session Initialization
    # ---------------------------------------------------------
    spark = SparkSession.builder \
        .appName("dwh_umsatz_konsolidierung_pyspark") \
        .getOrCreate()
    
    # ---------------------------------------------------------
    # 2. Extract Environment-Specific Global Variables
    # ---------------------------------------------------------
    GCP_PROJECT = os.environ.get("GCP_PROJECT")
    BQ_DATASET = os.environ.get("BQ_DATASET", "DWH_TARGET")
    GCS_BUCKET = os.environ.get("GCS_BUCKET")
    
    # ---------------------------------------------------------
    # 3. Parse Job-Specific Parameters
    # ---------------------------------------------------------
    if len(sys.argv) < 3:
        print("Usage: umsatz_konsolidierung.py <VERARBEITUNGSMONAT> <KONZERNGESELLSCHAFT>")
        sys.exit(1)
        
    VERARBEITUNGSMONAT = sys.argv[1]
    KONZERNGESELLSCHAFT = sys.argv[2]
    
    ERROR_OUTPUT_DIR = f"gs://{GCS_BUCKET}/opt/dwh/errors/umsatz"
    LOG_DIR = f"gs://{GCS_BUCKET}/opt/dwh/logs/umsatz"
    
    # ---------------------------------------------------------
    # 4. Read Sources from BigQuery
    # ---------------------------------------------------------
    # STG_UMSATZ_TRANSAKTIONEN Filtered by input metrics
    df_stg_umsatz_raw = spark.read.format("bigquery") \
        .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.STG_UMSATZ_TRANSAKTIONEN") \
        .load() \
        .filter(
            (F.col("VERARBEITUNGSMONAT") == VERARBEITUNGSMONAT) &
            (F.col("KONZERNGESELLSCHAFT") == KONZERNGESELLSCHAFT) &
            (F.col("ETL_STATUS") == "PENDING")
        )
        
    # Active group company dimension definitions
    df_dim_konzern_raw = spark.read.format("bigquery") \
        .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.DIM_KONZERNGESELLSCHAFT") \
        .load() \
        .filter(F.col("IS_CURRENT") == "Y")
        
    df_tarifgruppen_raw = spark.read.format("bigquery") \
        .option("table", f"{GCP_PROJECT}.{BQ_DATASET}.STG_TARIFGRUPPEN_MAPPING") \
        .load()

    # ---------------------------------------------------------
    # 5. Reformat & Normalise Transactions
    # ---------------------------------------------------------
    df_normalised = df_stg_umsatz_raw.select(
        F.col("umsatz_id"),
        F.upper(F.trim(F.col("konzerngesellschaft"))).alias("konzerngesellschaft"),
        F.trim(F.col("vertrag")).alias("vertrag"),
        F.trim(F.col("kunde")).alias("kunde"),
        F.upper(F.trim(F.col("tarifgruppen_code"))).alias("tarifgruppen_code"),
        F.col("buchungsdatum"),
        F.coalesce(F.col("waehrung"), F.lit("EUR")).alias("waehrung"),
        F.when(F.col("buchungsart").isin("STORNO", "GUTSCHRIFT"), "STORNO")
         .otherwise("REGULAER").alias("buchungsart"),
        F.round(F.col("umsatz_betrag") * 100.0, 0).cast(IntegerType()).alias("umsatz_betrag_cent")
    )

    # ---------------------------------------------------------
    # 6. Joins & Error Partitioning
    # ---------------------------------------------------------
    df_joined_konzern = df_normalised.join(
        df_dim_konzern_raw.select(F.col("konzerngesellschaft").alias("dim_konzern_id")),
        df_normalised["konzerngesellschaft"] == df_dim_konzern_raw["dim_konzern_id"],
        "left_outer"
    ).cache()
    
    # Process and log unmatched reference identifiers (DLQ Pattern)
    df_unmatched = df_joined_konzern.filter(F.col("dim_konzern_id").isNull())
    if df_unmatched.count() > 0:
        unmatched_path = f"{ERROR_OUTPUT_DIR}/umsatz_unmatched_{KONZERNGESELLSCHAFT}_{VERARBEITUNGSMONAT}.dat"
        df_unmatched.write.mode("overwrite").option("delimiter", "|").csv(unmatched_path)
        
    df_matched_konzern = df_joined_konzern.filter(F.col("dim_konzern_id").isNotNull())
    
    df_joined_complete = df_matched_konzern.join(
        df_tarifgruppen_raw,
        "tarifgruppen_code",
        "left_outer"
    )

    # ---------------------------------------------------------
    # 7. Aggregation Rollups
    # ---------------------------------------------------------
    df_regular = df_joined_complete.filter(F.col("buchungsart") == "REGULAER")
    df_stornos = df_joined_complete.filter(F.col("buchungsart") == "STORNO")

    # Regular accounting rollup
    df_rollup_regular = df_regular.groupBy(
        "konzerngesellschaft", "tarifgruppen_code", "waehrung"
    ).agg(
        F.sum("umsatz_betrag_cent").cast(IntegerType()).alias("umsatz_summe_cent"),
        F.count("umsatz_id").alias("anzahl_buchungen")
    ).withColumn("verarbeitungsmonat", F.lit(VERARBEITUNGSMONAT))

    # Storno accounting rollup
    df_rollup_stornos = df_stornos.groupBy(
        "konzerngesellschaft", "tarifgruppen_code", "waehrung"
    ).agg(
        F.sum("umsatz_betrag_cent").cast(IntegerType()).alias("storno_summe_cent")
    ).withColumn("verarbeitungsmonat", F.lit(VERARBEITUNGSMONAT))

    # ---------------------------------------------------------
    # 8. Merge & BigQuery Loading
    # ---------------------------------------------------------
    df_consolidated = df_rollup_regular.join(
        df_rollup_stornos,
        ["konzerngesellschaft", "verarbeitungsmonat", "tarifgruppen_code", "waehrung"],
        "left_outer"
    ).select(
        "konzerngesellschaft",
        "verarbeitungsmonat",
        "tarifgruppen_code",
        "waehrung",
        "umsatz_summe_cent",
        F.coalesce(F.col("storno_summe_cent"), F.lit(0)).alias("storno_summe_cent"),
        "anzahl_buchungen"
    ).cache()

    target_table = f"{GCP_PROJECT}.{BQ_DATASET}.FACT_UMSATZ_KONZERN_MONAT"
    df_consolidated.write.format("bigquery") \
        .option("table", target_table) \
        .mode("append") \
        .save()

    # ---------------------------------------------------------
    # 9. Audit Logging
    # ---------------------------------------------------------
    audit_row_count = df_consolidated.count()
    audit_log_path = f"{LOG_DIR}/umsatz_konsolidierung_audit_{KONZERNGESELLSCHAFT}_{VERARBEITUNGSMONAT}.log"
    
    audit_df = spark.createDataFrame([(audit_row_count,)], ["total_processed_records"])
    audit_df.write.mode("overwrite").json(audit_log_path)

    spark.stop()

if __name__ == "__main__":
    main()