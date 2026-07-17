#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Migrated from: abinitio/umsatz_konsolidierung.mp
Target: PySpark running on Dataproc Serverless (BigQuery)

Zweck: Monatliche Konsolidierung der Umsatzdaten (UMSATZ) ueber alle
       Konzerngesellschaften der DWH_KERN-Domaene.
"""

import sys
import argparse
import os
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, trim, upper, when, round, lit, sum as _sum, count

def init_spark_session(project_id: str, dataset_id: str) -> SparkSession:
    """Initializes and returns a configured Spark session."""
    return SparkSession.builder \
        .appName("dwh_umsatz_konsolidierung") \
        .config("viewsEnabled", "true") \
        .config("materializationProject", project_id) \
        .config("materializationDataset", dataset_id) \
        .getOrCreate()

def parse_arguments() -> argparse.Namespace:
    """Parses and returns pipeline command line arguments."""
    parser = argparse.ArgumentParser(description="Umsatz Konsolidierung PySpark Job")
    parser.add_argument("--verarbeitungsmonat", required=True, help="Processing month (YYYYMM)")
    parser.add_argument("--konzerngesellschaft", required=True, help="Group Company identifier")
    parser.add_argument("--gcp_project", required=False, help="Target GCP Project (Global)")
    parser.add_argument("--bq_dataset", required=False, help="Target BigQuery Dataset (Global)")
    parser.add_argument("--gcs_bucket", required=False, help="Target GCS Bucket (Global)")
    return parser.parse_args()

def extract_source_data(spark: SparkSession, project_id: str, dataset_id: str, args: argparse.Namespace) -> Dict[str, DataFrame]:
    """Reads source datasets from BigQuery and returns a dictionary of DataFrames."""
    df_stg_umsatz = spark.read.format("bigquery") \
        .option("table", f"{project_id}.{dataset_id}.STG_UMSATZ_TRANSAKTIONEN") \
        .load() \
        .filter(
            (col("VERARBEITUNGSMONAT") == args.verarbeitungsmonat) &
            (col("KONZERNGESELLSCHAFT") == args.konzerngesellschaft) &
            (col("ETL_STATUS") == 'PENDING')
        )

    df_dim_konzern = spark.read.format("bigquery") \
        .option("table", f"{project_id}.{dataset_id}.DIM_KONZERNGESELLSCHAFT") \
        .load() \
        .filter(col("IS_CURRENT") == 'Y')

    df_tarifgruppen = spark.read.format("bigquery") \
        .option("table", f"{project_id}.{dataset_id}.STG_TARIFGRUPPEN_MAPPING") \
        .load()

    return {
        "stg_umsatz": df_stg_umsatz,
        "dim_konzern": df_dim_konzern,
        "tarifgruppen": df_tarifgruppen
    }

def normalize_umsatz(df_stg_umsatz: DataFrame) -> DataFrame:
    """Standardizes string values, rounds monetary sums to cents, and classifies transaction types."""
    return df_stg_umsatz.select(
        col("umsatz_id"),
        upper(trim(col("konzerngesellschaft"))).alias("konzerngesellschaft"),
        trim(col("vertrag")).alias("vertrag"),
        trim(col("kunde")).alias("kunde"),
        upper(trim(col("tarifgruppen_code"))).alias("tarifgruppen_code"),
        col("buchungsdatum"),
        when(col("waehrung").isNull(), lit("EUR")).otherwise(col("waehrung")).alias("waehrung"),
        when(col("buchungsart").isin("STORNO", "GUTSCHRIFT"), lit("STORNO"))
            .otherwise(lit("REGULAER")).alias("buchungsart"),
        round(col("umsatz_betrag") * 100.0, 0).cast("long").alias("umsatz_betrag_cent")
    )

def enrich_and_split_data(df_normalized: DataFrame, df_dim_konzern: DataFrame, gcs_bucket: str, args: argparse.Namespace) -> DataFrame:
    """Enriches data with Group Company Dimension and splits unmatched records to GCS."""
    df_dim_konzern_renamed = df_dim_konzern.withColumnRenamed("konzerngesellschaft", "dim_konzerngesellschaft")
    
    df_joined_konzern = df_normalized.join(
        df_dim_konzern_renamed,
        df_normalized["konzerngesellschaft"] == df_dim_konzern_renamed["dim_konzerngesellschaft"],
        "left_outer"
    )

    # Separate unmatched rows
    df_unmatched = df_joined_konzern.filter(col("dim_konzerngesellschaft").isNull())
    
    # Save unmatched records to GCS
    unmatched_path = f"gs://{gcs_bucket}/errors/umsatz/umsatz_unmatched_{args.konzerngesellschaft}_{args.verarbeitungsmonat}.dat"
    df_unmatched.write \
        .mode("overwrite") \
        .option("delimiter", "|") \
        .option("header", "true") \
        .csv(unmatched_path)

    # Retain matched records
    return df_joined_konzern.filter(col("dim_konzerngesellschaft").isNotNull())

def aggregate_and_rollup(df_matched: DataFrame, df_tarifgruppen: DataFrame, verarbeitungsmonat: str) -> DataFrame:
    """Maps tariff groups, splits regular transactions from cancellation, and runs aggregations."""
    df_tarifgruppen_renamed = df_tarifgruppen.withColumnRenamed("tarifgruppen_code", "map_tarifgruppen_code")
    
    df_joined_tarif = df_matched.join(
        df_tarifgruppen_renamed,
        df_matched["tarifgruppen_code"] == df_tarifgruppen_renamed["map_tarifgruppen_code"],
        "left_outer"
    )

    df_regulaer = df_joined_tarif.filter(col("buchungsart") == "REGULAER")
    df_storno = df_joined_tarif.filter(col("buchungsart") == "STORNO")

    # Rollup regular transactions
    df_rollup_reg = df_regulaer.groupBy(
        "konzerngesellschaft", "tarifgruppen_code", "waehrung"
    ).agg(
        _sum("umsatz_betrag_cent").alias("umsatz_summe_cent"),
        count("umsatz_id").alias("anzahl_buchungen")
    ).withColumn("verarbeitungsmonat", lit(verarbeitungsmonat))

    # Rollup storno transactions
    df_rollup_storno = df_storno.groupBy(
        "konzerngesellschaft", "tarifgruppen_code", "waehrung"
    ).agg(
        _sum("umsatz_betrag_cent").alias("storno_summe_cent")
    ).withColumn("verarbeitungsmonat", lit(verarbeitungsmonat))

    # Combine regular and storno aggregations
    return df_rollup_reg.join(
        df_rollup_storno,
        on=["konzerngesellschaft", "verarbeitungsmonat", "tarifgruppen_code", "waehrung"],
        how="left_outer"
    ).fillna({"storno_summe_cent": 0})

def write_outputs(df_final: DataFrame, project_id: str, dataset_id: str, gcs_bucket: str, args: argparse.Namespace, spark: SparkSession) -> None:
    """Writes aggregation results to BigQuery and generates processing metrics."""
    # Write to target BigQuery Table
    df_final.select(
        "konzerngesellschaft",
        "verarbeitungsmonat",
        "tarifgruppen_code",
        "waehrung",
        "umsatz_summe_cent",
        "storno_summe_cent",
        "anzahl_buchungen"
    ).write.format("bigquery") \
     .option("table", f"{project_id}.{dataset_id}.FACT_UMSATZ_KONZERN_MONAT") \
     .mode("append") \
     .save()

    # Write audit log to GCS
    record_count = df_final.count()
    audit_log_path = f"gs://{gcs_bucket}/logs/umsatz/umsatz_konsolidierung_audit_{args.konzerngesellschaft}_{args.verarbeitungsmonat}.log"
    
    audit_df = spark.createDataFrame([(record_count,)], ["total_records_processed"])
    audit_df.write.mode("overwrite").json(audit_log_path)

def main():
    args = parse_arguments()

    # Resolve Environment Variables per Env Variable Policy
    GCP_PROJECT = args.gcp_project or os.environ.get("GCP_PROJECT")
    BQ_DATASET = args.bq_dataset or os.environ.get("BQ_DATASET", "dwh_kern")
    GCS_BUCKET = args.gcs_bucket or os.environ.get("GCS_BUCKET")
    
    if not GCP_PROJECT or not GCS_BUCKET:
        raise ValueError("Missing environment variables: GCP_PROJECT and GCS_BUCKET must be provided.")

    spark = init_spark_session(GCP_PROJECT, BQ_DATASET)

    # OUTPUT/PRINT LITERAL RULE: Exactly preserve original logging and error indicators
    print(f"Starte Umsatz-Konsolidierung fuer {args.konzerngesellschaft} und Monat {args.verarbeitungsmonat}...")

    # Phase 1: Read Source Data
    sources = extract_source_data(spark, GCP_PROJECT, BQ_DATASET, args)

    # Phase 2: Transformation and Filtering
    df_normalized = normalize_umsatz(sources["stg_umsatz"])
    df_matched = enrich_and_split_data(df_normalized, sources["dim_konzern"], GCS_BUCKET, args)

    # Phase 3: Aggregation
    df_final_rollup = aggregate_and_rollup(df_matched, sources["tarifgruppen"], args.verarbeitungsmonat)

    # Phase 4: Output Persistence and Audit
    write_outputs(df_final_rollup, GCP_PROJECT, BQ_DATASET, GCS_BUCKET, args, spark)
    
    print("Umsatz-Konsolidierung erfolgreich abgeschlossen.")

if __name__ == "__main__":
    main()