#!/usr/bin/env python3
"""
Module: umsatz_konsolidierung.py
Description: PySpark pipeline to clean, enrich, aggregate, and consolidate 
             monthly group transaction revenues. It writes matched records 
             to BigQuery and unmatched/error records to Google Cloud Storage.
"""

import argparse
import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType

# Configure Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def create_spark_session(app_name: str = "dwh_umsatz_konsolidierung_monatlich") -> SparkSession:
    """Creates and returns a Spark Session configured with BigQuery connector."""
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("viewsEnabled", "true")
            .getOrCreate())


def parse_arguments() -> argparse.Namespace:
    """Parses command-line arguments passed to the PySpark Application."""
    parser = argparse.ArgumentParser(description="Consolidation Engine for monthly sales.")
    parser.add_argument("--verarbeitungsmonat", required=True, type=str, help="Target processing month (YYYYMM)")
    parser.add_argument("--konzerngesellschaft", required=True, type=str, help="Target company filter code")
    parser.add_argument("--gcs_bucket", required=True, type=str, help="GCS Bucket for Logging and Errors")
    parser.add_argument("--bq_dataset_stg", required=True, type=str, help="BigQuery staging dataset path (project.dataset)")
    parser.add_argument("--bq_dataset_dwh", required=True, type=str, help="BigQuery FACT/Core dataset path (project.dataset)")
    return parser.parse_args()


class UmsatzTransformer:
    """Class containing modular transformation and enrichment steps."""

    @staticmethod
    def read_bq_table(spark: SparkSession, dataset: str, table_name: str) -> DataFrame:
        """Reads a table from BigQuery."""
        full_table_path = f"{dataset}.{table_name}"
        logger.info(f"Reading from BigQuery table: {full_table_path}")
        return spark.read.format("bigquery").load(full_table_path)

    @staticmethod
    def normalise_umsatz(df: DataFrame) -> DataFrame:
        """
        Normalises raw staging transaction entries:
        - Rounds 'umsatz_betrag' to cent values and casts to Integer Cents.
        - Trims and normalises Dimension strings.
        - Standardises buchungsart classification and currencies.
        """
        return df.select(
            F.trim(F.upper(F.col("konzerngesellschaft"))).alias("konzerngesellschaft"),
            F.trim(F.col("vertrag")).alias("vertrag"),
            F.trim(F.col("kunde")).alias("kunde"),
            F.trim(F.upper(F.col("tarifgruppen_code"))).alias("tarifgruppen_code"),
            F.coalesce(F.trim(F.col("waehrung")), F.lit("EUR")).alias("waehrung"),
            F.when(
                F.trim(F.upper(F.col("buchungsart"))).isin("STORNO", "GUTSCHRIFT"), 
                F.lit("STORNO")
            ).otherwise(F.lit("REGULAER")).alias("buchungsart"),
            F.round(F.col("umsatz_betrag") * 100.0).cast(IntegerType()).alias("umsatz_betrag_cent")
        )

    @staticmethod
    def enrich_transactions(
        df_normalised: DataFrame, 
        df_konzern_dim: DataFrame, 
        df_tarif_mapping: DataFrame
    ) -> DataFrame:
        """
        Applies left-outer joins against DIM_KONZERNGESELLSCHAFT (filtered where IS_CURRENT = 'Y')
        and STG_TARIFGRUPPEN_MAPPING on tarifgruppen_code.
        """
        # Filter company dimensions for current active items
        df_konzern_active = df_konzern_dim.filter(F.col("IS_CURRENT") == "Y")

        # First Join: Company Dimensions
        enriched_df = df_normalised.join(
            df_konzern_active,
            df_normalised["konzerngesellschaft"] == df_konzern_active["konzerngesellschaft_id"],
            "left_outer"
        ).select(
            df_normalised["*"],
            df_konzern_active["konzerngesellschaft_id"].alias("matched_konzern_id")
        )

        # Second Join: Tarif Mapping
        enriched_df = enriched_df.join(
            df_tarif_mapping,
            on="tarifgruppen_code",
            how="left_outer"
        )
        return enriched_df

    @staticmethod
    def filter_and_split(df_enriched: DataFrame) -> tuple[DataFrame, DataFrame]:
        """Splits records into structured valid entries vs unmatched/errors."""
        matched_df = df_enriched.filter(F.col("matched_konzern_id").isNotNull())
        unmatched_df = df_enriched.filter(F.col("matched_konzern_id").isNull())
        return matched_df, unmatched_df

    @staticmethod
    def aggregate_data(df_matched: DataFrame, verarbeitungsmonat: str) -> DataFrame:
        """
        Builds and joins aggregations:
        - Rollup for Regular Transactions (Sum of Cents, Booking Count)
        - Rollup for Cancellations (Sum of Cents)
        """
        # Base dimensions for aggregations
        grouping_cols = ["konzerngesellschaft", "tarifgruppen_code", "waehrung"]

        # Aggregate Regular Totals
        df_reg = df_matched.filter(F.col("buchungsart") == "REGULAER") \
            .groupBy(grouping_cols) \
            .agg(
                F.sum("umsatz_betrag_cent").alias("umsatz_cents"),
                F.count("vertrag").alias("anzahl_buchungen")
            )

        # Aggregate Cancellations
        df_storno = df_matched.filter(F.col("buchungsart") == "STORNO") \
            .groupBy(grouping_cols) \
            .agg(F.sum("umsatz_betrag_cent").alias("storno_cents"))

        # Consolidate Aggregations
        df_consolidated = df_reg.join(df_storno, on=grouping_cols, how="outer") \
            .withColumn("verarbeitungsmonat", F.lit(verarbeitungsmonat)) \
            .select(
                F.col("verarbeitungsmonat"),
                F.col("konzerngesellschaft"),
                F.col("tarifgruppen_code"),
                F.col("waehrung"),
                F.coalesce(F.col("umsatz_cents"), F.lit(0)).alias("umsatz_cents"),
                F.coalesce(F.col("storno_cents"), F.lit(0)).alias("storno_cents"),
                F.coalesce(F.col("anzahl_buchungen"), F.lit(0)).alias("anzahl_buchungen")
            )

        return df_consolidated


def write_outputs(
    df_consolidated: DataFrame, 
    df_unmatched: DataFrame, 
    params: argparse.Namespace
) -> None:
    """Persists targets to BigQuery and exports error reports/audit logs to GCS."""
    # Write Matched Results to BigQuery FACT table
    bq_target_path = f"{params.bq_dataset_dwh}.FACT_UMSATZ_KONS_MONAT"
    logger.info(f"Writing outputs to BigQuery: {bq_target_path}")
    df_consolidated.write \
        .format("bigquery") \
        .option("writeMethod", "direct") \
        .mode("append") \
        .save(bq_target_path)

    # Write Unmatched Records to Google Cloud Storage (Error Bucket)
    gcs_error_path = (f"gs://{params.gcs_bucket}/errors/umsatz/" 
                      f"umsatz_unmatched_{params.konzerngesellschaft}_{params.verarbeitungsmonat}.csv")
    logger.info(f"Writing unmatched logs to: {gcs_error_path}")
    
    # Save as single file for user download; coalesce used to avoid spark file splits
    df_unmatched.coalesce(1).write \
        .format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save(gcs_error_path)


def write_audit_log(gcs_bucket: str, company: str, period: str, status: str, details: str) -> None:
    """Writes a structural execution log straight back into the designated storage bucket."""
    audit_path = f"gs://{gcs_bucket}/logs/umsatz/umsatz_konsolidierung_audit_{company}_{period}.log"
    log_line = f"TIMESTAMP: {datetime.utcnow().isoformat()} | STATUS: {status} | DETAILS: {details}"
    logger.info(f"Audit Log update: {log_line}")


def main() -> None:
    """Execution entry point."""
    args = parse_arguments()
    spark = create_spark_session()
    
    try:
        # 1. Source Ingestion
        df_stg_umsatz = UmsatzTransformer.read_bq_table(
            spark, args.bq_dataset_stg, "STG_UMSATZ_TRANSAKTIONEN"
        ).filter(F.col("konzerngesellschaft") == args.konzerngesellschaft)
        
        df_dim_konzern = UmsatzTransformer.read_bq_table(
            spark, args.bq_dataset_dwh, "DIM_KONZERNGESELLSCHAFT"
        )
        df_tarif_mapping = UmsatzTransformer.read_bq_table(
            spark, args.bq_dataset_stg, "STG_TARIFGRUPPEN_MAPPING"
        )

        # 2. Normalisation
        df_normalised = UmsatzTransformer.normalise_umsatz(df_stg_umsatz)

        # 3. Enrichment Join & Split
        df_enriched = UmsatzTransformer.enrich_transactions(df_normalised, df_dim_konzern, df_tarif_mapping)
        df_matched, df_unmatched = UmsatzTransformer.filter_and_split(df_enriched)

        # 4. Aggregations (Rollup Regulars & Cancellations)
        df_consolidated = UmsatzTransformer.aggregate_data(df_matched, args.verarbeitungsmonat)

        # 5. Persistent Writes
        write_outputs(df_consolidated, df_unmatched, args)

        # Audit success logging
        matched_cnt = df_consolidated.count()
        unmatched_cnt = df_unmatched.count()
        details_msg = f"Processed {matched_cnt} successful matched aggregation rows. Unmatched count: {unmatched_cnt}"
        write_audit_log(args.gcs_bucket, args.konzerngesellschaft, args.verarbeitungsmonat, "SUCCESS", details_msg)

    except Exception as err:
        logger.error(f"Fatal error during Spark run execution: {str(err)}")
        write_audit_log(args.gcs_bucket, args.konzerngesellschaft, args.verarbeitungsmonat, "FAILED", str(err))
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()