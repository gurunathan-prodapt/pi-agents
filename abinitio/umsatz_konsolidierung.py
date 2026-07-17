#!/usr/bin/env python3
"""
Target File: abinitio/umsatz_konsolidierung.py
Description: Migrated Core PySpark process for 'umsatz_konsolidierung'.
             Normalizes, segregates, aggregates, and validates monthly
             corporate transaction revenues.
"""

import os
import sys
import argparse
import logging
from typing import Dict, Any, Tuple
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    TimestampType
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("umsatz_konsolidierung")


def parse_arguments() -> argparse.Namespace:
    """Parses incoming command-line execution parameters."""
    parser = argparse.ArgumentParser(
        description="Run the DWH Monthly Revenue Consolidation PySpark Pipeline."
    )
    parser.add_argument("--verarbeitungsmonat", required=True, help="Processing month in YYYYMM format")
    parser.add_argument("--konzerngesellschaft", required=True, help="Corporate entity identifier code")
    parser.add_argument("--ora_connect_string", required=True, help="JDBC Connection string to database")
    parser.add_argument("--error_output_dir", required=True, help="GCS URI target for writing unmatched records")
    parser.add_argument("--alert_output_dir", required=True, help="GCS URI target for publishing alert JSONs")
    parser.add_argument("--log_dir", required=True, help="GCS URI target path for operational logs")
    parser.add_argument("--konsolidierung_toleranz", type=float, default=2.5, help="Consolidation deviation tolerance threshold")
    parser.add_argument("--max_abweichungen", type=int, default=25, help="Maximum allowed variations limit")
    parser.add_argument("--min_row_count", type=int, default=1, help="Minimum volume threshold logic check")
    return parser.parse_args()


def get_db_credentials() -> Dict[str, str]:
    """
    Fetches database credentials securely from system environment variables.
    These are injected securely at runtime via GCP Secret Manager inside Cloud Composer/Dataproc.
    """
    db_user = os.environ.get("DB_USER")
    db_password = os.environ.get("DB_PASSWORD")
    
    if not db_user or not db_password:
        logger.warning("Database credentials DB_USER or DB_PASSWORD are not fully populated in environment.")
        # Fallback to default or dummy values for local dry runs
        return {"user": "UNKNOWN_USER", "password": "UNKNOWN_PASSWORD"}
        
    return {"user": db_user, "password": db_password}


def validate_processing_period(
    spark: SparkSession,
    connect_string: str,
    db_props: Dict[str, str],
    verarbeitungsmonat: str
) -> None:
    """
    Phase 1: Validates that the requested execution period is active inside the Master Calendar table.
    Raises ValueError to halt execution if calendar period does not exist.
    """
    logger.info(f"Validating processing period '{verarbeitungsmonat}' against DIM_PERIODE.")
    
    # Pushdown query structure
    period_query = f"(SELECT 1 FROM DIM_PERIODE WHERE ID_MONAT = '{verarbeitungsmonat}') temp"
    
    try:
        period_df = spark.read.jdbc(url=connect_string, table=period_query, properties=db_props)
        if period_df.isEmpty():
            err_msg = f"FATAL: The processing period {verarbeitungsmonat} is invalid, closed, or missing in DIM_PERIODE."
            logger.error(err_msg)
            raise ValueError(err_msg)
        logger.info(f"Verification Success: Period '{verarbeitungsmonat}' is open and active.")
    except Exception as e:
        logger.error("Error encountered during verification of period on DB target.")
        raise e


def ingest_source_data(
    spark: SparkSession,
    connect_string: str,
    db_props: Dict[str, str],
    verarbeitungsmonat: str,
    konzerngesellschaft: str
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """Phase 1: Performance-optimized JDBC Pushdown Reader logic for raw datasets."""
    logger.info("Initializing relational ingestion streams via JDBC pushdown queries.")
    
    # Apply filter constraints immediately at DB layer to avoid pulling massive unneeded datasets
    stg_umsatz_query = f"""
        (SELECT * FROM STG_UMSATZ_TRANSAKTIONEN 
         WHERE VERARBEITUNGSMONAT = '{verarbeitungsmonat}' 
           AND KONZERNGESELLSCHAFT = '{konzerngesellschaft}' 
           AND ETL_STATUS = 'PENDING') temp_umsatz
    """
    
    dim_gesellschaft_query = """
        (SELECT * FROM DIM_KONZERNGESELLSCHAFT WHERE IS_CURRENT = 'Y') temp_ges
    """
    
    df_stg_umsatz = spark.read.jdbc(url=connect_string, table=stg_umsatz_query, properties=db_props)
    df_dim_ges = spark.read.jdbc(url=connect_string, table=dim_gesellschaft_query, properties=db_props)
    df_tarif_mapping = spark.read.jdbc(url=connect_string, table="STG_TARIFGRUPPEN_MAPPING", properties=db_props)
    
    return df_stg_umsatz, df_dim_ges, df_tarif_mapping


def normalize_and_isolate_transactions(
    df_stg_umsatz: DataFrame,
    df_dim_ges: DataFrame,
    error_output_dir: str,
    verarbeitungsmonat: str
) -> DataFrame:
    """
    Phase 2: Performs string normalizations, maps booking codes, converts floats 
    to absolute cents (Long), resolves corporate relationships, and isolates rejections.
    """
    logger.info("Normalizing raw transaction attributes and executing corporate validation joins.")
    
    df_normalized = df_stg_umsatz.withColumn(
        "clean_ges", F.upper(F.trim(F.col("konzerngesellschaft")))
    ).withColumn(
        "clean_tarif", F.upper(F.trim(F.col("tarifgruppen_code")))
    ).withColumn(
        "clean_vertrag", F.trim(F.col("vertrag"))
    ).withColumn(
        "clean_kunde", F.trim(F.col("kunde"))
    ).withColumn(
        "clean_waehrung", F.coalesce(F.nullif(F.trim(F.col("waehrung")), F.lit("")), F.lit("EUR"))
    ).withColumn(
        "mapped_buchungsart", 
        F.when(F.col("buchungsart").isin("STORNO", "GUTSCHRIFT"), F.lit("STORNO"))
         .otherwise(F.lit("REGULAER"))
    ).withColumn(
        "umsatz_betrag_cent", F.round(F.col("umsatz_betrag") * 100.0, 0).cast(LongType())
    )

    # Join with Dim Corporate to enforce active relationship criteria
    df_dim_ges_aliased = df_dim_ges.select(
        F.upper(F.trim(F.col("konzerngesellschaft"))).alias("join_ges_key"),
        F.lit(True).alias("is_matched")
    )
    
    df_joined = df_normalized.join(
        df_dim_ges_aliased,
        df_normalized["clean_ges"] == df_dim_ges_aliased["join_ges_key"],
        "left_outer"
    )

    # Split unmatched rows representing orphan corporate codes
    df_unmatched = df_joined.filter(F.col("is_matched").isNull())
    df_matched = df_joined.filter(F.col("is_matched") == True).drop("join_ges_key", "is_matched")

    # Handle asynchronous writing of rejections to isolated Cloud Storage
    unmatched_count = df_unmatched.count()
    if unmatched_count > 0:
        logger.warning(f"Isolating {unmatched_count} unmapped corporate transactions to target path: {error_output_dir}")
        df_unmatched.select(
            "konzerngesellschaft", "clean_vertrag", "clean_waehrung", "umsatz_betrag", "clean_tarif"
        ).write 
         .mode("overwrite") 
         .option("delimiter", "|") 
         .option("header", "true") 
         .csv(f"{error_output_dir}/unmatched_{verarbeitungsmonat}")
         
    return df_matched


def join_tariff_mapping(df_matched: DataFrame, df_tarif_mapping: DataFrame) -> DataFrame:
    """Phase 2: Combines mapping details to append consolidated attributes."""
    logger.info("Executing left-outer join on Tariff Group Mappings.")
    
    df_tarif_clean = df_tarif_mapping.select(
        F.upper(F.trim(F.col("tarifgruppen_code"))).alias("join_tarif_key"),
        F.col("tarifgruppen_code").alias("mapped_tarif_code")
    ).distinct()

    return df_matched.join(
        df_tarif_clean,
        df_matched["clean_tarif"] == df_tarif_clean["join_tarif_key"],
        "left_outer"
    ).drop("join_tarif_key")


def aggregate_and_consolidate(df_mapped: DataFrame) -> DataFrame:
    """
    Phase 3: Splits normalized dataset into parallel aggregation pipelines for regular
    and canceled booking streams, then recombines them into a consolidated report view.
    """
    logger.info("Performing dual-stream segregation and aggregation (Rollup Phase).")
    
    group_cols = ["clean_ges", "verarbeitungsmonat", "mapped_tarif_code", "clean_waehrung"]

    # Stream A: Regular pipeline aggregation
    df_regular_rollup = df_mapped.filter(F.col("mapped_buchungsart") == "REGULAER") 
        .groupBy(group_cols) 
        .agg(
            F.sum("umsatz_betrag_cent").alias("umsatz_summe_cent"),
            F.count(F.lit(1)).alias("anzahl_buchungen")
        )

    # Stream B: Cancellations (Storno) pipeline aggregation
    df_storno_rollup = df_mapped.filter(F.col("mapped_buchungsart") == "STORNO") 
        .groupBy(group_cols) 
        .agg(
            F.sum("umsatz_betrag_cent").alias("storno_summe_cent")
        )

    # Merge streams using Left Outer Join to retain regular items that might not have cancellations
    df_consolidated = df_regular_rollup.join( 
        df_storno_rollup,
        group_cols,
        "left_outer"
    ).fillna({"storno_summe_cent": 0})

    # Prepare for final database schema load
    df_target_load = df_consolidated.select(
        F.col("clean_ges").alias("konzerngesellschaft"),
        F.col("verarbeitungsmonat"),
        F.coalesce(F.col("mapped_tarif_code"), F.lit("UNMAPPED")).alias("tarifgruppen_code"),
        F.col("clean_waehrung").alias("waehrung"),
        F.col("umsatz_summe_cent"),
        F.col("storno_summe_cent"),
        F.col("anzahl_buchungen"),
        F.current_timestamp().alias("load_timestamp")
    )
    
    return df_target_load


def write_audit_log(
    connect_string: str,
    db_props: Dict[str, str],
    verarbeitungsmonat: str,
    konzerngesellschaft: str,
    source_row_count: int,
    target_row_count: int,
    deviation_count: int,
    status: str
) -> None: 
    """Writes operational stats and metrics back to DB table AUDIT_UMSATZ_CONSOLIDATION."""
    logger.info(f"Logging process audit status '{status}' to remote relational database.")
    
    spark = SparkSession.getActiveSession()
    if not spark:
        return
        
    audit_schema = StructType([
        StructField("verarbeitungsmonat", StringType(), False),
        StructField("konzerngesellschaft", StringType(), False),
        StructField("source_row_count", LongType(), False),
        StructField("target_row_count", LongType(), False),
        StructField("deviation_count", LongType(), False),
        StructField("status", StringType(), False),
        StructField("execution_time", TimestampType(), False)
    ])
    
    audit_record = [(
        verarbeitungsmonat,
        konzerngesellschaft,
        int(source_row_count),
        int(target_row_count),
        int(deviation_count),
        status,
        datetime.now()
    )]
    
    df_audit = spark.createDataFrame(audit_record, schema=audit_schema)
    try:
        df_audit.write.jdbc(
            url=connect_string,
            table="AUDIT_UMSATZ_CONSOLIDATION",
            mode="append",
            properties=db_props
        )
        logger.info("Process audit record persisted successfully.")
    except Exception as e:
        logger.error(f"Failed to write audit metrics back to base table: {str(e)}")


def write_alert_record(
    spark: SparkSession,
    alert_dir: str,
    verarbeitungsmonat: str,
    alert_code: str,
    message: str
) -> None:
    """Generates JSON alert items directly to GCS path for SIEM/Monitoring scanning tools."""
    logger.warning(f"Triggering Alert [{alert_code}]: {message}")
    
    alert_payload = [{
        "verarbeitungsmonat": verarbeitungsmonat,
        "alert_code": alert_code,
        "error_message": message,
        "alert_timestamp": datetime.now().isoformat()
    }]
    
    try:
        alert_df = spark.createDataFrame(alert_payload)
        alert_df.write 
                .mode("append") 
                .json(alert_dir)
    except Exception as e:
        logger.error(f"Could not publish warning to JSON logs: {str(e)}")


def run_pipeline(args: argparse.Namespace) -> None:
    """Initializes Spark, maps configuration steps, and orchestrates pipeline execution."""
    spark = SparkSession.builder \
        .appName("DW-DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JP") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
        
    db_properties = get_db_credentials()
    db_properties["driver"] = "oracle.jdbc.driver.OracleDriver"
    
    try:
        # Step 1: Check if execution month exists
        validate_processing_period(spark, args.ora_connect_string, db_properties, args.verarbeitungsmonat)
        
        # Step 2: Read relational tables
        df_stg_umsatz, df_dim_ges, df_tarif_mapping = ingest_source_data(
            spark, args.ora_connect_string, db_properties, args.verarbeitungsmonat, args.konzerngesellschaft
        )
        
        source_count = df_stg_umsatz.count()
        logger.info(f"Retrieved {source_count} pending base transactions to consolidate.")
        
        if source_count == 0:
            logger.warning("Zero processing transactions located. Finalizing run with empty placeholder status.")
            write_audit_log(args.ora_connect_string, db_properties, args.verarbeitungsmonat,
                            args.konzerngesellschaft, 0, 0, 0, "SUCCESS")
            return

        # Step 3: Perform Normalization & Corporate Entity checks
        df_normalized = normalize_and_isolate_transactions(
            df_stg_umsatz, df_dim_ges, args.error_output_dir, args.verarbeitungsmonat
        )
        
        # Step 4: Map tariffs
        df_mapped = join_tariff_mapping(df_normalized, df_tarif_mapping)
        
        # Step 5: Dual Aggregation Rollup
        df_target_load = aggregate_and_consolidate(df_mapped)
        
        # Step 6: Load consolidation outputs to Fact Table
        logger.info("Persisting target calculations inside FACT_UMSATZ_KONZERN_MONAT table.")
        df_target_load.write.jdbc(
            url=args.ora_connect_string,
            table="FACT_UMSATZ_KONZERN_MONAT",
            mode="append",
            properties=db_properties
        )
        
        total_loaded_records = df_target_load.count()
        
        # Step 7: Post-Process Quality Gate Validation Checks
        # Validate baseline record count threshold limits
        if total_loaded_records < args.min_row_count:
            alert_msg = f"Target row volume {total_loaded_records} violates minimum config {args.min_row_count}."
            write_alert_record(spark, args.alert_output_dir, args.verarbeitungsmonat, "LOW_ROW_COUNT_WARNING", alert_msg)
            
        # Analyze absolute variance thresholds
        df_tolerance_check = df_target_load.withColumn(
            "cent_difference", F.abs(F.col("umsatz_summe_cent") - F.col("storno_summe_cent"))
        ).filter(F.col("cent_difference") > (args.konsolidierung_toleranz * 100))
        
        out_of_bounds_count = df_tolerance_check.count()
        
        if out_of_bounds_count > args.max_abweichungen:
            alert_msg = f"Critical threshold crossed: {out_of_bounds_count} metrics failed standard variation limits."
            write_alert_record(spark, args.alert_output_dir, args.verarbeitungsmonat, "TOLERANCE_BREACH", alert_msg)
            
            write_audit_log(
                args.ora_connect_string, db_properties, args.verarbeitungsmonat, args.konzerngesellschaft,
                source_count, total_loaded_records, out_of_bounds_count, "TOLERANCE_BREACH"
            )
            raise ValueError(f"Job execution terminated: {alert_msg}")
            
        # Logging standard execution success
        write_audit_log(
            args.ora_connect_string, db_properties, args.verarbeitungsmonat, args.konzerngesellschaft,
            source_count, total_loaded_records, out_of_bounds_count, "SUCCESS"
        )
        logger.info("Data Migration Process completed successfully.")
        
    except Exception as err:
        logger.exception("An unhandled exception occurred in the PySpark Processing Engine:")
        try:
            write_audit_log(
                args.ora_connect_string, db_properties, args.verarbeitungsmonat, args.konzerngesellschaft,
                0, 0, 0, "FAILED"
            )
        except Exception as audit_err:
            logger.error(f"Failed to log secondary database failure report: {str(audit_err)}")
        raise err
    finally:
        spark.stop()


if __name__ == "__main__":
    run_pipeline(parse_arguments())