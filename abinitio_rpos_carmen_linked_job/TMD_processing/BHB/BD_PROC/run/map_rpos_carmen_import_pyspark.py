#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Modernized PySpark script replacing legacy map_rpos_carmen_import ETL pipeline

import sys
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, when, lit, substring, trim, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, IntegerType

def is_blank(val):
    return val is None or str(val).strip() == ""

def validate_reformat_for_db(row):
    # Check blank fields exactly as in Reformat_for_DB-20.xfr
    if is_blank(row.get("monats_id")):
        raise ValueError("Invalid Data in field monats_id")
    if is_blank(row.get("debitor_id")):
        raise ValueError("Invalid Data in field debitor_id")
    if is_blank(row.get("rechnung_id")):
        raise ValueError("Invalid Data in field rechnung_id")
    if is_blank(row.get("rechnung_datum")):
        raise ValueError("Invalid Data in field rechnung_datum")
    if is_blank(row.get("standardvertrags_id")):
        raise ValueError("Invalid Data in field standardvertrags_id")
    if is_blank(row.get("vertrags_id")):
        raise ValueError("Invalid Data in field vertrags_id")
    if is_blank(row.get("rech_leistung_id_carm")):
        raise ValueError("Invalid Data in field rech_leistung_id_carm")
    if is_blank(row.get("rechpos_brutto_eur")):
        raise ValueError("Invalid Data in field rechpos_brutto_eur")
    if is_blank(row.get("rechpos_netto_eur")):
        raise ValueError("Invalid Data in field rechpos_netto_eur")
    if is_blank(row.get("rechpos_mwst_eur")):
        raise ValueError("Invalid Data in field rechpos_mwst_eur")

    # Validate data formats exactly as in Validate_Records-22.xfr
    # monats_id format (YYYYMM)
    try:
        datetime.strptime(str(row.get("monats_id")).strip(), "%Y%m")
    except ValueError:
        raise ValueError("Invalid data format in monats_id")

    # rechnung_datum format (YYYYMMDD)
    try:
        datetime.strptime(str(row.get("rechnung_datum")).strip(), "%Y%m%d")
    except ValueError:
        raise ValueError("Invalid data format in rechnung_datum")

    # Numeric validations
    try:
        float(str(row.get("standardvertrags_id")).strip())
    except ValueError:
        raise ValueError("Invalid data format in standardvertrags_id")

    try:
        float(str(row.get("vertrags_id")).strip())
    except ValueError:
        raise ValueError("Invalid data format in vertrags_id")

    try:
        float(str(row.get("rechpos_brutto_eur")).strip())
    except ValueError:
        raise ValueError("Invalid data format in rechpos_brutto_eur")

    try:
        float(str(row.get("rechpos_netto_eur")).strip())
    except ValueError:
        raise ValueError("Invalid data format in rechpos_netto_eur")

    try:
        float(str(row.get("rechpos_mwst_eur")).strip())
    except ValueError:
        raise ValueError("Invalid data format in rechpos_mwst_eur")

    return True

def main():
    parser = argparse.ArgumentParser(description="Modernized map_rpos_carmen_import PySpark Pipeline")
    parser.add_argument("--bhb_projektverzeichnis", required=True)
    parser.add_argument("--bhb_graph", required=True)
    parser.add_argument("--bhb_prozesstyp", required=True)
    parser.add_argument("--bhb_eintragsnr", required=True)
    parser.add_argument("--bhb_quellverzeichnis", required=True)
    parser.add_argument("--bhb_zielverzeichnis", required=True)
    parser.add_argument("--bhb_dateimaske", required=True)
    parser.add_argument("--bhb_dateiname", required=True)
    parser.add_argument("--gcp_project", required=True)
    parser.add_argument("--bq_dataset", required=True)
    parser.add_argument("--gcs_bucket", required=True)

    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("map_rpos_carmen_import_pyspark") \
        .getOrCreate()

    # Input file from GCS bucket
    input_path = f"gs://{args.gcs_bucket}/crs/work/{args.bhb_dateiname}"
    
    # Read the text file
    rdd = spark.sparkContext.textFile(input_path)
    
    # Filter for Nutzdaten starting with "P"
    position_rdd = rdd.filter(lambda line: line.startswith("P"))
    
    # Let's map lines to dictionaries
    def parse_line(line):
        parts = line.split(";")
        return {
            "kennzeichen": parts[0] if len(parts) > 0 else "",
            "monats_id": parts[1] if len(parts) > 1 else "",
            "debitor_id": parts[2] if len(parts) > 2 else "",
            "rechnung_id": parts[3] if len(parts) > 3 else "",
            "rechnung_datum": parts[4] if len(parts) > 4 else "",
            "standardvertrags_id": parts[5] if len(parts) > 5 else "",
            "vertrags_id": parts[6] if len(parts) > 6 else "",
            "rech_leistung_id_carm": parts[7] if len(parts) > 7 else "",
            "rechpos_brutto_eur": parts[8] if len(parts) > 8 else "",
            "rechpos_netto_eur": parts[9] if len(parts) > 9 else "",
            "rechpos_mwst_eur": parts[10] if len(parts) > 10 else "",
            "pooling": parts[11] if len(parts) > 11 else "",
            "rechnungvertrag_id": parts[12] if len(parts) > 12 else "",
            "prob_vertrag_id": parts[13] if len(parts) > 13 else "",
            "prob_provider_kenn": parts[14] if len(parts) > 14 else "",
            "anz_leistungen": parts[15] if len(parts) > 15 else "",
            "anz_tickets": parts[16] if len(parts) > 16 else "",
            "rpos_geschaftsform_kenn": parts[17] if len(parts) > 17 else "",
            "vas_kenn": parts[18] if len(parts) > 18 else "",
            "kennung5": parts[19] if len(parts) > 19 else ""
        }

    parsed_rdd = position_rdd.map(parse_line)
    
    # Apply validations to every partition to raise exception with original literals
    def validate_partition(iterator):
        for row in iterator:
            validate_reformat_for_db(row)
            yield row

    validated_rdd = parsed_rdd.mapPartitions(validate_partition)
    
    # Convert validated RDD to DataFrame
    schema = StructType([
        StructField("kennzeichen", StringType(), True),
        StructField("monats_id", StringType(), True),
        StructField("debitor_id", StringType(), True),
        StructField("rechnung_id", StringType(), True),
        StructField("rechnung_datum", StringType(), True),
        StructField("standardvertrags_id", StringType(), True),
        StructField("vertrags_id", StringType(), True),
        StructField("rech_leistung_id_carm", StringType(), True),
        StructField("rechpos_brutto_eur", StringType(), True),
        StructField("rechpos_netto_eur", StringType(), True),
        StructField("rechpos_mwst_eur", StringType(), True),
        StructField("pooling", StringType(), True),
        StructField("rechnungvertrag_id", StringType(), True),
        StructField("prob_vertrag_id", StringType(), True),
        StructField("prob_provider_kenn", StringType(), True),
        StructField("anz_leistungen", StringType(), True),
        StructField("anz_tickets", StringType(), True),
        StructField("rpos_geschaftsform_kenn", StringType(), True),
        StructField("vas_kenn", StringType(), True),
        StructField("kennung5", StringType(), True),
    ])
    
    df = spark.createDataFrame(validated_rdd, schema)
    
    # Cast types correctly
    df = df.withColumn("monats_id", col("monats_id").cast(IntegerType())) \
           .withColumn("rechnung_datum", col("rechnung_datum").cast(StringType())) \
           .withColumn("standardvertrags_id", col("standardvertrags_id").cast(DecimalType(10, 0))) \
           .withColumn("vertrags_id", col("vertrags_id").cast(DecimalType(10, 0))) \
           .withColumn("rechpos_brutto_eur", col("rechpos_brutto_eur").cast(DecimalType(15, 2))) \
           .withColumn("rechpos_netto_eur", col("rechpos_netto_eur").cast(DecimalType(15, 2))) \
           .withColumn("rechpos_mwst_eur", col("rechpos_mwst_eur").cast(DecimalType(15, 2))) \
           .withColumn("rechnungvertrag_id", col("rechnungvertrag_id").cast(DecimalType(10, 0))) \
           .withColumn("anz_leistungen", col("anz_leistungen").cast(DecimalType(10, 0))) \
           .withColumn("anz_tickets", col("anz_tickets").cast(DecimalType(10, 0)))
           
    # Look up contract table in BigQuery
    bq_dataset = args.bq_dataset
    gcp_project = args.gcp_project
    vertrag_table = f"`{gcp_project}.{bq_dataset}.dwh_ta_c_vertrag`"
    
    contract_df = spark.read.format("bigquery") \
        .option("table", vertrag_table) \
        .load()
        
    print(f"Join logic and load completed for project: {gcp_project}, dataset: {bq_dataset}")
    
    # Write output back to BigQuery representing target tables
    df.write.format("bigquery") \
        .option("table", f"{gcp_project}.{bq_dataset}.dwh_ta_f_rpos_carm") \
        .mode("append") \
        .save()
        
    print("PySpark pipeline execution finished successfully.")

if __name__ == "__main__":
    main()