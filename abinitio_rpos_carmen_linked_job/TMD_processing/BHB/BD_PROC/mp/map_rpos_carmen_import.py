#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Target PySpark pipeline for DW.RPOS_CARM_IMPORT
Converts: map_rpos_carmen_import.mp to PySpark on Dataproc Serverless.
"""

import os
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType
from google.cloud import bigquery

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("map_rpos_carmen_import") \
    .getOrCreate()

# ==============================================================================
# ENVIRONMENT & CONFIGURATION RESOLUTION (Global & Job-Specific Policies)
# ==============================================================================
GCP_PROJECT = os.environ.get("GCP_PROJECT")
GCS_BUCKET = os.environ.get("GCS_BUCKET")
BQ_DATASET = os.environ.get("BQ_DATASET", "DW_HOUSE_SCHEMA")

if not GCP_PROJECT or not GCS_BUCKET:
    print("ERROR: Mandatory Global Environment variables GCP_PROJECT or GCS_BUCKET are missing.")
    sys.exit(1)

# Job-specific inputs
bhb_dateiname = os.environ.get("BHB_Dateiname") or (sys.argv[1] if len(sys.argv) > 1 else None)
bhb_eintragsnr = os.environ.get("BHB_Eintragsnr") or (sys.argv[2] if len(sys.argv) > 2 else None)

JOB_CONFIG = {
    "BHB_Dateimaske": "CARMEN_B_*_pos.fix",
    "BHB_Kopfdatensatzkennung": "H",
    "BHB_Nutzdatensatzkennung": "P",
    "BHB_Endedatensatzkennung": "X",
    "BHB_Projektverzeichnis": "/Projects/TMD/processing/BHB/BD_PROC",
    "BHB_Quellverzeichnis": f"gs://{GCS_BUCKET}/crs/work/",
    "BHB_Zielverzeichnis": f