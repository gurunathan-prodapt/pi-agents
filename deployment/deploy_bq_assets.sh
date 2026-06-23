#!/bin/bash
# Deployment script for BigQuery assets
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh

PROJECT_ID="project"
DATASET_ID="dataset"

echo "Deploying BigQuery DDLs and Stored Procedures to ${PROJECT_ID}.${DATASET_ID}..."

# Create dataset if it doesn't exist
bq --project_id="${PROJECT_ID}" mk --dataset --force "${DATASET_ID}"

# Deploy Table DDLs
echo "Deploying DDLs..."
bq --project_id="${PROJECT_ID}" query --use_legacy_sql=false < ddl/DWTK_MELDUNGEN.sql
bq --project_id="${PROJECT_ID}" query --use_legacy_sql=false < ddl/SOF_TA_BPR_APN.sql
bq --project_id="${PROJECT_ID}" query --use_legacy_sql=false < ddl/SOF_TA_APN_VERTRAG.sql
bq --project_id="${PROJECT_ID}" query --use_legacy_sql=false < ddl/error_log.sql
bq --project_id="${PROJECT_ID}" query --use_legacy_sql=false < ddl/job_tracking.sql
echo "DDLs deployed."

# Deploy Stored Procedures
echo "Deploying Stored Procedures..."
bq --project_id="${PROJECT_ID}" query --use_legacy_sql=false < procedures/sp_d_ausd_bp_ta_apn_vertrag.sql
bq --project_id="${PROJECT_ID}" query --use_legacy_sql=false < procedures/sp_k_ausd_bp_ta_apn_vertrag.sql
echo "Stored Procedures deployed."

echo "Deployment complete."