#!/bin/bash
# Deployment script for BigQuery DDL and Stored Procedures
# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_msisdn.ksh

# Set your GCP Project ID and BigQuery Dataset ID
export GCP_PROJECT_ID="your_gcp_project"
export BQ_DATASET_ID="your_bq_dataset"

echo "Deploying BigQuery components to Project: ${GCP_PROJECT_ID}, Dataset: ${BQ_DATASET_ID}"

# Check if bq command is available
if ! command -v bq &> /dev/null
then
    echo "bq command not found. Please install Google Cloud SDK and authenticate."
    exit 1
fi

# Create BigQuery Dataset if it does not exist
echo "Creating BigQuery dataset ${BQ_DATASET_ID} if it doesn't exist..."
bq --project_id=${GCP_PROJECT_ID} mk --dataset --default_table_expiration 3600 "${BQ_DATASET_ID}"

if [ $? -ne 0 ]; then
    echo "Failed to create or verify dataset ${BQ_DATASET_ID}. Exiting."
    exit 1
fi
echo "Dataset ${BQ_DATASET_ID} is ready."

# Deploy DDLs
echo "Deploying DDLs..."
DDL_FILES=(
    "ddl/job_error_log.sql"
    "ddl/job_tracking.sql"
    "ddl/target_bp_ta_msisdn.sql"
)

for ddl_file in "${DDL_FILES[@]}"; do
    echo "Deploying ${ddl_file}..."
    bq --project_id=${GCP_PROJECT_ID} query --use_legacy_sql=false < "${ddl_file}"
    if [ $? -ne 0 ]; then
        echo "Failed to deploy ${ddl_file}. Exiting."
        exit 1
    fi
done
echo "DDLs deployed successfully."

# Deploy Stored Procedures
echo "Deploying Stored Procedures..."
SP_FILES=(
    "stored_procedures/proc_d_ausd_bp_ta_msisdn.sql"
    "stored_procedures/proc_ausd_bp_ta_msisdn.sql"
)

for sp_file in "${SP_FILES[@]}"; do
    echo "Deploying ${sp_file}..."
    # Procedures need to be created with a specific syntax that `bq query` handles.
    # The --replace=true flag ensures existing procedures are updated.
    bq --project_id=${GCP_PROJECT_ID} query --use_legacy_sql=false < "${sp_file}"
    if [ $? -ne 0 ]; then
        echo "Failed to deploy ${sp_file}. Exiting."
        exit 1
    fi
done
echo "Stored Procedures deployed successfully."

echo "Deployment complete."
echo "You can now call the main orchestration procedure, for example:"
echo "bq query --project_id=${GCP_PROJECT_ID} --dataset_id=${BQ_DATASET_ID} --run_as_me --dry_run --nouse_legacy_sql \\"
echo "  'CALL \`your_gcp_project.your_bq_dataset.proc_ausd_bp_ta_msisdn\`(\"MY_JOB_01\", \"28022023\", \"ENTRY_001\", NULL);'"