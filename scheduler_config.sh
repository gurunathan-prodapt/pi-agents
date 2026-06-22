# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_valid.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_valid.ksh
# Target: Cloud Scheduler configuration script for triggering BigQuery Stored Procedure

# This script creates a Google Cloud Scheduler job to trigger the BigQuery Stored Procedure.
# Replace placeholder values with your actual GCP project ID, region, BigQuery dataset,
# and the service account email that Cloud Scheduler will use.

# --- Configuration Variables (REPLACE THESE) ---
PROJECT_ID="your-gcp-project-id"             # Your GCP Project ID
REGION="your-gcp-region"                     # e.g., us-central1, europe-west1
BIGQUERY_DATASET="your-bigquery-dataset"     # The BigQuery dataset containing your stored procedures
SERVICE_ACCOUNT_EMAIL="your-service-account-email" # Email of the service account used by Cloud Scheduler
                                                     # (e.g., bert-scheduler-sa@your-gcp-project-id.iam.gserviceaccount.com)
JOB_NAME="bert-v-ta-cntrct-valid-scheduler"  # Name for the Cloud Scheduler job
JOB_DESCRIPTION="Schedule for BERT_V_TA_CNTRCT_VALID BigQuery Stored Procedure daily"
CRON_SCHEDULE="0 0 * * *"                    # Example: Run daily at midnight UTC. Adjust as needed.
                                             # For other schedules, see: https://cloud.google.com/scheduler/docs/configuring/cron-job-schedules

# --- BigQuery Stored Procedure Call Parameters ---
# These parameters will be passed to the BERT_V_TA_CNTRCT_VALID BigQuery Stored Procedure.
# You can customize these or make them dynamic based on your requirements.
DYNAMIC_JOB_KENNUNG="SCHEDULER_$(date +%Y%m%d)" # Example: Dynamic job ID based on current date
EINTRAEG_NR="1"                                 # Example entry number
PROGRAM_NAME="BERT_V_TA_CNTRCT_VALID"
CALLER_PROCESS="Cloud Scheduler"

# Construct the BigQuery SQL command to execute the stored procedure
BIGQUERY_SQL_COMMAND="CALL ${PROJECT_ID}.${BIGQUERY_DATASET}.BERT_V_TA_CNTRCT_VALID('${DYNAMIC_JOB_KENNUNG}', ${EINTRAEG_NR}, '${PROGRAM_NAME}', '${CALLER_PROCESS}');"

echo "Creating Cloud Scheduler job: ${JOB_NAME} in project ${PROJECT_ID}..."
echo "BigQuery SQL Command to be executed: ${BIGQUERY_SQL_COMMAND}"

# Create the Cloud Scheduler job using gcloud CLI
# This command sends an HTTP POST request to the BigQuery Jobs API to run a query.
gcloud scheduler jobs create http "${JOB_NAME}" \
    --project="${PROJECT_ID}" \
    --location="${REGION}" \
    --schedule="${CRON_SCHEDULE}" \
    --description="${JOB_DESCRIPTION}" \
    --uri="https://bigquery.googleapis.com/bigquery/v2/projects/${PROJECT_ID}/jobs" \
    --http-method=POST \
    --oauth-token-scope=https://www.googleapis.com/auth/cloud-platform \
    --service-account="${SERVICE_ACCOUNT_EMAIL}" \
    --headers="Content-Type=application/json" \
    --message-body="{
        \"configuration\": {
            \"query\": {
                \"query\": \"${BIGQUERY_SQL_COMMAND}\",
                \"useLegacySql\": false,
                \"defaultDataset\": {
                    \"projectId\": \"${PROJECT_ID}\",
                    \"datasetId\": \"${BIGQUERY_DATASET}\"
                }
            }
        }
    }"

echo "Cloud Scheduler job '${JOB_NAME}' creation request sent."
echo "Please ensure the service account '${SERVICE_ACCOUNT_EMAIL}' has the 'BigQuery Job User' role on project '${PROJECT_ID}'"
echo "and 'BigQuery Data Editor' if the procedures modify data."
echo "You can verify the job status in the Cloud Scheduler console or by running 'gcloud scheduler jobs describe ${JOB_NAME} --location=${REGION}'."