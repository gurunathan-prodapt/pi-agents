# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_valid.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_valid.ksh
# Target: Script for configuring IAM roles and service accounts

# This script configures the necessary IAM roles for the Cloud Scheduler service account
# to successfully invoke the BigQuery Stored Procedure.
# Replace placeholder values with your actual GCP project ID and desired service account name.

# --- Configuration Variables (REPLACE THESE) ---
PROJECT_ID="your-gcp-project-id" # Your GCP Project ID
# Define a dedicated service account for Cloud Scheduler.
# This enhances security by granting minimal necessary permissions.
SCHEDULER_SERVICE_ACCOUNT_NAME="bert-scheduler-sa"
SCHEDULER_SERVICE_ACCOUNT_EMAIL="${SCHEDULER_SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

echo "Configuring IAM for project: ${PROJECT_ID}"

# 1. Create a dedicated service account for Cloud Scheduler (if it doesn't exist)
echo "Creating service account '${SCHEDULER_SERVICE_ACCOUNT_EMAIL}' (if it does not exist)..."
gcloud iam service-accounts create "${SCHEDULER_SERVICE_ACCOUNT_NAME}" \
    --display-name="Service Account for BERT_V_TA_CNTRCT_VALID Cloud Scheduler" \
    --project="${PROJECT_ID}" \
    --format="json" || true # '|| true' prevents script from failing if SA already exists

# 2. Grant 'BigQuery Job User' role to the scheduler service account
#    This role allows the service account to create and manage BigQuery jobs, including executing stored procedures.
echo "Granting 'BigQuery Job User' role to '${SCHEDULER_SERVICE_ACCOUNT_EMAIL}' on project '${PROJECT_ID}'..."
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SCHEDULER_SERVICE_ACCOUNT_EMAIL}" \
    --role="roles/bigquery.jobUser" \
    --project="${PROJECT_ID}"

# 3. Grant 'BigQuery Data Editor' role if the stored procedures modify data
#    This role is necessary if the called BigQuery Stored Procedures perform DML operations (INSERT, UPDATE, DELETE).
#    If your stored procedures only read data, you might use 'BigQuery Data Viewer' or a custom role.
echo "Granting 'BigQuery Data Editor' role to '${SCHEDULER_SERVICE_ACCOUNT_EMAIL}' on project '${PROJECT_ID}' (if procedures modify data)..."
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SCHEDULER_SERVICE_ACCOUNT_EMAIL}" \
    --role="roles/bigquery.dataEditor" \
    --project="${PROJECT_ID}"

echo "IAM configuration complete."
echo "The service account '${SCHEDULER_SERVICE_ACCOUNT_EMAIL}' can now be used by Cloud Scheduler"
echo "to run BigQuery jobs and execute the defined stored procedures."
echo "Please ensure this service account email is used in your Cloud Scheduler job configuration."