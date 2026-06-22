# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_valid.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_valid.ksh
# Target: Script for integrating with Google Secret Manager

# This script demonstrates how to create and manage secrets in Google Secret Manager.
# It addresses the migration of sensitive environment variables from legacy .dw_init files.
# Replace placeholder values with your actual GCP project ID, secret name, and secret value.

# --- Configuration Variables (REPLACE THESE) ---
PROJECT_ID="your-gcp-project-id"   # Your GCP Project ID
SECRET_NAME="bert-db-password"     # Name for your secret (e.g., a database password or API key)
SECRET_VALUE="my-super-secret-value" # The actual sensitive value to store

echo "Configuring Google Secret Manager for project: ${PROJECT_ID}"

# Create the secret (if it doesn't exist already)
# The '|| true' allows the script to continue if the secret already exists.
echo "Attempting to create secret '${SECRET_NAME}'..."
gcloud secrets create "${SECRET_NAME}" \
    --project="${PROJECT_ID}" \
    --replication-policy="automatic" \
    --labels="job=bert-v-ta-cntrct-valid,environment=prod" \
    --description="Sensitive configuration value for BERT_V_TA_CNTRCT_VALID job, formerly in .dw_init" \
    --format="json" || true

# Add a new version with the secret value
# Using --data-file=- allows piping the value directly to avoid exposing it in shell history
echo "Adding new version to secret '${SECRET_NAME}'..."
echo "${SECRET_VALUE}" | gcloud secrets add-version "${SECRET_NAME}" \
    --project="${PROJECT_ID}" \
    --data-file=- \
    --format="json"

echo "Secret '${SECRET_NAME}' created/updated successfully in Secret Manager."
echo ""
echo "To grant a service account (e.g., Cloud Functions, Cloud Run, or a BigQuery data processing job's SA) access to this secret, run:"
echo "  gcloud secrets add-iam-policy-binding '${SECRET_NAME}' --project='${PROJECT_ID}' \\"
echo "    --role='roles/secretmanager.secretAccessor' \\"
echo "    --member='serviceAccount:your-service-account-email'"
echo ""
echo "Note: BigQuery Stored Procedures cannot directly access Secret Manager. Typically, a Cloud Function or Cloud Workflow"
echo "would read the secret and pass it as a parameter if needed, or if the secret is for connecting to an external system."