The migration of `r_ausd_v_ta_cntrct_valid.ksh` to Google Cloud Platform (GCP) involves re-platforming an orchestration script to BigQuery Stored Procedures and Cloud Scheduler. The tests below focus on validating the behavioral equivalence of this orchestration layer, particularly its logging, parameter handling, error management, and external system interactions, as the core data transformation logic (`k_ausd_v_ta_cntrct_valid.ksh`) is a separate migration unit.

---

## Migration Validation Tests for `r_ausd_v_ta_cntrct_valid.ksh`

### Test Case 1: Successful Execution and Logging (Output Parity & Transformation Correctness)

**Purpose:** Verify that the migrated BigQuery wrapper stored procedure (`BERT_V_TA_CNTRCT_VALID`) executes successfully, correctly logs a successful job run in the `job_log` table, and calls the core logic placeholder without error. This tests the basic control flow and success logging.

**Setup:**
1.  Ensure the `project.dataset.job_log` table is created.
2.  Ensure the `project.dataset.log_utils_sp` procedures (`generate_job_run_id`, `create_job_log_entry`, `update_job_log_status`) are deployed.
3.  Ensure the `project.dataset.BERT_K_TA_CNTRCT_VALID` placeholder stored procedure is deployed in its *successful* state (i.e., it does not raise an error).
4.  Ensure the `project.dataset.BERT_V_TA_CNTRCT_VALID` wrapper stored procedure is deployed.

**Action:**
Execute the `BERT_V_TA_CNTRCT_VALID` stored procedure with sample parameters.

```sql
-- Replace 'project' and 'dataset' with your actual values
CALL project.dataset.BERT_V_TA_CNTRCT_VALID(
    'TEST_JOB_SUCCESS',
    1001,
    'BERT_V_TA_CNTRCT_VALID',
    'Manual Test'
);
```

**Pass/Fail Criterion:**
1.  The `CALL` statement completes successfully without raising an error.
2.  A new entry exists in the `project.dataset.job_log` table with the following characteristics:
    *   `job_name` = `'BERT_V_TA_CNTRCT_VALID'`
    *   `status` = `'SUCCESS'`
    *   `start_timestamp` and `end_timestamp` are populated, with `end_timestamp` being after `start_timestamp`.
    *   `error_message` is `NULL`.
    *   `parameters_json` contains a valid JSON object reflecting the input parameters (e.g., `{"job_kennung":"TEST_JOB_SUCCESS", "eintragsnr":1001, "program_name":"BERT_V_TA_CNTRCT_VALID", "caller_process":"Manual Test"}`).
3.  Cloud Logging for the BigQuery job shows informational messages indicating the start and successful completion of the core logic.

```sql
-- Pytest assertion example (using a BigQuery client library)
def test_successful_job_execution(bigquery_client):
    job_kennung = 'TEST_JOB_SUCCESS'
    eintragsnr = 1001
    program_name = 'BERT_V_TA_CNTRCT_VALID'
    caller_process = 'Pytest'

    # Get initial job_log count
    initial_count_query = f"SELECT COUNT(*) FROM project.dataset.job_log WHERE job_name = '{program_name}' AND parameters_json.job_kennung = '{job_kennung}'"
    initial_count = bigquery_client.query(initial_count_query).result().to_dataframe().iloc[0, 0]

    # Action: Call the stored procedure
    call_sp_query = f"""
        CALL project.dataset.BERT_V_TA_CNTRCT_VALID(
            '{job_kennung}',
            {eintragsnr},
            '{program_name}',
            '{caller_process}'
        );
    """
    bigquery_client.query(call_sp_query).result() # This will raise an exception if the SP fails

    # Assertions
    final_count_query = f"SELECT * FROM project.dataset.job_log WHERE job_name = '{program_name}' AND parameters_json.job_kennung = '{job_kennung}' ORDER BY start_timestamp DESC LIMIT 1"
    result = bigquery_client.query(final_count_query).result().to_dataframe()

    assert len(result) == initial_count + 1, "Expected one new job log entry."
    assert result['status'].iloc[0] == 'SUCCESS', "Job status should be SUCCESS."
    assert result['error_message'].iloc[0] is None, "Error message should be NULL for success."
    assert result['start_timestamp'].iloc[0] is not None, "Start timestamp should be populated."
    assert result['end_timestamp'].iloc[0] is not None, "End timestamp should be populated."
    assert result['parameters_json'].iloc[0] == f'{{"job_kennung":"{job_kennung}","eintragsnr":{eintragsnr},"program_name":"{program_name}","caller_process":"{caller_process}"}}', "Parameters JSON mismatch."

    # Further checks for Cloud Logging can be done by querying Cloud Logging API
    # (more complex to automate in a simple pytest example, but crucial for manual verification)
```

---

### Test Case 2: Failed Execution and Error Handling (Transformation Correctness)

**Purpose:** Verify that the migrated BigQuery wrapper stored procedure correctly handles errors originating from the core logic, logs the failure, and propagates the error to the caller. This tests the error handling and failure logging.

**Setup:**
1.  Ensure the `project.dataset.job_log` table is created.
2.  Ensure the `project.dataset.log_utils_sp` procedures are deployed.
3.  **Modify** the `project.dataset.BERT_K_TA_CNTRCT_VALID` placeholder stored procedure to simulate a failure (e.g., by adding `SELECT 1 / 0;` or `RAISE USING MESSAGE = 'Simulated core logic error.';`).
4.  Ensure the `project.dataset.BERT_V_TA_CNTRCT_VALID` wrapper stored procedure is deployed.

**Action:**
Execute the `BERT_V_TA_CNTRCT_VALID` stored procedure with sample parameters.

```sql
-- Replace 'project' and 'dataset' with your actual values
-- This call is expected to fail and raise an error.
CALL project.dataset.BERT_V_TA_CNTRCT_VALID(
    'TEST_JOB_FAILURE',
    1002,
    'BERT_V_TA_CNTRCT_VALID',
    'Manual Test'
);
```

**Pass/Fail Criterion:**
1.  The `CALL` statement **must fail** and raise an error, indicating the job's failure to the caller.
2.  A new entry exists in the `project.dataset.job_log` table with the following characteristics:
    *   `job_name` = `'BERT_V_TA_CNTRCT_VALID'`
    *   `status` = `'FAILED'`
    *   `start_timestamp` and `end_timestamp` are populated.
    *   `error_message` is **not** `NULL` and contains details about the simulated error (e.g., "Division by zero" or "Simulated core logic error.").
    *   `parameters_json` correctly reflects the input parameters.
3.  Cloud Logging for the BigQuery job shows error messages, including "ERROR: Core logic failed with error:..." and the specific error details.

```sql
-- Pytest assertion example (using a BigQuery client library)
import pytest

def test_failed_job_execution(bigquery_client):
    job_kennung = 'TEST_JOB_FAILURE'
    eintragsnr = 1002
    program_name = 'BERT_V_TA_CNTRCT_VALID'
    caller_process = 'Pytest'

    # Modify the placeholder to fail (this would typically be done via a separate setup step)
    # For a real test, you'd deploy a failing version of BERT_K_TA_CNTRCT_VALID
    # Example: bigquery_client.query("CREATE OR REPLACE PROCEDURE ... SELECT 1/0;").result()

    # Action: Call the stored procedure, expecting it to raise an exception
    call_sp_query = f"""
        CALL project.dataset.BERT_V_TA_CNTRCT_VALID(
            '{job_kennung}',
            {eintragsnr},
            '{program_name}',
            '{caller_process}'
        );
    """
    with pytest.raises(Exception) as excinfo: # Expecting a BigQuery exception
        bigquery_client.query(call_sp_query).result()

    assert "Job BERT_V_TA_CNTRCT_VALID (Run ID:" in str(excinfo.value), "Wrapper should raise an error message."
    assert "failed. Error:" in str(excinfo.value), "Wrapper should indicate failure and error."

    # Assertions in job_log table
    result_query = f"SELECT * FROM project.dataset.job_log WHERE job_name = '{program_name}' AND parameters_json.job_kennung = '{job_kennung}' ORDER BY start_timestamp DESC LIMIT 1"
    result = bigquery_client.query(result_query).result().to_dataframe()

    assert len(result) == 1, "Expected one new job log entry for the failed run."
    assert result['status'].iloc[0] == 'FAILED', "Job status should be FAILED."
    assert result['error_message'].iloc[0] is not None, "Error message should be populated for failure."
    assert "Simulated core logic error" in result['error_message'].iloc[0] or "division by zero" in result['error_message'].iloc[0], "Error message content mismatch."
    assert result['end_timestamp'].iloc[0] is not None, "End timestamp should be populated even on failure."
```

---

### Test Case 3: Parameter Handling and `parameters_json` (Transformation Correctness)

**Purpose:** Verify that all input parameters to the wrapper stored procedure are correctly captured and stored in the `parameters_json` column of the `job_log` table. This replaces the legacy script's command-line argument parsing.

**Setup:**
1.  Ensure all BigQuery DDL and SPs are deployed, with `BERT_K_TA_CNTRCT_VALID` in its successful state.
2.  Ensure the `project.dataset.job_log` table is empty or you can filter for specific test runs.

**Action:**
Execute the `BERT_V_TA_CNTRCT_VALID` stored procedure with various valid parameter values.

```sql
-- Replace 'project' and 'dataset' with your actual values
CALL project.dataset.BERT_V_TA_CNTRCT_VALID(
    'CONTRACT_VALID_DAILY',
    20231026,
    'BERT_V_TA_CNTRCT_VALID',
    'Cloud Composer DAG'
);

CALL project.dataset.BERT_V_TA_CNTRCT_VALID(
    'CONTRACT_VALID_ADHOC',
    9999,
    'BERT_V_TA_CNTRCT_VALID',
    'Manual Override'
);
```

**Pass/Fail Criterion:**
1.  For each successful `CALL`, a corresponding entry exists in `project.dataset.job_log` with `status = 'SUCCESS'`.
2.  The `parameters_json` column for each entry accurately reflects the exact input parameters provided to the stored procedure.

```sql
-- SQL Assertion
-- Query to verify parameters for 'CONTRACT_VALID_DAILY'
SELECT
    parameters_json,
    JSON_EXTRACT_SCALAR(parameters_json, '$.job_kennung') AS extracted_job_kennung,
    CAST(JSON_EXTRACT_SCALAR(parameters_json, '$.eintragsnr') AS INT64) AS extracted_eintragsnr,
    JSON_EXTRACT_SCALAR(parameters_json, '$.program_name') AS extracted_program_name,
    JSON_EXTRACT_SCALAR(parameters_json, '$.caller_process') AS extracted_caller_process
FROM project.dataset.job_log
WHERE
    job_name = 'BERT_V_TA_CNTRCT_VALID'
    AND JSON_EXTRACT_SCALAR(parameters_json, '$.job_kennung') = 'CONTRACT_VALID_DAILY'
ORDER BY start_timestamp DESC
LIMIT 1;

-- Expected result for the above query:
-- parameters_json: {"job_kennung":"CONTRACT_VALID_DAILY","eintragsnr":20231026,"program_name":"BERT_V_TA_CNTRCT_VALID","caller_process":"Cloud Composer DAG"}
-- extracted_job_kennung: 'CONTRACT_VALID_DAILY'
-- extracted_eintragsnr: 20231026
-- extracted_program_name: 'BERT_V_TA_CNTRCT_VALID'
-- extracted_caller_process: 'Cloud Composer DAG'
```

---

### Test Case 4: Cloud Scheduler Integration (External-system replacements)

**Purpose:** Verify that the Cloud Scheduler job can successfully trigger the BigQuery stored procedure, and that the `caller_process` is correctly recorded. This replaces the legacy script's direct execution or cron job scheduling.

**Setup:**
1.  Ensure all BigQuery DDL and SPs are deployed, with `BERT_K_TA_CNTRCT_VALID` in its successful state.
2.  Run the `iam_config.sh` script to create the dedicated service account and grant necessary IAM roles (`roles/bigquery.jobUser`, `roles/bigquery.dataEditor`).
3.  Run the `scheduler_config.sh` script to create the Cloud Scheduler job, ensuring the `SERVICE_ACCOUNT_EMAIL` matches the one created in `iam_config.sh`.
4.  Ensure the `PROJECT_ID`, `REGION`, and `BIGQUERY_DATASET` variables in `scheduler_config.sh` are correctly set.

**Action:**
1.  Manually trigger the Cloud Scheduler job from the GCP Console or using `gcloud scheduler jobs run <JOB_NAME> --location=<REGION>`.
2.  Wait for the job to complete.

**Pass/Fail Criterion:**
1.  The Cloud Scheduler job execution status in the GCP Console is 'SUCCESS'.
2.  A new entry exists in the `project.dataset.job_log` table with:
    *   `job_name` = `'BERT_V_TA_CNTRCT_VALID'`
    *   `status` = `'SUCCESS'`
    *   `caller_process` = `'Cloud Scheduler'` (or the value configured in `scheduler_config.sh`).
    *   `parameters_json` reflects the dynamic parameters set in `scheduler_config.sh` (e.g., `DYNAMIC_JOB_KENNUNG`).
3.  Cloud Logging shows logs from the BigQuery SP execution, confirming it was triggered.

```bash
# Manual trigger command
gcloud scheduler jobs run bert-v-ta-cntrct-valid-scheduler --location=your-gcp-region

# SQL Assertion (after triggering and waiting for completion)
SELECT
    job_run_id,
    job_name,
    status,
    caller_process,
    parameters_json
FROM project.dataset.job_log
WHERE
    job_name = 'BERT_V_TA_CNTRCT_VALID'
    AND caller_process = 'Cloud Scheduler'
ORDER BY start_timestamp DESC
LIMIT 1;

-- Expected result:
-- job_name: 'BERT_V_TA_CNTRCT_VALID'
-- status: 'SUCCESS'
-- caller_process: 'Cloud Scheduler'
-- parameters_json: {"job_kennung":"SCHEDULER_YYYYMMDD", "eintragsnr":1, "program_name":"BERT_V_TA_CNTRCT_VALID", "caller_process":"Cloud Scheduler"}
-- (where YYYYMMDD is the date of execution)
```

---

### Test Case 5: `job_log` Table Schema and Data Quality (Data-quality / row-count / schema assertions)

**Purpose:** Verify that the `project.dataset.job_log` table schema is correctly defined, and that data types and NULL constraints are handled as expected, ensuring the integrity of the logging mechanism.

**Setup:**
1.  Ensure the `project.dataset.job_log_table_ddl.sql` has been executed to create the table.
2.  Perform Test Case 1 (Successful Execution) and Test Case 2 (Failed Execution) to populate the table with both success and failure entries.

**Action:**
Query the `INFORMATION_SCHEMA` for the table schema and inspect the data for various conditions.

**Pass/Fail Criterion:**
1.  **Schema Match**: The table schema in BigQuery's `INFORMATION_SCHEMA` matches the DDL provided in `job_log_table_ddl.sql` for column names, data types, and nullability.
    *   `job_run_id`: `STRING` (NOT NULL)
    *   `job_name`: `STRING` (NOT NULL)
    *   `start_timestamp`: `TIMESTAMP` (NOT NULL)
    *   `end_timestamp`: `TIMESTAMP` (NULLABLE)
    *   `status`: `STRING` (NOT NULL)
    *   `error_message`: `STRING` (NULLABLE)
    *   `parameters_json`: `JSON` (NULLABLE)
    *   `caller_process`: `STRING` (NULLABLE)
2.  **Partitioning/Clustering**: The table is partitioned by `DATE(start_timestamp)` and clustered by `job_name, status`.
3.  **Data Integrity (from previous tests)**:
    *   For `status = 'RUNNING'` (if observed directly after `create_job_log_entry` before `update_job_log_status`), `end_timestamp` and `error_message` are `NULL`.
    *   For `status = 'SUCCESS'`, `error_message` is `NULL`.
    *   For `status = 'FAILED'`, `error_message` is not `NULL`.
    *   `job_run_id` values are unique and appear to be valid UUIDs.
    *   `parameters_json` contains valid JSON.

```sql
-- SQL Assertion: Verify schema
SELECT
    column_name,
    data_type,
    is_nullable
FROM
    project.dataset.INFORMATION_SCHEMA.COLUMNS
WHERE
    table_name = 'job_log'
ORDER BY
    ordinal_position;

-- Expected output for schema verification:
-- column_name       data_type   is_nullable
-- job_run_id        STRING      NO
-- job_name          STRING      NO
-- start_timestamp   TIMESTAMP   NO
-- end_timestamp     TIMESTAMP   YES
-- status            STRING      NO
-- error_message     STRING      YES
-- parameters_json   JSON        YES
-- caller_process    STRING      YES

-- SQL Assertion: Verify partitioning and clustering
SELECT
    partitioning_column,
    partitioning_type,
    clustering_columns
FROM
    project.dataset.INFORMATION_SCHEMA.TABLE_OPTIONS
WHERE
    table_name = 'job_log'
    AND option_name = 'partitioning_column' OR option_name = 'clustering_columns';

-- Expected output for partitioning/clustering:
-- partitioning_column: start_timestamp
-- partitioning_type: DAY
-- clustering_columns: ["job_name", "status"]

-- SQL Assertion: Verify NULL handling for success/failure
SELECT
    status,
    COUNTIF(end_timestamp IS NULL) AS null_end_timestamp_count,
    COUNTIF(error_message IS NULL) AS null_error_message_count,
    COUNT(*) AS total_count
FROM project.dataset.job_log
WHERE job_name = 'BERT_V_TA_CNTRCT_VALID' -- Filter for relevant job runs
GROUP BY status;

-- Expected output for NULL handling (assuming at least one success and one failure run):
-- status    null_end_timestamp_count    null_error_message_count    total_count
-- SUCCESS   0                           [count_of_success_runs]     [count_of_success_runs]
-- FAILED    0                           0                           [count_of_failure_runs]
```

---

### Test Case 6: Environment Variable Replacement (Secret Manager)

**Purpose:** Verify that the Secret Manager setup for replacing legacy `.dw_init` environment variables is functional. While the BigQuery SP itself doesn't directly consume secrets, this test ensures the mechanism for storing and retrieving sensitive configuration is in place.

**Setup:**
1.  Ensure the GCP project is correctly configured.
2.  Run the `secret_manager_config.sh` script to create the secret `bert-db-password` and add a version.

**Action:**
Attempt to retrieve the secret value using the `gcloud` CLI.

```bash
# Replace 'your-gcp-project-id' with your actual project ID
gcloud secrets versions access latest --secret="bert-db-password" --project="your-gcp-project-id"
```

**Pass/Fail Criterion:**
1.  The command executes successfully.
2.  The output of the command is `my-super-secret-value` (or whatever value was configured in `secret_manager_config.sh`).
3.  The secret exists in Secret Manager and its metadata (labels, description) matches the configuration script.

```bash
# Pytest assertion example (using gcloud CLI via subprocess)
import subprocess
import json

def test_secret_manager_configuration(gcp_project_id):
    secret_name = "bert-db-password"
    expected_value = "my-super-secret-value"

    # Verify secret exists and can be accessed
    try:
        result = subprocess.run(
            ['gcloud', 'secrets', 'versions', 'access', 'latest',
             '--secret', secret_name, '--project', gcp_project_id],
            capture_output=True, text=True, check=True
        )
        assert result.stdout.strip() == expected_value, "Secret value retrieved does not match expected value."
    except subprocess.CalledProcessError as e:
        pytest.fail(f"Failed to access secret '{secret_name}': {e.stderr}")

    # Optionally, verify secret metadata
    try:
        metadata_result = subprocess.run(
            ['gcloud', 'secrets', 'describe', secret_name, '--project', gcp_project_id, '--format=json'],
            capture_output=True, text=True, check=True
        )
        metadata = json.loads(metadata_result.stdout)
        assert metadata['labels']['job'] == 'bert-v-ta-cntrct-valid', "Secret label 'job' mismatch."
        assert metadata['labels']['environment'] == 'prod', "Secret label 'environment' mismatch."
        assert "Sensitive configuration value" in metadata['replication']['automatic']['customerManagedEncryption']['kmsKeyName'], "Secret description mismatch." # This line is incorrect, description is a separate field.
        # Corrected:
        assert "Sensitive configuration value" in metadata['description'], "Secret description mismatch."

    except subprocess.CalledProcessError as e:
        pytest.fail(f"Failed to describe secret '{secret_name}': {e.stderr}")
```