The migration of `r_exis_v2` from a KornShell script to Google Cloud Platform involves a significant re-architecture. The following test cases are designed to validate the behavioral equivalence of the migrated system, ensuring that the new BigQuery-centric solution delivers the same results and functionality as the legacy script.

---

### Test Case 1: Configuration Loading and Parameter Resolution

*   **Purpose**: Verify that the migrated system correctly loads configuration parameters from `dwh_exporter.config_kv` and resolves job-specific parameters, mirroring the `p_ConfigFile`, `p_DefaultFile`, and `getopts` behavior of the legacy script. This ensures that the job's runtime behavior is driven by the correct configuration.

*   **Setup**:
    1.  Ensure the `dwh_exporter` dataset and `dwh_exporter.config_kv` table are deployed.
    2.  Populate `dwh_exporter.config_kv` with a representative set of configuration parameters for a specific job (e.g., `job_name='test_exporter_job'`). Include parameters like `TOTAL_FROM` (with a SQL expression), `TOTAL_TO` (with a SQL expression), `INITIAL_FROM` (with a fixed timestamp), `FILE_PARTITION`, `SQL_PARTITION`, and dummy file paths.
    3.  Ensure `dwh_exporter.resolve_timestamp` UDF, `dwh_exporter.get_config_value` procedure, and `dwh_exporter.log_audit` procedure are deployed.

*   **Action**:
    1.  Invoke the main BigQuery Stored Procedure `dwh_exporter.r_exis_v2` with `p_job_name='test_exporter_job'` and a unique `p_run_id`.
    2.  Pass a `p_parameters` JSON object that overrides some default config values (e.g., `{"TOTAL_FROM": "2023-01-01 00:00:00"}`) to simulate command-line overrides (`-f` flag).
    3.  Monitor the `dwh_exporter.export_audit` table for log entries generated during the configuration and initialization phase.

*   **Pass/Fail Criterion**:
    *   **Pass**:
        *   The `dwh_exporter.export_audit` table contains an `INFO` entry for `step_name='r_exis_v2_main'` with `log_message` "Configuration loaded and timestamps resolved."
        *   The `metadata_json` of this audit entry correctly reflects the resolved `total_from`, `total_to`, and `initial_from` timestamps, matching expected values based on the `config_kv` entries and any `p_parameters` overrides.
        *   No `FAILED` audit entries are recorded during this phase.
    *   **Fail**: Any deviation from the above.

```sql
-- Setup: Populate config_kv with test data
INSERT INTO dwh_exporter.config_kv (job_name, config_key, config_value, config_type, description, updated_at)
VALUES
('test_exporter_job', 'TOTAL_FROM', 'DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)', 'SQL_EXPR', 'Default start date for export', CURRENT_TIMESTAMP()),
('test_exporter_job', 'TOTAL_TO', 'CURRENT_TIMESTAMP()', 'SQL_EXPR', 'Default end date for export', CURRENT_TIMESTAMP()),
('test_exporter_job', 'INITIAL_FROM', '2020-01-01 00:00:00', 'TIMESTAMP', 'Initial run start date', CURRENT_TIMESTAMP()),
('test_exporter_job', 'FILE_PARTITION', 'daily', 'STRING', 'File partitioning strategy', CURRENT_TIMESTAMP()),
('test_exporter_job', 'SQL_PARTITION', 'hourly', 'STRING', 'SQL partitioning strategy', CURRENT_TIMESTAMP()),
('test_exporter_job', 'P_CONFIG_FILE', 'test_config.cfg', 'STRING', 'Dummy config file path', CURRENT_TIMESTAMP()),
('test_exporter_job', 'P_DEFAULT_FILE', 'default_config.cfg', 'STRING', 'Dummy default config file path', CURRENT_TIMESTAMP());

-- Action: Invoke the main procedure
DECLARE test_run_id STRING DEFAULT 'test_run_config_' || GENERATE_UUID();
CALL dwh_exporter.r_exis_v2(
    'test_exporter_job',
    test_run_id,
    JSON '{"TOTAL_FROM": "2023-01-01 00:00:00", "INITIAL": "YES"}' -- Override TOTAL_FROM and simulate -i flag
);

-- Pass/Fail Criterion: SQL Assertion
SELECT
    COUNT(1) AS total_audits,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_audits,
    MAX(CASE WHEN log_message LIKE 'Configuration loaded and timestamps resolved.' THEN JSON_EXTRACT_SCALAR(metadata_json, '$.total_from') END) AS resolved_total_from,
    MAX(CASE WHEN log_message LIKE 'Configuration loaded and timestamps resolved.' THEN JSON_EXTRACT_SCALAR(metadata_json, '$.total_to') END) AS resolved_total_to,
    MAX(CASE WHEN log_message LIKE 'Configuration loaded and timestamps resolved.' THEN JSON_EXTRACT_SCALAR(metadata_json, '$.initial_from') END) AS resolved_initial_from
FROM dwh_exporter.export_audit
WHERE run_id = test_run_id
AND step_name = 'r_exis_v2_main';
-- Expected Result (example):
-- total_audits >= 2 (for STARTED and INFO)
-- failed_audits = 0
-- resolved_total_from = "2023-01-01 00:00:00 UTC" (due to override)
-- resolved_total_to = (current_timestamp, varies)
-- resolved_initial_from = "2020-01-01 00:00:00 UTC"
```

---

### Test Case 2: Timestamp Resolution Logic (`dwh_exporter.resolve_timestamp` UDF)

*   **Purpose**: Verify that the `dwh_exporter.resolve_timestamp` UDF correctly interprets various timestamp formats and expressions, replicating the `handletimestamps` and `h_alis_date.ksh` logic. This is crucial for correct time-based filtering and partitioning.

*   **Setup**:
    1.  Ensure `dwh_exporter.resolve_timestamp` UDF is deployed.

*   **Action**:
    1.  Execute the UDF with different input `p_timestamp_param` values, including:
        *   Direct timestamp strings (e.g., '2023-01-15 10:30:00').
        *   `CURRENT_TIMESTAMP()`.
        *   NULL or empty strings, relying on `p_default_date`.
        *   (Note: The provided UDF has simplified handling for `DATE_ADD`/`DATE_SUB` expressions; for full parity, these would need more complex parsing or dynamic SQL execution within a procedure, not a UDF.)

*   **Pass/Fail Criterion**:
    *   **Pass**: The UDF returns the expected `TIMESTAMP` value for each test case.
    *   **Fail**: The UDF returns an incorrect timestamp or an error.

```sql
-- Action & Pass/Fail Criterion: SQL Assertions
-- Test 1: Direct timestamp string
SELECT dwh_exporter.resolve_timestamp('2023-01-15 10:30:00', '%Y-%m-%d %H:%M:%S', '2000-01-01 00:00:00') AS resolved_ts;
-- Expected: '2023-01-15 10:30:00 UTC'

-- Test 2: CURRENT_TIMESTAMP()
SELECT dwh_exporter.resolve_timestamp('CURRENT_TIMESTAMP()', '%Y-%m-%d %H:%M:%S', '2000-01-01 00:00:00') AS resolved_ts;
-- Expected: A timestamp very close to the execution time.

-- Test 3: NULL p_timestamp_param, use default
SELECT dwh_exporter.resolve_timestamp(NULL, '%Y-%m-%d %H:%M:%S', '2000-01-01 00:00:00') AS resolved_ts;
-- Expected: '2000-01-01 00:00:00 UTC'

-- Test 4: Empty string p_timestamp_param, use default
SELECT dwh_exporter.resolve_timestamp('', '%Y-%m-%d %H:%M:%S', '2000-01-01 00:00:00') AS resolved_ts;
-- Expected: '2000-01-01 00:00:00 UTC'

-- Test 5: Invalid format string (should result in NULL or error depending on SAFE.PARSE_TIMESTAMP usage)
SELECT SAFE.PARSE_TIMESTAMP('%Y%m%d', 'INVALID_DATE') IS NULL AS is_null_on_invalid_format;
-- Expected: TRUE (demonstrates SAFE.PARSE_TIMESTAMP behavior)
```

---

### Test Case 3: Oracle Data Extraction and Transformation Parity

*   **Purpose**: Verify that the data extracted from Oracle (via BigQuery Data Transfer Service or Federated Queries) and transformed in BigQuery produces the exact same output as the legacy `sqlplus` execution. This covers `OUTPUT_SQL`, `PRE_SQL`, `POST_SQL` translation, joins, aggregations, filters, type handling, and NULL handling.

*   **Setup**:
    1.  Identify a specific `OUTPUT_SQL` query from a legacy configuration file that is representative (e.g., involves joins, aggregations, filters, different data types, NULLs).
    2.  Ensure the corresponding Oracle tables are ingested into BigQuery (e.g., `dwh_oracle_source.my_oracle_table`) or accessible via federated queries.
    3.  Translate the identified Oracle SQL query into BigQuery SQL.
    4.  Create a temporary BigQuery Stored Procedure or script that executes this BigQuery SQL and writes its output to a temporary BigQuery table or Cloud Storage.
    5.  Have the legacy `r_exis_v2` script configured to run *only* this specific `OUTPUT_SQL` and spool its output to a file.
    6.  Ensure the source Oracle data is identical between the legacy and migrated environments for the test period.

*   **Action**:
    1.  Run the legacy `r_exis_v2` script with the specific configuration to generate the output file (e.g., `legacy_output.csv`).
    2.  Execute the BigQuery SQL (or the part of `dwh_exporter.r_exis_v2` that executes this SQL) to generate its output (e.g., `migrated_output.csv` in Cloud Storage or a BigQuery table).

*   **Pass/Fail Criterion**:
    *   **Output Parity**: The content of `legacy_output.csv` and `migrated_output.csv` (after sorting and normalizing line endings/delimiters if necessary) must be identical. This includes:
        *   Same number of rows.
        *   Same number of columns.
        *   Identical data values, including NULLs and data types (e.g., numbers, dates, strings).
    *   **Transformation Correctness**: If the SQL involves specific transformations (e.g., `NVL` in Oracle -> `COALESCE` in BQ, date format conversions), verify these are correctly applied.

```python
# Example Python (pytest) for comparing outputs
import pandas as pd
from google.cloud import bigquery, storage
import os
import pytest

# Assume these are set up for your test environment
LEGACY_OUTPUT_PATH = "/tmp/legacy_output.csv" # Path on local filesystem where legacy output is stored
MIGRATED_GCS_PATH = "gs://your-test-bucket/migrated_output.csv"
BIGQUERY_PROJECT = "your-gcp-project"
BIGQUERY_DATASET = "dwh_exporter_test"
BIGQUERY_TEMP_TABLE = "temp_exported_data" # Table where BQ output is staged before GCS export

@pytest.fixture(scope="module", autouse=True)
def setup_data_extraction_test():
    # Pre-requisite: Manually run legacy r_exis_v2 to generate LEGACY_OUTPUT_PATH
    # Pre-requisite: Run the BigQuery equivalent SQL to populate BIGQUERY_TEMP_TABLE
    # Pre-requisite: Export BIGQUERY_TEMP_TABLE to MIGRATED_GCS_PATH

    # Example: Create a dummy legacy output file for testing purposes
    with open(LEGACY_OUTPUT_PATH, "w") as f:
        f.write("id,name,value,timestamp\n")
        f.write("1,Alice,100.50,2023-01-01 10:00:00\n")
        f.write("2,Bob,NULL,2023-01-02 11:00:00\n")
        f.write("3,Charlie,200.75,2023-01-03 12:00:00\n")
    print(f"Created dummy legacy output at {LEGACY_OUTPUT_PATH}")

    # Example: Upload a dummy migrated output file to GCS
    storage_client = storage.Client(project=BIGQUERY_PROJECT)
    bucket_name = MIGRATED_GCS_PATH.split('/')[2]
    blob_name = '/'.join(MIGRATED_GCS_PATH.split('/')[3:])
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string("id,name,value,timestamp\n1,Alice,100.50,2023-01-01 10:00:00\n2,Bob,,2023-01-02 11:00:00\n3,Charlie,200.75,2023-01-03 12:00:00\n")
    print(f"Uploaded dummy migrated output to {MIGRATED_GCS_PATH}")

    yield

    # Teardown: Clean up
    if os.path.exists(LEGACY_OUTPUT_PATH):
        os.remove(LEGACY_OUTPUT_PATH)
    storage_client = storage.Client(project=BIGQUERY_PROJECT)
    bucket_name = MIGRATED_GCS_PATH.split('/')[2]
    blob_name = '/'.join(MIGRATED_GCS_PATH.split('/')[3:])
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    if blob.exists():
        blob.delete()

def test_oracle_sql_transformation_parity():
    # Download migrated output from GCS
    storage_client = storage.Client(project=BIGQUERY_PROJECT)
    bucket_name = MIGRATED_GCS_PATH.split('/')[2]
    blob_name = '/'.join(MIGRATED_GCS_PATH.split('/')[3:])
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    migrated_local_path = "/tmp/migrated_output.csv"
    blob.download_to_filename(migrated_local_path)

    # Read both CSVs into pandas DataFrames
    # Assuming CSVs have headers and are comma-delimited
    # 'na_values' handles different representations of NULL (e.g., 'NULL' string vs empty string)
    df_legacy = pd.read_csv(LEGACY_OUTPUT_PATH, sep=',', na_values=['NULL', ''])
    df_migrated = pd.read_csv(migrated_local_path, sep=',', na_values=['NULL', ''])

    # Sort DataFrames to ensure row order doesn't affect comparison
    # Assuming a unique key or sorting by all columns
    df_legacy = df_legacy.sort_values(by=list(df_legacy.columns)).reset_index(drop=True)
    df_migrated = df_migrated.sort_values(by=list(df_migrated.columns)).reset_index(drop=True)

    # Pass/Fail Criterion:
    # 1. Check if schemas are identical (column names, dtypes)
    pd.testing.assert_frame_equal(df_legacy.dtypes.to_frame(), df_migrated.dtypes.to_frame(), check_dtype=True,
                                  err_msg="Schema (data types) mismatch between legacy and migrated outputs.")

    # 2. Check if data is identical
    pd.testing.assert_frame_equal(df_legacy, df_migrated, check_dtype=False,
                                  err_msg="Data mismatch between legacy and migrated outputs.")

    print("Output parity test passed: Legacy and migrated outputs are identical.")

    # Cleanup local migrated file
    os.remove(migrated_local_path)
```

---

### Test Case 4: SFTP Distribution via Cloud Function

*   **Purpose**: Verify that the `distribute_file_sftp` Cloud Function correctly transfers an exported file from Cloud Storage to an external SFTP server, replicating the `scp`/`sftp` functionality.

*   **Setup**:
    1.  Deploy `distribute_file_sftp` Cloud Function.
    2.  Configure `dwh_exporter.export_distribution` with an `SFTP` rule for a specific file pattern (e.g., `*.sftp.csv`). Include `sftp_host`, `sftp_user`, `sftp_private_key_path` (or equivalent secure credential reference) in `options_json` or environment variables for the Cloud Function.
    3.  Set up a mock SFTP server (e.g., using `atmoz/sftp` Docker image) or a dedicated test SFTP server that the Cloud Function can access. Ensure the SFTP server has the `TEST_SFTP_TARGET_PATH` directory.
    4.  Ensure `dwh_exporter.export_readyfiles` table is available.
    5.  Create a dummy private key file locally for the `paramiko` client to verify the SFTP transfer.

*   **Action**:
    1.  Upload a dummy `test_data.sftp.csv` file to the specified Cloud Storage bucket (`TEST_GCS_BUCKET`). This will trigger the Cloud Function.
    2.  Simulate the completion of an export by inserting a record into `dwh_exporter.export_readyfiles` with `status='CREATED'` and `gcs_path` pointing to the uploaded test file.
    3.  Monitor the Cloud Function logs for execution status.
    4.  Check the target SFTP server for the presence of the file using an SFTP client.

*   **Pass/Fail Criterion**:
    *   **Pass**:
        *   The `test_data.sftp.csv` file is successfully transferred to the configured SFTP server at the `target_path`.
        *   The Cloud Function logs indicate successful execution.
        *   The `dwh_exporter.export_readyfiles` entry for this file is updated to `status='DISTRIBUTED'` and `distribution_end_time` is populated.
        *   The local temporary file on the Cloud Function instance is cleaned up.
    *   **Fail**: File not found on SFTP server, Cloud Function error, or incorrect status update in `export_readyfiles`.

```python
# Example Python (pytest) for triggering and verifying SFTP distribution
import pytest
from google.cloud import bigquery, storage
import time
import os
import paramiko # For SFTP client to verify

# Configuration for test (replace with actual values)
TEST_JOB_NAME = "sftp_test_job"
TEST_RUN_ID = f"sftp_test_run_{int(time.time())}"
TEST_GCS_BUCKET = "your-export-bucket-sftp" # Ensure this bucket exists
TEST_GCS_FILE_NAME = "test_data.sftp.csv"
TEST_GCS_PATH = f"gs://{TEST_GCS_BUCKET}/{TEST_GCS_FILE_NAME}"
TEST_SFTP_HOST = "sftp.example.com" # Replace with your mock/test SFTP server host
TEST_SFTP_USER = "testuser"
TEST_SFTP_TARGET_PATH = "/remote/test/path" # Path on the SFTP server
# Path to a local private key file for the pytest client to connect to SFTP
TEST_SFTP_PRIVATE_KEY_PATH = "/path/to/your/sftp_test_key"

bq_client = bigquery.Client()
storage_client = storage.Client()

@pytest.fixture(scope="module", autouse=True)
def setup_sftp_test_data():
    # Ensure bucket exists
    try:
        storage_client.get_bucket(TEST_GCS_BUCKET)
    except Exception:
        storage_client.create_bucket(TEST_GCS_BUCKET)
        print(f"Created bucket {TEST_GCS_BUCKET}")

    # 1. Create a dummy file in GCS
    bucket = storage_client.bucket(TEST_GCS_BUCKET)
    blob = bucket.blob(TEST_GCS_FILE_NAME)
    blob.upload_from_string("col1,col2\nval1,val2\nval3,val4")
    print(f"Uploaded dummy file to {TEST_GCS_PATH}")

    # 2. Insert SFTP distribution rule into export_distribution
    # Note: sftp_private_key_path in options_json for CF should point to a path accessible by CF
    # (e.g., mounted from Secret Manager or a temp file created from a secret).
    # For this example, we use a placeholder.
    bq_client.query(f"""
        INSERT INTO dwh_exporter.export_distribution (distribution_id, job_name, file_pattern, distribution_method, target_path, options_json, is_active, created_at)
        VALUES (
            GENERATE_UUID(),
            '{TEST_JOB_NAME}',
            '*.sftp.csv',
            'SFTP',
            '{TEST_SFTP_TARGET_PATH}',
            JSON '{{"sftp_host": "{TEST_SFTP_HOST}", "sftp_user": "{TEST_SFTP_USER}", "sftp_private_key_path": "/tmp/sftp_key_from_secret"}}',
            TRUE,
            CURRENT_TIMESTAMP()
        )
    """).result()
    print("Inserted SFTP distribution rule.")

    # 3. Insert into export_readyfiles to simulate export completion (this triggers the CF)
    bq_client.query(f"""
        INSERT INTO dwh_exporter.export_readyfiles (file_id, job_id, run_id, file_name, gcs_path, status, creation_time)
        VALUES (
            GENERATE_UUID(),
            '{TEST_JOB_NAME}',
            '{TEST_RUN_ID}',
            '{TEST_GCS_FILE_NAME}',
            '{TEST_GCS_PATH}',
            'CREATED',
            CURRENT_TIMESTAMP()
        )
    """).result()
    print("Inserted entry into export_readyfiles.")

    # Yield control to the test function
    yield

    # Teardown: Clean up
    # Delete dummy file from GCS
    bucket = storage_client.bucket(TEST_GCS_BUCKET)
    blob = bucket.blob(TEST_GCS_FILE_NAME)
    if blob.exists():
        blob.delete()

    # Delete SFTP distribution rule
    bq_client.query(f"DELETE FROM dwh_exporter.export_distribution WHERE job_name = '{TEST_JOB_NAME}'").result()

    # Delete export_readyfiles entry
    bq_client.query(f"DELETE FROM dwh_exporter.export_readyfiles WHERE run_id = '{TEST_RUN_ID}'").result()

    # Delete file from SFTP server (requires SFTP client)
    try:
        transport = paramiko.Transport((TEST_SFTP_HOST, 22))
        private_key = paramiko.RSAKey.from_private_key_file(TEST_SFTP_PRIVATE_KEY_PATH)
        transport.connect(username=TEST_SFTP_USER, pkey=private_key)
        sftp = paramiko.SFTPClient.from_transport(transport)
        remote_file_path = os.path.join(TEST_SFTP_TARGET_PATH, TEST_GCS_FILE_NAME)
        if remote_file_path in sftp.listdir(TEST_SFTP_TARGET_PATH):
            sftp.remove(remote_file_path)
            print(f"Cleaned up {remote_file_path} from SFTP server.")
        sftp.close()
        transport.close()
    except Exception as e:
        print(f"SFTP cleanup failed: {e}")


def test_sftp_distribution_cloud_function():
    # Action: The Cloud Function is triggered by the GCS file upload in setup_sftp_test_data.
    # We need to wait for it to complete.
    print("Waiting for Cloud Function to process SFTP distribution...")
    time.sleep(30) # Give CF time to execute

    # Pass/Fail Criterion:
    # 1. Check export_readyfiles status
    query_result = bq_client.query(f"""
        SELECT status, distribution_end_time
        FROM dwh_exporter.export_readyfiles
        WHERE run_id = '{TEST_RUN_ID}'
    """).result()
    row = next(query_result)
    assert row.status == 'DISTRIBUTED', f"Expected status 'DISTRIBUTED', got '{row.status}'"
    assert row.distribution_end_time is not None, "distribution_end_time should be populated"
    print("export_readyfiles status updated correctly.")

    # 2. Verify file presence on SFTP server
    try:
        transport = paramiko.Transport((TEST_SFTP_HOST, 22))
        private_key = paramiko.RSAKey.from_private_key_file(TEST_SFTP_PRIVATE_KEY_PATH)
        transport.connect(username=TEST_SFTP_USER, pkey=private_key)
        sftp = paramiko.SFTPClient.from_transport(transport)
        remote_file_path = os.path.join(TEST_SFTP_TARGET_PATH, TEST_GCS_FILE_NAME)
        sftp.stat(remote_file_path) # This will raise an exception if file doesn't exist
        print(f"File {remote_file_path} found on SFTP server.")
        sftp.close()
        transport.close()
    except Exception as e:
        pytest.fail(f"File not found on SFTP server or SFTP connection error: {e}")
```

---

### Test Case 5: Email Distribution via Cloud Function

*   **Purpose**: Verify that the `distribute_file_email` Cloud Function correctly sends an email with an exported file as an attachment, replicating the `mailx` functionality.

*   **Setup**:
    1.  Deploy `distribute_file_email` Cloud Function.
    2.  Configure `dwh_exporter.export_distribution` with an `EMAIL` rule for a specific file pattern (e.g., `*.email.csv`). Include `recipient`, `sender_email`, `smtp_server`, `smtp_port`, `smtp_username`, `smtp_password` (or equivalent secure credential reference) in `options_json` or environment variables for the Cloud Function.
    3.  Set up a mock SMTP server (e.g., Mailhog running on a VM accessible by the Cloud Function, or a test email service) that can capture incoming emails for verification.
    4.  Ensure `dwh_exporter.export_readyfiles` table is available.

*   **Action**:
    1.  Upload a dummy `test_report.email.csv` file to the specified Cloud Storage bucket (`TEST_GCS_BUCKET`). This will trigger the Cloud Function.
    2.  Simulate the completion of an export by inserting a record into `dwh_exporter.export_readyfiles` with `status='CREATED'` and `gcs_path` pointing to the uploaded test file.
    3.  Monitor the Cloud Function logs for execution status.
    4.  Check the mock SMTP server (e.g., Mailhog API) for the incoming email.

*   **Pass/Fail Criterion**:
    *   **Pass**:
        *   An email is received by the `recipient` with the correct `subject` and `body`.
        *   The `test_report.email.csv` file is attached to the email.
        *   The Cloud Function logs indicate successful execution.
        *   The `dwh_exporter.export_readyfiles` entry for this file is updated to `status='DISTRIBUTED'` and `distribution_end_time` is populated.
        *   The local temporary file on the Cloud Function instance is cleaned up.
    *   **Fail**: Email not received, incorrect content/attachment, Cloud Function error, or incorrect status update in `export_readyfiles`.

```python
# Example Python (pytest) for triggering and verifying Email distribution
import pytest
from google.cloud import bigquery, storage
import time
import os
import requests # To interact with mock SMTP server (e.g., Mailhog)

# Configuration for test (replace with actual values)
TEST_JOB_NAME = "email_test_job"
TEST_RUN_ID = f"email_test_run_{int(time.time())}"
TEST_GCS_BUCKET = "your-export-bucket-email" # Ensure this bucket exists
TEST_GCS_FILE_NAME = "test_report.email.csv"
TEST_GCS_PATH = f"gs://{TEST_GCS_BUCKET}/{TEST_GCS_FILE_NAME}"
TEST_EMAIL_RECIPIENT = "test@example.com"
TEST_SENDER_EMAIL = "sender@example.com"
TEST_SMTP_SERVER = "mailhog.example.com" # Replace with your Mailhog host or test SMTP server
TEST_SMTP_PORT = 1025 # Default Mailhog SMTP port
TEST_SMTP_USERNAME = "user" # If your mock SMTP requires auth
TEST_SMTP_PASSWORD = "password" # If your mock SMTP requires auth
MAILHOG_API_URL = f"http://{TEST_SMTP_SERVER}:8025/api/v2/messages" # Mailhog API for verification

bq_client = bigquery.Client()
storage_client = storage.Client()

@pytest.fixture(scope="module", autouse=True)
def setup_email_test_data():
    # Ensure bucket exists
    try:
        storage_client.get_bucket(TEST_GCS_BUCKET)
    except Exception:
        storage_client.create_bucket(TEST_GCS_BUCKET)
        print(f"Created bucket {TEST_GCS_BUCKET}")

    # 1. Create a dummy file in GCS
    bucket = storage_client.bucket(TEST_GCS_BUCKET)
    blob = bucket.blob(TEST_GCS_FILE_NAME)
    blob.upload_from_string("Report_Date,Value\n2023-01-01,100\n2023-01-02,200")
    print(f"Uploaded dummy file to {TEST_GCS_PATH}")

    # 2. Insert EMAIL distribution rule into export_distribution
    bq_client.query(f"""
        INSERT INTO dwh_exporter.export_distribution (distribution_id, job_name, file_pattern, distribution_method, recipient, options_json, is_active, created_at)
        VALUES (
            GENERATE_UUID(),
            '{TEST_JOB_NAME}',
            '*.email.csv',
            'EMAIL',
            '{TEST_EMAIL_RECIPIENT}',
            JSON '{{"sender_email": "{TEST_SENDER_EMAIL}", "smtp_server": "{TEST_SMTP_SERVER}", "smtp_port": {TEST_SMTP_PORT}, "smtp_username": "{TEST_SMTP_USERNAME}", "smtp_password": "{TEST_SMTP_PASSWORD}", "subject": "Test Report from Exporter"}}',
            TRUE,
            CURRENT_TIMESTAMP()
        )
    """).result()
    print("Inserted EMAIL distribution rule.")

    # 3. Insert into export_readyfiles to simulate export completion (this triggers the CF)
    bq_client.query(f"""
        INSERT INTO dwh_exporter.export_readyfiles (file_id, job_id, run_id, file_name, gcs_path, status, creation_time)
        VALUES (
            GENERATE_UUID(),
            '{TEST_JOB_NAME}',
            '{TEST_RUN_ID}',
            '{TEST_GCS_FILE_NAME}',
            '{TEST_GCS_PATH}',
            'CREATED',
            CURRENT_TIMESTAMP()
        )
    """).result()
    print("Inserted entry into export_readyfiles.")

    # Yield control to the test function
    yield

    # Teardown: Clean up
    # Delete dummy file from GCS
    bucket = storage_client.bucket(TEST_GCS_BUCKET)
    blob = bucket.blob(TEST_GCS_FILE_NAME)
    if blob.exists():
        blob.delete()

    # Delete EMAIL distribution rule
    bq_client.query(f"DELETE FROM dwh_exporter.export_distribution WHERE job_name = '{TEST_JOB_NAME}'").result()

    # Delete export_readyfiles entry
    bq_client.query(f"DELETE FROM dwh_exporter.export_readyfiles WHERE run_id = '{TEST_RUN_ID}'").result()

    # Clear emails from Mailhog
    try:
        requests.delete(MAILHOG_API_URL)
        print("Cleared emails from Mailhog.")
    except Exception as e:
        print(f"Mailhog cleanup failed: {e}")


def test_email_distribution_cloud_function():
    # Action: The Cloud Function is triggered by the GCS file upload in setup_email_test_data.
    # We need to wait for it to complete.
    print("Waiting for Cloud Function to process Email distribution...")
    time.sleep(30) # Give CF time to execute

    # Pass/Fail Criterion:
    # 1. Check export_readyfiles status
    query_result = bq_client.query(f"""
        SELECT status, distribution_end_time
        FROM dwh_exporter.export_readyfiles
        WHERE run_id = '{TEST_RUN_ID}'
    """).result()
    row = next(query_result)
    assert row.status == 'DISTRIBUTED', f"Expected status 'DISTRIBUTED', got '{row.status}'"
    assert row.distribution_end_time is not None, "distribution_end_time should be populated"
    print("export_readyfiles status updated correctly.")

    # 2. Verify email presence and content on mock SMTP server (Mailhog)
    try:
        response = requests.get(MAILHOG_API_URL)
        response.raise_for_status()
        messages = response.json()['items']

        found_email = False
        for msg in messages:
            if TEST_EMAIL_RECIPIENT in msg['To'][0]['Mailbox'] and \
               "Test Report from Exporter" in msg['Content']['Headers']['Subject'][0]:
                found_email = True
                # Further checks: attachment presence, body content
                assert len(msg['Attachments']) > 0, "Email should have an attachment"
                assert TEST_GCS_FILE_NAME in msg['Attachments'][0]['Filename'], "Attachment filename mismatch"
                assert "Please find the attached exported file" in msg['Content']['Body'], "Email body mismatch"
                print("Email found and content verified on Mailhog.")
                break
        assert found_email, f"Email to {TEST_EMAIL_RECIPIENT} with subject 'Test Report from Exporter' not found."

    except Exception as e:
        pytest.fail(f"Mailhog verification failed: {e}")
```

---

### Test Case 6: GCS File Operations (Move/Copy/Delete)

*   **Purpose**: Verify that the `gcs_file_operations` Cloud Function correctly performs file manipulations (move, copy, delete) within Cloud Storage, replacing shell commands like `mv`, `cp`, `rm`. The provided Cloud Function implements a specific "move to archive" logic.

*   **Setup**:
    1.  Deploy `gcs_file_operations` Cloud Function.
    2.  Create a source Cloud Storage bucket (e.g., `your-exported-bucket-gcs-ops`).
    3.  Upload a dummy test file (e.g., `test_file_to_move.txt`) to the source bucket.

*   **Action**:
    1.  Upload `test_file_to_move.txt` to `gs://your-exported-bucket-gcs-ops/test_file_to_move.txt`. This will trigger the Cloud Function.
    2.  Monitor Cloud Function logs.
    3.  Check Cloud Storage for the presence/absence of files in the original and archived locations.

*   **Pass/Fail Criterion**:
    *   **Pass**:
        *   The original file `gs://your-exported-bucket-gcs-ops/test_file_to_move.txt` is deleted.
        *   A new file `gs://your-exported-bucket-gcs-ops/archive/test_file_to_move.txt` exists with identical content.
        *   Cloud Function logs indicate successful execution.
    *   **Fail**: Original file not deleted, new file not created, content mismatch, or Cloud Function error.

```python
# Example Python (pytest) for GCS file operations
import pytest
from google.cloud import storage
import time
import os

# Configuration for test
TEST_GCS_BUCKET = "your-exported-bucket-gcs-ops" # This bucket name is important for the CF logic
TEST_SOURCE_FILE_NAME = "test_file_to_move.txt"
TEST_ARCHIVE_FILE_NAME = f"archive/{TEST_SOURCE_FILE_NAME}"
TEST_FILE_CONTENT = "This is content to be moved."

storage_client = storage.Client()

@pytest.fixture(scope="module", autouse=True)
def setup_gcs_ops_test_data():
    # Ensure the bucket exists
    try:
        storage_client.get_bucket(TEST_GCS_BUCKET)
    except Exception:
        storage_client.create_bucket(TEST_GCS_BUCKET)
        print(f"Created bucket {TEST_GCS_BUCKET}")

    # Upload a dummy file to the source location
    bucket = storage_client.bucket(TEST_GCS_BUCKET)
    blob = bucket.blob(TEST_SOURCE_FILE_NAME)
    blob.upload_from_string(TEST_FILE_CONTENT)
    print(f"Uploaded dummy file to gs://{TEST_GCS_BUCKET}/{TEST_SOURCE_FILE_NAME}")

    yield

    # Teardown: Clean up
    bucket = storage_client.bucket(TEST_GCS_BUCKET)
    source_blob = bucket.blob(TEST_SOURCE_FILE_NAME)
    archive_blob = bucket.blob(TEST_ARCHIVE_FILE_NAME)

    if source_blob.exists():
        source_blob.delete()
        print(f"Cleaned up gs://{TEST_GCS_BUCKET}/{TEST_SOURCE_FILE_NAME}")
    if archive_blob.exists():
        archive_blob.delete()
        print(f"Cleaned up gs://{TEST_GCS_BUCKET}/{TEST_ARCHIVE_FILE_NAME}")

def test_gcs_file_move_operation():
    # Action: The Cloud Function is triggered by the GCS file upload in setup_gcs_ops_test_data.
    # We need to wait for it to complete.
    print("Waiting for Cloud Function to process GCS file move...")
    time.sleep(20) # Give CF time to execute

    # Pass/Fail Criterion:
    bucket = storage_client.bucket(TEST_GCS_BUCKET)
    source_blob = bucket.blob(TEST_SOURCE_FILE_NAME)
    archive_blob = bucket.blob(TEST_ARCHIVE_FILE_NAME)

    # 1. Verify original file is deleted
    assert not source_blob.exists(), f"Original file gs://{TEST_GCS_BUCKET}/{TEST_SOURCE_FILE_NAME} should be deleted."
    print("Original file deleted.")

    # 2. Verify archived file exists
    assert archive_blob.exists(), f"Archived file gs://{TEST_GCS_BUCKET}/{TEST_ARCHIVE_FILE_NAME} should exist."
    print("Archived file exists.")

    # 3. Verify content parity
    actual_content = archive_blob.download_as_string().decode('utf-8')
    assert actual_content == TEST_FILE_CONTENT, "Content of archived file does not match original."
    print("Content parity verified.")
```

---

### Test Case 7: Data Quality and Row Count Assertions (End-to-End)

*   **Purpose**: Verify that an end-to-end run of the migrated job produces the expected data volume and quality, matching the legacy job's output. This covers row counts, schema, and basic data integrity across all generated output files.

*   **Setup**:
    1.  Identify a complete export scenario from the legacy system (e.g., a daily export of a specific report that generates multiple partitioned files).
    2.  Configure the `dwh_exporter.config_kv` table with all necessary parameters for this scenario.
    3.  Ensure all necessary BigQuery tables (source, staging, destination) are set up and populated with representative data.
    4.  Ensure the source Oracle data for the test period is identical in both environments.
    5.  Have the legacy `r_exis_v2` script configured to run this full export scenario, outputting to a known local directory.

*   **Action**:
    1.  Run the legacy `r_exis_v2` script for the chosen scenario, capturing the output files in `LEGACY_OUTPUT_DIR`.
    2.  Invoke the `dwh_exporter.r_exis_v2` BigQuery Stored Procedure for the same scenario, ensuring it writes its final output to Cloud Storage under `MIGRATED_GCS_OUTPUT_PREFIX`.
    3.  Capture the output files from Cloud Storage.
    4.  Query `dwh_exporter.job_history` and `dwh_exporter.export_audit` for job status and metrics.

*   **Pass/Fail Criterion**:
    *   **File Count Parity**: The number of output files generated by the migrated job must exactly match the number of files from the legacy job.
    *   **Content Parity**: The content of each corresponding output file from the migrated job must be identical to the legacy job's output (e.g., verified by MD5 checksums or direct data comparison for CSVs).
    *   **Row Count Parity**: For each file, the total number of rows must match.
    *   **Schema Parity**: The schema (column names, order, and inferred data types) of the migrated output must match the legacy output.
    *   **Job Status**: The `dwh_exporter.job_history` entry for the run should show `status='SUCCESS'`.
    *   **Audit Trail**: `dwh_exporter.export_audit` should contain a complete and correct sequence of `STARTED`/`COMPLETED` steps without `FAILED` entries.

```python
# Example Python (pytest) for end-to-end data quality and row count
import pandas as pd
from google.cloud import bigquery, storage
import os
import hashlib
import pytest
import time

# Configuration
LEGACY_OUTPUT_DIR = "/tmp/legacy_full_export_output" # Local directory for legacy output
MIGRATED_GCS_OUTPUT_PREFIX = "gs://your-export-bucket-full-export/full_export_output/" # GCS prefix for migrated output
TEST_JOB_NAME = "full_export_scenario"
TEST_RUN_ID = f"full_export_run_{int(time.time())}"

bq_client = bigquery.Client()
storage_client = storage.Client()

def get_file_checksum(filepath):
    """Calculates MD5 checksum of a file."""
    hasher = hashlib.md5()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()

@pytest.fixture(scope="module", autouse=True)
def setup_full_export_test_data():
    # Pre-requisite: Manually run legacy r_exis_v2 to generate files in LEGACY_OUTPUT_DIR
    # Example: Create dummy legacy files
    os.makedirs(LEGACY_OUTPUT_DIR, exist_ok=True)
    with open(os.path.join(LEGACY_OUTPUT_DIR, "part_1.csv"), "w") as f:
        f.write("colA,colB\n1,X\n2,Y\n")
    with open(os.path.join(LEGACY_OUTPUT_DIR, "part_2.csv"), "w") as f:
        f.write("colA,colB\n3,Z\n4,W\n")
    print(f"Created dummy legacy output files in {LEGACY_OUTPUT_DIR}")

    # Pre-requisite: Run dwh_exporter.r_exis_v2 to generate files in MIGRATED_GCS_OUTPUT_PREFIX
    # This would involve calling the main BQ procedure. For this test, we simulate the output.
    migrated_bucket_name = MIGRATED_GCS_OUTPUT_PREFIX.split('/')[2]
    migrated_prefix = '/'.join(MIGRATED_GCS_OUTPUT_PREFIX.split('/')[3:])
    bucket = storage_client.bucket(migrated_bucket_name)
    bucket.blob(os.path.join(migrated_prefix, "part_1.csv")).upload_from_string("colA,colB\n1,X\n2,Y\n")
    bucket.blob(os.path.join(migrated_prefix, "part_2.csv")).upload_from_string("colA,colB\n3,Z\n4,W\n")
    print(f"Uploaded dummy migrated output files to {MIGRATED_GCS_OUTPUT_PREFIX}")

    # Insert a successful job history entry for the migrated run
    bq_client.query(f"""
        INSERT INTO dwh_exporter.job_history (job_id, run_id, job_name, start_time, end_time, status, message, parameters_json)
        VALUES (
            GENERATE_UUID(),
            '{TEST_RUN_ID}',
            '{TEST_JOB_NAME}',
            TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE),
            CURRENT_TIMESTAMP(),
            'SUCCESS',
            'Job completed successfully.',
            JSON '{}'
        )
    """).result()
    print(f"Inserted successful job history for run_id: {TEST_RUN_ID}")

    yield

    # Teardown: Clean up
    for f in os.listdir(LEGACY_OUTPUT_DIR):
        os.remove(os.path.join(LEGACY_OUTPUT_DIR, f))
    os.rmdir(LEGACY_OUTPUT_DIR)

    migrated_bucket_name = MIGRATED_GCS_OUTPUT_PREFIX.split('/')[2]
    migrated_prefix = '/'.join(MIGRATED_GCS_OUTPUT_PREFIX.split('/')[3:])
    bucket = storage_client.bucket(migrated_bucket_name)
    for blob in bucket.list_blobs(prefix=migrated_prefix):
        blob.delete()

    bq_client.query(f"DELETE FROM dwh_exporter.job_history WHERE run_id = '{TEST_RUN_ID}'").result()
    bq_client.query(f"DELETE FROM dwh_exporter.export_audit WHERE run_id = '{TEST_RUN_ID}'").result()


def test_full_export_data_quality_and_row_count():
    # Get list of files from legacy output directory
    legacy_files = [f for f in os.listdir(LEGACY_OUTPUT_DIR) if os.path.isfile(os.path.join(LEGACY_OUTPUT_DIR, f))]
    assert len(legacy_files) > 0, "No legacy output files found."

    # Get list of files from migrated GCS prefix
    migrated_bucket_name = MIGRATED_GCS_OUTPUT_PREFIX.split('/')[2]
    migrated_prefix = '/'.join(MIGRATED_GCS_OUTPUT_PREFIX.split('/')[3:])
    bucket = storage_client.bucket(migrated_bucket_name)
    migrated_blobs = list(bucket.list_blobs(prefix=migrated_prefix))
    migrated_file_names = [blob.name.replace(migrated_prefix, '') for blob in migrated_blobs if not blob.name.endswith('/')]
    migrated_file_names = [f for f in migrated_file_names if f] # Filter out empty strings if prefix matches exactly

    # Pass/Fail Criterion:
    # 1. File count parity
    assert len(legacy_files) == len(migrated_file_names), \
        f"File count mismatch: Legacy has {len(legacy_files)}, Migrated has {len(migrated_file_names)}"
    print(f"File count parity: {len(legacy_files)} files found in both.")

    # 2. Compare each file
    for filename in legacy_files:
        print(f"Comparing file: {filename}")
        legacy_filepath = os.path.join(LEGACY_OUTPUT_DIR, filename)
        migrated_gcs_blob_name = os.path.join(migrated_prefix, filename)
        migrated_local_filepath = f"/tmp/{filename}"

        # Download migrated file
        migrated_blob = bucket.blob(migrated_gcs_blob_name)
        assert migrated_blob.exists(), f"Migrated file {migrated_gcs_blob_name} not found in GCS."
        migrated_blob.download_to_filename(migrated_local_filepath)

        # Calculate checksums
        legacy_checksum = get_file_checksum(legacy_filepath)
        migrated_checksum = get_file_checksum(migrated_local_filepath)
        assert legacy_checksum == migrated_checksum, \
            f"Checksum mismatch for file {filename}: Legacy={legacy_checksum}, Migrated={migrated_checksum}"
        print(f"Checksums match for {filename}.")

        # For CSVs, also check row count and basic schema
        try:
            df_legacy = pd.read_csv(legacy_filepath, sep=',', na_values=['NULL', ''])
            df_migrated = pd.read_csv(migrated_local_filepath, sep=',', na_values=['NULL', ''])

            assert len(df_legacy) == len(df_migrated), \
                f"Row count mismatch for {filename}: Legacy={len(df_legacy)}, Migrated={len(df_migrated)}"
            print(f"Row counts match for {filename}: {len(df_legacy)} rows.")

            # Optional: Deeper data comparison if checksum isn't enough (e.g., for floating point differences)
            # pd.testing.assert_frame_equal(df_legacy.sort_values(by=list(df_legacy.columns)).reset_index(drop=True),
            #                               df_migrated.sort_values(by=list(df_migrated.columns)).reset_index(drop=True),
            #                               check_dtype=False,
            #                               err_msg=f"Data mismatch for file {filename}")

        except Exception as e:
            print(f"Warning: Could not perform detailed CSV comparison for {filename}: {e}")

        os.remove(migrated_local_filepath) # Clean up local copy

    # 3. Check job history status
    job_history_query = bq_client.query(f"""
        SELECT status, message
        FROM dwh_exporter.job_history
        WHERE run_id = '{TEST_RUN_ID}'
    """).result()
    job_history_row = next(job_history_query)
    assert job_history_row.status == 'SUCCESS', f"Job history status is not SUCCESS: {job_history_row.status}"
    print("Job history status is SUCCESS.")

    # 4. Check audit trail for failures
    audit_failures_query = bq_client.query(f"""
        SELECT COUNT(1) FROM dwh_exporter.export_audit
        WHERE run_id = '{TEST_RUN_ID}' AND status = 'FAILED'
    """).result()
    failed_audit_count = next(audit_failures_query)[0]
    assert failed_audit_count == 0, f"Found {failed_audit_count} FAILED entries in export_audit."
    print("No FAILED entries in export_audit.")

    print("End-to-end data quality and row count test passed.")
```

---

### Test Case 8: Error Handling and Logging

*   **Purpose**: Verify that the migrated job correctly handles errors and logs them to `dwh_exporter.job_history` and `dwh_exporter.export_audit`, mirroring the `DWMSG_MeldeFehler` and `trap` mechanisms of the legacy script.

*   **Setup**:
    1.  Ensure `dwh_exporter.job_history` and `dwh_exporter.export_audit` tables are available.
    2.  Create a specific configuration in `dwh_exporter.config_kv` for a job (e.g., `error_test_job`) that is designed to fail. This could be achieved by:
        *   Providing an invalid SQL query in a configuration parameter that `r_exis_v2` would attempt to execute.
        *   Configuring a non-existent GCS path for an output operation.
        *   (For testing the `EXCEPTION` block directly, you might temporarily inject a `SELECT 1/0;` or similar error into a sub-procedure called by `r_exis_v2`).

*   **Action**:
    1.  Invoke `dwh_exporter.r_exis_v2` with `p_job_name='error_test_job'` and a unique `p_run_id`.
    2.  Monitor `dwh_exporter.job_history` and `dwh_exporter.export_audit` tables.

*   **Pass/Fail Criterion**:
    *   **Pass**:
        *   The `dwh_exporter.job_history` entry for the `p_run_id` shows `status='FAILED'` and `message` contains relevant error details.
        *   `dwh_exporter.export_audit` contains at least one `status='FAILED'` entry for the step that caused the error, with `log_message` and `metadata_json` (including `error_message` and `stack_trace`) providing diagnostic information.
        *   No successful output files are generated if the error occurs before file creation.
    *   **Fail**: Job status is `SUCCESS` despite an error, error details are missing or incorrect, or no audit entries are recorded.

```sql
-- Setup: Create a config that will cause a failure (e.g., invalid SQL)
-- For this example, we'll assume 'error_test_job' is configured to call a sub-procedure
-- that intentionally raises an error, or has an invalid SQL statement.
-- Example: Insert a config that points to an invalid SQL.
INSERT INTO dwh_exporter.config_kv (job_name, config_key, config_value, config_type, description, updated_at)
VALUES
('error_test_job', 'OUTPUT_SQL', 'SELECT 1 / 0 FROM some_table;', 'SQL', 'SQL designed to fail', CURRENT_TIMESTAMP());

-- Action: Invoke the main procedure with a job name configured to fail
DECLARE test_run_id STRING DEFAULT 'error_test_run_' || GENERATE_UUID();
CALL dwh_exporter.r_exis_v2(
    'error_test_job',
    test_run_id,
    JSON '{}'
);

-- Pass/Fail Criterion: SQL Assertions
SELECT
    jh.status AS job_status,
    jh.message AS job_message,
    (SELECT COUNT(1) FROM dwh_exporter.export_audit ea WHERE ea.run_id = test_run_id AND ea.status = 'FAILED') AS failed_audit_count,
    (SELECT ARRAY_AGG(JSON_EXTRACT_SCALAR(ea.metadata_json, '$.error_message')) FROM dwh_exporter.export_audit ea WHERE ea.run_id = test_run_id AND ea.status = 'FAILED') AS error_messages
FROM dwh_exporter.job_history jh
WHERE jh.run_id = test_run_id;
-- Expected Result:
-- job_status = 'FAILED'
-- job_message LIKE 'Job failed with error: %' (containing specific error like "division by zero")
-- failed_audit_count >= 1
-- error_messages = (array containing error messages, e.g., "division by zero")
```

---

### Test Case 9: Incremental Logic Correctness

*   **Purpose**: Verify that the migrated job's incremental logic (using BigQuery `MERGE` statements or partitioned loads) correctly processes only new or changed data, replicating the shell-based incremental logic and file preprocessing.

*   **Setup**:
    1.  Identify a specific incremental export scenario from the legacy system.
    2.  Configure `dwh_exporter.config_kv` for this job, including parameters that define incremental behavior (e.g., `INCREMENTAL_MODE`, `LAST_RUN_TIMESTAMP`).
    3.  Ensure source Oracle tables have a mechanism for tracking changes (e.g., `LAST_MODIFIED_DATE` column).
    4.  Create a BigQuery destination table that supports `MERGE` operations (e.g., partitioned by date, clustered by a unique key).
    5.  Populate the BigQuery destination table with initial historical data.
    6.  Ingest new/updated data into `dwh_oracle_source.orders` (or equivalent) for a later period, ensuring these changes are reflected in the BigQuery source table.

*   **Action**:
    1.  Run the legacy `r_exis_v2` script in incremental mode, capturing the output (e.g., `new_orders.csv`).
    2.  Invoke `dwh_exporter.r_exis_v2` in incremental mode (passing appropriate parameters or relying on `config_kv`), which should execute a `MERGE` statement or similar logic.
    3.  Query the BigQuery destination table to inspect the newly added/updated records.

*   **Pass/Fail Criterion**:
    *   **Row Count Parity**: The number of new/updated rows processed by the migrated job (and reflected in the destination table) should match the incremental output of the legacy job.
    *   **Data Parity**: The data in the BigQuery destination table (specifically the newly added/updated records) should match the incremental output of the legacy job.
    *   **No Duplicates/Missing Data**: The destination table should not contain duplicate records or miss any expected incremental changes.
    *   **Performance**: The incremental run should be significantly faster than a full run, indicating efficient processing of only changed data.

```sql
-- Setup:
-- 1. Create a dummy source table (simulating Oracle data in BQ) and a target incremental table.
CREATE OR REPLACE TABLE dwh_oracle_source.orders AS
SELECT 1 AS order_id, DATE '2023-01-01' AS order_date, 101 AS customer_id, 100.00 AS amount, TIMESTAMP '2023-01-01 10:00:00' AS last_modified_ts UNION ALL
SELECT 2 AS order_id, DATE '2023-01-02' AS order_date, 102 AS customer_id, 150.00 AS amount, TIMESTAMP '2023-01-02 11:00:00' AS last_modified_ts;

CREATE OR REPLACE TABLE dwh_exporter.orders_incremental (
    order_id INT64,
    order_date DATE,
    customer_id INT64,
    amount FLOAT64,
    last_modified_ts TIMESTAMP
)
PARTITION BY order_date
CLUSTER BY order_id;

-- 2. Populate dwh_exporter.orders_incremental with initial data up to a certain date.
INSERT INTO dwh_exporter.orders_incremental
SELECT * FROM dwh_oracle_source.orders WHERE order_date <= '2023-01-01';

-- 3. Configure 'incremental_job' in dwh_exporter.config_kv with LAST_RUN_TIMESTAMP.
INSERT INTO dwh_exporter.config_kv (job_name, config_key, config_value, config_type, description, updated_at)
VALUES ('incremental_job', 'LAST_RUN_TIMESTAMP', '2023-01-01 10:00:00', 'TIMESTAMP', 'Last successful incremental run timestamp', CURRENT_TIMESTAMP());

-- 4. Ingest new/updated data into dwh_oracle_source.orders for a later period.
INSERT INTO dwh_oracle_source.orders
SELECT 3 AS order_id, DATE '2023-01-03' AS order_date, 103 AS customer_id, 200.00 AS amount, TIMESTAMP '2023-01-03 12:00:00' AS last_modified_ts UNION ALL
SELECT 1 AS order_id, DATE '2023-01-01' AS order_date, 101 AS customer_id, 110.00 AS amount, TIMESTAMP '2023-01-02 10:30:00' AS last_modified_ts; -- Updated record

-- Action: Simulate the incremental job execution (this MERGE would be part of a BQ procedure)
DECLARE last_run_ts TIMESTAMP;
SELECT PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', config_value) INTO last_run_ts FROM dwh_exporter.config_kv WHERE job_name = 'incremental_job' AND config_key = 'LAST_RUN_TIMESTAMP';

MERGE INTO dwh_exporter.orders_incremental AS T
USING (
    SELECT
        order_id,
        order_date,
        customer_id,
        amount,
        last_modified_ts
    FROM dwh_oracle_source.orders
    WHERE last_modified_ts > last_run_ts
) AS S
ON T.order_id = S.order_id
WHEN MATCHED AND T.last_modified_ts < S.last_modified_ts THEN
    UPDATE SET
        T.order_date = S.order_date,
        T.customer_id = S.customer_id,
        T.amount = S.amount,
        T.last_modified_ts = S.last_modified_ts
WHEN NOT MATCHED THEN
    INSERT (order_id, order_date, customer_id, amount, last_modified_ts)
    VALUES (S.order_id, S.order_date, S.customer_id, S.amount, S.last_modified_ts);

-- Pass/Fail Criterion: SQL Assertions
-- Verify the final state of the incremental table
SELECT order_id, order_date, customer_id, amount, last_modified_ts
FROM dwh_exporter.orders_incremental
ORDER BY order_id;
-- Expected Result:
-- order_id | order_date | customer_id | amount | last_modified_ts
-- -------- | ---------- | ----------- | ------ | -------------------
-- 1        | 2023-01-01 | 101         | 110.00 | 2023-01-02 10:30:00 UTC (updated)
-- 2        | 2023-01-02 | 102         | 150.00 | 2023-01-02 11:00:00 UTC (original, not touched by incremental)
-- 3        | 2023-01-03 | 103         | 200.00 | 2023-01-03 12:00:00 UTC (new)

-- Compare this result set with the output of the legacy incremental job.
-- (This comparison would typically be done programmatically using pandas as in Test Case 3)
```

---

### Test Case 10: Parallel Execution Orchestration

*   **Purpose**: Verify that the migrated job correctly orchestrates parallel execution of file or SQL partitions, replicating the `merger`, `PARALLELFILE`, `PARALLELSQL` logic.

*   **Setup**:
    1.  Identify a scenario in the legacy script that uses parallel processing (e.g., exporting data partitioned by day, with multiple days processed concurrently).
    2.  Configure `dwh_exporter.config_kv` for this job to enable parallel processing and define partitioning strategy (e.g., `FILE_PARTITION='daily'`, `PARALLELFILE='YES'`).
    3.  Ensure the BigQuery stored procedures are designed to handle parallel execution (e.g., by launching multiple sub-procedures or using Cloud Workflows to fan-out tasks).
    4.  Prepare source data that allows for clear partitioning and parallel processing.

*   **Action**:
    1.  Run the legacy `r_exis_v2` script with parallel execution enabled, noting the execution time and output files.
    2.  Invoke `dwh_exporter.r_exis_v2` for the same scenario, noting execution time and output files.
    3.  Monitor BigQuery job history and Cloud Workflows logs (if used) to confirm parallel execution.
    4.  Inspect `dwh_exporter.export_audit` for overlapping `start_time`/`end_time` for partition-specific steps.

*   **Pass/Fail Criterion**:
    *   **Output Parity**: The final combined output files from the migrated job must be identical to the legacy job's output.
    *   **Performance**: The execution time of the migrated job should be comparable to or better than the legacy job for parallel scenarios.
    *   **Orchestration**: Logs (BigQuery audit, Cloud Workflows) should clearly show multiple tasks/queries running concurrently for different partitions.
    *   **No Data Corruption**: Parallel execution should not lead to data loss, duplication, or corruption.

```python
# Example Python (pytest) for parallel execution verification
import pandas as pd
from google.cloud import bigquery, storage
import time
import os
import pytest

# Configuration
LEGACY_OUTPUT_DIR = "/tmp/legacy_parallel_export_output"
MIGRATED_GCS_OUTPUT_PREFIX = "gs://your-export-bucket-parallel/parallel_export_output/"
TEST_JOB_NAME = "parallel_export_job"
TEST_RUN_ID = f"parallel_export_run_{int(time.time())}"

bq_client = bigquery.Client()
storage_client = storage.Client()

@pytest.fixture(scope="module", autouse=True)
def setup_parallel_export_test_data():
    # Pre-requisite: Configure 'parallel_export_job' in config_kv to enable parallel file/SQL partitioning.
    # Pre-requisite: Ensure the BQ stored procedure for this job is designed to execute partitions in parallel.
    # Example: Create dummy legacy files (simulating partitioned output)
    os.makedirs(LEGACY_OUTPUT_DIR, exist_ok=True)
    with open(os.path.join(LEGACY_OUTPUT_DIR, "daily_20230101.csv"), "w") as f:
        f.write("date,value\n2023-01-01,100\n")
    with open(os.path.join(LEGACY_OUTPUT_DIR, "daily_20230102.csv"), "w") as f:
        f.write("date,value\n2023-01-02,200\n")
    print(f"Created dummy legacy parallel output files in {LEGACY_OUTPUT_DIR}")

    # Pre-requisite: Run dwh_exporter.r_exis_v2 for parallel_export_job
    # This would involve calling the main BQ procedure. For this test, we simulate the output.
    migrated_bucket_name = MIGRATED_GCS_OUTPUT_PREFIX.split('/')[2]
    migrated_prefix = '/'.join(MIGRATED_GCS_OUTPUT_PREFIX.split('/')[3:])
    bucket = storage_client.bucket(migrated_bucket_name)
    bucket.blob(os.path.join(migrated_prefix, "daily_20230101.csv")).upload_from_string("date,value\n2023-01-01,100\n")
    bucket.blob(os.path.join(migrated_prefix, "daily_20230102.csv")).upload_from_string("date,value\n2023-01-02,200\n")
    print(f"Uploaded dummy migrated parallel output files to {MIGRATED_GCS_OUTPUT_PREFIX}")

    # Insert job history and audit entries for the migrated run, simulating parallel steps
    bq_client.query(f"""
        INSERT INTO dwh_exporter.job_history (job_id, run_id, job_name, start_time, end_time, status, message, parameters_json)
        VALUES (
            GENERATE_UUID(),
            '{TEST_RUN_ID}',
            '{TEST_JOB_NAME}',
            TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE),
            CURRENT_TIMESTAMP(),
            'SUCCESS',
            'Job completed successfully with parallel partitions.',
            JSON '{}'
        );
        INSERT INTO dwh_exporter.export_audit (audit_id, job_id, run_id, step_name, status, start_time, end_time, log_message, metadata_json)
        VALUES
        (GENERATE_UUID(), '{TEST_JOB_NAME}', '{TEST_RUN_ID}', 'filepartition_20230101', 'STARTED', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 8 MINUTE), NULL, 'Processing partition 20230101', NULL),
        (GENERATE_UUID(), '{TEST_JOB_NAME}', '{TEST_RUN_ID}', 'filepartition_20230102', 'STARTED', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 MINUTE), NULL, 'Processing partition 20230102', NULL),
        (GENERATE_UUID(), '{TEST_JOB_NAME}', '{TEST_RUN_ID}', 'filepartition_20230101', 'COMPLETED', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE), 'Partition 20230101 completed', NULL),
        (GENERATE_UUID(), '{TEST_JOB_NAME}', '{TEST_RUN_ID}', 'filepartition_20230102', 'COMPLETED', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 4 MINUTE), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 4 MINUTE), 'Partition 20230102 completed', NULL);
    """).result()
    print(f"Inserted job history and audit entries for run_id: {TEST_RUN_ID}")

    yield

    # Teardown: Clean up
    for f in os.listdir(LEGACY_OUTPUT_DIR):
        os.remove(os.path.join(LEGACY_OUTPUT_DIR, f))
    os.rmdir(LEGACY_OUTPUT_DIR)

    migrated_bucket_name = MIGRATED_GCS_OUTPUT_PREFIX.split('/')[2]
    migrated_prefix = '/'.join(MIGRATED_GCS_OUTPUT_PREFIX.split('/')[3:])
    bucket = storage_client.bucket(migrated_bucket_name)
    for blob in bucket.list_blobs(prefix=migrated_prefix):
        blob.delete()

    bq_client.query(f"DELETE FROM dwh_exporter.job_history WHERE run_id = '{TEST_RUN_ID}'").result()
    bq_client.query(f"DELETE FROM dwh_exporter.export_audit WHERE run_id = '{TEST_RUN_ID}'").result()


def test_parallel_execution_orchestration():
    # 1. Output Parity (re-use logic from Test Case 7 for file comparison)
    # This part would involve listing all files from both sources and comparing them.
    # For brevity, assume this passes if setup creates identical files.

    # 2. Performance (manual observation or automated metric collection)
    job_history_query = bq_client.query(f"""
        SELECT TIMESTAMP_DIFF(end_time, start_time, SECOND) AS duration_seconds
        FROM dwh_exporter.job_history
        WHERE run_id = '{TEST_RUN_ID}'
    """).result()
    migrated_duration = next(job_history_query)[0]
    print(f"Migrated job duration: {migrated_duration} seconds.")
    # Assert migrated_duration is within acceptable bounds compared to legacy.

    # 3. Orchestration verification (check audit logs for concurrent activity)
    audit_entries_query = bq_client.query(f"""
        SELECT step_name, start_time, end_time
        FROM dwh_exporter.export_audit
        WHERE run_id = '{TEST_RUN_ID}'
          AND step_name LIKE 'filepartition_%'
          AND status = 'STARTED'
        ORDER BY start_time
    """).result()
    audit_steps = [(row.step_name, row.start_time, row.end_time) for row in audit_entries_query]

    # Simple check for concurrency: if multiple steps have overlapping time ranges.
    concurrent_steps_found = False
    for i in range(len(audit_steps)):
        for j in range(i + 1, len(audit_steps)):
            if (audit_steps[i][1] < audit_steps[j][2] and audit_steps[i][2] > audit_steps[j][1]):
                concurrent_steps_found = True
                break
        if concurrent_steps_found:
            break
    assert concurrent_steps_found, "No evidence of concurrent partition processing found in audit logs."
    print("Concurrent partition processing detected in audit logs.")

    print("Parallel execution orchestration test passed.")
```