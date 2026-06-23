As a senior data-migration QA engineer, I've analyzed the migration design document and the generated GCP code for `r_exis_v2`. The migration involves a significant re-engineering effort from KornShell to a GCP-native stack (Airflow, BigQuery, Dataflow, GCS, Cloud Run). This complexity necessitates thorough validation.

The following test cases are designed to ensure behavioral equivalence, data integrity, and functional correctness across the migrated system, covering output parity, transformation logic, external system interactions, and data quality.

---

## Migration Validation Tests for `r_exis_v2`

**Pre-requisites for all tests:**

*   **Legacy Environment:** A fully functional legacy `r_exis_v2` environment with access to the Oracle source database and target file systems/SFTP/email. This environment is crucial for generating baseline outputs.
*   **GCP Environment:** A fully configured GCP project with BigQuery, Cloud Composer (Airflow), Dataform, Dataflow, GCS, Cloud Run, and Secret Manager. All necessary GCP services must be provisioned and have appropriate IAM permissions.
*   **Data Migration:** The Oracle source tables (e.g., `DWH$TA_K_MELDUNGEN`) and any relevant metadata tables must be migrated to BigQuery. This is a prerequisite for the migrated job to run.
*   **Configuration Migration:** The `r_exis_v2` configuration (from `k_exis_v2_defaults.cfg` and job-specific configs) must be loaded into the `exporter_config` BigQuery table.
*   **Secrets:** All necessary secrets (SFTP passwords, email API keys) must be securely stored in GCP Secret Manager and accessible by the Cloud Run service.
*   **Dataform Project:** A Dataform project with the `r_exis_v2_sql_model_pre`, `r_exis_v2_sql_model_output`, and `r_exis_v2_sql_model_post` models defined and deployed.
*   **Dataflow Template:** The `r_exis_v2_dataflow_pipeline.py` must be deployed as a Dataflow template in GCS.
*   **Cloud Run Service:** The `external_distributor_cloud_run.py` must be deployed as a Cloud Run service.

---

### Test Case 1: Configuration Loading and Parameter Resolution

*   **Purpose:** Verify that the Airflow DAG correctly loads job configuration from BigQuery and resolves dynamic parameters (e.g., `FROM_DATE`, `TO_DATE`, `JOB_NAME`) in a manner equivalent to the legacy script's `parser_getnode`, `fillattribs`, and `handletimestamps`. This ensures the job runs with the correct context.
*   **Setup:**
    *   Populate `your_gcp_project_id.your_bigquery_dataset.exporter_config` with a sample configuration for `r_exis_v2`. Include `PRE_SQL`, `OUTPUT_SQL`, `META` blocks with placeholders like `${FROM_DATE}`, `${TO_DATE}`, `${JOB_NAME}`.
    *   Ensure `handletimestamps_bq` and `fillattribs_bq` UDFs are deployed in BigQuery.
    *   Define a specific `data_interval_start` and `data_interval_end` for the Airflow DAG run.
*   **Action:**
    1.  Manually trigger the `r_exis_v2_dag` in Cloud Composer with a specific `data_interval_start` and `data_interval_end` (e.g., `2023-01-01` to `2023-01-02`).
    2.  Monitor the `get_job_config` and `resolve_parameters` tasks in the Airflow UI.
    3.  Inspect the XCom outputs of `resolve_parameters` for the `resolved_params` dictionary.
    4.  For the legacy script, run `r_exis_v2` with equivalent `-f` and `-t` parameters and enable debug logging (`-v`) to capture resolved variables.
*   **Pass/Fail Criterion:**
    *   **Pass:** The `resolved_params` XCom output from the `resolve_parameters` task in Airflow contains key-value pairs (e.g., `FROM_DATE`, `TO_DATE`, `JOB_NAME`) that exactly match the values resolved by the legacy `r_exis_v2` script for the same input dates. The `job_config` XCom should accurately reflect the BigQuery `exporter_config` table.
    *   **Fail:** Mismatch in resolved parameters or configuration loading.

    ```python
    # Conceptual Pytest assertion (requires Airflow API client and parsing legacy logs)
    import pytest
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    @pytest.mark.parametrize("start_date, end_date", [("2023-01-01", "2023-01-02"), ("2023-03-15", "2023-03-15")])
    def test_parameter_resolution_parity(airflow_client, legacy_script_executor, start_date, end_date):
        # 1. Trigger migrated DAG
        dag_run_id = airflow_client.trigger_dag("r_exis_v2_dag", execution_date=f"{start_date}T00:00:00Z")
        airflow_client.wait_for_dag_run_completion(dag_run_id)

        # 2. Get resolved parameters from migrated DAG's XCom
        migrated_params = airflow_client.get_xcom_value(dag_run_id, "resolve_parameters", "resolved_params")

        # 3. Run legacy script and parse its debug output for resolved parameters
        #    (This part is highly dependent on how legacy_script_executor works and parses)
        legacy_params = legacy_script_executor.run_and_parse_params(start_date, end_date)

        # 4. Assert parity
        assert migrated_params["FROM_DATE"] == legacy_params["FROM_DATE"]
        assert migrated_params["TO_DATE"] == legacy_params["TO_DATE"]
        assert migrated_params["JOB_NAME"] == legacy_params["JOB_NAME"] # Should be 'r_exis_v2'
        # Add assertions for other critical parameters like SYSDATE, PARAM_1, etc.
        # Note: SYSDATE might differ slightly due to execution time, focus on date part or allow tolerance.
    ```

---

### Test Case 2: Output Parity - Main Data Extraction (OUTPUT_SQL)

*   **Purpose:** Verify that the core data extraction logic, translated from Oracle `sqlplus` to BigQuery SQL (via Dataform or `BigQueryExecuteQueryOperator`), produces an identical dataset for a given set of inputs. This covers joins, aggregations, filters, and basic type handling.
*   **Setup:**
    *   Ensure `exporter_config` contains the `OUTPUT_SQL` node with the actual BigQuery SQL equivalent to the legacy Oracle SQL.
    *   Populate the BigQuery source tables (e.g., `your_gcp_project_id.your_bigquery_dataset.dwh_source_table`) with a representative dataset, including various data types, NULLs, and edge cases. This dataset should be identical to the Oracle source data used by the legacy job.
    *   For the legacy system, ensure the Oracle database has the same data.
*   **Action:**
    1.  **Legacy:** Run the `r_exis_v2` script with specific `FROM` and `TO` dates (e.g., `2023-01-01` to `2023-01-01`). Capture the generated output file.
    2.  **Migrated:** Trigger the `r_exis_v2_dag` in Cloud Composer for the same `data_interval_start` and `data_interval_end`.
    3.  After successful execution, download the exported file from GCS (`gs://your-gcs-landing-bucket/r_exis_v2/output_data_...`).
*   **Pass/Fail Criterion:**
    *   **Pass:** The content of the exported file from GCS is byte-for-byte identical (or semantically identical after accounting for minor differences like line endings or floating-point precision if unavoidable) to the output file generated by the legacy `r_exis_v2` script.
    *   **Fail:** Any difference in data, column order, formatting, or row count.

    ```python
    # Conceptual Pytest assertion
    import filecmp
    import pandas as pd
    import os

    @pytest.mark.parametrize("test_case_name, start_date, end_date", [
        ("standard_data", "2023-01-01", "2023-01-01"),
        ("aggregated_data", "2023-01-01", "2023-01-31"),
    ])
    def test_output_parity_main_extraction(airflow_client, legacy_output_dir, gcs_client, test_case_name, start_date, end_date):
        # 1. Trigger migrated DAG
        dag_run_id = airflow_client.trigger_dag("r_exis_v2_dag", execution_date=f"{start_date}T00:00:00Z")
        airflow_client.wait_for_dag_run_completion(dag_run_id)

        # 2. Define paths
        legacy_output_path = os.path.join(legacy_output_dir, f"legacy_output_{test_case_name}.csv")
        migrated_output_filename = f"output_data_{start_date.replace('-', '')}_{dag_run_id.replace('-', '_')}.csv"
        gcs_output_uri = f"gs://{GCS_LANDING_BUCKET}/r_exis_v2/{migrated_output_filename}"
        migrated_output_path = f"/tmp/{migrated_output_filename}"
        
        # 3. Download GCS file
        gcs_client.download_blob_to_file(gcs_output_uri, migrated_output_path)

        # 4. Compare files
        # Option A: Strict byte-for-byte comparison (best for identical environments)
        assert filecmp.cmp(legacy_output_path, migrated_output_path, shallow=False), \
            f"Output files differ for test case: {test_case_name}"

        # Option B: Semantic comparison (more robust for minor formatting differences like line endings, float precision)
        # df_legacy = pd.read_csv(legacy_output_path, sep='|', header=0, dtype=str) # Adjust sep/header/dtype as needed
        # df_migrated = pd.read_csv(migrated_output_path, sep='|', header=0, dtype=str)
        # pd.testing.assert_frame_equal(df_legacy, df_migrated, check_dtype=False, check_exact=False, rtol=1e-5,
        #                               obj=f"Semantic comparison failed for {test_case_name}")
    ```

---

### Test Case 3: Transformation Correctness - Complex Text Processing (Dataflow)

*   **Purpose:** Verify that complex `nawk`, `sed`, `perl` transformations are correctly replicated by the Dataflow pipeline. This is crucial for scenarios where BigQuery SQL is insufficient.
*   **Setup:**
    *   Identify a specific `r_exis_v2` configuration that uses complex `nawk`/`sed`/`perl` pipes.
    *   Create a small, representative input file (e.g., `test_data_complex.csv`) containing edge cases for the identified transformations (e.g., special characters, multi-line records, specific patterns for `sed`/`perl` regex).
    *   Ensure the `r_exis_v2_dataflow_pipeline.py` is deployed as a template and its `ProcessComplexText` DoFn accurately reflects the legacy logic.
    *   Configure the `exporter_config` with a `DATAFLOW_TRANSFORMATION_RULES` JSON object that the Dataflow pipeline can interpret.
*   **Action:**
    1.  **Legacy:** Manually run the relevant section of the `r_exis_v2` script (or a simplified version) with `test_data_complex.csv` as input, capturing the output.
    2.  **Migrated:**
        *   Upload `test_data_complex.csv` to a GCS bucket (e.g., `gs://your-gcs-temp-bucket/input/test_data_complex.csv`).
        *   Modify the `r_exis_v2_dag` (or create a dedicated test DAG) to trigger the `dataflow_complex_transformations` task with `test_data_complex.csv` as input and the specific `transformation_rules`.
        *   Alternatively, run the Dataflow pipeline directly via `gcloud dataflow jobs run` with the test input and parameters.
        *   Download the output file from GCS.
*   **Pass/Fail Criterion:**
    *   **Pass:** The output file from Dataflow is byte-for-byte identical (or semantically identical) to the output generated by the legacy `nawk`/`sed`/`perl` pipes.
    *   **Fail:** Any discrepancy in the transformed data.

    ```python
    # Conceptual Pytest assertion
    import filecmp
    import os

    def test_dataflow_complex_transformations(gcs_client, dataflow_job_runner, legacy_transformed_output_dir):
        input_filename = "test_data_complex_chars.csv"
        input_gcs_path = f"gs://{GCS_TEMP_BUCKET}/input/{input_filename}"
        output_gcs_path = f"gs://{GCS_LANDING_BUCKET}/dataflow_output/processed_{input_filename}"
        
        # Upload test input data to GCS
        gcs_client.upload_file_to_blob(f"test_data/{input_filename}", input_gcs_path)

        # Trigger Dataflow job (assuming dataflow_job_runner abstracts this)
        dataflow_job_id = dataflow_job_runner.run_dataflow_template(
            template_path=f"gs://{GCS_TEMP_BUCKET}/dataflow_templates/r_exis_v2_dataflow_pipeline",
            parameters={
                "input_file": input_gcs_path,
                "output_file": output_gcs_path,
                "transformation_rules": '{"replace_pattern": {"pattern": "old", "replacement": "new"}, "filter_condition": "active"}'
            }
        )
        dataflow_job_runner.wait_for_job_completion(dataflow_job_id)

        # Download Dataflow output
        migrated_output_path = f"/tmp/dataflow_output_{input_filename}"
        gcs_client.download_blob_to_file(output_gcs_path, migrated_output_path)

        # Compare with legacy output
        legacy_output_path = os.path.join(legacy_transformed_output_dir, f"legacy_transformed_{input_filename}")
        assert filecmp.cmp(legacy_output_path, migrated_output_path, shallow=False), \
            f"Dataflow output differs from legacy for {input_filename}"
    ```

---

### Test Case 4: External System Replacement - SFTP Distribution

*   **Purpose:** Verify that files are correctly transferred to an external SFTP server, replacing the legacy `sftp` command.
*   **Setup:**
    *   Configure `your_gcp_project_id.your_bigquery_dataset.exporter_config` with a `DISTRIBUTION` node specifying `method: SFTP`, `sftp_host`, `sftp_port`, `sftp_user`, `sftp_password_secret_name`, and `target_path`.
    *   Ensure the SFTP credentials are in Secret Manager.
    *   Set up a mock SFTP server (or a dedicated test SFTP server) that the Cloud Run service can access.
    *   Ensure the `external_distributor_cloud_run.py` service is deployed and has necessary IAM permissions.
*   **Action:**
    1.  **Legacy:** Run `r_exis_v2` to generate an output file and distribute it via SFTP. Verify the file appears on the legacy SFTP server.
    2.  **Migrated:** Trigger `r_exis_v2_dag`. Ensure the `export_to_gcs` task completes, followed by `queue_external_distribution` and `call_sftp_cloud_run`.
    3.  Monitor Cloud Run logs for `external_distributor_cloud_run` to confirm successful SFTP transfer.
    4.  Connect to the mock/test SFTP server and verify the presence and content of the transferred file.
*   **Pass/Fail Criterion:**
    *   **Pass:** The file appears on the target SFTP server with the correct name, content, and permissions. The `exporter_distribution_queue` table shows `status: COMPLETED` for the corresponding entry. Cloud Run logs indicate successful transfer.
    *   **Fail:** File not found, incorrect content, transfer errors, or incorrect status in `exporter_distribution_queue`.

    ```python
    # Conceptual Pytest assertion
    import pysftp # Requires pysftp library
    import os
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    def test_sftp_distribution(airflow_client, sftp_test_config, gcs_client):
        # 1. Trigger DAG
        dag_run_id = airflow_client.trigger_dag("r_exis_v2_dag", execution_date="2023-01-01T00:00:00Z")
        airflow_client.wait_for_dag_run_completion(dag_run_id)

        # 2. Get GCS path of the exported file
        gcs_output_filename = f"output_data_20230101_{dag_run_id.replace('-', '_')}.csv"
        gcs_output_uri = f"gs://{GCS_LANDING_BUCKET}/r_exis_v2/{gcs_output_filename}"
        
        # 3. Download GCS file to compare with SFTP content
        expected_content_path = f"/tmp/expected_sftp_content_{dag_run_id}.csv"
        gcs_client.download_blob_to_file(gcs_output_uri, expected_content_path)

        # 4. Connect to SFTP and verify
        with pysftp.Connection(
            sftp_test_config['host'],
            username=sftp_test_config['user'],
            password=sftp_test_config['password'], # In real test, fetch from Secret Manager
            port=sftp_test_config.get('port', 22)
        ) as sftp:
            remote_file_path = f"{sftp_test_config['target_path']}/{gcs_output_filename}"
            assert sftp.exists(remote_file_path), f"File not found on SFTP: {remote_file_path}"
            
            downloaded_sftp_file = f"/tmp/downloaded_sftp_file_{dag_run_id}.csv"
            sftp.get(remote_file_path, downloaded_sftp_file)
            
            assert filecmp.cmp(expected_content_path, downloaded_sftp_file, shallow=False), \
                f"Content of SFTP file differs from GCS source for run {dag_run_id}"

        # 5. Verify BigQuery distribution queue status
        bq_hook = BigQueryHook(gcp_conn_id="google_cloud_default")
        queue_status_query = f"""
            SELECT status
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.exporter_distribution_queue`
            WHERE run_id = '{dag_run_id}' AND distribution_method = 'SFTP'
            ORDER BY created_at DESC LIMIT 1;
        """
        queue_status = bq_hook.get_records(queue_status_query)[0][0]
        assert queue_status == "COMPLETED", f"SFTP distribution status in BQ is '{queue_status}', expected 'COMPLETED'"
    ```

---

### Test Case 5: External System Replacement - Email Notification

*   **Purpose:** Verify that email notifications are sent correctly upon job completion or failure, replacing the legacy `mailx` command.
*   **Setup:**
    *   Configure `your_gcp_project_id.your_bigquery_dataset.exporter_config` with a `DISTRIBUTION` node specifying `method: EMAIL`, `email_on_success: true`, and `email_recipients` (e.g., `["test@example.com"]`).
    *   Ensure SMTP credentials (e.g., SendGrid API key) are in Secret Manager.
    *   Set up a test email address or a mail trap service (e.g., Mailtrap, Ethereal) to capture sent emails.
    *   Ensure the `external_distributor_cloud_run.py` service is deployed.
*   **Action:**
    1.  **Legacy:** Run `r_exis_v2` to completion. Verify an email is received by the configured recipients.
    2.  **Migrated:** Trigger `r_exis_v2_dag`. Ensure `end_dag_status_success` (or `end_dag_status_failure`) and `call_email_cloud_run` tasks execute.
    3.  Monitor Cloud Run logs for `external_distributor_cloud_run`.
    4.  Check the test email inbox/mail trap for the notification.
*   **Pass/Fail Criterion:**
    *   **Pass:** An email is received by the specified recipients with the correct subject, sender, and body content (job name, run ID, status, message). Cloud Run logs indicate successful email sending.
    *   **Fail:** Email not received, incorrect content, or errors in Cloud Run logs.

    ```python
    # Conceptual Pytest assertion (requires a mail trap client)
    import pytest

    def test_email_notification(airflow_client, mail_trap_client):
        # 1. Trigger DAG for a successful run
        dag_run_id = airflow_client.trigger_dag("r_exis_v2_dag", execution_date="2023-01-01T00:00:00Z")
        airflow_client.wait_for_dag_run_completion(dag_run_id)

        # 2. Check mail trap for received emails
        recipient_email = "test@example.com" # Must match config
        received_emails = mail_trap_client.get_emails(recipient=recipient_email)
        
        assert len(received_emails) >= 1, "No emails received by the test recipient."
        
        # 3. Find the relevant email and assert its content
        email_found = False
        for email in received_emails:
            if f"r_exis_v2 Exporter Job Status: SUCCESS" in email.subject and \
               f"Job Name: r_exis_v2" in email.body and \
               f"Run ID: {dag_run_id}" in email.body and \
               "Exporter job completed successfully." in email.body:
                email_found = True
                break
        
        assert email_found, f"Expected success email for run {dag_run_id} not found or content incorrect."

        # Optional: Test failure email by triggering a DAG that's configured to fail
        # dag_run_id_fail = airflow_client.trigger_dag("r_exis_v2_dag_failing_config", execution_date="2023-01-02T00:00:00Z")
        # airflow_client.wait_for_dag_run_completion(dag_run_id_fail)
        # ... assert failure email content
    ```

---

### Test Case 6: Data Quality - Row Count and Schema Assertions

*   **Purpose:** Verify that the exported data maintains the expected row count and schema (column names, data types) compared to the legacy output. This is a crucial data quality check.
*   **Setup:**
    *   Use the same setup as Test Case 2 (Output Parity).
    *   Define expected schema (column names, types) for the output file.
*   **Action:**
    1.  **Legacy:** Run `r_exis_v2` and capture the output file. Determine its row count (e.g., using `wc -l`) and schema (e.g., using `head -1` then manual inspection or a schema inference tool).
    2.  **Migrated:** Trigger `r_exis_v2_dag`.
    3.  After `export_to_gcs` completes, query the temporary BigQuery table (`temp_r_exis_output_...`) or the exported GCS file.
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   The row count of the data in the temporary BigQuery table (or exported GCS file) matches the row count of the legacy output file.
        *   The schema (column names, order, and inferred data types) of the temporary BigQuery table (or exported GCS file) matches the legacy output schema.
    *   **Fail:** Mismatch in row count or schema.

    ```python
    # Conceptual Pytest assertion
    import pandas as pd
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    def test_row_count_and_schema(airflow_client, legacy_output_path, gcs_client):
        # 1. Trigger DAG
        dag_run_id = airflow_client.trigger_dag("r_exis_v2_dag", execution_date="2023-01-01T00:00:00Z")
        airflow_client.wait_for_dag_run_completion(dag_run_id)

        # 2. Get row count and schema from legacy file
        df_legacy = pd.read_csv(legacy_output_path, sep='|', header=0, dtype=str) # Adjust sep/header/dtype
        legacy_row_count = len(df_legacy)
        legacy_schema_columns = list(df_legacy.columns)

        # 3. Query BigQuery for row count and schema of the temporary table
        bq_hook = BigQueryHook(gcp_conn_id="google_cloud_default")
        temp_bq_table = f"`{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.temp_r_exis_output_{dag_run_id.replace('-', '_')}`"

        migrated_row_count_query = f"SELECT COUNT(*) FROM {temp_bq_table};"
        migrated_row_count = bq_hook.get_records(migrated_row_count_query)[0][0]
        
        migrated_schema_query = f"""
            SELECT column_name
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = 'temp_r_exis_output_{dag_run_id.replace('-', '_')}'
            ORDER BY ordinal_position;
        """
        migrated_schema_records = bq_hook.get_records(migrated_schema_query)
        migrated_schema_columns = [rec[0] for rec in migrated_schema_records]

        # 4. Assertions
        assert migrated_row_count == legacy_row_count, \
            f"Row count mismatch: Legacy={legacy_row_count}, Migrated={migrated_row_count}"
        assert migrated_schema_columns == legacy_schema_columns, \
            f"Schema column order/names mismatch: Legacy={legacy_schema_columns}, Migrated={migrated_schema_columns}"
        
        # Optional: Compare data types (requires more sophisticated parsing of legacy schema)
        # For example, if legacy_schema_dtypes = {'col1': 'int64', 'col2': 'object'}
        # migrated_schema_dtypes_query = f"SELECT column_name, data_type FROM ... WHERE table_name = '{temp_bq_table}'"
        # ... compare data types
    ```

---

### Test Case 7: Transformation Correctness - NULL Handling

*   **Purpose:** Verify that NULL values are handled consistently across the migration, especially when moving from Oracle's empty string = NULL to BigQuery's distinct NULL.
*   **Setup:**
    *   Create a specific test dataset in both Oracle and BigQuery source tables that includes columns with:
        *   Explicit `NULL` values.
        *   Empty strings (`''`).
        *   Columns that are `NOT NULL` in schema but might receive `NULL`s due to joins.
    *   Ensure `OUTPUT_SQL` and any Dataflow transformations explicitly handle NULLs as per the legacy behavior (e.g., converting BigQuery `NULL` to empty string if legacy output expects it).
*   **Action:**
    1.  **Legacy:** Run `r_exis_v2` with the test dataset. Capture the output file.
    2.  **Migrated:** Trigger `r_exis_v2_dag` with the same test dataset. Download the exported GCS file.
*   **Pass/Fail Criterion:**
    *   **Pass:** The representation of NULLs (e.g., empty string, `\N`, or actual `NULL` if the format supports it) in the migrated output file is identical to the legacy output file.
    *   **Fail:** Mismatch in NULL representation or unexpected data due to NULL handling differences.

    ```python
    # Conceptual Pytest assertion
    import pandas as pd

    def test_null_handling(airflow_client, legacy_output_with_nulls_path, gcs_client):
        # 1. Trigger DAG
        dag_run_id = airflow_client.trigger_dag("r_exis_v2_dag", execution_date="2023-01-01T00:00:00Z")
        airflow_client.wait_for_dag_run_completion(dag_run_id)

        # 2. Define paths
        gcs_output_filename = f"output_data_20230101_{dag_run_id.replace('-', '_')}.csv"
        gcs_output_uri = f"gs://{GCS_LANDING_BUCKET}/r_exis_v2/{gcs_output_filename}"
        migrated_output_path = f"/tmp/migrated_output_with_nulls_{dag_run_id}.csv"
        
        # 3. Download GCS file
        gcs_client.download_blob_to_file(gcs_output_uri, migrated_output_path)

        # 4. Compare files, explicitly handling NA values based on expected output format
        # Assume legacy output represents NULLs as empty strings
        df_legacy = pd.read_csv(legacy_output_with_nulls_path, sep='|', na_values=[''], keep_default_na=False, dtype=str)
        df_migrated = pd.read_csv(migrated_output_path, sep='|', na_values=[''], keep_default_na=False, dtype=str)
        
        pd.testing.assert_frame_equal(df_legacy, df_migrated, check_dtype=False, check_exact=True), \
            f"NULL handling mismatch for run {dag_run_id}"
    ```

---

### Test Case 8: Partitioning Logic (File and SQL)

*   **Purpose:** Verify that the `get_file_partitions_bq` and `get_sql_splits_bq` BigQuery Stored Procedures correctly generate partition boundaries and labels, matching the legacy `getsubintervalls` and `getsqlsplits` functions.
*   **Setup:**
    *   Ensure `get_file_partitions_bq` and `get_sql_splits_bq` are deployed.
    *   For file partitioning, define a `META` config with `partition_unit` (e.g., `DAY`, `MONTH`).
    *   For SQL partitioning, identify a BigQuery table and a numeric/date column for splitting.
*   **Action:**
    1.  **Legacy:**
        *   Run `r_exis_v2` with file partitioning enabled for a specific date range (e.g., 5 days). Observe the generated file names and their content.
        *   Manually execute the logic of `getsqlsplits` for a sample table and column, noting the generated SQL WHERE clauses.
    2.  **Migrated:**
        *   Trigger `r_exis_v2_dag` for the same date range.
        *   Inspect the `file_partitions` XCom output from `determine_file_partitions`.
        *   Manually call `get_sql_splits_bq` in BigQuery for the same sample table/column and `p_num_splits`.
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   The `file_partitions` XCom contains an array of structs with `start_date`, `end_date`, and `label` that precisely match the file partitions generated by the legacy script.
        *   The output of `get_sql_splits_bq` (e.g., `split_id`, `min_value`, `max_value`) correctly defines the SQL split ranges as expected from the legacy `getsqlsplits`.
    *   **Fail:** Mismatch in partition boundaries, labels, or generated split ranges.

    ```sql
    -- Example BigQuery SQL assertion for file partitions
    -- This would be run as part of a Python test or directly in BQ console
    DECLARE p_details ARRAY<STRUCT<partition_start_date DATE, partition_end_date DATE, partition_label STRING>>;
    CALL `your_gcp_project_id.your_bigquery_dataset.get_file_partitions_bq`(DATE '2023-01-01', DATE '2023-01-05', 'DAY', p_details);
    SELECT * FROM UNNEST(p_details) ORDER BY partition_start_date;
    /* Expected output for 'DAY' partition_unit:
    partition_start_date | partition_end_date | partition_label
    ---------------------|--------------------|----------------
    2023-01-01           | 2023-01-01         | 20230101
    2023-01-02           | 2023-01-02         | 20230102
    2023-01-03           | 2023-01-03         | 20230103
    2023-01-04           | 2023-01-04         | 20230104
    2023-01-05           | 2023-01-05         | 20230105
    */

    -- Example BigQuery SQL assertion for SQL splits
    DECLARE p_splits ARRAY<STRUCT<split_id INT, min_value STRING, max_value STRING>>;
    CALL `your_gcp_project_id.your_bigquery_dataset.get_sql_splits_bq`('your_source_table', 'id_column', 4, p_splits);
    SELECT * FROM UNNEST(p_splits) ORDER BY split_id;
    /* Expected output (assuming id_column min=1, max=1000):
    split_id | min_value | max_value
    ---------|-----------|----------
    1        | 1         | 250
    2        | 250       | 500
    3        | 500       | 750
    4        | 750       | 1000
    */
    ```

---

### Test Case 9: Logging and Status Updates

*   **Purpose:** Verify that the Airflow DAG correctly logs events to `exporter_log` and updates job status in `exporter_status` at various stages (start, success, failure), replicating the `DWMSG_*` functions and `$LogDatei` behavior.
*   **Setup:**
    *   Ensure `exporter_log` and `exporter_status` tables are created.
    *   Ensure the `_log_job_status` Python callable is correctly implemented in the DAG.
*   **Action:**
    1.  **Successful Run:** Trigger `r_exis_v2_dag` and let it complete successfully. Note the `run_id`.
    2.  **Failed Run:** Trigger `r_exis_v2_dag` but introduce a deliberate failure (e.g., by making an SQL query invalid in `OUTPUT_SQL` or causing the Dataflow job to fail). Note the `run_id`.
    3.  Query `your_gcp_project_id.your_bigquery_dataset.exporter_log` and `your_gcp_project_id.your_bigquery_dataset.exporter_status` tables in BigQuery.
    4.  Compare with legacy `$LogDatei` and `dwh$ta_k_meldungen` entries.
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   `exporter_status` table correctly reflects `RUNNING`, then `SUCCESS` (or `FAILED`) for the job, with accurate `start_time`, `end_time`, `duration_seconds`, and `error_message` (if failed).
        *   `exporter_log` contains detailed entries for each major task, with correct `log_level`, `task_id`, and `message`, mirroring the granularity of the legacy log file.
    *   **Fail:** Missing log entries, incorrect status, or inaccurate timestamps/messages.

    ```sql
    -- Example BigQuery SQL assertion for successful run status
    SELECT status, start_time, end_time, duration_seconds, error_message
    FROM `your_gcp_project_id.your_bigquery_dataset.exporter_status`
    WHERE job_name = 'r_exis_v2' AND last_run_id = '{{ successful_run_id }}';
    -- Expected: status = 'SUCCESS', start_time, end_time, duration_seconds populated, error_message IS NULL.

    -- Example BigQuery SQL assertion for failed run status
    SELECT status, start_time, end_time, duration_seconds, error_message
    FROM `your_gcp_project_id.your_bigquery_dataset.exporter_status`
    WHERE job_name = 'r_exis_v2' AND last_run_id = '{{ failed_run_id }}';
    -- Expected: status = 'FAILED', start_time, end_time, duration_seconds populated, error_message IS NOT NULL.

    -- Example BigQuery SQL assertion for log entries (successful run)
    SELECT log_level, task_id, message
    FROM `your_gcp_project_id.your_bigquery_dataset.exporter_log`
    WHERE job_name = 'r_exis_v2' AND run_id = '{{ successful_run_id }}'
    ORDER BY log_timestamp;
    -- Expected: A sequence of log entries corresponding to task execution, e.g.,
    -- INFO, start_dag_status, "DAG started"
    -- INFO, get_job_config, "Fetched job config..."
    -- ...
    -- INFO, end_dag_status_success, "DAG completed successfully"
    ```

---

### Test Case 10: Incremental Loading Logic

*   **Purpose:** Verify that the BigQuery `MERGE` statements (or equivalent logic) correctly implement the incremental merge logic previously handled by shell scripts (`nawk`, `grep`) based on timestamps (`TIMESTAMP_CSV`, `DELETE_OLDER`, `DELETE_NEWER`).
*   **Setup:**
    *   Identify a specific `r_exis_v2` configuration that uses incremental loading.
    *   Create a target BigQuery table (`your_target_table`) that will be incrementally updated.
    *   Prepare a staging BigQuery table (`your_staging_table`) with new, updated, and deleted records relative to `your_target_table`.
    *   Ensure the Dataform model for incremental loading (or a dedicated BigQuery `MERGE` task in Airflow) is correctly implemented.
*   **Action:**
    1.  **Legacy:**
        *   Run the legacy `r_exis_v2` script with incremental parameters.
        *   Observe the changes in the target file/database.
    2.  **Migrated:**
        *   Trigger `r_exis_v2_dag` with parameters that simulate an incremental run.
        *   The `OUTPUT_SQL` should extract data into a staging table.
        *   A subsequent task (e.g., a Dataform model or `BigQueryExecuteQueryOperator`) should execute the `MERGE` statement from staging to target.
        *   Query the `your_target_table` in BigQuery.
*   **Pass/Fail Criterion:**
    *   **Pass:** The data in `your_target_table` in BigQuery accurately reflects the incremental updates (inserts, updates, deletes) as performed by the legacy script. The final state of the target table should be identical.
    *   **Fail:** Mismatched row counts, incorrect updates, or missing/extra records in the target table.

    ```sql
    -- Example BigQuery SQL assertion for incremental merge
    -- Assuming a target table 'my_incremental_table' and a staging table 'my_staging_table'
    -- After running the incremental DAG:
    SELECT COUNT(*) FROM `your_gcp_project_id.your_bigquery_dataset.my_incremental_table`;
    -- Compare this count with the expected count after incremental load.

    -- To find differences between migrated target table and a known good state (e.g., legacy output loaded into BQ)
    SELECT * FROM `your_gcp_project_id.your_bigquery_dataset.my_incremental_table`
    EXCEPT DISTINCT
    SELECT * FROM `your_gcp_project_id.your_bigquery_dataset.legacy_equivalent_target_table_snapshot`;
    -- Expected result: 0 rows (no differences)
    ```

---

### Test Case 11: Edge Cases - Empty Input, No Data, Special Characters

*   **Purpose:** Verify the robustness of the migrated job when dealing with edge cases in input data.
*   **Setup:**
    *   Create specific test datasets in BigQuery source tables:
        *   An empty table.
        *   A table with only header but no data rows.
        *   A table with data containing various special characters (e.g., commas, quotes, newlines within fields, non-ASCII characters) that might break CSV/delimiter parsing or regex.
    *   Ensure `exporter_config` is set up for these scenarios.
*   **Action:**
    1.  **Legacy:** Run `r_exis_v2` with each edge case dataset. Observe the output files (empty, header only, correctly escaped/formatted data).
    2.  **Migrated:** Trigger `r_exis_v2_dag` for each edge case. Download the exported GCS files.
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   For empty input, the output file is empty or contains only a header, matching legacy behavior.
        *   For no data, the output file is empty or contains only a header, matching legacy behavior.
        *   For special characters, the output file correctly escapes/formats the data, matching the legacy output.
    *   **Fail:** Errors during execution, malformed output files, or data corruption.

    ```python
    # Conceptual Pytest assertion
    import filecmp
    import os

    @pytest.mark.parametrize("test_scenario, input_file_path, expected_output_file_path", [
        ("empty_input", "empty_data.csv", "legacy_empty_output.csv"),
        ("header_only", "header_only.csv", "legacy_header_only_output.csv"),
        ("special_chars", "data_with_special_chars.csv", "legacy_special_chars_output.csv"),
    ])
    def test_edge_cases(airflow_client, gcs_client, test_scenario, input_file_path, expected_output_file_path):
        # 1. Prepare BigQuery source table with specific test data (empty, header-only, special chars)
        #    (This step would involve loading data into BQ before triggering the DAG)

        # 2. Trigger DAG
        dag_run_id = airflow_client.trigger_dag(f"r_exis_v2_dag_{test_scenario}", execution_date="2023-01-01T00:00:00Z")
        airflow_client.wait_for_dag_run_completion(dag_run_id)

        # 3. Define paths
        gcs_output_filename = f"output_data_20230101_{dag_run_id.replace('-', '_')}.csv"
        gcs_output_uri = f"gs://{GCS_LANDING_BUCKET}/r_exis_v2/{gcs_output_filename}"
        migrated_output_path = f"/tmp/migrated_output_{test_scenario}_{dag_run_id}.csv"
        
        # 4. Download GCS file
        gcs_client.download_blob_to_file(gcs_output_uri, migrated_output_path)

        # 5. Compare with legacy output
        assert filecmp.cmp(expected_output_file_path, migrated_output_path, shallow=False), \
            f"Output for '{test_scenario}' differs from legacy."
    ```

---

### Test Case 12: Cleanup Tasks

*   **Purpose:** Verify that temporary BigQuery tables and GCS files created during the DAG run are properly cleaned up, regardless of whether the DAG succeeds or fails.
*   **Setup:**
    *   Ensure `cleanup_temp_bq_table` and `cleanup_temp_gcs_files` tasks are correctly configured in the DAG with `trigger_rule=TriggerRule.ALL_DONE`.
*   **Action:**
    1.  Trigger `r_exis_v2_dag` for a successful run. Note the `run_id`.
    2.  Trigger `r_exis_v2_dag` for a failed run (e.g., by making `OUTPUT_SQL` invalid). Note the `run_id`.
    3.  After each run, check BigQuery for the existence of `temp_r_exis_output_...` tables.
    4.  Check GCS for the existence of temporary Dataflow files (`dataflow_temp/r_exis_v2_...`).
*   **Pass/Fail Criterion:**
    *   **Pass:** The `temp_r_exis_output_...` BigQuery table does not exist after the DAG run (successful or failed). The temporary Dataflow files in GCS are removed.
    *   **Fail:** Temporary resources persist after the DAG completes.

    ```sql
    -- Example BigQuery SQL assertion for cleanup (to be run after DAG completion)
    SELECT table_name
    FROM `your_gcp_project_id.your_bigquery_dataset.INFORMATION_SCHEMA.TABLES`
    WHERE table_name LIKE 'temp_r_exis_output_%'
      AND creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR); -- Check for recently created temp tables
    -- Expected result: 0 rows

    -- Example gcloud command for GCS cleanup check (to be run after DAG completion)
    # gcloud storage ls gs://your-gcs-temp-bucket/dataflow_temp/r_exis_v2_YOUR_SUCCESSFUL_RUN_ID_PART/
    # gcloud storage ls gs://your-gcs-temp-bucket/dataflow_temp/r_exis_v2_YOUR_FAILED_RUN_ID_PART/
    # Expected result for both: No objects found or empty output
    ```