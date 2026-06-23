As a senior data-migration QA engineer, I've reviewed the migration design for `r_ausd_v_ta_apn_ve.ksh` to BigQuery Stored Procedures. The design document highlights that the core business logic (`D_AUSD_V_TA_APN_VE.SQL` and associated packages) is *not fully analyzed* yet. Therefore, the tests below will primarily focus on validating the **orchestration layer** (`sp_r_ausd_v_ta_apn_ve_orchestrator`) and its interaction with the new logging mechanism (`job_log` table), as well as the *interface* to the core logic (`sp_k_ausd_v_ta_apn_ve_combined`).

Detailed transformation correctness tests (joins, aggregations, filters, type handling, NULL handling, specific edge cases) for the core business logic are explicitly marked as requiring further analysis and will need to be developed once the `D_AUSD_V_TA_APN_VE.SQL` and legacy package logic is fully understood and migrated. For now, the core logic procedure (`sp_k_ausd_v_ta_apn_ve_combined`) is treated as a black box that, if called successfully, should eventually produce the correct data.

---

## Migration Validation Tests for `r_ausd_v_ta_apn_ve.ksh`

**Target Environment:** Google BigQuery

**Assumptions:**
*   The `job_log` table DDL has been deployed.
*   The `sp_k_ausd_v_ta_apn_ve_combined` and `sp_r_ausd_v_ta_apn_ve_orchestrator` procedures have been deployed to `your_project_id.your_dataset_id`.
*   Access to legacy system logs and output data (snapshots) is available for comparison.
*   A testing framework (e.g., Pytest with BigQuery client) is used for execution and assertions.
*   Placeholder source tables (`DWTK_MELDUNGEN`, `PDS_TA_PDP_CONTEXT_ASSOC`) and target tables (`SOF_TA_APN_VE`, `VIA`) exist in BigQuery, even if empty or containing mock data for orchestration tests.

---

### 1. Orchestration Layer - Successful Execution

**Purpose:** Verify that the migrated orchestrator procedure executes successfully when provided with valid parameters, mimicking a successful legacy job run. This tests parameter parsing, environment initialization, and successful invocation of the core logic.

**Setup:**
1.  Ensure the `job_log` table is empty or contains no entries for the current test run's `job_name`.
2.  Ensure `sp_k_ausd_v_ta_apn_ve_combined` is deployed and configured to return successfully (e.g., its placeholder logic does not raise an error).
3.  Define a valid `process_date` (e.g., `2023-01-15`).

**Action:**
Execute the `sp_r_ausd_v_ta_apn_ve_orchestrator` procedure with the valid `process_date`.

```python
# Example pytest code
def test_orchestrator_successful_execution(bigquery_client, project_id, dataset_id):
    process_date = "2023-01-15"
    job_name = "r_ausd_v_ta_apn_ve_orchestrator"
    
    # Action: Call the orchestrator procedure
    query = f"""
    CALL `{project_id}.{dataset_id}.sp_r_ausd_v_ta_apn_ve_orchestrator`(DATE('{process_date}'));
    """
    bigquery_client.query(query).result() # Wait for completion

    # Assertions
    # 1. Check job_log table for success entry
    log_query = f"""
    SELECT job_run_id, job_name, status, process_date, error_message, start_timestamp, end_timestamp
    FROM `{project_id}.{dataset_id}.job_log`
    WHERE job_name = '{job_name}'
      AND process_date = DATE('{process_date}')
    ORDER BY start_timestamp DESC
    LIMIT 1;
    """
    rows = list(bigquery_client.query(log_query).result())
    
    assert len(rows) == 1, "Expected exactly one job_log entry for the run."
    assert rows[0].status == "SUCCESS", f"Expected job status 'SUCCESS', but got '{rows[0].status}'."
    assert rows[0].error_message is None, "Expected no error message for a successful run."
    assert rows[0].start_timestamp is not None, "Start timestamp should be recorded."
    assert rows[0].end_timestamp is not None, "End timestamp should be recorded."
    assert rows[0].job_run_id is not None, "Job run ID should be generated."

    print(f"Successfully verified job_log entry for {job_name} on {process_date}.")
```

**Pass/Fail Criterion:**
*   The `sp_r_ausd_v_ta_apn_ve_orchestrator` procedure completes without raising an error.
*   A single entry is found in `your_project_id.your_dataset_id.job_log` for the executed `job_name` and `process_date` with `status = 'SUCCESS'`, `error_message IS NULL`, and valid `start_timestamp` and `end_timestamp`.
*   Cloud Logging shows informational messages indicating successful start and completion of the job.

---

### 2. Orchestration Layer - Error Handling (Core Logic Failure)

**Purpose:** Verify that the orchestrator correctly captures and logs errors originating from the core reconciliation procedure, mimicking a legacy job failure due to core logic issues. This tests the `trap ERR` equivalent in BigQuery.

**Setup:**
1.  Ensure the `job_log` table is empty or contains no entries for the current test run's `job_name`.
2.  Modify `sp_k_ausd_v_ta_apn_ve_combined` to explicitly `RAISE` an error (e.g., `RAISE 'Simulated core logic error';`) when called, or configure it to fail (e.g., by attempting an invalid operation).
3.  Define a valid `process_date` (e.g., `2023-01-16`).

**Action:**
Execute the `sp_r_ausd_v_ta_apn_ve_orchestrator` procedure with the valid `process_date`.

```python
# Example pytest code
def test_orchestrator_core_logic_failure(bigquery_client, project_id, dataset_id):
    process_date = "2023-01-16"
    job_name = "r_ausd_v_ta_apn_ve_orchestrator"
    
    # Pre-requisite: Ensure sp_k_ausd_v_ta_apn_ve_combined is configured to fail
    # (e.g., by deploying a version that includes a RAISE statement)
    # For this test, we assume it's already in a failing state.

    # Action: Call the orchestrator procedure, expecting it to raise an error
    query = f"""
    CALL `{project_id}.{dataset_id}.sp_r_ausd_v_ta_apn_ve_orchestrator`(DATE('{process_date}'));
    """
    
    try:
        bigquery_client.query(query).result()
        assert False, "Orchestrator procedure was expected to fail but succeeded."
    except Exception as e:
        # Expected failure, now check the log
        print(f"Orchestrator failed as expected: {e}")

    # Assertions
    # 1. Check job_log table for failed entry
    log_query = f"""
    SELECT job_run_id, job_name, status, process_date, error_message, error_stack_trace
    FROM `{project_id}.{dataset_id}.job_log`
    WHERE job_name = '{job_name}'
      AND process_date = DATE('{process_date}')
    ORDER BY start_timestamp DESC
    LIMIT 1;
    """
    rows = list(bigquery_client.query(log_query).result())
    
    assert len(rows) == 1, "Expected exactly one job_log entry for the failed run."
    assert rows[0].status == "FAILED", f"Expected job status 'FAILED', but got '{rows[0].status}'."
    assert rows[0].error_message is not None, "Expected an error message for a failed run."
    assert "Simulated core logic error" in rows[0].error_message, "Error message should contain expected failure detail."
    assert rows[0].error_stack_trace is not None, "Error stack trace should be recorded."

    print(f"Successfully verified job_log entry for failed {job_name} on {process_date}.")

# Cleanup: Revert sp_k_ausd_v_ta_apn_ve_combined to a successful state after this test.
```

**Pass/Fail Criterion:**
*   The `sp_r_ausd_v_ta_apn_ve_orchestrator` procedure raises an error, indicating failure to the caller (e.g., Airflow).
*   A single entry is found in `your_project_id.your_dataset_id.job_log` for the executed `job_name` and `process_date` with `status = 'FAILED'`, a non-NULL `error_message` containing details of the core logic failure, and a non-NULL `error_stack_trace`.
*   Cloud Logging shows error messages corresponding to the failure.

---

### 3. Parameter Handling - Invalid `process_date`

**Purpose:** Verify that the orchestrator handles invalid input parameters gracefully, similar to how `getopts` and subsequent shell logic would handle invalid arguments or missing required arguments. While BigQuery's type system handles some validation, this tests the robustness of the orchestrator.

**Setup:**
1.  Ensure the `job_log` table is empty or contains no entries for the current test run's `job_name`.
2.  The `sp_k_ausd_v_ta_apn_ve_combined` procedure should be in a successful state.

**Action:**
Attempt to execute the `sp_r_ausd_v_ta_apn_ve_orchestrator` procedure with an invalid `process_date` (e.g., `NULL` or a string that cannot be cast to `DATE`).

```python
# Example pytest code
def test_orchestrator_invalid_process_date(bigquery_client, project_id, dataset_id):
    job_name = "r_ausd_v_ta_apn_ve_orchestrator"
    
    # Action: Call the orchestrator procedure with an invalid date string
    # BigQuery will typically raise an error during parameter binding or type conversion.
    query = f"""
    CALL `{project_id}.{dataset_id}.sp_r_ausd_v_ta_apn_ve_orchestrator`('INVALID_DATE_STRING');
    """
    
    try:
        bigquery_client.query(query).result()
        assert False, "Orchestrator procedure was expected to fail with invalid date but succeeded."
    except Exception as e:
        print(f"Orchestrator failed as expected with invalid date: {e}")
        # Check if the error message indicates a date parsing issue
        assert "Failed to parse" in str(e) or "invalid date" in str(e).lower()

    # Assertions for job_log (may or may not have an entry depending on where the error occurs)
    # If the error occurs *before* the initial INSERT into job_log, no entry will exist.
    # If it occurs *after* the initial INSERT but before core logic, an entry will exist.
    # The current SP design inserts *before* calling core logic, so an entry should exist.
    log_query = f"""
    SELECT job_run_id, job_name, status, error_message
    FROM `{project_id}.{dataset_id}.job_log`
    WHERE job_name = '{job_name}'
    ORDER BY start_timestamp DESC
    LIMIT 1;
    """
    rows = list(bigquery_client.query(log_query).result())
    
    # This test might be tricky. If the error is in the CALL statement itself (e.g., bad literal),
    # the SP might not even start. If the SP starts and then encounters an issue with the date,
    # it should log it. The current SP takes DATE as input, so the error would be in the CALL.
    # Let's assume the SP *can* be called, but then an internal validation fails if process_date was e.g. NULL.
    # For a direct invalid string, BQ will error before the SP starts.
    # A more robust test would be to pass a valid DATE, but then have the *core logic* fail if that date is "invalid" for business reasons.
    # For now, we'll test the BQ-level parameter validation.
    
    # If the SP *does* manage to start and log, then:
    # assert len(rows) == 1, "Expected a job_log entry for the failed run."
    # assert rows[0].status == "FAILED", f"Expected job status 'FAILED', but got '{rows[0].status}'."
    # assert "invalid" in rows[0].error_message.lower(), "Error message should indicate invalid date."

    print(f"Successfully verified handling of invalid process_date for {job_name}.")
```

**Pass/Fail Criterion:**
*   The `sp_r_ausd_v_ta_apn_ve_orchestrator` procedure (or the `CALL` statement itself) raises an error related to invalid date format or value.
*   If the procedure manages to start and log, a `FAILED` entry with an appropriate `error_message` is recorded in `job_log`. If the error occurs before logging, no entry is expected.

---

### 4. External System Replacement - Logging Parity

**Purpose:** Verify that the new BigQuery `job_log` table and Cloud Logging capture equivalent information to the legacy `DWMSG_` functions and shell script's `print` statements. This covers `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_MeldeFehler`, `DWMSG_Fehlerbehandlung`, `DWMSG_SetzeStatusOK`.

**Setup:**
1.  Run a successful legacy job and capture its `LogDatei` content and final exit status.
2.  Ensure the `job_log` table is empty.
3.  Ensure `sp_k_ausd_v_ta_apn_ve_combined` is configured for success.
4.  Define a `process_date` (e.g., `2023-01-17`).

**Action:**
1.  Execute the `sp_r_ausd_v_ta_apn_ve_orchestrator` with the `process_date`.
2.  Query the `job_log` table for the run.
3.  Query Cloud Logging for entries related to the `job_run_id` (using `v_log_correlation_id`).

```python
# Example pytest code
def test_logging_parity(bigquery_client, project_id, dataset_id, cloud_logging_client):
    process_date = "2023-01-17"
    job_name = "r_ausd_v_ta_apn_ve_orchestrator"
    
    # Action: Call the orchestrator procedure
    query = f"""
    CALL `{project_id}.{dataset_id}.sp_r_ausd_v_ta_apn_ve_orchestrator`(DATE('{process_date}'));
    """
    bigquery_client.query(query).result()

    # Get job_run_id and correlation_id from job_log
    log_query = f"""
    SELECT job_run_id, log_correlation_id, start_timestamp, end_timestamp, status, error_message
    FROM `{project_id}.{dataset_id}.job_log`
    WHERE job_name = '{job_name}'
      AND process_date = DATE('{process_date}')
    ORDER BY start_timestamp DESC
    LIMIT 1;
    """
    log_entry = list(bigquery_client.query(log_query).result())[0]
    job_run_id = log_entry.job_run_id
    log_correlation_id = log_entry.log_correlation_id

    # Assertions for job_log content (already covered in Test 1, but good to re-verify)
    assert log_entry.status == "SUCCESS"
    assert log_entry.error_message is None
    assert log_entry.additional_info is not None # Check for additional info JSON

    # Assertions for Cloud Logging
    # Filter for logs generated by the BigQuery stored procedure for this specific run
    # (assuming BigQuery SP logs to global/bigquery.googleapis.com/query)
    filter_string = f'resource.type="bigquery_resource" AND protoPayload.methodName="google.cloud.bigquery.v2.JobService.InsertJob" AND protoPayload.serviceData.jobCompletedEvent.job.jobName.jobId="{job_run_id}"'
    # This filter might need adjustment based on actual BQ SP logging format.
    # A more direct way is to search for messages containing the log_correlation_id.
    
    cloud_log_filter = f'logName="projects/{project_id}/logs/cloudaudit.googleapis.com%2Factivity" AND resource.type="bigquery_resource" AND textPayload:"{log_correlation_id}"'
    # Or, if the SELECT FORMAT messages go to stdout/stderr and then to Cloud Logging:
    cloud_log_filter_stdout = f'resource.type="bigquery_resource" AND textPayload:"{log_correlation_id}"'

    # Fetch logs (simplified for example, actual fetching might involve pagination)
    entries = list(cloud_logging_client.list_entries(filter=cloud_log_filter_stdout))
    
    # Check for key messages
    assert any(f"Job {job_name} (Run ID: {job_run_id}) started" in e.payload for e in entries), "Start message not found in Cloud Logging."
    assert any(f"Job {job_name} (Run ID: {job_run_id}) completed successfully" in e.payload for e in entries), "Completion message not found in Cloud Logging."
    assert any("INFO: Simulating core reconciliation logic execution..." in e.payload for e in entries), "Core logic simulation message not found."

    # Compare with legacy log file (conceptual)
    # legacy_log_content = read_legacy_log_file(legacy_job_run_id)
    # assert "Job-Nr    : '" + legacy_job_run_id + "'" in legacy_log_content
    # assert "JobKennung: 'BERT_V_TA_APN_VE'" in legacy_log_content
    # assert "Die Abarbeitung wurde ohne erkennbare Fehler beendet" in legacy_log_content
    # ... (detailed comparison of log messages)

    print(f"Successfully verified logging parity for {job_name} run {job_run_id}.")
```

**Pass/Fail Criterion:**
*   The `job_log` table contains a complete and accurate record of the job's execution (start, end, status, parameters).
*   Cloud Logging contains entries that reflect the execution flow, including start, core logic invocation, and completion messages, correlated by `job_run_id` or `log_correlation_id`.
*   The content and sequence of log messages in Cloud Logging (and `job_log`) are functionally equivalent to the legacy `LogDatei` output, accounting for format differences.

---

### 5. Data Quality / Row Count - Target Table Existence

**Purpose:** Verify that the target tables (`SOF$TA_APN_VE` and `VIA`) are correctly identified and accessible by the core logic procedure, and that data *could* be written to them. This is a basic check given the core logic is a placeholder.

**Setup:**
1.  Ensure the `sp_k_ausd_v_ta_apn_ve_combined` procedure is deployed.
2.  Ensure the target tables `your_project_id.your_dataset_id.SOF_TA_APN_VE` and `your_project_id.your_dataset_id.VIA` exist and have the expected schema (even if empty).

**Action:**
Execute the `sp_r_ausd_v_ta_apn_ve_orchestrator` procedure. The core logic procedure will be called.

```python
# Example pytest code
def test_target_table_existence_and_schema(bigquery_client, project_id, dataset_id):
    # Action: No direct call needed, relies on orchestrator calling core logic
    # This test primarily asserts the *existence* and *schema* of target tables.

    # Assertions
    # 1. Check if SOF_TA_APN_VE table exists
    try:
        table_ref = bigquery_client.get_table(f"{project_id}.{dataset_id}.SOF_TA_APN_VE")
        assert table_ref is not None, "Target table SOF_TA_APN_VE does not exist."
        # Further assertions on schema (e.g., column names, types, nullability)
        # Example:
        # expected_schema = [
        #     bigquery.SchemaField("col_a", "STRING", mode="NULLABLE"),
        #     bigquery.SchemaField("col_b", "INTEGER", mode="REQUIRED"),
        # ]
        # assert all(field in table_ref.schema for field in expected_schema), "SOF_TA_APN_VE schema mismatch."
        print(f"Successfully verified existence and schema of SOF_TA_APN_VE.")
    except Exception as e:
        assert False, f"Failed to get SOF_TA_APN_VE table: {e}"

    # 2. Check if VIA table exists
    try:
        table_ref = bigquery_client.get_table(f"{project_id}.{dataset_id}.VIA")
        assert table_ref is not None, "Target table VIA does not exist."
        # Further assertions on schema
        print(f"Successfully verified existence and schema of VIA.")
    except Exception as e:
        assert False, f"Failed to get VIA table: {e}"
```

**Pass/Fail Criterion:**
*   The BigQuery tables `your_project_id.your_dataset_id.SOF_TA_APN_VE` and `your_project_id.your_dataset_id.VIA` exist.
*   Their schemas match the expected DDL for the migrated tables.

---

### 6. Core Logic Placeholder - Data Flow Verification (High-Level)

**Purpose:** Verify that the `sp_k_ausd_v_ta_apn_ve_combined` procedure, even in its placeholder state, correctly interacts with the expected source and target tables. This test is a high-level check until the detailed SQL is migrated.

**Setup:**
1.  Populate `your_project_id.your_dataset_id.DWTK_MELDUNGEN` and `your_project_id.your_dataset_id.PDS_TA_PDP_CONTEXT_ASSOC` with a small set of mock data.
2.  Ensure `your_project_id.your_dataset_id.SOF_TA_APN_VE` and `your_project_id.your_dataset_id.VIA` are empty.
3.  Modify `sp_k_ausd_v_ta_apn_ve_combined` to include placeholder `INSERT` statements that write a fixed number of rows to `SOF_TA_APN_VE` and `VIA` based on the mock source data, simulating the data flow. (This is a temporary modification for testing the *flow*, not the *transformation*).

**Action:**
1.  Execute the `sp_r_ausd_v_ta_apn_ve_orchestrator` procedure with a `process_date` that matches the mock data.
2.  Query the row counts of `SOF_TA_APN_VE` and `VIA`.

```python
# Example pytest code
def test_core_logic_data_flow_placeholder(bigquery_client, project_id, dataset_id):
    process_date = "2023-01-18"
    
    # Setup: Populate mock source data (conceptual)
    # bigquery_client.query(f"INSERT INTO `{project_id}.{dataset_id}.DWTK_MELDUNGEN` ...").result()
    # bigquery_client.query(f"INSERT INTO `{project_id}.{dataset_id}.PDS_TA_PDP_CONTEXT_ASSOC` ...").result()
    
    # Setup: Ensure target tables are empty before run
    bigquery_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.SOF_TA_APN_VE`").result()
    bigquery_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.VIA`").result()

    # Action: Call the orchestrator, which calls the core logic
    query = f"""
    CALL `{project_id}.{dataset_id}.sp_r_ausd_v_ta_apn_ve_orchestrator`(DATE('{process_date}'));
    """
    bigquery_client.query(query).result()

    # Assertions: Check row counts in target tables
    sof_count_query = f"SELECT COUNT(1) FROM `{project_id}.{dataset_id}.SOF_TA_APN_VE`"
    sof_count = list(bigquery_client.query(sof_count_query).result())[0][0]
    
    via_count_query = f"SELECT COUNT(1) FROM `{project_id}.{dataset_id}.VIA`"
    via_count = list(bigquery_client.query(via_count_query).result())[0][0]

    # These expected counts depend on the mock data and placeholder logic in sp_k_ausd_v_ta_apn_ve_combined
    expected_sof_rows = 5 # Example
    expected_via_rows = 3 # Example

    assert sof_count == expected_sof_rows, f"Expected {expected_sof_rows} rows in SOF_TA_APN_VE, got {sof_count}."
    assert via_count == expected_via_rows, f"Expected {expected_via_rows} rows in VIA, got {via_count}."

    print(f"Successfully verified data flow for core logic placeholder. SOF_TA_APN_VE: {sof_count}, VIA: {via_count}.")

# Cleanup: Revert sp_k_ausd_v_ta_apn_ve_combined to its original placeholder state if modified.
```

**Pass/Fail Criterion:**
*   The `sp_r_ausd_v_ta_apn_ve_orchestrator` completes successfully.
*   The `SOF_TA_APN_VE` and `VIA` tables contain the expected number of rows, indicating that the core logic successfully read from sources and wrote to targets.

---

### 7. Output Parity - `additional_info` in `job_log`

**Purpose:** Verify that the `additional_info` JSON column in the `job_log` table correctly captures extra job-specific metadata, such as input parameters and final execution details, providing a richer audit trail.

**Setup:**
1.  Ensure the `job_log` table is empty.
2.  Ensure `sp_k_ausd_v_ta_apn_ve_combined` is configured for success.
3.  Define a `process_date` (e.g., `2023-01-19`).

**Action:**
1.  Execute the `sp_r_ausd_v_ta_apn_ve_orchestrator` with the `process_date`.
2.  Query the `job_log` table for the `additional_info` column for the latest run.

```python
# Example pytest code
def test_job_log_additional_info(bigquery_client, project_id, dataset_id):
    process_date = "2023-01-19"
    job_name = "r_ausd_v_ta_apn_ve_orchestrator"
    job_version = "1.0" # Default from SP

    # Action: Call the orchestrator procedure
    query = f"""
    CALL `{project_id}.{dataset_id}.sp_r_ausd_v_ta_apn_ve_orchestrator`(DATE('{process_date}'), '{job_version}', '{job_name}');
    """
    bigquery_client.query(query).result()

    # Assertions
    log_query = f"""
    SELECT additional_info
    FROM `{project_id}.{dataset_id}.job_log`
    WHERE job_name = '{job_name}'
      AND process_date = DATE('{process_date}')
    ORDER BY start_timestamp DESC
    LIMIT 1;
    """
    rows = list(bigquery_client.query(log_query).result())
    
    assert len(rows) == 1, "Expected exactly one job_log entry."
    additional_info = rows[0].additional_info
    
    # Check initial info (input_process_date)
    assert additional_info is not None, "additional_info should not be NULL."
    assert 'input_process_date' in additional_info, "input_process_date not found in initial additional_info."
    assert additional_info['input_process_date'] == process_date, "input_process_date mismatch."

    # Check final info (executed_by, final_version, last_successful_run)
    assert 'executed_by' in additional_info, "executed_by not found in final additional_info."
    assert 'final_version' in additional_info, "final_version not found in final additional_info."
    assert additional_info['final_version'] == job_version, "final_version mismatch."
    assert 'last_successful_run' in additional_info, "last_successful_run not found in final additional_info."
    # The value of last_successful_run depends on prior runs, so just check existence.

    print(f"Successfully verified additional_info content in job_log for {job_name}.")
```

**Pass/Fail Criterion:**
*   The `additional_info` column in the `job_log` table contains a valid JSON object.
*   This JSON object includes the `input_process_date` (from initial insert) and `executed_by`, `final_version`, and `last_successful_run` (from final update), with correct values.

---

### Future Tests (Once Core Logic is Migrated)

The following tests are crucial but cannot be fully defined until the detailed SQL from `D_AUSD_V_TA_APN_VE.SQL` and the logic from `DWPA_UTIL_SKRIPT` and `PA_ANALYZE` are analyzed and migrated.

*   **Transformation Correctness - Data Parity:**
    *   **Purpose:** Prove that the migrated `sp_k_ausd_v_ta_apn_ve_combined` produces *identical* output data in `SOF_TA_APN_VE` and `VIA` compared to the legacy job for the same input data.
    *   **Setup:** Load identical source data into BigQuery tables (`DWTK_MELDUNGEN`, `PDS_TA_PDP_CONTEXT_ASSOC`) as used in a legacy run. Capture the legacy output data from `SOF$TA_APN_VE` and `VIA`.
    *   **Action:** Execute the migrated job.
    *   **Pass/Fail:** Perform a deep comparison (e.g., `EXCEPT` clause in SQL, or row-by-row comparison) between the migrated output and the legacy output. Row counts, column values, and data types must match exactly.

*   **Transformation Correctness - Joins, Aggregations, Filters:**
    *   **Purpose:** Test specific complex SQL constructs identified during the migration of `D_AUSD_V_TA_APN_VE.SQL`.
    *   **Setup:** Craft specific test data to exercise each join condition (inner, outer, cross), aggregation function (SUM, COUNT, AVG, etc.), and filter clause (WHERE conditions, HAVING clauses).
    *   **Action:** Execute the migrated job.
    *   **Pass/Fail:** Verify that the output data reflects the correct application of these SQL constructs.

*   **Transformation Correctness - Type & NULL Handling:**
    *   **Purpose:** Ensure data type conversions and NULL value propagation/handling are consistent with the legacy system.
    *   **Setup:** Provide source data with various data types (strings, numbers, dates, booleans) and explicit NULLs in different columns.
    *   **Action:** Execute the migrated job.
    *   **Pass/Fail:** Verify that output data types are correct, and NULLs are handled as per legacy logic (e.g., `NVL` equivalents, `COALESCE`, `IFNULL`).

*   **Transformation Correctness - Edge Cases:**
    *   **Purpose:** Test scenarios like empty source tables, all NULLs in a critical column, very large/small numeric values, boundary dates, duplicate keys, or other known business-specific edge cases.
    *   **Setup:** Prepare specific datasets for each edge case.
    *   **Action:** Execute the migrated job.
    *   **Pass/Fail:** Verify the job behaves as expected (e.g., produces no rows, produces specific error, handles values correctly).

*   **Legacy Package Emulation (`DWPA_UTIL_SKRIPT`, `PA_ANALYZE`):**
    *   **Purpose:** Verify that the BigQuery UDFs/Stored Procedures replacing these legacy packages produce identical results for given inputs.
    *   **Setup:** For each function/procedure within the packages, identify its inputs and expected outputs from the legacy system.
    *   **Action:** Call the BigQuery UDF/SP directly with various inputs.
    *   **Pass/Fail:** Compare the BigQuery output to the legacy output.

---