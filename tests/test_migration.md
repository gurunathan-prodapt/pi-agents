The migration of `r_ausd_bp_ta_iccid_einzeln.ksh` to a BigQuery Stored Procedure (`sp_ausd_bp_ta_iccid_einzeln_wrapper`) primarily involves replicating its orchestration, parameter handling, logging, and error management. The core data transformation logic resides in the `k_ausd_bp_ta_iccid_einzeln.ksh` script, which is migrated to `sp_ausd_bp_ta_iccid_einzeln_kernel`.

The tests below focus on validating the `sp_ausd_bp_ta_iccid_einzeln_wrapper`'s behavior, ensuring it correctly handles parameters, logs job status, and orchestrates the call to the kernel procedure as specified in the design.

**Assumptions for Testing:**
*   The `job_log` and `job_status` tables exist in `your_gcp_project.your_bq_dataset` with the specified schemas.
*   The `sp_ausd_bp_ta_iccid_einzeln_kernel` procedure exists in `your_gcp_project.your_bq_dataset`. For these tests, a placeholder `sp_ausd_bp_ta_iccid_einzeln_kernel` that logs its invocation and can optionally simulate an error is sufficient.

**Placeholder `sp_ausd_bp_ta_iccid_einzeln_kernel` for Testing:**

```sql
-- This placeholder procedure is used to test the wrapper's interaction with the kernel.
-- It logs its call and can simulate an error if p_simulate_error is TRUE.
CREATE OR REPLACE PROCEDURE `your_gcp_project.your_bq_dataset.sp_ausd_bp_ta_iccid_einzeln_kernel`(
    IN p_stichtag_final STRING,
    IN p_wiederanlaufwert_final STRING,
    IN p_simulate_error BOOL DEFAULT FALSE -- Added for testing error handling
)
BEGIN
    INSERT INTO `your_gcp_project.your_bq_dataset.job_log` (log_id, job_name, log_timestamp, log_level, message, stichtag, wiederanlaufwert)
    VALUES (GENERATE_UUID(), 'sp_ausd_bp_ta_iccid_einzeln_kernel', CURRENT_TIMESTAMP(), 'INFO',
            FORMAT('Kernel called with Stichtag: %s, Wiederanlaufwert: %s', p_stichtag_final, p_wiederanlaufwert_final),
            p_stichtag_final, p_wiederanlaufwert_final);

    IF p_simulate_error THEN
        RAISE USING MESSAGE 'Simulated error in kernel procedure.';
    END IF;
END;
```

---

## Test Suite: `sp_ausd_bp_ta_iccid_einzeln_wrapper` Migration Validation

### Test Case 1: Successful Execution with All Parameters Provided

**Purpose:**
To verify that the wrapper procedure executes successfully when both `p_stichtag` and `p_wiederanlaufWert` are explicitly provided, and that it correctly logs the job's progress and status. This covers output parity and external system replacements (logging).

**Setup:**
1.  Ensure `your_gcp_project.your_bq_dataset.job_log` and `your_gcp_project.your_bq_dataset.job_status` tables are empty.
2.  Ensure the placeholder `sp_ausd_bp_ta_iccid_einzeln_kernel` is deployed and does not simulate an error.

**Action:**
Execute the wrapper procedure with specific values for both parameters.

```sql
CALL `your_gcp_project.your_bq_dataset.sp_ausd_bp_ta_iccid_einzeln_wrapper`('01012023', '12345');
```

**Pass/Fail Criterion:**
1.  The `job_log` table contains at least 4 entries for `v_job_name` ('r_ausd_bp_ta_iccid_einzeln.ksh'):
    *   'Job started.' (INFO)
    *   'Parameters determined: Stichtag = 01012023, Wiederanlaufwert = 12345' (INFO)
    *   'Parameter validation successful.' (INFO)
    *   'Job completed successfully.' (INFO)
2.  The `job_log` table contains 1 entry for `sp_ausd_bp_ta_iccid_einzeln_kernel` with `message` containing "Kernel called with Stichtag: 01012023, Wiederanlaufwert: 12345".
3.  The `job_status` table contains one entry for `job_name = 'r_ausd_bp_ta_iccid_einzeln.ksh'` with `status = 'SUCCEEDED'`, `last_stichtag = '01012023'`, and `last_wiederanlaufwert = '12345'`.

```python
# Pytest assertion example
from google.cloud import bigquery
client = bigquery.Client()
dataset_id = "your_bq_dataset"
project_id = "your_gcp_project"

def test_successful_execution_with_all_params():
    # Setup: Clear log and status tables
    client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_log`").result()
    client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_status`").result()

    # Action: Call the wrapper SP
    client.query(f"CALL `{project_id}.{dataset_id}.sp_ausd_bp_ta_iccid_einzeln_wrapper`('01012023', '12345')").result()

    # Assertions for job_log
    log_query = f"SELECT log_level, message, stichtag, wiederanlaufwert FROM `{project_id}.{dataset_id}.job_log` WHERE job_name = 'r_ausd_bp_ta_iccid_einzeln.ksh' ORDER BY log_timestamp"
    logs = list(client.query(log_query).result())
    assert len(logs) >= 4 # At least 4 logs from wrapper
    assert logs[0].message == 'Job started.'
    assert logs[1].message == 'Parameters determined: Stichtag = 01012023, Wiederanlaufwert = 12345'
    assert logs[2].message == 'Parameter validation successful.'
    assert logs[3].message == 'Job completed successfully.'
    assert all(log.stichtag == '01012023' and log.wiederanlaufwert == '12345' for log in logs)

    # Assertions for kernel call in job_log
    kernel_log_query = f"SELECT message FROM `{project_id}.{dataset_id}.job_log` WHERE job_name = 'sp_ausd_bp_ta_iccid_einzeln_kernel'"
    kernel_logs = list(client.query(kernel_log_query).result())
    assert len(kernel_logs) == 1
    assert "Kernel called with Stichtag: 01012023, Wiederanlaufwert: 12345" in kernel_logs[0].message

    # Assertions for job_status
    status_query = f"SELECT status, last_stichtag, last_wiederanlaufwert FROM `{project_id}.{dataset_id}.job_status` WHERE job_name = 'r_ausd_bp_ta_iccid_einzeln.ksh'"
    status_row = list(client.query(status_query).result())[0]
    assert status_row.status == 'SUCCEEDED'
    assert status_row.last_stichtag == '01012023'
    assert status_row.last_wiederanlaufwert == '12345'
```

### Test Case 2: `p_stichtag` Missing (Default to System Date)

**Purpose:**
To verify that if `p_stichtag` is not provided (or is NULL/empty), the wrapper correctly defaults it to the current system date (DDMMYYYY format) before passing it to the kernel. This covers transformation correctness (date handling) and output parity.

**Setup:**
1.  Ensure `your_gcp_project.your_bq_dataset.job_log` and `your_gcp_project.your_bq_dataset.job_status` tables are empty.
2.  Ensure the placeholder `sp_ausd_bp_ta_iccid_einzeln_kernel` is deployed and does not simulate an error.

**Action:**
Execute the wrapper procedure without `p_stichtag` (pass `NULL` or an empty string) but with `p_wiederanlaufWert`.

```sql
-- Option 1: Pass NULL for p_stichtag
CALL `your_gcp_project.your_bq_dataset.sp_ausd_bp_ta_iccid_einzeln_wrapper`(NULL, '54321');

-- Option 2: Pass empty string for p_stichtag
-- CALL `your_gcp_project.your_bq_dataset.sp_ausd_bp_ta_iccid_einzeln_wrapper`('', '54321');
```

**Pass/Fail Criterion:**
1.  The `job_log` table contains entries indicating the job started, parameters determined, validation successful, and job completed successfully.
2.  The `job_log` entry for 'Parameters determined' and the kernel call message should show `Stichtag` as `FORMAT_DATE('%d%m%Y', CURRENT_DATE())` (the current system date in DDMMYYYY format) and `Wiederanlaufwert` as '54321'.
3.  The `job_status` table shows `status = 'SUCCEEDED'`, `last_stichtag` as the current system date, and `last_wiederanlaufwert = '54321'`.

```python
# Pytest assertion example
import datetime
def test_stichtag_defaults_to_system_date():
    client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_log`").result()
    client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_status`").result()

    # Action: Call the wrapper SP with NULL stichtag
    client.query(f"CALL `{project_id}.{dataset_id}.sp_ausd_bp_ta_iccid_einzeln_wrapper`(NULL, '54321')").result()

    expected_stichtag = datetime.datetime.now().strftime('%d%m%Y')

    # Assertions for job_log
    log_query = f"SELECT message, stichtag, wiederanlaufwert FROM `{project_id}.{dataset_id}.job_log` WHERE job_name = 'r_ausd_bp_ta_iccid_einzeln.ksh' AND message LIKE 'Parameters determined:%'"
    param_log = list(client.query(log_query).result())[0]
    assert param_log.stichtag == expected_stichtag
    assert param_log.wiederanlaufwert == '54321'

    # Assertions for kernel call in job_log
    kernel_log_query = f"SELECT message FROM `{project_id}.{dataset_id}.job_log` WHERE job_name = 'sp_ausd_bp_ta_iccid_einzeln_kernel'"
    kernel_logs = list(client.query(kernel_log_query).result())
    assert len(kernel_logs) == 1
    assert f"Kernel called with Stichtag: {expected_stichtag}, Wiederanlaufwert: 54321" in kernel_logs[0].message

    # Assertions for job_status
    status_query = f"SELECT status, last_stichtag, last_wiederanlaufwert FROM `{project_id}.{dataset_id}.job_status` WHERE job_name = 'r_ausd_bp_ta_iccid_einzeln.ksh'"
    status_row = list(client.query(status_query).result())[0]
    assert status_row.status == 'SUCCEEDED'
    assert status_row.last_stichtag == expected_stichtag
    assert status_row.last_wiederanlaufwert == '54321'
```

### Test Case 3: `p_wiederanlaufWert` Missing (Default to '0')

**Purpose:**
To verify that if `p_wiederanlaufWert` is not provided (or is NULL/empty), the wrapper correctly defaults it to '0' before passing it to the kernel. This covers transformation correctness (NULL handling) and output parity.

**Setup:**
1.  Ensure `your_gcp_project.your_bq_dataset.job_log` and `your_gcp_project.your_bq_dataset.job_status` tables are empty.
2.  Ensure the placeholder `sp_ausd_bp_ta_iccid_einzeln_kernel` is deployed and does not simulate an error.

**Action:**
Execute the wrapper procedure with `p_stichtag` but without `p_wiederanlaufWert` (pass `NULL` or an empty string).

```sql
-- Option 1: Pass NULL for p_wiederanlaufWert
CALL `your_gcp_project.your_bq_dataset.sp_ausd_bp_ta_iccid_einzeln_wrapper`('02022023', NULL);

-- Option 2: Pass empty string for p_wiederanlaufWert
-- CALL `your_gcp_project.your_bq_dataset.sp_ausd_bp_ta_iccid_einzeln_wrapper`('02022023', '');
```

**Pass/Fail Criterion:**
1.  The `job_log` table contains entries indicating the job started, parameters determined, validation successful, and job completed successfully.
2.  The `job_log` entry for 'Parameters determined' and the kernel call message should show `Stichtag` as '02022023' and `Wiederanlaufwert` as '0'.
3.  The `job_status` table shows `status = 'SUCCEEDED'`, `last_stichtag = '02022023'`, and `last_wiederanlaufwert = '0'`.

```python
# Pytest assertion example
def test_wiederanlaufwert_defaults_to_zero():
    client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_log`").result()
    client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_status`").result()

    # Action: Call the wrapper SP with NULL wiederanlaufWert
    client.query(f"CALL `{project_id}.{dataset_id}.sp_ausd_bp_ta_iccid_einzeln_wrapper`('02022023', NULL)").result()

    # Assertions for job_log
    log_query = f"SELECT message, stichtag, wiederanlaufwert FROM `{project_id}.{dataset_id}.job_log` WHERE job_name = 'r_ausd_bp_ta_iccid_einzeln.ksh' AND message LIKE 'Parameters determined:%'"
    param_log = list(client.query(log_query).result())[0]
    assert param_log.stichtag == '02022023'
    assert param_log.wiederanlaufwert == '0'

    # Assertions for kernel call in job_log
    kernel_log_query = f"SELECT message FROM `{project_id}.{dataset_id}.job_log` WHERE job_name = 'sp_ausd_bp_ta_iccid_einzeln_kernel'"
    kernel_logs = list(client.query(kernel_log_query).result())
    assert len(kernel_logs) == 1
    assert "Kernel called with Stichtag: 02022023, Wiederanlaufwert: 0" in kernel_logs[0].message

    # Assertions for job_status
    status_query = f"SELECT status, last_stichtag, last_wiederanlaufwert FROM `{project_id}.{dataset_id}.job_status` WHERE job_name = 'r_ausd_bp_ta_iccid_einzeln.ksh'"
    status_row = list(client.query(status_query).result())[0]
    assert status_row.status == 'SUCCEEDED'
    assert status_row.last_stichtag == '02022023'
    assert status_row.last_wiederanlaufwert == '0'
```

### Test Case 4: Kernel Script Failure

**Purpose:**
To verify that if the `sp_ausd_bp_ta_iccid_einzeln_kernel` procedure encounters an error, the wrapper correctly catches it, logs the error, updates the job status to 'FAILED', and re-raises the error. This covers error handling and external system replacements (logging).

**Setup:**
1.  Ensure `your_gcp_project.your_bq_dataset.job_log` and `your_gcp_project.your_bq_dataset.job_status` tables are empty.
2.  Modify the placeholder `sp_ausd_bp_ta_iccid_einzeln_kernel` to simulate an error when `p_simulate_error` is `TRUE`.

**Action:**
Execute the wrapper procedure, configuring the kernel call to simulate an error.

```sql
-- Note: This requires modifying the sp_ausd_bp_ta_iccid_einzeln_wrapper to pass p_simulate_error=TRUE
-- to the kernel, or directly modifying the kernel to always fail for this test.
-- Assuming the wrapper is temporarily modified for this test:
-- CALL `your_gcp_project.your_bq_dataset.sp_ausd_bp_ta_iccid_einzeln_wrapper`('03032023', '999', TRUE);
-- Or, if the kernel is modified to always fail:
CALL `your_gcp_project.your_bq_dataset.sp_ausd_bp_ta_iccid_einzeln_wrapper`('03032023', '999');
```
*(Self-correction: The wrapper does not pass `p_simulate_error`. The kernel needs to be temporarily modified to always fail, or the wrapper needs a temporary modification to pass a flag. For a clean test, let's assume the kernel is temporarily modified to fail unconditionally for this test case, or we'll simulate the error in the wrapper itself if possible. The current wrapper code has `RAISE USING MESSAGE v_error_message;` in the `EXCEPTION` block, which is good. The kernel placeholder has `p_simulate_error BOOL DEFAULT FALSE`. We need to modify the wrapper's `CALL` statement for this test to `CALL ...kernel(..., TRUE);`)*

**Revised Action for Test Case 4:**
Temporarily modify `sp_ausd_bp_ta_iccid_einzeln_wrapper` to pass `TRUE` for `p_simulate_error` to the kernel for this test.
```sql
-- Temporary modification in sp_ausd_bp_ta_iccid_einzeln_wrapper for this test:
-- Change: CALL `your_gcp_project.your_bq_dataset.sp_ausd_bp_ta_iccid_einzeln_kernel`(v_stichtag_final, v_wiederanlaufwert_final);
-- To:     CALL `your_gcp_project.your_bq_dataset.sp_ausd_bp_ta_iccid_einzeln_kernel`(v_stichtag_final, v_wiederanlaufwert_final, TRUE);

-- Then execute the wrapper:
CALL `your_gcp_project.your_bq_dataset.sp_ausd_bp_ta_iccid_einzeln_wrapper`('03032023', '999');
```

**Pass/Fail Criterion:**
1.  The execution of the wrapper procedure should fail and raise an error.
2.  The `job_log` table contains entries for job start, parameter determination, validation, and an 'ERROR' level entry with `message = 'Job failed.'` and `error_details` containing "Simulated error in kernel procedure.".
3.  The `job_status` table contains one entry for `job_name = 'r_ausd_bp_ta_iccid_einzeln.ksh'` with `status = 'FAILED'`, `last_stichtag = '03032023'`, `last_wiederanlaufwert = '999'`, and `last_error_message` containing "Simulated error in kernel procedure.".

```python
# Pytest assertion example
import pytest
def test_kernel_script_failure_handling():
    client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_log`").result()
    client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_status`").result()

    # Temporarily modify wrapper to pass p_simulate_error=TRUE to kernel
    # (This part would be handled by a deployment script or direct modification for testing)
    # For this example, we'll assume the kernel is temporarily modified to always fail.
    # Or, if the wrapper is modified to pass the flag:
    # client.query(f"CALL `{project_id}.{dataset_id}.sp_ausd_bp_ta_iccid_einzeln_wrapper`('03032023', '999', TRUE)").result()
    # If the kernel is modified to always fail:
    # client.query(f"CREATE OR REPLACE PROCEDURE `{project_id}.{dataset_id}.sp_ausd_bp_ta_iccid_einzeln_kernel`(...) BEGIN RAISE USING MESSAGE 'Simulated error in kernel procedure.'; END;").result()
    # Then call the wrapper:

    # Action: Call the wrapper SP, expecting it to fail
    with pytest.raises(Exception) as excinfo:
        client.query(f"CALL `{project_id}.{dataset_id}.sp_ausd_bp_ta_iccid_einzeln_wrapper`('03032023', '999')").result()
    assert "Simulated error in kernel procedure." in str(excinfo.value)

    # Assertions for job_log
    log_query = f"SELECT log_level, message, error_details, stichtag, wiederanlaufwert FROM `{project_id}.{dataset_id}.job_log` WHERE job_name = 'r_ausd_bp_ta_iccid_einzeln.ksh' ORDER BY log_timestamp DESC"
    logs = list(client.query(log_query).result())
    assert len(logs) >= 4 # At least start, params, validation, error
    error_log = logs[0] # Most recent log should be the error
    assert error_log.log_level == 'ERROR'
    assert error_log.message == 'Job failed.'
    assert "Simulated error in kernel procedure." in error_log.error_details
    assert error_log.stichtag == '03032023'
    assert error_log.wiederanlaufwert == '999'

    # Assertions for job_status
    status_query = f"SELECT status, last_error_message, last_stichtag, last_wiederanlaufwert FROM `{project_id}.{dataset_id}.job_status` WHERE job_name = 'r_ausd_bp_ta_iccid_einzeln.ksh'"
    status_row = list(client.query(status_query).result())[0]
    assert status_row.status == 'FAILED'
    assert "Simulated error in kernel procedure." in status_row.last_error_message
    assert status_row.last_stichtag == '03032023'
    assert status_row.last_wiederanlaufwert == '999'
```

### Test Case 5: `Stichtag` Validation Failure

**Purpose:**
To verify that if `p_stichtag` is explicitly provided as an empty string or NULL, and the system date defaulting logic somehow fails (or if `v_stichtag_final` ends up being NULL/empty), the wrapper correctly raises an error and logs it. This covers transformation correctness (validation) and error handling.

**Setup:**
1.  Ensure `your_gcp_project.your_bq_dataset.job_log` and `your_gcp_project.your_bq_dataset.job_status` tables are empty.
2.  For this specific test, we need to ensure `v_stichtag_final` becomes NULL/empty after defaulting. This is hard to achieve with `CURRENT_DATE()` being reliable. A more realistic scenario is if the input `p_stichtag` is an invalid format that `PARSE_DATE` would fail on *if* it were used earlier, but here it's just a string. The current validation `IF v_stichtag_final IS NULL OR v_stichtag_final = ''` is robust. To trigger this, we'd need `COALESCE(NULLIF(p_stichtag, ''), v_system_date_ddmmyyyy)` to result in NULL/empty. This implies `p_stichtag` is NULL/empty AND `v_system_date_ddmmyyyy` is also NULL/empty, which is practically impossible for `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
    *Let's re-evaluate the legacy script's `pruefeParameterGesetzt Stichtag p_stichtag`. This function would check if `p_stichtag` is non-empty. Since `p_stichtag` defaults to `v_sysdate` if not provided, it will *always* be set unless `v_sysdate` itself is empty, which is unlikely. The only way for the legacy script to fail this check is if `h_alis_date.ksh` fails to set `v_sysdate` AND `p_stichtag` was not provided. The BigQuery equivalent `v_system_date_ddmmyyyy = FORMAT_DATE('%d%m%Y', CURRENT_DATE());` is very robust.
    *Therefore, this test case is primarily for robustness against unexpected `NULL` values, rather than a common failure path. We can simulate it by temporarily modifying the `v_stichtag_final` assignment to force it to NULL.*

**Revised Action for Test Case 5:**
Temporarily modify `sp_ausd_bp_ta_iccid_einzeln_wrapper` to force `v_stichtag_final` to `NULL` after its initial assignment, before the validation check.
```sql
-- Temporary modification in sp_ausd_bp_ta_iccid_einzeln_wrapper for this test:
-- After: SET v_stichtag_final = COALESCE(NULLIF(p_stichtag, ''), v_system_date_ddmmyyyy);
-- Add:   SET v_stichtag_final = NULL; -- Force NULL for testing validation failure

-- Then execute the wrapper:
CALL `your_gcp_project.your_bq_dataset.sp_ausd_bp_ta_iccid_einzeln_wrapper`(NULL, '100');
```

**Pass/Fail Criterion:**
1.  The execution of the wrapper procedure should fail and raise an error.
2.  The `job_log` table contains entries for job start, parameter determination, and an 'ERROR' level entry with `message = 'Job failed.'` and `error_details` containing "ERROR: Stichtag parameter is missing after defaulting. Cannot proceed.".
3.  The `job_status` table contains one entry for `job_name = 'r_ausd_bp_ta_iccid_einzeln.ksh'` with `status = 'FAILED'`, `last_stichtag = NULL` (or the original input `NULL`), `last_wiederanlaufwert = '100'`, and `last_error_message` containing "ERROR: Stichtag parameter is missing after defaulting. Cannot proceed.".

```python
# Pytest assertion example
def test_stichtag_validation_failure():
    client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_log`").result()
    client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_status`").result()

    # Action: Call the wrapper SP, expecting it to fail due to Stichtag validation
    # (Requires temporary modification to wrapper to force v_stichtag_final to NULL)
    with pytest.raises(Exception) as excinfo:
        client.query(f"CALL `{project_id}.{dataset_id}.sp_ausd_bp_ta_iccid_einzeln_wrapper`(NULL, '100')").result()
    assert "Stichtag parameter is missing after defaulting. Cannot proceed." in str(excinfo.value)

    # Assertions for job_log
    log_query = f"SELECT log_level, message, error_details, stichtag, wiederanlaufwert FROM `{project_id}.{dataset_id}.job_log` WHERE job_name = 'r_ausd_bp_ta_iccid_einzeln.ksh' ORDER BY log_timestamp DESC"
    logs = list(client.query(log_query).result())
    assert len(logs) >= 2 # At least start and error
    error_log = logs[0]
    assert error_log.log_level == 'ERROR'
    assert error_log.message == 'Job failed.'
    assert "Stichtag parameter is missing after defaulting. Cannot proceed." in error_log.error_details
    # Note: stichtag in log might be NULL or the original input NULL, depending on where the error is caught.
    # The final stichtag passed to the log should be NULL in this case.
    assert error_log.stichtag is None # Or original input if it was passed

    # Assertions for job_status
    status_query = f"SELECT status, last_error_message, last_stichtag, last_wiederanlaufwert FROM `{project_id}.{dataset_id}.job_status` WHERE job_name = 'r_ausd_bp_ta_iccid_einzeln.ksh'"
    status_row = list(client.query(status_query).result())[0]
    assert status_row.status == 'FAILED'
    assert "Stichtag parameter is missing after defaulting. Cannot proceed." in status_row.last_error_message
    assert status_row.last_stichtag is None # Or original input if it was passed
    assert status_row.last_wiederanlaufwert == '100'
```

### Test Case 6: Schema and Data Quality of Logging Tables

**Purpose:**
To ensure that the `job_log` and `job_status` tables adhere to their defined schemas and that data types are correctly handled during insertion. This covers data quality and schema assertions.

**Setup:**
1.  Ensure `your_gcp_project.your_bq_dataset.job_log` and `your_gcp_project.your_bq_dataset.job_status` tables are empty.
2.  Execute a successful run of the wrapper procedure (e.g., `CALL your_gcp_project.your_bq_dataset.sp_ausd_bp_ta_iccid_einzeln_wrapper('04042023', '789');`).

**Action:**
Query the information schema for the tables and inspect the data types of inserted records.

```sql
-- Query for job_log schema
SELECT column_name, data_type
FROM `your_gcp_project.your_bq_dataset.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'job_log'
ORDER BY ordinal_position;

-- Query for job_status schema
SELECT column_name, data_type
FROM `your_gcp_project.your_bq_dataset.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'job_status'
ORDER BY ordinal_position;

-- Sample data from job_log to check types
SELECT log_id, job_name, log_timestamp, log_level, message, stichtag, wiederanlaufwert, error_details
FROM `your_gcp_project.your_bq_dataset.job_log`
LIMIT 1;
```

**Pass/Fail Criterion:**
1.  **`job_log` Schema:**
    *   `log_id`: `STRING`
    *   `job_name`: `STRING`
    *   `log_timestamp`: `TIMESTAMP`
    *   `log_level`: `STRING`
    *   `message`: `STRING`
    *   `stichtag`: `STRING`
    *   `wiederanlaufwert`: `STRING`
    *   `error_details`: `STRING`
2.  **`job_status` Schema:**
    *   `job_name`: `STRING`
    *   `last_run_timestamp`: `TIMESTAMP`
    *   `status`: `STRING`
    *   `last_error_message`: `STRING`
    *   `last_stichtag`: `STRING`
    *   `last_wiederanlaufwert`: `STRING`
3.  All inserted values conform to their respective data types (e.g., `log_timestamp` is a valid timestamp, `stichtag` and `wiederanlaufwert` are strings).

```python
# Pytest assertion example
def test_logging_table_schema_and_data_quality():
    client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_log`").result()
    client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_status`").result()

    # Action: Execute a successful run to populate tables
    client.query(f"CALL `{project_id}.{dataset_id}.sp_ausd_bp_ta_iccid_einzeln_wrapper`('04042023', '789')").result()

    # Assert job_log schema
    log_schema_query = f"""
        SELECT column_name, data_type
        FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'job_log'
        ORDER BY ordinal_position
    """
    log_schema = {row.column_name: row.data_type for row in client.query(log_schema_query).result()}
    expected_log_schema = {
        'log_id': 'STRING', 'job_name': 'STRING', 'log_timestamp': 'TIMESTAMP',
        'log_level': 'STRING', 'message': 'STRING', 'stichtag': 'STRING',
        'wiederanlaufwert': 'STRING', 'error_details': 'STRING'
    }
    assert log_schema == expected_log_schema

    # Assert job_status schema
    status_schema_query = f"""
        SELECT column_name, data_type
        FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'job_status'
        ORDER BY ordinal_position
    """
    status_schema = {row.column_name: row.data_type for row in client.query(status_schema_query).result()}
    expected_status_schema = {
        'job_name': 'STRING', 'last_run_timestamp': 'TIMESTAMP', 'status': 'STRING',
        'last_error_message': 'STRING', 'last_stichtag': 'STRING', 'last_wiederanlaufwert': 'STRING'
    }
    assert status_schema == expected_status_schema

    # Assert data types in job_log (sample)
    sample_log_query = f"SELECT log_id, log_timestamp, stichtag FROM `{project_id}.{dataset_id}.job_log` LIMIT 1"
    sample_log = list(client.query(sample_log_query).result())[0]
    assert isinstance(sample_log.log_id, str)
    assert isinstance(sample_log.log_timestamp, datetime.datetime)
    assert isinstance(sample_log.stichtag, str)
```