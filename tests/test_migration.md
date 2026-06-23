The migration of `r_ausd_bp_ta_cntrct_dist.ksh` to a BigQuery Stored Procedure (`project.dataset.ausd_bp_ta_cntrct_dist_wrapper`) primarily involves refactoring orchestration, parameter handling, logging, and error management. The core business logic is assumed to be migrated to `project.dataset.ausd_bp_ta_cntrct_dist_core`.

The following tests validate the behavior of the `ausd_bp_ta_cntrct_dist_wrapper` stored procedure, ensuring it correctly handles parameters, logs job status, and orchestrates the call to the core procedure.

**Prerequisites for Testing:**

1.  **BigQuery Project and Dataset:** Ensure `project.dataset` exists.
2.  **Log Tables DDL:** The `job_control`, `job_audit_log`, and `job_error_log` tables must be created as per the provided DDL.
3.  **Mock Core Stored Procedure:** A mock `project.dataset.ausd_bp_ta_cntrct_dist_core` is required to simulate the behavior of the actual core logic and record its invocation parameters. This mock procedure includes a `p_simulate_failure` parameter to test error handling.

    ```sql
    -- Mock Core Stored Procedure for testing purposes
    CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_cntrct_dist_core`(
        IN p_core_stichtag STRING,
        IN p_core_restart_value INT64,
        IN p_simulate_failure BOOL DEFAULT FALSE
    )
    BEGIN
        -- Create a temporary table to log calls to this mock procedure
        CREATE TEMPORARY TABLE IF NOT EXISTS `temp_core_sp_calls` (
            call_id INT64 OPTIONS(description="Unique call identifier."),
            stichtag STRING,
            restart_value INT64,
            call_timestamp TIMESTAMP,
            simulated_failure BOOL
        );

        -- Log the parameters received by the mock core SP
        INSERT INTO `temp_core_sp_calls` (call_id, stichtag, restart_value, call_timestamp, simulated_failure)
        VALUES (
            (SELECT IFNULL(MAX(call_id), 0) + 1 FROM `temp_core_sp_calls`),
            p_core_stichtag,
            p_core_restart_value,
            CURRENT_TIMESTAMP(),
            p_simulate_failure
        );

        -- Simulate failure if requested
        IF p_simulate_failure THEN
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated core SP failure for testing purposes.';
        END IF;
    END;
    ```
4.  **Modified Wrapper Stored Procedure for Testing:** The provided `ausd_bp_ta_cntrct_dist_wrapper` needs a slight modification to pass the `p_simulate_core_failure` flag to the mock core SP. This is a testing-specific addition.

    ```sql
    -- Modified BigQuery Stored Procedure for `r_ausd_bp_ta_cntrct_dist.ksh` wrapper logic
    -- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_dist.ksh
    -- Note: Added `p_simulate_core_failure` parameter for testing purposes.
    CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_cntrct_dist_wrapper`(
        IN p_stichtag STRING,
        IN p_wiederanlaufWert INT64,
        IN p_simulate_core_failure BOOL DEFAULT FALSE -- Added for testing core SP failure handling
    )
    BEGIN
        DECLARE v_job_nr INT64;
        DECLARE v_job_kennung STRING DEFAULT 'ausd_bp_ta_cntrct_dist_wrapper';
        DECLARE v_source_program STRING DEFAULT 'r_ausd_bp_ta_cntrct_dist.ksh';
        DECLARE v_restart_value INT64;
        DECLARE v_sysdate STRING;
        DECLARE v_effective_stichtag STRING;
        DECLARE v_current_timestamp TIMESTAMP;
        DECLARE v_status STRING;
        DECLARE v_error_message STRING;
        DECLARE v_log_file_name STRING DEFAULT 'placeholder_log_file.log'; -- Placeholder as per design document schema

        -- Get next job_nr and current timestamp for job_control entry
        SET v_job_nr = (SELECT IFNULL(MAX(job_nr), 0) + 1 FROM `project.dataset.job_control`);
        SET v_current_timestamp = CURRENT_TIMESTAMP();

        -- Set defaults and derived values based on input parameters
        SET v_restart_value = IFNULL(p_wiederanlaufWert, 0);
        SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());
        SET v_effective_stichtag = IFNULL(NULLIF(p_stichtag, ''), v_sysdate);
        SET v_status = 'RUNNING';

        -- Insert initial job control entry
        INSERT INTO `project.dataset.job_control` (
            job_nr, job_kennung, source_program, stichtag, sysdate, restart_value, created_at, status
        ) VALUES (
            v_job_nr, v_job_kennung, v_source_program, p_stichtag, v_sysdate, v_restart_value, v_current_timestamp, v_status
        );

        -- Log job start to audit log
        INSERT INTO `project.dataset.job_audit_log` (
            log_id, job_nr, job_kennung, log_file_name, stichtag, sysdate, message, created_at
        ) VALUES (
            GENERATE_UUID(), v_job_nr, v_job_kennung, v_log_file_name, v_effective_stichtag, v_sysdate,
            'Job started. Effective Stichtag: ' || v_effective_stichtag || ', Restart Value: ' || v_restart_value,
            v_current_timestamp
        );

        -- Validate if v_effective_stichtag is set
        -- Note: As per legacy behavior, p_stichtag is always defaulted to v_sysdate if not provided or empty.
        -- Thus, this validation block for v_effective_stichtag being NULL or empty will likely never be hit
        -- unless CURRENT_DATE() itself somehow returns an empty or NULL value, which is not expected in BQ.
        IF v_effective_stichtag IS NULL OR v_effective_stichtag = '' THEN
            SET v_status = 'FAILED';
            SET v_error_message = 'ERROR: Effective Stichtag parameter is missing or empty. Please provide a valid date.';

            -- Log error to job_error_log
            INSERT INTO `project.dataset.job_error_log` (
                error_id, job_kennung, err_nr, err_arg, created_at, message
            ) VALUES (
                GENERATE_UUID(), v_job_kennung, 1, 'Stichtag_Validation', CURRENT_TIMESTAMP(), v_error_message
            );

            -- Update job_control status to FAILED
            UPDATE `project.dataset.job_control`
            SET status = v_status, finished_at = CURRENT_TIMESTAMP()
            WHERE job_nr = v_job_nr;

            -- Log validation failure to audit log
            INSERT INTO `project.dataset.job_audit_log` (
                log_id, job_nr, job_kennung, log_file_name, stichtag, sysdate, message, created_at
            ) VALUES (
                GENERATE_UUID(), v_job_nr, v_job_kennung, v_log_file_name, v_effective_stichtag, v_sysdate,
                'Validation failed: ' || v_error_message, CURRENT_TIMESTAMP()
            );

            -- Signal an error to the caller
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
        END IF;

        -- Begin an exception block to handle errors during the core procedure call
        BEGIN
            -- Call the core business logic stored procedure
            CALL `project.dataset.ausd_bp_ta_cntrct_dist_core`(v_effective_stichtag, v_restart_value, p_simulate_core_failure);

            -- If core procedure succeeds, update job status
            SET v_status = 'SUCCESS';
            UPDATE `project.dataset.job_control`
            SET status = v_status, finished_at = CURRENT_TIMESTAMP()
            WHERE job_nr = v_job_nr;

            -- Log success to audit log
            INSERT INTO `project.dataset.job_audit_log` (
                log_id, job_nr, job_kennung, log_file_name, stichtag, sysdate, message, created_at
            ) VALUES (
                GENERATE_UUID(), v_job_nr, v_job_kennung, v_log_file_name, v_effective_stichtag, v_sysdate,
                'Core procedure `ausd_bp_ta_cntrct_dist_core` completed successfully.', CURRENT_TIMESTAMP()
            );

        EXCEPTION WHEN ERROR THEN
            -- If an error occurs during the core procedure call
            SET v_status = 'FAILED';
            SET v_error_message = @@error.message;

            -- Log the error to job_error_log
            INSERT INTO `project.dataset.job_error_log` (
                error_id, job_kennung, err_nr, err_arg, created_at, message
            ) VALUES (
                GENERATE_UUID(), v_job_kennung, 2, 'Core_Procedure_Call', CURRENT_TIMESTAMP(), v_error_message
            );

            -- Update job_control status to FAILED
            UPDATE `project.dataset.job_control`
            SET status = v_status, finished_at = CURRENT_TIMESTAMP()
            WHERE job_nr = v_job_nr;

            -- Log failure to audit log
            INSERT INTO `project.dataset.job_audit_log` (
                log_id, job_nr, job_kennung, log_file_name, stichtag, sysdate, message, created_at
            ) VALUES (
                GENERATE_UUID(), v_job_nr, v_job_kennung, v_log_file_name, v_effective_stichtag, v_sysdate,
                'Core procedure `ausd_bp_ta_cntrct_dist_core` failed with error: ' || v_error_message, CURRENT_TIMESTAMP()
            );

            -- Re-raise the error to propagate it to the caller
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
        END;

    END;
    ```

---

## Test Case 1: Default Parameters - No Stichtag, No Wiederanlaufwert

**Purpose:** Verify that when no `Stichtag` and no `Wiederanlaufwert` are provided, `p_stichtag` defaults to the current system date (`DDMMYYYY` format) and `p_wiederanlaufWert` defaults to `0`.

**Setup:**
1.  Ensure `project.dataset.job_control`, `project.dataset.job_audit_log`, and `project.dataset.job_error_log` tables are empty.
2.  Ensure `temp_core_sp_calls` (from the mock core SP) is empty.

**Action:**
Execute the wrapper stored procedure without any parameters (or with `NULL` for both).

```sql
CALL `project.dataset.ausd_bp_ta_cntrct_dist_wrapper`(NULL, NULL);
```

**Pass/Fail Criterion:**
1.  One entry in `project.dataset.job_control` with `status = 'SUCCESS'`.
2.  The `job_control` entry's `stichtag` and `sysdate` fields match `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
3.  The `job_control` entry's `restart_value` is `0`.
4.  Two entries in `project.dataset.job_audit_log` (one for start, one for core success).
5.  The `job_audit_log` entries' `stichtag` and `sysdate` fields match `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
6.  One entry in `temp_core_sp_calls` with `stichtag` matching `FORMAT_DATE('%d%m%Y', CURRENT_DATE())` and `restart_value = 0`.
7.  `project.dataset.job_error_log` is empty.

```python
# Example pytest assertion (assuming BigQuery client 'bq_client' and project/dataset 'project.dataset')
def test_default_parameters_no_inputs(bq_client):
    bq_client.query("TRUNCATE TABLE `project.dataset.job_control`").result()
    bq_client.query("TRUNCATE TABLE `project.dataset.job_audit_log`").result()
    bq_client.query("TRUNCATE TABLE `project.dataset.job_error_log`").result()
    bq_client.query("DROP TEMPORARY TABLE IF EXISTS `temp_core_sp_calls`").result()

    bq_client.query("CALL `project.dataset.ausd_bp_ta_cntrct_dist_wrapper`(NULL, NULL)").result()

    today_str = bq_client.query("SELECT FORMAT_DATE('%d%m%Y', CURRENT_DATE())").result().to_dataframe().iloc[0, 0]

    job_control_df = bq_client.query("SELECT * FROM `project.dataset.job_control`").result().to_dataframe()
    audit_log_df = bq_client.query("SELECT * FROM `project.dataset.job_audit_log`").result().to_dataframe()
    core_calls_df = bq_client.query("SELECT * FROM `temp_core_sp_calls`").result().to_dataframe()

    assert len(job_control_df) == 1
    assert job_control_df.iloc[0]['status'] == 'SUCCESS'
    assert job_control_df.iloc[0]['stichtag'] == today_str
    assert job_control_df.iloc[0]['sysdate'] == today_str
    assert job_control_df.iloc[0]['restart_value'] == 0

    assert len(audit_log_df) == 2
    assert all(audit_log_df['stichtag'] == today_str)
    assert all(audit_log_df['sysdate'] == today_str)
    assert "Job started" in audit_log_df.iloc[0]['message']
    assert "Core procedure" in audit_log_df.iloc[1]['message']

    assert len(core_calls_df) == 1
    assert core_calls_df.iloc[0]['stichtag'] == today_str
    assert core_calls_df.iloc[0]['restart_value'] == 0
    assert not core_calls_df.iloc[0]['simulated_failure']

    error_log_df = bq_client.query("SELECT * FROM `project.dataset.job_error_log`").result().to_dataframe()
    assert len(error_log_df) == 0
```

---

## Test Case 2: Provided Stichtag, No Wiederanlaufwert

**Purpose:** Verify that when a `Stichtag` is provided, it is used, and `p_wiederanlaufWert` defaults to `0`.

**Setup:**
1.  Ensure log tables and `temp_core_sp_calls` are empty.

**Action:**
Execute the wrapper stored procedure with a specific `Stichtag` and `NULL` for `Wiederanlaufwert`.

```sql
CALL `project.dataset.ausd_bp_ta_cntrct_dist_wrapper`('15032023', NULL);
```

**Pass/Fail Criterion:**
1.  One entry in `project.dataset.job_control` with `status = 'SUCCESS'`.
2.  The `job_control` entry's `stichtag` is `'15032023'`.
3.  The `job_control` entry's `restart_value` is `0`.
4.  Two entries in `project.dataset.job_audit_log`.
5.  The `job_audit_log` entries' `stichtag` is `'15032023'`.
6.  One entry in `temp_core_sp_calls` with `stichtag = '15032023'` and `restart_value = 0`.
7.  `project.dataset.job_error_log` is empty.

---

## Test Case 3: No Stichtag, Provided Wiederanlaufwert

**Purpose:** Verify that when `Wiederanlaufwert` is provided, it is used, and `p_stichtag` defaults to the current system date.

**Setup:**
1.  Ensure log tables and `temp_core_sp_calls` are empty.

**Action:**
Execute the wrapper stored procedure with `NULL` for `Stichtag` and a specific `Wiederanlaufwert`.

```sql
CALL `project.dataset.ausd_bp_ta_cntrct_dist_wrapper`(NULL, 12345);
```

**Pass/Fail Criterion:**
1.  One entry in `project.dataset.job_control` with `status = 'SUCCESS'`.
2.  The `job_control` entry's `stichtag` matches `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
3.  The `job_control` entry's `restart_value` is `12345`.
4.  Two entries in `project.dataset.job_audit_log`.
5.  The `job_audit_log` entries' `stichtag` matches `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
6.  One entry in `temp_core_sp_calls` with `stichtag` matching `FORMAT_DATE('%d%m%Y', CURRENT_DATE())` and `restart_value = 12345`.
7.  `project.dataset.job_error_log` is empty.

---

## Test Case 4: Both Parameters Provided

**Purpose:** Verify that both `Stichtag` and `Wiederanlaufwert` are used as provided when both are supplied.

**Setup:**
1.  Ensure log tables and `temp_core_sp_calls` are empty.

**Action:**
Execute the wrapper stored procedure with specific values for both parameters.

```sql
CALL `project.dataset.ausd_bp_ta_cntrct_dist_wrapper`('01012024', 54321);
```

**Pass/Fail Criterion:**
1.  One entry in `project.dataset.job_control` with `status = 'SUCCESS'`.
2.  The `job_control` entry's `stichtag` is `'01012024'`.
3.  The `job_control` entry's `restart_value` is `54321`.
4.  Two entries in `project.dataset.job_audit_log`.
5.  The `job_audit_log` entries' `stichtag` is `'01012024'`.
6.  One entry in `temp_core_sp_calls` with `stichtag = '01012024'` and `restart_value = 54321`.
7.  `project.dataset.job_error_log` is empty.

---

## Test Case 5: Stichtag Validation - No Failure Expected (Behavioral Equivalence)

**Purpose:** Verify that the BigQuery wrapper's `Stichtag` validation logic behaves equivalently to the legacy script, where `p_stichtag` is always defaulted to `v_sysdate` if not provided or empty, thus preventing a validation failure at that stage.

**Setup:**
1.  Ensure log tables and `temp_core_sp_calls` are empty.

**Action:**
Execute the wrapper stored procedure with `p_stichtag` as an empty string.

```sql
CALL `project.dataset.ausd_bp_ta_cntrct_dist_wrapper`('', NULL);
```

**Pass/Fail Criterion:**
1.  The procedure completes successfully (does NOT signal an error).
2.  One entry in `project.dataset.job_control` with `status = 'SUCCESS'`.
3.  The `job_control` entry's `stichtag` (the *effective* stichtag) matches `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
4.  `project.dataset.job_error_log` is empty.
5.  Two entries in `project.dataset.job_audit_log`.
6.  One entry in `temp_core_sp_calls` with `stichtag` matching `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.

**Note on Discrepancy:** The design document's target transformation section states: "An `IF` statement checks for `NULL` or empty `v_effective_stichtag`. If invalid, an error is logged...". However, given the preceding defaulting logic (`v_effective_stichtag = IFNULL(NULLIF(p_stichtag, ''), v_sysdate)`), `v_effective_stichtag` will *always* be a valid date string (either the input `p_stichtag` or `CURRENT_DATE()`). This means the `IF v_effective_stichtag IS NULL OR v_effective_stichtag = ''` block will never be executed. This test confirms this behavior, which aligns with the legacy script's effective behavior after defaulting, where `pruefeParameterGesetzt` would not fail for `p_stichtag`.

---

## Test Case 6: Core Stored Procedure Failure Handling

**Purpose:** Verify that the wrapper correctly catches, logs, and re-raises an error if the `ausd_bp_ta_cntrct_dist_core` procedure fails.

**Setup:**
1.  Ensure log tables and `temp_core_sp_calls` are empty.

**Action:**
Execute the wrapper stored procedure, instructing the mock core SP to simulate a failure.

```sql
-- This call is expected to fail and raise an error.
-- In a testing framework, you would typically wrap this in a try-except block
-- or use a mechanism to assert that an exception is raised.
CALL `project.dataset.ausd_bp_ta_cntrct_dist_wrapper`('01012023', 0, TRUE);
```

**Pass/Fail Criterion:**
1.  The `CALL` statement to `ausd_bp_ta_cntrct_dist_wrapper` results in an error being signaled (e.g., `SQLSTATE '45000'`).
2.  One entry in `project.dataset.job_control` with `status = 'FAILED'`.
3.  The `job_control` entry's `finished_at` is populated.
4.  Three entries in `project.dataset.job_audit_log`: one for job start, one for core SP failure, and potentially one for the wrapper's own error handling.
5.  One entry in `project.dataset.job_error_log` with `err_nr = 2` (for 'Core_Procedure_Call') and a message indicating the simulated failure.
6.  One entry in `temp_core_sp_calls` with `simulated_failure = TRUE`.

```python
# Example pytest assertion
import pytest

def test_core_sp_failure_handling(bq_client):
    bq_client.query("TRUNCATE TABLE `project.dataset.job_control`").result()
    bq_client.query("TRUNCATE TABLE `project.dataset.job_audit_log`").result()
    bq_client.query("TRUNCATE TABLE `project.dataset.job_error_log`").result()
    bq_client.query("DROP TEMPORARY TABLE IF EXISTS `temp_core_sp_calls`").result()

    with pytest.raises(Exception) as excinfo:
        bq_client.query("CALL `project.dataset.ausd_bp_ta_cntrct_dist_wrapper`('01012023', 0, TRUE)").result()
    
    assert "Simulated core SP failure" in str(excinfo.value)

    job_control_df = bq_client.query("SELECT * FROM `project.dataset.job_control`").result().to_dataframe()
    audit_log_df = bq_client.query("SELECT * FROM `project.dataset.job_audit_log`").result().to_dataframe()
    error_log_df = bq_client.query("SELECT * FROM `project.dataset.job_error_log`").result().to_dataframe()
    core_calls_df = bq_client.query("SELECT * FROM `temp_core_sp_calls`").result().to_dataframe()

    assert len(job_control_df) == 1
    assert job_control_df.iloc[0]['status'] == 'FAILED'
    assert job_control_df.iloc[0]['finished_at'] is not None

    assert len(audit_log_df) == 2 # Job started, Core procedure failed
    assert "Job started" in audit_log_df.iloc[0]['message']
    assert "Core procedure `ausd_bp_ta_cntrct_dist_core` failed" in audit_log_df.iloc[1]['message']

    assert len(error_log_df) == 1
    assert error_log_df.iloc[0]['err_nr'] == 2
    assert "Simulated core SP failure" in error_log_df.iloc[0]['message']

    assert len(core_calls_df) == 1
    assert core_calls_df.iloc[0]['simulated_failure'] == True
```

---

## Test Case 7: Logging Content and Sequence (Successful Run)

**Purpose:** Verify that `job_control` and `job_audit_log` entries are created with correct data and in the expected sequence for a successful run, reflecting the legacy `DWMSG_*` functions.

**Setup:**
1.  Ensure log tables and `temp_core_sp_calls` are empty.

**Action:**
Execute the wrapper stored procedure with valid parameters.

```sql
CALL `project.dataset.ausd_bp_ta_cntrct_dist_wrapper`('20042023', 100);
```

**Pass/Fail Criterion:**
1.  One `job_control` entry exists with `job_kennung = 'ausd_bp_ta_cntrct_dist_wrapper'`, `source_program = 'r_ausd_bp_ta_cntrct_dist.ksh'`, `stichtag = '20042023'`, `restart_value = 100`, `status = 'SUCCESS'`, and `created_at` and `finished_at` are populated.
2.  Two `job_audit_log` entries exist for the same `job_nr`:
    *   The first entry's `message` contains "Job started. Effective Stichtag: 20042023, Restart Value: 100".
    *   The second entry's `message` contains "Core procedure `ausd_bp_ta_cntrct_dist_core` completed successfully."
    *   Both `job_audit_log` entries have `job_kennung = 'ausd_bp_ta_cntrct_dist_wrapper'`, `stichtag = '20042023'`, `sysdate` matching `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`, and `log_file_name = 'placeholder_log_file.log'`.
3.  `project.dataset.job_error_log` is empty.

---

## Test Case 8: `job_nr` Incrementing and Uniqueness

**Purpose:** Verify that the `job_nr` in `job_control` increments correctly and uniquely identifies each job execution, mimicking `DW_EintragsNr`.

**Setup:**
1.  Ensure log tables and `temp_core_sp_calls` are empty.

**Action:**
Execute the wrapper stored procedure multiple times with different parameters.

```sql
CALL `project.dataset.ausd_bp_ta_cntrct_dist_wrapper`('01012023', 1);
CALL `project.dataset.ausd_bp_ta_cntrct_dist_wrapper`('02012023', 2);
CALL `project.dataset.ausd_bp_ta_cntrct_dist_wrapper`('03012023', 3);
```

**Pass/Fail Criterion:**
1.  Three distinct entries in `project.dataset.job_control`.
2.  The `job_nr` values in `job_control` are sequential integers (e.g., 1, 2, 3).
3.  For each `job_nr` in `job_control`, corresponding `job_audit_log` entries exist and correctly reference that `job_nr`.
4.  The `job_nr` in `job_control` is unique for each execution.

```python
# Example pytest assertion
def test_job_nr_incrementing(bq_client):
    bq_client.query("TRUNCATE TABLE `project.dataset.job_control`").result()
    bq_client.query("TRUNCATE TABLE `project.dataset.job_audit_log`").result()
    bq_client.query("TRUNCATE TABLE `project.dataset.job_error_log`").result()
    bq_client.query("DROP TEMPORARY TABLE IF EXISTS `temp_core_sp_calls`").result()

    bq_client.query("CALL `project.dataset.ausd_bp_ta_cntrct_dist_wrapper`('01012023', 1)").result()
    bq_client.query("CALL `project.dataset.ausd_bp_ta_cntrct_dist_wrapper`('02012023', 2)").result()
    bq_client.query("CALL `project.dataset.ausd_bp_ta_cntrct_dist_wrapper`('03012023', 3)").result()

    job_control_df = bq_client.query("SELECT job_nr FROM `project.dataset.job_control` ORDER BY job_nr").result().to_dataframe()
    
    assert len(job_control_df) == 3
    assert list(job_control_df['job_nr']) == [1, 2, 3] # Assuming starting from 1

    # Verify audit logs also reference correct job_nr
    audit_log_df = bq_client.query("SELECT job_nr, COUNT(*) as log_count FROM `project.dataset.job_audit_log` GROUP BY job_nr ORDER BY job_nr").result().to_dataframe()
    assert len(audit_log_df) == 3
    assert all(audit_log_df['log_count'] == 2) # 2 logs per successful job
```

---

## Test Case 9: Metadata Mapping (`job_kennung`, `source_program`, `log_file_name`)

**Purpose:** Verify that the static metadata fields like `ProgName`, `ProgVersion`, `JobKennung`, and `LogDatei` from the legacy script are correctly mapped to their BigQuery counterparts (`job_kennung`, `source_program`, `log_file_name`).

**Setup:**
1.  Ensure log tables and `temp_core_sp_calls` are empty.

**Action:**
Execute the wrapper stored procedure.

```sql
CALL `project.dataset.ausd_bp_ta_cntrct_dist_wrapper`('10052023', 0);
```

**Pass/Fail Criterion:**
1.  The `job_control` entry has `job_kennung = 'ausd_bp_ta_cntrct_dist_wrapper'` and `source_program = 'r_ausd_bp_ta_cntrct_dist.ksh'`.
2.  All `job_audit_log` entries have `job_kennung = 'ausd_bp_ta_cntrct_dist_wrapper'` and `log_file_name = 'placeholder_log_file.log'`.
3.  All `job_error_log` entries (if any) have `job_kennung = 'ausd_bp_ta_cntrct_dist_wrapper'`.

---