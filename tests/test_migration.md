The migration of `r_ausd_bp_ta_bpr_basis.ksh` to a BigQuery Stored Procedure `project.dataset.Bereitstellung_Basisprodukte_BERT` primarily involves replicating control flow, parameter handling, and logging. The core business logic resides in a separate kernel script (`k_ausd_bp_ta_bpr_basis.ksh`), which is also being migrated.

These tests focus on the wrapper procedure's behavior, ensuring it correctly handles inputs, defaults, validation, logging, and orchestrates the call to the kernel procedure.

**Assumptions for Testing:**
*   The BigQuery DDLs for `job_audit_log`, `job_error_log`, and `job_run_log` have been executed in `my_project.my_dataset`.
*   The `k_ausd_bp_ta_bpr_basis` stored procedure exists in `my_project.my_dataset`. For some tests, it will be temporarily modified to record its inputs or simulate failure.
*   The `DW_EintragsNr` derivation in the BigQuery Stored Procedure (`SET DW_EintragsNr = (SELECT IFNULL(MAX(job_id), 0) + 1 FROM project.dataset.job_audit_log);`) is assumed to be corrected to `FROM project.dataset.job_run_log` as `job_audit_log` does not contain a `job_id` column in its DDL. If this correction is not made, Test Case 7 will fail, indicating a defect.

---

### Test Case 1: Successful Execution - All Parameters Provided

*   **Purpose**: Verify the stored procedure executes successfully when both `p_stichtag` and `p_wiederanlaufWert` are explicitly provided. This tests parameter parsing, correct value assignment, and successful logging.
*   **Setup**:
    1.  Ensure `my_project.my_dataset.job_audit_log`, `my_project.my_dataset.job_error_log`, `my_project.my_dataset.job_run_log` tables are empty or truncated.
    2.  Ensure `my_project.my_dataset.k_ausd_bp_ta_bpr_basis` is configured as a functional placeholder that records its inputs and returns successfully (see "Temporary `k_ausd_bp_ta_bpr_basis` for Testing" below).
*   **Action**: Call the BigQuery Stored Procedure with specific valid parameters.
    ```sql
    CALL `my_project.my_dataset.Bereitstellung_Basisprodukte_BERT`('01012023', 12345);
    ```
*   **Pass/Fail Criterion**:
    1.  The call completes without raising an `SQLSTATE` error.
    2.  `my_project.my_dataset.job_error_log` contains 0 rows.
    3.  `my_project.my_dataset.job_audit_log` contains exactly two entries for `job_name = 'ausd_bp_ta_bpr_basis'`:
        *   One with `status = 'STARTED'`, `stichtag = '01012023'`, `wiederanlaufwert = 12345`.
        *   One with `status = 'SUCCESS'`, `stichtag = '01012023'`, `wiederanlaufwert = 12345`.
    4.  `my_project.my_dataset.job_run_log` contains exactly one entry for `job_name = 'ausd_bp_ta_bpr_basis'`:
        *   `status = 'OK'`, `stichtag = '01012023'`, `wiederanlaufwert` (if present in `job_run_log`) = `12345`.
    5.  `my_project.my_dataset.k_ausd_bp_ta_bpr_basis_test_log` contains one entry where:
        *   `job_kennung = 'ausd_bp_ta_bpr_basis'`, `stichtag = '01012023'`, `wiederanlaufwert = 12345`.
        *   `dw_eintragsnr` matches the `job_id` from `my_project.my_dataset.job_run_log`.

---

### Test Case 2: Successful Execution - Default `p_stichtag`

*   **Purpose**: Verify the stored procedure correctly defaults `p_stichtag` to the current system date (`DDMMYYYY`) when not provided, and executes successfully.
*   **Setup**:
    1.  Truncate log tables.
    2.  Ensure `my_project.my_dataset.k_ausd_bp_ta_bpr_basis` is configured as a functional placeholder (see "Temporary `k_ausd_bp_ta_bpr_basis` for Testing" below).
*   **Action**: Call the BigQuery Stored Procedure without `p_stichtag_input`.
    ```sql
    CALL `my_project.my_dataset.Bereitstellung_Basisprodukte_BERT`(NULL, 54321);
    ```
*   **Pass/Fail Criterion**:
    1.  The call completes without raising an `SQLSTATE` error.
    2.  `my_project.my_dataset.job_error_log` contains 0 rows.
    3.  `my_project.my_dataset.job_audit_log` contains exactly two entries for `job_name = 'ausd_bp_ta_bpr_basis'`:
        *   One with `status = 'STARTED'`, `stichtag = FORMAT_DATE('%d%m%Y', CURRENT_DATE())`, `wiederanlaufwert = 54321`.
        *   One with `status = 'SUCCESS'`, `stichtag = FORMAT_DATE('%d%m%Y', CURRENT_DATE())`, `wiederanlaufwert = 54321`.
    4.  `my_project.my_dataset.job_run_log` contains exactly one entry for `job_name = 'ausd_bp_ta_bpr_basis'`:
        *   `status = 'OK'`, `stichtag = FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
    5.  `my_project.my_dataset.k_ausd_bp_ta_bpr_basis_test_log` contains one entry where:
        *   `stichtag = FORMAT_DATE('%d%m%Y', CURRENT_DATE())`, `wiederanlaufwert = 54321`.

---

### Test Case 3: Successful Execution - Default `p_wiederanlaufWert`

*   **Purpose**: Verify the stored procedure correctly defaults `p_wiederanlaufWert` to `0` when not provided, and executes successfully.
*   **Setup**:
    1.  Truncate log tables.
    2.  Ensure `my_project.my_dataset.k_ausd_bp_ta_bpr_basis` is configured as a functional placeholder (see "Temporary `k_ausd_bp_ta_bpr_basis` for Testing" below).
*   **Action**: Call the BigQuery Stored Procedure without `p_wiederanlaufWert_input`.
    ```sql
    CALL `my_project.my_dataset.Bereitstellung_Basisprodukte_BERT`('15032024', NULL);
    ```
*   **Pass/Fail Criterion**:
    1.  The call completes without raising an `SQLSTATE` error.
    2.  `my_project.my_dataset.job_error_log` contains 0 rows.
    3.  `my_project.my_dataset.job_audit_log` contains exactly two entries for `job_name = 'ausd_bp_ta_bpr_basis'`:
        *   One with `status = 'STARTED'`, `stichtag = '15032024'`, `wiederanlaufwert = 0`.
        *   One with `status = 'SUCCESS'`, `stichtag = '15032024'`, `wiederanlaufwert = 0`.
    4.  `my_project.my_dataset.job_run_log` contains exactly one entry for `job_name = 'ausd_bp_ta_bpr_basis'`:
        *   `status = 'OK'`, `stichtag = '15032024'`.
    5.  `my_project.my_dataset.k_ausd_bp_ta_bpr_basis_test_log` contains one entry where:
        *   `stichtag = '15032024'`, `wiederanlaufwert = 0`.

---

### Test Case 4: Successful Execution - Both Parameters Defaulted

*   **Purpose**: Verify the stored procedure correctly defaults both `p_stichtag` (to current system date) and `p_wiederanlaufWert` (to `0`) when neither is provided, and executes successfully.
*   **Setup**:
    1.  Truncate log tables.
    2.  Ensure `my_project.my_dataset.k_ausd_bp_ta_bpr_basis` is configured as a functional placeholder (see "Temporary `k_ausd_bp_ta_bpr_basis` for Testing" below).
*   **Action**: Call the BigQuery Stored Procedure with both parameters as `NULL`.
    ```sql
    CALL `my_project.my_dataset.Bereitstellung_Basisprodukte_BERT`(NULL, NULL);
    ```
*   **Pass/Fail Criterion**:
    1.  The call completes without raising an `SQLSTATE` error.
    2.  `my_project.my_dataset.job_error_log` contains 0 rows.
    3.  `my_project.my_dataset.job_audit_log` contains exactly two entries for `job_name = 'ausd_bp_ta_bpr_basis'`:
        *   One with `status = 'STARTED'`, `stichtag = FORMAT_DATE('%d%m%Y', CURRENT_DATE())`, `wiederanlaufwert = 0`.
        *   One with `status = 'SUCCESS'`, `stichtag = FORMAT_DATE('%d%m%Y', CURRENT_DATE())`, `wiederanlaufwert = 0`.
    4.  `my_project.my_dataset.job_run_log` contains exactly one entry for `job_name = 'ausd_bp_ta_bpr_basis'`:
        *   `status = 'OK'`, `stichtag = FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
    5.  `my_project.my_dataset.k_ausd_bp_ta_bpr_basis_test_log` contains one entry where:
        *   `stichtag = FORMAT_DATE('%d%m%Y', CURRENT_DATE())`, `wiederanlaufwert = 0`.

---

### Test Case 5: Error Handling - Missing/Empty `p_stichtag` (After Defaulting)

*   **Purpose**: Verify the stored procedure correctly identifies and handles the error condition where `p_stichtag` is ultimately `NULL` or empty after defaulting logic, logging the error and signaling an `SQLSTATE`. This covers the `ErrNr=193` case from the legacy script.
*   **Setup**:
    1.  Truncate log tables.
    2.  Ensure `my_project.my_dataset.k_ausd_bp_ta_bpr_basis` is configured as a functional placeholder (its execution should not be reached).
*   **Action**: Call the BigQuery Stored Procedure with `p_stichtag_input` as an empty string.
    ```sql
    CALL `my_project.my_dataset.Bereitstellung_Basisprodukte_BERT`('', 123);
    ```
*   **Pass/Fail Criterion**:
    1.  The call fails with an `SQLSTATE '45000'` error and `MESSAGE_TEXT = 'Parameter validation failed'`.
    2.  `my_project.my_dataset.job_error_log` contains exactly one entry:
        *   `job_name = 'ausd_bp_ta_bpr_basis'`, `error_nr = 193`, `error_arg = 'Stichtag'`.
    3.  `my_project.my_dataset.job_audit_log` contains 0 rows (no `STARTED` or `SUCCESS` entries).
    4.  `my_project.my_dataset.job_run_log` contains 0 rows.
    5.  `my_project.my_dataset.k_ausd_bp_ta_bpr_basis_test_log` contains 0 rows (the kernel procedure was not called).

---

### Test Case 6: Error Handling - Kernel Script Failure

*   **Purpose**: Verify the stored procedure correctly handles a failure in the invoked kernel script, logging the error and updating job status to 'FAILED'. This tests the `EXCEPTION WHEN ERROR` block.
*   **Setup**:
    1.  Truncate log tables.
    2.  Modify `my_project.my_dataset.k_ausd_bp_ta_bpr_basis` to intentionally raise an error (see "Temporary `k_ausd_bp_ta_bpr_basis` for Testing" below).
*   **Action**: Call the BigQuery Stored Procedure with valid parameters.
    ```sql
    CALL `my_project.my_dataset.Bereitstellung_Basisprodukte_BERT`('01012023', 12345);
    ```
*   **Pass/Fail Criterion**:
    1.  The call fails with an `SQLSTATE '45000'` error and `MESSAGE_TEXT` indicating the execution failure (e.g., `Execution failed for job 'ausd_bp_ta_bpr_basis' with error: Simulated kernel script failure`).
    2.  `my_project.my_dataset.job_error_log` contains exactly one entry:
        *   `job_name = 'ausd_bp_ta_bpr_basis'`, `error_nr = 999`, `error_arg = 'Unhandled exception'`.
    3.  `my_project.my_dataset.job_audit_log` contains exactly two entries for `job_name = 'ausd_bp_ta_bpr_basis'`:
        *   One with `status = 'STARTED'`.
        *   One with `status = 'FAILED'`. (There should be no `SUCCESS` entry).
    4.  `my_project.my_dataset.job_run_log` contains exactly one entry for `job_name = 'ausd_bp_ta_bpr_basis'`:
        *   `status = 'FAILED'`.
    5.  `my_project.my_dataset.k_ausd_bp_ta_bpr_basis_test_log` contains one entry (indicating the kernel procedure was called before it failed).

---

### Test Case 7: `DW_EintragsNr` Incrementing Correctly

*   **Purpose**: Verify that `DW_EintragsNr` is correctly derived by incrementing the maximum `job_id` from `job_run_log`, ensuring unique and sequential IDs for job runs.
*   **Setup**:
    1.  Truncate log tables.
    2.  Ensure `my_project.my_dataset.k_ausd_bp_ta_bpr_basis` is configured as a functional placeholder (see "Temporary `k_ausd_bp_ta_bpr_basis` for Testing" below).
*   **Action**:
    1.  Call the BigQuery Stored Procedure successfully:
        ```sql
        CALL `my_project.my_dataset.Bereitstellung_Basisprodukte_BERT`('01012023', 1);
        ```
    2.  Call the BigQuery Stored Procedure successfully again:
        ```sql
        CALL `my_project.my_dataset.Bereitstellung_Basisprodukte_BERT`('02012023', 2);
        ```
*   **Pass/Fail Criterion**:
    1.  Both calls complete successfully.
    2.  Query `SELECT job_id FROM my_project.my_dataset.job_run_log WHERE job_name = 'ausd_bp_ta_bpr_basis' ORDER BY created_at ASC;`. The `job_id` for the second run must be exactly one greater than the `job_id` for the first run.
    3.  The `dw_eintragsnr` recorded in `my_project.my_dataset.k_ausd_bp_ta_bpr_basis_test_log` for each run must match the respective `job_id` from `my_project.my_dataset.job_run_log`.

---

### Test Case 8: `LogDatei` Naming Convention

*   **Purpose**: Verify the `LogDatei` variable is constructed according to the specified naming convention (`job_<job_id>_<JobKennung>.log`), even though it's not used for actual file logging. This ensures parity with the legacy script's internal logic.
*   **Setup**:
    1.  Truncate log tables.
    2.  Ensure `my_project.my_dataset.k_ausd_bp_ta_bpr_basis` is configured as a functional placeholder (see "Temporary `k_ausd_bp_ta_bpr_basis` for Testing" below).
*   **Action**: Call the BigQuery Stored Procedure successfully.
    ```sql
    CALL `my_project.my_dataset.Bereitstellung_Basisprodukte_BERT`('01012023', 12345);
    ```
*   **Pass/Fail Criterion**:
    1.  The call completes successfully.
    2.  Query `SELECT job_id, log_file FROM my_project.my_dataset.job_run_log WHERE job_name = 'ausd_bp_ta_bpr_basis';`.
    3.  The `log_file` column for the entry must match the pattern `job_<job_id>_ausd_bp_ta_bpr_basis.log`. For example, if `job_id` is `1`, `log_file` should be `job_1_ausd_bp_ta_bpr_basis.log`.

---

### Test Case 9: `v_sysdate` Handling

*   **Purpose**: Verify that `v_sysdate` is correctly determined as `FORMAT_DATE('%d%m%Y', CURRENT_DATE())` and used consistently for defaulting `p_stichtag` and in log entries.
*   **Setup**:
    1.  Truncate log tables.
    2.  Ensure `my_project.my_dataset.k_ausd_bp_ta_bpr_basis` is configured as a functional placeholder (see "Temporary `k_ausd_bp_ta_bpr_basis` for Testing" below).
*   **Action**: Call the BigQuery Stored Procedure with `p_stichtag_input = NULL`.
    ```sql
    CALL `my_project.my_dataset.Bereitstellung_Basisprodukte_BERT`(NULL, 123);
    ```
*   **Pass/Fail Criterion**:
    1.  The call completes successfully.
    2.  Query `SELECT stichtag, sysdate_value FROM my_project.my_dataset.job_audit_log WHERE status = 'SUCCESS' AND job_name = 'ausd_bp_ta_bpr_basis';`.
    3.  Both `stichtag` and `sysdate_value` must be equal to `FORMAT_DATE('%d%m%Y', CURRENT_DATE())` (today's date in `DDMMYYYY` format).
    4.  Query `SELECT stichtag, sysdate_value FROM my_project.my_dataset.job_run_log WHERE status = 'OK' AND job_name = 'ausd_bp_ta_bpr_basis';`.
    5.  Both `stichtag` and `sysdate_value` must be equal to `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.

---

### Temporary `k_ausd_bp_ta_bpr_basis` for Testing

To facilitate testing of parameter passing and error handling for the kernel script, `my_project.my_dataset.k_ausd_bp_ta_bpr_basis` can be temporarily modified as follows:

**1. Functional Placeholder (for successful runs):**
This version records the parameters it received into a dedicated test log table.

```sql
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.k_ausd_bp_ta_bpr_basis`(
  IN p_JobKennung STRING,
  IN p_Stichtag STRING,
  IN p_DW_EintragsNr INT64,
  IN p_WiederanlaufWert INT64
)
BEGIN
  -- Create a temporary table to log parameters if it doesn't exist
  CREATE TABLE IF NOT EXISTS `my_project.my_dataset.k_ausd_bp_ta_bpr_basis_test_log` (
    job_kennung STRING,
    stichtag STRING,
    dw_eintragsnr INT64,
    wiederanlaufwert INT64,
    call_timestamp TIMESTAMP
  );
  -- Insert the received parameters into the log table
  INSERT INTO `my_project.my_dataset.k_ausd_bp_ta_bpr_basis_test_log` (job_kennung, stichtag, dw_eintragsnr, wiederanlaufwert, call_timestamp)
  VALUES (p_JobKennung, p_Stichtag, p_DW_EintragsNr, p_WiederanlaufWert, CURRENT_TIMESTAMP());
  -- Simulate successful execution
  SELECT 'k_ausd_bp_ta_bpr_basis executed successfully (placeholder).';
END;
```

**2. Failure Simulation (for error handling tests, e.g., Test Case 6):**
This version records parameters and then immediately raises an error.

```sql
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.k_ausd_bp_ta_bpr_basis`(
  IN p_JobKennung STRING,
  IN p_Stichtag STRING,
  IN p_DW_EintragsNr INT64,
  IN p_WiederanlaufWert INT64
)
BEGIN
  -- Create a temporary table to log parameters if it doesn't exist
  CREATE TABLE IF NOT EXISTS `my_project.my_dataset.k_ausd_bp_ta_bpr_basis_test_log` (
    job_kennung STRING,
    stichtag STRING,
    dw_eintragsnr INT64,
    wiederanlaufwert INT64,
    call_timestamp TIMESTAMP
  );
  -- Insert the received parameters into the log table
  INSERT INTO `my_project.my_dataset.k_ausd_bp_ta_bpr_basis_test_log` (job_kennung, stichtag, dw_eintragsnr, wiederanlaufwert, call_timestamp)
  VALUES (p_JobKennung, p_Stichtag, p_DW_EintragsNr, p_WiederanlaufWert, CURRENT_TIMESTAMP());
  -- Simulate failure
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated kernel script failure';
END;
```