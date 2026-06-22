The migration of `k_ausd_bp_ta_bpr_basis_his.ksh` involves a significant shift from a KornShell-orchestrated, SQL*Plus-driven process to an Airflow-orchestrated, BigQuery-native solution. The tests below focus on validating the behavioral equivalence of the new orchestration layer, parameter handling, date logic, and auditing mechanisms, assuming the core `d_ausd_bp_ta_bpr_basis_his` BigQuery stored procedure (SP) will be tested separately for its internal data transformations.

---

## Migration Validation Tests for `k_ausd_bp_ta_bpr_basis_his.ksh`

These tests aim to prove that the migrated BigQuery Stored Procedure (`r_ausd_bp_ta_bpr_basis_his`) and its Airflow orchestration behave equivalently to the legacy KornShell script.

**Assumptions:**
*   The `project.dataset.d_ausd_bp_ta_bpr_basis_his` BigQuery Stored Procedure, while currently a placeholder, is assumed to correctly implement the data transformation logic of the original `d_ausd_bp_ta_bpr_basis_his.sql` and populate `project.dataset.PoolBasisprodukt`.
*   BigQuery datasets `project.dataset` and `project.audit` exist.
*   The Airflow DAG `k_ausd_bp_ta_bpr_basis_his_orchestration` is deployed and configured.
*   Each test case starts with a clean state for the target tables (`PoolBasisprodukt`, `job_audit`, `error_log`) to ensure isolation.

---

### Test Case 1.1: Successful Execution - Parameter Passing and Date Derivation

*   **Purpose:** Verify that the Airflow DAG correctly triggers the BigQuery orchestrator SP, all parameters are passed as expected, date derivation (`heute`, `gestern`) is accurate, and the core SP is called successfully, leading to a successful audit entry and data insertion.
*   **Setup:**
    1.  Ensure `project.dataset.PoolBasisprodukt`, `project.audit.job_audit`, and `project.audit.error_log` tables are empty.
    2.  Ensure the `project.dataset.d_ausd_bp_ta_bpr_basis_his` SP is in its default (dummy) state, inserting one record.
*   **Action:**
    1.  Trigger the Airflow DAG `k_ausd_bp_ta_bpr_basis_his_orchestration` with the following parameters:
        *   `job_kennung`: `'TEST_JOB_001'`
        *   `eintrags_nr`: `'ENTRY_001'`
        *   `stichtag`: `'01012023'`
        *   `wiederanlauf_wert`: `'100'`
*   **Pass/Fail Criterion:**
    *   The Airflow task `execute_bigquery_orchestrator_sp` completes successfully.
    *   `project.dataset.PoolBasisprodukt` contains exactly one new record with `stichtag = '2023-01-01'`, `eintrags_nr = 'ENTRY_001'`, and `job_kennung = 'TEST_JOB_001'`.
    *   `project.audit.job_audit` contains two entries for `job_name = 'k_ausd_bp_ta_bpr_basis_his'` and the same `job_id`:
        *   One with `status = 'STARTED'`, `start_time` populated.
        *   One with `status = 'COMPLETED'`, `start_time`, `end_time`, `duration_seconds` populated, `processed_records = 1`, and `parameters` JSON matching the input.
    *   `project.audit.error_log` contains no new entries.

*   **Runnable Test Code (SQL Assertions):**
    ```sql
    -- Assert PoolBasisprodukt content
    SELECT
        COUNT(*) AS record_count,
        COUNTIF(stichtag = PARSE_DATE('%Y-%m-%d', '2023-01-01')) AS correct_stichtag_count,
        COUNTIF(eintrags_nr = 'ENTRY_001') AS correct_eintrags_nr_count,
        COUNTIF(job_kennung = 'TEST_JOB_001') AS correct_job_kennung_count
    FROM `project.dataset.PoolBasisprodukt`;
    -- Expected: record_count = 1, correct_stichtag_count = 1, correct_eintrags_nr_count = 1, correct_job_kennung_count = 1

    -- Assert job_audit entries
    SELECT
        status,
        processed_records,
        JSON_EXTRACT_SCALAR(parameters, '$.p_JobKennung') AS p_JobKennung_param,
        JSON_EXTRACT_SCALAR(parameters, '$.p_EintragsNr') AS p_EintragsNr_param,
        JSON_EXTRACT_SCALAR(parameters, '$.p_Stichtag') AS p_Stichtag_param,
        JSON_EXTRACT_SCALAR(parameters, '$.p_wiederanlaufWert') AS p_wiederanlaufWert_param
    FROM `project.audit.job_audit`
    WHERE job_name = 'k_ausd_bp_ta_bpr_basis_his'
    ORDER BY audit_timestamp ASC;
    /* Expected result (example):
    [
      {"status": "STARTED", "processed_records": null, "p_JobKennung_param": "TEST_JOB_001", "p_EintragsNr_param": "ENTRY_001", "p_Stichtag_param": "01012023", "p_wiederanlaufWert_param": "100"},
      {"status": "COMPLETED", "processed_records": 1, "p_JobKennung_param": "TEST_JOB_001", "p_EintragsNr_param": "ENTRY_001", "p_Stichtag_param": "01012023", "p_wiederanlaufWert_param": "100"}
    ]
    */

    -- Assert error_log is empty
    SELECT COUNT(*) FROM `project.audit.error_log` WHERE job_name = 'k_ausd_bp_ta_bpr_basis_his';
    -- Expected: 0
    ```

---

### Test Case 1.2: `wiederanlaufWert` Defaulting

*   **Purpose:** Verify that the `p_wiederanlaufWert` parameter correctly defaults to `'0'` if not provided, mirroring the legacy script's behavior.
*   **Setup:**
    1.  Ensure `project.dataset.PoolBasisprodukt`, `project.audit.job_audit`, and `project.audit.error_log` tables are empty.
*   **Action:**
    1.  Trigger the Airflow DAG `k_ausd_bp_ta_bpr_basis_his_orchestration` with valid parameters, but omit or provide an empty string for `wiederanlauf_wert`:
        *   `job_kennung`: `'TEST_JOB_002'`
        *   `eintrags_nr`: `'ENTRY_002'`
        *   `stichtag`: `'02012023'`
        *   `wiederanlauf_wert`: `''` (empty string)
*   **Pass/Fail Criterion:**
    *   The Airflow task completes successfully.
    *   `project.audit.job_audit` contains a `COMPLETED` entry where the `parameters` JSON shows `p_wiederanlaufWert` as `'0'`.
    *   `project.dataset.PoolBasisprodukt` contains one new record with `stichtag = '2023-01-02'`, `eintrags_nr = 'ENTRY_002'`, and `job_kennung = 'TEST_JOB_002'`.

*   **Runnable Test Code (SQL Assertions):**
    ```sql
    -- Assert job_audit parameters for defaulting
    SELECT
        JSON_EXTRACT_SCALAR(parameters, '$.p_wiederanlaufWert') AS p_wiederanlaufWert_param
    FROM `project.audit.job_audit`
    WHERE job_name = 'k_ausd_bp_ta_bpr_basis_his'
      AND status = 'COMPLETED'
      AND JSON_EXTRACT_SCALAR(parameters, '$.p_JobKennung') = 'TEST_JOB_002';
    -- Expected: '0'

    -- Assert PoolBasisprodukt content
    SELECT COUNT(*) FROM `project.dataset.PoolBasisprodukt`
    WHERE stichtag = PARSE_DATE('%Y-%m-%d', '2023-01-02')
      AND eintrags_nr = 'ENTRY_002'
      AND job_kennung = 'TEST_JOB_002';
    -- Expected: 1
    ```

---

### Test Case 2.1: Missing Mandatory Parameter (`JobKennung`)

*   **Purpose:** Verify that the orchestrator SP correctly identifies and handles a missing mandatory parameter (`JobKennung`), logging an error and marking the job as failed.
*   **Setup:**
    1.  Ensure `project.audit.job_audit` and `project.audit.error_log` tables are empty.
*   **Action:**
    1.  Trigger the Airflow DAG `k_ausd_bp_ta_bpr_basis_his_orchestration` with `job_kennung` as an empty string:
        *   `job_kennung`: `''`
        *   `eintrags_nr`: `'ENTRY_FAIL_01'`
        *   `stichtag`: `'03012023'`
        *   `wiederanlauf_wert`: `'0'`
*   **Pass/Fail Criterion:**
    *   The Airflow task `execute_bigquery_orchestrator_sp` fails.
    *   `project.audit.job_audit` contains two entries for the job:
        *   One with `status = 'STARTED'`.
        *   One with `status = 'FAILED'`, `end_time` populated, `processed_records = 0`.
    *   `project.audit.error_log` contains one entry with:
        *   `job_name = 'k_ausd_bp_ta_bpr_basis_his'`
        *   `error_message` containing `'FEHLER: JobKennung Parameter fehlt.'`
        *   `error_detail` containing `'Mandatory parameter p_JobKennung is missing or empty.'`
    *   `project.dataset.PoolBasisprodukt` contains no new records from this run.

*   **Runnable Test Code (SQL Assertions):**
    ```sql
    -- Assert job_audit entries for failure
    SELECT status, processed_records FROM `project.audit.job_audit`
    WHERE job_name = 'k_ausd_bp_ta_bpr_basis_his'
    ORDER BY audit_timestamp ASC;
    /* Expected result (example):
    [
      {"status": "STARTED", "processed_records": null},
      {"status": "FAILED", "processed_records": 0}
    ]
    */

    -- Assert error_log content
    SELECT error_message, error_detail FROM `project.audit.error_log`
    WHERE job_name = 'k_ausd_bp_ta_bpr_basis_his'
      AND error_message LIKE '%JobKennung Parameter fehlt%';
    /* Expected result:
    [
      {"error_message": "FEHLER: JobKennung Parameter fehlt.", "error_detail": "Mandatory parameter p_JobKennung is missing or empty."}
    ]
    */

    -- Assert PoolBasisprodukt is unchanged (assuming it was empty before this run)
    SELECT COUNT(*) FROM `project.dataset.PoolBasisprodukt`
    WHERE stichtag = PARSE_DATE('%Y-%m-%d', '2023-01-03');
    -- Expected: 0
    ```

---

### Test Case 2.2: Invalid `Stichtag` Format

*   **Purpose:** Verify that the orchestrator SP correctly validates the `Stichtag` format (`DDMMYYYY`), logging an error and failing the job if the format is incorrect.
*   **Setup:**
    1.  Ensure `project.audit.job_audit` and `project.audit.error_log` tables are empty.
*   **Action:**
    1.  Trigger the Airflow DAG `k_ausd_bp_ta_bpr_basis_his_orchestration` with an invalid `stichtag` format:
        *   `job_kennung`: `'TEST_JOB_FAIL_02'`
        *   `eintrags_nr`: `'ENTRY_FAIL_02'`
        *   `stichtag`: `'2023-01-04'` (YYYY-MM-DD instead of DDMMYYYY)
        *   `wiederanlauf_wert`: `'0'`
*   **Pass/Fail Criterion:**
    *   The Airflow task `execute_bigquery_orchestrator_sp` fails.
    *   `project.audit.job_audit` contains a `FAILED` entry.
    *   `project.audit.error_log` contains one entry with:
        *   `job_name = 'k_ausd_bp_ta_bpr_basis_his'`
        *   `error_message` containing `'FEHLER: Ungueltiges Datumsformat fuer Stichtag.'`
        *   `error_detail` containing `'Input Stichtag '2023-01-04' does not match format 'DDMMYYYY'.'`
    *   `project.dataset.PoolBasisprodukt` contains no new records from this run.

*   **Runnable Test Code (SQL Assertions):**
    ```sql
    -- Assert job_audit entries for failure
    SELECT status FROM `project.audit.job_audit`
    WHERE job_name = 'k_ausd_bp_ta_bpr_basis_his'
    ORDER BY audit_timestamp ASC;
    /* Expected result (example):
    [
      {"status": "STARTED"},
      {"status": "FAILED"}
    ]
    */

    -- Assert error_log content
    SELECT error_message, error_detail FROM `project.audit.error_log`
    WHERE job_name = 'k_ausd_bp_ta_bpr_basis_his'
      AND error_message LIKE '%Ungueltiges Datumsformat%';
    /* Expected result:
    [
      {"error_message": "FEHLER: Ungueltiges Datumsformat fuer Stichtag.", "error_detail": "Input Stichtag '2023-01-04' does not match format 'DDMMYYYY'."}
    ]
    */
    ```

---

### Test Case 2.3: Core SQL SP Failure (Simulated)

*   **Purpose:** Verify that if the `d_ausd_bp_ta_bpr_basis_his` SP fails, the orchestrator SP catches the error, logs it, and marks the overall job as failed, preventing partial data writes.
*   **Setup:**
    1.  **Modify `d_ausd_bp_ta_bpr_basis_his`:** Temporarily modify the `project.dataset.d_ausd_bp_ta_bpr_basis_his` stored procedure to intentionally raise an error.
        ```sql
        CREATE OR REPLACE PROCEDURE `project.dataset.d_ausd_bp_ta_bpr_basis_his`(
          IN p_EintragsNr STRING,
          IN p_JobKennung STRING,
          IN p_Stichtag STRING,
          IN p_wiederanlaufWert STRING,
          IN p_datum_heute STRING,
          IN p_datum_gestern STRING
        )
        BEGIN
          RAISE USING MESSAGE 'Simulated core SP failure'; -- INTENTIONAL ERROR
        END;
        ```
    2.  Ensure `project.audit.job_audit` and `project.audit.error_log` tables are empty.
*   **Action:**
    1.  Trigger the Airflow DAG `k_ausd_bp_ta_bpr_basis_his_orchestration` with valid parameters:
        *   `job_kennung`: `'TEST_JOB_FAIL_03'`
        *   `eintrags_nr`: `'ENTRY_FAIL_03'`
        *   `stichtag`: `'05012023'`
        *   `wiederanlauf_wert`: `'0'`
*   **Pass/Fail Criterion:**
    *   The Airflow task `execute_bigquery_orchestrator_sp` fails.
    *   `project.audit.job_audit` contains a `FAILED` entry.
    *   `project.audit.error_log` contains one entry with:
        *   `job_name = 'k_ausd_bp_ta_bpr_basis_his'`
        *   `error_message` containing `'Simulated core SP failure'`.
    *   `project.dataset.PoolBasisprodukt` contains no new records from this run (due to the error and BigQuery's transactional behavior for SPs).

*   **Runnable Test Code (SQL Assertions):**
    ```sql
    -- Assert job_audit entries for failure
    SELECT status FROM `project.audit.job_audit`
    WHERE job_name = 'k_ausd_bp_ta_bpr_basis_his'
    ORDER BY audit_timestamp ASC;
    /* Expected result (example):
    [
      {"status": "STARTED"},
      {"status": "FAILED"}
    ]
    */

    -- Assert error_log content
    SELECT error_message FROM `project.audit.error_log`
    WHERE job_name = 'k_ausd_bp_ta_bpr_basis_his'
      AND error_message LIKE '%Simulated core SP failure%';
    /* Expected result:
    [
      {"error_message": "Simulated core SP failure"}
    ]
    */

    -- Assert PoolBasisprodukt is unchanged
    SELECT COUNT(*) FROM `project.dataset.PoolBasisprodukt`
    WHERE stichtag = PARSE_DATE('%Y-%m-%d', '2023-01-05');
    -- Expected: 0
    ```
*   **Cleanup:** Revert the `project.dataset.d_ausd_bp_ta_bpr_basis_his` stored procedure to its original (dummy) state.

---

### Test Case 3.1: Audit Log Schema and Data Integrity

*   **Purpose:** Verify that the `job_audit` table correctly captures all expected fields for both successful and failed runs, ensuring comprehensive job tracking.
*   **Setup:**
    1.  Execute Test Case 1.1 (Successful Execution).
    2.  Execute Test Case 2.1 (Missing Mandatory Parameter).
    3.  Execute Test Case 2.2 (Invalid `Stichtag` Format).
*   **Action:**
    1.  Query the `project.audit.job_audit` table for entries related to `k_ausd_bp_ta_bpr_basis_his`.
*   **Pass/Fail Criterion:**
    *   The `job_audit` table schema matches the DDL.
    *   For `COMPLETED` entries: `status='COMPLETED'`, `start_time`, `end_time`, `duration_seconds` are populated, `processed_records` is `>0`, `parameters` JSON is valid and complete.
    *   For `FAILED` entries: `status='FAILED'`, `start_time`, `end_time`, `duration_seconds` are populated, `processed_records` is `0`, `parameters` JSON is valid and complete.
    *   `audit_timestamp`, `job_name`, `job_id`, `audited_by` are always populated for all entries.

*   **Runnable Test Code (SQL Assertions):**
    ```sql
    SELECT
        COUNT(*) AS total_audit_entries,
        COUNTIF(status = 'STARTED') AS started_count,
        COUNTIF(status = 'COMPLETED') AS completed_count,
        COUNTIF(status = 'FAILED') AS failed_count,
        COUNTIF(start_time IS NULL) AS null_start_time,
        COUNTIF(end_time IS NULL AND status != 'STARTED') AS null_end_time_non_started,
        COUNTIF(duration_seconds IS NULL AND status != 'STARTED') AS null_duration_non_started,
        COUNTIF(processed_records IS NULL AND status = 'COMPLETED') AS null_processed_records_completed,
        COUNTIF(parameters IS NULL) AS null_parameters,
        COUNTIF(job_id IS NULL) AS null_job_id,
        COUNTIF(audited_by IS NULL) AS null_audited_by
    FROM `project.audit.job_audit`
    WHERE job_name = 'k_ausd_bp_ta_bpr_basis_his';
    -- Expected: total_audit_entries >= 6 (3 STARTED, 1 COMPLETED, 2 FAILED),
    --           started_count >= 3, completed_count >= 1, failed_count >= 2,
    --           all other counts = 0
    ```

---

### Test Case 3.2: Error Log Schema and Data Integrity

*   **Purpose:** Verify that the `error_log` table correctly captures all expected fields for failed runs, providing sufficient detail for debugging and operational support.
*   **Setup:**
    1.  Execute Test Case 2.1 (Missing Mandatory Parameter).
    2.  Execute Test Case 2.2 (Invalid `Stichtag` Format).
    3.  Execute Test Case 2.3 (Core SQL SP Failure).
*   **Action:**
    1.  Query the `project.audit.error_log` table for entries related to `k_ausd_bp_ta_bpr_basis_his`.
*   **Pass/Fail Criterion:**
    *   The `error_log` table schema matches the DDL.
    *   For each error entry: `log_timestamp`, `job_name`, `job_id`, `error_message`, `error_detail`, `parameters` JSON, `logged_by` are all populated. `error_code` might be NULL if not explicitly set by BigQuery.

*   **Runnable Test Code (SQL Assertions):**
    ```sql
    SELECT
        COUNT(*) AS total_errors,
        COUNTIF(log_timestamp IS NULL) AS null_log_timestamp,
        COUNTIF(job_name IS NULL) AS null_job_name,
        COUNTIF(job_id IS NULL) AS null_job_id,
        COUNTIF(error_message IS NULL) AS null_error_message,
        COUNTIF(error_detail IS NULL) AS null_error_detail,
        COUNTIF(parameters IS NULL) AS null_parameters,
        COUNTIF(logged_by IS NULL) AS null_logged_by
    FROM `project.audit.error_log`
    WHERE job_name = 'k_ausd_bp_ta_bpr_basis_his';
    -- Expected: total_errors >= 3, all other counts = 0
    ```

---

### Test Case 4.1: `PoolBasisprodukt` Schema and Basic Data Integrity

*   **Purpose:** Verify that the `PoolBasisprodukt` table has the expected schema and that data inserted by the core SP adheres to basic data quality (e.g., `stichtag` is a valid date, mandatory fields are not NULL).
*   **Setup:**
    1.  Execute Test Case 1.1 (Successful Execution).
    2.  Execute Test Case 1.2 (`wiederanlaufWert` Defaulting).
*   **Action:**
    1.  Query the `project.dataset.PoolBasisprodukt` table.
*   **Pass/Fail Criterion:**
    *   The `PoolBasisprodukt` table schema matches the DDL provided.
    *   The `stichtag` column contains valid `DATE` values.
    *   `id`, `produkt_name`, `eintrags_nr`, `job_kennung`, `last_update_ts` are not NULL for inserted records.

*   **Runnable Test Code (SQL Assertions):**
    ```sql
    SELECT
        COUNT(*) AS total_records,
        COUNTIF(stichtag IS NULL) AS null_stichtag,
        COUNTIF(PARSE_DATE('%Y-%m-%d', CAST(stichtag AS STRING)) IS NULL) AS invalid_stichtag_format,
        COUNTIF(id IS NULL) AS null_id,
        COUNTIF(produkt_name IS NULL) AS null_produkt_name,
        COUNTIF(eintrags_nr IS NULL) AS null_eintrags_nr,
        COUNTIF(job_kennung IS NULL) AS null_job_kennung,
        COUNTIF(last_update_ts IS NULL) AS null_last_update_ts
    FROM `project.dataset.PoolBasisprodukt`;
    -- Expected: total_records >= 2, all other counts = 0
    ```

---

### Test Case 5.1: Optional File Processing (If Reactivated)

*   **Purpose:** If the commented-out `sed`, `sort`, `join` logic is reactivated, verify that the BigQuery SQL equivalent (`bigquery/sql/optional_file_processing.sql`) produces the same output as the legacy shell script.
*   **Setup:**
    1.  **Legacy Environment:**
        *   Prepare sample input files: `cibasis_data24.dat`, `cibasis_data96.dat`, `cibasis_fax.dat`.
        *   Uncomment and execute the relevant `sed`, `sort`, `join` commands in the legacy `k_ausd_bp_ta_bpr_basis_his.ksh` script (or a standalone script) to generate `cibasisprodukt.csv`.
        *   Extract the content of `cibasisprodukt.csv` for comparison.
    2.  **Migrated Environment:**
        *   Ingest the *exact same* sample input data into BigQuery tables: `project.dataset.cibasis_data24_raw`, `project.dataset.cibasis_data96_raw`, `project.dataset.cibasis_fax_raw`. Ensure schema matches the expected input format (e.g., single `STRING` column for raw lines).
        *   Execute the `bigquery/sql/optional_file_processing.sql` script to populate `project.dataset.cibasisprodukt`.
*   **Action:**
    1.  Compare the content of the legacy `cibasisprodukt.csv` with the data in `project.dataset.cibasisprodukt`. This can be done by:
        *   Exporting `project.dataset.cibasisprodukt` to a CSV and performing a file-level comparison.
        *   Loading the legacy `cibasisprodukt.csv` into a temporary BigQuery table and performing a SQL-based comparison.
*   **Pass/Fail Criterion:**
    *   The data in `project.dataset.cibasisprodukt` must be identical to the data in the legacy `cibasisprodukt.csv` (considering potential differences in column order or minor formatting if not explicitly controlled).

*   **Runnable Test Code (SQL Assertion - assuming legacy data is in `temp_dataset.legacy_cibasisprodukt`):**
    ```sql
    -- Create a temporary table for legacy data (replace with actual load method)
    -- CREATE OR REPLACE TABLE `temp_dataset.legacy_cibasisprodukt` (
    --     join_key STRING,
    --     data24_info STRING,
    --     data96_info STRING,
    --     fax_info STRING
    -- );
    -- INSERT INTO `temp_dataset.legacy_cibasisprodukt` VALUES (...); -- Load data from legacy CSV

    SELECT
        COUNT(*) AS total_mismatches
    FROM (
        SELECT * FROM `project.dataset.cibasisprodukt`
        EXCEPT DISTINCT
        SELECT * FROM `temp_dataset.legacy_cibasisprodukt`
    )
    UNION ALL
    (
        SELECT * FROM `temp_dataset.legacy_cibasisprodukt`
        EXCEPT DISTINCT
        SELECT * FROM `project.dataset.cibasisprodukt`
    );
    -- Expected: total_mismatches = 0
    ```