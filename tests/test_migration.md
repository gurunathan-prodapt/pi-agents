The migration of `k_ausd_bp_ta_rn_vertrag.ksh` to a BigQuery stored procedure `project.dataset.r_ausd_bp_ta_rn_vertrag` involves significant re-interpretation of parameters and replacement of shell utilities with BigQuery scripting. The core SQL logic (`d_ausd_bp_ta_rn_vertrag.sql`) is not provided, so tests for its specific transformations are placeholders.

The tests below focus on the orchestration logic, parameter validation, and external system replacements as defined in the BigQuery stored procedure, while also highlighting discrepancies in parameter interpretation compared to the original KornShell script.

**Assumptions for Testing:**
1.  The BigQuery project and dataset (`project.dataset`) exist.
2.  The `project.dataset.job_error_log` and `project.dataset.job_audit_log` tables are created as specified in the BigQuery stored procedure code.
    ```sql
    CREATE TABLE IF NOT EXISTS project.dataset.job_error_log (
        job_id STRING,
        log_timestamp TIMESTAMP,
        error_message STRING
    );
    CREATE TABLE IF NOT EXISTS project.dataset.job_audit_log (
        job_id STRING,
        log_timestamp TIMESTAMP,
        event_type STRING,
        details STRING
    );
    ```
3.  The `project.dataset.r_ausd_bp_ta_rn_vertrag` stored procedure has been deployed.
4.  The core SQL logic from `d_ausd_bp_ta_rn_vertrag.sql` is currently a placeholder in the BigQuery SP, returning `-1` for `v_processed_records`. Tests related to actual data transformation will be marked as dependent on the full migration of `d_ausd_bp_ta_rn_vertrag.sql`.

---

### Test Case 1: Successful Execution with Valid Parameters

*   **Purpose**: Verify that the BigQuery stored procedure executes successfully with valid input parameters, logs the start and success events, and captures a (placeholder) record count. This tests the basic orchestration flow and parameter handling.
*   **Setup**:
    *   Ensure `job_audit_log` and `job_error_log` tables are empty before execution.
*   **Action**:
    *   Call the BigQuery stored procedure with valid parameters.
    ```sql
    CALL project.dataset.r_ausd_bp_ta_rn_vertrag(
        p_job_id => 'test_job_id_001',
        p_date_today_str => '20231027',
        p_date_yesterday_str => '20231026',
        p_mandant => 'MANDANT_A'
    );
    ```
*   **Expected Result / Pass/Fail Criterion**:
    *   The procedure completes without raising an error.
    *   Two entries are found in `project.dataset.job_audit_log` for `job_id = 'test_job_id_001'`:
        *   One with `event_type = 'START'`.
        *   One with `event_type = 'SUCCESS'` and `details` containing "Processed records: -1" (due to the current placeholder).
    *   No entries are found in `project.dataset.job_error_log` for `job_id = 'test_job_id_001'`.

    ```sql
    -- Pass/Fail Criterion SQL
    SELECT
        (SELECT COUNT(*) FROM project.dataset.job_audit_log WHERE job_id = 'test_job_id_001' AND event_type = 'START') = 1 AS start_log_exists,
        (SELECT COUNT(*) FROM project.dataset.job_audit_log WHERE job_id = 'test_job_id_001' AND event_type = 'SUCCESS' AND details LIKE '%Processed records: -1%') = 1 AS success_log_exists,
        (SELECT COUNT(*) FROM project.dataset.job_error_log WHERE job_id = 'test_job_id_001') = 0 AS no_error_logs;
    ```
    *   **Pass**: All three boolean results are `TRUE`.

---

### Test Case 2: Missing Mandatory Parameter (`p_date_today_str`)

*   **Purpose**: Verify that the procedure correctly identifies and handles a missing mandatory parameter (`-f` in KSH, `p_date_today_str` in BQ SP), logs an error, and terminates. This tests the `pruefeParameterGesetzt` equivalent logic.
*   **Setup**:
    *   Ensure `job_audit_log` and `job_error_log` tables are empty before execution.
*   **Action**:
    *   Call the BigQuery stored procedure with `p_date_today_str` as `NULL` or empty string.
    ```sql
    -- Using NULL
    CALL project.dataset.r_ausd_bp_ta_rn_vertrag(
        p_job_id => 'test_job_id_002',
        p_date_today_str => NULL,
        p_date_yesterday_str => '20231026',
        p_mandant => 'MANDANT_A'
    );

    -- Or using empty string
    -- CALL project.dataset.r_ausd_bp_ta_rn_vertrag(
    --     p_job_id => 'test_job_id_002',
    --     p_date_today_str => '',
    --     p_date_yesterday_str => '20231026',
    --     p_mandant => 'MANDANT_A'
    -- );
    ```
*   **Expected Result / Pass/Fail Criterion**:
    *   The procedure raises an error (e.g., `SQLSTATE '45000'`) with a message indicating the missing parameter.
    *   One entry is found in `project.dataset.job_error_log` for `job_id = 'test_job_id_002'` with `error_message` containing "Parameter -f (date_today) is mandatory."
    *   One entry is found in `project.dataset.job_audit_log` for `job_id = 'test_job_id_002'` with `event_type = 'START'`. No `SUCCESS` log.

    ```sql
    -- Pass/Fail Criterion SQL (run after attempting the CALL)
    SELECT
        (SELECT COUNT(*) FROM project.dataset.job_audit_log WHERE job_id = 'test_job_id_002' AND event_type = 'START') = 1 AS start_log_exists,
        (SELECT COUNT(*) FROM project.dataset.job_audit_log WHERE job_id = 'test_job_id_002' AND event_type = 'SUCCESS') = 0 AS no_success_log,
        (SELECT COUNT(*) FROM project.dataset.job_error_log WHERE job_id = 'test_job_id_002' AND error_message LIKE '%Parameter -f (date_today) is mandatory.%') = 1 AS correct_error_log_exists;
    ```
    *   **Pass**: All three boolean results are `TRUE`, and the `CALL` statement failed as expected.

---

### Test Case 3: Invalid Date Format (`p_date_today_str`)

*   **Purpose**: Verify that the procedure correctly validates date formats (equivalent to `DWDate_Datum_Check` in KSH), logs an error, and terminates.
*   **Setup**:
    *   Ensure `job_audit_log` and `job_error_log` tables are empty before execution.
*   **Action**:
    *   Call the BigQuery stored procedure with an invalid date format for `p_date_today_str`.
    ```sql
    CALL project.dataset.r_ausd_bp_ta_rn_vertrag(
        p_job_id => 'test_job_id_003',
        p_date_today_str => '2023-10-27', -- Expected YYYYMMDD
        p_date_yesterday_str => '20231026',
        p_mandant => 'MANDANT_A'
    );
    ```
*   **Expected Result / Pass/Fail Criterion**:
    *   The procedure raises an error (e.g., `SQLSTATE '45000'`) with a message indicating the invalid date format.
    *   One entry is found in `project.dataset.job_error_log` for `job_id = 'test_job_id_003'` with `error_message` containing "Invalid date format for -f (date_today). Expected YYYYMMDD."
    *   One entry is found in `project.dataset.job_audit_log` for `job_id = 'test_job_id_003'` with `event_type = 'START'`. No `SUCCESS` log.

    ```sql
    -- Pass/Fail Criterion SQL (run after attempting the CALL)
    SELECT
        (SELECT COUNT(*) FROM project.dataset.job_audit_log WHERE job_id = 'test_job_id_003' AND event_type = 'START') = 1 AS start_log_exists,
        (SELECT COUNT(*) FROM project.dataset.job_audit_log WHERE job_id = 'test_job_id_003' AND event_type = 'SUCCESS') = 0 AS no_success_log,
        (SELECT COUNT(*) FROM project.dataset.job_error_log WHERE job_id = 'test_job_id_003' AND error_message LIKE '%Invalid date format for -f (date_today). Expected YYYYMMDD.%') = 1 AS correct_error_log_exists;
    ```
    *   **Pass**: All three boolean results are `TRUE`, and the `CALL` statement failed as expected.

---

### Test Case 4: Logical Date Error (`p_date_yesterday_str` not (p_date_today_str - 1 day))

*   **Purpose**: Verify that the procedure correctly checks the logical relationship between `p_date_today_str` and `p_date_yesterday_str` (replacing `gestern.ksh` logic for validation), logs an error, and terminates.
*   **Setup**:
    *   Ensure `job_audit_log` and `job_error_log` tables are empty before execution.
*   **Action**:
    *   Call the BigQuery stored procedure with `p_date_yesterday_str` not being the day before `p_date_today_str`.
    ```sql
    CALL project.dataset.r_ausd_bp_ta_rn_vertrag(
        p_job_id => 'test_job_id_004',
        p_date_today_str => '20231027',
        p_date_yesterday_str => '20231025', -- Should be 20231026
        p_mandant => 'MANDANT_A'
    );
    ```
*   **Expected Result / Pass/Fail Criterion**:
    *   The procedure raises an error (e.g., `SQLSTATE '45000'`) with a message indicating the logical date error.
    *   One entry is found in `project.dataset.job_error_log` for `job_id = 'test_job_id_004'` with `error_message` containing "Logical error: -s (date_yesterday) is not one day before -f (date_today)."
    *   One entry is found in `project.dataset.job_audit_log` for `job_id = 'test_job_id_004'` with `event_type = 'START'`. No `SUCCESS` log.

    ```sql
    -- Pass/Fail Criterion SQL (run after attempting the CALL)
    SELECT
        (SELECT COUNT(*) FROM project.dataset.job_audit_log WHERE job_id = 'test_job_id_004' AND event_type = 'START') = 1 AS start_log_exists,
        (SELECT COUNT(*) FROM project.dataset.job_audit_log WHERE job_id = 'test_job_id_004' AND event_type = 'SUCCESS') = 0 AS no_success_log,
        (SELECT COUNT(*) FROM project.dataset.job_error_log WHERE job_id = 'test_job_id_004' AND error_message LIKE '%Logical error: -s (date_yesterday) is not one day before -f (date_today).%') = 1 AS correct_error_log_exists;
    ```
    *   **Pass**: All three boolean results are `TRUE`, and the `CALL` statement failed as expected.

---

### Test Case 5: External System Replacement - Date Derivation (`gestern.ksh`)

*   **Purpose**: Verify that the BigQuery stored procedure correctly replaces the functionality of `gestern.ksh` by using BigQuery's native date functions for validation. While `gestern.ksh` *derives* dates, the BQ SP *validates* the relationship of input dates. This test ensures the validation logic is sound.
*   **Setup**:
    *   This test is covered by Test Case 4. No additional setup.
*   **Action**:
    *   As in Test Case 4, call the BigQuery stored procedure with `p_date_yesterday_str` not being the day before `p_date_today_str`.
*   **Expected Result / Pass/Fail Criterion**:
    *   The procedure should fail with the specific logical date error, demonstrating that BigQuery's `DATE_SUB()` function (or similar logic) correctly identified the mismatch.

    ```sql
    -- Pass/Fail Criterion SQL (same as Test Case 4)
    SELECT
        (SELECT COUNT(*) FROM project.dataset.job_audit_log WHERE job_id = 'test_job_id_004' AND event_type = 'START') = 1 AS start_log_exists,
        (SELECT COUNT(*) FROM project.dataset.job_audit_log WHERE job_id = 'test_job_id_004' AND event_type = 'SUCCESS') = 0 AS no_success_log,
        (SELECT COUNT(*) FROM project.dataset.job_error_log WHERE job_id = 'test_job_id_004' AND error_message LIKE '%Logical error: -s (date_yesterday) is not one day before -f (date_today).%') = 1 AS correct_error_log_exists;
    ```
    *   **Pass**: All three boolean results are `TRUE`, and the `CALL` statement failed as expected.

---

### Test Case 6: External System Replacement - Record Count Capture (Temporary File)

*   **Purpose**: Verify that the BigQuery stored procedure correctly replaces the temporary file mechanism (`$DW_DIR_UTL/bert_k_ausd_bp_ta_rn_vertrag.tmp`) for capturing record counts with an internal variable and logs it to the audit table.
*   **Setup**:
    *   This test is covered by Test Case 1. No additional setup.
*   **Action**:
    *   As in Test Case 1, call the BigQuery stored procedure with valid parameters.
*   **Expected Result / Pass/Fail Criterion**:
    *   The `SUCCESS` entry in `project.dataset.job_audit_log` should contain the `v_processed_records` value. Currently, this is hardcoded to `-1` in the BQ SP placeholder.

    ```sql
    -- Pass/Fail Criterion SQL (part of Test Case 1)
    SELECT
        (SELECT COUNT(*) FROM project.dataset.job_audit_log WHERE job_id = 'test_job_id_001' AND event_type = 'SUCCESS' AND details LIKE '%Processed records: -1%') = 1 AS success_log_with_record_count_exists;
    ```
    *   **Pass**: The boolean result is `TRUE`.
    *   **Note**: Once `d_ausd_bp_ta_rn_vertrag.sql` is fully migrated and returns an actual record count, this test should be updated to assert the *correct* count.

---

### Test Case 7: Transformation Correctness - Core SQL Logic (Placeholder)

*   **Purpose**: Acknowledge and plan for testing the core data transformation logic from `d_ausd_bp_ta_rn_vertrag.sql` once it's migrated to BigQuery. This test cannot be fully implemented without the source SQL.
*   **Setup**:
    *   **Future**: Create mock source tables in BigQuery that mimic the Oracle source data for `d_ausd_bp_ta_rn_vertrag.sql`.
    *   **Future**: Define expected output data in BigQuery target tables based on the original Oracle SQL's behavior.
*   **Action**:
    *   **Future**: Call the BigQuery stored procedure with parameters that trigger the core SQL logic.
    ```sql
    -- Future Action
    CALL project.dataset.r_ausd_bp_ta_rn_vertrag(
        p_job_id => 'test_job_id_core_sql',
        p_date_today_str => '20231027',
        p_date_yesterday_str => '20231026',
        p_mandant => 'MANDANT_A'
    );
    ```
*   **Expected Result / Pass/Fail Criterion**:
    *   **Future**: The data in the BigQuery target tables (populated by the migrated `d_ausd_bp_ta_rn_vertrag.sql` logic) should be identical to the data produced by the original Oracle `d_ausd_bp_ta_rn_vertrag.sql` script when run against the same source data.
    *   **Future**: The `v_processed_records` value logged in `job_audit_log` should match the actual number of rows processed/inserted/updated by the core SQL logic.

    ```sql
    -- Future Pass/Fail Criterion SQL (example)
    -- Compare target table content
    SELECT
        (SELECT COUNT(*) FROM project.dataset.your_target_table_bq WHERE processing_date = '2023-10-27' AND mandant_id = 'MANDANT_A') = (SELECT COUNT(*) FROM legacy_oracle_target_table_snapshot) AS row_count_parity,
        (SELECT TO_JSON_STRING(ARRAY_AGG(t ORDER BY primary_key))) = (SELECT TO_JSON_STRING(ARRAY_AGG(l ORDER BY primary_key))) FROM project.dataset.your_target_table_bq t JOIN legacy_oracle_target_table_snapshot l ON t.primary_key = l.primary_key WHERE t.processing_date = '2023-10-27' AND t.mandant_id = 'MANDANT_A' AS data_parity;

    -- Verify processed records count
    SELECT
        (SELECT CAST(REGEXP_EXTRACT(details, r'Processed records: (\d+)') AS INT64) FROM project.dataset.job_audit_log WHERE job_id = 'test_job_id_core_sql' AND event_type = 'SUCCESS') = (SELECT COUNT(*) FROM project.dataset.your_target_table_bq WHERE processing_date = '2023-10-27' AND mandant_id = 'MANDANT_A') AS record_count_match;
    ```
    *   **Pass**: All future assertions are `TRUE`.

---

### Test Case 8: Parameter Re-interpretation Discrepancy

*   **Purpose**: Highlight and document the behavioral change in parameter interpretation between the legacy KornShell script and the BigQuery stored procedure. The KSH script's `-f` and `-l` parameters (`p_EintragsNr`, `p_wiederanlaufWert`) are re-interpreted as `p_date_today_str` and `p_mandant` in the BQ SP, respectively.
*   **Setup**:
    *   No specific setup beyond having both the KSH script and BQ SP available for comparison.
*   **Action**:
    *   Compare the `getopts` section of `k_ausd_bp_ta_rn_vertrag.ksh` with the parameter definitions and validation logic in `project.dataset.r_ausd_bp_ta_rn_vertrag`.
*   **Expected Result / Pass/Fail Criterion**:
    *   **Observation**:
        *   KSH `-f` maps to `p_EintragsNr` (Entry Number). BQ SP `-f` maps to `p_date_today_str` (Date Today).
        *   KSH `-l` maps to `p_wiederanlaufWert` (Restart Value). BQ SP `-l` maps to `p_mandant` (Client/Mandator).
        *   KSH `-s` maps to `p_Stichtag` (Key Date), which is validated as a date. BQ SP `-s` maps to `p_date_yesterday_str` (Date Yesterday), which is also validated as a date. This mapping is plausible.
    *   **Pass**: The discrepancy is clearly identified and documented as a deliberate re-design or an oversight in the migration. If this re-design is intentional and approved, the BQ SP's behavior is correct according to its new design. If it's an oversight, it's a **Fail** and requires correction.
    *   **Recommendation**: Clarify with the design team if this parameter re-interpretation is intentional. If so, update the design document to reflect this change explicitly. If not, adjust the BQ SP parameters to align with the original KSH script's intent or provide a clear mapping.

---

These tests cover the orchestration, validation, and logging aspects of the migrated BigQuery stored procedure, acknowledging the current placeholder for the core SQL logic and the re-interpretation of parameters. Full behavioral equivalence for data transformation will require the complete migration of `d_ausd_bp_ta_rn_vertrag.sql`.