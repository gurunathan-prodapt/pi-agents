The migration of `r_ausd_bp_ta_bcp_iccid.ksh` to a BigQuery Stored Procedure requires comprehensive validation to ensure behavioral equivalence. The tests below cover output parity, transformation correctness, external system replacements (logging), and data quality assertions.

---

## Migration Validation Tests for `r_ausd_bp_ta_bcp_iccid.ksh`

### Test 1: Default Parameter Handling and Successful Execution

*   **Purpose:** Verify that the BigQuery procedure correctly handles cases where no `Stichtag` or `Wiederanlaufwert` is provided, defaulting them as per the legacy script's logic, and executes successfully. This tests parameter defaulting, successful kernel invocation, and logging of a successful run.
*   **Setup:**
    1.  **BigQuery:**
        *   Ensure `project.dataset.dwmsg_log` and `project.dataset.fos_tabelle` are empty.
        *   Populate `project.dataset.vertrag_cache` with diverse test data, including contracts that are valid for `CURRENT_DATE()`, some with `Gueltig_bis IS NULL`, and some with `ladedatum` variations.
            ```sql
            -- Example data for vertrag_cache
            INSERT INTO project.dataset.vertrag_cache (dwh_vertrag_id, gueltig_von, gueltig_bis, ladedatum) VALUES
            (100, '2023-01-01', '2024-12-31', '2023-01-01'), -- Valid for current date
            (101, '2023-01-01', NULL, '2023-01-05'),         -- Valid indefinitely
            (102, '2023-01-01', '2023-01-15', '2023-01-10'), -- Expired before current date
            (103, '2023-10-20', '2023-10-25', '2023-10-20'), -- Valid for a specific current date range
            (104, '2023-01-01', '2024-12-31', '2023-01-02'), -- Multiple ladedatum for same contract
            (104, '2023-01-01', '2024-12-31', '2023-01-05'); -- Max ladedatum for 104
            ```
    2.  **Legacy Environment:**
        *   Ensure `fos_tabelle` (or its equivalent) is empty.
        *   Populate `vertrag_cache` (or its equivalent) with the same data as BigQuery.
*   **Action:**
    1.  **Legacy:** Execute the legacy script without any parameters:
        ```bash
        ./r_ausd_bp_ta_bcp_iccid.ksh
        ```
        Capture the standard output, standard error, the generated log file content, and the final state of the `fos_tabelle` equivalent.
    2.  **BigQuery:** Execute the BigQuery stored procedure without parameters (passing `NULL` for optional string parameters):
        ```sql
        CALL project.dataset.ausd_bp_ta_ibcp_ccid(NULL, NULL);
        ```
*   **Pass/Fail Criteria:**
    *   **Output Parity (Logs):**
        *   The BigQuery `dwmsg_log` table should contain one `RUNNING` entry and one `SUCCESS` entry for the `v_run_id`.
        *   The `parameters` JSON in the `SUCCESS` log entry should show `stichtag` as `CURRENT_DATE()` and `wiederanlaufwert` as `0`.
        *   The `start_time` and `end_time` should be populated, and `error_details` should be `NULL`.
        *   The overall flow and status in `dwmsg_log` should reflect a successful execution, similar to the legacy log file.
    *   **Transformation Correctness (Data):**
        *   The number of rows in `project.dataset.fos_tabelle` should be identical to the number of rows in the legacy `fos_tabelle` equivalent.
        *   The content of `project.dataset.fos_tabelle` (columns `dwh_vertrag_id`, `gueltig_von`, `gueltig_bis`, `ladedatum`, `stichtag_wert`) should exactly match the legacy output, considering `stichtag_wert` will be `CURRENT_DATE()`.
    *   **Return Code:** The BigQuery procedure should complete without raising an unhandled exception (implicitly indicating success).
    ```python
    # Pytest assertion example for data parity
    def test_default_parameters_data_parity(bq_client, legacy_db_conn):
        # ... setup ...
        bq_client.query("CALL project.dataset.ausd_bp_ta_ibcp_ccid(NULL, NULL);").result()
        # ... run legacy script ...

        bq_fos_data = bq_client.query("SELECT dwh_vertrag_id, gueltig_von, gueltig_bis, ladedatum, stichtag_wert FROM project.dataset.fos_tabelle ORDER BY dwh_vertrag_id").to_dataframe()
        legacy_fos_data = pd.read_sql("SELECT dwh_vertrag_id, gueltig_von, gueltig_bis, ladedatum, stichtag_wert FROM legacy_fos_table ORDER BY dwh_vertrag_id", legacy_db_conn)

        pd.testing.assert_frame_equal(bq_fos_data, legacy_fos_data, check_dtype=False)

        # Pytest assertion example for log status
        log_entry = bq_client.query("SELECT status, parameters FROM project.dataset.dwmsg_log WHERE job_name = 'r_ausd_bp_ta_bcp_iccid' AND status = 'SUCCESS'").to_dataframe()
        assert not log_entry.empty
        assert json.loads(log_entry['parameters'].iloc[0])['stichtag'] == str(datetime.date.today())
        assert json.loads(log_entry['parameters'].iloc[0])['wiederanlaufwert'] == 0
    ```

### Test 2: Specific Stichtag and Wiederanlaufwert

*   **Purpose:** Verify that the BigQuery procedure correctly processes explicit `Stichtag` and `Wiederanlaufwert` parameters, including the deletion and insertion logic in the kernel procedure. This tests parameter parsing, data filtering, and the `MAX(ladedatum)` logic.
*   **Setup:**
    1.  **BigQuery:**
        *   Clear `project.dataset.dwmsg_log` and `project.dataset.fos_tabelle`.
        *   Populate `project.dataset.vertrag_cache` with data that will be affected by both `Stichtag` and `Wiederanlaufwert`.
            ```sql
            -- Example data for vertrag_cache
            INSERT INTO project.dataset.vertrag_cache (dwh_vertrag_id, gueltig_von, gueltig_bis, ladedatum) VALUES
            (100, '2023-01-01', '2023-03-31', '2023-01-01'), -- Expired before Stichtag '2023-06-01'
            (101, '2023-05-01', '2023-07-31', '2023-05-01'), -- Valid for Stichtag '2023-06-01'
            (102, '2023-06-15', NULL, '2023-06-15'),         -- Starts after Stichtag '2023-06-01'
            (103, '2023-01-01', '2024-12-31', '2023-01-01'), -- Valid, dwh_vertrag_id < Wiederanlaufwert (e.g., 105)
            (105, '2023-01-01', '2024-12-31', '2023-01-01'), -- Valid, dwh_vertrag_id >= Wiederanlaufwert
            (105, '2023-01-01', '2024-12-31', '2023-05-15'), -- Max ladedatum for 105, before Stichtag
            (106, '2023-01-01', '2024-12-31', '2023-01-01'), -- Valid, dwh_vertrag_id >= Wiederanlaufwert
            (106, '2023-01-01', '2024-12-31', '2023-06-05'); -- Max ladedatum for 106, after Stichtag
            ```
    2.  **Legacy Environment:**
        *   Clear `fos_tabelle` equivalent.
        *   Populate `vertrag_cache` equivalent with the same data.
*   **Action:**
    1.  **Legacy:** Execute the legacy script with specific parameters:
        ```bash
        ./r_ausd_bp_ta_bcp_iccid.ksh -s 01062023 -l 105
        ```
        Capture output, log, and final `fos_tabelle` equivalent.
    2.  **BigQuery:** Execute the BigQuery stored procedure with equivalent parameters:
        ```sql
        CALL project.dataset.ausd_bp_ta_ibcp_ccid('2023-06-01', '105');
        ```
*   **Pass/Fail Criteria:**
    *   **Output Parity (Logs):**
        *   `dwmsg_log` should show `stichtag` as `2023-06-01` and `wiederanlaufwert` as `105` in the `SUCCESS` log entry's `parameters` JSON.
        *   The overall logging flow should match the legacy script.
    *   **Transformation Correctness (Data):**
        *   The number of rows and content of `project.dataset.fos_tabelle` should exactly match the legacy output.
        *   Specifically, `dwh_vertrag_id` 100, 102, 103 should not be present. `dwh_vertrag_id` 101 should be present. `dwh_vertrag_id` 105 should be present with `ladedatum` '2023-05-15'. `dwh_vertrag_id` 106 should *not* be present because its `MAX(ladedatum)` is after the `stichtag`.
    *   **Return Code:** BigQuery procedure completes successfully.

### Test 3: Error Handling - Invalid Stichtag Format

*   **Purpose:** Verify that the BigQuery procedure correctly handles invalid `Stichtag` input, logs the error, and raises an exception, mirroring the legacy script's error behavior.
*   **Setup:**
    1.  **BigQuery:** Clear `project.dataset.dwmsg_log`. `vertrag_cache` and `fos_tabelle` can be in any state.
    2.  **Legacy Environment:** Ensure helper scripts for error handling (`f_alis_msgerr.ksh`) are functional.
*   **Action:**
    1.  **Legacy:** Execute the legacy script with an invalid `Stichtag` format:
        ```bash
        ./r_ausd_bp_ta_bcp_iccid.ksh -s 2023/10/26
        ```
        Capture standard output, standard error, and the log file content. Note the exit code.
    2.  **BigQuery:** Execute the BigQuery stored procedure with an invalid `Stichtag` format:
        ```sql
        CALL project.dataset.ausd_bp_ta_ibcp_ccid('2023/10/26', NULL);
        ```
*   **Pass/Fail Criteria:**
    *   **Output Parity (Logs):**
        *   The BigQuery `dwmsg_log` table should contain one `RUNNING` entry and one `FAILED` entry for the `v_run_id`.
        *   The `error_details` field in the `FAILED` log entry should contain a message indicating a date parsing error (e.g., "Failed to parse date string '2023/10/26'").
        *   The legacy log file should show an error message related to date parsing or parameter validation, and the script should exit with a non-zero status.
    *   **Error Handling:** The BigQuery `CALL` statement should result in an error being raised, preventing further execution of the procedure and propagating the error to the caller.
    ```python
    # Pytest assertion example for error handling
    import pytest
    def test_invalid_stichtag_error_handling(bq_client):
        # ... setup ...
        with pytest.raises(Exception) as excinfo:
            bq_client.query("CALL project.dataset.ausd_bp_ta_ibcp_ccid('2023/10/26', NULL);").result()
        assert "Failed to parse date string" in str(excinfo.value)

        log_entry = bq_client.query("SELECT status, error_details FROM project.dataset.dwmsg_log WHERE job_name = 'r_ausd_bp_ta_bcp_iccid' AND status = 'FAILED'").to_dataframe()
        assert not log_entry.empty
        assert "Failed to parse date string" in log_entry['error_details'].iloc[0]
    ```

### Test 4: Error Handling - Invalid Wiederanlaufwert Format

*   **Purpose:** Verify that the BigQuery procedure correctly handles invalid `Wiederanlaufwert` input, logs the error, and raises an exception.
*   **Setup:**
    1.  **BigQuery:** Clear `project.dataset.dwmsg_log`.
    2.  **Legacy Environment:** Ensure helper scripts for error handling are functional.
*   **Action:**
    1.  **Legacy:** Execute the legacy script with an invalid `Wiederanlaufwert` (e.g., non-numeric):
        ```bash
        ./r_ausd_bp_ta_bcp_iccid.ksh -l ABC
        ```
        Capture output, error, and log file. Note the exit code.
    2.  **BigQuery:** Execute the BigQuery stored procedure with an invalid `Wiederanlaufwert`:
        ```sql
        CALL project.dataset.ausd_bp_ta_ibcp_ccid(NULL, 'ABC');
        ```
*   **Pass/Fail Criteria:**
    *   **Output Parity (Logs):**
        *   `dwmsg_log` should contain `RUNNING` and `FAILED` entries.
        *   `error_details` in the `FAILED` entry should indicate a casting error (e.g., "Bad int64 value: 'ABC'").
        *   Legacy log should show an error related to numeric conversion or parameter validation, and the script should exit with a non-zero status.
    *   **Error Handling:** The BigQuery `CALL` statement should raise an error.

### Test 5: Kernel Procedure Failure and Wrapper Error Logging

*   **Purpose:** Verify that if the invoked kernel procedure (`k_ausd_bp_ta_bcp_iccid`) fails, the wrapper procedure (`ausd_bp_ta_ibcp_ccid`) correctly catches the error, logs it, and propagates the failure.
*   **Setup:**
    1.  **BigQuery:**
        *   Clear `project.dataset.dwmsg_log`.
        *   Modify `project.dataset.k_ausd_bp_ta_bcp_iccid` temporarily to force an error (e.g., attempt to insert into a non-existent column or divide by zero).
            ```sql
            -- Temporarily modify k_ausd_bp_ta_bcp_iccid to force an error
            CREATE OR REPLACE PROCEDURE project.dataset.k_ausd_bp_ta_bcp_iccid(
                p_stichtag DATE,
                p_wiederanlaufwert INT64
            )
            BEGIN
                -- Force an error, e.g., divide by zero
                SELECT 1 / 0;
            END;
            ```
    2.  **Legacy Environment:**
        *   Temporarily modify `k_ausd_bp_ta_bcp_iccid.ksh` to force an error (e.g., `exit 1` or an invalid SQL command).
*   **Action:**
    1.  **Legacy:** Execute the legacy wrapper script (which will invoke the modified kernel script):
        ```bash
        ./r_ausd_bp_ta_bcp_iccid.ksh
        ```
        Capture output, error, and log file. Note the exit code.
    2.  **BigQuery:** Execute the BigQuery wrapper procedure:
        ```sql
        CALL project.dataset.ausd_bp_ta_ibcp_ccid(NULL, NULL);
        ```
*   **Pass/Fail Criteria:**
    *   **Output Parity (Logs):**
        *   `dwmsg_log` should contain `RUNNING` and `FAILED` entries.
        *   `error_details` in the `FAILED` entry should reflect the error from the kernel procedure (e.g., "division by zero" or a generic BigQuery error message).
        *   The legacy log file should indicate the kernel script's failure and the wrapper's subsequent error handling, with a non-zero exit code.
    *   **Error Handling:** The BigQuery `CALL` statement should raise an error, indicating the job failed.
    *   **Cleanup:** Revert the temporary changes to `k_ausd_bp_ta_bcp_iccid` after this test.

### Test 6: Data Quality - Row Counts and Schema Assertions

*   **Purpose:** Verify the schema of the logging tables and ensure row counts are as expected after various runs.
*   **Setup:**
    1.  **BigQuery:**
        *   Run a series of successful and failed `ausd_bp_ta_ibcp_ccid` calls (e.g., from previous tests).
        *   Ensure `project.dataset.dwmsg_log` and `project.dataset.dwmsg_job_sequence` tables exist.
*   **Action:**
    1.  Query the schema of `dwmsg_log` and `dwmsg_job_sequence`.
    2.  Query row counts from `dwmsg_log`.
*   **Pass/Fail Criteria:**
    *   **Schema:**
        *   `project.dataset.dwmsg_log` should have columns: `log_id` (STRING), `job_name` (STRING), `run_id` (STRING), `start_time` (TIMESTAMP), `end_time` (TIMESTAMP), `status` (STRING), `message` (STRING), `parameters` (JSON), `error_details` (STRING), `log_timestamp` (TIMESTAMP).
        *   `project.dataset.dwmsg_job_sequence` should have columns: `job_name` (STRING), `current_sequence_value` (INT64), `last_updated` (TIMESTAMP).
    *   **Row Counts:**
        *   The number of `RUNNING` entries in `dwmsg_log` should match the number of times the procedure was invoked.
        *   The number of `SUCCESS` or `FAILED` entries should also match the number of invocations. (Each invocation creates one `RUNNING` and one final status entry, potentially updating the same `log_id`).
        *   The `dwmsg_job_sequence` table should contain an entry for `job_name = 'r_ausd_bp_ta_bcp_iccid'` if it were used (currently it's not, so this table's usage needs re-evaluation or this check adapted).
    ```sql
    -- SQL Assertion for dwmsg_log schema
    SELECT
        column_name,
        data_type
    FROM
        `project.dataset.INFORMATION_SCHEMA.COLUMNS`
    WHERE
        table_name = 'dwmsg_log'
    ORDER BY
        ordinal_position;

    -- Expected output (example):
    -- column_name          data_type
    -- log_id               STRING
    -- job_name             STRING
    -- run_id               STRING
    -- start_time           TIMESTAMP
    -- end_time             TIMESTAMP
    -- status               STRING
    -- message              STRING
    -- parameters           JSON
    -- error_details        STRING
    -- log_timestamp        TIMESTAMP

    -- SQL Assertion for row counts
    SELECT
        status,
        COUNT(*) AS count
    FROM
        project.dataset.dwmsg_log
    WHERE
        job_name = 'r_ausd_bp_ta_bcp_iccid'
    GROUP BY
        status;
    ```

### Test 7: Null Handling in `vertrag_cache` for `Gueltig_bis`

*   **Purpose:** Verify that the kernel procedure correctly handles `Gueltig_bis IS NULL` in `vertrag_cache` records, treating them as valid indefinitely.
*   **Setup:**
    1.  **BigQuery:**
        *   Clear `project.dataset.dwmsg_log` and `project.dataset.fos_tabelle`.
        *   Populate `project.dataset.vertrag_cache` with records where `Gueltig_bis` is `NULL`.
            ```sql
            INSERT INTO project.dataset.vertrag_cache (dwh_vertrag_id, gueltig_von, gueltig_bis, ladedatum) VALUES
            (200, '2020-01-01', NULL, '2020-01-01'), -- Valid indefinitely
            (201, '2023-01-01', '2023-01-15', '2023-01-01'); -- Not valid for future stichtag
            ```
    2.  **Legacy Environment:** Same data setup.
*   **Action:**
    1.  **Legacy:** Execute with a `Stichtag` far in the future:
        ```bash
        ./r_ausd_bp_ta_bcp_iccid.ksh -s 01012050
        ```
    2.  **BigQuery:** Execute with a `Stichtag` far in the future:
        ```sql
        CALL project.dataset.ausd_bp_ta_ibcp_ccid('2050-01-01', NULL);
        ```
*   **Pass/Fail Criteria:**
    *   **Transformation Correctness (Data):**
        *   `project.dataset.fos_tabelle` should contain `dwh_vertrag_id = 200` but not `201`.
        *   The `stichtag_wert` for `200` should be `2050-01-01`.
        *   Data in `fos_tabelle` should match legacy output.

### Test 8: `LADEDATUM` Logic with Multiple Versions

*   **Purpose:** Verify that the kernel procedure correctly applies the `MAX(ladedatum)` logic for a given `dwh_vertrag_id` and `stichtag`, ensuring only the latest relevant version is selected.
*   **Setup:**
    1.  **BigQuery:**
        *   Clear `project.dataset.dwmsg_log` and `project.dataset.fos_tabelle`.
        *   Populate `project.dataset.vertrag_cache` with multiple `ladedatum` entries for the same `dwh_vertrag_id`.
            ```sql
            INSERT INTO project.dataset.vertrag_cache (dwh_vertrag_id, gueltig_von, gueltig_bis, ladedatum) VALUES
            (300, '2023-01-01', '2024-12-31', '2023-01-01'), -- Old version
            (300, '2023-01-01', '2024-12-31', '2023-03-01'), -- Newer version
            (300, '2023-01-01', '2024-12-31', '2023-05-01'), -- Latest version
            (301, '2023-01-01', '2024-12-31', '2023-02-01'), -- Only one version
            (302, '2023-01-01', '2024-12-31', '2023-04-01'); -- Latest version after stichtag
            ```
    2.  **Legacy Environment:** Same data setup.
*   **Action:**
    1.  **Legacy:** Execute with a `Stichtag` that falls between `ladedatum` entries:
        ```bash
        ./r_ausd_bp_ta_bcp_iccid.ksh -s 01042023
        ```
    2.  **BigQuery:** Execute with the same `Stichtag`:
        ```sql
        CALL project.dataset.ausd_bp_ta_ibcp_ccid('2023-04-01', NULL);
        ```
*   **Pass/Fail Criteria:**
    *   **Transformation Correctness (Data):**
        *   `project.dataset.fos_tabelle` should contain `dwh_vertrag_id = 300` with `ladedatum = '2023-03-01'` (as `2023-05-01` is after `stichtag`).
        *   `dwh_vertrag_id = 301` should be present with `ladedatum = '2023-02-01'`.
        *   `dwh_vertrag_id = 302` should *not* be present because its `ladedatum` is after the `stichtag`.
        *   Data in `fos_tabelle` should match legacy output.

---