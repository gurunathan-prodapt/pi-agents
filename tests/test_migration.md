As a senior data-migration QA engineer, I've analyzed the provided migration design and generated BigQuery code for `k_ausd_v_ta_barrier_zusgf.ksh` and its invoked SQL script. The tests below are designed to validate the behavioral equivalence of the migrated solution.

**Assumptions:**
*   `your_project_id.your_dataset_id` will be replaced with actual BigQuery project and dataset names, e.g., `my_project.my_dataset`.
*   Legacy output data (from `SOF$TA_BARRIER_ZUSGF` and job control mechanisms) is available for comparison. This is crucial for output parity tests.
*   The `TABLE` table, `DWTK_MELDUNGEN`, `R_BAR`, and `VIA` tables, though mentioned in the design's `READS_TABLE`/`WRITES_TABLE` sections, are not actively used by the provided `p_d_ausd_v_ta_barrier_zusgf` BigQuery stored procedure. The tests will focus on the actual logic implemented in the migrated code, which primarily uses `SOF_TA_BARRIER` as input and `SOF_TA_BARRIER_ZUSGF` as output. If the original SQL script *did* use these other tables for the core logic, this would represent a migration defect outside the scope of testing the *provided* migrated code.

---

## Migration Validation Tests for `k_ausd_v_ta_barrier_zusgf.ksh`

### 1. Orchestration: Parameter Validation - Missing `p_job_kennung`

*   **Purpose:** Verify that the migrated BigQuery stored procedure `p_k_ausd_v_ta_barrier_zusgf` correctly handles missing or empty `p_job_kennung` parameters, mirroring the legacy script's error handling (`pruefeParameterGesetzt Jobkennung p_JobKennung`).
*   **Setup:**
    *   Ensure `job_error_log` table is empty.
*   **Action:**
    *   Execute the migrated orchestration procedure with a `NULL` or empty string for `p_job_kennung`.
    ```sql
    CALL `my_project.my_dataset.p_k_ausd_v_ta_barrier_zusgf`(NULL, '12345');
    -- OR
    CALL `my_project.my_dataset.p_k_ausd_v_ta_barrier_zusgf`('', '12345');
    ```
*   **Pass/Fail Criterion:**
    *   **Pass:** The procedure execution fails with an error message indicating a missing `p_job_kennung`. An entry is recorded in `job_error_log` with `error_code = 193` and a message similar to 'ERROR: p_job_kennung parameter is missing or empty.'.
    *   **Fail:** The procedure executes successfully, or fails with a different error, or no entry is made in `job_error_log`, or the `error_code` is incorrect.
*   **Test Code (SQL Assertion):**
    ```sql
    -- After attempting to call the procedure with invalid parameters
    SELECT
        COUNT(*)
    FROM
        `my_project.my_dataset.job_error_log`
    WHERE
        error_code = 193
        AND error_message LIKE '%p_job_kennung parameter is missing or empty%';
    -- Expected result: 1
    ```

### 2. Orchestration: Parameter Validation - Missing `p_eintrags_nr`

*   **Purpose:** Verify that the migrated BigQuery stored procedure `p_k_ausd_v_ta_barrier_zusgf` correctly handles missing or empty `p_eintrags_nr` parameters, mirroring the legacy script's error handling.
*   **Setup:**
    *   Ensure `job_error_log` table is empty.
*   **Action:**
    *   Execute the migrated orchestration procedure with a `NULL` or empty string for `p_eintrags_nr`.
    ```sql
    CALL `my_project.my_dataset.p_k_ausd_v_ta_barrier_zusgf`('JOB_A', NULL);
    -- OR
    CALL `my_project.my_dataset.p_k_ausd_v_ta_barrier_zusgf`('JOB_A', '');
    ```
*   **Pass/Fail Criterion:**
    *   **Pass:** The procedure execution fails with an error message indicating a missing `p_eintrags_nr`. An entry is recorded in `job_error_log` with `error_code = 193` and a message similar to 'ERROR: p_eintrags_nr parameter is missing or empty.'.
    *   **Fail:** The procedure executes successfully, or fails with a different error, or no entry is made in `job_error_log`, or the `error_code` is incorrect.
*   **Test Code (SQL Assertion):**
    ```sql
    -- After attempting to call the procedure with invalid parameters
    SELECT
        COUNT(*)
    FROM
        `my_project.my_dataset.job_error_log`
    WHERE
        error_code = 193
        AND error_message LIKE '%p_eintrags_nr parameter is missing or empty%';
    -- Expected result: 1
    ```

### 3. Orchestration: Job Control - Ignore Active Job

*   **Purpose:** Verify that the migrated procedure correctly identifies and ignores a new execution if an identical job (`job_kennung`, `eintrags_nr`) is already marked as 'RUNNING', as per the legacy script's behavior ("aktive Jobs werden ignoriert").
*   **Setup:**
    *   Insert a record into `job_control` marking a job as 'RUNNING'.
    ```sql
    INSERT INTO `my_project.my_dataset.job_control` (job_kennung, eintrags_nr, start_time, status)
    VALUES ('JOB_ACTIVE', 'ENTRY_1', CURRENT_TIMESTAMP(), 'RUNNING');
    ```
    *   Ensure `job_error_log` is empty.
*   **Action:**
    *   Execute the migrated orchestration procedure with the same `job_kennung` and `eintrags_nr` as the active job.
    ```sql
    CALL `my_project.my_dataset.p_k_ausd_v_ta_barrier_zusgf`('JOB_ACTIVE', 'ENTRY_1');
    ```
*   **Pass/Fail Criterion:**
    *   **Pass:** The procedure completes without error, and no new `job_control` entry is created for this specific run. The existing 'RUNNING' entry remains unchanged. A 'WARNING' entry is recorded in `job_error_log` indicating the job was ignored.
    *   **Fail:** The procedure attempts to run the data processing logic, or creates a duplicate 'RUNNING' entry, or fails with an error, or does not log the warning.
*   **Test Code (SQL Assertion):**
    ```sql
    -- After calling the procedure
    SELECT
        status
    FROM
        `my_project.my_dataset.job_control`
    WHERE
        job_kennung = 'JOB_ACTIVE' AND eintrags_nr = 'ENTRY_1';
    -- Expected result: 'RUNNING' (only one row)

    SELECT
        COUNT(*)
    FROM
        `my_project.my_dataset.job_error_log`
    WHERE
        job_kennung = 'JOB_ACTIVE'
        AND eintrags_nr = 'ENTRY_1'
        AND severity = 'WARNING'
        AND error_message LIKE '%already active. Ignoring this run%';
    -- Expected result: 1
    ```

### 4. Orchestration: Job Control - Deactivate Older Active Jobs

*   **Purpose:** Verify that the migrated procedure deactivates any previous 'RUNNING' jobs for the same `job_kennung` before starting a new instance, as per the legacy script's behavior ("alte aktive Jobs werden einfach dekativiert").
*   **Setup:**
    *   Insert a record into `job_control` marking an older job for the same `job_kennung` as 'RUNNING'.
    ```sql
    INSERT INTO `my_project.my_dataset.job_control` (job_kennung, eintrags_nr, start_time, status)
    VALUES ('JOB_DEACTIVATE', 'OLD_ENTRY', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR), 'RUNNING');
    ```
*   **Action:**
    *   Execute the migrated orchestration procedure with the same `job_kennung` but a *different* `eintrags_nr`.
    ```sql
    CALL `my_project.my_dataset.p_k_ausd_v_ta_barrier_zusgf`('JOB_DEACTIVATE', 'NEW_ENTRY');
    ```
*   **Pass/Fail Criterion:**
    *   **Pass:** The older `job_control` entry (`OLD_ENTRY`) is updated to `status = 'INACTIVE'` and `error_message = 'Deactivated by new job instance'`. A new `job_control` entry (`NEW_ENTRY`) is created with `status = 'RUNNING'` (and eventually 'SUCCESS' if data processing succeeds).
    *   **Fail:** The older job remains 'RUNNING', or the new job fails to start, or the deactivation message is incorrect.
*   **Test Code (SQL Assertion):**
    ```sql
    -- After calling the procedure
    SELECT
        status, error_message
    FROM
        `my_project.my_dataset.job_control`
    WHERE
        job_kennung = 'JOB_DEACTIVATE' AND eintrags_nr = 'OLD_ENTRY';
    -- Expected result: status = 'INACTIVE', error_message = 'Deactivated by new job instance'

    SELECT
        status
    FROM
        `my_project.my_dataset.job_control`
    WHERE
        job_kennung = 'JOB_DEACTIVATE' AND eintrags_nr = 'NEW_ENTRY';
    -- Expected result: 'SUCCESS' (assuming data processing was successful)
    ```

### 5. Orchestration: Successful Job Execution and Record Count

*   **Purpose:** Verify that a successful run of the migrated orchestration procedure correctly updates the `job_control` table with 'SUCCESS' status, `start_time`, `end_time`, and the correct `record_count`.
*   **Setup:**
    *   Populate `my_project.my_dataset.SOF_TA_BARRIER` with sample data (e.g., 5 rows).
    *   Ensure `job_control` and `job_error_log` are clean for the test `job_kennung`/`eintrags_nr`.
*   **Action:**
    *   Execute the migrated orchestration procedure.
    ```sql
    CALL `my_project.my_dataset.p_k_ausd_v_ta_barrier_zusgf`('JOB_SUCCESS', 'ENTRY_S');
    ```
*   **Pass/Fail Criterion:**
    *   **Pass:** A `job_control` entry for 'JOB_SUCCESS'/'ENTRY_S' exists with `status = 'SUCCESS'`, `start_time` and `end_time` populated, and `record_count` matching the number of rows inserted into `SOF_TA_BARRIER_ZUSGF` (which should be derived from the input `SOF_TA_BARRIER` data). No entries in `job_error_log` for this run.
    *   **Fail:** `job_control` status is not 'SUCCESS', `record_count` is incorrect, or an error is logged.
*   **Test Code (SQL Assertion):**
    ```sql
    -- After calling the procedure
    SELECT
        status, record_count, start_time IS NOT NULL AS start_set, end_time IS NOT NULL AS end_set
    FROM
        `my_project.my_dataset.job_control`
    WHERE
        job_kennung = 'JOB_SUCCESS' AND eintrags_nr = 'ENTRY_S';
    -- Expected result: status = 'SUCCESS', record_count = [number of expected output rows], start_set = TRUE, end_set = TRUE
    ```

### 6. Orchestration: Job Failure Handling

*   **Purpose:** Verify that if the data processing step (`p_d_ausd_v_ta_barrier_zusgf`) fails, the orchestration procedure correctly updates `job_control` with 'FAILED' status and logs the error in `job_error_log`.
*   **Setup:**
    *   Modify `p_d_ausd_v_ta_barrier_zusgf` temporarily to force an error (e.g., attempt to divide by zero, or reference a non-existent column).
    *   Ensure `job_control` and `job_error_log` are clean for the test `job_kennung`/`eintrags_nr`.
*   **Action:**
    *   Execute the migrated orchestration procedure.
    ```sql
    CALL `my_project.my_dataset.p_k_ausd_v_ta_barrier_zusgf`('JOB_FAIL', 'ENTRY_F');
    ```
*   **Pass/Fail Criterion:**
    *   **Pass:** A `job_control` entry for 'JOB_FAIL'/'ENTRY_F' exists with `status = 'FAILED'`, `start_time` and `end_time` populated, and `error_message` containing details of the failure. An entry is also present in `job_error_log` with `severity = 'ERROR'` and the error details.
    *   **Fail:** `job_control` status is not 'FAILED', or `error_message` is empty/incorrect, or no entry in `job_error_log`.
*   **Test Code (SQL Assertion):**
    ```sql
    -- After calling the procedure
    SELECT
        status, error_message, start_time IS NOT NULL AS start_set, end_time IS NOT NULL AS end_set
    FROM
        `my_project.my_dataset.job_control`
    WHERE
        job_kennung = 'JOB_FAIL' AND eintrags_nr = 'ENTRY_F';
    -- Expected result: status = 'FAILED', error_message LIKE '%[expected error message]%', start_set = TRUE, end_set = TRUE

    SELECT
        COUNT(*)
    FROM
        `my_project.my_dataset.job_error_log`
    WHERE
        job_kennung = 'JOB_FAIL'
        AND eintrags_nr = 'ENTRY_F'
        AND severity = 'ERROR'
        AND error_message LIKE '%[expected error message]%';
    -- Expected result: 1
    ```
    *(Remember to revert the temporary modification to `p_d_ausd_v_ta_barrier_zusgf` after this test.)*

### 7. Transformation Correctness: Output Parity (End-to-End)

*   **Purpose:** Verify that for a given set of input data, the final output in `SOF_TA_BARRIER_ZUSGF` produced by the migrated job is identical to the output produced by the legacy job. This is the most critical test for behavioral equivalence.
*   **Setup:**
    *   **Legacy:** Run the original `k_ausd_v_ta_barrier_zusgf.ksh` with a controlled set of input data in `SOF$TA_BARRIER`. Capture the exact output of `SOF$TA_BARRIER_ZUSGF` (e.g., export to CSV or a temporary table).
    *   **Migrated:** Load the *exact same* input data into `my_project.my_dataset.SOF_TA_BARRIER`.
    *   Ensure `SOF_TA_BARRIER_ZUSGF` is empty before execution.
*   **Action:**
    *   Execute the migrated orchestration procedure.
    ```sql
    CALL `my_project.my_dataset.p_k_ausd_v_ta_barrier_zusgf`('JOB_PARITY', 'ENTRY_P');
    ```
*   **Pass/Fail Criterion:**
    *   **Pass:** The data in `my_project.my_dataset.SOF_TA_BARRIER_ZUSGF` is row-for-row and column-for-column identical to the legacy output. This includes row counts, column values, and data types.
    *   **Fail:** Any discrepancy in data, row count, or data types.
*   **Test Code (SQL Assertion - assuming legacy output is in `legacy_output_table`):**
    ```sql
    -- Compare row counts
    SELECT
        (SELECT COUNT(*) FROM `my_project.my_dataset.SOF_TA_BARRIER_ZUSGF`) =
        (SELECT COUNT(*) FROM `my_project.my_dataset.legacy_output_table`);
    -- Expected result: TRUE

    -- Compare all data (assuming column names and types match)
    SELECT
        COUNT(*)
    FROM
        (
            SELECT * FROM `my_project.my_dataset.SOF_TA_BARRIER_ZUSGF`
            EXCEPT DISTINCT
            SELECT * FROM `my_project.my_dataset.legacy_output_table`
        )
    UNION ALL
    SELECT
        COUNT(*)
    FROM
        (
            SELECT * FROM `my_project.my_dataset.legacy_output_table`
            EXCEPT DISTINCT
            SELECT * FROM `my_project.my_dataset.SOF_TA_BARRIER_ZUSGF`
        );
    -- Expected result: 0 (meaning no differences in either direction)
    ```

### 8. Transformation Correctness: Aggregation and String Handling

*   **Purpose:** Verify the correctness of `STRING_AGG`, `SUBSTR`, and `REPLACE` logic for `sperrart_alle`, `sperrgrund_alle`, and `stilllegungszeitraum_alle`.
*   **Setup:**
    *   Populate `my_project.my_dataset.SOF_TA_BARRIER` with data that includes:
        *   Multiple rows for the same `cntrct_id` with different `sperrart` (including 'Rufnummern' and spaces), `sperrgrund`.
        *   `sperrart` values that, after `REPLACE`, would be identical, to test `DISTINCT`.
        *   Aggregated strings that exceed the `SUBSTR` limits (500 for `sperrart_alle`/`sperrgrund_alle`, 100 for `stilllegungszeitraum_alle`).
        *   `NULL` values in `sperrart` or `sperrgrund`.
*   **Action:**
    *   Execute `p_d_ausd_v_ta_barrier_zusgf` directly.
    ```sql
    CALL `my_project.my_dataset.p_d_ausd_v_ta_barrier_zusgf`();
    ```
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   `sperrart_alle` correctly aggregates distinct `sperrart` values (after `REPLACE('Rufnummern','')` and `REPLACE(' ','')`), ordered, and truncated to 500 characters. `NULL` `sperrart` values are ignored in aggregation.
        *   `sperrgrund_alle` correctly aggregates distinct `sperrgrund` values, ordered, and truncated to 500 characters. `NULL` `sperrgrund` values are ignored.
        *   `stilllegungszeitraum_alle` correctly aggregates distinct values, ordered, and truncated to 100 characters.
    *   **Fail:** Any incorrect aggregation, truncation, or `REPLACE` behavior.
*   **Test Code (SQL Assertion - example for `sperrart_alle`):**
    ```sql
    -- Example: Test a specific cntrct_id with complex sperrart values
    -- Input:
    -- cntrct_id | sperrart
    -- 101       | 'A Rufnummern B'
    -- 101       | 'C D'
    -- 101       | 'A Rufnummern B'
    -- 101       | 'E'
    -- Expected sperrart_alle: 'AB,C D,E' (assuming alphabetical order and distinct)

    SELECT
        sperrart_alle
    FROM
        `my_project.my_dataset.SOF_TA_BARRIER_ZUSGF`
    WHERE
        cntrct_id = 101;
    -- Expected result: 'AB,C D,E' (or truncated if longer than 500)

    -- Test truncation (requires input data designed to exceed limit)
    SELECT
        LENGTH(sperrart_alle) <= 500 AS is_truncated_correctly
    FROM
        `my_project.my_dataset.SOF_TA_BARRIER_ZUSGF`
    WHERE
        cntrct_id = [some_id_with_long_sperrart_aggregation];
    -- Expected result: TRUE
    ```

### 9. Transformation Correctness: `stilllegungszeitraum_calc` Logic

*   **Purpose:** Verify the `CASE` logic for `stilllegungszeitraum_calc` based on `ist_stillegung`, `sperr_beginn`, and `sperr_ende`. This mimics Oracle's `DECODE` functionality.
*   **Setup:**
    *   Populate `my_project.my_dataset.SOF_TA_BARRIER` with rows covering all combinations:
        *   `ist_stillegung = 1`, `sperr_ende IS NULL`, `sperr_beginn` set.
        *   `ist_stillegung = 1`, `sperr_ende IS NOT NULL`, `sperr_beginn` set.
        *   `ist_stillegung = 0` (or any other value), `sperr_beginn`/`sperr_ende` set/null.
        *   `sperr_beginn` or `sperr_ende` as `NULL` in relevant cases.
*   **Action:**
    *   Execute `p_d_ausd_v_ta_barrier_zusgf` directly.
    ```sql
    CALL `my_project.my_dataset.p_d_ausd_v_ta_barrier_zusgf`();
    ```
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   If `ist_stillegung = 1` and `sperr_ende IS NULL`, `stilllegungszeitraum_calc` is `CONCAT('ab ', FORMAT_DATE('%d.%m.%Y', DATE(sperr_beginn)))`.
        *   If `ist_stillegung = 1` and `sperr_ende IS NOT NULL`, `stilllegungszeitraum_calc` is `CONCAT(FORMAT_DATE('%d.%m.%Y', DATE(sperr_beginn)), ' - ', FORMAT_DATE('%d.%m.%Y', DATE(sperr_ende)))`.
        *   If `ist_stillegung != 1`, `stilllegungszeitraum_calc` is `NULL`.
    *   **Fail:** Incorrect date formatting, concatenation, or `CASE` logic.
*   **Test Code (SQL Assertion):**
    ```sql
    -- Test Case 1: ist_stillegung = 1, sperr_ende IS NULL
    -- Input: cntrct_id=201, ist_stillegung=1, sperr_beginn='2023-01-15', sperr_ende=NULL
    SELECT
        stilllegungszeitraum_alle
    FROM
        `my_project.my_dataset.SOF_TA_BARRIER_ZUSGF`
    WHERE
        cntrct_id = 201;
    -- Expected result: 'ab 15.01.2023'

    -- Test Case 2: ist_stillegung = 1, sperr_ende IS NOT NULL
    -- Input: cntrct_id=202, ist_stillegung=1, sperr_beginn='2023-02-01', sperr_ende='2023-02-28'
    SELECT
        stilllegungszeitraum_alle
    FROM
        `my_project.my_dataset.SOF_TA_BARRIER_ZUSGF`
    WHERE
        cntrct_id = 202;
    -- Expected result: '01.02.2023 - 28.02.2023'

    -- Test Case 3: ist_stillegung = 0
    -- Input: cntrct_id=203, ist_stillegung=0, sperr_beginn='2023-03-01', sperr_ende='2023-03-31'
    SELECT
        stilllegungszeitraum_alle
    FROM
        `my_project.my_dataset.SOF_TA_BARRIER_ZUSGF`
    WHERE
        cntrct_id = 203;
    -- Expected result: NULL
    ```

### 10. Transformation Correctness: `sperrgrund_zusgf` Logic

*   **Purpose:** Verify the `CASE` logic for `sperrgrund_zusgf_calc` and the subsequent `MAX` aggregation for `sperrgrund_zusgf`. This mimics Oracle's `DECODE` and aggregation.
*   **Setup:**
    *   Populate `my_project.my_dataset.SOF_TA_BARRIER` with rows covering:
        *   `barrier_reason_cv = 2` for a `cntrct_id`.
        *   `barrier_reason_cv = 3` for a `cntrct_id`.
        *   Mixed `barrier_reason_cv` values (e.g., 2 and 3) for the same `cntrct_id`.
        *   Other `barrier_reason_cv` values (e.g., 1, NULL) for a `cntrct_id`.
*   **Action:**
    *   Execute `p_d_ausd_v_ta_barrier_zusgf` directly.
    ```sql
    CALL `my_project.my_dataset.p_d_ausd_v_ta_barrier_zusgf`();
    ```
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   If all `barrier_reason_cv` for a `cntrct_id` are 2, then `sperrgrund_zusgf` is 2.
        *   If any `barrier_reason_cv` for a `cntrct_id` is *not* 2 (e.g., 3, 1, NULL), then `sperrgrund_zusgf` is 3. (This is due to `CASE bar.barrier_reason_cv WHEN 2 THEN 2 ELSE 3 END` and `MAX` aggregation).
    *   **Fail:** Incorrect `sperrgrund_zusgf` value.
*   **Test Code (SQL Assertion):**
    ```sql
    -- Test Case 1: All barrier_reason_cv = 2
    -- Input: cntrct_id=301, barrier_reason_cv=2 (multiple rows)
    SELECT
        sperrgrund_zusgf
    FROM
        `my_project.my_dataset.SOF_TA_BARRIER_ZUSGF`
    WHERE
        cntrct_id = 301;
    -- Expected result: 2

    -- Test Case 2: Mixed barrier_reason_cv (2 and 3)
    -- Input: cntrct_id=302, barrier_reason_cv=2, barrier_reason_cv=3
    SELECT
        sperrgrund_zusgf
    FROM
        `my_project.my_dataset.SOF_TA_BARRIER_ZUSGF`
    WHERE
        cntrct_id = 302;
    -- Expected result: 3 (because MAX(2,3) = 3)

    -- Test Case 3: All barrier_reason_cv != 2 (e.g., 1, NULL)
    -- Input: cntrct_id=303, barrier_reason_cv=1, barrier_reason_cv=NULL
    SELECT
        sperrgrund_zusgf
    FROM
        `my_project.my_dataset.SOF_TA_BARRIER_ZUSGF`
    WHERE
        cntrct_id = 303;
    -- Expected result: 3 (because MAX(3,3) = 3, as 1 and NULL map to 3)
    ```

### 11. Data Quality: Schema and Data Types

*   **Purpose:** Verify that the schema and data types of the target table `SOF_TA_BARRIER_ZUSGF` match the expected design and the legacy table's characteristics.
*   **Setup:**
    *   Ensure `SOF_TA_BARRIER_ZUSGF` exists.
*   **Action:**
    *   Query the information schema for the table.
*   **Pass/Fail Criterion:**
    *   **Pass:** The table `my_project.my_dataset.SOF_TA_BARRIER_ZUSGF` exists and has the following schema:
        *   `cntrct_id` INT64
        *   `sperrart_alle` STRING
        *   `sperrgrund_alle` STRING
        *   `stilllegungszeitraum_alle` STRING
        *   `sperrgrund_zusgf` INT64
    *   **Fail:** Mismatched column names, data types, or missing columns.
*   **Test Code (SQL Assertion):**
    ```sql
    SELECT
        column_name, data_type
    FROM
        `my_project.my_dataset.INFORMATION_SCHEMA.COLUMNS`
    WHERE
        table_name = 'SOF_TA_BARRIER_ZUSGF'
    ORDER BY
        ordinal_position;
    /* Expected Result:
    column_name             data_type
    -----------------------------------
    cntrct_id               INT64
    sperrart_alle           STRING
    sperrgrund_alle         STRING
    stilllegungszeitraum_alle STRING
    sperrgrund_zusgf        INT64
    */
    ```

### 12. Data Quality: NULL Handling in Output

*   **Purpose:** Verify that `NULL` values are handled correctly in the output, especially for aggregated columns where all inputs might be `NULL` or for `CASE` statements resulting in `NULL`.
*   **Setup:**
    *   Populate `my_project.my_dataset.SOF_TA_BARRIER` with:
        *   A `cntrct_id` where all associated `sperrart` values are `NULL`.
        *   A `cntrct_id` where all associated `sperrgrund` values are `NULL`.
        *   A `cntrct_id` where `ist_stillegung = 0` (leading to `NULL` for `stilllegungszeitraum_alle`).
*   **Action:**
    *   Execute `p_d_ausd_v_ta_barrier_zusgf` directly.
    ```sql
    CALL `my_project.my_dataset.p_d_ausd_v_ta_barrier_zusgf`();
    ```
*   **Pass/Fail Criterion:**
    *   **Pass:**
        *   For `cntrct_id` with all `NULL` `sperrart` inputs, `sperrart_alle` is `NULL`.
        *   For `cntrct_id` with all `NULL` `sperrgrund` inputs, `sperrgrund_alle` is `NULL`.
        *   For `cntrct_id` with `ist_stillegung = 0`, `stilllegungszeitraum_alle` is `NULL`.
    *   **Fail:** Empty strings instead of `NULL`, or unexpected values.
*   **Test Code (SQL Assertion):**
    ```sql
    -- Test Case 1: All sperrart are NULL for a cntrct_id
    -- Input: cntrct_id=401, sperrart=NULL (multiple rows)
    SELECT
        sperrart_alle IS NULL AS sperrart_is_null
    FROM
        `my_project.my_dataset.SOF_TA_BARRIER_ZUSGF`
    WHERE
        cntrct_id = 401;
    -- Expected result: TRUE

    -- Test Case 2: All sperrgrund are NULL for a cntrct_id
    -- Input: cntrct_id=402, sperrgrund=NULL (multiple rows)
    SELECT
        sperrgrund_alle IS NULL AS sperrgrund_is_null
    FROM
        `my_project.my_dataset.SOF_TA_BARRIER_ZUSGF`
    WHERE
        cntrct_id = 402;
    -- Expected result: TRUE
    ```