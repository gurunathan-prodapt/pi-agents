As a senior data-migration QA engineer, I've analyzed the migration design for `r_ausd_v_ta_inv_assign.ksh` to Google BigQuery. The migration focuses on translating the wrapper script's orchestration, parameter handling, and logging to BigQuery stored procedures and tables. The core business logic in `k_ausd_v_ta_inv_assign.ksh` is assumed to be migrated separately.

The following test cases are designed to validate the behavioral equivalence of the migrated BigQuery wrapper procedure (`vertragsdatenabgleich_wrapper`) and its associated logging tables, covering output parity, transformation correctness, external system replacements (specifically logging), and data quality.

**Assumptions:**
*   BigQuery project and dataset (`your_gcp_project_id.your_bq_dataset_name`) are configured.
*   The DDLs for `dw_job_entries`, `dw_job_audit`, and `dw_error_log` have been executed.
*   The stored procedures `k_ausd_v_ta_inv_assign` (placeholder) and `vertragsdatenabgleich_wrapper` have been deployed.
*   For tests involving `k_ausd_v_ta_inv_assign`'s behavior (success/failure), its implementation will be temporarily modified as specified in the test setup.

---

## Test Case 1.1: Help Parameter Handling

*   **Purpose:** Verify that calling the BigQuery wrapper procedure with the help parameter (`p_h => TRUE`) displays usage information and exits gracefully without executing core logic or logging job entries, mirroring the legacy script's `-h` behavior.
*   **Setup:**
    1.  Ensure the logging tables are empty:
        ```sql
        TRUNCATE TABLE `your_gcp_project_id.your_bq_dataset_name.dw_job_entries`;
        TRUNCATE TABLE `your_gcp_project_id.your_bq_dataset_name.dw_job_audit`;
        TRUNCATE TABLE `your_gcp_project_id.your_bq_dataset_name.dw_error_log`;
        ```
*   **Action:** Execute the BigQuery wrapper procedure with the help flag.
    ```sql
    CALL `your_gcp_project_id.your_bq_dataset_name.vertragsdatenabgleich_wrapper`(p_h => TRUE, p_s => NULL, p_l => NULL);
    ```
*   **Pass/Fail Criterion:**
    1.  The `CALL` statement completes successfully (no error is raised by BigQuery).
    2.  No new rows are inserted into `dw_job_entries`, `dw_job_audit`, or `dw_error_log`.
        ```sql
        SELECT COUNT(*) FROM `your_gcp_project_id.your_bq_dataset_name.dw_job_entries`; -- Expected: 0
        SELECT COUNT(*) FROM `your_gcp_project_id.your_bq_dataset_name.dw_job_audit`;   -- Expected: 0
        SELECT COUNT(*) FROM `your_gcp_project_id.your_bq_dataset_name.dw_error_log`;   -- Expected: 0
        ```
    3.  (Optional, if BigQuery client allows capturing procedure output) The client output contains the usage message defined in the procedure.

---

## Test Case 1.2: Unused Parameters Handling

*   **Purpose:** Verify that the BigQuery wrapper procedure accepts and ignores the `p_s` and `p_l` parameters, maintaining behavioral parity with the legacy script where these parameters were parsed but not explicitly used within the wrapper.
*   **Setup:**
    1.  Ensure the logging tables are empty:
        ```sql
        TRUNCATE TABLE `your_gcp_project_id.your_bq_dataset_name.dw_job_entries`;
        TRUNCATE TABLE `your_gcp_project_id.your_bq_dataset_name.dw_job_audit`;
        TRUNCATE TABLE `your_gcp_project_id.your_bq_dataset_name.dw_error_log`;
        ```
    2.  Ensure the `k_ausd_v_ta_inv_assign` placeholder procedure is configured to succeed (as per the provided `sp/k_ausd_v_ta_inv_assign.sql`).
*   **Action:** Execute the BigQuery wrapper procedure with values for `p_s` and `p_l`.
    ```sql
    CALL `your_gcp_project_id.your_bq_dataset_name.vertragsdatenabgleich_wrapper`(p_h => FALSE, p_s => 'test_s_param', p_l => 'test_l_param');
    ```
*   **Pass/Fail Criterion:**
    1.  The `CALL` statement completes successfully.
    2.  A new job entry is created in `dw_job_entries` with `status = 'OK'`.
        ```sql
        SELECT status FROM `your_gcp_project_id.your_bq_dataset_name.dw_job_entries` WHERE job_kennung = 'BERT_V_TA_INV_ASSIGN'; -- Expected: 'OK'
        ```
    3.  Audit logs are created in `dw_job_audit` indicating job start, core logic execution, and successful completion.
        ```sql
        SELECT COUNTIF(message LIKE '%Job gestartet%') AS start_msg,
               COUNTIF(message LIKE '%Core logic executed successfully%') AS core_msg,
               COUNTIF(message LIKE '%ohne erkennbare Fehler beendet%') AS success_msg
        FROM `your_gcp_project_id.your_bq_dataset_name.dw_job_audit`
        WHERE job_kennung = 'BERT_V_TA_INV_ASSIGN'; -- Expected: start_msg >= 1, core_msg >= 1, success_msg >= 1
        ```
    4.  The values 'test_s_param' and 'test_l_param' are not present in any logged messages in `dw_job_audit` or `dw_error_log`, confirming they are ignored by the wrapper's logic.

---

## Test Case 1.3: Parameter Type Mismatch (Invocation Error)

*   **Purpose:** Verify that BigQuery's intrinsic error handling correctly captures and prevents execution when the wrapper procedure is called with an invalid parameter type, simulating an invocation error that would prevent the legacy script from running due to `getopts` errors.
*   **Setup:**
    1.  Ensure the logging tables are empty:
        ```sql
        TRUNCATE TABLE `your_gcp_project_id.your_bq_dataset_name.dw_job_entries`;
        TRUNCATE TABLE `your_gcp_project_id.your_bq_dataset_name.dw_job_audit`;
        TRUNCATE TABLE `your_gcp_project_id.your_bq_dataset_name.dw_error_log`;
        ```
*   **Action:** Attempt to execute the BigQuery wrapper procedure with a type mismatch for `p_h`.
    ```sql
    -- This call is expected to fail at the BigQuery engine level before procedure execution.
    CALL `your_gcp_project_id.your_bq_dataset_name.vertragsdatenabgleich_wrapper`(p_h => 'invalid_string', p_s => NULL, p_l => NULL);
    ```
*   **Pass/Fail Criterion:**
    1.  The `CALL` statement fails with a BigQuery runtime error (e.g., "Invalid value: 'invalid_string' for parameter p_h of type BOOL").
    2.  No new rows are inserted into `dw_job_entries`, `dw_job_audit`, or `dw_error_log`, as the procedure's execution should not have started.
        ```sql
        SELECT COUNT(*) FROM `your_gcp_project_id.your_bq_dataset_name.dw_job_entries`; -- Expected: 0
        SELECT COUNT(*) FROM `your_gcp_project_id.your_bq_dataset_name.dw_job_audit`;   -- Expected: 0
        SELECT COUNT(*) FROM `your_gcp_project_id.your_bq_dataset_name.dw_error_log`;   -- Expected: 0
        ```

---

## Test Case 2.1: Successful Job Execution - `dw_job_entries` Logging

*   **Purpose:** Verify that a successful execution of the wrapper procedure correctly records a 'STARTED' and then an 'OK' status in the `dw_job_entries` table, along with other job metadata.
*   **Setup:**
    1.  Clear `dw_job_entries` table:
        ```sql
        TRUNCATE TABLE `your_gcp_project_id.your_bq_dataset_name.dw_job_entries`;
        ```
    2.  Ensure `k_ausd_v_ta_inv_assign` is configured to succeed (as per the provided `sp/k_ausd_v_ta_inv_assign.sql`).
*   **Action:** Execute the BigQuery wrapper procedure.
    ```sql
    CALL `your_gcp_project_id.your_bq_dataset_name.vertragsdatenabgleich_wrapper`(p_h => FALSE, p_s => NULL, p_l => NULL);
    ```
*   **Pass/Fail Criterion:**
    1.  Exactly one row exists in `dw_job_entries` for `JobKennung = 'BERT_V_TA_INV_ASSIGN'`.
    2.  The `status` column for this row is 'OK'.
    3.  `script_name` is 'Vertragsdatenabgleich'.
    4.  `sysdate_ddmmyyyy` matches `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
    5.  `created_at` and `finished_at` are populated, and `finished_at` is after `created_at`.
    6.  The `entry_nr` is 1 (as the table was truncated).
        ```sql
        SELECT
            entry_nr,
            job_kennung,
            script_name,
            sysdate_ddmmyyyy,
            status,
            created_at IS NOT NULL AS created_at_populated,
            finished_at IS NOT NULL AS finished_at_populated,
            finished_at > created_at AS finished_after_created
        FROM `your_gcp_project_id.your_bq_dataset_name.dw_job_entries`
        WHERE job_kennung = 'BERT_V_TA_INV_ASSIGN';
        -- Expected: 1 row with entry_nr=1, job_kennung='BERT_V_TA_INV_ASSIGN', script_name='Vertragsdatenabgleich',
        --           sysdate_ddmmyyyy=CURRENT_DATE() formatted as DDMMYYYY, status='OK',
        --           created_at_populated=TRUE, finished_at_populated=TRUE, finished_after_created=TRUE.
        ```

---

## Test Case 2.2: Successful Job Execution - `dw_job_audit` Logging

*   **Purpose:** Verify that a successful execution logs appropriate messages to the `dw_job_audit` table, including job start, core logic execution, and successful completion messages, reflecting the content of the legacy `LogDatei`.
*   **Setup:**
    1.  Clear `dw_job_audit` table:
        ```sql
        TRUNCATE TABLE `your_gcp_project_id.your_bq_dataset_name.dw_job_audit`;
        ```
    2.  Ensure `k_ausd_v_ta_inv_assign` is configured to succeed.
*   **Action:** Execute the BigQuery wrapper procedure.
    ```sql
    CALL `your_gcp_project_id.your_bq_dataset_name.vertragsdatenabgleich_wrapper`(p_h => FALSE, p_s => NULL, p_l => NULL);
    ```
*   **Pass/Fail Criterion:**
    1.  At least 3 rows exist in `dw_job_audit` for the `JobKennung` and `entry_nr` of the current run.
    2.  The `message` column contains the expected sequence of events:
        *   A message starting with `'Job gestartet...'`.
        *   A message from the core procedure: `'k_ausd_v_ta_inv_assign: Core logic executed successfully (placeholder)'`.
        *   A message indicating successful completion: `'Die Abarbeitung wurde ohne erkennbare Fehler beendet'`.
    3.  `created_at` timestamps are sequential, reflecting the order of events.
    4.  `entry_nr` and `job_kennung` match the corresponding `dw_job_entries` record.
        ```sql
        SELECT message, created_at
        FROM `your_gcp_project_id.your_bq_dataset_name.dw_job_audit`
        WHERE job_kennung = 'BERT_V_TA_INV_ASSIGN'
        ORDER BY created_at;
        -- Expected:
        -- Row 1: message LIKE 'Job gestartet. JobKennung: BERT_V_TA_INV_ASSIGN, EntryNr: 1'
        -- Row 2: message = 'k_ausd_v_ta_inv_assign: Core logic executed successfully (placeholder)'
        -- Row 3: message = 'Die Abarbeitung wurde ohne erkennbare Fehler beendet'
        -- All rows should have entry_nr = 1 and job_kennung = 'BERT_V_TA_INV_ASSIGN'.
        ```

---

## Test Case 2.3: Error Handling - Core Script Failure

*   **Purpose:** Verify that if the invoked core script (`k_ausd_v_ta_inv_assign`) raises an error, the wrapper correctly catches it, logs the error details, updates `dw_job_entries` to 'ERROR', and re-raises the error to the caller, mimicking the legacy `trap ERR` behavior.
*   **Setup:**
    1.  Clear all logging tables:
        ```sql
        TRUNCATE TABLE `your_gcp_project_id.your_bq_dataset_name.dw_job_entries`;
        TRUNCATE TABLE `your_gcp_project_id.your_bq_dataset_name.dw_job_audit`;
        TRUNCATE TABLE `your_gcp_project_id.your_bq_dataset_name.dw_error_log`;
        ```
    2.  Modify `k_ausd_v_ta_inv_assign` to explicitly raise an error:
        ```sql
        CREATE OR REPLACE PROCEDURE `your_gcp_project_id.your_bq_dataset_name.k_ausd_v_ta_inv_assign`(
          IN p_job_kennung STRING,
          IN p_dw_eintrags_nr INT64
        )
        BEGIN
          INSERT INTO `your_gcp_project_id.your_bq_dataset_name.dw_job_audit`
            (entry_nr, job_kennung, message, created_at)
          VALUES
            (p_dw_eintrags_nr, p_job_kennung, 'k_ausd_v_ta_inv_assign: Simulating an error', CURRENT_TIMESTAMP());
          SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated core script error';
        END;
        ```
*   **Action:** Execute the BigQuery wrapper procedure.
    ```sql
    CALL `your_gcp_project_id.your_bq_dataset_name.vertragsdatenabgleich_wrapper`(p_h => FALSE, p_s => NULL, p_l => NULL);
    ```
*   **Pass/Fail Criterion:**
    1.  The `CALL` statement fails with a `SQLSTATE '45000'` error and a message similar to 'Job execution failed: Simulated core script error'.
    2.  Exactly one row exists in `dw_job_entries` for the run, with `status = 'ERROR'`.
        ```sql
        SELECT status FROM `your_gcp_project_id.your_bq_dataset_name.dw_job_entries` WHERE job_kennung = 'BERT_V_TA_INV_ASSIGN'; -- Expected: 'ERROR'
        ```
    3.  `dw_job_audit` contains messages reflecting the error path:
        *   `'Job gestartet...'`
        *   `'k_ausd_v_ta_inv_assign: Simulating an error'`
        *   `'AppError: Abbruch. Error: Simulated core script error...'`
        ```sql
        SELECT message
        FROM `your_gcp_project_id.your_bq_dataset_name.dw_job_audit`
        WHERE job_kennung = 'BERT_V_TA_INV_ASSIGN'
        ORDER BY created_at;
        -- Expected: Messages should include the three listed above.
        ```
    4.  Exactly one row exists in `dw_error_log` for the run, with `error_message = 'Simulated core script error'` and `error_code` populated.
        ```sql
        SELECT error_message, error_code
        FROM `your_gcp_project_id.your_bq_dataset_name.dw_error_log`
        WHERE job_kennung = 'BERT_V_TA_INV_ASSIGN';
        -- Expected: 1 row with error_message='Simulated core script error' and error_code='BQ-20000' (or similar BigQuery error code for SIGNAL SQLSTATE).
        ```
*   **Cleanup:** Revert `k_ausd_v_ta_inv_assign` to its successful placeholder state for subsequent tests.

---

## Test Case 3.1: Schema Validation of Logging Tables

*   **Purpose:** Verify that the schemas of the logging tables (`dw_job_entries`, `dw_job_audit`, `dw_error_log`) match the design document's specifications for column names, data types, and nullability.
*   **Setup:** None (assumes tables are created as per DDLs).
*   **Action:** Query BigQuery's `INFORMATION_SCHEMA` views for table metadata.
*   **Pass/Fail Criterion:**
    1.  **`dw_job_entries` schema:**
        ```sql
        SELECT column_name, data_type, is_nullable
        FROM `your_gcp_project_id.your_bq_dataset_name`.INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = 'dw_job_entries'
        ORDER BY ordinal_position;
        -- Expected:
        -- column_name | data_type | is_nullable
        -- -------------|-----------|------------
        -- entry_nr    | INT64     | NO
        -- job_kennung | STRING    | NO
        -- script_name | STRING    | YES
        -- sysdate_ddmmyyyy | STRING | YES
        -- status      | STRING    | YES
        -- created_at  | TIMESTAMP | YES
        -- finished_at | TIMESTAMP | YES
        ```
    2.  **`dw_job_audit` schema:**
        ```sql
        SELECT column_name, data_type, is_nullable
        FROM `your_gcp_project_id.your_bq_dataset_name`.INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = 'dw_job_audit'
        ORDER BY ordinal_position;
        -- Expected:
        -- column_name | data_type | is_nullable
        -- -------------|-----------|------------
        -- entry_nr    | INT64     | NO
        -- job_kennung | STRING    | NO
        -- message     | STRING    | YES
        -- created_at  | TIMESTAMP | YES
        ```
    3.  **`dw_error_log` schema:**
        ```sql
        SELECT column_name, data_type, is_nullable
        FROM `your_gcp_project_id.your_bq_dataset_name`.INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = 'dw_error_log'
        ORDER BY ordinal_position;
        -- Expected:
        -- column_name | data_type | is_nullable
        -- -------------|-----------|------------
        -- entry_nr    | INT64     | NO
        -- job_kennung | STRING    | NO
        -- error_message | STRING  | YES
        -- error_code  | STRING    | YES
        -- created_at  | TIMESTAMP | YES
        ```

---

## Test Case 3.2: `DW_EintragsNr` Increment Across Runs

*   **Purpose:** Verify that the `DW_EintragsNr` (job entry number) is correctly incremented for each new job run, regardless of whether the run succeeds or fails, ensuring proper sequencing of job entries.
*   **Setup:**
    1.  Clear all logging tables:
        ```sql
        TRUNCATE TABLE `your_gcp_project_id.your_bq_dataset_name.dw_job_entries`;
        TRUNCATE TABLE `your_gcp_project_id.your_bq_dataset_name.dw_job_audit`;
        TRUNCATE TABLE `your_gcp_project_id.your_bq_dataset_name.dw_error_log`;
        ```
    2.  Ensure `k_ausd_v_ta_inv_assign` is configured to succeed initially.
*   **Action:**
    1.  **Run 1 (Success):** `CALL `your_gcp_project_id.your_bq_dataset_name.vertragsdatenabgleich_wrapper`(p_h => FALSE, p_s => NULL, p_l => NULL);`
    2.  **Modify `k_ausd_v_ta_inv_assign` to fail:**
        ```sql
        CREATE OR REPLACE PROCEDURE `your_gcp_project_id.your_bq_dataset_name.k_ausd_v_ta_inv_assign`(
          IN p_job_kennung STRING,
          IN p_dw_eintrags_nr INT64
        )
        BEGIN
          SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated failure for Run 2';
        END;
        ```
    3.  **Run 2 (Failure):** `CALL `your_gcp_project_id.your_bq_dataset_name.vertragsdatenabgleich_wrapper`(p_h => FALSE, p_s => NULL, p_l => NULL);` (This call is expected to fail).
    4.  **Revert `k_ausd_v_ta_inv_assign` to succeed:**
        ```sql
        CREATE OR REPLACE PROCEDURE `your_gcp_project_id.your_bq_dataset_name.k_ausd_v_ta_inv_assign`(
          IN p_job_kennung STRING,
          IN p_dw_eintrags_nr INT64
        )
        BEGIN
          INSERT INTO `your_gcp_project_id.your_bq_dataset_name.dw_job_audit`
            (entry_nr, job_kennung, message, created_at)
          VALUES
            (p_dw_eintrags_nr, p_job_kennung, 'k_ausd_v_ta_inv_assign: Core logic executed successfully (placeholder)', CURRENT_TIMESTAMP());
        END;
        ```
    5.  **Run 3 (Success):** `CALL `your_gcp_project_id.your_bq_dataset_name.vertragsdatenabgleich_wrapper`(p_h => FALSE, p_s => NULL, p_l => NULL);`
*   **Pass/Fail Criterion:**
    1.  Query `dw_job_entries` for `JobKennung = 'BERT_V_TA_INV_ASSIGN'`. There should be 3 rows.
    2.  The `entry_nr` values for these 3 rows should be 1, 2, and 3 respectively.
    3.  The `status` values should be 'OK', 'ERROR', 'OK' corresponding to the runs.
        ```sql
        SELECT entry_nr, status, created_at
        FROM `your_gcp_project_id.your_bq_dataset_name.dw_job_entries`
        WHERE job_kennung = 'BERT_V_TA_INV_ASSIGN'
        ORDER BY created_at;
        -- Expected:
        -- entry_nr | status | created_at
        -- ----------|--------|-----------
        -- 1        | OK     | <timestamp1>
        -- 2        | ERROR  | <timestamp2>
        -- 3        | OK     | <timestamp3>
        -- (where timestamp1 < timestamp2 < timestamp3)
        ```
*   **Cleanup:** Revert `k_ausd_v_ta_inv_assign` to its successful placeholder state if not already done.

---

## Test Case 4.1: Logging Mechanism Replacement (External System)

*   **Purpose:** Verify that the BigQuery logging tables (`dw_job_entries`, `dw_job_audit`, `dw_error_log`) effectively replace the file-based logging of the legacy script, capturing all relevant job metadata and detailed messages. This validates the replacement of a file-based external system with a BigQuery table-based system.
*   **Setup:**
    1.  Clear all logging tables.
    2.  Ensure `k_ausd_v_ta_inv_assign` is configured to succeed.
*   **Action:** Execute the BigQuery wrapper procedure.
    ```sql
    CALL `your_gcp_project_id.your_bq_dataset_name.vertragsdatenabgleich_wrapper`(p_h => FALSE, p_s => NULL, p_l => NULL);
    ```
*   **Pass/Fail Criterion:**
    1.  `dw_job_entries` contains a complete record of the job's metadata (start/end times, status, script name, date), confirming the replacement of `DWMSG_SetzeStatusOK` and initial entry creation.
        ```sql
        SELECT COUNT(*) FROM `your_gcp_project_id.your_bq_dataset_name.dw_job_entries` WHERE job_kennung = 'BERT_V_TA_INV_ASSIGN' AND status = 'OK'; -- Expected: 1
        ```
    2.  `dw_job_audit` contains a detailed sequence of events, including the job start message, the core script execution message, and the success message, equivalent to the content expected in the legacy `LogDatei` generated by `DWMSG_ErzeugeEintrag` and `print`/`tee`.
        ```sql
        SELECT COUNT(*) FROM `your_gcp_project_id.your_bq_dataset_name.dw_job_audit` WHERE job_kennung = 'BERT_V_TA_INV_ASSIGN'; -- Expected: >= 3
        SELECT message FROM `your_gcp_project_id.your_bq_dataset_name.dw_job_audit` WHERE message LIKE 'Job gestartet%' AND job_kennung = 'BERT_V_TA_INV_ASSIGN'; -- Expected: 1 row
        SELECT message FROM `your_gcp_project_id.your_bq_dataset_name.dw_job_audit` WHERE message LIKE '%Core logic executed successfully%' AND job_kennung = 'BERT_V_TA_INV_ASSIGN'; -- Expected: 1 row
        SELECT message FROM `your_gcp_project_id.your_bq_dataset_name.dw_job_audit` WHERE message LIKE 'Die Abarbeitung wurde ohne erkennbare Fehler beendet' AND job_kennung = 'BERT_V_TA_INV_ASSIGN'; -- Expected: 1 row
        ```
    3.  No entries are present in `dw_error_log` for this successful run, confirming correct error logging separation.
        ```sql
        SELECT COUNT(*) FROM `your_gcp_project_id.your_bq_dataset_name.dw_error_log` WHERE job_kennung = 'BERT_V_TA_INV_ASSIGN'; -- Expected: 0
        ```

---