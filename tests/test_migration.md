As a senior data-migration QA engineer, I've reviewed the migration design for `k_ausd_v_ta_cntrct_templ.ksh` to a BigQuery Stored Procedure orchestrated by Cloud Composer. The following test cases are designed to ensure behavioral equivalence, data integrity, and correctness of the migrated solution.

---

## Migration Validation Tests: `k_ausd_v_ta_cntrct_templ.ksh`

### Pre-requisites for all Tests:

*   **Legacy Environment:**
    *   Access to the legacy database (assumed Oracle) containing `ta_cntrct_templ`, `cds_ta_cntrct_template`, `cds_ta_care_description`, and `dwtk_meldungen` tables.
    *   Ability to execute `k_ausd_v_ta_cntrct_templ.ksh` with specified parameters.
    *   Tools to inspect legacy database table contents and the temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_cntrct_templ_$$.tmp`).
*   **Migrated Environment:**
    *   A GCP project and BigQuery dataset (`project.dataset`) configured.
    *   The following BigQuery tables created:
        *   `project.dataset.job_table` (DDL provided in design)
        *   `project.dataset.job_error_log` (DDL provided in design)
        *   `project.dataset.ta_cntrct_templ` (target table, schema matching legacy `ta_cntrct_templ`)
        *   `project.dataset.dwtk_meldungen` (source for `v_datum_bq`, schema matching legacy `dwtk_meldungen`)
        *   `project.dataset.cds_ta_cntrct_template` (source, schema matching legacy `cds_ta_cntrct_template`)
        *   `project.dataset.cds_ta_care_description` (source, schema matching legacy `cds_ta_care_description`)
    *   The BigQuery Stored Procedure `project.dataset.r_ausd_v_ta_cntrct_templ` deployed.
    *   The Cloud Composer DAG `k_ausd_v_ta_cntrct_templ_dag.py` deployed and accessible.
    *   Appropriate IAM permissions for BigQuery data manipulation and Composer DAG execution.

---

### Test Case 1: Happy Path - Successful Data Migration

**Purpose:** Verify that the migrated job successfully processes data, populates the target table correctly, and updates the job tracking tables as expected under normal operating conditions. This covers output parity and transformation correctness for a typical scenario.

**Setup:**
1.  **Legacy:**
    *   Populate legacy `cds_ta_cntrct_template` and `cds_ta_care_description` with diverse data, including records that should match the `WHERE` clause conditions and some that should not.
    *   Populate legacy `dwtk_meldungen` with a `job_kennung = 'BERT_DROP_TEMP_TABLE'` entry, ensuring `MAX(timecreated)` results in a specific `YYYYMMDD` date (e.g., '20030109').
    *   Ensure legacy `ta_cntrct_templ` is empty or contains old data to be truncated.
    *   Ensure no active jobs for `p_JobKennung` in the legacy job tracking mechanism.
2.  **Migrated:**
    *   Populate BigQuery `project.dataset.cds_ta_cntrct_template` and `project.dataset.cds_ta_care_description` with *identical* data as the legacy sources.
    *   Populate BigQuery `project.dataset.dwtk_meldungen` with *identical* data as the legacy source, ensuring the same `MAX(timecreated)` for 'BERT_DROP_TEMP_TABLE'.
    *   Ensure `project.dataset.ta_cntrct_templ` is empty.
    *   Ensure `project.dataset.job_table` and `project.dataset.job_error_log` are empty.

**Action:**
1.  **Legacy:** Execute the KornShell script:
    ```bash
    k_ausd_v_ta_cntrct_templ.ksh -j "TEST_JOB_HAPPY" -f 1001
    ```
2.  **Migrated:** Trigger the Cloud Composer DAG `k_ausd_v_ta_cntrct_templ_dag` with parameters:
    ```json
    {
      "job_kennung": "TEST_JOB_HAPPY",
      "eintragsnr": 1001
    }
    ```

**Pass/Fail Criterion:**
*   **Output Parity (Data):** The number of rows and the content of `project.dataset.ta_cntrct_templ` must be *identical* to the number of rows and content of the legacy `ta_cntrct_templ` table.
    *   **SQL Assertion (BigQuery):**
        ```sql
        -- Compare row counts
        SELECT COUNT(*) FROM project.dataset.ta_cntrct_templ;
        -- Expected: <count from legacy ta_cntrct_templ>

        -- Compare content (example for a few columns, ideally all)
        SELECT CNTRCT_TEMPLATE_ID, CDS_DESCRIPTION_ID, CDS_DESCRIPTION
        FROM project.dataset.ta_cntrct_templ
        ORDER BY CNTRCT_TEMPLATE_ID;
        -- Expected: <exact data set from legacy ta_cntrct_templ>
        ```
*   **Transformation Correctness (Job Table):**
    *   `project.dataset.job_table` must contain two entries for `job_kennung = 'TEST_JOB_HAPPY'` and `eintragsnr = 1001`:
        1.  One with `status = 'STARTED'`, `start_time` populated, `end_time` and `records_processed` NULL.
        2.  One with `status = 'FINISHED'`, `start_time`, `end_time` populated, and `records_processed` matching the row count in `ta_cntrct_templ`.
    *   The `records_processed` value in the `FINISHED` entry must match the `v_records` value captured by the legacy script (from `$DW_DIR_UTL/bert_k_ausd_v_ta_cntrct_templ_$$.tmp`).
    *   `project.dataset.job_error_log` must be empty.
    *   **SQL Assertion (BigQuery):**
        ```sql
        SELECT job_kennung, eintragsnr, status, records_processed
        FROM project.dataset.job_table
        WHERE job_kennung = 'TEST_JOB_HAPPY' AND eintragsnr = 1001
        ORDER BY start_time;
        -- Expected:
        -- job_kennung | eintragsnr | status    | records_processed
        -- ------------|------------|-----------|------------------
        -- TEST_JOB_HAPPY | 1001       | STARTED   | NULL
        -- TEST_JOB_HAPPY | 1001       | FINISHED  | <count from ta_cntrct_templ>

        SELECT COUNT(*) FROM project.dataset.job_error_log;
        -- Expected: 0
        ```

---

### Test Case 2: Parameter Validation - Missing `p_JobKennung`

**Purpose:** Verify that the migrated job correctly handles missing mandatory parameters, logs the error, and terminates gracefully without processing data.

**Setup:**
1.  **Legacy:**
    *   Ensure legacy `ta_cntrct_templ` is empty.
    *   Ensure legacy job tracking tables are in a known state.
2.  **Migrated:**
    *   Ensure `project.dataset.ta_cntrct_templ` is empty.
    *   Ensure `project.dataset.job_table` and `project.dataset.job_error_log` are empty.

**Action:**
1.  **Legacy:** Execute the KornShell script without `-j`:
    ```bash
    k_ausd_v_ta_cntrct_templ.ksh -f 1002
    ```
2.  **Migrated:** Trigger the Cloud Composer DAG `k_ausd_v_ta_cntrct_templ_dag` with parameters, omitting `job_kennung`:
    ```json
    {
      "eintragsnr": 1002
    }
    ```
    (Note: Airflow's `params` might default `job_kennung` to `DEFAULT_JOB_KENNUNG`. If so, explicitly pass `null` or an empty string if the SP handles it, or test with an empty string as per the SP's `IF p_JobKennung IS NULL OR p_JobKennung = ''` check). Let's assume the DAG passes an empty string if not provided, or we explicitly pass `""`.

**Pass/Fail Criterion:**
*   **Legacy:** The script must exit with an error code (e.g., `193` or `192` as per `ErrNr` in the script) and print an error message to `stderr` or `stdout` indicating a missing parameter. `ta_cntrct_templ` should remain empty.
*   **Migrated:**
    *   The Cloud Composer DAG run must fail.
    *   `project.dataset.ta_cntrct_templ` must remain empty.
    *   `project.dataset.job_table` must remain empty (as the error occurs before job registration).
    *   `project.dataset.job_error_log` must contain one entry for `eintragsnr = 1002` (or -1 if `p_EintragsNr` is also not passed or invalid) with `error_message` indicating "p_JobKennung cannot be NULL or empty." and `sql_state = 'P0001'`.
    *   **SQL Assertion (BigQuery):**
        ```sql
        SELECT COUNT(*) FROM project.dataset.ta_cntrct_templ;
        -- Expected: 0

        SELECT COUNT(*) FROM project.dataset.job_table;
        -- Expected: 0

        SELECT job_kennung, eintragsnr, error_message, sql_state
        FROM project.dataset.job_error_log
        WHERE eintragsnr = 1002; -- Or -1 if eintragsnr is also not passed
        -- Expected:
        -- job_kennung | eintragsnr | error_message                  | sql_state
        -- ------------|------------|--------------------------------|-----------
        -- UNKNOWN     | 1002       | p_JobKennung cannot be NULL or empty. | P0001
        ```

---

### Test Case 3: Parameter Validation - Missing `p_EintragsNr`

**Purpose:** Verify that the migrated job correctly handles missing mandatory parameters, logs the error, and terminates gracefully without processing data.

**Setup:**
1.  **Legacy:**
    *   Ensure legacy `ta_cntrct_templ` is empty.
    *   Ensure legacy job tracking tables are in a known state.
2.  **Migrated:**
    *   Ensure `project.dataset.ta_cntrct_templ` is empty.
    *   Ensure `project.dataset.job_table` and `project.dataset.job_error_log` are empty.

**Action:**
1.  **Legacy:** Execute the KornShell script without `-f`:
    ```bash
    k_ausd_v_ta_cntrct_templ.ksh -j "TEST_JOB_MISSING_F"
    ```
2.  **Migrated:** Trigger the Cloud Composer DAG `k_ausd_v_ta_cntrct_templ_dag` with parameters, omitting `eintragsnr`:
    ```json
    {
      "job_kennung": "TEST_JOB_MISSING_F"
    }
    ```
    (Note: Airflow's `params` might default `eintragsnr` to `1`. If so, explicitly pass `null` or omit the parameter entirely if the Airflow operator allows passing `null` to the SP). Let's assume the DAG passes `null` if not provided, or we explicitly pass `null`.

**Pass/Fail Criterion:**
*   **Legacy:** The script must exit with an error code (e.g., `193` or `192`) and print an error message to `stderr` or `stdout` indicating a missing parameter. `ta_cntrct_templ` should remain empty.
*   **Migrated:**
    *   The Cloud Composer DAG run must fail.
    *   `project.dataset.ta_cntrct_templ` must remain empty.
    *   `project.dataset.job_table` must remain empty.
    *   `project.dataset.job_error_log` must contain one entry for `job_kennung = 'TEST_JOB_MISSING_F'` with `error_message` indicating "p_EintragsNr cannot be NULL." and `sql_state = 'P0002'`.
    *   **SQL Assertion (BigQuery):**
        ```sql
        SELECT COUNT(*) FROM project.dataset.ta_cntrct_templ;
        -- Expected: 0

        SELECT COUNT(*) FROM project.dataset.job_table;
        -- Expected: 0

        SELECT job_kennung, eintragsnr, error_message, sql_state
        FROM project.dataset.job_error_log
        WHERE job_kennung = 'TEST_JOB_MISSING_F';
        -- Expected:
        -- job_kennung      | eintragsnr | error_message                  | sql_state
        -- -----------------|------------|--------------------------------|-----------
        -- TEST_JOB_MISSING_F | -1         | p_EintragsNr cannot be NULL. | P0002
        ```

---

### Test Case 4: Job Deactivation Logic

**Purpose:** Verify that the migrated job correctly deactivates older active jobs for the same `p_JobKennung` before starting its own process. This tests the `UPDATE project.dataset.job_table SET status = 'DEACTIVATED'` logic.

**Setup:**
1.  **Legacy:**
    *   Populate legacy job tracking tables with an entry for `job_kennung = 'TEST_JOB_DEACTIVATE'` and `eintragsnr = 999` with a status indicating 'active' (e.g., 'STARTED').
    *   Ensure `ta_cntrct_templ` is empty.
2.  **Migrated:**
    *   Insert a 'STARTED' entry into `project.dataset.job_table`:
        ```sql
        INSERT INTO project.dataset.job_table (job_kennung, eintragsnr, status, start_time)
        VALUES ('TEST_JOB_DEACTIVATE', 999, 'STARTED', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR));
        ```
    *   Populate source tables (`cds_ta_cntrct_template`, `cds_ta_care_description`, `dwtk_meldungen`) to allow for successful data processing.
    *   Ensure `project.dataset.ta_cntrct_templ` is empty.
    *   Ensure `project.dataset.job_error_log` is empty.

**Action:**
1.  **Legacy:** Execute the KornShell script:
    ```bash
    k_ausd_v_ta_cntrct_templ.ksh -j "TEST_JOB_DEACTIVATE" -f 1003
    ```
2.  **Migrated:** Trigger the Cloud Composer DAG `k_ausd_v_ta_cntrct_templ_dag` with parameters:
    ```json
    {
      "job_kennung": "TEST_JOB_DEACTIVATE",
      "eintragsnr": 1003
    }
    ```

**Pass/Fail Criterion:**
*   **Legacy:** The legacy job tracking mechanism should show the `eintragsnr = 999` job as 'deactivated' or similar, and the `eintragsnr = 1003` job as 'started' then 'finished'.
*   **Migrated:**
    *   The Cloud Composer DAG run must succeed.
    *   `project.dataset.job_table` must contain:
        *   One entry for `job_kennung = 'TEST_JOB_DEACTIVATE'`, `eintragsnr = 999`, with `status = 'DEACTIVATED'` and `end_time` populated.
        *   Two entries for `job_kennung = 'TEST_JOB_DEACTIVATE'`, `eintragsnr = 1003`, one 'STARTED' and one 'FINISHED' (as per Test Case 1).
    *   `project.dataset.ta_cntrct_templ` must be populated correctly.
    *   **SQL Assertion (BigQuery):**
        ```sql
        SELECT job_kennung, eintragsnr, status, records_processed
        FROM project.dataset.job_table
        WHERE job_kennung = 'TEST_JOB_DEACTIVATE'
        ORDER BY start_time, eintragsnr;
        -- Expected:
        -- job_kennung      | eintragsnr | status      | records_processed
        -- -----------------|------------|-------------|------------------
        -- TEST_JOB_DEACTIVATE | 999        | DEACTIVATED | NULL (or original count if tracked)
        -- TEST_JOB_DEACTIVATE | 1003       | STARTED     | NULL
        -- TEST_JOB_DEACTIVATE | 1003       | FINISHED    | <count from ta_cntrct_templ>
        ```

---

### Test Case 5: Core SQL Logic - No Matching Records

**Purpose:** Verify that the migrated job correctly handles scenarios where the core SQL `INSERT` statement finds no records matching its `WHERE` clause, resulting in zero records processed. This tests transformation correctness and record counting.

**Setup:**
1.  **Legacy:**
    *   Populate legacy `cds_ta_cntrct_template` and `cds_ta_care_description` such that *no records* satisfy the `WHERE` clause conditions (e.g., all `is_production = 0`, or `language != 1`, or dates outside the `v_datum` range).
    *   Populate legacy `dwtk_meldungen` to define `v_datum`.
    *   Ensure legacy `ta_cntrct_templ` is empty.
2.  **Migrated:**
    *   Populate BigQuery source tables (`cds_ta_cntrct_template`, `cds_ta_care_description`, `dwtk_meldungen`) *identically* to the legacy setup, ensuring zero matching records.
    *   Ensure `project.dataset.ta_cntrct_templ` is empty.
    *   Ensure `project.dataset.job_table` and `project.dataset.job_error_log` are empty.

**Action:**
1.  **Legacy:** Execute the KornShell script:
    ```bash
    k_ausd_v_ta_cntrct_templ.ksh -j "TEST_JOB_NO_RECORDS" -f 1004
    ```
2.  **Migrated:** Trigger the Cloud Composer DAG `k_ausd_v_ta_cntrct_templ_dag` with parameters:
    ```json
    {
      "job_kennung": "TEST_JOB_NO_RECORDS",
      "eintragsnr": 1004
    }
    ```

**Pass/Fail Criterion:**
*   **Output Parity (Data):** Both legacy and migrated `ta_cntrct_templ` tables must remain empty.
    *   **SQL Assertion (BigQuery):**
        ```sql
        SELECT COUNT(*) FROM project.dataset.ta_cntrct_templ;
        -- Expected: 0
        ```
*   **Transformation Correctness (Job Table):**
    *   The `records_processed` value in the `FINISHED` entry in `project.dataset.job_table` must be `0`.
    *   The `v_records` variable captured by the legacy script (from `$DW_DIR_UTL/bert_k_ausd_v_ta_cntrct_templ_$$.tmp`) must also be `0`.
    *   **SQL Assertion (BigQuery):**
        ```sql
        SELECT job_kennung, eintragsnr, status, records_processed
        FROM project.dataset.job_table
        WHERE job_kennung = 'TEST_JOB_NO_RECORDS' AND eintragsnr = 1004
        ORDER BY start_time;
        -- Expected:
        -- job_kennung       | eintragsnr | status    | records_processed
        -- ------------------|------------|-----------|------------------
        -- TEST_JOB_NO_RECORDS | 1004       | STARTED   | NULL
        -- TEST_JOB_NO_RECORDS | 1004       | FINISHED  | 0
        ```

---

### Test Case 6: Core SQL Logic - NULL Handling in Dates

**Purpose:** Verify that the migrated job correctly handles `NULL` values in `modified_at` and `valid_to` columns as per the `WHERE` clause logic (`IS NULL OR ... > PARSE_DATE`). This tests transformation correctness.

**Setup:**
1.  **Legacy:**
    *   Populate legacy `cds_ta_cntrct_template` with records where `modified_at IS NULL` and `valid_to IS NULL`, and other records where these dates are populated but satisfy/don't satisfy the `WHERE` clause.
    *   Populate legacy `cds_ta_care_description` and `dwtk_meldungen` as needed.
    *   Ensure legacy `ta_cntrct_templ` is empty.
2.  **Migrated:**
    *   Populate BigQuery source tables (`cds_ta_cntrct_template`, `cds_ta_care_description`, `dwtk_meldungen`) *identically* to the legacy setup, specifically mirroring the `NULL` and non-`NULL` date scenarios.
    *   Ensure `project.dataset.ta_cntrct_templ` is empty.
    *   Ensure `project.dataset.job_table` and `project.dataset.job_error_log` are empty.

**Action:**
1.  **Legacy:** Execute the KornShell script:
    ```bash
    k_ausd_v_ta_cntrct_templ.ksh -j "TEST_JOB_NULL_DATES" -f 1005
    ```
2.  **Migrated:** Trigger the Cloud Composer DAG `k_ausd_v_ta_cntrct_templ_dag` with parameters:
    ```json
    {
      "job_kennung": "TEST_JOB_NULL_DATES",
      "eintragsnr": 1005
    }
    ```

**Pass/Fail Criterion:**
*   **Output Parity (Data):** The number of rows and the content of `project.dataset.ta_cntrct_templ` must be *identical* to the number of rows and content of the legacy `ta_cntrct_templ` table. This specifically validates that records with `NULL` `modified_at` or `valid_to` are included/excluded consistently between legacy and migrated.
    *   **SQL Assertion (BigQuery):**
        ```sql
        SELECT COUNT(*) FROM project.dataset.ta_cntrct_templ;
        -- Expected: <count from legacy ta_cntrct_templ>

        SELECT CNTRCT_TEMPLATE_ID, CDS_DESCRIPTION_ID, CDS_DESCRIPTION
        FROM project.dataset.ta_cntrct_templ
        ORDER BY CNTRCT_TEMPLATE_ID;
        -- Expected: <exact data set from legacy ta_cntrct_templ>
        ```

---

### Test Case 7: Error Handling - Core SQL Failure

**Purpose:** Verify that if an error occurs during the core SQL execution (e.g., a data type mismatch, or a constraint violation if the target table had one), the migrated job correctly logs the error, updates the job status to 'ERROR', and signals failure.

**Setup:**
1.  **Legacy:**
    *   Set up legacy source data (`cds_ta_cntrct_template`, `cds_ta_care_description`, `dwtk_meldungen`) such that the `d_ausd_v_ta_cntrct_templ.sql` script would encounter an error (e.g., if `cds_description_id` in `cds_ta_care_description` was `VARCHAR` but `cntrct_template_id` in `cds_ta_cntrct_template` was `INT` and contained non-numeric data, causing a join failure or implicit conversion error). This might require modifying the legacy source table schema temporarily or inserting malformed data.
    *   Ensure legacy `ta_cntrct_templ` is empty.
2.  **Migrated:**
    *   Populate BigQuery source tables (`cds_ta_cntrct_template`, `cds_ta_care_description`, `dwtk_meldungen`) *identically* to the legacy setup, specifically to trigger an error in the `INSERT` statement.
    *   Ensure `project.dataset.ta_cntrct_templ` is empty.
    *   Ensure `project.dataset.job_table` and `project.dataset.job_error_log` are empty.

**Action:**
1.  **Legacy:** Execute the KornShell script. Observe its exit status and error messages.
    ```bash
    k_ausd_v_ta_cntrct_templ.ksh -j "TEST_JOB_SQL_ERROR" -f 1006
    ```
2.  **Migrated:** Trigger the Cloud Composer DAG `k_ausd_v_ta_cntrct_templ_dag` with parameters:
    ```json
    {
      "job_kennung": "TEST_JOB_SQL_ERROR",
      "eintragsnr": 1006
    }
    ```

**Pass/Fail Criterion:**
*   **Legacy:** The script must exit with a non-zero error code and print error messages related to the SQL execution failure. `ta_cntrct_templ` should remain empty or in an inconsistent state depending on the error.
*   **Migrated:**
    *   The Cloud Composer DAG run must fail.
    *   `project.dataset.ta_cntrct_templ` must remain empty (due to `TRUNCATE` and subsequent `INSERT` failure).
    *   `project.dataset.job_table` must contain:
        *   One entry for `job_kennung = 'TEST_JOB_SQL_ERROR'`, `eintragsnr = 1006`, with `status = 'STARTED'`.
        *   One entry for `job_kennung = 'TEST_JOB_SQL_ERROR'`, `eintragsnr = 1006`, with `status = 'ERROR'`, `end_time` populated, and `error_message` containing details of the SQL failure.
    *   `project.dataset.job_error_log` must contain one entry for `job_kennung = 'TEST_JOB_SQL_ERROR'`, `eintragsnr = 1006`, with `error_message` and `sql_state` reflecting the BigQuery SQL error.
    *   **SQL Assertion (BigQuery):**
        ```sql
        SELECT COUNT(*) FROM project.dataset.ta_cntrct_templ;
        -- Expected: 0

        SELECT job_kennung, eintragsnr, status, error_message
        FROM project.dataset.job_table
        WHERE job_kennung = 'TEST_JOB_SQL_ERROR' AND eintragsnr = 1006
        ORDER BY start_time;
        -- Expected:
        -- job_kennung      | eintragsnr | status  | error_message
        -- -----------------|------------|---------|---------------------------------
        -- TEST_JOB_SQL_ERROR | 1006       | STARTED | NULL
        -- TEST_JOB_SQL_ERROR | 1006       | ERROR   | <BigQuery SQL error message>

        SELECT job_kennung, eintragsnr, error_message, sql_state
        FROM project.dataset.job_error_log
        WHERE job_kennung = 'TEST_JOB_SQL_ERROR' AND eintragsnr = 1006;
        -- Expected:
        -- job_kennung      | eintragsnr | error_message                  | sql_state
        -- -----------------|------------|--------------------------------|-----------
        -- TEST_JOB_SQL_ERROR | 1006       | <BigQuery SQL error message> | P9999 (or specific SQLSTATE)
        ```

---

### Test Case 8: `v_datum_bq` Derivation from `dwtk_meldungen`

**Purpose:** Verify that the `v_datum_bq` variable in the BigQuery Stored Procedure is correctly derived from `project.dataset.dwtk_meldungen` using `MAX(m.timecreated)` for `job_kennung = 'BERT_DROP_TEMP_TABLE'`, matching the implicit behavior of the legacy system. This is a critical external system replacement validation.

**Setup:**
1.  **Legacy:**
    *   Populate legacy `dwtk_meldungen` with multiple entries for `job_kennung = 'BERT_DROP_TEMP_TABLE'` with varying `timecreated` values, and also entries for other `job_kennung` values. Ensure a clear `MAX(timecreated)` for the target `job_kennung`.
    *   Example:
        *   `job_kennung='BERT_DROP_TEMP_TABLE', timecreated='2003-01-09 10:00:00'`
        *   `job_kennung='BERT_DROP_TEMP_TABLE', timecreated='2003-01-08 15:30:00'`
        *   `job_kennung='OTHER_JOB', timecreated='2003-01-10 12:00:00'`
    *   Populate other source tables to allow for successful data processing.
    *   Ensure legacy `ta_cntrct_templ` is empty.
2.  **Migrated:**
    *   Populate BigQuery `project.dataset.dwtk_meldungen` *identically* to the legacy setup.
    *   Populate other BigQuery source tables (`cds_ta_cntrct_template`, `cds_ta_care_description`) to allow for successful data processing, ensuring some records will match the `WHERE` clause when `v_datum_bq` is correctly derived.
    *   Ensure `project.dataset.ta_cntrct_templ` is empty.
    *   Ensure `project.dataset.job_table` and `project.dataset.job_error_log` are empty.

**Action:**
1.  **Legacy:** Execute the KornShell script.
    ```bash
    k_ausd_v_ta_cntrct_templ.ksh -j "TEST_JOB_DATE_DERIV" -f 1007
    ```
2.  **Migrated:** Trigger the Cloud Composer DAG `k_ausd_v_ta_cntrct_templ_dag` with parameters:
    ```json
    {
      "job_kennung": "TEST_JOB_DATE_DERIV",
      "eintragsnr": 1007
    }
    ```

**Pass/Fail Criterion:**
*   **Output Parity (Data):** The number of rows and the content of `project.dataset.ta_cntrct_templ` must be *identical* to the number of rows and content of the legacy `ta_cntrct_templ` table. This implicitly validates that the `v_datum_bq` (or equivalent `v_datum` in legacy) used for filtering was the same.
    *   **SQL Assertion (BigQuery):**
        ```sql
        -- First, determine the expected v_datum_bq from the setup data
        SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101')
        FROM project.dataset.dwtk_meldungen m
        WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
        -- Expected: '20030109' (based on example setup)

        -- Then, verify the ta_cntrct_templ content
        SELECT COUNT(*) FROM project.dataset.ta_cntrct_templ;
        -- Expected: <count from legacy ta_cntrct_templ>

        SELECT CNTRCT_TEMPLATE_ID, CDS_DESCRIPTION_ID, CDS_DESCRIPTION
        FROM project.dataset.ta_cntrct_templ
        ORDER BY CNTRCT_TEMPLATE_ID;
        -- Expected: <exact data set from legacy ta_cntrct_templ, filtered by '20030109'>
        ```

---

### Test Case 9: Idempotency - Rerunning a Successful Job

**Purpose:** Verify that rerunning the migrated job with the same parameters results in the same final state of the target data table, and that job tracking is handled correctly (new entries for the rerun, but `ta_cntrct_templ` is truncated and re-populated).

**Setup:**
1.  **Legacy:**
    *   Populate legacy source tables (`cds_ta_cntrct_template`, `cds_ta_care_description`, `dwtk_meldungen`) to allow for successful data processing.
    *   Ensure legacy `ta_cntrct_templ` is empty.
2.  **Migrated:**
    *   Populate BigQuery source tables (`cds_ta_cntrct_template`, `cds_ta_care_description`, `dwtk_meldungen`) *identically* to the legacy setup.
    *   Ensure `project.dataset.ta_cntrct_templ` is empty.
    *   Ensure `project.dataset.job_table` and `project.dataset.job_error_log` are empty.

**Action:**
1.  **Legacy:** Execute the KornShell script twice with the same parameters:
    ```bash
    k_ausd_v_ta_cntrct_templ.ksh -j "TEST_JOB_RERUN" -f 1008
    k_ausd_v_ta_cntrct_templ.ksh -j "TEST_JOB_RERUN" -f 1008
    ```
2.  **Migrated:** Trigger the Cloud Composer DAG `k_ausd_v_ta_cntrct_templ_dag` twice with the same parameters:
    ```json
    {
      "job_kennung": "TEST_JOB_RERUN",
      "eintragsnr": 1008
    }
    ```

**Pass/Fail Criterion:**
*   **Output Parity (Data):** After both runs, the content of `project.dataset.ta_cntrct_templ` must be *identical* to the content of the legacy `ta_cntrct_templ` table. The `TRUNCATE` operation ensures idempotency for the target data.
    *   **SQL Assertion (BigQuery):**
        ```sql
        SELECT COUNT(*) FROM project.dataset.ta_cntrct_templ;
        -- Expected: <count from legacy ta_cntrct_templ after two runs>

        SELECT CNTRCT_TEMPLATE_ID, CDS_DESCRIPTION_ID, CDS_DESCRIPTION
        FROM project.dataset.ta_cntrct_templ
        ORDER BY CNTRCT_TEMPLATE_ID;
        -- Expected: <exact data set from legacy ta_cntrct_templ after two runs>
        ```
*   **Transformation Correctness (Job Table):**
    *   `project.dataset.job_table` must contain four entries for `job_kennung = 'TEST_JOB_RERUN'` and `eintragsnr = 1008`: two 'STARTED' and two 'FINISHED' entries, reflecting the two distinct runs. The `records_processed` for both 'FINISHED' entries should be the same.
    *   The `DEACTIVATED` logic should not apply here as `eintragsnr` is the same.
    *   **SQL Assertion (BigQuery):**
        ```sql
        SELECT job_kennung, eintragsnr, status, records_processed
        FROM project.dataset.job_table
        WHERE job_kennung = 'TEST_JOB_RERUN' AND eintragsnr = 1008
        ORDER BY start_time;
        -- Expected:
        -- job_kennung   | eintragsnr | status    | records_processed
        -- --------------|------------|-----------|------------------
        -- TEST_JOB_RERUN | 1008       | STARTED   | NULL
        -- TEST_JOB_RERUN | 1008       | FINISHED  | <count from ta_cntrct_templ>
        -- TEST_JOB_RERUN | 1008       | STARTED   | NULL
        -- TEST_JOB_RERUN | 1008       | FINISHED  | <count from ta_cntrct_templ>
        ```

---