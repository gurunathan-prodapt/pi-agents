As a senior data-migration QA engineer, I have prepared a comprehensive suite of migration validation tests for the `k_ausd_bp_ta_bpr_apn.ksh` job. These tests are designed to ensure the migrated BigQuery components and Airflow orchestration are behaviourally equivalent to the legacy KornShell script.

**General Prerequisites for All Tests:**

*   All BigQuery DDLs provided in the "GENERATED MIGRATION CODE" section (for `dwtk_meldungen`, `sof_ta_bpr_instance`, `sof_ta_apn_carmen`, `sof_ta_bpr_apn`, `job_log`, `error_log`, `cibasis_data24`, `cibasis_data96`, `cibasis_fax`, `cibasisprodukt`) must be executed in `your_project_id.your_dataset_id`.
*   The BigQuery Stored Procedures `sp_d_ausd_bp_ta_bpr_apn` and `sp_k_ausd_bp_ta_bpr_apn` must be created in `your_project_id.your_dataset_id`.
*   The Airflow DAG `k_ausd_bp_ta_bpr_apn_migration_dag` must be deployed to Cloud Composer.
*   The `google_cloud_default` Airflow connection must be configured correctly to access BigQuery.
*   For output parity tests, a baseline of the legacy system's output (`SOF$TA_BPR_APN` table content and `cibasisprodukt.csv` if applicable) for specific input parameters must be available.
*   For data comparison, it is assumed that the source data from Oracle (`DWTK_MELDUNGEN`, `SOF$TA_BPR_INSTANCE`, `SOF$TA_APN_CARMEN`) has been ingested into the corresponding BigQuery tables (`dwtk_meldungen`, `sof_ta_bpr_instance`, `sof_ta_apn_carmen`) with identical content.

---

### Test Case 1: Parameter Validation - Missing Required Parameters

*   **Purpose**: Verify that the `sp_k_ausd_bp_ta_bpr_apn` stored procedure correctly identifies and raises an error when a required parameter (`p_JobKennung`, `p_EintragsNr`, or `p_Stichtag`) is missing or empty, mirroring the `pruefeParameterGesetzt` functionality.
*   **Setup**:
    1.  Ensure `job_log` and `error_log` tables are empty.
    2.  Prepare a call to `sp_k_ausd_bp_ta_bpr_apn` with one or more required parameters intentionally omitted or set to an empty string.
*   **Action**:
    Execute the BigQuery stored procedure call via a BigQuery client or an Airflow task (configured to pass invalid parameters).

    ```sql
    -- Attempt 1: Missing p_JobKennung
    CALL `your_project_id.your_dataset_id.sp_k_ausd_bp_ta_bpr_apn`(
        p_JobKennung => '',
        p_EintragsNr => 'TEST_ENTRY_001',
        p_Stichtag => '01012023',
        p_wiederanlaufWert => 0
    );

    -- Attempt 2: Missing p_EintragsNr
    CALL `your_project_id.your_dataset_id.sp_k_ausd_bp_ta_bpr_apn`(
        p_JobKennung => 'TEST_JOB',
        p_EintragsNr => NULL,
        p_Stichtag => '01012023',
        p_wiederanlaufWert => 0
    );

    -- Attempt 3: Missing p_Stichtag
    CALL `your_project_id.your_dataset_id.sp_k_ausd_bp_ta_bpr_apn`(
        p_JobKennung => 'TEST_JOB',
        p_EintragsNr => 'TEST_ENTRY_001',
        p_Stichtag => '',
        p_wiederanlaufWert => 0
    );
    ```
*   **Pass/Fail Criterion**:
    *   **Pass**: Each execution attempt results in a BigQuery error (e.g., `RAISE` statement) with a message indicating the missing parameter (e.g., "Parameter p_JobKennung (j) is required."). The `job_log` table should contain an entry for the failed run with `status = 'FAILED'` and an appropriate error message. The `error_log` table should contain a corresponding entry.
    *   **Fail**: The procedure executes without raising an error, or the error message is incorrect, or logging is incomplete/incorrect.

---

### Test Case 2: Parameter Validation - Invalid Stichtag Format

*   **Purpose**: Verify that `sp_k_ausd_bp_ta_bpr_apn` correctly identifies and raises an error for an invalid `p_Stichtag` format, mirroring the `DWDate_Datum_Check` functionality.
*   **Setup**:
    1.  Ensure `job_log` and `error_log` tables are empty.
    2.  Prepare a call to `sp_k_ausd_bp_ta_bpr_apn` with `p_Stichtag` in an incorrect format (e.g., `YYYY-MM-DD`, `DD/MM/YYYY`, or non-date string).
*   **Action**:
    Execute the BigQuery stored procedure call.

    ```sql
    CALL `your_project_id.your_dataset_id.sp_k_ausd_bp_ta_bpr_apn`(
        p_JobKennung => 'TEST_JOB',
        p_EintragsNr => 'TEST_ENTRY_002',
        p_Stichtag => '2023-01-01', -- Invalid format
        p_wiederanlaufWert => 0
    );

    CALL `your_project_id.your_dataset_id.sp_k_ausd_bp_ta_bpr_apn`(
        p_JobKennung => 'TEST_JOB',
        p_EintragsNr => 'TEST_ENTRY_003',
        p_Stichtag => 'NOT_A_DATE', -- Invalid format
        p_wiederanlaufWert => 0
    );
    ```
*   **Pass/Fail Criterion**:
    *   **Pass**: Each execution attempt results in a BigQuery error with a message indicating the invalid date format (e.g., "Parameter p_Stichtag (s) has an invalid date format. Expected DDMMYYYY."). The `job_log` table should contain an entry for the failed run with `status = 'FAILED'` and an appropriate error message. The `error_log` table should contain a corresponding entry.
    *   **Fail**: The procedure executes without raising an error, or the error message is incorrect, or logging is incomplete/incorrect.

---

### Test Case 3: Successful Execution and Output Parity (Core Logic)

*   **Purpose**: Verify that with valid inputs, the migrated BigQuery stored procedures (`sp_k_ausd_bp_ta_bpr_apn` and `sp_d_ausd_bp_ta_bpr_apn`) execute successfully, produce the exact same output in the `sof_ta_bpr_apn` table as the legacy system, and log correctly.
*   **Setup**:
    1.  Populate `your_project_id.your_dataset_id.dwtk_meldungen`, `sof_ta_bpr_instance`, and `sof_ta_apn_carmen` with a representative dataset that mirrors the legacy Oracle source data for a specific run.
    2.  Obtain the baseline output from the legacy `SOF$TA_BPR_APN` table for the same input data and parameters. Store this in a temporary BigQuery table (e.g., `sof_ta_bpr_apn_legacy_baseline`).
    3.  Ensure `sof_ta_bpr_apn`, `job_log`, and `error_log` tables are empty.
*   **Action**:
    Execute the main orchestration stored procedure with valid parameters.

    ```sql
    CALL `your_project_id.your_dataset_id.sp_k_ausd_bp_ta_bpr_apn`(
        p_JobKennung => 'PROD_JOB_001',
        p_EintragsNr => 'PROD_ENTRY_001',
        p_Stichtag => '15032023',
        p_wiederanlaufWert => 0
    );
    ```
*   **Pass/Fail Criterion**:
    *   **Pass**:
        1.  The `sp_k_ausd_bp_ta_bpr_apn` call completes successfully without error.
        2.  The `job_log` table contains a `SUCCESS` entry for this run, with `record_count` matching the number of rows inserted into `sof_ta_bpr_apn`.
        3.  The `error_log` table is empty for this run.
        4.  The content of `your_project_id.your_dataset_id.sof_ta_bpr_apn` is identical to `sof_ta_bpr_apn_legacy_baseline`. This can be verified using a `MINUS` or `EXCEPT` query:
            ```sql
            -- Check for rows in new table not in legacy baseline
            SELECT 'Only in new' AS source, * FROM `your_project_id.your_dataset_id.sof_ta_bpr_apn`
            EXCEPT DISTINCT
            SELECT 'Only in new' AS source, * FROM `your_project_id.your_dataset_id.sof_ta_bpr_apn_legacy_baseline`;

            -- Check for rows in legacy baseline not in new table
            SELECT 'Only in legacy' AS source, * FROM `your_project_id.your_dataset_id.sof_ta_bpr_apn_legacy_baseline`
            EXCEPT DISTINCT
            SELECT 'Only in legacy' AS source, * FROM `your_project_id.your_dataset_id.sof_ta_bpr_apn`;
            ```
            Both queries should return 0 rows.
    *   **Fail**: Any of the above conditions are not met.

---

### Test Case 4: Transformation Correctness - `DISTINCT` and `JOIN` Logic

*   **Purpose**: Verify that the `DISTINCT` clause and `INNER JOIN` logic in `sp_d_ausd_bp_ta_bpr_apn` correctly handle duplicate source records and non-matching join keys, producing the expected unique output.
*   **Setup**:
    1.  Populate `sof_ta_bpr_instance` and `sof_ta_apn_carmen` with specific test data:
        *   `sof_ta_bpr_instance`: Include records with duplicate `cntrct_id_ref` values, and records where `bpr_id` is both in and out of the filter list.
        *   `sof_ta_apn_carmen`: Include records that match `cntrct_id_ref` from `sof_ta_bpr_instance`, and some that do not.
        *   Example Data:
            ```sql
            -- sof_ta_bpr_instance
            INSERT INTO `your_project_id.your_dataset_id.sof_ta_bpr_instance` (cntrct_id, bpr_id, cntrct_id_ref) VALUES
            ('C1', 2828, 'REF1'), -- Match, in filter
            ('C2', 2828, 'REF1'), -- Duplicate REF1, in filter
            ('C3', 2829, 'REF2'), -- Match, in filter
            ('C4', 1000, 'REF3'), -- Match, NOT in filter
            ('C5', 2830, 'REF4'), -- No match in apn_carmen, in filter
            ('C6', 2831, 'REF5'), -- Match, in filter
            ('C7', 2828, 'REF1'); -- Another duplicate REF1, in filter

            -- sof_ta_apn_carmen
            INSERT INTO `your_project_id.your_dataset_id.sof_ta_apn_carmen` (cntrct_id, access_point_name) VALUES
            ('REF1', 'APN_A'),
            ('REF2', 'APN_B'),
            ('REF5', 'APN_E'),
            ('REF_NOMATCH', 'APN_X');
            ```
    2.  Ensure `sof_ta_bpr_apn` is empty.
*   **Action**:
    Execute the main orchestration stored procedure with valid parameters.

    ```sql
    CALL `your_project_id.your_dataset_id.sp_k_ausd_bp_ta_bpr_apn`(
        p_JobKennung => 'TEST_JOB_DISTINCT',
        p_EintragsNr => 'TEST_ENTRY_DISTINCT',
        p_Stichtag => '01012023',
        p_wiederanlaufWert => 0
    );
    ```
*   **Pass/Fail Criterion**:
    *   **Pass**: The `sof_ta_bpr_apn` table contains exactly the following distinct records:
        ```
        CNTRCT_ID | BPR_ID | CNTRCT_ID_REF | ACCESS_POINT_NAME
        ----------|--------|---------------|------------------
        C1        | 2828   | REF1          | APN_A
        C2        | 2828   | REF1          | APN_A
        C3        | 2829   | REF2          | APN_B
        C6        | 2831   | REF5          | APN_E
        ```
        (Note: The `DISTINCT` in the `SELECT` applies to the *entire row* of the final output, not just the join keys. So, if `cntrct_id` is different, even with same `bpr_id` and `cntrct_id_ref`, it will be a distinct row. The example output reflects this. If the original SQL meant `DISTINCT ON (cntrct_id_ref, bpr_id)`, the interpretation would change. Based on `SELECT DISTINCT bp.cntrct_id, bp.bpr_id, bp.cntrct_id_ref, ap.access_point_name`, the `cntrct_id` from `bp` is part of the distinctness.)
        The `ROW_COUNT()` should be 4.
    *   **Fail**: The output table contains more or fewer rows, or the content of the rows is incorrect (e.g., duplicates where there shouldn't be, missing records, incorrect `ACCESS_POINT_NAME`).

---

### Test Case 5: Transformation Correctness - `WHERE` Clause Filtering

*   **Purpose**: Verify that the `WHERE bp.bpr_id IN (2828, 2829, ..., 3000)` clause in `sp_d_ausd_bp_ta_bpr_apn` correctly filters records based on the `bpr_id` values.
*   **Setup**:
    1.  Populate `sof_ta_bpr_instance` and `sof_ta_apn_carmen` with data where `bpr_id` values are both within and outside the specified `IN` list, and ensure all `cntrct_id_ref` values have matches in `sof_ta_apn_carmen`.
        *   Example Data:
            ```sql
            -- sof_ta_bpr_instance
            INSERT INTO `your_project_id.your_dataset_id.sof_ta_bpr_instance` (cntrct_id, bpr_id, cntrct_id_ref) VALUES
            ('C_IN_1', 2828, 'REF_IN_1'),
            ('C_IN_2', 2999, 'REF_IN_2'),
            ('C_OUT_1', 1000, 'REF_OUT_1'), -- Should be filtered out
            ('C_OUT_2', 5000, 'REF_OUT_2'); -- Should be filtered out

            -- sof_ta_apn_carmen
            INSERT INTO `your_project_id.your_dataset_id.sof_ta_apn_carmen` (cntrct_id, access_point_name) VALUES
            ('REF_IN_1', 'APN_IN_1'),
            ('REF_IN_2', 'APN_IN_2'),
            ('REF_OUT_1', 'APN_OUT_1'),
            ('REF_OUT_2', 'APN_OUT_2');
            ```
    2.  Ensure `sof_ta_bpr_apn` is empty.
*   **Action**:
    Execute the main orchestration stored procedure.

    ```sql
    CALL `your_project_id.your_dataset_id.sp_k_ausd_bp_ta_bpr_apn`(
        p_JobKennung => 'TEST_JOB_FILTER',
        p_EintragsNr => 'TEST_ENTRY_FILTER',
        p_Stichtag => '01012023',
        p_wiederanlaufWert => 0
    );
    ```
*   **Pass/Fail Criterion**:
    *   **Pass**: The `sof_ta_bpr_apn` table contains only records where `bpr_id` is one of `2828, 2999`. Specifically:
        ```
        CNTRCT_ID | BPR_ID | CNTRCT_ID_REF | ACCESS_POINT_NAME
        ----------|--------|---------------|------------------
        C_IN_1    | 2828   | REF_IN_1      | APN_IN_1
        C_IN_2    | 2999   | REF_IN_2      | APN_IN_2
        ```
        The `ROW_COUNT()` should be 2.
    *   **Fail**: Records with `bpr_id` outside the `IN` list are present, or expected records are missing.

---

### Test Case 6: NULL Handling in Core Logic

*   **Purpose**: Verify how `sp_d_ausd_bp_ta_bpr_apn` handles `NULL` values in the join key (`cntrct_id_ref`) from `sof_ta_bpr_instance`.
*   **Setup**:
    1.  Populate `sof_ta_bpr_instance` with records where `cntrct_id_ref` is `NULL`, and other records with valid join keys.
        *   Example Data:
            ```sql
            -- sof_ta_bpr_instance
            INSERT INTO `your_project_id.your_dataset_id.sof_ta_bpr_instance` (cntrct_id, bpr_id, cntrct_id_ref) VALUES
            ('C_NULL_REF', 2828, NULL), -- Should not join
            ('C_VALID_REF', 2829, 'REF_VALID'); -- Should join

            -- sof_ta_apn_carmen
            INSERT INTO `your_project_id.your_dataset_id.sof_ta_apn_carmen` (cntrct_id, access_point_name) VALUES
            ('REF_VALID', 'APN_VALID');
            ```
    2.  Ensure `sof_ta_bpr_apn` is empty.
*   **Action**:
    Execute the main orchestration stored procedure.

    ```sql
    CALL `your_project_id.your_dataset_id.sp_k_ausd_bp_ta_bpr_apn`(
        p_JobKennung => 'TEST_JOB_NULL',
        p_EintragsNr => 'TEST_ENTRY_NULL',
        p_Stichtag => '01012023',
        p_wiederanlaufWert => 0
    );
    ```
*   **Pass/Fail Criterion**:
    *   **Pass**: The `sof_ta_bpr_apn` table contains only the record with a valid join key, as `INNER JOIN` implicitly excludes `NULL` values.
        ```
        CNTRCT_ID   | BPR_ID | CNTRCT_ID_REF | ACCESS_POINT_NAME
        ------------|--------|---------------|------------------
        C_VALID_REF | 2829   | REF_VALID     | APN_VALID
        ```
        The `ROW_COUNT()` should be 1.
    *   **Fail**: Records with `NULL` `cntrct_id_ref` are present in the output, or the expected valid record is missing.

---

### Test Case 7: Record Count and Job Logging

*   **Purpose**: Verify that the `v_records_processed` output from `sp_d_ausd_bp_ta_bpr_apn` and the `record_count` in the `job_log` table accurately reflect the number of records inserted into `sof_ta_bpr_apn`. This replaces the temporary file (`tmpFile`) mechanism.
*   **Setup**:
    1.  Populate `sof_ta_bpr_instance` and `sof_ta_apn_carmen` with a known number of records that will satisfy the join and filter conditions (e.g., 5 records).
    2.  Ensure `sof_ta_bpr_apn`, `job_log`, and `error_log` tables are empty.
*   **Action**:
    Execute the main orchestration stored procedure.

    ```sql
    CALL `your_project_id.your_dataset_id.sp_k_ausd_bp_ta_bpr_apn`(
        p_JobKennung => 'TEST_JOB_COUNT',
        p_EintragsNr => 'TEST_ENTRY_COUNT',
        p_Stichtag => '01012023',
        p_wiederanlaufWert => 0
    );
    ```
*   **Pass/Fail Criterion**:
    *   **Pass**:
        1.  The `sp_k_ausd_bp_ta_bpr_apn` call completes successfully.
        2.  The `job_log` table contains a `SUCCESS` entry for this run.
        3.  The `record_count` field in the `job_log` entry matches the actual number of rows in `your_project_id.your_dataset_id.sof_ta_bpr_apn` (e.g., 5).
        4.  The `message` field in `job_log` should reflect the processed count.
    *   **Fail**: The `record_count` in `job_log` does not match the actual number of rows inserted, or the job log entry is missing/incorrect.

---

### Test Case 8: Error Logging Mechanism

*   **Purpose**: Verify that any unhandled errors during the execution of `sp_k_ausd_bp_ta_bpr_apn` (or its called sub-procedures) are correctly captured and logged in the `error_log` table, and the `job_log` status is updated to `FAILED`. This replaces `DWMSG_MeldeFehler`.
*   **Setup**:
    1.  Create a scenario that will cause an error *after* initial parameter validation but *before* or during the core SQL execution. For example, temporarily revoke `INSERT` permissions on `sof_ta_bpr_apn` for the service account running the procedure, or introduce a deliberate syntax error in `sp_d_ausd_bp_ta_bpr_apn` (for testing purposes only, then revert).
    2.  Ensure `job_log` and `error_log` tables are empty.
*   **Action**:
    Execute the main orchestration stored procedure, triggering the error condition.

    ```sql
    -- Example: Assuming a deliberate error in sp_d_ausd_bp_ta_bpr_apn or permission issue
    CALL `your_project_id.your_dataset_id.sp_k_ausd_bp_ta_bpr_apn`(
        p_JobKennung => 'TEST_JOB_ERROR',
        p_EintragsNr => 'TEST_ENTRY_ERROR',
        p_Stichtag => '01012023',
        p_wiederanlaufWert => 0
    );
    ```
*   **Pass/Fail Criterion**:
    *   **Pass**:
        1.  The `sp_k_ausd_bp_ta_bpr_apn` call terminates with a BigQuery error.
        2.  The `job_log` table contains an entry for this run with `status = 'FAILED'`, `end_time` populated, and `message` containing the error details.
        3.  The `error_log` table contains a corresponding entry with `job_name`, `entry_number`, `error_message`, and `error_timestamp` populated.
    *   **Fail**: The error is not caught, the `job_log` status is not `FAILED`, or the `error_log` entry is missing or incomplete.

---

### Test Case 9: Commented Post-processing Logic - Output Parity (`cibasisprodukt`)

*   **Purpose**: If the commented post-processing logic is activated, verify that the `cibasisprodukt` table generated by `cibasisprodukt_processor.sql` matches the legacy `cibasisprodukt.csv` output.
*   **Setup**:
    1.  Populate `your_project_id.your_dataset_id.cibasis_data24`, `cibasis_data96`, and `cibasis_fax` with data that precisely mirrors the intermediate files (`cibasis_data24.dat`, `cibasis_data96.dat`, `cibasis_fax.dat`) used in a specific legacy run.
    2.  Obtain the baseline `cibasisprodukt.csv` generated by the legacy script for this input data. Load this into a temporary BigQuery table (e.g., `cibasisprodukt_legacy_baseline`).
    3.  Ensure `your_project_id.your_dataset_id.cibasisprodukt` is empty.
*   **Action**:
    Execute the `cibasisprodukt_processor.sql` script (e.g., via a `BigQueryInsertJobOperator` in Airflow, or directly in BigQuery).

    ```sql
    -- This would be part of the Airflow DAG or a direct BigQuery script execution
    -- (as provided in the generated code for `cibasisprodukt_processor.sql`)
    -- Example:
    -- CREATE OR REPLACE TEMPORARY TABLE `cibasis_data24_cleaned` AS ...;
    -- ...
    -- CREATE OR REPLACE TABLE `your_project_id.your_dataset_id.cibasisprodukt` AS ...;
    ```
*   **Pass/Fail Criterion**:
    *   **Pass**: The content of `your_project_id.your_dataset_id.cibasisprodukt` is identical to `cibasisprodukt_legacy_baseline`.
        ```sql
        -- Check for rows in new table not in legacy baseline
        SELECT 'Only in new' AS source, * FROM `your_project_id.your_dataset_id.cibasisprodukt`
        EXCEPT DISTINCT
        SELECT 'Only in new' AS source, * FROM `your_project_id.your_dataset_id.cibasisprodukt_legacy_baseline`;

        -- Check for rows in legacy baseline not in new table
        SELECT 'Only in legacy' AS source, * FROM `your_project_id.your_dataset_id.cibasisprodukt_legacy_baseline`
        EXCEPT DISTINCT
        SELECT 'Only in legacy' AS source, * FROM `your_project_id.your_dataset_id.cibasisprodukt`;
        ```
        Both queries should return 0 rows.
    *   **Fail**: The output table differs from the legacy baseline.

---

### Test Case 10: Commented Post-processing Logic - `REPLACE`, `DISTINCT`, `JOIN` Transformations

*   **Purpose**: Verify the specific transformations (`REPLACE`, `DISTINCT`, `FULL OUTER JOIN`, `LEFT OUTER JOIN`) within `cibasisprodukt_processor.sql` function as expected.
*   **Setup**:
    1.  Populate `cibasis_data24`, `cibasis_data96`, and `cibasis_fax` with test data designed to exercise each transformation:
        *   `cibasis_data24`: Records with leading/trailing spaces, internal spaces, and duplicates.
        *   `cibasis_data96`: Records with spaces, duplicates, and keys that match/don't match `cibasis_data24`.
        *   `cibasis_fax`: Records with spaces, and keys that match/don't match the result of the first join.
        *   Example Data:
            ```sql
            -- cibasis_data24
            INSERT INTO `your_project_id.your_dataset_id.cibasis_data24` (id, data_field_24_1, data_field_24_2) VALUES
            (' ID1', 'Data A ', 'X'),
            ('ID2', 'Data B', 'Y'),
            (' ID1', 'Data A ', 'X'), -- Duplicate
            ('ID3', 'Data C', 'Z'); -- No match in data96

            -- cibasis_data96
            INSERT INTO `your_project_id.your_dataset_id.cibasis_data96` (id, data_field_96_1, data_field_96_2) VALUES
            ('ID1', 'Info 1', 'P'),
            ('ID2', 'Info 2', 'Q'),
            ('ID4', 'Info 4', 'R'); -- No match in data24

            -- cibasis_fax
            INSERT INTO `your_project_id.your_dataset_id.cibasis_fax` (id, fax_data) VALUES
            ('ID1', 'Fax 1'),
            ('ID2', 'Fax 2'),
            ('ID5', 'Fax 5'); -- No match in combined tmp
            ```
    2.  Ensure `your_project_id.your_dataset_id.cibasisprodukt` is empty.
*   **Action**:
    Execute the `cibasisprodukt_processor.sql` script.
*   **Pass/Fail Criterion**:
    *   **Pass**: The `cibasisprodukt` table contains the following records, demonstrating correct space removal, distinctness, and join logic:
        ```
        id  | data_field_24_1 | data_field_24_2 | data_field_96_1 | data_field_96_2 | fax_data
        ----|-----------------|-----------------|-----------------|-----------------|---------
        ID1 | DataA           | X               | Info1           | P               | Fax1
        ID2 | DataB           | Y               | Info2           | Q               | Fax2
        ID3 | DataC           | Z               | NULL            | NULL            | NULL
        ID4 | NULL            | NULL            | Info4           | R               | NULL
        ```
        (Note: `ID1` and `ID2` from `cibasis_data24` and `cibasis_data96` are joined. `ID3` from `cibasis_data24` is kept due to `FULL OUTER JOIN`. `ID4` from `cibasis_data96` is kept due to `FULL OUTER JOIN`. `ID5` from `cibasis_fax` is not included because the final join is `LEFT OUTER JOIN` from `cibasis_24_96_tmp` to `cibasis_fax_cleaned`.)
    *   **Fail**: The output table contains incorrect data, indicating issues with `REPLACE`, `DISTINCT`, or `JOIN` operations.

---

### Test Case 11: Airflow DAG Orchestration

*   **Purpose**: Verify that the `k_ausd_bp_ta_bpr_apn_migration_dag` in Airflow successfully triggers the BigQuery Stored Procedures with correct parameters and manages task dependencies.
*   **Setup**:
    1.  Ensure the Airflow DAG is deployed and visible in the Airflow UI.
    2.  Ensure `google_cloud_default` connection is configured.
    3.  Populate source BigQuery tables (`sof_ta_bpr_instance`, `sof_ta_apn_carmen`) with valid test data.
    4.  Ensure target tables (`sof_ta_bpr_apn`, `job_log`, `error_log`, `cibasisprodukt` if post-processing is enabled) are empty.
*   **Action**:
    Trigger the `k_ausd_bp_ta_bpr_apn_migration_dag` manually or allow it to run on its schedule in the Airflow UI.
*   **Pass/Fail Criterion**:
    *   **Pass**:
        1.  The Airflow DAG run completes successfully with all tasks (`start_task`, `call_main_orchestration_sp`, `run_cibasisprodukt_post_processing` (if enabled), `end_task`) marked as `success`.
        2.  The `job_log` table contains a `SUCCESS` entry corresponding to the DAG run.
        3.  The `sof_ta_bpr_apn` table is populated with data as expected from the core logic.
        4.  If post-processing is enabled, the `cibasisprodukt` table is populated correctly.
        5.  No errors are reported in Airflow logs or the BigQuery `error_log` table.
    *   **Fail**: The DAG run fails, one or more tasks fail, parameters are not passed correctly, or the BigQuery tables are not populated as expected.

---