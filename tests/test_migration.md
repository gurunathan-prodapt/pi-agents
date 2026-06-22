The migration of `k_ausd_bp_ta_rn_einzeln.ksh` to a BigQuery Stored Procedure `r_ausd_bp_ta_rn_einzeln` involves significant architectural and technological changes. The following test cases are designed to validate the behavioral equivalence and correctness of the migrated solution across various aspects.

---

## Migration Validation Tests for `r_ausd_bp_ta_rn_einzeln`

### 1. Orchestration & Parameter Handling Tests

#### Test Case 1.1: Successful Execution with Valid Parameters and Restart Value
*   **Purpose:** Verify the stored procedure executes successfully with all mandatory parameters and an optional restart value, and correctly logs its status and record count.
*   **Setup:**
    1.  Create and populate `project.dataset.sof_ta_bpr_basis` and `project.dataset.sof_ta_msisdn` with diverse sample data that will result in a non-zero record count in the target table.
    2.  Ensure `project.dataset.sof_ta_rn_einzeln` is empty before execution.
    3.  Clear `project.dataset.error_log` and `project.dataset.job_table`.
*   **Action:** Execute the BigQuery Stored Procedure:
    ```sql
    CALL `project.dataset.r_ausd_bp_ta_rn_einzeln`(
        p_JobKennung => 'TEST_JOB_001',
        p_EintragsNr => 'ENTRY_001',
        p_Stichtag => '01012023', -- Example key date
        p_wiederanlaufWert => '1'  -- Simulate a restart
    );
    ```
*   **Pass/Fail Criteria:**
    *   The stored procedure completes successfully without raising any unhandled errors.
    *   `project.dataset.sof_ta_rn_einzeln` contains the expected number of records (e.g., > 0).
    *   `project.dataset.error_log` is empty.
    *   `project.dataset.job_table` contains two entries for `job_kennung = 'TEST_JOB_001'` and `eintrags_nr = 'ENTRY_001'`:
        *   One entry with `status_a = 'START'`, `status_i = 'RUNNING'`.
        *   One entry with `status_a = 'COMPLETED'`, `status_i = 'SUCCESS'`, `record_count` matching `SELECT COUNT(*) FROM project.dataset.sof_ta_rn_einzeln`, `stichtag_from = '2023-01-01'`, `stichtag_to = '2023-01-01'`, and `restart_flag = TRUE`.

#### Test Case 1.2: Successful Execution without Restart Value
*   **Purpose:** Verify the stored procedure executes successfully when the `p_wiederanlaufWert` is not provided (NULL or empty), and logs `restart_flag` correctly.
*   **Setup:** Same as Test Case 1.1.
*   **Action:** Execute the BigQuery Stored Procedure:
    ```sql
    CALL `project.dataset.r_ausd_bp_ta_rn_einzeln`(
        p_JobKennung => 'TEST_JOB_002',
        p_EintragsNr => 'ENTRY_002',
        p_Stichtag => '15062023',
        p_wiederanlaufWert => NULL -- No restart value
    );
    ```
*   **Pass/Fail Criteria:**
    *   The stored procedure completes successfully.
    *   `project.dataset.job_table` contains a 'COMPLETED' entry for `TEST_JOB_002` with `restart_flag = FALSE`.
    *   Other conditions as in Test Case 1.1.

#### Test Case 1.3: Missing Mandatory Parameter `p_JobKennung`
*   **Purpose:** Verify error handling when `p_JobKennung` is missing or empty.
*   **Setup:** Clear `project.dataset.error_log` and `project.dataset.job_table`.
*   **Action:** Execute the BigQuery Stored Procedure with `p_JobKennung` as NULL:
    ```sql
    CALL `project.dataset.r_ausd_bp_ta_rn_einzeln`(
        p_JobKennung => NULL,
        p_EintragsNr => 'ENTRY_003',
        p_Stichtag => '01012023',
        p_wiederanlaufWert => '0'
    );
    ```
*   **Pass/Fail Criteria:**
    *   The stored procedure terminates with an error message containing "p_JobKennung is mandatory".
    *   `project.dataset.error_log` remains empty.
    *   `project.dataset.job_table` contains only the initial 'START' entry for `ENTRY_003` (if `p_JobKennung` was not NULL but empty string), or no entry at all (if `p_JobKennung` was NULL and the `INSERT` for 'START' failed).
    *   **Note on Behavioral Discrepancy:** The legacy script would log this error using `DWMSG_MeldeFehler` and then exit. The migrated BigQuery SP, as written, will `RAISE` an error *before* the `EXCEPTION` block, meaning no entry will be made in `error_log` and the `job_table` will not be updated to 'FAILED'. This is a behavioral difference that should be acknowledged.

#### Test Case 1.4: Missing Mandatory Parameter `p_Stichtag`
*   **Purpose:** Verify error handling when `p_Stichtag` is missing or empty.
*   **Setup:** Clear `project.dataset.error_log` and `project.dataset.job_table`.
*   **Action:** Execute the BigQuery Stored Procedure with `p_Stichtag` as NULL:
    ```sql
    CALL `project.dataset.r_ausd_bp_ta_rn_einzeln`(
        p_JobKennung => 'TEST_JOB_004',
        p_EintragsNr => 'ENTRY_004',
        p_Stichtag => NULL,
        p_wiederanlaufWert => '0'
    );
    ```
*   **Pass/Fail Criteria:**
    *   The stored procedure terminates with an error message containing "p_Stichtag is mandatory".
    *   `project.dataset.error_log` remains empty.
    *   `project.dataset.job_table` contains only the initial 'START' entry for `TEST_JOB_004`.
    *   **Note on Behavioral Discrepancy:** Same as Test Case 1.3.

#### Test Case 1.5: Invalid `p_Stichtag` Format
*   **Purpose:** Verify date format validation (`DDMMYYYY`).
*   **Setup:** Clear `project.dataset.error_log` and `project.dataset.job_table`.
*   **Action:** Execute the BigQuery Stored Procedure with an incorrectly formatted `p_Stichtag`:
    ```sql
    CALL `project.dataset.r_ausd_bp_ta_rn_einzeln`(
        p_JobKennung => 'TEST_JOB_005',
        p_EintragsNr => 'ENTRY_005',
        p_Stichtag => '2023-01-01', -- Incorrect format (YYYY-MM-DD)
        p_wiederanlaufWert => '0'
    );
    ```
*   **Pass/Fail Criteria:**
    *   The stored procedure terminates with an error message containing "Invalid date format for p_Stichtag".
    *   `project.dataset.error_log` remains empty.
    *   `project.dataset.job_table` contains only the initial 'START' entry for `TEST_JOB_005`.
    *   **Note on Behavioral Discrepancy:** Same as Test Case 1.3.

### 2. Output Parity & Transformation Correctness Tests

#### Test Case 2.1: Output Parity - Full Data Comparison
*   **Purpose:** Prove that the migrated job produces an identical dataset in `project.dataset.sof_ta_rn_einzeln` compared to the legacy job's output for the same inputs.
*   **Setup:**
    1.  **Legacy Data Capture:**
        *   Identify a representative set of source data in the legacy Oracle environment for `PoolBasisprodukt` and related MSISDN tables.
        *   Run the legacy `k_ausd_bp_ta_rn_einzeln.ksh` script with a specific `p_Stichtag` (e.g., '01012023') and other parameters.
        *   Extract the full output data from the legacy target table (e.g., `sof_ta_rn_einzeln` or equivalent) into a CSV or a temporary BigQuery table (`legacy_sof_ta_rn_einzeln_output`).
    2.  **BigQuery Source Setup:**
        *   Replicate the *exact* source data identified in step 1 into `project.dataset.sof_ta_bpr_basis` and `project.dataset.sof_ta_msisdn` in BigQuery. Ensure data types and NULLability match.
    3.  Ensure `project.dataset.sof_ta_rn_einzeln` is empty.
*   **Action:** Execute the BigQuery Stored Procedure with the *exact same parameters* as the legacy run:
    ```sql
    CALL `project.dataset.r_ausd_bp_ta_rn_einzeln`(
        p_JobKennung => 'PARITY_TEST',
        p_EintragsNr => 'PARITY_ENTRY',
        p_Stichtag => '01012023', -- Use the same Stichtag as legacy run
        p_wiederanlaufWert => '0'
    );
    ```
*   **Pass/Fail Criteria:**
    *   The stored procedure completes successfully.
    *   The row count in `project.dataset.sof_ta_rn_einzeln` matches the row count in `legacy_sof_ta_rn_einzeln_output`.
    *   A full data comparison query yields zero differences. This can be done using `EXCEPT DISTINCT`:
        ```sql
        -- Check for rows in migrated output not in legacy output
        SELECT 'Only in Migrated' AS source, * FROM `project.dataset.sof_ta_rn_einzeln`
        EXCEPT DISTINCT
        SELECT 'Only in Migrated' AS source, * FROM `project.dataset.legacy_sof_ta_rn_einzeln_output`;

        -- Check for rows in legacy output not in migrated output
        SELECT 'Only in Legacy' AS source, * FROM `project.dataset.legacy_sof_ta_rn_einzeln_output`
        EXCEPT DISTINCT
        SELECT 'Only in Legacy' AS source, * FROM `project.dataset.sof_ta_rn_einzeln`;
        ```
        Both queries must return 0 rows.

#### Test Case 2.2: Transformation - `CASE` Logic for `bpr_id` and `callnumber_role_id`
*   **Purpose:** Verify the correctness of the complex `CASE` statements that determine output column values based on `bp.bpr_id`, `ms.callnumber_role_id`, and `bp.slave_number`.
*   **Setup:**
    1.  Populate `project.dataset.sof_ta_bpr_basis` and `project.dataset.sof_ta_msisdn` with specific test data covering:
        *   All `bpr_id` values mentioned in the SQL (31, 2759, 2800, 2835, 2836, 2837, 3848).
        *   All `callnumber_role_id` values mentioned (1, 2, 3, 5, 7, 8, 9, 12).
        *   Combinations of `bpr_id` and `callnumber_role_id` that should result in non-NULL values for specific output columns.
        *   Combinations that should result in NULL values (e.g., `bpr_id = 31` but `callnumber_role_id = 7`).
        *   For `bpr_id = 3848`, include `slave_number = 1` and `slave_number = 2`.
        *   Include `msisdn` values and `valid_to` dates that will trigger both 'L' and 'A' statuses.
    2.  Set `p_Stichtag` to a specific date (e.g., '01012023').
*   **Action:** Execute the BigQuery Stored Procedure:
    ```sql
    CALL `project.dataset.r_ausd_bp_ta_rn_einzeln`(
        p_JobKennung => 'TRANSFORM_TEST',
        p_EintragsNr => 'TRANSFORM_ENTRY',
        p_Stichtag => '01012023',
        p_wiederanlaufWert => '0'
    );
    ```
*   **Pass/Fail Criteria:**
    *   Query `project.dataset.sof_ta_rn_einzeln` and assert that each output column's value (e.g., `TN_MULTI_SINGLE`, `TN_TEL_MSISDN`, `TN_TEL_STATUS`, `MS_RN_1_MSISDN`, etc.) correctly reflects the logic defined in the `CASE` statements for each input row.
    *   Example assertion (using Python/pytest with BigQuery client):
        ```python
        # Example: Verify TN_MULTI_SINGLE for bpr_id=31, callnumber_role_id=1
        result = client.query("""
            SELECT TN_MULTI_SINGLE FROM `project.dataset.sof_ta_rn_einzeln`
            WHERE CNTRCT_ID = 'contract_id_for_bpr31_role1'
        """).to_dataframe()
        assert result['TN_MULTI_SINGLE'].iloc[0] == 'Singlenumbering'

        # Example: Verify MS_RN_1_MSISDN for bpr_id=3848, slave_number=1
        result = client.query("""
            SELECT MS_RN_1_MSISDN FROM `project.dataset.sof_ta_rn_einzeln`
            WHERE CNTRCT_ID = 'contract_id_for_bpr3848_slave1'
        """).to_dataframe()
        assert result['MS_RN_1_MSISDN'].iloc[0] == 'msisdn_value_for_slave1'

        # Example: Verify NULL handling
        result = client.query("""
            SELECT TN_MULTI_SINGLE FROM `project.dataset.sof_ta_rn_einzeln`
            WHERE CNTRCT_ID = 'contract_id_for_bpr_not_31'
        """).to_dataframe()
        assert pd.isna(result['TN_MULTI_SINGLE'].iloc[0])
        ```

#### Test Case 2.3: Date Comparison Logic for Status ('L'/'A')
*   **Purpose:** Verify the `ms.valid_to <= PARSE_DATE('%Y%m%d', _stichtag_yyyymmdd)` logic for determining 'L' (Legacy/Expired) or 'A' (Active) status.
*   **Setup:**
    1.  Populate `project.dataset.sof_ta_bpr_basis` with `bpr_id = 31`.
    2.  Populate `project.dataset.sof_ta_msisdn` with `callnumber_role_id` in (1, 2) and `ms.valid_to` values relative to a chosen `p_Stichtag` (e.g., '01012023'):
        *   `valid_to` date *before* `p_Stichtag` (e.g., '2022-12-31').
        *   `valid_to` date *equal to* `p_Stichtag` (e.g., '2023-01-01').
        *   `valid_to` date *after* `p_Stichtag` (e.g., '2023-01-02').
        *   `valid_to` date is `NULL`.
*   **Action:** Execute the BigQuery Stored Procedure with `p_Stichtag = '01012023'`.
*   **Pass/Fail Criteria:**
    *   For rows where `ms.valid_to < '2023-01-01'`, `TN_TEL_STATUS` is 'L'.
    *   For rows where `ms.valid_to = '2023-01-01'`, `TN_TEL_STATUS` is 'L'.
    *   For rows where `ms.valid_to > '2023-01-01'`, `TN_TEL_STATUS` is 'A'.
    *   For rows where `ms.valid_to IS NULL`, `TN_TEL_STATUS` is NULL.

#### Test Case 2.4: Join Conditions and Filtering
*   **Purpose:** Verify that the `JOIN` clause and the `AND` filters (`bp.bpr_id IN (...)` and `ms.callnumber_role_id IN (...)`) correctly filter and combine data from source tables.
*   **Setup:**
    1.  Populate `project.dataset.sof_ta_bpr_basis` and `project.dataset.sof_ta_msisdn` with data including:
        *   Rows that match all join and filter conditions (expected in output).
        *   Rows where `bp.bpr_instance_id` does not match `ms.bpr_instance_id` (should be excluded).
        *   Rows where `bp.bpr_id` is *not* in the specified list (e.g., `bpr_id = 9999`) but `bpr_instance_id` matches (should be excluded or result in all NULLs for relevant output columns).
        *   Rows where `ms.callnumber_role_id` is *not* in the specified list (e.g., `callnumber_role_id = 99`) but `bpr_instance_id` matches (should be excluded or result in all NULLs for relevant output columns).
*   **Action:** Execute the BigQuery Stored Procedure.
*   **Pass/Fail Criteria:**
    *   Only rows satisfying all `JOIN` and `AND` conditions contribute to the output.
    *   Rows that should be excluded due to non-matching join keys or filter conditions are not present in `project.dataset.sof_ta_rn_einzeln`.
    *   Rows that join but have `bpr_id` or `callnumber_role_id` outside the `CASE` statement conditions correctly produce `NULL` values for the respective output columns.

#### Test Case 2.5: Empty Source Tables
*   **Purpose:** Verify the job handles empty source tables gracefully.
*   **Setup:** Ensure `project.dataset.sof_ta_bpr_basis` and `project.dataset.sof_ta_msisdn` are empty.
*   **Action:** Execute the BigQuery Stored Procedure:
    ```sql
    CALL `project.dataset.r_ausd_bp_ta_rn_einzeln`(
        p_JobKennung => 'EMPTY_SOURCE_TEST',
        p_EintragsNr => 'EMPTY_ENTRY',
        p_Stichtag => '01012023',
        p_wiederanlaufWert => '0'
    );
    ```
*   **Pass/Fail Criteria:**
    *   The stored procedure completes successfully.
    *   `project.dataset.sof_ta_rn_einzeln` is empty.
    *   `project.dataset.job_table` contains a 'COMPLETED' entry with `record_count = 0`.

### 3. External-System Replacements (Logging & Auditing) Tests

#### Test Case 3.1: `job_table` and `error_log` for Core SQL Execution Failure
*   **Purpose:** Verify that errors occurring during the core SQL execution (e.g., table not found, data type mismatch) are correctly logged to `error_log` and `job_table`.
*   **Setup:**
    1.  Introduce a controlled error in the core SQL logic. For example, temporarily rename `project.dataset.sof_ta_msisdn` to `sof_ta_msisdn_backup` so the `JOIN` fails.
    2.  Clear `project.dataset.error_log` and `project.dataset.job_table`.
*   **Action:** Execute the BigQuery Stored Procedure with valid parameters:
    ```sql
    CALL `project.dataset.r_ausd_bp_ta_rn_einzeln`(
        p_JobKennung => 'SQL_FAIL_TEST',
        p_EintragsNr => 'SQL_FAIL_ENTRY',
        p_Stichtag => '01012023',
        p_wiederanlaufWert => '0'
    );
    ```
*   **Pass/Fail Criteria:**
    *   The stored procedure terminates with an error.
    *   `project.dataset.error_log` contains one entry with:
        *   `job_name = 'k_ausd_bp_ta_rn_einzeln'`
        *   `error_nr` (BigQuery's internal error code for the specific SQL error).
        *   `error_arg = 'CORE_SQL_EXECUTION'`.
        *   `message` containing details of the SQL error (e.g., "Not found: Table project.dataset.sof_ta_msisdn").
    *   `project.dataset.job_table` contains two entries:
        *   One 'START' entry.
        *   One 'FAILED' entry with `status_a = 'FAILED'`, `status_i = 'FAILED'`, and `record_count = 0`.
    *   `project.dataset.sof_ta_rn_einzeln` remains empty (due to `TRUNCATE` followed by failed `INSERT`).

### 4. Data Quality / Row-Count / Schema Assertions

#### Test Case 4.1: Row Count Accuracy
*   **Purpose:** Verify that the `record_count` logged in `project.dataset.job_table` accurately reflects the number of rows inserted into `project.dataset.sof_ta_rn_einzeln`.
*   **Setup:**
    1.  Populate `project.dataset.sof_ta_bpr_basis` and `project.dataset.sof_ta_msisdn` with data designed to produce a specific, known number of output rows (e.g., 100 rows).
    2.  Clear `project.dataset.job_table`.
*   **Action:** Execute the BigQuery Stored Procedure:
    ```sql
    CALL `project.dataset.r_ausd_bp_ta_rn_einzeln`(
        p_JobKennung => 'COUNT_TEST',
        p_EintragsNr => 'COUNT_ENTRY',
        p_Stichtag => '01012023',
        p_wiederanlaufWert => '0'
    );
    ```
*   **Pass/Fail Criteria:**
    *   The stored procedure completes successfully.
    *   `SELECT record_count FROM project.dataset.job_table WHERE job_kennung = 'COUNT_TEST' AND status_i = 'SUCCESS'` returns the expected number of rows (e.g., 100).
    *   `SELECT COUNT(*) FROM project.dataset.sof_ta_rn_einzeln` also returns the same expected number of rows.

#### Test Case 4.2: Target Table Schema Conformance
*   **Purpose:** Verify that the schema of the target table `project.dataset.sof_ta_rn_einzeln` matches the expected DDL and data types.
*   **Setup:** Ensure `project.dataset.sof_ta_rn_einzeln` has been created by the DDL script.
*   **Action:** Inspect the schema of `project.dataset.sof_ta_rn_einzeln` using BigQuery's `INFORMATION_SCHEMA`.
*   **Pass/Fail Criteria:**
    *   All columns listed in the `INSERT` statement of `d_ausd_bp_ta_rn_einzeln_core.sql` exist in `project.dataset.sof_ta_rn_einzeln`.
    *   The data types of these columns match the expected types:
        *   `CNTRCT_ID`: `STRING`
        *   `*_MULTI_SINGLE`: `STRING`
        *   `*_MSISDN`: `STRING`
        *   `*_STATUS`: `STRING`
        *   `*_VALID_TO`: `DATE`
    *   No unexpected columns are present.
    *   Example BigQuery SQL for schema assertion:
        ```sql
        SELECT column_name, data_type
        FROM `project.dataset.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'sof_ta_rn_einzeln'
        ORDER BY ordinal_position;
        ```
        Compare this output against the expected schema.

#### Test Case 4.3: NULL Handling in Output
*   **Purpose:** Verify that `NULL` values are correctly generated in the output table based on the `CASE` logic and source data.
*   **Setup:**
    1.  Populate `project.dataset.sof_ta_bpr_basis` and `project.dataset.sof_ta_msisdn` with data that will explicitly trigger `NULL` outcomes in the `CASE` statements (e.g., `bpr_id` not matching any `WHEN` clause for a specific output column, `ms.msisdn` being NULL in the source for a relevant `bpr_id`/`callnumber_role_id` combination).
    2.  Include rows where `ms.valid_to` is NULL.
*   **Action:** Execute the BigQuery Stored Procedure.
*   **Pass/Fail Criteria:**
    *   Query `project.dataset.sof_ta_rn_einzeln` and assert that `NULL` values appear exactly where expected according to the transformation logic.
    *   For example, if `bp.bpr_id` is 100 (not in the list), then all `TN_`, `TC_`, `TB_`, `DA_`, `VDA_`, `TK_`, `MS_` prefixed columns for that `CNTRCT_ID` should be `NULL`.
    *   If `ms.valid_to` is NULL for a row that otherwise matches, the corresponding `*_STATUS` should be NULL.

---