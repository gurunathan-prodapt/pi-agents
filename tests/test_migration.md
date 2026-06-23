The migration of `k_ausd_bp_ta_p_basisprod.ksh` to Google BigQuery involves translating KornShell orchestration logic and Oracle SQL data transformations. The following test cases are designed to ensure the migrated BigQuery Stored Procedure (`sp_k_ausd_bp_ta_p_basisprod`) is behaviourally equivalent to the legacy script.

**Assumptions:**
*   Access to both the legacy Oracle database and the BigQuery environment.
*   Ability to load controlled test data into both systems.
*   The `project.dataset` placeholders will be replaced with actual BigQuery project and dataset names.
*   The DDL for all source tables (`sof_ta_cntrct_dist`, `sof_ta_bcp_iccid`, `sof_ta_bcp_msisdn`, `sof_ta_cntrct_evn`, `sof_ta_iccid_vertrag`, `sof_ta_rn_vertrag`, `sof_ta_rn_da_vda_tk`, `sof_ta_tarifoption`, `sof_ta_apn_vertrag`) has been created in BigQuery and populated with data mirroring the Oracle source.
*   The `d_ausd_bp_ta_p_basisprod.sql` has been translated into the `EXECUTE IMMEDIATE` block within the BigQuery stored procedure.

---

### Test Case 1: Output Parity - Full Data Set Comparison

*   **Purpose:** To verify that the migrated BigQuery stored procedure produces an identical final output table (`sof_ta_p_basisprod`) compared to the legacy KornShell script for a comprehensive dataset. This is the "golden record" test.
*   **Setup:**
    1.  **Legacy Data:** Load a representative, production-like dataset into all source tables in the legacy Oracle environment (e.g., `sof$ta_cntrct_dist`, `sof$ta_bcp_iccid`, etc.).
    2.  **BigQuery Data:** Load an exact replica of the legacy source data into the corresponding BigQuery source tables (`project.dataset.sof_ta_cntrct_dist`, `project.dataset.sof_ta_bcp_iccid`, etc.).
    3.  **Parameters:** Define a set of valid input parameters (e.g., `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
*   **Action:**
    1.  Execute the legacy KornShell script (`k_ausd_bp_ta_p_basisprod.ksh`) with the defined parameters.
    2.  Execute the migrated BigQuery stored procedure (`project.dataset.sp_k_ausd_bp_ta_p_basisprod`) with the *exact same* parameters.
*   **Pass/Fail Criterion:**
    *   The number of rows in the target table `sof_ta_p_basisprod` in Oracle and BigQuery must be identical.
    *   Every column for every row in the BigQuery `sof_ta_p_basisprod` table must exactly match the corresponding column and row in the Oracle `sof$ta_p_basisprod` table.
    *   **Runnable Test Code (SQL/Python):**

    ```sql
    -- BigQuery: Count rows in the target table
    SELECT COUNT(*) FROM `project.dataset.sof_ta_p_basisprod`;

    -- Oracle (using SQL*Plus or similar client): Count rows in the target table
    SELECT COUNT(*) FROM sof$ta_p_basisprod;

    -- BigQuery: Compare content (assuming a temporary table 'oracle_sof_ta_p_basisprod_snapshot' holds Oracle's output)
    -- This requires exporting Oracle's output and loading it into a BigQuery temp table.
    -- Alternatively, a row-by-row comparison can be done if the number of rows is manageable.
    SELECT 'Mismatch in BigQuery only' AS issue, bq.*
    FROM `project.dataset.sof_ta_p_basisprod` bq
    LEFT JOIN `project.dataset.oracle_sof_ta_p_basisprod_snapshot` ora ON
        bq.CNTRCT_ID = ora.CNTRCT_ID AND -- Assuming CNTRCT_ID is a primary key or unique identifier
        bq.EVN = ora.EVN AND
        -- ... compare all other columns ...
        (bq.TNV_ICCID IS NOT DISTINCT FROM ora.TNV_ICCID) AND -- Use IS NOT DISTINCT FROM for NULL-safe comparison
        -- ...
        (bq.MS10_VALID IS NOT DISTINCT FROM ora.MS10_VALID)
    WHERE ora.CNTRCT_ID IS NULL
    UNION ALL
    SELECT 'Mismatch in Oracle only' AS issue, ora.*
    FROM `project.dataset.oracle_sof_ta_p_basisprod_snapshot` ora
    LEFT JOIN `project.dataset.sof_ta_p_basisprod` bq ON
        ora.CNTRCT_ID = bq.CNTRCT_ID AND
        ora.EVN = bq.EVN AND
        -- ... compare all other columns ...
        (ora.TNV_ICCID IS NOT DISTINCT FROM bq.TNV_ICCID) AND
        -- ...
        (ora.MS10_VALID IS NOT DISTINCT FROM bq.MS10_VALID)
    WHERE bq.CNTRCT_ID IS NULL
    UNION ALL
    SELECT 'Content Mismatch' AS issue, bq.*
    FROM `project.dataset.sof_ta_p_basisprod` bq
    JOIN `project.dataset.oracle_sof_ta_p_basisprod_snapshot` ora ON
        bq.CNTRCT_ID = ora.CNTRCT_ID AND
        bq.EVN = ora.EVN
    WHERE NOT (
        (bq.TNV_ICCID IS NOT DISTINCT FROM ora.TNV_ICCID) AND
        -- ... all other columns must match ...
        (bq.MS10_VALID IS NOT DISTINCT FROM ora.MS10_VALID)
    );
    -- The query should return 0 rows for a pass.
    ```

---

### Test Case 2: Parameter Validation - Missing `p_JobKennung`

*   **Purpose:** To verify that the BigQuery stored procedure correctly handles the scenario where the mandatory `p_JobKennung` parameter is missing or empty, raising an appropriate error.
*   **Setup:** None.
*   **Action:** Attempt to call the BigQuery stored procedure with `p_JobKennung` set to `NULL` or an empty string.
    ```sql
    CALL `project.dataset.sp_k_ausd_bp_ta_p_basisprod`(
      p_JobKennung => NULL,
      p_EintragsNr => 'TEST_ENTRY',
      p_Stichtag => '01012023',
      p_wiederanlaufWert => '0'
    );
    ```
*   **Pass/Fail Criterion:**
    *   The stored procedure execution must fail and raise an error.
    *   The error message should contain `'ERROR: p_JobKennung is not set.'`.
    *   An entry should be recorded in `project.dataset.job_audit_table` with `status = 'FAILED'` and a `message` indicating the parameter error.
    *   **Runnable Test Code (SQL):**

    ```sql
    -- After attempting the CALL, check the audit table
    SELECT status, message
    FROM `project.dataset.job_audit_table`
    WHERE job_name = 'k_ausd_bp_ta_p_basisprod.ksh'
      AND status = 'FAILED'
      AND message LIKE '%p_JobKennung is not set%'
    ORDER BY start_time DESC
    LIMIT 1;
    -- Pass if one row is returned matching the criteria.
    ```

---

### Test Case 3: Date Validation - Invalid `p_Stichtag` Format

*   **Purpose:** To verify that the BigQuery stored procedure correctly validates the `p_Stichtag` parameter for the expected `DDMMYYYY` format.
*   **Setup:** None.
*   **Action:** Attempt to call the BigQuery stored procedure with `p_Stichtag` in an incorrect format (e.g., `YYYY-MM-DD`).
    ```sql
    CALL `project.dataset.sp_k_ausd_bp_ta_p_basisprod`(
      p_JobKennung => 'TEST_JOB',
      p_EintragsNr => 'TEST_ENTRY',
      p_Stichtag => '2023-01-01', -- Incorrect format
      p_wiederanlaufWert => '0'
    );
    ```
*   **Pass/Fail Criterion:**
    *   The stored procedure execution must fail and raise an error.
    *   The error message should contain `'ERROR: p_Stichtag format is invalid. Expected DDMMYYYY.'`.
    *   An entry should be recorded in `project.dataset.job_audit_table` with `status = 'FAILED'` and a `message` indicating the date format error.
    *   **Runnable Test Code (SQL):**

    ```sql
    -- After attempting the CALL, check the audit table
    SELECT status, message
    FROM `project.dataset.job_audit_table`
    WHERE job_name = 'k_ausd_bp_ta_p_basisprod.ksh'
      AND status = 'FAILED'
      AND message LIKE '%p_Stichtag format is invalid%'
    ORDER BY start_time DESC
    LIMIT 1;
    -- Pass if one row is returned matching the criteria.
    ```

---

### Test Case 4: Transformation Correctness - `APN` Column Logic (`DECODE` to `CASE WHEN`)

*   **Purpose:** To specifically verify the correct translation of the Oracle `DECODE` function for the `APN` column, including its NULL handling behavior during concatenation, to BigQuery's `CASE WHEN` and `CONCAT` functions.
*   **Setup:**
    1.  **Legacy Data:** Populate `sof$ta_apn_vertrag` in Oracle with specific test cases:
        *   `cntrct_id = 'C1'`, `apn = 'APN1'`, `apn_cntrct = 'C1_EXT'` (Both present)
        *   `cntrct_id = 'C2'`, `apn = 'APN2'`, `apn_cntrct = NULL` (APN present, APN_CNTRCT NULL)
        *   `cntrct_id = 'C3'`, `apn = NULL`, `apn_cntrct = 'C3_EXT'` (APN NULL, APN_CNTRCT present)
        *   `cntrct_id = 'C4'`, `apn = NULL`, `apn_cntrct = NULL` (Both NULL)
    2.  **BigQuery Data:** Replicate the exact same data in `project.dataset.sof_ta_apn_vertrag`.
    3.  **Other Source Tables:** Ensure other necessary source tables are populated with minimal data to allow the join to succeed for these `cntrct_id`s.
*   **Action:**
    1.  Execute the legacy KornShell script with valid parameters.
    2.  Execute the migrated BigQuery stored procedure with the same valid parameters.
*   **Pass/Fail Criterion:**
    *   Query the `APN` column for `cntrct_id`s 'C1', 'C2', 'C3', 'C4' from both the Oracle and BigQuery target tables. The results must match.
    *   **Expected Oracle Output for `APN`:**
        *   'C1': `'APN1,C1_EXT'`
        *   'C2': `'APN2,'` (Oracle `||` treats NULL as empty string)
        *   'C3': `NULL` (Due to `decode(av.apn, null, av.apn, ...)` logic)
        *   'C4': `NULL` (Due to `decode(av.apn, null, av.apn, ...)` logic)
    *   **Note on BigQuery Translation Discrepancy:** The provided BigQuery code `CASE WHEN av.apn IS NULL THEN av.apn ELSE CONCAT(av.apn, ',', av.apn_cntrct) END` will produce `NULL` for `cntrct_id = 'C2'` (where `av.apn_cntrct` is NULL), which *differs* from Oracle's `'APN2,'`.
    *   **Proposed BigQuery Fix (if Oracle behavior is desired):** The BigQuery `EXECUTE IMMEDIATE` block should be updated to:
        ```sql
        CASE WHEN av.apn IS NULL THEN NULL ELSE CONCAT(av.apn, ',', COALESCE(av.apn_cntrct, '')) END
        ```
        This corrected logic would produce `'APN2,'` for `cntrct_id = 'C2'`, matching Oracle.
    *   **Runnable Test Code (SQL):**

    ```sql
    -- BigQuery:
    SELECT
        t.CNTRCT_ID,
        t.APN
    FROM `project.dataset.sof_ta_p_basisprod` t
    WHERE t.CNTRCT_ID IN ('C1', 'C2', 'C3', 'C4')
    ORDER BY t.CNTRCT_ID;

    -- Oracle:
    SELECT
        t.CNTRCT_ID,
        t.APN
    FROM sof$ta_p_basisprod t
    WHERE t.CNTRCT_ID IN ('C1', 'C2', 'C3', 'C4')
    ORDER BY t.CNTRCT_ID;

    -- Compare the results manually or programmatically.
    -- If the BigQuery code is NOT fixed, the test for 'C2' will fail.
    ```

---

### Test Case 5: Data Quality - Record Count and Audit Logging (Success)

*   **Purpose:** To verify that the BigQuery stored procedure accurately captures the number of processed records and logs the job's successful completion in the `job_audit_table`.
*   **Setup:**
    1.  Populate BigQuery source tables with a known number of rows that will result in a predictable number of output rows (e.g., 100 rows in `sof_ta_cntrct_dist` leading to 100 output rows).
    2.  Ensure `project.dataset.job_audit_table` exists.
*   **Action:** Call the BigQuery stored procedure with valid parameters.
    ```sql
    CALL `project.dataset.sp_k_ausd_bp_ta_p_basisprod`(
      p_JobKennung => 'AUDIT_TEST_SUCCESS',
      p_EintragsNr => '12345',
      p_Stichtag => '15032023',
      p_wiederanlaufWert => '0'
    );
    ```
*   **Pass/Fail Criterion:**
    *   The stored procedure must complete successfully without errors.
    *   The `project.dataset.sof_ta_p_basisprod` table must contain the expected number of rows (e.g., 100).
    *   An entry must exist in `project.dataset.job_audit_table` for `job_name = 'k_ausd_bp_ta_p_basisprod.ksh'` and `job_kennung = 'AUDIT_TEST_SUCCESS'` with:
        *   `status = 'COMPLETED'`
        *   `records_processed` matching the actual row count in `sof_ta_p_basisprod`.
        *   `start_time` and `end_time` populated, with `end_time` > `start_time`.
        *   `message = 'Job completed successfully.'`
    *   **Runnable Test Code (SQL):**

    ```sql
    -- Check target table row count
    SELECT COUNT(*) FROM `project.dataset.sof_ta_p_basisprod`; -- Expected: 100

    -- Check audit table entry
    SELECT
        status,
        records_processed,
        message,
        DATETIME_DIFF(end_time, start_time, SECOND) AS duration_seconds
    FROM `project.dataset.job_audit_table`
    WHERE job_name = 'k_ausd_bp_ta_p_basisprod.ksh'
      AND job_kennung = 'AUDIT_TEST_SUCCESS'
    ORDER BY start_time DESC
    LIMIT 1;
    -- Pass if status is 'COMPLETED', records_processed matches actual count, and message is correct.
    ```

---

### Test Case 6: Data Quality - Record Count and Audit Logging (Failure)

*   **Purpose:** To verify that the BigQuery stored procedure correctly logs job failures and associated error messages in the `job_audit_table`.
*   **Setup:** Ensure `project.dataset.job_audit_table` exists.
*   **Action:** Call the BigQuery stored procedure with an invalid `p_Stichtag` that will cause a known failure.
    ```sql
    CALL `project.dataset.sp_k_ausd_bp_ta_p_basisprod`(
      p_JobKennung => 'AUDIT_TEST_FAILURE',
      p_EintragsNr => '67890',
      p_Stichtag => '99999999', -- Invalid date
      p_wiederanlaufWert => '0'
    );
    ```
*   **Pass/Fail Criterion:**
    *   The stored procedure execution must fail and raise an error.
    *   An entry must exist in `project.dataset.job_audit_table` for `job_name = 'k_ausd_bp_ta_p_basisprod.ksh'` and `job_kennung = 'AUDIT_TEST_FAILURE'` with:
        *   `status = 'FAILED'`
        *   `records_processed` being `NULL` or `0` (as the main logic didn't run).
        *   `start_time` and `end_time` populated.
        *   `message` containing the error details (e.g., `'ERROR: Could not parse p_Stichtag to a valid date.'`).
    *   **Runnable Test Code (SQL):**

    ```sql
    -- Check audit table entry
    SELECT
        status,
        records_processed,
        message
    FROM `project.dataset.job_audit_table`
    WHERE job_name = 'k_ausd_bp_ta_p_basisprod.ksh'
      AND job_kennung = 'AUDIT_TEST_FAILURE'
    ORDER BY start_time DESC
    LIMIT 1;
    -- Pass if status is 'FAILED' and message contains the expected error.
    ```

---

### Test Case 7: Schema Assertions - Target Table `sof_ta_p_basisprod`

*   **Purpose:** To verify that the schema (column names, data types, nullability) of the target table `project.dataset.sof_ta_p_basisprod` matches the expected definition, ensuring data integrity and compatibility.
*   **Setup:** Ensure the `project.dataset.sof_ta_p_basisprod` table has been created using the provided DDL.
*   **Action:** Query BigQuery's `INFORMATION_SCHEMA` for the target table's schema.
*   **Pass/Fail Criterion:**
    *   The query results must match the expected schema definition (e.g., from `bigquery/ddl_target_tables.sql`). This includes:
        *   All expected columns are present.
        *   Column names are correct (case-sensitive if applicable).
        *   Data types are correct (e.g., `STRING`, `DATE`).
        *   Nullability constraints (e.g., `CNTRCT_ID` is likely `NOT NULL` in source, should be reflected).
    *   **Runnable Test Code (SQL):**

    ```sql
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM `project.dataset.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'sof_ta_p_basisprod'
    ORDER BY ordinal_position;

    -- Expected output (partial example):
    -- column_name | data_type | is_nullable
    -- -------------|-----------|-------------
    -- CNTRCT_ID   | STRING    | YES
    -- EVN         | STRING    | YES
    -- TNV_ICCID   | STRING    | YES
    -- TNV_ICC_VALID | DATE      | YES
    -- ...
    ```

---

### Test Case 8: External System Replacement - Source Table Existence

*   **Purpose:** To ensure that all source tables referenced in the core SQL logic (`d_ausd_bp_ta_p_basisprod.sql` equivalent) are accessible and exist within the BigQuery environment, replacing the Oracle source tables.
*   **Setup:**
    1.  Ensure DDL for all source tables (`sof_ta_cntrct_dist`, `sof_ta_bcp_iccid`, `sof_ta_bcp_msisdn`, `sof_ta_cntrct_evn`, `sof_ta_iccid_vertrag`, `sof_ta_rn_vertrag`, `sof_ta_rn_da_vda_tk`, `sof_ta_tarifoption`, `sof_ta_apn_vertrag`) has been executed in `project.dataset`.
    2.  Populate these tables with at least one row of valid data.
*   **Action:** Call the BigQuery stored procedure with valid parameters.
    ```sql
    CALL `project.dataset.sp_k_ausd_bp_ta_p_basisprod`(
      p_JobKennung => 'SOURCE_TABLE_CHECK',
      p_EintragsNr => '11111',
      p_Stichtag => '01012023',
      p_wiederanlaufWert => '0'
    );
    ```
*   **Pass/Fail Criterion:**
    *   The stored procedure must execute successfully without any "table not found" or "view not found" errors related to the source tables.
    *   An entry should be recorded in `project.dataset.job_audit_table` with `status = 'COMPLETED'`.
    *   **Runnable Test Code (SQL):** (This is more of a pre-check, but a successful run confirms table existence.)

    ```sql
    -- After the CALL, check the audit table for successful completion
    SELECT status, message
    FROM `project.dataset.job_audit_table`
    WHERE job_name = 'k_ausd_bp_ta_p_basisprod.ksh'
      AND job_kennung = 'SOURCE_TABLE_CHECK'
    ORDER BY start_time DESC
    LIMIT 1;
    -- Pass if status is 'COMPLETED'.
    ```

---

### Test Case 9: Commented-out File Processing (Conditional Test)

*   **Purpose:** If the commented-out `sed`, `sort`, and `join` operations from the legacy script are activated and translated into BigQuery SQL, this test verifies their correctness.
*   **Setup:**
    1.  **Cloud Storage Data:** Place sample input files (e.g., `cibasis_data24.dat`, `cibasis_data96.dat`, `cibasis_fax.dat`) in a Google Cloud Storage bucket. These files should contain data that exercises the `sed` (space removal), `sort -u -k 1 -t ';'` (unique sort by first field), and `join` logic.
    2.  **BigQuery SP Modification:** The `project.dataset.sp_k_ausd_bp_ta_p_basisprod` must be modified to include the BigQuery-native translation of these operations (e.g., `LOAD DATA` from GCS, `SELECT DISTINCT`, `REGEXP_REPLACE`, `JOIN`).
    3.  **Legacy Output:** Run the legacy script with the commented-out sections uncommented (if possible) or manually perform the `sed`, `sort`, `join` operations on the sample files to generate expected output files.
*   **Action:**
    1.  Execute the modified BigQuery stored procedure with valid parameters.
*   **Pass/Fail Criterion:**
    *   The BigQuery tables generated by these operations (e.g., `cibasis_24_96_tmp`, `cibasisprodukt_csv`) must exactly match the content of the corresponding output files generated by the legacy `sed`/`sort`/`join` operations.
    *   **Runnable Test Code (SQL):**

    ```sql
    -- Example: Compare the final joined output table
    -- Assuming 'legacy_cibasisprodukt_csv_snapshot' is a BigQuery table loaded from the legacy output file.
    SELECT 'Mismatch in BigQuery only' AS issue, bq.*
    FROM `project.dataset.cibasisprodukt_csv` bq
    LEFT JOIN `project.dataset.legacy_cibasisprodukt_csv_snapshot` legacy ON
        bq.column1 = legacy.column1 AND -- Assuming column1 is a key for comparison
        -- ... compare all other columns ...
        (bq.columnN IS NOT DISTINCT FROM legacy.columnN)
    WHERE legacy.column1 IS NULL
    UNION ALL
    SELECT 'Mismatch in Legacy only' AS issue, legacy.*
    FROM `project.dataset.legacy_cibasisprodukt_csv_snapshot` legacy
    LEFT JOIN `project.dataset.cibasisprodukt_csv` bq ON
        legacy.column1 = bq.column1 AND
        -- ... compare all other columns ...
        (legacy.columnN IS NOT DISTINCT FROM bq.columnN)
    WHERE bq.column1 IS NULL
    UNION ALL
    SELECT 'Content Mismatch' AS issue, bq.*
    FROM `project.dataset.cibasisprodukt_csv` bq
    JOIN `project.dataset.legacy_cibasisprodukt_csv_snapshot` legacy ON
        bq.column1 = legacy.column1
    WHERE NOT (
        (bq.column2 IS NOT DISTINCT FROM legacy.column2) AND
        -- ... all other columns must match ...
        (bq.columnN IS NOT DISTINCT FROM legacy.columnN)
    );
    -- The query should return 0 rows for a pass.
    ```