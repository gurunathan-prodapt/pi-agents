As a senior data-migration QA engineer, I've analyzed the migration design and the provided code for `r_ausd_bp_ta_rn_da_vda_tk.ksh` to BigQuery. The following test cases are designed to ensure behavioral equivalence, transformation correctness, and robust operation of the migrated BigQuery solution.

**Key Observations & Assumptions:**

1.  **`p_wiederanlaufWert` Discrepancy:** The legacy KornShell script's `usage` message indicates that `p_wiederanlaufWert` (restart value) should filter contracts based on `DWH_VERTRAG_ID`. However, the description of the core SQL script (`d_ausd_bp_ta_rn_da_vda_tk.sql`) and the migrated BigQuery SQL (`sp_ausd_bp_ta_rn_da_vda_tk`) *do not include this filtering logic*. The BigQuery stored procedure `sp_ausd_bp_ta_rn_da_vda_tk` accepts `p_restart_threshold` but does not use it. This represents a **behavioral difference** between the legacy script's *documented intent* and the *implemented SQL logic* (both legacy and migrated). For these tests, I will assume the SQL logic (filtering only on MSISDN NULLs) is the authoritative behavior to be migrated, as per the design document's description of the SQL script. This should be flagged for clarification with the business/development team.
2.  **`p_stichtag` Format:** The legacy script uses `DDMMYYYY` for `p_stichtag`. The migrated BigQuery orchestrator defaults `v_current_date` to `YYYYMMDD`. It is assumed that if `p_stichtag` is explicitly provided to the BigQuery procedure, it should be in `YYYYMMDD` format for consistency.
3.  **Environment:** Tests assume access to both the legacy Oracle environment and the new BigQuery environment for comparison.
4.  **Data Consistency:** For output parity tests, it's crucial that the BigQuery source tables (`dwtk_meldungen`, `sof_ta_rn_einzeln`) are populated with data identical to their Oracle counterparts *before* running the migrated job.

---

## Test Case 1: Schema and Data Type Equivalence

*   **Purpose:** Verify that the BigQuery table schemas for source and target tables (`dwtk_meldungen`, `sof_ta_rn_einzeln`, `sof_ta_rn_da_vda_tk`) are functionally equivalent to their Oracle counterparts, ensuring correct data type mapping and column presence. Also, validate the `job_audit_log` schema.
*   **Setup:**
    *   Ensure all BigQuery DDLs (`ddl/*.sql`) have been executed.
    *   Have access to Oracle schema definitions for `ISBERT_SCHEMA.DWTK_MELDUNGEN`, `SOF_TA_RN_EINZELN`, `SOF_TA_RN_DA_VDA_TK`.
*   **Action:**
    1.  Retrieve schema definitions for Oracle tables.
    2.  Retrieve schema definitions for BigQuery tables.
*   **Pass/Fail Criterion:**
    *   All columns present in Oracle tables are present in their corresponding BigQuery tables.
    *   Data types are compatible (e.g., Oracle `VARCHAR2` to BigQuery `STRING`, Oracle `NUMBER` to BigQuery `NUMERIC` or `INT64`, Oracle `DATE`/`TIMESTAMP` to BigQuery `DATE`/`TIMESTAMP`).
    *   The `job_audit_log` table has the expected columns and data types as defined in `ddl/job_audit_log.sql`.
*   **Test Code (Conceptual - requires Oracle and BigQuery metadata access):**

    ```python
    # Example using BigQuery client library (similar logic for Oracle)
    from google.cloud import bigquery

    client = bigquery.Client()

    def get_bq_schema(table_id):
        table = client.get_table(table_id)
        return {field.name: field.field_type for field in table.schema}

    oracle_dwtk_meldungen_schema = {
        "JOB_KENNUNG": "VARCHAR2",
        "TIMECREATED": "TIMESTAMP",
        "MESSAGE": "VARCHAR2",
        # ... other Oracle fields
    }
    bq_dwtk_meldungen_schema = get_bq_schema("my_project.my_dataset.dwtk_meldungen")

    assert "job_kennung" in bq_dwtk_meldungen_schema
    assert bq_dwtk_meldungen_schema["job_kennung"] == "STRING"
    assert "timecreated" in bq_dwtk_meldungen_schema
    assert bq_dwtk_meldungen_schema["timecreated"] == "TIMESTAMP"
    # ... similar assertions for other tables and columns
    ```

---

## Test Case 2: Initial Data Load Verification (Prerequisite)

*   **Purpose:** Confirm that the initial data migration from Oracle source tables (`isbert_schema.dwtk_meldungen`, `sof$ta_rn_einzeln`) to their BigQuery counterparts (`my_project.my_dataset.dwtk_meldungen`, `my_project.my_dataset.sof_ta_rn_einzeln`) is accurate. This is a critical prerequisite for testing the job's logic.
*   **Setup:**
    *   Oracle source tables are populated with representative test data.
    *   BigQuery source tables have been loaded with data from Oracle using the chosen ETL method (e.g., Dataflow, DTS).
*   **Action:**
    1.  Query row counts for each source table in both Oracle and BigQuery.
    2.  For a sample of data (or all data if feasible), compare content using checksums or direct row-by-row comparison for key columns.
*   **Pass/Fail Criterion:**
    *   Row counts for `dwtk_meldungen` and `sof_ta_rn_einzeln` match exactly between Oracle and BigQuery.
    *   A statistically significant sample of data (or all data) shows exact matches for all relevant columns, considering data type conversions (e.g., precision for numbers, timezone for timestamps).
*   **Test Code (SQL):**

    ```sql
    -- For dwtk_meldungen
    SELECT COUNT(*) FROM ISBERT_SCHEMA.DWTK_MELDUNGEN; -- Oracle
    SELECT COUNT(*) FROM my_project.my_dataset.dwtk_meldungen; -- BigQuery

    -- For sof_ta_rn_einzeln
    SELECT COUNT(*) FROM SOF_TA_RN_EINZELN; -- Oracle
    SELECT COUNT(*) FROM my_project.my_dataset.sof_ta_rn_einzeln; -- BigQuery

    -- Example for content comparison (conceptual, might need more robust hashing/checksums for large datasets)
    -- Oracle:
    SELECT
        DBMS_METADATA.GET_DDL('TABLE','DWTK_MELDUNGEN','ISBERT_SCHEMA') AS DDL,
        ORA_HASH(TO_CHAR(TIMECREATED) || JOB_KENNUNG || MESSAGE) AS ROW_HASH
    FROM ISBERT_SCHEMA.DWTK_MELDUNGEN
    ORDER BY ROW_HASH;

    -- BigQuery:
    SELECT
        TO_JSON_STRING(t) AS DDL, -- Or specific column concatenation
        FARM_FINGERPRINT(CONCAT(FORMAT_TIMESTAMP('%Y%m%d%H%M%S', timecreated), job_kennung, message)) AS ROW_HASH
    FROM my_project.my_dataset.dwtk_meldungen AS t
    ORDER BY ROW_HASH;
    ```

---

## Test Case 3: Orchestration - Default Parameter Handling

*   **Purpose:** Verify that `sp_bereitstellung_basisprodukte_bert` correctly handles default values for `p_stichtag` and `p_wiederanlaufWert` when they are not provided (or are empty strings).
*   **Setup:**
    *   BigQuery source tables (`dwtk_meldungen`, `sof_ta_rn_einzeln`) are populated with test data.
    *   `job_audit_log` and `sof_ta_rn_da_vda_tk` tables are empty.
*   **Action:**
    1.  Call the BigQuery orchestrator procedure without parameters:
        `CALL my_project.my_dataset.sp_bereitstellung_basisprodukte_bert(NULL, NULL);`
    2.  Query `job_audit_log` for the latest run.
    3.  Query `sof_ta_rn_da_vda_tk` for inserted data.
*   **Pass/Fail Criterion:**
    *   The `job_audit_log` shows a successful run (`status = 'SUCCESS'`).
    *   `parameter_stichtag` in `job_audit_log` matches `FORMAT_DATE('%Y%m%d', CURRENT_DATE())` at the time of execution.
    *   `parameter_wiederanlaufwert` in `job_audit_log` is '0'.
    *   The `sof_ta_rn_da_vda_tk` table contains data filtered by the core logic.
*   **Test Code (BigQuery SQL):**

    ```sql
    -- Clear previous runs for a clean test
    TRUNCATE TABLE my_project.my_dataset.job_audit_log;
    TRUNCATE TABLE my_project.my_dataset.sof_ta_rn_da_vda_tk;

    -- Action
    CALL my_project.my_dataset.sp_bereitstellung_basisprodukte_bert(NULL, NULL);

    -- Assertions
    SELECT
        job_run_id,
        status,
        parameter_stichtag,
        parameter_wiederanlaufwert,
        message
    FROM my_project.my_dataset.job_audit_log
    ORDER BY start_timestamp DESC
    LIMIT 1;
    -- Expected: status = 'SUCCESS', parameter_stichtag = CURRENT_DATE in YYYYMMDD, parameter_wiederanlaufwert = '0'

    SELECT COUNT(*) FROM my_project.my_dataset.sof_ta_rn_da_vda_tk;
    -- Expected: > 0 if source data exists and matches filter
    ```

---

## Test Case 4: Orchestration - Explicit Parameter Handling

*   **Purpose:** Verify that `sp_bereitstellung_basisprodukte_bert` correctly uses explicitly provided `p_stichtag` and `p_wiederanlaufWert`.
*   **Setup:**
    *   BigQuery source tables (`dwtk_meldungen`, `sof_ta_rn_einzeln`) are populated with test data.
    *   `job_audit_log` and `sof_ta_rn_da_vda_tk` tables are empty.
    *   Choose a specific `stichtag` (e.g., '20230115') and `wiederanlaufWert` (e.g., '100').
*   **Action:**
    1.  Call the BigQuery orchestrator procedure with explicit parameters:
        `CALL my_project.my_dataset.sp_bereitstellung_basisprodukte_bert('20230115', '100');`
    2.  Query `job_audit_log` for the latest run.
*   **Pass/Fail Criterion:**
    *   The `job_audit_log` shows a successful run (`status = 'SUCCESS'`).
    *   `parameter_stichtag` in `job_audit_log` is '20230115'.
    *   `parameter_wiederanlaufwert` in `job_audit_log` is '100'.
    *   The `sof_ta_rn_da_vda_tk` table contains data filtered by the core logic (note: `p_wiederanlaufWert` does not affect the core SQL filter).
*   **Test Code (BigQuery SQL):**

    ```sql
    TRUNCATE TABLE my_project.my_dataset.job_audit_log;
    TRUNCATE TABLE my_project.my_dataset.sof_ta_rn_da_vda_tk;

    CALL my_project.my_dataset.sp_bereitstellung_basisprodukte_bert('20230115', '100');

    SELECT
        job_run_id,
        status,
        parameter_stichtag,
        parameter_wiederanlaufwert,
        message
    FROM my_project.my_dataset.job_audit_log
    ORDER BY start_timestamp DESC
    LIMIT 1;
    -- Expected: status = 'SUCCESS', parameter_stichtag = '20230115', parameter_wiederanlaufwert = '100'
    ```

---

## Test Case 5: Orchestration - Invalid Parameter Handling

*   **Purpose:** Verify that `sp_bereitstellung_basisprodukte_bert` correctly handles invalid `p_stichtag` values (e.g., incorrect format or length) and logs the error.
*   **Setup:**
    *   `job_audit_log` table is empty.
*   **Action:**
    1.  Attempt to call the BigQuery orchestrator procedure with an invalid `p_stichtag`:
        `CALL my_project.my_dataset.sp_bereitstellung_basisprodukte_bert('2023-01-15', NULL);` (Invalid format)
        `CALL my_project.my_dataset.sp_bereitstellung_basisprodukte_bert('202301', NULL);` (Invalid length)
    2.  Query `job_audit_log` for the latest run.
*   **Pass/Fail Criterion:**
    *   The procedure call should raise an error (SQLSTATE '45000').
    *   The `job_audit_log` should contain an entry for the failed run (`status = 'FAILED'`) with an appropriate error message (e.g., "ERROR: Parameter -s (Stichtag) is invalid or not provided.").
*   **Test Code (BigQuery SQL):**

    ```sql
    TRUNCATE TABLE my_project.my_dataset.job_audit_log;

    -- Attempt to call with invalid stichtag format
    BEGIN
        CALL my_project.my_dataset.sp_bereitstellung_basisprodukte_bert('2023-01-15', NULL);
    EXCEPTION WHEN ERROR THEN
        SELECT 'Caught expected error for invalid stichtag format.' AS message;
    END;

    -- Attempt to call with invalid stichtag length
    BEGIN
        CALL my_project.my_dataset.sp_bereitstellung_basisprodukte_bert('202301', NULL);
    EXCEPTION WHEN ERROR THEN
        SELECT 'Caught expected error for invalid stichtag length.' AS message;
    END;

    -- Assertions for job_audit_log
    SELECT
        job_run_id,
        status,
        message
    FROM my_project.my_dataset.job_audit_log
    ORDER BY start_timestamp DESC
    LIMIT 2;
    -- Expected: Two entries with status = 'FAILED' and messages indicating invalid parameter.
    ```

---

## Test Case 6: Core Logic - `v_datum` Determination

*   **Purpose:** Verify that `sp_ausd_bp_ta_rn_da_vda_tk` correctly determines the `v_datum` variable based on `dwtk_meldungen` and handles the `NVL` / `COALESCE` logic.
*   **Setup:**
    *   `sof_ta_rn_da_vda_tk` is empty.
    *   Populate `my_project.my_dataset.dwtk_meldungen` with specific test data:
        *   Scenario A: One row with `job_kennung = 'BERT_DROP_TEMP_TABLE'` and `timecreated = '2023-03-15 10:00:00 UTC'`.
        *   Scenario B: Multiple rows, one with `job_kennung = 'BERT_DROP_TEMP_TABLE'` and `timecreated = '2023-03-15 10:00:00 UTC'`, another with `job_kennung = 'BERT_DROP_TEMP_TABLE'` and `timecreated = '2023-03-16 11:00:00 UTC'`.
        *   Scenario C: No rows with `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
*   **Action:**
    1.  For each scenario, populate `dwtk_meldungen`.
    2.  Call `sp_ausd_bp_ta_rn_da_vda_tk` (it's called by the orchestrator, but for isolated testing, we can call it directly with dummy `job_run_id`).
    3.  Query `job_audit_log` for the `v_datum` message.
*   **Pass/Fail Criterion:**
    *   Scenario A: `v_datum` in `job_audit_log` is '20230315'.
    *   Scenario B: `v_datum` in `job_audit_log` is '20230316' (MAX timecreated).
    *   Scenario C: `v_datum` in `job_audit_log` is '19000101'.
*   **Test Code (BigQuery SQL):**

    ```sql
    DECLARE v_job_run_id STRING DEFAULT GENERATE_UUID();

    -- Scenario A: Single matching entry
    TRUNCATE TABLE my_project.my_dataset.dwtk_meldungen;
    INSERT INTO my_project.my_dataset.dwtk_meldungen (job_kennung, timecreated, message)
    VALUES ('BERT_DROP_TEMP_TABLE', '2023-03-15 10:00:00 UTC', 'Test message');
    TRUNCATE TABLE my_project.my_dataset.job_audit_log;
    CALL my_project.my_dataset.sp_ausd_bp_ta_rn_da_vda_tk(v_job_run_id, '20230315', 0, 0);
    SELECT message FROM my_project.my_dataset.job_audit_log WHERE job_run_id = v_job_run_id AND message LIKE 'Determined v_datum:%';
    -- Expected: message LIKE '%Determined v_datum: 20230315%'

    -- Scenario B: Multiple matching entries
    SET v_job_run_id = GENERATE_UUID();
    TRUNCATE TABLE my_project.my_dataset.dwtk_meldungen;
    INSERT INTO my_project.my_dataset.dwtk_meldungen (job_kennung, timecreated, message)
    VALUES
        ('BERT_DROP_TEMP_TABLE', '2023-03-15 10:00:00 UTC', 'Test message 1'),
        ('BERT_DROP_TEMP_TABLE', '2023-03-16 11:00:00 UTC', 'Test message 2'),
        ('OTHER_JOB', '2023-03-17 12:00:00 UTC', 'Other job message');
    TRUNCATE TABLE my_project.my_dataset.job_audit_log;
    CALL my_project.my_dataset.sp_ausd_bp_ta_rn_da_vda_tk(v_job_run_id, '20230316', 0, 0);
    SELECT message FROM my_project.my_dataset.job_audit_log WHERE job_run_id = v_job_run_id AND message LIKE 'Determined v_datum:%';
    -- Expected: message LIKE '%Determined v_datum: 20230316%'

    -- Scenario C: No matching entries
    SET v_job_run_id = GENERATE_UUID();
    TRUNCATE TABLE my_project.my_dataset.dwtk_meldungen;
    INSERT INTO my_project.my_dataset.dwtk_meldungen (job_kennung, timecreated, message)
    VALUES ('OTHER_JOB', '2023-03-17 12:00:00 UTC', 'Other job message');
    TRUNCATE TABLE my_project.my_dataset.job_audit_log;
    CALL my_project.my_dataset.sp_ausd_bp_ta_rn_da_vda_tk(v_job_run_id, '20230317', 0, 0);
    SELECT message FROM my_project.my_dataset.job_audit_log WHERE job_run_id = v_job_run_id AND message LIKE 'Determined v_datum:%';
    -- Expected: message LIKE '%Determined v_datum: 19000101%'
    ```

---

## Test Case 7: Core Logic - Filter and Insert (Output Parity)

*   **Purpose:** This is the most critical test. Verify that the main `INSERT ... SELECT` logic in `sp_ausd_bp_ta_rn_da_vda_tk` produces identical results to the legacy Oracle job when given identical source data. This covers transformation correctness, NULL handling, and output parity.
*   **Setup:**
    *   Populate Oracle `sof$ta_rn_einzeln` with a comprehensive set of test data, including various combinations of NULL and non-NULL values for `DA_RN_msisdn`, `VDA_RN_msisdn`, and `TK_RN_msisdn`.
    *   Populate BigQuery `my_project.my_dataset.sof_ta_rn_einzeln` with *exactly the same data* as Oracle.
    *   Ensure `dwtk_meldungen` in both environments has data that results in a consistent `v_datum` (though `v_datum` doesn't affect the `INSERT` filter).
    *   Clear target tables: `sof$ta_rn_da_vda_tk` (Oracle) and `my_project.my_dataset.sof_ta_rn_da_vda_tk` (BigQuery).
*   **Action:**
    1.  Execute the legacy job (`r_ausd_bp_ta_rn_da_vda_tk.ksh`) against the Oracle environment.
    2.  Execute the migrated job (`sp_bereitstellung_basisprodukte_bert`) against the BigQuery environment (e.g., `CALL my_project.my_dataset.sp_bereitstellung_basisprodukte_bert('20230101', '0');`).
    3.  Compare the contents of the target tables.
*   **Pass/Fail Criterion:**
    *   The row count of `my_project.my_dataset.sof_ta_rn_da_vda_tk` exactly matches `sof$ta_rn_da_vda_tk`.
    *   A full data comparison (e.g., using checksums, row-by-row comparison, or `EXCEPT DISTINCT` queries) confirms that all columns and rows in the BigQuery target table are identical to the Oracle target table.
*   **Test Code (SQL for comparison):**

    ```sql
    -- After running both jobs:

    -- Row count comparison
    SELECT COUNT(*) FROM SOF_TA_RN_DA_VDA_TK; -- Oracle
    SELECT COUNT(*) FROM my_project.my_dataset.sof_ta_rn_da_vda_tk; -- BigQuery
    -- Expected: Counts must be equal

    -- Data comparison (BigQuery vs. Oracle via federated query or external table if possible,
    -- otherwise export to CSV and compare, or use a data comparison tool)
    -- Example using EXCEPT DISTINCT (assuming similar column names and types for simplicity)
    SELECT 'Only in Oracle' AS source, t.* FROM (
        SELECT contract_id, product_code, DA_RN_msisdn, VDA_RN_msisdn, TK_RN_msisdn, valid_from_dt, valid_to_dt, data_value
        FROM SOF_TA_RN_DA_VDA_TK -- Oracle
        EXCEPT DISTINCT
        SELECT contract_id, product_code, DA_RN_msisdn, VDA_RN_msisdn, TK_RN_msisdn, valid_from_dt, valid_to_dt, data_value
        FROM my_project.my_dataset.sof_ta_rn_da_vda_tk -- BigQuery
    ) t;

    SELECT 'Only in BigQuery' AS source, t.* FROM (
        SELECT contract_id, product_code, DA_RN_msisdn, VDA_RN_msisdn, TK_RN_msisdn, valid_from_dt, valid_to_dt, data_value
        FROM my_project.my_dataset.sof_ta_rn_da_vda_tk -- BigQuery
        EXCEPT DISTINCT
        SELECT contract_id, product_code, DA_RN_msisdn, VDA_RN_msisdn, TK_RN_msisdn, valid_from_dt, valid_to_dt, data_value
        FROM SOF_TA_RN_DA_VDA_TK -- Oracle
    ) t;
    -- Expected: Both queries should return 0 rows.
    ```

---

## Test Case 8: Core Logic - Filter Edge Cases

*   **Purpose:** Test the `WHERE DA_RN_msisdn IS NOT NULL OR VDA_RN_msisdn IS NOT NULL OR TK_RN_msisdn IS NOT NULL` filter with specific edge cases in `sof_ta_rn_einzeln`.
*   **Setup:**
    *   Populate `my_project.my_dataset.sof_ta_rn_einzeln` with the following data:
        *   Row 1: `DA_RN_msisdn = '123'`, `VDA_RN_msisdn = NULL`, `TK_RN_msisdn = NULL` (Expected: Included)
        *   Row 2: `DA_RN_msisdn = NULL`, `VDA_RN_msisdn = '456'`, `TK_RN_msisdn = NULL` (Expected: Included)
        *   Row 3: `DA_RN_msisdn = NULL`, `VDA_RN_msisdn = NULL`, `TK_RN_msisdn = '789'` (Expected: Included)
        *   Row 4: `DA_RN_msisdn = '111'`, `VDA_RN_msisdn = '222'`, `TK_RN_msisdn = '333'` (Expected: Included)
        *   Row 5: `DA_RN_msisdn = NULL`, `VDA_RN_msisdn = NULL`, `TK_RN_msisdn = NULL` (Expected: Excluded)
        *   Row 6: `DA_RN_msisdn = ''`, `VDA_RN_msisdn = NULL`, `TK_RN_msisdn = NULL` (Expected: Included, as empty string is not NULL)
    *   Clear `my_project.my_dataset.sof_ta_rn_da_vda_tk`.
*   **Action:**
    1.  Call `my_project.my_dataset.sp_bereitstellung_basisprodukte_bert` (or `sp_ausd_bp_ta_rn_da_vda_tk` directly).
    2.  Query `my_project.my_dataset.sof_ta_rn_da_vda_tk`.
*   **Pass/Fail Criterion:**
    *   Rows 1, 2, 3, 4, 6 are present in `sof_ta_rn_da_vda_tk`.
    *   Row 5 is *not* present in `sof_ta_rn_da_vda_tk`.
    *   Total row count is 5.
*   **Test Code (BigQuery SQL):**

    ```sql
    TRUNCATE TABLE my_project.my_dataset.sof_ta_rn_einzeln;
    INSERT INTO my_project.my_dataset.sof_ta_rn_einzeln (contract_id, product_code, DA_RN_msisdn, VDA_RN_msisdn, TK_RN_msisdn, valid_from_dt, valid_to_dt, data_value)
    VALUES
        ('C1', 'P1', '123', NULL, NULL, '2023-01-01', '2023-12-31', 10.0),
        ('C2', 'P1', NULL, '456', NULL, '2023-01-01', '2023-12-31', 20.0),
        ('C3', 'P1', NULL, NULL, '789', '2023-01-01', '2023-12-31', 30.0),
        ('C4', 'P1', '111', '222', '333', '2023-01-01', '2023-12-31', 40.0),
        ('C5', 'P1', NULL, NULL, NULL, '2023-01-01', '2023-12-31', 50.0),
        ('C6', 'P1', '', NULL, NULL, '2023-01-01', '2023-12-31', 60.0); -- Empty string is not NULL

    TRUNCATE TABLE my_project.my_dataset.sof_ta_rn_da_vda_tk;
    TRUNCATE TABLE my_project.my_dataset.job_audit_log;

    CALL my_project.my_dataset.sp_bereitstellung_basisprodukte_bert('20230101', '0');

    SELECT contract_id, DA_RN_msisdn, VDA_RN_msisdn, TK_RN_msisdn
    FROM my_project.my_dataset.sof_ta_rn_da_vda_tk
    ORDER BY contract_id;
    -- Expected: C1, C2, C3, C4, C6 should be present. C5 should be absent.
    -- Expected count: 5 rows.
    ```

---

## Test Case 9: Row Count Assertion

*   **Purpose:** Verify that the number of rows inserted into the target table is consistent across different runs and scenarios, and matches the expected count based on the filtering logic.
*   **Setup:**
    *   Populate `my_project.my_dataset.sof_ta_rn_einzeln` with a known number of rows that satisfy the filter condition and a known number that do not.
    *   Clear `my_project.my_dataset.sof_ta_rn_da_vda_tk`.
*   **Action:**
    1.  Run the `sp_bereitstellung_basisprodukte_bert` procedure.
    2.  Query the row count of `my_project.my_dataset.sof_ta_rn_da_vda_tk`.
*   **Pass/Fail Criterion:**
    *   The row count in `my_project.my_dataset.sof_ta_rn_da_vda_tk` matches the expected count (number of rows in `sof_ta_rn_einzeln` where `DA_RN_msisdn IS NOT NULL OR VDA_RN_msisdn IS NOT NULL OR TK_RN_msisdn IS NOT NULL`).
*   **Test Code (BigQuery SQL):**

    ```sql
    TRUNCATE TABLE my_project.my_dataset.sof_ta_rn_einzeln;
    INSERT INTO my_project.my_dataset.sof_ta_rn_einzeln (contract_id, product_code, DA_RN_msisdn, VDA_RN_msisdn, TK_RN_msisdn, valid_from_dt, valid_to_dt, data_value)
    VALUES
        ('A', 'P', '1', NULL, NULL, '2023-01-01', '2023-12-31', 1),
        ('B', 'P', NULL, '2', NULL, '2023-01-01', '2023-12-31', 2),
        ('C', 'P', NULL, NULL, '3', '2023-01-01', '2023-12-31', 3),
        ('D', 'P', '4', '5', '6', '2023-01-01', '2023-12-31', 4),
        ('E', 'P', NULL, NULL, NULL, '2023-01-01', '2023-12-31', 5),
        ('F', 'P', '7', NULL, NULL, '2023-01-01', '2023-12-31', 6),
        ('G', 'P', NULL, NULL, NULL, '2023-01-01', '2023-12-31', 7);

    TRUNCATE TABLE my_project.my_dataset.sof_ta_rn_da_vda_tk;
    TRUNCATE TABLE my_project.my_dataset.job_audit_log;

    CALL my_project.my_dataset.sp_bereitstellung_basisprodukte_bert('20230101', '0');

    SELECT COUNT(*) FROM my_project.my_dataset.sof_ta_rn_da_vda_tk;
    -- Expected: 5 rows (A, B, C, D, F)

    -- Verify against source directly
    SELECT COUNT(*)
    FROM my_project.my_dataset.sof_ta_rn_einzeln AS rp
    WHERE
        rp.DA_RN_msisdn IS NOT NULL
        OR rp.VDA_RN_msisdn IS NOT NULL
        OR rp.TK_RN_msisdn IS NOT NULL;
    -- This query should return the same count as the previous one.
    ```

---

## Test Case 10: Logging and Error Handling (End-to-End)

*   **Purpose:** Verify that the `job_audit_log` correctly records job start, informational messages, and final status (success or failure) for both the orchestrator and core processing procedures.
*   **Setup:**
    *   `job_audit_log` table is empty.
    *   `dwtk_meldungen` and `sof_ta_rn_einzeln` are populated with valid data.
*   **Action (Success Scenario):**
    1.  Call `my_project.my_dataset.sp_bereitstellung_basisprodukte_bert('20230101', '0');`
    2.  Query `job_audit_log` for all entries related to the latest `job_run_id`.
*   **Pass/Fail Criterion (Success Scenario):**
    *   `job_audit_log` contains entries for:
        *   `sp_bereitstellung_basisprodukte_bert` with `status = 'RUNNING'`.
        *   `sp_ausd_bp_ta_rn_da_vda_tk` with `status = 'INFO'` (for `v_datum` determination).
        *   `sp_ausd_bp_ta_rn_da_vda_tk` with `status = 'INFO'` (for truncation).
        *   `sp_ausd_bp_ta_rn_da_vda_tk` with `status = 'INFO'` (for insertion completion).
        *   `sp_bereitstellung_basisprodukte_bert` with `status = 'SUCCESS'`.
    *   All entries share the same `job_run_id`.
    *   `start_timestamp` and `end_timestamp` are correctly populated.
*   **Action (Failure Scenario - e.g., invalid table name):**
    1.  Temporarily rename `sof_ta_rn_einzeln` to simulate an error (or modify `sp_ausd_bp_ta_rn_da_vda_tk` to reference a non-existent table).
    2.  Call `my_project.my_dataset.sp_bereitstellung_basisprodukte_bert('20230101', '0');`
    3.  Query `job_audit_log` for all entries related to the latest `job_run_id`.
*   **Pass/Fail Criterion (Failure Scenario):**
    *   The orchestrator procedure call raises an error.
    *   `job_audit_log` contains entries for:
        *   `sp_bereitstellung_basisprodukte_bert` with `status = 'RUNNING'`.
        *   `sp_ausd_bp_ta_rn_da_vda_tk` with `status = 'FAILED'` and an error message (e.g., "Table not found").
        *   `sp_bereitstellung_basisprodukte_bert` with `status = 'FAILED'` and an error message.
    *   All entries share the same `job_run_id`.
*   **Test Code (BigQuery SQL):**

    ```sql
    -- Success Scenario
    TRUNCATE TABLE my_project.my_dataset.job_audit_log;
    -- Ensure source tables are populated for a successful run
    INSERT INTO my_project.my_dataset.dwtk_meldungen (job_kennung, timecreated) VALUES ('BERT_DROP_TEMP_TABLE', CURRENT_TIMESTAMP());
    INSERT INTO my_project.my_dataset.sof_ta_rn_einzeln (contract_id, DA_RN_msisdn) VALUES ('C1', '123');

    CALL my_project.my_dataset.sp_bereitstellung_basisprodukte_bert('20230101', '0');

    SELECT job_name, status, message, start_timestamp, end_timestamp
    FROM my_project.my_dataset.job_audit_log
    WHERE job_run_id = (SELECT job_run_id FROM my_project.my_dataset.job_audit_log ORDER BY start_timestamp DESC LIMIT 1)
    ORDER BY start_timestamp;
    -- Expected: Sequence of RUNNING, INFO (v_datum), INFO (truncate), INFO (insert), SUCCESS.

    -- Failure Scenario (conceptual - requires simulating an error, e.g., by dropping a table)
    TRUNCATE TABLE my_project.my_dataset.job_audit_log;
    -- Simulate error: Drop a required table
    -- DROP TABLE my_project.my_dataset.sof_ta_rn_einzeln;

    BEGIN
        CALL my_project.my_dataset.sp_bereitstellung_basisprodukte_bert('20230101', '0');
    EXCEPTION WHEN ERROR THEN
        SELECT 'Caught expected error for failure scenario.' AS message;
    END;

    SELECT job_name, status, message, start_timestamp, end_timestamp
    FROM my_project.my_dataset.job_audit_log
    WHERE job_run_id = (SELECT job_run_id FROM my_project.my_dataset.job_audit_log ORDER BY start_timestamp DESC LIMIT 1)
    ORDER BY start_timestamp;
    -- Expected: Sequence of RUNNING, FAILED (from sp_ausd_bp_ta_rn_da_vda_tk), FAILED (from sp_bereitstellung_basisprodukte_bert).
    -- The message should indicate the underlying error.
    ```