As a senior data-migration QA engineer, I have analyzed the provided migration design and generated code for `r_ausd_bp_ta_bpr_evn.ksh`. The migration involves porting a KornShell orchestration script to BigQuery Stored Procedures and Cloud Composer.

A critical discrepancy has been identified in the `p_wiederanlaufWert` (restart value) logic:
*   **Legacy Description:** "werden nur Vertraege zu DWH_VERTRAG_ID > Wiederanlaufwert in die FOS-Tabelle geschrieben (die Eintraege bzgl. Werten >= diesem Wert werden geloescht)"
*   **Migrated BigQuery Code (`ausd_bp_ta_bpr_evn_core`):** It correctly deletes records with `DWH_VERTRAG_ID >= p_wiederanlaufWert`. However, the subsequent `INSERT` statement *does not* filter by `DWH_VERTRAG_ID > p_wiederanlaufWert`. This means the BigQuery job will re-insert *all* records matching the date criteria, potentially including those with `DWH_VERTRAG_ID <= p_wiederanlaufWert`, which contradicts the legacy description.

**Recommendation:** The `INSERT` statement in `ausd_bp_ta_bpr_evn_core` should be modified to include the filter `AND (p_wiederanlaufWert = 0 OR src.DWH_VERTRAG_ID > p_wiederanlaufWert)` to achieve behavioral equivalence with the legacy description. This will be highlighted in Test Case 3.

---

## Migration Validation Tests for `r_ausd_bp_ta_bpr_evn.ksh`

### Test Setup Prerequisites

Before running any tests, ensure the following:

1.  **BigQuery Tables:**
    *   `your_gcp_project.your_bigquery_dataset.job_audit_log`
    *   `your_gcp_project.your_bigquery_dataset.contract_cache_source`
    *   `your_gcp_project.your_bigquery_dataset.fos_target_table`
    are created with the specified schemas.
2.  **BigQuery Stored Procedures:**
    *   `your_gcp_project.your_bigquery_dataset.ausd_bp_ta_bpr_evn_core`
    *   `your_gcp_project.your_bigquery_dataset.ausd_bp_ta_bpr_evn_wrapper`
    are deployed.
3.  **Cloud Composer DAG:**
    *   `ausd_bp_ta_bpr_evn_dag` is deployed and configured to call the wrapper procedure.
4.  **Legacy Environment:** Access to the legacy system to run `r_ausd_bp_ta_bpr_evn.ksh` and inspect its outputs (log files, database contents).
5.  **Test Data:** Prepare various sets of test data for `contract_cache_source` and `fos_target_table` to cover different scenarios.
6.  **Placeholders:** Replace `your_gcp_project` and `your_bigquery_dataset` with actual values.

---

### Test Case 1: Happy Path - Full Data Load (Output Parity & Transformation Correctness)

**Purpose:** Verify that the migrated job correctly processes data and produces identical output in the `fos_target_table` and `job_audit_log` when run with a specific `stichtag` and no restart value, matching the legacy job's behavior.

**Setup:**
1.  **Legacy:**
    *   Populate `contract_cache_source` with diverse test data, including records that match and do not match the filtering criteria for a chosen `p_stichtag`.
    *   Ensure `fos_target_table` is empty or contains only data for other `stichtag` values.
2.  **Migrated:**
    *   Populate `your_gcp_project.your_bigquery_dataset.contract_cache_source` with the *exact same* data as the legacy source.
    *   Ensure `your_gcp_project.your_bigquery_dataset.fos_target_table` and `job_audit_log` are empty.
    *   Choose a `p_stichtag` (e.g., '2023-01-15') and ensure `p_wiederanlaufWert` is 0.

**Action:**
1.  **Legacy:** Execute `r_ausd_bp_ta_bpr_evn.ksh -s 15012023 -l 0`.
2.  **Migrated:** Execute the Cloud Composer DAG `ausd_bp_ta_bpr_evn_dag` for `execution_date = '2023-01-15'` with `params={"p_wiederanlaufWert": 0}`. This will call `ausd_bp_ta_bpr_evn_wrapper('2023-01-15', 0)`.

**Pass/Fail Criterion:**
1.  **Output Parity (Data):** The number of rows and the content of `your_gcp_project.your_bigquery_dataset.fos_target_table` for `stichtag = '2023-01-15'` must be *identical* to the data produced by the legacy job in its target table for the same `stichtag`.
    ```sql
    -- Pytest assertion (using a BigQuery client)
    def test_output_parity_happy_path(bq_client, legacy_db_client):
        legacy_data = legacy_db_client.execute("SELECT * FROM legacy_fos_target_table WHERE stichtag = '2023-01-15' ORDER BY DWH_VERTRAG_ID")
        migrated_data = bq_client.query(f"SELECT * FROM `your_gcp_project.your_bigquery_dataset.fos_target_table` WHERE stichtag = '2023-01-15' ORDER BY DWH_VERTRAG_ID").result().to_dataframe()
        assert len(legacy_data) == len(migrated_data)
        # Further assertions to compare row by row, column by column
        # (e.g., convert both to pandas DataFrames and use .equals())
    ```
2.  **Output Parity (Logs):** The `your_gcp_project.your_bigquery_dataset.job_audit_log` table must contain two entries for `job_name = 'r_ausd_bp_ta_bpr_evn'` and `stichtag = '2023-01-15'`: one with `status = 'STARTED'` and one with `status = 'SUCCESS'`. The legacy job's log file should also show a successful run.
    ```sql
    -- BigQuery assertion
    SELECT
        COUNT(1)
    FROM
        `your_gcp_project.your_bigquery_dataset.job_audit_log`
    WHERE
        job_name = 'r_ausd_bp_ta_bpr_evn'
        AND stichtag = '2023-01-15'
        AND status IN ('STARTED', 'SUCCESS');
    -- Expected result: 2
    ```

---

### Test Case 2: Parameter Handling - Default Stichtag (Transformation Correctness)

**Purpose:** Verify that when `p_stichtag` is not provided, the migrated job correctly defaults to `CURRENT_DATE()` (system date) for filtering and logging, matching the legacy job's behavior.

**Setup:**
1.  **Legacy:**
    *   Populate `contract_cache_source` with data relevant to `CURRENT_DATE()`.
    *   Ensure `fos_target_table` is empty for `CURRENT_DATE()`.
2.  **Migrated:**
    *   Populate `your_gcp_project.your_bigquery_dataset.contract_cache_source` with the *exact same* data.
    *   Ensure `your_gcp_project.your_bigquery_dataset.fos_target_table` and `job_audit_log` are empty.
    *   Note `CURRENT_DATE()` on the day of the test (e.g., '2023-10-27').

**Action:**
1.  **Legacy:** Execute `r_ausd_bp_ta_bpr_evn.ksh -l 0` (without `-s` parameter).
2.  **Migrated:** Execute the Cloud Composer DAG `ausd_bp_ta_bpr_evn_dag` for `execution_date = '2023-10-27'` with `params={"p_wiederanlaufWert": 0}`. This will call `ausd_bp_ta_bpr_evn_wrapper(NULL, 0)` (or `''` for `p_stichtag`). The DAG's `{{ ds }}` will provide the current date.

**Pass/Fail Criterion:**
1.  **Output Parity (Data):** The `fos_target_table` for `stichtag = CURRENT_DATE()` must contain the same data as produced by the legacy job.
2.  **Output Parity (Logs):** The `job_audit_log` must contain `STARTED` and `SUCCESS` entries with `stichtag = CURRENT_DATE()`.
    ```sql
    -- BigQuery assertion for logging
    SELECT
        COUNT(1)
    FROM
        `your_gcp_project.your_bigquery_dataset.job_audit_log`
    WHERE
        job_name = 'r_ausd_bp_ta_bpr_evn'
        AND stichtag = CURRENT_DATE()
        AND status IN ('STARTED', 'SUCCESS');
    -- Expected result: 2
    ```

---

### Test Case 3: Restart Mechanism (`p_wiederanlaufWert > 0`) (Transformation Correctness & Discrepancy Check)

**Purpose:** Verify the `p_wiederanlaufWert` logic, including the conditional `DELETE` and `INSERT` behavior, and highlight the identified discrepancy.

**Setup:**
1.  **Legacy & Migrated:**
    *   Populate `contract_cache_source` with data for `p_stichtag = '2023-01-15'`, including records with various `DWH_VERTRAG_ID` values (e.g., 100, 200, 300, 400, 500).
    *   Pre-populate `fos_target_table` with some data for `stichtag = '2023-01-15'`, including records that would be affected by a restart (e.g., `DWH_VERTRAG_ID` 300, 400, 500).
    *   Choose `p_stichtag = '2023-01-15'` and `p_wiederanlaufWert = 350`.

**Action:**
1.  **Legacy:** Execute `r_ausd_bp_ta_bpr_evn.ksh -s 15012023 -l 350`.
2.  **Migrated:** Execute the Cloud Composer DAG `ausd_bp_ta_bpr_evn_dag` for `execution_date = '2023-01-15'` with `params={"p_wiederanlaufWert": 350}`. This will call `ausd_bp_ta_bpr_evn_wrapper('2023-01-15', 350)`.

**Pass/Fail Criterion:**
1.  **Discrepancy Check (Critical):**
    *   **Legacy Expected:** `fos_target_table` for `stichtag = '2023-01-15'` should contain records with `DWH_VERTRAG_ID < 350` (from previous runs) AND new records with `DWH_VERTRAG_ID > 350` (from the current run).
    *   **Migrated Actual:** `fos_target_table` for `stichtag = '2023-01-15'` will contain records with `DWH_VERTRAG_ID < 350` (from previous runs) AND new records with `DWH_VERTRAG_ID > 0` (all records matching date filters, as the `INSERT` is not filtered by `p_wiederanlaufWert`).
    *   **Result:** This test case is expected to **FAIL** due to the identified discrepancy. The `fos_target_table` in BigQuery will likely have more rows or different `DWH_VERTRAG_ID`s than the legacy system.
    *   **Resolution:** The `ausd_bp_ta_bpr_evn_core` procedure's `INSERT` statement needs to be updated as follows:
        ```sql
        -- Proposed fix for ausd_bp_ta_bpr_evn_core
        INSERT INTO `your_gcp_project.your_bigquery_dataset.fos_target_table`
        SELECT
          src.* EXCEPT(contract_data_field_1, contract_data_field_2),
          src.contract_data_field_1,
          src.contract_data_field_2,
          p_stichtag AS stichtag
        FROM `your_gcp_project.your_bigquery_dataset.contract_cache_source` AS src
        WHERE src.Gueltig_von <= p_stichtag
          AND p_stichtag < src.Gueltig_bis
          AND src.LADEDATUM < p_stichtag
          AND (p_wiederanlaufWert = 0 OR src.DWH_VERTRAG_ID > p_wiederanlaufWert); -- ADDED FILTER
        ```
2.  **After Fix (Pass Criterion):** Once the fix is applied, the `fos_target_table` contents for `stichtag = '2023-01-15'` must be *identical* to the legacy output.

---

### Test Case 4: Edge Case - No Active Cache Records (Transformation Correctness)

**Purpose:** Verify the conditional deletion of the target table when no active contract cache records are found for the given `stichtag`.

**Setup:**
1.  **Legacy & Migrated:**
    *   Populate `contract_cache_source` such that *no* records satisfy the filtering criteria (`Gueltig_von <= p_stichtag AND p_stichtag < Gueltig_bis AND LADEDATUM < p_stichtag`) for `p_stichtag = '2023-02-01'`.
    *   Pre-populate `fos_target_table` with some existing data for `stichtag = '2023-02-01'`.
    *   Set `p_wiederanlaufWert = 0`.

**Action:**
1.  **Legacy:** Execute `r_ausd_bp_ta_bpr_evn.ksh -s 01022023 -l 0`.
2.  **Migrated:** Execute the Cloud Composer DAG `ausd_bp_ta_bpr_evn_dag` for `execution_date = '2023-02-01'` with `params={"p_wiederanlaufWert": 0}`. This will call `ausd_bp_ta_bpr_evn_wrapper('2023-02-01', 0)`.

**Pass/Fail Criterion:**
1.  **Output Parity (Data):** The `fos_target_table` for `stichtag = '2023-02-01'` must be *empty* in both legacy and migrated systems.
    ```sql
    -- BigQuery assertion
    SELECT
        COUNT(1)
    FROM
        `your_gcp_project.your_bigquery_dataset.fos_target_table`
    WHERE
        stichtag = '2023-02-01';
    -- Expected result: 0
    ```
2.  **Output Parity (Logs):** The `job_audit_log` must contain `STARTED` and `SUCCESS` entries for `stichtag = '2023-02-01'`.

---

### Test Case 5: Error Handling - Invalid Stichtag Format (External-system replacements)

**Purpose:** Verify that the `ausd_bp_ta_bpr_evn_wrapper` correctly handles and logs an invalid `p_stichtag` format, and that the error is propagated.

**Setup:**
1.  **Migrated:** Ensure `job_audit_log` is empty.

**Action:**
1.  **Migrated:** Manually call the wrapper procedure with an invalid date string:
    ```sql
    CALL `your_gcp_project.your_bigquery_dataset.ausd_bp_ta_bpr_evn_wrapper`('2023/01/15', 0);
    ```
    (Note: The Cloud Composer DAG passes `{{ ds }}` which is always `YYYY-MM-DD`, so this specific error path might not be triggered by the DAG itself, but it's important to test the procedure's robustness).

**Pass/Fail Criterion:**
1.  **Error Propagation:** The `CALL` statement must fail with an error message indicating an invalid `p_stichtag` format.
2.  **Logging:** The `job_audit_log` table must contain two entries:
    *   One with `status = 'STARTED'` for `job_name = 'r_ausd_bp_ta_bpr_evn'`.
    *   One with `status = 'FAILED'` for `job_name = 'r_ausd_bp_ta_bpr_evn'`, and its `message` column should contain text similar to "Invalid p_stichtag format. Expected YYYY-MM-DD, got: 2023/01/15".
    ```sql
    -- BigQuery assertion
    SELECT
        status,
        message
    FROM
        `your_gcp_project.your_bigquery_dataset.job_audit_log`
    WHERE
        job_name = 'r_ausd_bp_ta_bpr_evn'
        AND log_ts = (SELECT MAX(log_ts) FROM `your_gcp_project.your_bigquery_dataset.job_audit_log` WHERE job_name = 'r_ausd_bp_ta_bpr_evn');
    -- Expected result for status: 'FAILED'
    -- Expected result for message: CONTAINS 'Invalid p_stichtag format'
    ```

---

### Test Case 6: Error Handling - Core Procedure Failure (External-system replacements)

**Purpose:** Verify that errors occurring within the `ausd_bp_ta_bpr_evn_core` procedure are caught, logged, and re-raised by the `ausd_bp_ta_bpr_evn_wrapper`.

**Setup:**
1.  **Migrated:**
    *   Modify `ausd_bp_ta_bpr_evn_core` temporarily to force an error (e.g., by adding `RAISE USING MESSAGE = 'Simulated core error';` at the beginning of the `BEGIN` block).
    *   Ensure `job_audit_log` is empty.

**Action:**
1.  **Migrated:** Execute the Cloud Composer DAG `ausd_bp_ta_bpr_evn_dag` for a chosen `execution_date` (e.g., '2023-03-01') with `params={"p_wiederanlaufWert": 0}`. This will call `ausd_bp_ta_bpr_evn_wrapper('2023-03-01', 0)`.

**Pass/Fail Criterion:**
1.  **Error Propagation:** The Cloud Composer task must fail, indicating an error from the BigQuery stored procedure.
2.  **Logging:** The `job_audit_log` table must contain two entries:
    *   One with `status = 'STARTED'` for `job_name = 'r_ausd_bp_ta_bpr_evn'`.
    *   One with `status = 'FAILED'` for `job_name = 'r_ausd_bp_ta_bpr_evn'`, and its `message` column should contain text similar to "Job failed with error: Simulated core error".
    ```sql
    -- BigQuery assertion
    SELECT
        status,
        message
    FROM
        `your_gcp_project.your_bigquery_dataset.job_audit_log`
    WHERE
        job_name = 'r_ausd_bp_ta_bpr_evn'
        AND stichtag = '2023-03-01'
        AND log_ts = (SELECT MAX(log_ts) FROM `your_gcp_project.your_bigquery_dataset.job_audit_log` WHERE job_name = 'r_ausd_bp_ta_bpr_evn');
    -- Expected result for status: 'FAILED'
    -- Expected result for message: CONTAINS 'Simulated core error'
    ```
3.  **Cleanup:** Revert the temporary error-inducing change in `ausd_bp_ta_bpr_evn_core`.

---

### Test Case 7: Data Quality - Schema and Data Types (Schema Assertions)

**Purpose:** Verify that the schema of the `fos_target_table` is as expected and that data types are correctly handled during insertion.

**Setup:**
1.  **Migrated:** Ensure `contract_cache_source` contains data that covers various data types (e.g., NULLs, boundary values for numbers, long strings if applicable).

**Action:**
1.  Execute Test Case 1 (Happy Path) to populate `fos_target_table`.
2.  Inspect the schema of `fos_target_table`.

**Pass/Fail Criterion:**
1.  **Schema Match:** The schema of `your_gcp_project.your_bigquery_dataset.fos_target_table` must match the defined schema and the expected types from `contract_cache_source`.
    ```sql
    -- BigQuery assertion (example for DWH_VERTRAG_ID)
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM
        `your_gcp_project.your_bigquery_dataset.INFORMATION_SCHEMA.COLUMNS`
    WHERE
        table_name = 'fos_target_table'
        AND column_name = 'DWH_VERTRAG_ID';
    -- Expected result: column_name='DWH_VERTRAG_ID', data_type='INT64', is_nullable='NO' (or 'YES' depending on source)
    ```
2.  **Data Type Integrity:** Data inserted into `fos_target_table` must retain its original type and value integrity (e.g., numbers are not truncated, dates are correctly parsed).
    ```sql
    -- Pytest assertion (example for a specific row/column)
    def test_data_type_integrity(bq_client):
        result = bq_client.query(f"SELECT DWH_VERTRAG_ID, Gueltig_von FROM `your_gcp_project.your_bigquery_dataset.fos_target_table` WHERE stichtag = '2023-01-15' AND DWH_VERTRAG_ID = 100").result().to_dataframe()
        assert result['DWH_VERTRAG_ID'].iloc[0] == 100
        assert result['Gueltig_von'].iloc[0] == datetime.date(2022, 1, 1) # Example date
    ```

---

### Test Case 8: Row Count Assertion (Row-count Assertions)

**Purpose:** Verify that the total number of rows in `fos_target_table` after a full run matches the expected count based on the source data and filtering logic.

**Setup:**
1.  **Legacy & Migrated:**
    *   Populate `contract_cache_source` with a known number of records.
    *   Determine the *expected* number of rows that should be inserted into `fos_target_table` for a given `p_stichtag` based on the filtering logic.

**Action:**
1.  Execute Test Case 1 (Happy Path) with a specific `p_stichtag`.

**Pass/Fail Criterion:**
1.  **Row Count Match:** The total number of rows in `your_gcp_project.your_bigquery_dataset.fos_target_table` for the `p_stichtag` must exactly match the expected count derived from the source data and filtering rules.
    ```sql
    -- BigQuery assertion
    SELECT
        COUNT(1)
    FROM
        `your_gcp_project.your_bigquery_dataset.fos_target_table`
    WHERE
        stichtag = '2023-01-15';
    -- Expected result: [Calculated expected row count]
    ```

---