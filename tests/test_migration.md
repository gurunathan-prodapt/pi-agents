As a senior data-migration QA engineer, I've reviewed the migration design document and the generated Airflow DAG and BigQuery SQL for `DW.BERT_AUSD_V_TA_CNTRCT_CRS2`. Below is a comprehensive suite of migration validation tests designed to ensure behavioral equivalence, data integrity, and correctness.

---

### Test Plan for DW.BERT_AUSD_V_TA_CNTRCT_CRS2 Migration

**Test Environment Setup:**

*   **Legacy Environment:** Access to the original Oracle database (read-only) and the ability to execute the legacy KornShell/SQL job. A snapshot of the legacy target table (`sof$ta_cntrct_crs2`) *after* a successful run of the legacy job is essential for output parity tests.
*   **Migration Environment:** A GCP project with BigQuery and Airflow (Cloud Composer) configured.
*   **Data Synchronization:** BigQuery source tables (`dw_bert_staging.sof_ta_cntrct_crs`, `dw_bert_staging.dwtk_meldungen`) must be populated with an exact replica of the data from the legacy Oracle source tables *before* running the migrated job. This ensures a consistent baseline for comparison.
*   **Test Data:** A representative dataset covering various scenarios (e.g., contracts with and without parents, different `cntrct_ty` values, NULLs in relevant columns, edge cases) should be prepared in both legacy and BigQuery source tables.

---

### Test Case 1: Schema Parity of Target Table

*   **Purpose:** To verify that the schema of the migrated BigQuery target table (`dw_bert_staging.sof_ta_cntrct_crs2`) matches the schema of the legacy Oracle target table (`sof$ta_cntrct_crs2`) in terms of column names, data types, and nullability.
*   **Setup:**
    1.  Ensure the `dw_bert_staging.sof_ta_cntrct_crs2` table has been created by the Airflow DAG's `create_target_table_if_not_exists` task.
    2.  Have access to the schema definition of the legacy `sof$ta_cntrct_crs2` table in Oracle.
*   **Action:**
    1.  Extract the schema of `dw_bert_staging.sof_ta_cntrct_crs2` from BigQuery.
    2.  Extract the schema of `sof$ta_cntrct_crs2` from Oracle.
    3.  Compare the column names, data types, and nullability.
*   **Pass/Fail Criterion:** The BigQuery table schema must be functionally equivalent to the Oracle table schema. Data types should be compatible (e.g., Oracle `NUMBER` to BigQuery `INT64` or `BIGNUMERIC`, Oracle `VARCHAR2` to BigQuery `STRING`, Oracle `DATE` to BigQuery `DATE`). All columns present in the Oracle table must be present in the BigQuery table with compatible types and nullability.

```sql
-- BigQuery: Extract schema
SELECT
    column_name,
    data_type,
    is_nullable
FROM
    `dw_bert_staging`.INFORMATION_SCHEMA.COLUMNS
WHERE
    table_name = 'sof_ta_cntrct_crs2'
ORDER BY
    ordinal_position;

-- Oracle: Extract schema (example, actual query might vary based on Oracle version)
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    NULLABLE
FROM
    ALL_TAB_COLUMNS
WHERE
    OWNER = 'SOF' AND TABLE_NAME = 'TA_CNTRCT_CRS2'
ORDER BY
    COLUMN_ID;
```

---

### Test Case 2: Row Count Parity

*   **Purpose:** To ensure that the total number of rows processed and inserted into the target table by the migrated job is identical to the legacy job for the same input data.
*   **Setup:**
    1.  Populate `dw_bert_staging.sof_ta_cntrct_crs` with an exact replica of `sof$ta_cntrct_crs` from Oracle.
    2.  Run the legacy job to populate `sof$ta_cntrct_crs2`. Record the row count.
    3.  Run the migrated Airflow DAG `dw_bert_ausd_v_ta_cntrct_crs2_dag`.
*   **Action:**
    1.  Query the row count from the legacy Oracle target table `sof$ta_cntrct_crs2`.
    2.  Query the row count from the migrated BigQuery target table `dw_bert_staging.sof_ta_cntrct_crs2`.
*   **Pass/Fail Criterion:** The row count in `dw_bert_staging.sof_ta_cntrct_crs2` must be exactly equal to the row count in `sof$ta_cntrct_crs2`.

```sql
-- BigQuery: Get row count from migrated target table
SELECT COUNT(*) FROM `dw_bert_staging.sof_ta_cntrct_crs2`;

-- Oracle: Get row count from legacy target table
SELECT COUNT(*) FROM SOF.TA_CNTRCT_CRS2;
```

---

### Test Case 3: Full Data Parity (Output Parity)

*   **Purpose:** To verify that the content of the migrated target table is an exact match, row-for-row and column-for-column, with the legacy target table. This is the ultimate test for output parity.
*   **Setup:**
    1.  Ensure source data is identical in both environments (as per Test Case 2 setup).
    2.  Run both legacy and migrated jobs to populate their respective target tables.
    3.  Export the legacy `sof$ta_cntrct_crs2` table data into a temporary BigQuery table (e.g., `dw_bert_staging.legacy_sof_ta_cntrct_crs2_snapshot`) for direct comparison.
*   **Action:** Perform a full outer join comparison between the migrated BigQuery table and the legacy snapshot table to identify any discrepancies.
*   **Pass/Fail Criterion:** The comparison query must return 0 rows, indicating no differences in data between the migrated and legacy target tables.

```sql
-- BigQuery: Identify data mismatches (assuming a unique key like cntrct_id)
SELECT
    'Mismatch' as status,
    COALESCE(bq.cntrct_id, ora.cntrct_id) as cntrct_id,
    bq.* EXCEPT(cntrct_id),
    ora.* EXCEPT(cntrct_id)
FROM
    `dw_bert_staging.sof_ta_cntrct_crs2` bq
FULL OUTER JOIN
    `dw_bert_staging.legacy_sof_ta_cntrct_crs2_snapshot` ora
ON
    bq.cntrct_id = ora.cntrct_id
WHERE
    NOT (
        bq.cntrct_id IS NOT DISTINCT FROM ora.cntrct_id AND
        bq.obj_version IS NOT DISTINCT FROM ora.obj_version AND
        bq.contract_number IS NOT DISTINCT FROM ora.contract_number AND
        bq.cntrct_template_id IS NOT DISTINCT FROM ora.cntrct_template_id AND
        bq.cntrct_validity_id IS NOT DISTINCT FROM ora.cntrct_validity_id AND
        bq.valid_from IS NOT DISTINCT FROM ora.valid_from AND
        bq.com_per_ext_rea_cv IS NOT DISTINCT FROM ora.com_per_ext_rea_cv AND
        bq.billcycle_id IS NOT DISTINCT FROM ora.billcycle_id AND
        bq.vo_code IS NOT DISTINCT FROM ora.vo_code AND
        bq.cntrct_start_date IS NOT DISTINCT FROM ora.cntrct_start_date AND
        bq.cntrct_st IS NOT DISTINCT FROM ora.cntrct_st AND
        bq.cntrct_parent IS NOT DISTINCT FROM ora.cntrct_parent AND
        bq.cntrct_ty IS NOT DISTINCT FROM ora.cntrct_ty AND
        bq.cost_centre IS NOT DISTINCT FROM ora.cost_centre AND
        bq.cost_centre_user IS NOT DISTINCT FROM ora.cost_centre_user AND
        bq.commitment_reference_date IS NOT DISTINCT FROM ora.commitment_reference_date AND
        bq.order_number IS NOT DISTINCT FROM ora.order_number AND
        bq.rv_num IS NOT DISTINCT FROM ora.rv_num
    );
-- The query should return 0 rows for a pass.
```

---

### Test Case 4: Transformation Correctness - Join Logic and `RV_NUM` Population

*   **Purpose:** To specifically validate the `LEFT OUTER JOIN` logic and the correct population of the `RV_NUM` column, especially when parent contracts are present, absent, or do not meet the `cr.cntrct_ty = 10` condition. This directly tests the Oracle `(+)` outer join translation.
*   **Setup:**
    1.  Prepare `dw_bert_staging.sof_ta_cntrct_crs` with specific test data scenarios for `c.cntrct_id` (child contract) and `cr.cntrct_id` (parent contract):
        *   **Scenario A (Parent exists, type 10):** `c.cntrct_parent` matches `cr.cntrct_id`, and `cr.cntrct_ty = 10`. (`RV_NUM` should be populated with `cr.contract_number`).
        *   **Scenario B (Parent exists, type not 10):** `c.cntrct_parent` matches `cr.cntrct_id`, but `cr.cntrct_ty <> 10`. (`RV_NUM` should be NULL).
        *   **Scenario C (No matching parent):** `c.cntrct_parent` has no corresponding `cr.cntrct_id`. (`RV_NUM` should be NULL).
        *   **Scenario D (Parent is NULL):** `c.cntrct_parent` is NULL. (`RV_NUM` should be NULL).
        *   **Scenario E (Parent exists, type 10, but `cr.contract_number` is NULL):** `c.cntrct_parent` matches `cr.cntrct_id`, `cr.cntrct_ty = 10`, but `cr.contract_number` is NULL. (`RV_NUM` should be NULL).
    2.  Ensure `c.cntrct_ty <> 10` for all test cases in `c` (as these are child contracts).
    3.  Run the migrated Airflow DAG.
*   **Action:** Query `dw_bert_staging.sof_ta_cntrct_crs2` for the `cntrct_id`s corresponding to the test scenarios and inspect the `RV_NUM` column.
*   **Pass/Fail Criterion:** The `RV_NUM` column must be populated according to the expected behavior for each scenario, matching the legacy job's output.

```sql
-- BigQuery: Example query to verify RV_NUM for specific cntrct_id
SELECT
    cntrct_id,
    cntrct_parent,
    rv_num
FROM
    `dw_bert_staging.sof_ta_cntrct_crs2`
WHERE
    cntrct_id IN (/* list of test cntrct_id from scenarios A-E */)
ORDER BY cntrct_id;

-- Expected results (example for specific cntrct_id values):
-- cntrct_id | cntrct_parent | rv_num
-- ----------|---------------|------------------------
-- 101       | 1000          | 'PARENT_CONTRACT_1000' (Scenario A)
-- 102       | 1001          | NULL                   (Scenario B: cr.cntrct_ty != 10)
-- 103       | 1002          | NULL                   (Scenario C: No matching cr.cntrct_id)
-- 104       | NULL          | NULL                   (Scenario D: c.cntrct_parent is NULL)
-- 105       | 1003          | NULL                   (Scenario E: cr.contract_number is NULL)
```

---

### Test Case 5: Transformation Correctness - Filter Logic (`c.cntrct_ty <> 10`)

*   **Purpose:** To verify that contracts with `c.cntrct_ty = 10` (frame contract parents) are correctly excluded from the target table, as specified in the design.
*   **Setup:**
    1.  Populate `dw_bert_staging.sof_ta_cntrct_crs` with test data including:
        *   Contracts where `cntrct_ty = 10`.
        *   Contracts where `cntrct_ty <> 10`.
    2.  Run the migrated Airflow DAG.
*   **Action:** Query `dw_bert_staging.sof_ta_cntrct_crs2` to check for the presence of any `cntrct_id` where the original `cntrct_ty` (from table `c`) was `10`.
*   **Pass/Fail Criterion:** No rows with `cntrct_ty = 10` (from the original `c` table) should exist in `dw_bert_staging.sof_ta_cntrct_crs2`. The count of such rows must be 0.

```sql
-- BigQuery: Check for excluded contracts
SELECT
    COUNT(*)
FROM
    `dw_bert_staging.sof_ta_cntrct_crs2`
WHERE
    cntrct_ty = 10;
-- Expected result: 0
```

---

### Test Case 6: Transformation Correctness - NULL Handling for Source Columns

*   **Purpose:** To ensure that NULL values in source columns (other than `cntrct_parent` and `cr.contract_number` already covered) are correctly propagated to the target table without unexpected transformations or errors.
*   **Setup:**
    1.  Populate `dw_bert_staging.sof_ta_cntrct_crs` with rows where various columns (e.g., `obj_version`, `contract_number`, `valid_from`, `cost_centre`) contain NULLs.
    2.  Ensure these rows also satisfy `c.cntrct_ty <> 10` and have a parent contract of type 10 (to ensure `RV_NUM` is populated correctly or NULL as expected).
    3.  Run the migrated Airflow DAG.
*   **Action:**
    1.  Select a sample of `cntrct_id`s from `dw_bert_staging.sof_ta_cntrct_crs2` that correspond to the test data with NULLs.
    2.  Compare the values in these columns with the original source data and the legacy target data.
*   **Pass/Fail Criterion:** NULL values in source columns must be preserved as NULLs in the corresponding target columns, matching the legacy job's behavior.

```sql
-- BigQuery: Example to check NULL propagation for specific columns
SELECT
    cntrct_id,
    obj_version,
    contract_number,
    valid_from,
    cost_centre,
    commitment_reference_date
FROM
    `dw_bert_staging.sof_ta_cntrct_crs2`
WHERE
    cntrct_id IN (/* list of test cntrct_id with NULLs in source */);

-- Manual inspection and comparison against the legacy output for the same cntrct_id.
```

---

### Test Case 7: External System Replacement - Carmen DB (`@pcrs1`) Verification

*   **Purpose:** To confirm that the migrated job's core `INSERT` logic does not attempt to access Carmen DB (`@pcrs1`), and that this aligns with the actual legacy `INSERT` statement's behavior. The design document mentions `@pcrs1` as a source, but the provided Oracle SQL snippet for the `INSERT` does not include it.
*   **Setup:**
    1.  Thoroughly review the complete legacy Oracle SQL script (`d_ausd_v_ta_cntrct_crs2.sql`) to confirm whether `@pcrs1` is referenced *within the `INSERT INTO sof$ta_cntrct_crs2 ... SELECT ...` statement*.
    2.  Review the migrated BigQuery SQL (`d_ausd_v_ta_cntrct_crs2.bqsql` and the Airflow DAG code).
*   **Action:**
    1.  If the legacy `INSERT` statement *did* use `@pcrs1` for populating `sof$ta_cntrct_crs2`, then the migrated BigQuery SQL is incomplete and this test fails. A strategy for Carmen DB data (e.g., federated query, pre-ingestion) must be implemented.
    2.  If the legacy `INSERT` statement *did NOT* use `@pcrs1` (as implied by the provided snippets), then verify that the BigQuery SQL also does not reference it.
*   **Pass/Fail Criterion:**
    *   **Pass:** The legacy `INSERT` statement for `sof$ta_cntrct_crs2` does not reference `@pcrs1`, AND the migrated BigQuery SQL also does not reference any external system for this specific `INSERT` operation beyond the explicitly migrated `sof_ta_cntrct_crs`.
    *   **Fail:** The legacy `INSERT` statement for `sof$ta_cntrct_crs2` *did* reference `@pcrs1`, but the migrated BigQuery SQL does not include this data source, leading to a critical functional difference.

---

### Test Case 8: External System Replacement - `dwtk_meldungen` Usage

*   **Purpose:** To verify that the `dwtk_meldungen` table, which was used in legacy for `s_datum` calculation, is correctly handled in the migration. The migrated BigQuery SQL comments out this logic. This test confirms that `s_datum` was indeed not used in the final `INSERT` into `sof$ta_cntrct_crs2`.
*   **Setup:**
    1.  Thoroughly review the complete legacy Oracle SQL script (`d_ausd_v_ta_cntrct_crs2.sql`) to confirm if the `s_datum` variable (derived from `dwtk_meldungen`) was ever used *within the `INSERT INTO sof$ta_cntrct_crs2 ... SELECT ...` statement*.
    2.  Review the migrated BigQuery SQL (`d_ausd_v_ta_cntrct_crs2.bqsql` and DAG code) to confirm `s_datum` logic is commented out and not used in the `INSERT`.
*   **Action:**
    1.  If `s_datum` was *not* used in the legacy `INSERT` statement, then the current BigQuery implementation (commenting it out) is correct.
    2.  If `s_datum` *was* used in the legacy `INSERT` statement, then the BigQuery implementation is functionally different and needs correction.
*   **Pass/Fail Criterion:**
    *   **Pass:** The legacy `INSERT` statement for `sof$ta_cntrct_crs2` does not use the `s_datum` variable, and the migrated BigQuery SQL correctly omits or comments out the `s_datum` calculation.
    *   **Fail:** The legacy `INSERT` statement *did* use `s_datum`, but the migrated BigQuery SQL does not, leading to a functional difference.

---

### Test Case 9: Idempotency and Truncate-Load Behavior

*   **Purpose:** To verify that running the Airflow DAG multiple times produces the same result in the target table, ensuring the `TRUNCATE` and `INSERT` operations work as expected and the job is idempotent.
*   **Setup:**
    1.  Populate `dw_bert_staging.sof_ta_cntrct_crs` with a consistent set of source data.
    2.  Run the migrated Airflow DAG once.
    3.  Record the row count and a checksum/hash of the target table `dw_bert_staging.sof_ta_cntrct_crs2`.
*   **Action:**
    1.  Run the migrated Airflow DAG a second time.
    2.  Query the row count and checksum/hash of `dw_bert_staging.sof_ta_cntrct_crs2`.
*   **Pass/Fail Criterion:** The row count and checksum/hash of the target table after the second run must be identical to those after the first run. This confirms the truncate-load pattern is working correctly.

```sql
-- BigQuery: Get row count and a simple checksum (e.g., sum of a unique ID column)
-- Assuming cntrct_id is unique and non-null for checksum.
SELECT
    COUNT(*) AS row_count,
    SUM(cntrct_id) AS checksum_cntrct_id -- Or a more robust hash of concatenated columns
FROM
    `dw_bert_staging.sof_ta_cntrct_crs2`;

-- Compare results from run 1 and run 2.
```

---

### Test Case 10: Airflow DAG Orchestration and Error Handling

*   **Purpose:** To verify that the Airflow DAG executes successfully under normal conditions and handles potential errors gracefully (e.g., source table not found, SQL syntax error), mimicking the error trapping of the legacy KornShell scripts.
*   **Setup:**
    1.  **Success Scenario:** Ensure all BigQuery source tables exist and contain valid data.
    2.  **Failure Scenario 1 (Source Table Missing):** Temporarily rename or drop `dw_bert_staging.sof_ta_cntrct_crs` in BigQuery.
    3.  **Failure Scenario 2 (SQL Syntax Error):** Introduce a deliberate syntax error into the `load_contract_data` task's SQL (e.g., `SELECT * FROM non_existent_column` or `INSERT INTO ... VALUES (1, 'a', 'b')` if schema expects `INT64`).
*   **Action:**
    1.  Trigger the Airflow DAG for the success scenario.
    2.  Trigger the Airflow DAG for Failure Scenario 1.
    3.  Trigger the Airflow DAG for Failure Scenario 2.
*   **Pass/Fail Criterion:**
    *   **Success Scenario:** The DAG must complete successfully, and the target table must be populated correctly (verified by other tests).
    *   **Failure Scenarios:** The DAG must fail gracefully, marking the relevant task(s) as failed, and Airflow's logging should capture the specific error message (e.g., "Table not found", "Syntax error"). This demonstrates that the Airflow orchestration correctly detects and reports issues, similar to how the KornShell scripts would trap errors.

---

### Test Case 11: Data Type Handling and Implicit Conversions

*   **Purpose:** To verify that data type conversions from Oracle to BigQuery (e.g., `DATE` to `DATE`, `NUMBER` to `INT64`/`BIGNUMERIC`, `VARCHAR2` to `STRING`) are handled correctly without data loss, truncation, or corruption.
*   **Setup:**
    1.  Populate `dw_bert_staging.sof_ta_cntrct_crs` with test data covering the full range of values for each column's data type:
        *   Max/min dates for `DATE` columns.
        *   Long strings (e.g., `contract_number`, `com_per_ext_rea_cv`) up to BigQuery's `STRING` limits.
        *   Large integers for `INT64` columns (e.g., `cntrct_id`, `obj_version`).
        *   NULLs for all nullable columns.
    2.  Run the migrated Airflow DAG.
*   **Action:**
    1.  Select specific rows from `dw_bert_staging.sof_ta_cntrct_crs2` and inspect the values of all columns.
    2.  Compare these values against the original Oracle source data and the legacy target data.
*   **Pass/Fail Criterion:** All values must be accurately represented in the BigQuery target table, matching the legacy output. No truncation of strings, no overflow of numbers, no incorrect date conversions, and correct handling of NULLs.

```sql
-- BigQuery: Select a sample of data to inspect for type handling
SELECT
    cntrct_id,
    contract_number,
    valid_from,
    commitment_reference_date,
    obj_version,
    billcycle_id,
    com_per_ext_rea_cv,
    rv_num
FROM
    `dw_bert_staging.sof_ta_cntrct_crs2`
WHERE
    cntrct_id IN (/* list of test cntrct_id with diverse data types and edge values */);

-- Manual inspection and comparison with legacy output.
```