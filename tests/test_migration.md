The following migration validation tests are designed to ensure the BigQuery implementation of `r_ausd_v_ta_inv_assign.ksh` is behaviourally equivalent to its legacy Oracle/KornShell counterpart.

**Pre-requisites for all tests:**

*   All BigQuery DDLs for logging tables (`job_log`, `job_error_log`, `job_table`) are deployed in `your_project_id.your_dataset_id`.
*   All helper stored procedures (`DWMSG_MeldeFehler`, `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_Fehlerbehandlung`, `DWMSG_SetzeStatusOK`) are deployed in `your_project_id.your_dataset_id`.
*   The core stored procedure `k_ausd_v_ta_inv_assign` is deployed in `your_project_id.your_dataset_id`.
*   The wrapper stored procedure `Vertragsdatenabgleich` is deployed in `your_project_id.your_dataset_id`.
*   BigQuery source tables `your_project_id.your_dataset_id.dwtk_meldungen` and `your_project_id.your_dataset_id.cds$ta_inv_assignment` exist with the following schemas:
    *   `dwtk_meldungen`: `timecreated` (TIMESTAMP), `job_kennung` (STRING)
    *   `cds$ta_inv_assignment`: `cntrct_id` (INT64), `inv_definition_id` (INT64), `insert_at` (DATE), `modified_at` (DATE), `valid_from` (DATE), `valid_to` (DATE), `is_production` (INT64)
*   BigQuery target table `your_project_id.your_dataset_id.sof$ta_inv_assign` exists with the schema:
    *   `sof$ta_inv_assign`: `cntrct_id` (INT64), `inv_definition_id` (INT64)
*   All test data for source tables (`dwtk_meldungen`, `cds$ta_inv_assignment`) is loaded into BigQuery, mirroring the Oracle source data used for legacy runs.
*   The `p_job_id` parameter for the wrapper SP `Vertragsdatenabgleich` will be `BERT_V_TA_INV_ASSIGN` to match the legacy `JobKennung`.

---

## Test Case 1: Successful End-to-End Execution and Output Parity

**Purpose:**
To verify that the migrated BigQuery job executes successfully, produces the exact same output data in the target table as the legacy job, and correctly logs its success. This covers output parity, core transformation, and basic logging.

**Setup:**
1.  **Legacy System:**
    *   Prepare `isbert_schema.dwtk_meldungen` with a `timecreated` entry for `job_kennung = 'BERT_DROP_TEMP_TABLE'` (e.g., `2023-01-15 10:00:00`).
    *   Populate `cds$ta_inv_assignment` with a diverse set of records covering all filter conditions (match, no match, NULLs, different dates, `is_production=1` and `is_production=0`).
    *   Run the legacy job `r_ausd_v_ta_inv_assign.ksh`.
    *   Capture the final state of `sof$ta_inv_assign` (e.g., `SELECT cntrct_id, inv_definition_id FROM sof$ta_inv_assign ORDER BY 1, 2;`).
    *   Capture the legacy job's log file content.
2.  **BigQuery System:**
    *   Replicate the exact same data into `your_project_id.your_dataset_id.dwtk_meldungen` and `your_project_id.your_dataset_id.cds$ta_inv_assignment`.
    *   Ensure `your_project_id.your_dataset_id.sof$ta_inv_assign` is empty before execution (the job will truncate it).
    *   Clear `job_log`, `job_error_log`, `job_table` for a clean run.

**Action:**
Execute the BigQuery wrapper stored procedure:
```sql
CALL `your_project_id.your_dataset_id.Vertragsdatenabgleich`(
  'BERT_V_TA_INV_ASSIGN',
  CURRENT_DATE() -- p_reporting_date, used for job_table metadata only
);
```

**Pass/Fail Criterion:**
1.  **Output Parity:** The data in `your_project_id.your_dataset_id.sof$ta_inv_assign` must be identical to the data captured from the legacy `sof$ta_inv_assign` table.
    ```sql
    -- Example assertion (pseudo-code for comparison)
    -- Assuming legacy_sof_ta_inv_assign is a temporary table with legacy output
    SELECT COUNT(*) FROM `your_project_id.your_dataset_id.sof$ta_inv_assign` EXCEPT DISTINCT SELECT COUNT(*) FROM `legacy_sof_ta_inv_assign`; -- Should be 0
    SELECT COUNT(*) FROM `legacy_sof_ta_inv_assign` EXCEPT DISTINCT SELECT COUNT(*) FROM `your_project_id.your_dataset_id.sof$ta_inv_assign`; -- Should be 0
    ```
2.  **Row Count:** The number of rows inserted into `sof$ta_inv_assign` (captured by `ROW_COUNT()` in BQ and logged in `job_table`) must match the row count from the legacy run.
    ```sql
    SELECT processed_rows FROM `your_project_id.your_dataset_id.job_table` WHERE job_id = 'BERT_V_TA_INV_ASSIGN' AND status = 'SUCCESS';
    -- Compare this value to the legacy job's reported row count.
    ```
3.  **Job Status:** The `job_table` must contain an entry for `BERT_V_TA_INV_ASSIGN` with `status = 'SUCCESS'` and `end_time` populated.
    ```sql
    SELECT status FROM `your_project_id.your_dataset_id.job_table` WHERE job_id = 'BERT_V_TA_INV_ASSIGN' ORDER BY entry_nr DESC LIMIT 1; -- Should be 'SUCCESS'
    ```
4.  **Logging:** The `job_log` table must contain `INFO` entries indicating job start, date determination, truncation, insertion, and successful completion, without any `ERROR` entries in `job_log` or `job_error_log`.
    ```sql
    SELECT COUNT(*) FROM `your_project_id.your_dataset_id.job_log` WHERE job_id = 'BERT_V_TA_INV_ASSIGN' AND log_level = 'ERROR'; -- Should be 0
    SELECT COUNT(*) FROM `your_project_id.your_dataset_id.job_error_log` WHERE job_id = 'BERT_V_TA_INV_ASSIGN'; -- Should be 0
    ```

---

## Test Case 2: Transformation Correctness - Filtering Logic (Date and `is_production`)

**Purpose:**
To specifically validate the complex filtering logic involving dates (`insert_at`, `modified_at`, `valid_from`, `valid_to`) and the `is_production` flag, including NULL handling for dates. This covers transformation correctness and NULL handling.

**Setup:**
1.  **Legacy System:**
    *   Set `isbert_schema.dwtk_meldungen` to yield a specific `v_datum` (e.g., `2023-01-01`).
    *   Populate `cds$ta_inv_assignment` with records designed to test each part of the WHERE clause:
        *   `insert_at` before, on, and after `v_datum`.
        *   `modified_at` IS NULL, before, on, and after `v_datum`.
        *   `valid_from` before, on, and after `v_datum`.
        *   `valid_to` IS NULL, before, on, and after `v_datum`.
        *   `is_production = 1` and `is_production = 0`.
        *   Combinations of the above to ensure all conditions are met or failed correctly.
    *   Run the legacy job and capture `sof$ta_inv_assign`.
2.  **BigQuery System:**
    *   Replicate the exact same data into `your_project_id.your_dataset_id.dwtk_meldungen` and `your_project_id.your_dataset_id.cds$ta_inv_assignment`.
    *   Clear target and log tables.

**Action:**
Execute the BigQuery wrapper stored procedure:
```sql
CALL `your_project_id.your_dataset_id.Vertragsdatenabgleich`(
  'BERT_V_TA_INV_ASSIGN',
  CURRENT_DATE()
);
```

**Pass/Fail Criterion:**
1.  **Output Parity:** The data in `your_project_id.your_dataset_id.sof$ta_inv_assign` must be identical to the data captured from the legacy `sof$ta_inv_assign` table. This confirms all filter conditions, including NULLs and date comparisons, are correctly translated.
    ```sql
    -- Example assertion (pseudo-code)
    SELECT COUNT(*) FROM `your_project_id.your_dataset_id.sof$ta_inv_assign` EXCEPT DISTINCT SELECT COUNT(*) FROM `legacy_sof_ta_inv_assign`; -- Should be 0
    SELECT COUNT(*) FROM `legacy_sof_ta_inv_assign` EXCEPT DISTINCT SELECT COUNT(*) FROM `your_project_id.your_dataset_id.sof$ta_inv_assign`; -- Should be 0
    ```
2.  **Row Count:** The `processed_rows` in `job_table` must match the legacy job's output.

---

## Test Case 3: Transformation Correctness - `v_datum` Default Value

**Purpose:**
To verify that the `v_datum` variable correctly defaults to '19000101' when no matching `timecreated` entry is found in `dwtk_meldungen`. This covers transformation correctness and edge cases.

**Setup:**
1.  **Legacy System:**
    *   Ensure `isbert_schema.dwtk_meldungen` contains *no* entries for `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
    *   Populate `cds$ta_inv_assignment` with records that would *only* be included if `v_datum` is '19000101' (e.g., `insert_at` is '1900-01-01', `modified_at` is NULL or after '1900-01-01', etc.).
    *   Run the legacy job and capture `sof$ta_inv_assign`.
2.  **BigQuery System:**
    *   Replicate the exact same data into `your_project_id.your_dataset_id.dwtk_meldungen` (empty for the specific `job_kennung`) and `your_project_id.your_dataset_id.cds$ta_inv_assignment`.
    *   Clear target and log tables.

**Action:**
Execute the BigQuery wrapper stored procedure:
```sql
CALL `your_project_id.your_dataset_id.Vertragsdatenabgleich`(
  'BERT_V_TA_INV_ASSIGN',
  CURRENT_DATE()
);
```

**Pass/Fail Criterion:**
1.  **Output Parity:** The data in `your_project_id.your_dataset_id.sof$ta_inv_assign` must be identical to the data captured from the legacy `sof$ta_inv_assign` table. This confirms the '19000101' default is correctly applied.
2.  **Logging:** The `job_log` table should contain an `INFO` message confirming `v_datum_str` was '19000101'.
    ```sql
    SELECT COUNT(*) FROM `your_project_id.your_dataset_id.job_log`
    WHERE job_id = 'BERT_V_TA_INV_ASSIGN' AND message LIKE '%Determined v_datum_str: 19000101%'; -- Should be 1
    ```

---

## Test Case 4: External System Replacements - `TRUNCATE TABLE`

**Purpose:**
To verify that the `TRUNCATE TABLE` operation, which was originally called via `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`, is correctly executed in BigQuery using native `TRUNCATE TABLE`. This covers external-system replacements.

**Setup:**
1.  **BigQuery System:**
    *   Populate `your_project_id.your_dataset_id.sof$ta_inv_assign` with some dummy data (e.g., 5 rows).
    *   Populate `dwtk_meldungen` and `cds$ta_inv_assignment` such that the job will insert at least one row.
    *   Clear `job_log`, `job_error_log`, `job_table`.

**Action:**
Execute the BigQuery wrapper stored procedure:
```sql
CALL `your_project_id.your_dataset_id.Vertragsdatenabgleich`(
  'BERT_V_TA_INV_ASSIGN',
  CURRENT_DATE()
);
```

**Pass/Fail Criterion:**
1.  **Truncation Success:** The `sof$ta_inv_assign` table must contain only the rows inserted by the job, and no remnants of the pre-existing dummy data. The row count should match the expected number of inserted rows, not the initial dummy rows plus inserted rows.
    ```sql
    -- Verify the final row count matches the expected inserted rows.
    -- If the job inserted 3 rows, and we started with 5 dummy rows, the final count should be 3.
    SELECT COUNT(*) FROM `your_project_id.your_dataset_id.sof$ta_inv_assign`;
    ```
2.  **Logging:** The `job_log` table should contain an `INFO` message indicating the truncation occurred.
    ```sql
    SELECT COUNT(*) FROM `your_project_id.your_dataset_id.job_log`
    WHERE job_id = 'BERT_V_TA_INV_ASSIGN' AND message LIKE '%Truncating target table sof$ta_inv_assign%'; -- Should be 1
    ```

---

## Test Case 5: Error Handling and Logging

**Purpose:**
To verify that the migrated job correctly handles errors, logs them to `job_error_log` and `job_log`, and updates the `job_table` status to 'FAILED', mimicking the legacy `trap` and `DWMSG_MeldeFehler` behavior. This covers external-system replacements (error framework) and data quality (logging).

**Setup:**
1.  **BigQuery System:**
    *   Introduce a controlled error condition in the `k_ausd_v_ta_inv_assign` stored procedure. For example, temporarily rename `cds$ta_inv_assignment` to `cds$ta_inv_assignment_BAK` so the `INSERT` statement fails due to a missing table.
    *   Clear `job_log`, `job_error_log`, `job_table`.

**Action:**
Execute the BigQuery wrapper stored procedure:
```sql
CALL `your_project_id.your_dataset_id.Vertragsdatenabgleich`(
  'BERT_V_TA_INV_ASSIGN',
  CURRENT_DATE()
);
```
(After the test, revert the table name change.)

**Pass/Fail Criterion:**
1.  **Job Status:** The `job_table` must contain an entry for `BERT_V_TA_INV_ASSIGN` with `status = 'FAILED'` and `end_time` populated.
    ```sql
    SELECT status FROM `your_project_id.your_dataset_id.job_table` WHERE job_id = 'BERT_V_TA_INV_ASSIGN' ORDER BY entry_nr DESC LIMIT 1; -- Should be 'FAILED'
    ```
2.  **Error Logging:** The `job_error_log` table must contain at least one entry for the job run, detailing the error (e.g., "Not found: Table your_project_id.your_dataset_id.cds$ta_inv_assignment").
    ```sql
    SELECT COUNT(*) FROM `your_project_id.your_dataset_id.job_error_log` WHERE job_id = 'BERT_V_TA_INV_ASSIGN'; -- Should be > 0
    SELECT error_message FROM `your_project_id.your_dataset_id.job_error_log` WHERE job_id = 'BERT_V_TA_INV_ASSIGN' ORDER BY timestamp DESC LIMIT 1; -- Should contain error details
    ```
3.  **General Logging:** The `job_log` table must contain an `ERROR` level entry corresponding to the failure.
    ```sql
    SELECT COUNT(*) FROM `your_project_id.your_dataset_id.job_log` WHERE job_id = 'BERT_V_TA_INV_ASSIGN' AND log_level = 'ERROR'; -- Should be > 0
    ```
4.  **No Data Inserted (if error before insert):** If the error occurred before or during the `INSERT` statement, `sof$ta_inv_assign` should remain empty or in its pre-error state (depending on when the `TRUNCATE` occurred relative to the error).

---

## Test Case 6: Data Quality - Schema and Data Types

**Purpose:**
To ensure that the target table `sof$ta_inv_assign` in BigQuery has the correct schema and data types, matching the legacy Oracle table. This covers schema assertions.

**Setup:**
1.  **Legacy System:**
    *   Obtain the schema (column names and data types) for `sof$ta_inv_assign` from Oracle.
2.  **BigQuery System:**
    *   Ensure the `sof$ta_inv_assign` table is created in BigQuery.

**Action:**
Query the schema of the BigQuery target table.

```sql
SELECT
  column_name,
  data_type
FROM
  `your_project_id.your_dataset_id.INFORMATION_SCHEMA.COLUMNS`
WHERE
  table_name = 'sof$ta_inv_assign'
ORDER BY
  ordinal_position;
```

**Pass/Fail Criterion:**
1.  The `column_name` and `data_type` for `cntrct_id` and `inv_definition_id` in BigQuery must match the expected types based on the Oracle schema (e.g., `INT64` for Oracle `NUMBER`).
    *   `cntrct_id`: `INT64`
    *   `inv_definition_id`: `INT64`

---

## Test Case 7: Empty Source Table (`cds$ta_inv_assignment`)

**Purpose:**
To verify the job handles an empty source `cds$ta_inv_assignment` table gracefully, resulting in an empty target table and correct logging. This covers transformation correctness and edge cases.

**Setup:**
1.  **Legacy System:**
    *   Ensure `isbert_schema.dwtk_meldungen` has a valid `timecreated` entry.
    *   Ensure `cds$ta_inv_assignment` is completely empty.
    *   Run the legacy job and capture `sof$ta_inv_assign` (should be empty).
2.  **BigQuery System:**
    *   Replicate the `dwtk_meldungen` data.
    *   Ensure `your_project_id.your_dataset_id.cds$ta_inv_assignment` is empty.
    *   Clear target and log tables.

**Action:**
Execute the BigQuery wrapper stored procedure:
```sql
CALL `your_project_id.your_dataset_id.Vertragsdatenabgleich`(
  'BERT_V_TA_INV_ASSIGN',
  CURRENT_DATE()
);
```

**Pass/Fail Criterion:**
1.  **Output Parity:** `your_project_id.your_dataset_id.sof$ta_inv_assign` must be empty.
    ```sql
    SELECT COUNT(*) FROM `your_project_id.your_dataset_id.sof$ta_inv_assign`; -- Should be 0
    ```
2.  **Row Count:** The `processed_rows` in `job_table` must be `0`.
    ```sql
    SELECT processed_rows FROM `your_project_id.your_dataset_id.job_table` WHERE job_id = 'BERT_V_TA_INV_ASSIGN' AND status = 'SUCCESS'; -- Should be 0
    ```
3.  **Job Status:** `job_table` status should be 'SUCCESS'.
4.  **Logging:** `job_log` should show successful completion with 0 rows inserted.

---