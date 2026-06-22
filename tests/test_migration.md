As a senior data-migration QA engineer, I have reviewed the migration design document and the generated BigQuery SQL and Airflow DAG for the `BERT_V_TA_DISC_ZUSGF` job. The migration aims to re-implement an Oracle PL/SQL job in BigQuery, orchestrated by Airflow.

The core transformation involves aggregating and concatenating discount descriptions from `sof$ta_discount` into `sof$ta_disc_zusgf`, with a critical implicit 500-character length limit for the final concatenated string. The BigQuery implementation uses `STRING_AGG` and `SUBSTR` for this.

Below are the detailed migration validation tests, covering output parity, transformation correctness, external system replacements, and data quality assertions.

---

## Migration Validation Tests for BERT_V_TA_DISC_ZUSGF

### Test Case 1: End-to-End Output Parity (Golden Record Comparison)

*   **Purpose:** To verify that the migrated BigQuery job produces an identical final output table (`sof_ta_disc_zusgf`) compared to the legacy Oracle job for a given set of source data. This is the primary test for behavioral equivalence.
*   **Setup:**
    1.  **Source Data Preparation:** Select a representative, diverse dataset from the legacy Oracle `sof$ta_discount` and `isbert_schema.dwtk_meldungen` tables. This dataset should include:
        *   Contracts with a single discount.
        *   Contracts with multiple discounts.
        *   Contracts with no discounts (to test `LEFT JOIN` behavior).
        *   Discounts with `NULL` `rabatt` or `rabatthoehe`.
        *   Discounts that, when concatenated, result in `rabatt_alle` strings both under and over 500 characters (for critical length limit validation).
        *   `dwtk_meldungen` entries that result in a non-default `v_datum`.
    2.  **Legacy Execution:** Execute the original Oracle PL/SQL job (`d_ausd_v_ta_disc_zusgf.sql`) with this prepared source data.
    3.  **Legacy Output Capture:** Export the entire content of the resulting Oracle `sof$ta_disc_zusgf` table into a "golden record" CSV or JSON file.
    4.  **Migrated Source Data Loading:** Load the *exact same* prepared source data into the BigQuery `dwtk_meldungen` and `sof_ta_discount` tables.
    5.  **Migrated Job Deployment:** Ensure the Airflow DAG (`dw_bert_ausd_v_ta_disc_zusgf.py`) and the BigQuery SQL script (`d_ausd_v_ta_disc_zusgf_bq.sql`) are deployed to the GCP environment.
*   **Action:**
    1.  Trigger the Airflow DAG `dw_bert_ausd_v_ta_disc_zusgf` in the GCP environment.
    2.  Once the DAG completes successfully, query the BigQuery `sof_ta_disc_zusgf` table.
    3.  Export the content of the BigQuery `sof_ta_disc_zusgf` table.
    4.  Compare the exported BigQuery output with the "golden record" from Oracle.
*   **Pass/Fail Criterion:**
    *   The row count of `sof_ta_disc_zusgf` in BigQuery must be identical to the Oracle golden record.
    *   All column values (`cntrct_id`, `cntrct_obj_version`, `disc_vector_ty`, `rabatt_alle`) for every row in the BigQuery `sof_ta_disc_zusgf` table must be identical to the corresponding row in the Oracle golden record. A robust data comparison tool should be used to ensure exact byte-for-byte or value-for-value match, accounting for potential data type differences (e.g., `NUMBER` vs `INT64` for exact values).

### Test Case 2: `v_datum` Derivation Correctness

*   **Purpose:** To verify that the `v_datum` variable, which determines the processing date, is derived correctly in BigQuery, matching the Oracle logic.
*   **Setup:**
    1.  **Oracle Setup:**
        *   Insert a row into `isbert_schema.dwtk_meldungen` with `job_kennung = 'BERT_DROP_TEMP_TABLE'` and `timecreated` set to a specific date (e.g., `SYSDATE - 5`).
        *   Insert other rows with different `job_kennung` or older `timecreated` values.
        *   Insert a row where `timecreated` is `NULL` for `BERT_DROP_TEMP_TABLE`.
        *   Ensure `isbert_schema.dwtk_meldungen` is empty.
    2.  **BigQuery Setup:** Replicate the exact same data scenarios in the BigQuery `dwtk_meldungen` table.
*   **Action:**
    1.  **Oracle:** Execute the `SELECT COALESCE(TO_CHAR(MAX(m.timecreated), 'YYYYMMDD'), '19000101') FROM isbert_schema.dwtk_meldungen m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';` query for each setup scenario.
    2.  **BigQuery:** Execute only the `DECLARE v_datum STRING DEFAULT (...)` part of the BigQuery SQL for each setup scenario and retrieve the `v_datum` value.
*   **Pass/Fail Criterion:**
    *   For each scenario, the `v_datum` value derived in BigQuery must be identical to the value derived from Oracle.
    *   Specifically:
        *   If `MAX(timecreated)` exists for `BERT_DROP_TEMP_TABLE`, `v_datum` should be `YYYYMMDD` format of that date.
        *   If `MAX(timecreated)` is `NULL` or no rows match `BERT_DROP_TEMP_TABLE`, `v_datum` should default to `'19000101'`.

### Test Case 3: `TRUNCATE TABLE` Behavior

*   **Purpose:** To verify that the target table `sof_ta_disc_zusgf` is correctly truncated (emptied) before new data is inserted, matching the Oracle behavior.
*   **Setup:**
    1.  Populate the BigQuery `sof_ta_disc_zusgf` table with a significant amount of dummy data (e.g., 1000 rows).
    2.  Ensure `sof_ta_discount` and `dwtk_meldungen` contain data that will result in new rows being inserted.
*   **Action:**
    1.  Trigger the Airflow DAG `dw_bert_ausd_v_ta_disc_zusgf`.
    2.  Immediately after the `TRUNCATE TABLE` statement (if testing granularly, or by observing the final state), query the row count of `sof_ta_disc_zusgf`.
*   **Pass/Fail Criterion:**
    *   The job must complete successfully.
    *   The final row count of `sof_ta_disc_zusgf` must reflect only the data inserted by the current job run, not the initial dummy data plus the new data. This implicitly confirms the truncation occurred.

### Test Case 4: `STRING_AGG` Concatenation and Ordering

*   **Purpose:** To verify that discount descriptions are concatenated correctly, separated by `, `, and ordered alphabetically as specified by `ORDER BY rabatt_text` in BigQuery.
*   **Setup:**
    1.  Populate BigQuery `sof_ta_discount` with data for a single `cntrct_id`/`cntrct_obj_version` combination that has multiple discounts with varying `rabatt` values (e.g., "A-Discount", "C-Discount", "B-Discount").
    2.  Include cases where `rabatt` values are identical but `rabatthoehe` differs.
*   **Action:**
    1.  Run the migrated BigQuery job.
    2.  Query `sof_ta_disc_zusgf` for the specific `cntrct_id`/`cntrct_obj_version` combination.
    3.  Inspect the `rabatt_alle` column.
*   **Pass/Fail Criterion:**
    *   The `rabatt_alle` string must contain all individual discount descriptions (`rabatt || ' (' || rabatthoehe || '%)'`) for that contract, separated by `, `.
    *   The individual discount descriptions within `rabatt_alle` must be sorted alphabetically based on their `rabatt_text` value (e.g., "A-Discount (10%)", "B-Discount (5%)", "C-Discount (15%)").
    *   **Note on Oracle Parity:** If the original Oracle PL/SQL did not guarantee order, this BigQuery behavior is an improvement. If Oracle did guarantee order, this test ensures the BigQuery order matches. For strict parity, the `ORDER BY` clause in `STRING_AGG` should reflect the original Oracle ordering (if any).

### Test Case 5: `STRING_AGG` Length Limit (500 characters) - CRITICAL

*   **Purpose:** To verify the 500-character length limit for `rabatt_alle` is correctly applied, matching Oracle's `VARCHAR2(500)` behavior. The current BigQuery code has an intermediate `SUBSTR` which is a potential deviation from standard Oracle `VARCHAR2` column behavior.
*   **Setup:**
    1.  **Case A (No Truncation):** Populate `sof_ta_discount` with data such that the final concatenated `rabatt_alle` string (without any truncation) is less than or equal to 500 characters.
    2.  **Case B (Final Truncation):** Populate `sof_ta_discount` with data such that the final concatenated `rabatt_alle` string (without any truncation) is *greater* than 500 characters.
    3.  **Case C (Individual Part Truncation - Potential Deviation):** Populate `sof_ta_discount` with a single row where `rabatt` is an extremely long string (e.g., 600 characters). This will make the individual `rabatt_text` (`rabatt || ' (' || rabatthoehe || '%)'`) exceed 500 characters.
*   **Action:**
    1.  Run the legacy Oracle job with the test data for each case. Record the `rabatt_alle` output and its length.
    2.  Run the migrated BigQuery job with the test data for each case. Record the `rabatt_alle` output and its length.
    3.  Compare the `rabatt_alle` values and their lengths between Oracle and BigQuery for each case.
*   **Pass/Fail Criterion:**
    *   **Case A:** `rabatt_alle` values and lengths must be identical between Oracle and BigQuery.
    *   **Case B:** `rabatt_alle` values must be identical between Oracle and BigQuery, both truncated to exactly 500 characters.
    *   **Case C (Critical Deviation Check):**
        *   **Expected Oracle behavior:** An Oracle `VARCHAR2(500)` column would typically truncate the *final* string if it exceeds 500 characters upon insertion. It would *not* truncate individual parts of a string being built *before* the final concatenation. So, if an individual `rabatt_text` is 600 chars, the final `rabatt_alle` would be the first 500 chars of that 600-char string.
        *   **Current BigQuery code behavior:** The `prepared_discounts` CTE includes `SUBSTR(CONCAT(rabatt, ' (', CAST(rabatthoehe AS STRING), '%)'), 1, 500) AS rabatt_text`. This means individual discount descriptions are truncated to 500 characters *before* aggregation. Then, the final `SELECT` statement applies `SUBSTR(ad.rabatt_alle, 1, 500) AS rabatt_alle` *again*. This double truncation will likely produce a different result than Oracle.
        *   **Pass/Fail:** This test case is expected to **FAIL** if the BigQuery output for Case C differs from Oracle. The `SUBSTR` in the `prepared_discounts` CTE should be removed to align with the likely Oracle behavior of truncating only the *final* aggregated string to 500 characters. If the `SUBSTR` in `prepared_discounts` is removed, then the test passes if the final `rabatt_alle` matches Oracle's output (truncated to 500 chars).

### Test Case 6: `NULL` Handling in Discount Fields

*   **Purpose:** To verify that `NULL` values in `rabatt` or `rabatthoehe` are handled correctly during concatenation, matching Oracle's `||` operator behavior (which treats `NULL` as an empty string).
*   **Setup:**
    1.  Populate BigQuery `sof_ta_discount` with rows for a single `cntrct_id`/`cntrct_obj_version` where:
        *   `rabatt` is `NULL`, `rabatthoehe` has a value (e.g., 10).
        *   `rabatt` has a value (e.g., 'Special'), `rabatthoehe` is `NULL`.
        *   Both `rabatt` and `rabatthoehe` are `NULL`.
        *   A mix of `NULL` and non-`NULL` values for multiple discounts for the same contract.
*   **Action:**
    1.  Run the migrated BigQuery job.
    2.  Query `sof_ta_disc_zusgf` for the relevant `cntrct_id`s.
    3.  Inspect the `rabatt_alle` column.
*   **Pass/Fail Criterion:**
    *   The job must complete successfully without errors.
    *   For `rabatt` is `NULL`, `rabatthoehe` is 10: `rabatt_alle` should contain ` ' (10%)'`.
    *   For `rabatt` is 'Special', `rabatthoehe` is `NULL`: `rabatt_alle` should contain `'Special ()'`.
    *   For both `rabatt` and `rabatthoehe` are `NULL`: `rabatt_alle` should contain `' ()'`.
    *   The concatenated string should match the expected Oracle output for `NULL` handling.

### Test Case 7: Empty Source Tables

*   **Purpose:** To verify the job handles scenarios where source tables are empty gracefully, without errors, and produces an empty target table.
*   **Setup:**
    1.  Ensure both BigQuery `dwtk_meldungen` and `sof_ta_discount` tables are completely empty.
*   **Action:**
    1.  Trigger the Airflow DAG `dw_bert_ausd_v_ta_disc_zusgf`.
    2.  After completion, query the row count of `sof_ta_disc_zusgf`.
*   **Pass/Fail Criterion:**
    *   The Airflow DAG must complete successfully (green status) without any task failures.
    *   The BigQuery `sof_ta_disc_zusgf` table must be empty (row count = 0).
    *   The `v_datum` variable should correctly default to `'19000101'`.

### Test Case 8: Row Count Parity

*   **Purpose:** To verify that the total number of rows inserted into the target table `sof_ta_disc_zusgf` in BigQuery matches the number of rows in the legacy Oracle `sof$ta_disc_zusgf` for a given dataset.
*   **Setup:**
    1.  Use the same representative dataset as in Test Case 1.
    2.  Run the legacy Oracle job and record `COUNT(*)` from `sof$ta_disc_zusgf`.
    3.  Load the same data into BigQuery source tables.
*   **Action:**
    1.  Run the migrated BigQuery job via Airflow.
    2.  Execute `SELECT COUNT(*) FROM `gcp_project_id.dwh_prod.sof_ta_disc_zusgf`;` in BigQuery.
*   **Pass/Fail Criterion:**
    *   The `COUNT(*)` from BigQuery `sof_ta_disc_zusgf` must be exactly equal to the `COUNT(*)` from the Oracle `sof$ta_disc_zusgf`.

### Test Case 9: Schema Parity and Data Types

*   **Purpose:** To verify that the schema (column names, data types) of the target table `sof_ta_disc_zusgf` in BigQuery correctly reflects the migrated Oracle schema.
*   **Setup:** N/A (this is a schema-level check).
*   **Action:**
    1.  Inspect the schema of the BigQuery `sof_ta_disc_zusgf` table using BigQuery UI or `bq show --schema` command.
    2.  Compare it against the expected BigQuery types based on the Oracle source types.
*   **Pass/Fail Criterion:**
    *   The BigQuery `sof_ta_disc_zusgf` table must have the following columns with the specified types:
        *   `cntrct_id`: `INT64` (corresponding to Oracle `NUMBER(10)`)
        *   `cntrct_obj_version`: `INT64` (corresponding to Oracle `NUMBER(10)`)
        *   `disc_vector_ty`: `STRING` (corresponding to Oracle `VARCHAR2`)
        *   `rabatt_alle`: `STRING` (corresponding to Oracle `VARCHAR2(500)`)
    *   No unexpected columns should be present.

### Test Case 10: Airflow Orchestration and Logging

*   **Purpose:** To verify that the Airflow DAG executes successfully, handles task dependencies (though minimal here), and integrates with Cloud Logging for observability.
*   **Setup:**
    1.  Deploy the `dw_bert_ausd_v_ta_disc_zusgf.py` DAG to a Cloud Composer environment.
    2.  Ensure the `google_cloud_default` Airflow connection is correctly configured with permissions to access BigQuery.
*   **Action:**
    1.  Manually trigger the `dw_bert_ausd_v_ta_disc_zusgf` DAG from the Airflow UI.
    2.  Monitor the DAG run status and task logs.
*   **Pass/Fail Criterion:**
    *   The Airflow DAG must run to completion with a "success" status.
    *   The `execute_discount_concatenation` task must complete successfully.
    *   Task logs must be accessible via the Airflow UI and correctly forwarded to Cloud Logging, containing relevant BigQuery job IDs and execution details.
    *   No Airflow or BigQuery errors should be reported in the logs.

---
**Summary of Critical Findings and Recommendations:**

The most critical area for validation is the `STRING_AGG` length limit (Test Case 5). The current BigQuery SQL includes `SUBSTR(CONCAT(...), 1, 500)` within the `prepared_discounts` CTE, which truncates individual discount descriptions *before* aggregation. This is a likely deviation from Oracle's `VARCHAR2(500)` column behavior, which would typically apply the 500-character limit to the *final* aggregated string.

**Recommendation:** Remove `SUBSTR(..., 1, 500)` from the `prepared_discounts` CTE. The final `SUBSTR(ad.rabatt_alle, 1, 500)` in the main `SELECT` statement is sufficient to enforce the 500-character limit on the aggregated string, aligning with standard `VARCHAR2` column behavior. This change should be made and then Test Case 5 re-executed to confirm parity.