As a senior data-migration QA engineer, I've analyzed the migration design document and the generated code for `DW.BERT_AUSD_V_TA_VERTRAG_TMP`. The following test cases are designed to ensure the migrated job is behaviourally equivalent to its legacy Oracle/UC4 counterpart.

---

## Migration Validation Tests: DW.BERT_AUSD_V_TA_VERTRAG_TMP

### Test Environment Setup (Prerequisites for all tests)

Before executing any tests, ensure the following:

1.  **Identical Source Data:** All source tables (`isbert_schema.dwtk_meldungen`, `sof$ta_cntrct_crs3`, `sof$ta_bp_ref`, `sof$ta_inv_acc`, `dwh$vi_s_rd_segment`, `sof$ta_notice`, `sof$ta_barrier_zusgf`, `sof$ta_cntrct_templ`, `sof$ta_cntrct_valid`, `sof$ta_period`, `sof$ta_vvl_upgrade`, `sof$ta_apn_ve`, `sof$ta_action_assoc`, `sof$vi_c_bfc`) have been loaded with an identical, representative dataset in both the legacy Oracle environment and the target BigQuery environment. This dataset should include:
    *   Typical data scenarios.
    *   Edge cases for `NULL` values in all nullable columns.
    *   Data that triggers all branches of `CASE` statements (e.g., `cntrct_st` values 5, 6, and others; `inv_pay_ty_cv` values 1-4 and others; `inv_media_cv` values 1-6 and others).
    *   Data specifically designed to test the `upgradeberechtigt` logic, including various `number_time_measurement`, `commitment_reference_date`, `cntrct_start_date`, `sperrart_alle`, and `sperrgrund_zusgf` combinations.
    *   Data for `cntrct_ty` values 20 and non-20.
    *   Data for `dwtk_meldungen` to test `v_datum` derivation (rows existing, rows not existing for `BERT_DROP_TEMP_TABLE`).
    *   Data that results in non-matching `LEFT JOIN` conditions to verify `NULL` propagation.
2.  **Target Table Schema:** The BigQuery target table (`bert_dw_staging.bert_ausd_v_ta_vertrag_tmp`) has been created with a schema that precisely matches the output schema of the legacy Oracle job.
3.  **Access:** Necessary credentials and network access are configured for:
    *   Querying the legacy Oracle database.
    *   Executing the legacy UC4 job (or its underlying KornShell/SQL).
    *   Interacting with the Airflow API/UI.
    *   Querying BigQuery.

---

### 1. Output Parity - Full Data Comparison

*   **Purpose:** To verify that the migrated BigQuery job produces an identical dataset (row count and content of all columns) as the legacy Oracle job when given the same input data. This is the most comprehensive test for behavioral equivalence.
*   **Setup:**
    *   Ensure the test environment prerequisites are met, especially identical source data in both Oracle and BigQuery.
    *   Identify a unique key or a combination of columns that can uniquely identify each row in the output (e.g., `vertrag_id_carmen`). If no natural key exists, consider hashing all columns for comparison.
*   **Action:**
    1.  Execute the legacy `DW.BERT_AUSD_V_TA_VERTRAG_TMP` job in the Oracle environment.
    2.  Extract all data from the legacy target table (`sof$ta_vertrag_tmp`) into a canonical format (e.g., CSV, JSON, or a temporary table in a neutral database).
    3.  Trigger the Airflow DAG `dw_bert_ausd_v_ta_vertrag_tmp` to execute the migrated BigQuery job.
    4.  Extract all data from the BigQuery target table (`bert_dw_staging.bert_ausd_v_ta_vertrag_tmp`) into the same canonical format.
    5.  Compare the two extracted datasets.
*   **Pass/Fail Criterion:**
    *   The total row count in the legacy output must exactly match the total row count in the migrated output.
    *   For every column, the data type and value must be identical for corresponding rows.
    *   No rows should be present in one output but not the other.

```python
# Example Python (pytest) assertion for full data comparison
import pandas as pd
from google.cloud import bigquery
import cx_Oracle # Assuming cx_Oracle for Oracle connection

def test_full_output_parity(legacy_oracle_db_conn, bigquery_client):
    # --- Step 1 & 2: Execute legacy job and extract data ---
    # This part assumes you have a way to trigger the legacy job and then query its output.
    # For automated testing, you might need to mock or directly query the Oracle DB.
    # For this example, let's assume the legacy job has already run and populated 'legacy_target_table'.
    legacy_query = "SELECT * FROM your_oracle_schema.sof$ta_vertrag_tmp ORDER BY vertrag_id_carmen, partner_id_carmen"
    legacy_df = pd.read_sql(legacy_query, legacy_oracle_db_conn)

    # --- Step 3: Trigger migrated job (Airflow DAG) ---
    # This would typically involve an Airflow API call or a manual trigger.
    # For this test, we assume the DAG has been triggered and completed successfully.
    # Example (conceptual): airflow_api_client.trigger_dag('dw_bert_ausd_v_ta_vertrag_tmp')
    # Wait for DAG completion...

    # --- Step 4: Extract data from migrated BigQuery target ---
    migrated_query = f"SELECT * FROM `project.dataset.bert_ausd_v_ta_vertrag_tmp` ORDER BY vertrag_id_carmen, partner_id_carmen"
    migrated_df = bigquery_client.query(migrated_query).to_dataframe()

    # --- Step 5: Compare datasets ---
    # Ensure column names and order are consistent for comparison
    migrated_df.columns = [col.upper() for col in migrated_df.columns] # Oracle often uses uppercase
    legacy_df.columns = [col.upper() for col in legacy_df.columns]

    # Sort both dataframes by a common key for reliable comparison
    sort_cols = ['VERTRAG_ID_CARMEN', 'PARTNER_ID_CARMEN'] # Adjust based on actual unique key
    legacy_df = legacy_df.sort_values(by=sort_cols).reset_index(drop=True)
    migrated_df = migrated_df.sort_values(by=sort_cols).reset_index(drop=True)

    # Convert object columns to string for consistent comparison (e.g., mixed types in Oracle)
    for col in legacy_df.select_dtypes(include='object').columns:
        legacy_df[col] = legacy_df[col].astype(str)
    for col in migrated_df.select_dtypes(include='object').columns:
        migrated_df[col] = migrated_df[col].astype(str)

    pd.testing.assert_frame_equal(legacy_df, migrated_df, check_dtype=True, check_exact=False, rtol=1e-9)
    # check_exact=False and rtol for potential floating point differences if any, though not expected here.
    # For strict equality, remove check_exact=False, rtol.
```

### 2. Transformation Correctness - `v_datum` Derivation

*   **Purpose:** To specifically validate the correct translation of the `v_datum` variable derivation from Oracle's `NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101')` to BigQuery's `IFNULL(FORMAT_DATE('%Y%m%d', MAX(DATE(m.timecreated))), '19000101')`.
*   **Setup:**
    *   Populate `isbert_schema.dwtk_meldungen` in both Oracle and BigQuery with specific test data:
        *   Case 1: Multiple rows with `job_kennung = 'BERT_DROP_TEMP_TABLE'` and varying `timecreated` dates.
        *   Case 2: No rows with `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
        *   Case 3: Rows with `job_kennung = 'BERT_DROP_TEMP_TABLE'` but `timecreated` is `NULL`.
*   **Action:**
    1.  **Legacy:** Execute the Oracle SQL snippet to derive `v_datum` directly.
        ```sql
        SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS v_datum_legacy
        FROM isbert_schema.dwtk_meldungen m
        WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
        ```
    2.  **Migrated:** Execute the BigQuery `DECLARE` statement and then select the variable.
        ```sql
        DECLARE v_datum STRING DEFAULT (
          SELECT IFNULL(FORMAT_DATE('%Y%m%d', MAX(DATE(m.timecreated))), '19000101')
          FROM `project.dataset.isbert_schema_dwtk_meldungen` m
          WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
        );
        SELECT v_datum;
        ```
*   **Pass/Fail Criterion:** The `v_datum` value derived from Oracle must exactly match the `v_datum` value derived from BigQuery for all test cases.

### 3. Transformation Correctness - `CASE` Statements and `NULL` Handling

*   **Purpose:** To verify the correct translation of Oracle `DECODE` to BigQuery `CASE` statements, and general `NULL` handling for key derived columns.
*   **Setup:**
    *   Ensure source data includes rows that exercise all branches of the `CASE` statements for `vertragsstatus`, `rechnungszahlart`, `rechnungsmedium`, `upgradeberechtigt`, and `VDA`.
    *   Include rows where input columns to these `CASE` statements are `NULL`.
*   **Action:**
    1.  Execute both legacy and migrated jobs (as in Test 1).
    2.  Query the output tables for specific rows and columns that target these transformations.
    3.  Compare the values of `vertragsstatus`, `rechnungszahlart`, `rechnungsmedium`, `upgradeberechtigt`, and `VDA` for these rows.
*   **Pass/Fail Criterion:** The values for these derived columns in the migrated output must exactly match the values in the legacy output for all test rows, including correct `NULL` propagation.

```sql
-- Example BigQuery assertion for a specific CASE statement (e.g., vertragsstatus)
-- This would be run AFTER the migrated job has populated the target table.
SELECT
  t.vertrag_id_carmen,
  t.vertragsstatus AS migrated_vertragsstatus,
  (SELECT vertragsstatus FROM your_oracle_schema.sof$ta_vertrag_tmp WHERE vertrag_id_carmen = t.vertrag_id_carmen) AS legacy_vertragsstatus
FROM `project.dataset.bert_ausd_v_ta_vertrag_tmp` t
WHERE t.vertrag_id_carmen IN ('CONTRACT_ID_1', 'CONTRACT_ID_2', 'CONTRACT_ID_WITH_NULL_STATUS') -- Specific test cases
  AND t.vertragsstatus <> (SELECT vertragsstatus FROM your_oracle_schema.sof$ta_vertrag_tmp WHERE vertrag_id_carmen = t.vertrag_id_carmen);

-- Pass if this query returns 0 rows.
```

### 4. Transformation Correctness - `upgradeberechtigt` Logic (Complex Date & Conditional Logic)

*   **Purpose:** To thoroughly test the complex `CASE` statement for `upgradeberechtigt`, which involves `DATE_DIFF`, `PARSE_DATE`, `COALESCE`, and multiple conditional checks with `NULL` handling.
*   **Setup:**
    *   Populate source tables with data covering all branches and edge cases for `upgradeberechtigt`:
        *   `number_time_measurement` is `NULL` or `0`, `b.sperrart_alle` is `NULL`.
        *   `number_time_measurement` is `NULL` or `0`, `b.sperrart_alle` is not `NULL` but `b.sperrgrund_zusgf = 2`.
        *   `number_time_measurement` is `NULL` or `0`, `b.sperrart_alle` is not `NULL` and `b.sperrgrund_zusgf <> 2`.
        *   `number_time_measurement = 12`, `DATE_DIFF` > 9 months, `b.sperrart_alle` conditions met/not met.
        *   `number_time_measurement = 12`, `DATE_DIFF` <= 9 months.
        *   `number_time_measurement` is `NULL` or `0` or `24`, `DATE_DIFF` > 23 months, `b.sperrart_alle` conditions met/not met.
        *   `number_time_measurement` is `NULL` or `0` or `24`, `DATE_DIFF` <= 23 months.
        *   Cases where `c.commitment_reference_date` is `NULL` (testing `COALESCE`).
*   **Action:**
    1.  Execute both legacy and migrated jobs.
    2.  Query the output tables for the `upgradeberechtigt` column for the specific test rows.
    3.  Compare the values.
*   **Pass/Fail Criterion:** The `upgradeberechtigt` value in the migrated output must exactly match the legacy output for all test rows.

### 5. Transformation Correctness - Join Conditions and Filters

*   **Purpose:** To confirm that all `JOIN` conditions (especially `LEFT JOIN`s replacing Oracle `(+)`) and `WHERE` clauses are correctly translated, ensuring no unintended row loss or duplication.
*   **Setup:**
    *   Source data should include:
        *   Rows that match all `JOIN` conditions.
        *   Rows that do not match `LEFT JOIN` conditions (to verify `NULL`s from the right table).
        *   Rows where `c.cntrct_ty = 20` (for the second `UNION ALL` branch).
        *   Rows where `c.cntrct_ty <> 20` (for the first `UNION ALL` branch).
        *   Rows that would be excluded by the `WHERE` clauses.
*   **Action:**
    1.  Execute both legacy and migrated jobs.
    2.  Perform a row-by-row comparison (as in Test 1).
    3.  Additionally, for specific `LEFT JOIN` scenarios, query the output for columns from the right-hand table to ensure `NULL`s are correctly populated when no match exists.
*   **Pass/Fail Criterion:**
    *   The total row count must match (covered by Test 1).
    *   All rows present in the legacy output must be present in the migrated output, and vice-versa.
    *   `NULL` values resulting from `LEFT JOIN`s must be consistent between legacy and migrated outputs.

### 6. External System Replacements - Airflow Orchestration

*   **Purpose:** To verify that the Airflow DAG correctly replaces the UC4 scheduler and orchestrates the BigQuery transformation.
*   **Setup:**
    *   Airflow environment is running and the `dw_bert_ausd_v_ta_vertrag_tmp.py` DAG is deployed and unpaused.
    *   BigQuery project and dataset are accessible by the Airflow service account.
*   **Action:**
    1.  Manually trigger the `dw_bert_ausd_v_ta_vertrag_tmp` DAG from the Airflow UI or via the Airflow CLI/API.
    2.  Monitor the DAG run status in the Airflow UI.
    3.  Check BigQuery job history for the corresponding job initiated by Airflow.
*   **Pass/Fail Criterion:**
    *   The Airflow DAG run completes successfully without errors.
    *   A BigQuery job corresponding to the transformation SQL is successfully executed and completes in BigQuery.
    *   The BigQuery target table (`bert_dw_staging.bert_ausd_v_ta_vertrag_tmp`) is populated with data.

```python
# Example Python (pytest) assertion for Airflow DAG execution
import requests
import os

# Assuming Airflow API endpoint and authentication details are available
AIRFLOW_API_URL = os.getenv("AIRFLOW_API_URL", "http://localhost:8080/api/v1")
AIRFLOW_AUTH_HEADERS = {"Authorization": "Bearer YOUR_AIRFLOW_TOKEN"} # Or other auth method

def test_airflow_dag_execution():
    dag_id = 'dw_bert_ausd_v_ta_vertrag_tmp'
    
    # Trigger the DAG
    trigger_response = requests.post(
        f"{AIRFLOW_API_URL}/dags/{dag_id}/dagRuns",
        headers=AIRFLOW_AUTH_HEADERS,
        json={"conf": {}} # Optional: pass DAG run configuration
    )
    trigger_response.raise_for_status()
    dag_run_id = trigger_response.json().get('dag_run_id')
    assert dag_run_id is not None, "Failed to trigger DAG"

    # Poll for DAG run status (simplified, in real test use a proper polling mechanism)
    max_attempts = 60
    for i in range(max_attempts):
        status_response = requests.get(
            f"{AIRFLOW_API_URL}/dags/{dag_id}/dagRuns/{dag_run_id}",
            headers=AIRFLOW_AUTH_HEADERS
        )
        status_response.raise_for_status()
        state = status_response.json().get('state')
        if state in ['success', 'failed']:
            break
        time.sleep(10) # Wait 10 seconds before polling again

    assert state == 'success', f"Airflow DAG run {dag_run_id} failed with state: {state}"

    # Further checks could involve querying BigQuery job history to confirm job completion
    # (Requires BigQuery API client and job ID extraction from Airflow logs or BigQuery metadata)
```

### 7. Data Quality - Row Count Assertion

*   **Purpose:** To ensure that the total number of rows processed and inserted into the target table remains consistent between the legacy and migrated systems. This is a quick sanity check for major data loss or unexpected additions.
*   **Setup:** Identical source data in both environments.
*   **Action:**
    1.  Execute the legacy job.
    2.  Get the row count from the legacy target table: `SELECT COUNT(*) FROM your_oracle_schema.sof$ta_vertrag_tmp;`
    3.  Execute the migrated job via Airflow.
    4.  Get the row count from the BigQuery target table: `SELECT COUNT(*) FROM `project.dataset.bert_ausd_v_ta_vertrag_tmp`;`
*   **Pass/Fail Criterion:** The row count from the BigQuery target table must exactly match the row count from the Oracle target table.

### 8. Data Quality - Schema Assertion

*   **Purpose:** To confirm that the schema (column names, data types, nullability) of the migrated BigQuery target table precisely matches the legacy Oracle target table.
*   **Setup:** The BigQuery target table `bert_dw_staging.bert_ausd_v_ta_vertrag_tmp` has been created.
*   **Action:**
    1.  Extract the schema definition for `sof$ta_vertrag_tmp` from Oracle (e.g., `DESCRIBE sof$ta_vertrag_tmp` or `ALL_TAB_COLUMNS`).
    2.  Extract the schema definition for `bert_dw_staging.bert_ausd_v_ta_vertrag_tmp` from BigQuery (e.g., `INFORMATION_SCHEMA.COLUMNS`).
    3.  Compare column names, data types, and nullability properties.
*   **Pass/Fail Criterion:**
    *   All column names must match (case-insensitivity might need to be considered if Oracle uses mixed case and BigQuery uses lowercase by default, but the logical names should align).
    *   Data types must be compatible and correctly translated (e.g., `VARCHAR2` to `STRING`, `NUMBER` to `INT64`/`FLOAT64`, `DATE` to `DATE`/`TIMESTAMP`).
    *   Nullability constraints should be consistent.

```sql
-- Example BigQuery SQL for schema assertion
SELECT
  column_name,
  data_type,
  is_nullable
FROM `project.dataset.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'bert_ausd_v_ta_vertrag_tmp'
ORDER BY ordinal_position;

-- Compare this output to the Oracle schema definition.
-- Example Oracle SQL for schema assertion (conceptual, actual query might vary by Oracle version)
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    NULLABLE
FROM ALL_TAB_COLUMNS
WHERE OWNER = 'YOUR_ORACLE_SCHEMA' AND TABLE_NAME = 'SOF$TA_VERTRAG_TMP'
ORDER BY COLUMN_ID;
```