As a senior data-migration QA engineer, I have analyzed the migration design document and the provided legacy and target code. The following test cases are designed to validate the behavioral equivalence of the migrated job `k_ausd_v_ta_barrier_zusgf.ksh` to its BigQuery/Airflow counterpart.

---

## Migration Validation Tests for `k_ausd_v_ta_barrier_zusgf.ksh`

### 1. Output Parity

**Purpose:** To ensure that for identical input data, the migrated BigQuery job produces an output dataset in `sof_ta_barrier_zusgf` that is byte-for-byte identical to the output produced by the legacy Oracle job in `sof$ta_barrier_zusgf`. This is the most critical test for behavioral equivalence.

**Setup:**
1.  **Legacy Environment:**
    *   An operational Oracle environment with the legacy `k_ausd_v_ta_barrier_zusgf.ksh` script and its dependent `d_ausd_v_ta_barrier_zusgf.sql` script.
    *   Source tables `sof$ta_barrier` and `isbert_schema.dwtk_meldungen` populated with a comprehensive, representative test dataset. This dataset should cover various scenarios, including:
        *   Multiple `cntrct_id` values.
        *   `cntrct_id`s with single and multiple barrier entries.
        *   `ist_stillegung` values of `0` and `1`.
        *   `sperr_ende` being `NULL` and `NOT NULL` when `ist_stillegung = 1`.
        *   `sperrart` values containing 'Rufnummern' and spaces, as well as simple strings.
        *   `barrier_reason_cv` values of `2` and other values (e.g., `1`, `3`, `NULL`).
        *   Edge cases like `sperr_beginn` being `NULL` when `ist_stillegung = 1`.
2.  **Migrated Environment:**
    *   A GCP project with BigQuery enabled.
    *   An Airflow (Cloud Composer) environment configured with the `k_ausd_v_ta_barrier_zusgf_dag.py` DAG.
    *   BigQuery source tables `your-gcp-project-id.source_dataset.sof_ta_barrier` and `your-gcp-project-id.source_dataset.dwtk_meldungen` populated with *exactly the same data* as their Oracle counterparts.
    *   The target table `your-gcp-project-id.target_dataset.sof_ta_barrier_zusgf` created using the provided DDL.

**Action:**
1.  **Execute Legacy Job:** Run the `k_ausd_v_ta_barrier_zusgf.ksh` script in the Oracle environment.
2.  **Extract Legacy Output:** Query `sof$ta_barrier_zusgf` in Oracle and export the entire dataset (e.g., to CSV, JSON, or directly load into a BigQuery staging table for comparison).
3.  **Execute Migrated Job:** Trigger the `k_ausd_v_ta_barrier_zusgf_dag.py` DAG in Airflow.
4.  **Extract Migrated Output:** Query `your-gcp-project-id.target_dataset.sof_ta_barrier_zusgf` in BigQuery.
5.  **Compare Outputs:** Perform a detailed comparison of the two extracted datasets.

**Pass/Fail Criterion:**
*   The total number of rows in the Oracle `sof$ta_barrier_zusgf` and BigQuery `sof_ta_barrier_zusgf` tables must be identical.
*   A row-by-row, column-by-column comparison of the two datasets must show no differences. This can be achieved by:
    *   Loading both datasets into a comparison tool.
    *   Using SQL `EXCEPT DISTINCT` (if both datasets are in BigQuery or a common SQL-accessible system).

**Runnable Test Code (Conceptual for BigQuery comparison, assuming Oracle output is staged):**

```python
import pandas as pd
from google.cloud import bigquery

# --- Configuration (replace with actual values) ---
PROJECT_ID = "your-gcp-project-id"
TARGET_DATASET = "target_dataset"
COMPARISON_DATASET = "comparison_dataset" # Dataset where Oracle output is staged
ORACLE_STAGING_TABLE = "oracle_sof_ta_barrier_zusgf_output"
BIGQUERY_TARGET_TABLE = "sof_ta_barrier_zusgf"

def compare_outputs():
    client = bigquery.Client(project=PROJECT_ID)

    # Query to find rows only in BigQuery target table
    query_only_in_bq = f"""
    SELECT * FROM `{PROJECT_ID}.{TARGET_DATASET}.{BIGQUERY_TARGET_TABLE}`
    EXCEPT DISTINCT
    SELECT * FROM `{PROJECT_ID}.{COMPARISON_DATASET}.{ORACLE_STAGING_TABLE}`
    """
    rows_only_in_bq = client.query(query_only_in_bq).to_dataframe()

    # Query to find rows only in Oracle staging table
    query_only_in_oracle = f"""
    SELECT * FROM `{PROJECT_ID}.{COMPARISON_DATASET}.{ORACLE_STAGING_TABLE}`
    EXCEPT DISTINCT
    SELECT * FROM `{PROJECT_ID}.{TARGET_DATASET}.{BIGQUERY_TARGET_TABLE}`
    """
    rows_only_in_oracle = client.query(query_only_in_oracle).to_dataframe()

    if not rows_only_in_bq.empty:
        print("FAIL: Rows found only in BigQuery target table:")
        print(rows_only_in_bq)
        return False
    if not rows_only_in_oracle.empty:
        print("FAIL: Rows found only in Oracle staging table:")
        print(rows_only_in_oracle)
        return False

    print("PASS: Output parity confirmed. Both tables are identical.")
    return True

# Example usage (would be part of a larger test suite)
# if __name__ == "__main__":
#     assert compare_outputs(), "Output parity test failed!"
```

---

### 2. Transformation Correctness

**Purpose:** To verify that each specific transformation rule, including joins, aggregations, filters, type handling, and NULL handling, is correctly implemented in the BigQuery SQL, matching the logic of the original Oracle PL/SQL.

**Setup:**
*   A BigQuery environment with the `create_target_table` task already executed.
*   For each sub-test, `your-gcp-project-id.source_dataset.sof_ta_barrier` should be populated with a minimal, specific dataset designed to isolate and test the particular transformation logic.

**Action:**
1.  Populate `your-gcp-project-id.source_dataset.sof_ta_barrier` with the specific test data for each sub-test.
2.  Execute the `load_transformed_data` task of the Airflow DAG.
3.  Query `your-gcp-project-id.target_dataset.sof_ta_barrier_zusgf` and/or intermediate CTEs (if accessible for debugging) to verify the results.

**Pass/Fail Criteria (Sub-tests):**

#### 2.1. `cntrct_id` Casting and `DISTINCT`
*   **Purpose:** Verify `cntrct_id` is correctly cast to `INT64` and that `DISTINCT` is applied within the `barrier_src` CTE before aggregation.
*   **Setup:** `sof_ta_barrier` with `cntrct_id` values that might be `NUMERIC` in Oracle but fit `INT64`, and duplicate rows (identical `cntrct_id` and other relevant columns).
    ```sql
    -- Example source data for sof_ta_barrier
    INSERT INTO `your-gcp-project-id.source_dataset.sof_ta_barrier` (cntrct_id, sperrart, sperrgrund, ist_stillegung, sperr_beginn, sperr_ende, barrier_reason_cv) VALUES
    (101, 'Sperre A', 'Reason A', 1, '2023-01-01', NULL, 2),
    (101, 'Sperre A', 'Reason A', 1, '2023-01-01', NULL, 2), -- Duplicate row
    (102, 'Sperre B', 'Reason B', 0, NULL, NULL, 1);
    ```
*   **Pass/Fail:**
    *   The `cntrct_id` column in `sof_ta_barrier_zusgf` must have a data type of `INT64`.
    *   For `cntrct_id = 101`, only one aggregated output row should exist in `sof_ta_barrier_zusgf`.

#### 2.2. `sperrart` Cleaning (`REPLACE` logic)
*   **Purpose:** Verify the `REPLACE(REPLACE(sperrart, 'Rufnummern', ''), ' ', '')` logic.
*   **Setup:** `sof_ta_barrier` with various `sperrart` values.
    ```sql
    INSERT INTO `your-gcp-project-id.source_dataset.sof_ta_barrier` (cntrct_id, sperrart, sperrgrund, ist_stillegung, sperr_beginn, sperr_ende, barrier_reason_cv) VALUES
    (201, 'Rufnummern Sperre', 'R1', 0, NULL, NULL, 1),
    (202, 'Sperre Rufnummern', 'R2', 0, NULL, NULL, 1),
    (203, '  Sperre  ', 'R3', 0, NULL, NULL, 1),
    (204, 'Rufnummern', 'R4', 0, NULL, NULL, 1),
    (205, 'SperreRufnummern', 'R5', 0, NULL, NULL, 1),
    (206, 'Simple', 'R6', 0, NULL, NULL, 1);
    ```
*   **Pass/Fail:**
    *   For `cntrct_id = 201`, `sperrart_alle` should be `'Sperre'`.
    *   For `cntrct_id = 202`, `sperrart_alle` should be `'Sperre'`.
    *   For `cntrct_id = 203`, `sperrart_alle` should be `'Sperre'`.
    *   For `cntrct_id = 204`, `sperrart_alle` should be `''` (empty string).
    *   For `cntrct_id = 205`, `sperrart_alle` should be `'Sperre'`.
    *   For `cntrct_id = 206`, `sperrart_alle` should be `'Simple'`.

#### 2.3. `stilllegungszeitraum_alle` Logic (Date Formatting and NULL Handling)
*   **Purpose:** Verify `ist_stillegung` condition, `sperr_ende IS NULL` handling, and `FORMAT_DATE` correctness.
*   **Setup:** `sof_ta_barrier` with specific date combinations.
    ```sql
    INSERT INTO `your-gcp-project-id.source_dataset.sof_ta_barrier` (cntrct_id, sperrart, sperrgrund, ist_stillegung, sperr_beginn, sperr_ende, barrier_reason_cv) VALUES
    (301, 'S1', 'R1', 1, '2023-01-01', '2023-01-31', 1),
    (302, 'S2', 'R2', 1, '2023-02-15', NULL, 1),
    (303, 'S3', 'R3', 0, '2023-03-01', '2023-03-31', 1),
    (304, 'S4', 'R4', 1, NULL, '2023-04-30', 1); -- sperr_beginn is NULL
    ```
*   **Pass/Fail:**
    *   For `cntrct_id = 301`, `stilllegungszeitraum_alle` should be `'01.01.2023 - 31.01.2023'`.
    *   For `cntrct_id = 302`, `stilllegungszeitraum_alle` should be `'ab 15.02.2023'`.
    *   For `cntrct_id = 303`, `stilllegungszeitraum_alle` should be `NULL`.
    *   For `cntrct_id = 304`, `stilllegungszeitraum_alle` should be `NULL` (as `DATE(NULL)` results in `NULL`, making `CONCAT` result in `NULL`).

#### 2.4. `sperrgrund_zusgf` (Initial Derivation in `barrier_src`)
*   **Purpose:** Verify `CASE WHEN barrier_reason_cv = 2 THEN 2 ELSE 3 END` logic.
*   **Setup:** `sof_ta_barrier` with various `barrier_reason_cv` values.
    ```sql
    INSERT INTO `your-gcp-project-id.source_dataset.sof_ta_barrier` (cntrct_id, sperrart, sperrgrund, ist_stillegung, sperr_beginn, sperr_ende, barrier_reason_cv) VALUES
    (401, 'S1', 'R1', 0, NULL, NULL, 1),
    (402, 'S2', 'R2', 0, NULL, NULL, 2),
    (403, 'S3', 'R3', 0, NULL, NULL, 3),
    (404, 'S4', 'R4', 0, NULL, NULL, NULL);
    ```
*   **Pass/Fail:** (This tests the internal `barrier_src` CTE logic, which is then aggregated. The final `sperrgrund_zusgf` will be derived from this. For direct verification, one might need to query the CTE if possible, or infer from the final aggregated value.)
    *   For `cntrct_id = 401`, the intermediate `sperrgrund_zusgf` should be `3`.
    *   For `cntrct_id = 402`, the intermediate `sperrgrund_zusgf` should be `2`.
    *   For `cntrct_id = 403`, the intermediate `sperrgrund_zusgf` should be `3`.
    *   For `cntrct_id = 404`, the intermediate `sperrgrund_zusgf` should be `3`.

#### 2.5. `STRING_AGG` and `ORDER BY`
*   **Purpose:** Verify correct concatenation and ordering for `sperrart_alle`, `sperrgrund_alle`, `stilllegungszeitraum_alle` using `STRING_AGG` with `ORDER BY sperrart`.
*   **Setup:** `sof_ta_barrier` with multiple rows for a single `cntrct_id` with varying `sperrart` values.
    ```sql
    INSERT INTO `your-gcp-project-id.source_dataset.sof_ta_barrier` (cntrct_id, sperrart, sperrgrund, ist_stillegung, sperr_beginn, sperr_ende, barrier_reason_cv) VALUES
    (501, 'B-Sperre', 'Reason B', 0, NULL, NULL, 1),
    (501, 'A-Sperre', 'Reason A', 0, NULL, NULL, 1),
    (501, 'C-Sperre', 'Reason C', 0, NULL, NULL, 1);
    ```
*   **Pass/Fail:**
    *   For `cntrct_id = 501`:
        *   `sperrart_alle` should be `'A-Sperre,B-Sperre,C-Sperre'`.
        *   `sperrgrund_alle` should be `'Reason A,Reason B,Reason C'`.
        *   `stilllegungszeitraum_alle` should be `NULL` (as `ist_stillegung = 0` for all).

#### 2.6. Aggregated `sperrgrund_zusgf` Logic (`COUNTIF`)
*   **Purpose:** Verify `CASE WHEN COUNTIF(sperrgrund_zusgf != 2) > 0 THEN 3 ELSE 2 END` logic.
*   **Setup:** `sof_ta_barrier` with `cntrct_id`s having different combinations of `barrier_reason_cv`.
    ```sql
    INSERT INTO `your-gcp-project-id.source_dataset.sof_ta_barrier` (cntrct_id, sperrart, sperrgrund, ist_stillegung, sperr_beginn, sperr_ende, barrier_reason_cv) VALUES
    (601, 'S1', 'R1', 0, NULL, NULL, 2), -- All barrier_reason_cv = 2
    (601, 'S2', 'R2', 0, NULL, NULL, 2),
    (602, 'S3', 'R3', 0, NULL, NULL, 2), -- Mixed barrier_reason_cv
    (602, 'S4', 'R4', 0, NULL, NULL, 1),
    (603, 'S5', 'R5', 0, NULL, NULL, 1), -- All barrier_reason_cv != 2
    (603, 'S6', 'R6', 0, NULL, NULL, 3);
    ```
*   **Pass/Fail:**
    *   For `cntrct_id = 601`, `sperrgrund_zusgf` should be `2`.
    *   For `cntrct_id = 602`, `sperrgrund_zusgf` should be `3`.
    *   For `cntrct_id = 603`, `sperrgrund_zusgf` should be `3`.

---

### 3. External-System Replacements

**Purpose:** To confirm that all external dependencies and interactions of the legacy KSH script (Oracle database, custom KSH utilities, file system operations) have been successfully replaced by their BigQuery/Airflow equivalents, and that the job functions entirely within the new GCP ecosystem.

**Setup:**
*   An Airflow (Cloud Composer) environment with the `k_ausd_v_ta_barrier_zusgf_dag.py` DAG deployed.
*   BigQuery datasets (`your-gcp-project-id.source_dataset`, `your-gcp-project-id.target_dataset`) exist.
*   Source tables `your-gcp-project-id.source_dataset.sof_ta_barrier` and `your-gcp-project-id.source_dataset.dwtk_meldungen` are populated with minimal test data.
*   Crucially, the Airflow environment should *not* have access to the legacy Oracle database, nor should it contain any of the legacy KSH utility scripts or environment configurations.

**Action:**
1.  Trigger the `k_ausd_v_ta_barrier_zusgf_dag.py` DAG in Airflow.
2.  Monitor the DAG execution logs in Airflow and Cloud Logging.

**Pass/Fail Criteria (Sub-tests):**

#### 3.1. Oracle Source Data Replacement
*   **Purpose:** Verify that the job reads exclusively from BigQuery source tables.
*   **Pass/Fail:** The DAG must complete successfully, and the `load_transformed_data` task must populate `sof_ta_barrier_zusgf` using data solely from `your-gcp-project-id.source_dataset.sof_ta_barrier`. There should be no errors or warnings related to Oracle database connections or missing Oracle tables.

#### 3.2. `TRUNCATE TABLE` Replacement
*   **Purpose:** Verify that the Oracle `TRUNCATE TABLE` operation is correctly replaced by BigQuery's `TRUNCATE TABLE`.
*   **Setup:** Populate `your-gcp-project-id.target_dataset.sof_ta_barrier_zusgf` with some dummy data *before* running the DAG.
*   **Pass/Fail:** After the `load_transformed_data` task completes, `sof_ta_barrier_zusgf` should contain only the newly processed data, indicating that the `TRUNCATE TABLE` command executed successfully prior to the `INSERT`. The row count should match the expected output from the source data, not the sum of dummy data + new data.

#### 3.3. KSH Environment/Utility Script Replacement
*   **Purpose:** Verify that the legacy KSH environment setup (`. $HOME/.dw_init`) and utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) are no longer dependencies.
*   **Pass/Fail:** The Airflow DAG must execute without any errors related to missing KSH scripts, environment variables, or `sqlplus` commands. Airflow's native logging and error handling mechanisms should be observed to be functioning correctly.

#### 3.4. `v_datum` Handling (Risk Mitigation)
*   **Purpose:** Confirm that the `v_datum` logic, derived from `isbert_schema.dwtk_meldungen` in the legacy job but noted as potentially redundant in the migration design, is handled as specified (i.e., not used in the core transformation).
*   **Pass/Fail:** The `load_transformed_data` task must complete successfully without querying `your-gcp-project-id.source_dataset.dwtk_meldungen` or failing due to its absence/content. The output in `sof_ta_barrier_zusgf` should be entirely independent of the `dwtk_meldungen` table, confirming its non-usage in the core transformation logic.

---

### 4. Data Quality / Row Count / Schema Assertions

**Purpose:** To ensure the migrated job maintains data integrity, produces expected row counts, and adheres to the defined schema in BigQuery.

**Setup:**
*   A BigQuery environment with the `create_target_table` task already executed.
*   `your-gcp-project-id.source_dataset.sof_ta_barrier` populated with a diverse dataset, including edge cases (e.g., all NULLs for a `cntrct_id`, very long strings, dates at year boundaries, empty strings).

**Action:**
1.  Execute the `load_transformed_data` task of the Airflow DAG.
2.  Perform SQL assertions on the resulting `your-gcp-project-id.target_dataset.sof_ta_barrier_zusgf` table.

**Pass/Fail Criteria (Sub-tests):**

#### 4.1. Row Count Parity (Source to Target)
*   **Purpose:** Verify that the number of unique `cntrct_id`s in the source table matches the total number of rows in the target table.
*   **Action:**
    ```sql
    SELECT COUNT(DISTINCT cntrct_id) FROM `your-gcp-project-id.source_dataset.sof_ta_barrier`;
    SELECT COUNT(*) FROM `your-gcp-project-id.target_dataset.sof_ta_barrier_zusgf`;
    ```
*   **Pass/Fail:** The `COUNT(*)` from the target table must be equal to the `COUNT(DISTINCT cntrct_id)` from the source table.

#### 4.2. Schema Validation
*   **Purpose:** Verify that the target table `sof_ta_barrier_zusgf` has the correct columns and data types as defined in the DDL.
*   **Action:** Query BigQuery's `INFORMATION_SCHEMA` or use the `bq show --schema` command.
*   **Pass/Fail:** The schema of `your-gcp-project-id.target_dataset.sof_ta_barrier_zusgf` must match:
    *   `cntrct_id`: `INT64`
    *   `sperrart_alle`: `STRING`
    *   `sperrgrund_alle`: `STRING`
    *   `stilllegungszeitraum_alle`: `STRING`
    *   `sperrgrund_zusgf`: `INT64`

#### 4.3. NULL Handling in Aggregations
*   **Purpose:** Verify that `STRING_AGG` correctly ignores `NULL` values during concatenation.
*   **Setup:** `sof_ta_barrier` with a `cntrct_id` having some `sperrgrund` or `stilllegungszeitraum_alle` values as `NULL`.
    ```sql
    INSERT INTO `your-gcp-project-id.source_dataset.sof_ta_barrier` (cntrct_id, sperrart, sperrgrund, ist_stillegung, sperr_beginn, sperr_ende, barrier_reason_cv) VALUES
    (701, 'A', 'Reason A', 1, '2023-01-01', NULL, 1),
    (701, 'B', NULL, 0, NULL, NULL, 1), -- sperrgrund is NULL, stilllegungszeitraum_alle will be NULL
    (701, 'C', 'Reason C', 1, '2023-02-01', '2023-02-28', 1);
    ```
*   **Pass/Fail:** For `cntrct_id = 701`:
    *   `sperrart_alle` should be `'A,B,C'`.
    *   `sperrgrund_alle` should be `'Reason A,Reason C'` (NULL `sperrgrund` ignored).
    *   `stilllegungszeitraum_alle` should be `'ab 01.01.2023, 01.02.2023 - 28.02.2023'` (NULL `stilllegungszeitraum_alle` ignored).

#### 4.4. Data Integrity - No Unexpected Values
*   **Purpose:** Ensure that the transformation does not introduce invalid or unexpected values into the target table.
*   **Action:** Execute the following SQL assertions.
*   **Pass/Fail:** All queries below should return `0` rows.
    ```sql
    -- Check for unexpected sperrgrund_zusgf values
    SELECT COUNT(*) FROM `your-gcp-project-id.target_dataset.sof_ta_barrier_zusgf`
    WHERE sperrgrund_zusgf NOT IN (2, 3);

    -- Check for NULL cntrct_id (should not happen due to GROUP BY)
    SELECT COUNT(*) FROM `your-gcp-project-id.target_dataset.sof_ta_barrier_zusgf`
    WHERE cntrct_id IS NULL;

    -- Check for 'Rufnummern' or extra spaces in sperrart_alle after cleaning
    SELECT COUNT(*) FROM `your-gcp-project-id.target_dataset.sof_ta_barrier_zusgf`
    WHERE sperrart_alle LIKE '%Rufnummern%' OR sperrart_alle LIKE '% %';

    -- Check for malformed dates in stilllegungszeitraum_alle (e.g., 'ab NULL')
    SELECT COUNT(*) FROM `your-gcp-project-id.target_dataset.sof_ta_barrier_zusgf`
    WHERE stilllegungszeitraum_alle LIKE '%NULL%';
    ```

---