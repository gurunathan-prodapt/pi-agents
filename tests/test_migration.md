As a senior data-migration QA engineer, I've designed a comprehensive suite of tests to validate the migration of `DW.BERT_AUSD_V_TA_P_VERTRAG` to Google Cloud Platform. These tests aim to ensure behavioral equivalence, data integrity, and correctness across all transformation stages.

A critical prerequisite for these tests is the establishment of a **"Golden Dataset"**. This dataset should be carefully crafted to represent a wide range of real-world scenarios, including typical data, edge cases (NULLs, empty strings, boundary values), and specific conditions relevant to the transformation logic (e.g., `twin_vertrag_id` matching/non-matching, `dwtk_meldungen` entries for `v_datum`). This Golden Dataset must be loaded identically into both the legacy Oracle source tables and the target BigQuery source tables for comparison.

---

## 1. Output Parity Tests

### Test Case 1.1: Full Data Parity (Happy Path)

*   **Purpose:** To verify that the migrated BigQuery job produces an identical final dataset in `sof_ta_p_vertrag_bq` as the legacy Oracle job produces in `sof$ta_p_vertrag`, given the same input data. This is the most critical test for behavioral equivalence.
*   **Setup:**
    1.  Load the "Golden Dataset" into the legacy Oracle tables: `isbert_schema.dwtk_meldungen` and `sof$ta_vertrag_tmp`.
    2.  Load the *exact same* "Golden Dataset" into the BigQuery source tables: `your_bigquery_dataset.dwtk_meldungen_bq` and `your_bigquery_dataset.sof_ta_vertrag_tmp_bq`.
    3.  Ensure all other `sof$` temporary tables (both Oracle and BigQuery) are in a consistent, known state (e.g., empty) before execution.
*   **Action:**
    1.  Execute the legacy Oracle job (`DW.BERT_AUSD_V_TA_P_VERTRAG`) to populate `sof$ta_p_vertrag`.
    2.  Execute the migrated Airflow DAG (`dw_bert_ausd_v_ta_p_vertrag_dag`) to populate `sof_ta_p_vertrag_bq`.
*   **Pass/Fail Criterion:**
    *   The row count of `sof_ta_p_vertrag_bq` must exactly match the row count of `sof$ta_p_vertrag`.
    *   A deep, column-by-column comparison of the data in `sof_ta_p_vertrag_bq` and `sof$ta_p_vertrag` must reveal no differences. This can be achieved by exporting both tables and comparing, or by using SQL `EXCEPT` / `MINUS` operations after careful type casting.

```sql
-- BigQuery Assertion (after running both jobs)
-- Replace 'your-gcp-project.your_bigquery_dataset' with actual values
-- This query assumes a linked server or data export mechanism to compare Oracle data.
-- For a direct comparison, you'd typically export Oracle data to a temporary BQ table.

-- Step 1: Compare Row Counts
SELECT
    (SELECT COUNT(*) FROM `your-gcp-project.your_bigquery_dataset.sof_ta_p_vertrag_bq`) AS bq_row_count,
    (SELECT COUNT(*) FROM `your-gcp-project.your_bigquery_dataset.sof_ta_p_vertrag_oracle_export`) AS oracle_row_count;

-- Pass if bq_row_count = oracle_row_count

-- Step 2: Deep Data Comparison (assuming Oracle data is exported to 'sof_ta_p_vertrag_oracle_export')
-- This query identifies rows present in BigQuery but not in Oracle, or vice-versa.
-- It requires careful casting to ensure compatible types for comparison.
SELECT 'Only in BigQuery' AS source, * FROM `your-gcp-project.your_bigquery_dataset.sof_ta_p_vertrag_bq`
EXCEPT DISTINCT
SELECT 'Only in BigQuery' AS source, * FROM `your-gcp-project.your_bigquery_dataset.sof_ta_p_vertrag_oracle_export`
UNION ALL
SELECT 'Only in Oracle' AS source, * FROM `your-gcp-project.your_bigquery_dataset.sof_ta_p_vertrag_oracle_export`
EXCEPT DISTINCT
SELECT 'Only in Oracle' AS source, * FROM `your-gcp-project.your_bigquery_dataset.sof_ta_p_vertrag_bq`;

-- Pass if the above query returns 0 rows.
```

---

## 2. Transformation Correctness Tests

### Test Case 2.1: `v_datum` Determination Logic

*   **Purpose:** To verify that the `v_datum` variable, derived from `dwtk_meldungen`, is calculated identically in BigQuery as it was in Oracle, including the `COALESCE` (Oracle `NVL`) for default value.
*   **Setup:**
    1.  **Scenario A (Multiple entries):** Populate `dwtk_meldungen` (Oracle) and `dwtk_meldungen_bq` (BigQuery) with multiple rows for `job_kennung = 'BERT_DROP_TEMP_TABLE'`, each with different `timecreated` values.
    2.  **Scenario B (No entries):** Ensure `dwtk_meldungen` and `dwtk_meldungen_bq` have *no* rows for `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
    3.  **Scenario C (NULL `timecreated` - if applicable):** Populate with `job_kennung = 'BERT_DROP_TEMP_TABLE'` but `timecreated` is `NULL` (if Oracle allows this for the column type).
*   **Action:**
    1.  For each scenario, execute the `v_datum` determination logic in Oracle and BigQuery separately.
        *   Oracle: `SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') FROM isbert_schema.dwtk_meldungen m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';`
        *   BigQuery: `DECLARE v_datum STRING; SET v_datum = COALESCE((SELECT FORMAT_DATE('%Y%m%d', MAX(m.timecreated)) FROM your-gcp-project.your_bigquery_dataset.dwtk_meldungen_bq m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'), '19000101'); SELECT v_datum;`
*   **Pass/Fail Criterion:** The `v_datum` value obtained from BigQuery must be identical to the value obtained from Oracle for all scenarios.

### Test Case 2.2: `LEFT JOIN` (Oracle `(+)`) Equivalence

*   **Purpose:** To confirm that the BigQuery `LEFT JOIN` correctly replicates the behavior of the Oracle `(+)` outer join, specifically how it handles matching and non-matching records and the resulting NULLs.
*   **Setup:**
    1.  Populate `sof$ta_vertrag_tmp` (Oracle) and `sof_ta_vertrag_tmp_bq` (BigQuery) with the following data patterns for `vertrag_id_carmen` and `twin_vertrag_id`:
        *   Rows where `v.twin_vertrag_id` *successfully matches* a `pv.vertrag_id_carmen`.
        *   Rows where `v.twin_vertrag_id` *does not find a match* in `pv.vertrag_id_carmen`.
        *   Rows where `v.twin_vertrag_id` is `NULL`.
        *   Rows where `v.vertrag_id_carmen` is `NULL` (if allowed).
*   **Action:**
    1.  Execute only the `SELECT` portion of the main `INSERT` statement in Oracle.
    2.  Execute only the `SELECT` portion of the main `INSERT` statement in BigQuery.
*   **Pass/Fail Criterion:** The result sets (all columns) from both the Oracle and BigQuery `SELECT` statements must be identical. Specifically, for rows where `v.twin_vertrag_id` has no match, all columns originating from `pv` in the output must be `NULL` in both systems.

```sql
-- BigQuery Assertion (conceptual, comparing SELECT results)
-- This would typically involve inserting the SELECT results into temporary tables
-- and then performing an EXCEPT DISTINCT comparison as in Test Case 1.1.

-- Example of the SELECT statement to test:
SELECT
       v.vertrag_id_carmen,
       v.partner_id_carmen,
       -- ... other v columns ...
       v.cntrct_validity_id
  FROM
        `your-gcp-project.your_bigquery_dataset.sof_ta_vertrag_tmp_bq` v
  LEFT JOIN
        `your-gcp-project.your_bigquery_dataset.sof_ta_vertrag_tmp_bq` pv
    ON
        v.twin_vertrag_id = pv.vertrag_id_carmen;

-- Compare the output of this BigQuery SELECT with the equivalent Oracle SELECT.
```

### Test Case 2.3: Data Type and NULL Handling

*   **Purpose:** To ensure that all column data types are correctly mapped from Oracle to BigQuery and that NULL values are consistently handled and propagated through the transformation.
*   **Setup:**
    1.  Populate `sof$ta_vertrag_tmp` (Oracle) and `sof_ta_vertrag_tmp_bq` (BigQuery) with data that specifically tests various data types and NULL scenarios:
        *   Maximum length strings.
        *   Numeric values at boundaries (e.g., 0, max/min values for integer types).
        *   Date/Timestamp values at boundaries (e.g., '1900-01-01', current date).
        *   Explicit `NULL` values for every nullable column.
        *   Empty strings (Oracle treats as NULL, BigQuery does not; ensure this distinction doesn't cause issues if empty strings are present in source data).
*   **Action:**
    1.  Execute the full job for both legacy and migrated systems.
*   **Pass/Fail Criterion:**
    *   The schema comparison (Test Case 4.2) must show compatible data types.
    *   The full data parity check (Test Case 1.1) must pass, confirming that all values, including NULLs and specific data type representations, are identical.

---

## 3. External-System Replacements Tests

### Test Case 3.1: `dwtk_meldungen` Source Replacement

*   **Purpose:** To confirm that `your_bigquery_dataset.dwtk_meldungen_bq` correctly replaces `isbert_schema.dwtk_meldungen` as the source for the `v_datum` calculation.
*   **Setup:**
    1.  Populate `isbert_schema.dwtk_meldungen` (Oracle) and `your_bigquery_dataset.dwtk_meldungen_bq` (BigQuery) with identical data, including a row for `job_kennung = 'BERT_DROP_TEMP_TABLE'` with a specific `timecreated`.
*   **Action:**
    1.  Run the full legacy Oracle job.
    2.  Run the full migrated BigQuery job.
*   **Pass/Fail Criterion:** The `v_datum` value used internally by both jobs (which can be logged or inspected) must be identical. The final output in `sof_ta_p_vertrag_bq` should reflect the correct `v_datum` if it were used in the main transformation (though in this specific SQL, `v_datum` is declared but not used in the `INSERT`). This test primarily validates the source data retrieval for `v_datum`.

### Test Case 3.2: `sof$ta_vertrag_tmp` Source Replacement (and `PCRS1` implication)

*   **Purpose:** To verify that `your_bigquery_dataset.sof_ta_vertrag_tmp_bq` effectively replaces `sof$ta_vertrag_tmp` as the primary input for the contract data transformation. This test implicitly validates the upstream data ingestion pipeline from `PCRS1` (if `sof$ta_vertrag_tmp` is sourced from there) into BigQuery.
*   **Setup:**
    1.  Ensure the upstream data ingestion process (e.g., Dataflow, Fivetran) has successfully loaded identical data from the Oracle `PCRS1` source (or directly from `sof$ta_vertrag_tmp` if `PCRS1` is not the direct source for this table) into both `sof$ta_vertrag_tmp` (Oracle) and `sof_ta_vertrag_tmp_bq` (BigQuery).
    2.  Load the "Golden Dataset" into both `sof$ta_vertrag_tmp` and `sof_ta_vertrag_tmp_bq`.
*   **Action:**
    1.  Execute the full legacy Oracle job.
    2.  Execute the full migrated BigQuery job.
*   **Pass/Fail Criterion:** The full data parity check (Test Case 1.1) must pass. This confirms that the data consumed from the BigQuery source table is processed equivalently to the data from the Oracle source table.

### Test Case 3.3: `DWPA_UTIL_SKRIPT.runstatement` Truncation Replacement

*   **Purpose:** To verify that the BigQuery `TRUNCATE TABLE` statements correctly replace the Oracle `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` calls for clearing tables.
*   **Setup:**
    1.  Populate `sof$ta_p_vertrag` (Oracle) and `your_bigquery_dataset.sof_ta_p_vertrag_bq` (BigQuery) with some dummy data.
    2.  Populate a selection of other `sof$` temporary tables (e.g., `sof$ta_disc_zusgf`, `sof$ta_discount`) in both Oracle and BigQuery with dummy data.
*   **Action:**
    1.  Execute the legacy Oracle job (or isolate and run just the truncation parts).
    2.  Execute the migrated BigQuery job (or isolate and run just the truncation parts).
*   **Pass/Fail Criterion:** After execution, all specified tables (`sof$ta_p_vertrag`, `sof$ta_disc_zusgf`, `sof$ta_discount`, etc.) in both Oracle and BigQuery must have a row count of zero.

```sql
-- BigQuery Assertion (after running the job)
SELECT
    (SELECT COUNT(*) FROM `your-gcp-project.your_bigquery_dataset.sof_ta_p_vertrag_bq`) AS p_vertrag_count,
    (SELECT COUNT(*) FROM `your-gcp-project.your_bigquery_dataset.sof_ta_disc_zusgf_bq`) AS disc_zusgf_count,
    (SELECT COUNT(*) FROM `your-gcp-project.your_bigquery_dataset.sof_ta_vertrag_tmp_bq`) AS vertrag_tmp_count;
-- ... and so on for all truncated tables.

-- Pass if all counts are 0.
```

---

## 4. Data Quality / Row Count / Schema Assertions

### Test Case 4.1: Row Count Parity (Target Table)

*   **Purpose:** To ensure that the total number of records processed and inserted into the target table remains consistent between the legacy and migrated systems.
*   **Setup:**
    1.  Load the "Golden Dataset" into the source tables for both Oracle and BigQuery.
*   **Action:**
    1.  Execute the full legacy Oracle job.
    2.  Execute the full migrated BigQuery job.
*   **Pass/Fail Criterion:** The row count of `sof_ta_p_vertrag_bq` must be exactly equal to the row count of `sof$ta_p_vertrag`.

```sql
-- BigQuery Assertion (after running both jobs)
-- Assuming Oracle data is exported to 'sof_ta_p_vertrag_oracle_export'
SELECT
    (SELECT COUNT(*) FROM `your-gcp-project.your_bigquery_dataset.sof_ta_p_vertrag_bq`) AS bq_row_count,
    (SELECT COUNT(*) FROM `your-gcp-project.your_bigquery_dataset.sof_ta_p_vertrag_oracle_export`) AS oracle_row_count;

-- Pass if bq_row_count = oracle_row_count
```

### Test Case 4.2: Schema Parity

*   **Purpose:** To verify that the schema (column names, data types, nullability) of the target BigQuery table `sof_ta_p_vertrag_bq` is functionally equivalent to the legacy Oracle table `sof$ta_p_vertrag`.
*   **Setup:**
    1.  Ensure both `sof$ta_p_vertrag` (Oracle) and `sof_ta_p_vertrag_bq` (BigQuery) exist.
*   **Action:**
    1.  Retrieve schema definitions for both tables using database metadata queries.
*   **Pass/Fail Criterion:**
    *   All column names must match exactly.
    *   Oracle data types must have appropriate and compatible BigQuery equivalents (e.g., `VARCHAR2(N)` -> `STRING`, `NUMBER` -> `INT64` or `NUMERIC`, `DATE`/`TIMESTAMP` -> `DATE`/`TIMESTAMP`).
    *   Nullability constraints should be consistent (e.g., `NOT NULL` in Oracle should map to `REQUIRED` in BigQuery, `NULLABLE` in Oracle to `NULLABLE` in BigQuery).

```python
# pytest example for schema comparison (conceptual, requires Oracle DB connection)
import pytest
from google.cloud import bigquery
import cx_Oracle # or other Oracle client library

def get_oracle_schema(connection_string, table_name):
    conn = cx_Oracle.connect(connection_string)
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT COLUMN_NAME, DATA_TYPE, NULLABLE
        FROM ALL_TAB_COLUMNS
        WHERE OWNER = 'ISBERT_SCHEMA' AND TABLE_NAME = '{table_name.upper()}'
        ORDER BY COLUMN_ID
    """)
    schema = []
    for col_name, data_type, nullable in cursor:
        schema.append({
            'name': col_name,
            'type': data_type,
            'nullable': (nullable == 'Y')
        })
    conn.close()
    return schema

def get_bigquery_schema(project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)
    schema = []
    for field in table.schema:
        schema.append({
            'name': field.name.upper(), # Oracle column names are typically uppercase
            'type': field.field_type,
            'nullable': (field.mode == 'NULLABLE')
        })
    return schema

def map_oracle_type_to_bigquery(oracle_type):
    # Simplified mapping, needs to be comprehensive for all types
    if 'VARCHAR' in oracle_type: return 'STRING'
    if 'NUMBER' in oracle_type: return 'NUMERIC' # Or INT64, FLOAT64 depending on precision
    if 'DATE' in oracle_type: return 'DATE'
    if 'TIMESTAMP' in oracle_type: return 'TIMESTAMP'
    # Add more mappings as needed
    return oracle_type # Fallback for unmapped types

def test_schema_parity():
    oracle_conn_str = "user/password@host:port/service_name"
    project_id = "your-gcp-project"
    dataset_id = "your_bigquery_dataset"
    oracle_table = "sof$ta_p_vertrag"
    bq_table = "sof_ta_p_vertrag_bq"

    oracle_schema = get_oracle_schema(oracle_conn_str, oracle_table)
    bq_schema = get_bigquery_schema(project_id, dataset_id, bq_table)

    assert len(oracle_schema) == len(bq_schema), "Column count mismatch"

    for oracle_col, bq_col in zip(oracle_schema, bq_schema):
        assert oracle_col['name'] == bq_col['name'], f"Column name mismatch: {oracle_col['name']} vs {bq_col['name']}"
        
        expected_bq_type = map_oracle_type_to_bigquery(oracle_col['type'])
        assert expected_bq_type == bq_col['type'], f"Type mismatch for {oracle_col['name']}: Oracle {oracle_col['type']} -> Expected BQ {expected_bq_type}, Got BQ {bq_col['type']}"
        
        assert oracle_col['nullable'] == bq_col['nullable'], f"Nullability mismatch for {oracle_col['name']}"

```

### Test Case 4.3: Data Quality - Key Uniqueness (if applicable)

*   **Purpose:** If `vertrag_id_carmen` is expected to be unique in the target table, verify that this constraint is maintained after migration.
*   **Setup:**
    1.  Load the "Golden Dataset" into the source tables. Include scenarios that might (or might not) produce duplicate `vertrag_id_carmen` in the output based on the transformation logic.
*   **Action:**
    1.  Execute the full job for both legacy and migrated systems.
*   **Pass/Fail Criterion:**
    *   `SELECT vertrag_id_carmen FROM sof_ta_p_vertrag_bq GROUP BY 1 HAVING COUNT(*) > 1` should return the same number of duplicate `vertrag_id_carmen` (ideally zero if uniqueness is expected) as the equivalent query on `sof$ta_p_vertrag`.

```sql
-- BigQuery Assertion
SELECT vertrag_id_carmen, COUNT(*) as duplicate_count
FROM `your-gcp-project.your_bigquery_dataset.sof_ta_p_vertrag_bq`
GROUP BY vertrag_id_carmen
HAVING COUNT(*) > 1;

-- Pass if the number of rows returned matches the Oracle equivalent.
-- If vertrag_id_carmen is expected to be unique, this query should return 0 rows.
```

### Test Case 4.4: Data Quality - Referential Integrity for `twin_vertrag_id`

*   **Purpose:** To verify that the referential integrity between `twin_vertrag_id` and `vertrag_id_carmen` within `sof_ta_p_vertrag_bq` is consistent with the legacy system.
*   **Setup:**
    1.  Load the "Golden Dataset" into the source tables. Include `twin_vertrag_id` values that:
        *   Successfully reference an existing `vertrag_id_carmen`.
        *   Reference a non-existent `vertrag_id_carmen` (orphan records).
        *   Are `NULL`.
*   **Action:**
    1.  Execute the full job for both legacy and migrated systems.
*   **Pass/Fail Criterion:** The count of "orphan" `twin_vertrag_id` values (i.e., `twin_vertrag_id` is not NULL but does not match any `vertrag_id_carmen` in the same table) must be identical in both `sof_ta_p_vertrag_bq` and `sof$ta_p_vertrag`.

```sql
-- BigQuery Assertion
SELECT COUNT(DISTINCT t1.twin_vertrag_id)
FROM `your-gcp-project.your_bigquery_dataset.sof_ta_p_vertrag_bq` t1
LEFT JOIN `your-gcp-project.your_bigquery_dataset.sof_ta_p_vertrag_bq` t2
  ON t1.twin_vertrag_id = t2.vertrag_id_carmen
WHERE t1.twin_vertrag_id IS NOT NULL
  AND t2.vertrag_id_carmen IS NULL;

-- Pass if the count returned matches the Oracle equivalent.
```