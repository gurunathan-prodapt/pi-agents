As a senior data-migration QA engineer, I've designed a comprehensive suite of validation tests for the `DW.BERT_AUSD_BP_TA_P_BASISPROD` job migration. These tests aim to ensure the migrated BigQuery/Cloud Composer solution is functionally and behaviorally equivalent to the legacy Oracle/KornShell system.

The tests are categorized to cover output parity, transformation correctness, external system replacements, and data quality assertions.

---

## Migration Validation Tests: DW.BERT_AUSD_BP_TA_P_BASISPROD

### Prerequisites for All Tests:

*   **Access**: Read access to the legacy Oracle database (specifically `SOF$TA_P_BASISPROD` and all source `sof$ta_` tables, `isbert_schema.dwtk_meldungen`) and the ability to execute the legacy KornShell scripts.
*   **Access**: Full access to the Google Cloud Project, including BigQuery (`bert_dwh_prod` dataset and all tables) and Cloud Composer (to trigger DAGs and view logs).
*   **Test Data**: A representative set of test data must be loaded into both the legacy Oracle source tables and the migrated BigQuery source tables. This data should cover typical scenarios, edge cases (NULLs, empty tables, specific date values), and a sufficient volume for performance checks.
*   **Snapshotting**: For output parity tests, a mechanism to snapshot the legacy Oracle target table (`SOF$TA_P_BASISPROD`) *before* the migration run is crucial.

---

### 1. Schema Validation - Target Table

*   **Purpose**: To ensure the BigQuery target table `bert_dwh_prod.sof_ta_p_basisprod` has the correct schema (column names, data types, nullability) as derived from the legacy `SOF$TA_P_BASISPROD` table. This is a foundational check for data compatibility.
*   **Setup**:
    *   The `bert_dwh_prod.sof_ta_p_basisprod` table has been created in BigQuery using the provided DDL.
    *   Access to the legacy Oracle `SOF$TA_P_BASISPROD` table's schema definition.
*   **Action**:
    1.  Extract the schema (column names, data types, nullability) of the legacy `SOF$TA_P_BASISPROD` table from Oracle.
    2.  Extract the schema of the migrated `bert_dwh_prod.sof_ta_p_basisprod` table from BigQuery.
    3.  Compare the two schemas programmatically.
*   **Pass/Fail Criterion**:
    *   **PASS**: All column names match (case-insensitivity might be allowed if consistently handled). Oracle data types are correctly mapped to BigQuery data types (e.g., `VARCHAR2` to `STRING`, `NUMBER` to `INT64`/`BIGNUMERIC`, `DATE` to `TIMESTAMP`). Nullability constraints are equivalent.
    *   **FAIL**: Any mismatch in column names, incompatible data type mappings, or differing nullability constraints.

```python
# Example Python (pytest) assertion for schema validation
import pytest
from google.cloud import bigquery
import cx_Oracle # Assuming cx_Oracle for connecting to legacy DB

# Configuration
BIGQUERY_PROJECT_ID = "your-gcp-project-id"
BIGQUERY_DATASET_ID = "bert_dwh_prod"
BIGQUERY_TABLE_ID = "sof_ta_p_basisprod"

ORACLE_USER = "isbert_schema"
ORACLE_PASSWORD = "your_oracle_password"
ORACLE_DSN = "your_oracle_dsn" # e.g., 'hostname:port/service_name'
ORACLE_TABLE_NAME = "SOF$TA_P_BASISPROD"

def get_oracle_schema(user, password, dsn, table_name):
    """Fetches schema from Oracle."""
    conn = cx_Oracle.connect(user=user, password=password, dsn=dsn)
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT COLUMN_NAME, DATA_TYPE, NULLABLE
        FROM ALL_TAB_COLUMNS
        WHERE OWNER = UPPER('{user}') AND TABLE_NAME = UPPER('{table_name}')
        ORDER BY COLUMN_ID
    """)
    schema = []
    for col_name, data_type, nullable in cursor:
        schema.append({
            "name": col_name.upper(), # Oracle column names are typically uppercase
            "type": data_type,
            "nullable": (nullable == 'Y')
        })
    cursor.close()
    conn.close()
    return schema

def get_bigquery_schema(project_id, dataset_id, table_id):
    """Fetches schema from BigQuery."""
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)
    schema = []
    for field in table.schema:
        schema.append({
            "name": field.name.upper(), # BigQuery names might be lowercase, normalize for comparison
            "type": field.field_type,
            "nullable": (field.mode == 'NULLABLE')
        })
    return schema

def map_oracle_to_bigquery_type(oracle_type):
    """Simple mapping function for common types."""
    if 'VARCHAR' in oracle_type or 'CHAR' in oracle_type:
        return 'STRING'
    elif 'NUMBER' in oracle_type:
        # This might need more nuance depending on precision/scale
        return 'INT64' # Or BIGNUMERIC, FLOAT64
    elif 'DATE' in oracle_type:
        return 'TIMESTAMP' # Or DATE, DATETIME depending on exact Oracle usage
    # Add more mappings as needed
    return oracle_type # Fallback

def test_target_table_schema_parity():
    oracle_schema = get_oracle_schema(ORACLE_USER, ORACLE_PASSWORD, ORACLE_DSN, ORACLE_TABLE_NAME)
    bigquery_schema = get_bigquery_schema(BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID, BIGQUERY_TABLE_ID)

    # Basic check: number of columns
    assert len(oracle_schema) == len(bigquery_schema), \
        f"Column count mismatch: Oracle has {len(oracle_schema)}, BigQuery has {len(bigquery_schema)}"

    # Detailed column comparison
    oracle_schema_map = {col['name']: col for col in oracle_schema}
    bigquery_schema_map = {col['name']: col for col in bigquery_schema}

    for oracle_col_name, oracle_col_def in oracle_schema_map.items():
        assert oracle_col_name in bigquery_schema_map, \
            f"Column '{oracle_col_name}' missing in BigQuery target table."

        bq_col_def = bigquery_schema_map[oracle_col_name]

        # Check data type mapping
        expected_bq_type = map_oracle_to_bigquery_type(oracle_col_def['type'])
        assert bq_col_def['type'] == expected_bq_type, \
            f"Type mismatch for column '{oracle_col_name}': Oracle '{oracle_col_def['type']}' mapped to BigQuery '{bq_col_def['type']}', expected '{expected_bq_type}'."

        # Check nullability (if strict equivalence is required)
        # Note: BigQuery defaults to NULLABLE, Oracle might have NOT NULL.
        # This check depends on migration strategy for nullability.
        # For simplicity, assuming BigQuery columns are generally NULLABLE unless explicitly NOT NULL.
        # If Oracle column is NOT NULL, BigQuery should ideally be REQUIRED.
        if not oracle_col_def['nullable']:
            assert not bq_col_def['nullable'], \
                f"Nullability mismatch for column '{oracle_col_name}': Oracle is NOT NULL, BigQuery is NULLABLE."

    print(f"Schema validation passed for {BIGQUERY_TABLE_ID}.")

# To run this test:
# 1. Install google-cloud-bigquery and cx_Oracle.
# 2. Configure connection details.
# 3. Run `pytest your_test_file.py`
```

### 2. Source Data Synchronization Verification (Prerequisite)

*   **Purpose**: To confirm that the BigQuery source tables (`sof_ta_cntrct_dist`, `sof_ta_bcp_iccid`, etc.) accurately reflect the data in their corresponding legacy Oracle source tables. This is critical for ensuring that the migrated job operates on the same input data as the legacy job.
*   **Setup**:
    *   A snapshot of Oracle source tables has been loaded into BigQuery.
    *   The ongoing CDC/replication mechanism is active.
*   **Action**:
    1.  For each source table (`sof_ta_cntrct_dist`, `sof_ta_bcp_iccid`, `sof_ta_bcp_msisdn`, `sof_ta_cntrct_evn`, `sof_ta_iccid_vertrag`, `sof_ta_rn_vertrag`, `sof_ta_rn_da_vda_tk`, `sof_ta_tarifoption`, `sof_ta_apn_vertrag`, `dwtk_meldungen`):
        *   Query the row count from the Oracle table.
        *   Query the row count from the corresponding BigQuery table.
        *   Perform a checksum or hash comparison of the data (e.g., `MD5(CONCAT_WS('|', *))` for all columns) for a sample or the entire table if feasible.
        *   Alternatively, compare a random sample of rows or specific key-based lookups.
*   **Pass/Fail Criterion**:
    *   **PASS**: Row counts match exactly. Data checksums/hashes match for identical data sets. Sampled rows show exact data parity.
    *   **FAIL**: Any discrepancy in row counts or data content between Oracle and BigQuery source tables.

```sql
-- Example SQL assertion for row count parity (to be run for each source table)
-- Replace 'your_oracle_schema' and 'your_oracle_table' with actual values
-- Replace 'project_id.bert_dwh_prod.bq_table_name' with actual values

-- For sof_ta_cntrct_dist
SELECT
    (SELECT COUNT(*) FROM your_oracle_schema.SOF$TA_CNTRCT_DIST) AS oracle_count,
    (SELECT COUNT(*) FROM `project_id.bert_dwh_prod.sof_ta_cntrct_dist`) AS bigquery_count;

-- Pass if oracle_count = bigquery_count

-- Example SQL for data checksum (conceptual, actual implementation depends on data types and scale)
-- This would be run on both Oracle and BigQuery and results compared.
-- Oracle (conceptual):
SELECT ORA_HASH(DBMS_LOB.GETLENGTH(TO_CLOB(
    CNTRCT_ID || '|' || EVN || '|' || ... -- Concatenate all columns
))) AS data_checksum
FROM your_oracle_schema.SOF$TA_CNTRCT_DIST;

-- BigQuery (conceptual):
SELECT FARM_FINGERPRINT(TO_JSON_STRING(t)) AS data_checksum
FROM `project_id.bert_dwh_prod.sof_ta_cntrct_dist` AS t;

-- Pass if checksums match for identical data.
```

### 3. Date Parameter (`stichtag`) Calculation Parity

*   **Purpose**: To verify that the `_get_stichtag` Python function in the Airflow DAG correctly replicates the legacy Oracle `v_datum` determination logic, specifically the `MAX(timecreated)` from `dwtk_meldungen` for `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
*   **Setup**:
    *   The `bert_dwh_prod.dwtk_meldungen` table in BigQuery contains data mirroring `isbert_schema.dwtk_meldungen`.
    *   The legacy Oracle `isbert_schema.dwtk_meldungen` table contains relevant data for `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
*   **Action**:
    1.  Manually or programmatically execute the legacy Oracle SQL to determine `v_datum`:
        ```sql
        SELECT NVL(TO_CHAR(MAX(timecreated), 'YYYYMMDD'), '19000101')
        FROM isbert_schema.dwtk_meldungen
        WHERE job_kennung = 'BERT_DROP_TEMP_TABLE';
        ```
    2.  Trigger the Airflow DAG `bert_ausd_bp_ta_p_basisprod_dag`.
    3.  Inspect the XCom output of the `parameter_setup_and_date_determination` task to retrieve the `stichtag` value.
*   **Pass/Fail Criterion**:
    *   **PASS**: The `stichtag` value pushed to XCom by the Airflow task exactly matches the `v_datum` calculated from the legacy Oracle query.
    *   **FAIL**: Any discrepancy in the calculated date.

```python
# Example Python (pytest) assertion for stichtag calculation
import pytest
from google.cloud import bigquery
import cx_Oracle
from datetime import datetime

# Configuration (same as schema validation)
BIGQUERY_PROJECT_ID = "your-gcp-project-id"
BIGQUERY_DATASET_ID = "bert_dwh_prod"
BIGQUERY_DWTK_MELDUNGEN_TABLE = "dwtk_meldungen"

ORACLE_USER = "isbert_schema"
ORACLE_PASSWORD = "your_oracle_password"
ORACLE_DSN = "your_oracle_dsn"
ORACLE_DWTK_MELDUNGEN_TABLE = "DWTK_MELDUNGEN"

def get_legacy_stichtag(user, password, dsn, table_name):
    """Calculates stichtag from legacy Oracle logic."""
    conn = cx_Oracle.connect(user=user, password=password, dsn=dsn)
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT NVL(TO_CHAR(MAX(TIMECREATED), 'YYYYMMDD'), '19000101')
        FROM {user}.{table_name}
        WHERE JOB_KENNUNG = 'BERT_DROP_TEMP_TABLE'
    """)
    result = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return result

def get_migrated_stichtag_from_bq(project_id, dataset_id, table_id):
    """Simulates the BigQuery stichtag calculation from the DAG's PythonOperator."""
    client = bigquery.Client(project=project_id)
    query = f"""
    SELECT
        COALESCE(FORMAT_DATE('%Y%m%d', MAX(timecreated)), '19000101')
    FROM
        `{project_id}.{dataset_id}.{table_id}`
    WHERE
        job_kennung = 'BERT_DROP_TEMP_TABLE'
    """
    query_job = client.query(query)
    rows = query_job.result()
    stichtag = next(rows)[0]
    return stichtag

def test_stichtag_calculation_parity():
    legacy_stichtag = get_legacy_stichtag(ORACLE_USER, ORACLE_PASSWORD, ORACLE_DSN, ORACLE_DWTK_MELDUNGEN_TABLE)
    migrated_stichtag = get_migrated_stichtag_from_bq(BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID, BIGQUERY_DWTK_MELDUNGEN_TABLE)

    assert legacy_stichtag == migrated_stichtag, \
        f"Stichtag calculation mismatch: Legacy='{legacy_stichtag}', Migrated='{migrated_stichtag}'"
    print(f"Stichtag calculation parity passed: {legacy_stichtag}")

# Note: For actual Airflow DAG testing, you'd trigger the DAG and read XComs.
# This example directly tests the underlying logic.
```

### 4. Target Table Truncation Verification

*   **Purpose**: To confirm that the `truncate_target_table_task` in the Airflow DAG correctly truncates the `bert_dwh_prod.sof_ta_p_basisprod` table before the main transformation, mirroring the `isbert_schema.dwpa_util_skript.runstatement` call.
*   **Setup**:
    *   The `bert_dwh_prod.sof_ta_p_basisprod` table contains some existing data.
    *   The Airflow DAG is configured.
*   **Action**:
    1.  Insert a few dummy rows into `bert_dwh_prod.sof_ta_p_basisprod`.
    2.  Trigger the Airflow DAG `bert_ausd_bp_ta_p_basisprod_dag`.
    3.  Immediately after the `truncate_target_table_task` completes (or before `execute_transformation_task` starts), query the row count of `bert_dwh_prod.sof_ta_p_basisprod`.
*   **Pass/Fail Criterion**:
    *   **PASS**: The row count of `bert_dwh_prod.sof_ta_p_basisprod` is 0 after the truncate task.
    *   **FAIL**: The table still contains rows.

```sql
-- Example SQL assertion for BigQuery
-- Step 1: Insert dummy data (manual or via test setup)
-- INSERT INTO `project_id.bert_dwh_prod.sof_ta_p_basisprod` (...) VALUES (...);

-- Step 2: Trigger Airflow DAG.

-- Step 3: Query row count after truncate task
SELECT COUNT(*) FROM `project_id.bert_dwh_prod.sof_ta_p_basisprod`;

-- Pass if result is 0.
```

### 5. Row Count Parity (Target Table)

*   **Purpose**: To ensure that the migrated BigQuery job produces the same number of output rows in `bert_dwh_prod.sof_ta_p_basisprod` as the legacy Oracle job produces in `SOF$TA_P_BASISPROD` for identical input data.
*   **Setup**:
    *   Identical source data loaded into both Oracle and BigQuery source tables.
    *   Both legacy and migrated jobs have been executed with the same `stichtag` (or equivalent date parameter).
*   **Action**:
    1.  Execute the legacy Oracle job.
    2.  Query the row count from `SOF$TA_P_BASISPROD`.
    3.  Execute the migrated Airflow DAG.
    4.  Query the row count from `bert_dwh_prod.sof_ta_p_basisprod`.
*   **Pass/Fail Criterion**:
    *   **PASS**: The row count from `bert_dwh_prod.sof_ta_p_basisprod` exactly matches the row count from `SOF$TA_P_BASISPROD`.
    *   **FAIL**: Any discrepancy in row counts.

```sql
-- Example SQL assertion for row count parity
SELECT
    (SELECT COUNT(*) FROM your_oracle_schema.SOF$TA_P_BASISPROD) AS oracle_count,
    (SELECT COUNT(*) FROM `project_id.bert_dwh_prod.sof_ta_p_basisprod`) AS bigquery_count;

-- Pass if oracle_count = bigquery_count
```

### 6. Full Data Parity (Target Table)

*   **Purpose**: This is the most critical test for "output parity". To verify that for identical input data, the migrated BigQuery job produces exactly the same data in `bert_dwh_prod.sof_ta_p_basisprod` as the legacy Oracle job produces in `SOF$TA_P_BASISPROD`, column by column.
*   **Setup**:
    *   Identical source data loaded into both Oracle and BigQuery source tables.
    *   Both legacy and migrated jobs have been executed with the same `stichtag`.
    *   A temporary staging table in BigQuery containing the data from the legacy Oracle `SOF$TA_P_BASISPROD` table (after its run).
*   **Action**:
    1.  Execute the legacy Oracle job.
    2.  Extract the data from `SOF$TA_P_BASISPROD` and load it into a BigQuery staging table (e.g., `bert_dwh_prod.sof_ta_p_basisprod_legacy_snapshot`). Ensure data types are mapped correctly during this load.
    3.  Execute the migrated Airflow DAG.
    4.  Compare `bert_dwh_prod.sof_ta_p_basisprod` with `bert_dwh_prod.sof_ta_p_basisprod_legacy_snapshot` using a full outer join and checking for differences.
*   **Pass/Fail Criterion**:
    *   **PASS**: No differences are found between the migrated BigQuery output and the legacy Oracle output (after accounting for potential minor differences like floating-point precision or timestamp representation if not strictly identical).
    *   **FAIL**: Any row or column value mismatch.

```sql
-- Example SQL assertion for full data parity in BigQuery
-- Assumes `bert_dwh_prod.sof_ta_p_basisprod_legacy_snapshot` contains the Oracle output.

SELECT
    'Only in Legacy' AS diff_type,
    legacy.*
FROM
    `project_id.bert_dwh_prod.sof_ta_p_basisprod_legacy_snapshot` AS legacy
FULL OUTER JOIN
    `project_id.bert_dwh_prod.sof_ta_p_basisprod` AS migrated
ON
    legacy.CNTRCT_ID = migrated.CNTRCT_ID -- Assuming CNTRCT_ID is the primary key
    -- Add all other key columns if composite primary key
WHERE
    migrated.CNTRCT_ID IS NULL -- Row exists only in legacy
    OR NOT (
        -- Compare all columns, handling NULLs
        COALESCE(legacy.CNTRCT_ID, '') = COALESCE(migrated.CNTRCT_ID, '') AND
        COALESCE(legacy.EVN, '') = COALESCE(migrated.EVN, '') AND
        -- ... repeat for all 100+ columns ...
        COALESCE(FORMAT_TIMESTAMP('%Y%m%d%H%M%S', legacy.TNV_ICC_VALID), '') = COALESCE(FORMAT_TIMESTAMP('%Y%m%d%H%M%S', migrated.TNV_ICC_VALID), '')
        -- Note: For TIMESTAMP comparisons, consider formatting to a common string representation
        -- or using TIMESTAMP_DIFF with a small tolerance if precision differences are expected.
    )
UNION ALL
SELECT
    'Only in Migrated' AS diff_type,
    migrated.*
FROM
    `project_id.bert_dwh_prod.sof_ta_p_basisprod_legacy_snapshot` AS legacy
FULL OUTER JOIN
    `project_id.bert_dwh_prod.sof_ta_p_basisprod` AS migrated
ON
    legacy.CNTRCT_ID = migrated.CNTRCT_ID
WHERE
    legacy.CNTRCT_ID IS NULL; -- Row exists only in migrated

-- Pass if the query returns 0 rows.
```

### 7. Transformation Logic - `APN` Field (`DECODE` / `CASE`)

*   **Purpose**: To specifically test the translation of the Oracle `DECODE` function for the `APN` field into BigQuery's `CASE WHEN ... END` and `CONCAT` logic.
*   **Setup**:
    *   Source data in `sof_ta_apn_vertrag` (both Oracle and BigQuery) with various combinations for `apn` and `apn_cntrct` (e.g., both NULL, `apn` NULL `apn_cntrct` not NULL, `apn` not NULL `apn_cntrct` NULL, both not NULL).
    *   Ensure `sof_ta_cntrct_dist` has `cntrct_id`s that join to these `sof_ta_apn_vertrag` records.
    *   Both jobs executed.
*   **Action**:
    1.  Query the `APN` column from `SOF$TA_P_BASISPROD` for specific `CNTRCT_ID`s.
    2.  Query the `APN` column from `bert_dwh_prod.sof_ta_p_basisprod` for the same `CNTRCT_ID`s.
*   **Pass/Fail Criterion**:
    *   **PASS**: The `APN` values match exactly for all test cases.
    *   **FAIL**: Any mismatch in the `APN` field.

```sql
-- Example SQL assertion for APN field
SELECT
    t1.CNTRCT_ID,
    t1.APN AS legacy_apn,
    t2.APN AS migrated_apn
FROM
    `project_id.bert_dwh_prod.sof_ta_p_basisprod_legacy_snapshot` AS t1
JOIN
    `project_id.bert_dwh_prod.sof_ta_p_basisprod` AS t2
ON
    t1.CNTRCT_ID = t2.CNTRCT_ID
WHERE
    t1.APN IS DISTINCT FROM t2.APN; -- Checks for differences, including NULL vs non-NULL

-- Pass if the query returns 0 rows.
```

### 8. Transformation Logic - BCP Subquery Join

*   **Purpose**: To validate the correct translation of the `sof_ta_bcp_iccid` INNER JOIN `sof_ta_bcp_msisdn` subquery and its subsequent LEFT JOIN to `sof_ta_cntrct_dist`. This ensures complex join logic is preserved.
*   **Setup**:
    *   Test data in `sof_ta_bcp_iccid` and `sof_ta_bcp_msisdn` (both Oracle and BigQuery) covering:
        *   Matching `CNTRCT_ID` and `CNTRCT_ID_REF` in both BCP tables.
        *   `CNTRCT_ID` in `sof_ta_cntrct_dist` that matches the BCP subquery result.
        *   `CNTRCT_ID` in `sof_ta_cntrct_dist` that *does not* match the BCP subquery result (to test LEFT JOIN behavior).
        *   Records in one BCP table but not the other (to test INNER JOIN behavior).
    *   Both jobs executed.
*   **Action**:
    1.  Query `BCP_VERTRAG`, `BCP_ICCID`, `BCP_HLR`, `BCP_TN_TEL` from `SOF$TA_P_BASISPROD` for specific `CNTRCT_ID`s.
    2.  Query the same fields from `bert_dwh_prod.sof_ta_p_basisprod` for the same `CNTRCT_ID`s.
*   **Pass/Fail Criterion**:
    *   **PASS**: All BCP-related fields match exactly, including NULLs where no join occurred.
    *   **FAIL**: Any mismatch in these fields.

```sql
-- Example SQL assertion for BCP fields
SELECT
    t1.CNTRCT_ID,
    t1.BCP_VERTRAG AS legacy_bcp_vertrag,
    t2.BCP_VERTRAG AS migrated_bcp_vertrag,
    t1.BCP_ICCID AS legacy_bcp_iccid,
    t2.BCP_ICCID AS migrated_bcp_iccid,
    t1.BCP_HLR AS legacy_bcp_hlr,
    t2.BCP_HLR AS migrated_bcp_hlr,
    t1.BCP_TN_TEL AS legacy_bcp_tn_tel,
    t2.BCP_TN_TEL AS migrated_bcp_tn_tel
FROM
    `project_id.bert_dwh_prod.sof_ta_p_basisprod_legacy_snapshot` AS t1
JOIN
    `project_id.bert_dwh_prod.sof_ta_p_basisprod` AS t2
ON
    t1.CNTRCT_ID = t2.CNTRCT_ID
WHERE
    t1.BCP_VERTRAG IS DISTINCT FROM t2.BCP_VERTRAG OR
    t1.BCP_ICCID IS DISTINCT FROM t2.BCP_ICCID OR
    t1.BCP_HLR IS DISTINCT FROM t2.BCP_HLR OR
    t1.BCP_TN_TEL IS DISTINCT FROM t2.BCP_TN_TEL;

-- Pass if the query returns 0 rows.
```

### 9. Transformation Logic - NULL Handling in Joins

*   **Purpose**: To ensure that `LEFT JOIN` behavior in BigQuery correctly mirrors Oracle's `OUTER JOIN (+)` syntax, especially concerning NULL values in non-matching records.
*   **Setup**:
    *   Test data in `sof_ta_cntrct_dist` with `cntrct_id`s that *do not* have matches in one or more of the LEFT JOINed tables (e.g., `sof_ta_cntrct_evn`, `sof_ta_iccid_vertrag`, etc.).
    *   Both jobs executed.
*   **Action**:
    1.  Select a `CNTRCT_ID` from `sof_ta_cntrct_dist` that is known to have no matching record in, for example, `sof_ta_cntrct_evn`.
    2.  Query the `EVN` column from `SOF$TA_P_BASISPROD` for this `CNTRCT_ID`.
    3.  Query the `EVN` column from `bert_dwh_prod.sof_ta_p_basisprod` for the same `CNTRCT_ID`.
    4.  Repeat for other LEFT JOINed tables and their respective columns.
*   **Pass/Fail Criterion**:
    *   **PASS**: All columns from non-matching LEFT JOINed tables are NULL in both legacy and migrated outputs.
    *   **FAIL**: Any non-NULL value where a NULL is expected, or vice-versa.

```sql
-- Example SQL assertion for NULL handling (e.g., for EVN field)
SELECT
    t1.CNTRCT_ID,
    t1.EVN AS legacy_evn,
    t2.EVN AS migrated_evn
FROM
    `project_id.bert_dwh_prod.sof_ta_p_basisprod_legacy_snapshot` AS t1
JOIN
    `project_id.bert_dwh_prod.sof_ta_p_basisprod` AS t2
ON
    t1.CNTRCT_ID = t2.CNTRCT_ID
WHERE
    t1.EVN IS NULL AND t2.EVN IS NOT NULL OR
    t1.EVN IS NOT NULL AND t2.EVN IS NULL OR
    t1.EVN != t2.EVN; -- If both are not null, check for value difference

-- Pass if the query returns 0 rows.
```

### 10. Transformation Logic - Data Type Conversion (TIMESTAMP)

*   **Purpose**: To verify that Oracle `DATE` columns are correctly converted to BigQuery `TIMESTAMP` columns, preserving precision and value.
*   **Setup**:
    *   Source data in Oracle tables (e.g., `sof$ta_iccid_vertrag.tn_valid_to`) with various date/time values, including dates with time components, dates without time components, and NULLs.
    *   Both jobs executed.
*   **Action**:
    1.  Query a specific `TIMESTAMP` column (e.g., `TNV_ICC_VALID`) from `SOF$TA_P_BASISPROD` for various `CNTRCT_ID`s.
    2.  Query the same `TIMESTAMP` column from `bert_dwh_prod.sof_ta_p_basisprod` for the same `CNTRCT_ID`s.
    3.  Compare the values. Pay attention to time zones if applicable, though the design implies direct mapping.
*   **Pass/Fail Criterion**:
    *   **PASS**: All `TIMESTAMP` values match exactly (or within acceptable precision limits if Oracle DATE stores less precision than BigQuery TIMESTAMP). NULLs are also matched.
    *   **FAIL**: Any discrepancy in date/time values.

```sql
-- Example SQL assertion for TIMESTAMP field
SELECT
    t1.CNTRCT_ID,
    t1.TNV_ICC_VALID AS legacy_tnv_icc_valid,
    t2.TNV_ICC_VALID AS migrated_tnv_icc_valid
FROM
    `project_id.bert_dwh_prod.sof_ta_p_basisprod_legacy_snapshot` AS t1
JOIN
    `project_id.bert_dwh_prod.sof_ta_p_basisprod` AS t2
ON
    t1.CNTRCT_ID = t2.CNTRCT_ID
WHERE
    t1.TNV_ICC_VALID IS DISTINCT FROM t2.TNV_ICC_VALID;

-- Pass if the query returns 0 rows.
```

### 11. External System Replacement - Logging Verification

*   **Purpose**: To ensure that the migrated job's logging mechanism (Cloud Logging or BigQuery logging table) captures essential job execution information, mirroring the `DWMSG_*` functions from the legacy KornShell scripts.
*   **Setup**:
    *   The Airflow DAG is configured with the `_log_status` task.
    *   Cloud Logging is enabled for the Airflow environment, or a BigQuery logging table is set up as per the design's placeholder.
*   **Action**:
    1.  Trigger the Airflow DAG.
    2.  Monitor Cloud Logging for entries related to the DAG run, specifically from the `logging_and_status_update` task.
    3.  Verify that the log messages contain expected information (e.g., job name, execution date, status, `stichtag`).
*   **Pass/Fail Criterion**:
    *   **PASS**: Relevant log messages are generated in Cloud Logging (or the BigQuery logging table) for successful and failed runs, containing key job metadata.
    *   **FAIL**: Missing log entries, incorrect status reporting, or insufficient detail in logs.

```python
# Example Python (pytest) for checking Cloud Logging (conceptual)
# This would typically involve using the Google Cloud Logging client library
# to query logs after a DAG run.

import pytest
from google.cloud import logging_v2
from datetime import datetime, timedelta
import time

# Configuration
GCP_PROJECT_ID = "your-gcp-project-id"
DAG_ID = "bert_ausd_bp_ta_p_basisprod_dag"

def get_recent_logs(project_id, dag_id, since_minutes=10):
    """Fetches recent logs for a given DAG."""
    client = logging_v2.Client(project=project_id)
    filter_str = f'resource.type="cloud_composer_environment" AND labels.dag_id="{dag_id}" AND timestamp>="{datetime.utcnow() - timedelta(minutes=since_minutes):%Y-%m-%dT%H:%M:%SZ}"'
    entries = client.list_entries(filter_=filter_str)
    return list(entries)

def test_logging_status_update():
    # Trigger the DAG (this part would be external or mocked in a real test)
    # For this example, assume the DAG has just run.
    # In a real scenario, you'd use Airflow's API to trigger the DAG.
    print(f"Assuming DAG '{DAG_ID}' has just been triggered and completed.")
    time.sleep(30) # Give time for logs to propagate

    logs = get_recent_logs(GCP_PROJECT_ID, DAG_ID)

    assert len(logs) > 0, "No logs found for the DAG run."

    # Check for specific log messages from the _log_status task
    status_log_found = False
    for entry in logs:
        if entry.payload and isinstance(entry.payload, dict) and "message" in entry.payload:
            if f"Job DW.BERT_AUSD_BP_TA_P_BASISPROD completed with status: SUCCESS" in entry.payload["message"]:
                status_log_found = True
                break
        elif entry.text_payload and f"Job DW.BERT_AUSD_BP_TA_P_BASISPROD completed with status: SUCCESS" in entry.text_payload:
            status_log_found = True
            break

    assert status_log_found, "Expected 'Job completed with status: SUCCESS' log message not found."
    print("Logging status update test passed.")
```

### 12. Edge Case - Empty Source Tables

*   **Purpose**: To verify that the migrated job handles scenarios where one or more source tables are empty gracefully, producing an empty target table or expected NULLs without errors.
*   **Setup**:
    *   Load empty data into all BigQuery source tables (`sof_ta_cntrct_dist`, `sof_ta_bcp_iccid`, etc.).
    *   Ensure corresponding Oracle tables are also empty.
    *   Both jobs executed.
*   **Action**:
    1.  Execute the legacy Oracle job.
    2.  Query the row count from `SOF$TA_P_BASISPROD`.
    3.  Execute the migrated Airflow DAG.
    4.  Query the row count from `bert_dwh_prod.sof_ta_p_basisprod`.
*   **Pass/Fail Criterion**:
    *   **PASS**: Both legacy and migrated jobs complete successfully without errors, and both target tables (`SOF$TA_P_BASISPROD` and `bert_dwh_prod.sof_ta_p_basisprod`) contain 0 rows.
    *   **FAIL**: Job failure, or non-zero rows in the target table.

```sql
-- Example SQL assertion for empty source tables
SELECT
    (SELECT COUNT(*) FROM your_oracle_schema.SOF$TA_P_BASISPROD) AS oracle_count,
    (SELECT COUNT(*) FROM `project_id.bert_dwh_prod.sof_ta_p_basisprod`) AS bigquery_count;

-- Pass if oracle_count = 0 AND bigquery_count = 0.
```

### 13. Edge Case - All NULLs in Key Fields

*   **Purpose**: To verify how the join logic and transformation handle records where key fields (e.g., `cntrct_id`) are NULL in source tables.
*   **Setup**:
    *   Load data into source tables where `cntrct_id` (or other join keys) is NULL for some records.
    *   Ensure corresponding Oracle tables have similar data.
    *   Both jobs executed.
*   **Action**:
    1.  Execute the legacy Oracle job.
    2.  Query `SOF$TA_P_BASISPROD` for records where `CNTRCT_ID` (or other relevant fields) might be NULL or affected by NULL join keys.
    3.  Execute the migrated Airflow DAG.
    4.  Query `bert_dwh_prod.sof_ta_p_basisprod` for the same conditions.
*   **Pass/Fail Criterion**:
    *   **PASS**: The output records (including their presence/absence and column values) match between legacy and migrated systems for these NULL key scenarios.
    *   **FAIL**: Discrepancies in output due to different NULL handling in joins or transformations.

```sql
-- Example SQL assertion for NULLs in join keys (part of full data parity, but can be focused)
-- Assuming CNTRCT_ID is the primary join key.
SELECT
    t1.CNTRCT_ID,
    t1.EVN AS legacy_evn,
    t2.EVN AS migrated_evn
FROM
    `project_id.bert_dwh_prod.sof_ta_p_basisprod_legacy_snapshot` AS t1
FULL OUTER JOIN
    `project_id.bert_dwh_prod.sof_ta_p_basisprod` AS t2
ON
    t1.CNTRCT_ID = t2.CNTRCT_ID
WHERE
    (t1.CNTRCT_ID IS NULL OR t2.CNTRCT_ID IS NULL) -- Focus on rows where CNTRCT_ID might be NULL or not join
    AND (t1.EVN IS DISTINCT FROM t2.EVN); -- Check for differences in other fields

-- Pass if the query returns 0 rows.
```

---

These tests provide a robust framework for validating the migration of `DW.BERT_AUSD_BP_TA_P_BASISPROD`. The emphasis on comparing outputs and specific transformation logic, combined with checks for external system replacements and edge cases, ensures a high degree of confidence in the migrated solution's behavioral equivalence.