As a senior data-migration QA engineer, I've analyzed the migration design for `DW.BERT_AUSD_BP_TA_ICCID_VERTRAG`. The migration involves moving from an Oracle-based KornShell/SQL workflow to a Google Cloud Composer (Airflow) and BigQuery-based solution. The core transformation is a `GROUP BY` and `MAX()` aggregation to pivot ICCID data by contract ID.

The following test cases are designed to ensure the migrated job is behaviourally equivalent to the legacy system, covering output parity, transformation correctness, external system interactions, and data quality.

---

## Migration Validation Tests for DW.BERT_AUSD_BP_TA_ICCID_VERTRAG

### Test Environment Setup (Pre-requisites for all tests)

*   **Legacy Environment:** Access to the Oracle database (`SOF$TA_ICCID_EINZELN`, `SOF$TA_ICCID_VERTRAG`) and the ability to execute the legacy KornShell scripts (`r_ausd_bp_ta_iccid_vertrag.ksh`, `k_ausd_bp_ta_iccid_vertrag.ksh`, `d_ausd_bp_ta_iccid_vertrag.sql`).
*   **Migrated Environment:** Access to a Google Cloud Project with BigQuery (`PROJECT_ID.DATASET.SOF_TA_ICCID_EINZELN`, `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG`) and a Cloud Composer environment with the `dag_dw_bert_ausd_bp_ta_iccid_vertrag` DAG deployed.
*   **Data Comparison Tool:** A robust tool or script capable of comparing large datasets between Oracle and BigQuery, handling potential differences in data types (e.g., Oracle `NUMBER` vs. BigQuery `INT64`/`NUMERIC`, Oracle `DATE` vs. BigQuery `DATE`/`TIMESTAMP`).
*   **Test Data:** A controlled set of input data for `SOF$TA_ICCID_EINZELN` that can be loaded into both Oracle and BigQuery.

---

### 1. Output Parity - Full Data Comparison

**Purpose:** To verify that for identical input data, the migrated job produces an output table (`SOF_TA_ICCID_VERTRAG`) that is exactly identical to the output produced by the legacy job in `SOF$TA_ICCID_VERTRAG`. This is the most comprehensive test for behavioral equivalence.

**Setup:**
1.  Prepare a comprehensive test dataset for `SOF$TA_ICCID_EINZELN` covering various scenarios:
    *   Contracts with single ICCID types (TN, TC, TB, MS1-MS10).
    *   Contracts with multiple ICCID types.
    *   Contracts with all fields populated.
    *   Contracts with some fields NULL for certain ICCID types.
    *   Contracts where all ICCID-related fields are NULL for a specific type.
    *   Contracts with multiple records for the *same* ICCID type (to test `MAX()` behavior).
    *   A contract with no associated ICCID records in the source (should not appear in target).
2.  Load this test dataset into the Oracle `SOF$TA_ICCID_EINZELN` table.
3.  Load the *exact same* test dataset into the BigQuery `PROJECT_ID.DATASET.SOF_TA_ICCID_EINZELN` table.
4.  Ensure both target tables (`SOF$TA_ICCID_VERTRAG` in Oracle and `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG` in BigQuery) are empty before execution.

**Action:**
1.  Execute the legacy job `DW.BERT_AUSD_BP_TA_ICCID_VERTRAG` via its Automic/KornShell entry point.
2.  Execute the migrated Airflow DAG `dag_dw_bert_ausd_bp_ta_iccid_vertrag`.
3.  Extract all data from the Oracle `SOF$TA_ICCID_VERTRAG` table.
4.  Extract all data from the BigQuery `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG` table.

**Pass/Fail Criterion:**
The extracted datasets from Oracle and BigQuery must be identical in terms of:
*   Number of rows.
*   Number of columns.
*   Column names and their order.
*   Data types (allowing for BigQuery's equivalent types).
*   All cell values, including NULLs, for every row and column.

**Runnable Test Code (Conceptual SQL for comparison):**

```sql
-- Step 1: Count rows in both tables
SELECT COUNT(*) FROM SOF$TA_ICCID_VERTRAG; -- Legacy Oracle
SELECT COUNT(*) FROM `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG`; -- Migrated BigQuery

-- Step 2: Compare row counts
-- If counts differ, investigate immediately.

-- Step 3: Full data comparison (example for a subset of columns)
-- This requires a robust comparison tool or a series of SQL queries.
-- For a direct SQL comparison, you might need to export both to a common format
-- or use federated queries if possible.
-- Here's a conceptual SQL approach for comparing a sample of data:

-- Create a temporary view/table in BigQuery from Oracle data (if federated query is an option)
-- Or, more practically, export Oracle data to GCS and load into a temp BQ table.
-- CREATE OR REPLACE TABLE `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG_ORACLE_EXPORT` AS
-- SELECT * FROM EXTERNAL_TABLE_LINK_TO_ORACLE.SOF$TA_ICCID_VERTRAG;

-- Compare row-by-row, column-by-column
-- This query identifies rows present in BigQuery but not in Oracle (after type casting)
SELECT 'Only in BigQuery' AS source, A.*
FROM `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG` AS A
LEFT JOIN `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG_ORACLE_EXPORT` AS B
  ON A.CNTRCT_ID = B.CNTRCT_ID
  AND A.TN_ICCID = B.TN_ICCID -- Repeat for all 118 columns, handling NULLs
  -- ... (add all columns for comparison, using IFNULL or COALESCE for NULL-safe comparison)
WHERE B.CNTRCT_ID IS NULL;

-- This query identifies rows present in Oracle but not in BigQuery
SELECT 'Only in Oracle' AS source, B.*
FROM `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG_ORACLE_EXPORT` AS B
LEFT JOIN `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG` AS A
  ON A.CNTRCT_ID = B.CNTRCT_ID
  AND A.TN_ICCID = B.TN_ICCID -- Repeat for all 118 columns, handling NULLs
  -- ...
WHERE A.CNTRCT_ID IS NULL;

-- This query identifies rows with differing values for the same CNTRCT_ID
SELECT 'Mismatch' AS source, A.CNTRCT_ID,
       A.TN_ICCID AS BQ_TN_ICCID, B.TN_ICCID AS Oracle_TN_ICCID,
       -- ... (repeat for all columns)
FROM `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG` AS A
JOIN `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG_ORACLE_EXPORT` AS B
  ON A.CNTRCT_ID = B.CNTRCT_ID
WHERE NOT (
    (A.TN_ICCID IS NULL AND B.TN_ICCID IS NULL) OR (A.TN_ICCID = B.TN_ICCID)
    -- ... (repeat for all columns)
);

-- Pass if all three comparison queries return 0 rows.
```

---

### 2. Transformation Correctness - Aggregation and Pivoting Logic

**Purpose:** To specifically test the `GROUP BY CNTRCT_ID` and `MAX()` aggregation logic, ensuring that ICCID types are correctly pivoted into their respective columns.

**Setup:**
1.  Prepare a small, targeted test dataset for `SOF$TA_ICCID_EINZELN` (Oracle) and `PROJECT_ID.DATASET.SOF_TA_ICCID_EINZELN` (BigQuery) with the following characteristics for a single `CNTRCT_ID` (e.g., `101`):
    *   One record for `ICCID_TYPE = 'TN'` with `TN_ICCID = 'TN_VAL_1'`, `TN_IMSI_MCC = '123'`, etc.
    *   One record for `ICCID_TYPE = 'TC'` with `TC_ICCID = 'TC_VAL_1'`, `TC_IMSI_MCC = '456'`, etc.
    *   One record for `ICCID_TYPE = 'MS1'` with `MS1_ICCID = 'MS1_VAL_1'`, `MS1_IMSI_MCC = '789'`, etc.
    *   All other pivoted columns in these source records should be `NULL`.
2.  Ensure both target tables are empty.

**Action:**
1.  Execute the legacy job.
2.  Execute the migrated Airflow DAG.
3.  Query the `SOF$TA_ICCID_VERTRAG` (Oracle) and `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG` (BigQuery) tables for `CNTRCT_ID = 101`.

**Pass/Fail Criterion:**
For `CNTRCT_ID = 101`, both target tables must contain exactly one row. This row must have:
*   `TN_ICCID = 'TN_VAL_1'`, `TN_IMSI_MCC = '123'`, etc.
*   `TC_ICCID = 'TC_VAL_1'`, `TC_IMSI_MCC = '456'`, etc.
*   `MS1_ICCID = 'MS1_VAL_1'`, `MS1_IMSI_MCC = '789'`, etc.
*   All other `MSx_ICCID`, `TB_ICCID`, and their associated fields must be `NULL`.
The values for all columns must match between Oracle and BigQuery.

**Runnable Test Code (SQL Assertion):**

```sql
-- Expected output for CNTRCT_ID = 101
SELECT
    CNTRCT_ID,
    TN_ICCID, TN_IMSI_MCC,
    TC_ICCID, TC_IMSI_MCC,
    MS1_ICCID, MS1_IMSI_MCC,
    MS2_ICCID, MS2_IMSI_MCC -- Check a few other MSx fields for NULL
FROM `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG`
WHERE CNTRCT_ID = 101;

-- Expected result:
-- CNTRCT_ID | TN_ICCID | TN_IMSI_MCC | TC_ICCID | TC_IMSI_MCC | MS1_ICCID | MS1_IMSI_MCC | MS2_ICCID | MS2_IMSI_MCC
-- ----------|----------|-------------|----------|-------------|-----------|--------------|-----------|--------------
-- 101       | TN_VAL_1 | 123         | TC_VAL_1 | 456         | MS1_VAL_1 | 789          | NULL      | NULL

-- Assertions in a testing framework (e.g., Python with BigQuery client):
# from google.cloud import bigquery
# client = bigquery.Client()
# query = """
#     SELECT
#         CNTRCT_ID,
#         TN_ICCID, TN_IMSI_MCC,
#         TC_ICCID, TC_IMSI_MCC,
#         MS1_ICCID, MS1_IMSI_MCC,
#         MS2_ICCID, MS2_IMSI_MCC
#     FROM `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG`
#     WHERE CNTRCT_ID = 101
# """
# rows = list(client.query(query).result())
# assert len(rows) == 1
# row = rows[0]
# assert row["CNTRCT_ID"] == 101
# assert row["TN_ICCID"] == "TN_VAL_1"
# assert row["TN_IMSI_MCC"] == "123"
# assert row["TC_ICCID"] == "TC_VAL_1"
# assert row["TC_IMSI_MCC"] == "456"
# assert row["MS1_ICCID"] == "MS1_VAL_1"
# assert row["MS1_IMSI_MCC"] == "789"
# assert row["MS2_ICCID"] is None
# assert row["MS2_IMSI_MCC"] is None
```

---

### 3. Transformation Correctness - NULL Handling in Aggregation

**Purpose:** To verify that the `MAX()` aggregation correctly handles `NULL` values, ensuring that a non-`NULL` value is chosen over `NULL` if available, and `NULL` is returned if all values for a given pivoted field are `NULL`.

**Setup:**
1.  Prepare a test dataset for `SOF$TA_ICCID_EINZELN` (Oracle) and `PROJECT_ID.DATASET.SOF_TA_ICCID_EINZELN` (BigQuery) for `CNTRCT_ID = 102`:
    *   Record 1: `CNTRCT_ID = 102`, `ICCID_TYPE = 'TN'`, `TN_ICCID = 'TN_A'`, `TN_IMSI_MCC = '111'`, `TN_STATUS = 'ACTIVE'`, `TN_VALID_TO = '2024-12-31'`, `TN_E_ID = 'E1'`, `TN_CARD_TYPE_NAME = 'TypeA'`
    *   Record 2: `CNTRCT_ID = 102`, `ICCID_TYPE = 'TN'`, `TN_ICCID = NULL`, `TN_IMSI_MCC = '222'`, `TN_STATUS = NULL`, `TN_VALID_TO = '2025-01-01'`, `TN_E_ID = NULL`, `TN_CARD_TYPE_NAME = 'TypeB'`
    *   Record 3: `CNTRCT_ID = 102`, `ICCID_TYPE = 'TN'`, `TN_ICCID = NULL`, `TN_IMSI_MCC = NULL`, `TN_STATUS = NULL`, `TN_VALID_TO = NULL`, `TN_E_ID = NULL`, `TN_CARD_TYPE_NAME = NULL`
    *   Record 4: `CNTRCT_ID = 102`, `ICCID_TYPE = 'TC'`, `TC_ICCID = NULL`, `TC_IMSI_MCC = NULL`, `TC_STATUS = NULL`, `TC_VALID_TO = NULL`, `TC_E_ID = NULL`, `TC_CARD_TYPE_NAME = NULL`
2.  Ensure both target tables are empty.

**Action:**
1.  Execute the legacy job.
2.  Execute the migrated Airflow DAG.
3.  Query the `SOF$TA_ICCID_VERTRAG` (Oracle) and `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG` (BigQuery) tables for `CNTRCT_ID = 102`.

**Pass/Fail Criterion:**
For `CNTRCT_ID = 102`, both target tables must contain exactly one row. This row must have:
*   `TN_ICCID = 'TN_A'` (non-NULL value preferred over NULL)
*   `TN_IMSI_MCC = '222'` (alphabetically highest non-NULL value, or latest for dates, or just one non-NULL if multiple exist and are not equal) - *Note: Oracle's `MAX()` on strings/dates will pick the "highest" value. BigQuery's `MAX()` behaves similarly.*
*   `TN_STATUS = 'ACTIVE'`
*   `TN_VALID_TO = '2025-01-01'`
*   `TN_E_ID = 'E1'`
*   `TN_CARD_TYPE_NAME = 'TypeB'`
*   All `TC_ICCID` and its associated fields must be `NULL` (as all source values were NULL).
The values for all columns must match between Oracle and BigQuery.

**Runnable Test Code (SQL Assertion):**

```sql
-- Expected output for CNTRCT_ID = 102
SELECT
    CNTRCT_ID,
    TN_ICCID, TN_IMSI_MCC, TN_STATUS, TN_VALID_TO, TN_E_ID, TN_CARD_TYPE_NAME,
    TC_ICCID, TC_IMSI_MCC, TC_STATUS, TC_VALID_TO, TC_E_ID, TC_CARD_TYPE_NAME
FROM `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG`
WHERE CNTRCT_ID = 102;

-- Expected result (assuming MAX picks 'highest' non-NULL):
-- CNTRCT_ID | TN_ICCID | TN_IMSI_MCC | TN_STATUS | TN_VALID_TO | TN_E_ID | TN_CARD_TYPE_NAME | TC_ICCID | ...
-- ----------|----------|-------------|-----------|-------------|---------|-------------------|----------|-----
-- 102       | TN_A     | 222         | ACTIVE    | 2025-01-01  | E1      | TypeB             | NULL     | ...

-- Assertions in a testing framework:
# ... (similar to previous test, assert specific values and NULLs)
# assert row["TN_ICCID"] == "TN_A"
# assert row["TN_IMSI_MCC"] == "222"
# assert row["TN_STATUS"] == "ACTIVE"
# assert row["TN_VALID_TO"] == datetime.date(2025, 1, 1) # Or appropriate date type
# assert row["TN_E_ID"] == "E1"
# assert row["TN_CARD_TYPE_NAME"] == "TypeB"
# assert row["TC_ICCID"] is None
```

---

### 4. Transformation Correctness - Edge Case: Contract with No ICCID Records

**Purpose:** To ensure that contracts that do not have any corresponding records in the `SOF_TA_ICCID_EINZELN` source table are correctly excluded from the target `SOF_TA_ICCID_VERTRAG` table.

**Setup:**
1.  Prepare a test dataset for `SOF$TA_ICCID_EINZELN` (Oracle) and `PROJECT_ID.DATASET.SOF_TA_ICCID_EINZELN` (BigQuery) that *does not* include any records for `CNTRCT_ID = 103`.
2.  Ensure both target tables are empty.

**Action:**
1.  Execute the legacy job.
2.  Execute the migrated Airflow DAG.
3.  Query the `SOF$TA_ICCID_VERTRAG` (Oracle) and `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG` (BigQuery) tables for `CNTRCT_ID = 103`.

**Pass/Fail Criterion:**
Both queries must return 0 rows for `CNTRCT_ID = 103`.

**Runnable Test Code (SQL Assertion):**

```sql
SELECT COUNT(*) FROM `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG` WHERE CNTRCT_ID = 103;
-- Expected result: 0

-- Assertions in a testing framework:
# ...
# query = "SELECT COUNT(*) FROM `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG` WHERE CNTRCT_ID = 103"
# count = client.query(query).result().total_rows
# assert count == 0
```

---

### 5. Transformation Correctness - Edge Case: Empty Source Table

**Purpose:** To verify that the job handles an empty source table gracefully, resulting in an empty target table.

**Setup:**
1.  Ensure both `SOF$TA_ICCID_EINZELN` (Oracle) and `PROJECT_ID.DATASET.SOF_TA_ICCID_EINZELN` (BigQuery) are completely empty.
2.  Ensure both target tables are empty.

**Action:**
1.  Execute the legacy job.
2.  Execute the migrated Airflow DAG.
3.  Query the `SOF$TA_ICCID_VERTRAG` (Oracle) and `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG` (BigQuery) tables for their total row counts.

**Pass/Fail Criterion:**
Both queries must return 0 rows.

**Runnable Test Code (SQL Assertion):**

```sql
SELECT COUNT(*) FROM `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG`;
-- Expected result: 0

-- Assertions in a testing framework:
# ...
# query = "SELECT COUNT(*) FROM `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG`"
# count = client.query(query).result().total_rows
# assert count == 0
```

---

### 6. External System Replacements - Airflow Orchestration and Parameter Handling

**Purpose:** To verify that the Airflow DAG correctly orchestrates the job, including parameter parsing, date validation, and passing these to the BigQuery transformation.

**Setup:**
1.  Ensure the `dag_dw_bert_ausd_bp_ta_iccid_vertrag` DAG is deployed in Cloud Composer.
2.  Prepare a test dataset in `PROJECT_ID.DATASET.SOF_TA_ICCID_EINZELN`.
3.  Ensure `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG` is empty.

**Action:**
1.  Manually trigger the Airflow DAG, providing a specific `stichtag` (e.g., `20231026`) and `wiederanlaufwert` (e.g., `1`) via the DAG run configuration.
2.  Monitor the Airflow UI for task execution status.
3.  Check Airflow task logs for `r_ausd_bp_ta_iccid_vertrag_task` and `execute_bq_sql_task`.
4.  Query the target table `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG` to confirm data was loaded.

**Pass/Fail Criterion:**
*   The Airflow DAG must complete successfully without errors.
*   The `r_ausd_bp_ta_iccid_vertrag_task` logs must show that the provided `stichtag` and `wiederanlaufwert` were correctly parsed and validated.
*   The `execute_bq_sql_task` must show successful execution of the BigQuery SQL.
*   The `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG` table must contain data, indicating the transformation ran.
*   (Optional, if `stichtag` was used in the SQL) Verify that the `stichtag` parameter was correctly used in the BigQuery SQL (e.g., if it filtered data). *Note: Current SQL does not use these parameters, so this is a future-proofing check.*

**Runnable Test Code (Conceptual Pytest for `bert_utilities.py`):**

```python
# utils/test_bert_utilities.py
import pytest
import datetime
from utils.bert_utilities import parse_stichtag_and_wiederanlaufwert, validate_date, get_date_range

def test_parse_stichtag_and_wiederanlaufwert_valid_args():
    args = ["-s", "20231026", "-l", "1"]
    stichtag, wiederanlaufwert = parse_stichtag_and_wiederanlaufwert(args)
    assert stichtag == "20231026"
    assert wiederanlaufwert == "1"

def test_parse_stichtag_and_wiederanlaufwert_default_stichtag():
    args = ["-l", "0"]
    stichtag, wiederanlaufwert = parse_stichtag_and_wiederanlaufwert(args)
    # Default stichtag should be yesterday's date in YYYYMMDD format
    expected_stichtag = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y%m%d")
    assert stichtag == expected_stichtag
    assert wiederanlaufwert == "0"

def test_parse_stichtag_and_wiederanlaufwert_missing_stichtag_arg():
    with pytest.raises(ValueError, match="Missing argument for -s"):
        parse_stichtag_and_wiederanlaufwert(["-s"])

def test_validate_date_valid():
    assert validate_date("20231026") is True
    assert validate_date("19990101") is True

def test_validate_date_invalid():
    assert validate_date("20231301") is False  # Invalid month
    assert validate_date("InvalidDate") is False
    assert validate_date("2023-10-26") is False # Wrong format

def test_get_date_range_valid():
    date_info = get_date_range("20231026")
    assert date_info["stichtag"] == "20231026"
    assert date_info["stichtag_minus_1_day"] == "20231025"
    assert date_info["stichtag_month_start"] == "20231001"
    assert date_info["stichtag_year"] == "2023"

def test_get_date_range_invalid_stichtag():
    with pytest.raises(ValueError):
        get_date_range("InvalidDate")
```

---

### 7. Data Quality - Schema and Data Type Integrity

**Purpose:** To ensure that the target BigQuery table schema (`PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG`) accurately reflects the legacy Oracle table schema (`SOF$TA_ICCID_VERTRAG`), and that data types are correctly mapped and preserved during the migration and transformation.

**Setup:**
1.  Ensure a representative dataset has been processed by both legacy and migrated jobs (e.g., using the setup from Test Case 1).
2.  Have access to the schema definition of the Oracle `SOF$TA_ICCID_VERTRAG` table.

**Action:**
1.  Retrieve the schema of the Oracle `SOF$TA_ICCID_VERTRAG` table (column names, data types, nullability).
2.  Retrieve the schema of the BigQuery `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG` table.
3.  Compare the two schemas.
4.  For a sample of rows, query specific columns from both tables and verify their data types and values.

**Pass/Fail Criterion:**
*   All column names in BigQuery must match those in Oracle.
*   BigQuery data types must be appropriate equivalents for Oracle data types (e.g., `VARCHAR2` -> `STRING`, `NUMBER` -> `INT64`/`NUMERIC`, `DATE` -> `DATE`/`TIMESTAMP`).
*   Nullability constraints should be consistent (e.g., if a column is `NOT NULL` in Oracle, it should be `REQUIRED` in BigQuery, or `NULLABLE` if it was `NULLABLE`).
*   No data truncation or unexpected type conversions should occur (e.g., a string column in Oracle should not become an integer in BigQuery unless explicitly designed).

**Runnable Test Code (Conceptual Python for schema comparison):**

```python
# pytest_schema_comparison.py
import pytest
from google.cloud import bigquery
# Assume you have a way to get Oracle schema, e.g., via cx_Oracle or a metadata export
# For this example, we'll define a mock Oracle schema.

# Mock Oracle schema (replace with actual Oracle schema extraction)
ORACLE_SCHEMA = {
    "CNTRCT_ID": {"type": "NUMBER", "nullable": False, "precision": 10, "scale": 0},
    "TN_ICCID": {"type": "VARCHAR2", "nullable": True, "length": 20},
    "TN_IMSI_MCC": {"type": "VARCHAR2", "nullable": True, "length": 3},
    "TN_VALID_TO": {"type": "DATE", "nullable": True},
    # ... define all 118 columns
}

# Mapping Oracle types to expected BigQuery types
TYPE_MAP = {
    "NUMBER": {"precision": 10, "scale": 0}: "INT64",
    "NUMBER": {"precision": 18, "scale": 2}: "NUMERIC", # Example for decimal
    "VARCHAR2": "STRING",
    "DATE": "DATE", # Or TIMESTAMP if time component is relevant
    # ...
}

def get_bigquery_schema(project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)
    bq_schema = {}
    for field in table.schema:
        bq_schema[field.name] = {
            "type": field.field_type,
            "nullable": field.mode == "NULLABLE"
        }
    return bq_schema

def test_schema_parity():
    project_id = "PROJECT_ID"
    dataset_id = "DATASET"
    table_id = "SOF_TA_ICCID_VERTRAG"

    bq_schema = get_bigquery_schema(project_id, dataset_id, table_id)

    # Check column count
    assert len(bq_schema) == len(ORACLE_SCHEMA), "Column count mismatch"

    for col_name, oracle_col_def in ORACLE_SCHEMA.items():
        assert col_name in bq_schema, f"Column {col_name} missing in BigQuery"
        bq_col_def = bq_schema[col_name]

        # Check data type mapping
        oracle_type_key = oracle_col_def["type"]
        if oracle_type_key == "NUMBER":
            oracle_type_key = (oracle_type_key, {"precision": oracle_col_def.get("precision"), "scale": oracle_col_def.get("scale")})
        
        expected_bq_type = TYPE_MAP.get(oracle_type_key, "UNKNOWN")
        assert bq_col_def["type"] == expected_bq_type, \
            f"Type mismatch for {col_name}: Expected {expected_bq_type}, got {bq_col_def['type']}"

        # Check nullability
        expected_nullable = oracle_col_def["nullable"]
        assert bq_col_def["nullable"] == expected_nullable, \
            f"Nullability mismatch for {col_name}: Expected {expected_nullable}, got {bq_col_def['nullable']}"

    print("Schema parity test passed!")

# To run this test:
# 1. Ensure `google-cloud-bigquery` is installed (`pip install google-cloud-bigquery`)
# 2. Authenticate your Python environment to GCP.
# 3. Replace `PROJECT_ID` and `DATASET` with actual values.
# 4. Populate `ORACLE_SCHEMA` with the actual schema of your Oracle table.
# 5. Run `pytest pytest_schema_comparison.py`
```

---

### 8. Data Quality - Row Count Parity

**Purpose:** To confirm that the total number of records processed and generated by the migrated job matches the legacy job. This is a quick sanity check for data completeness.

**Setup:**
1.  Use the same comprehensive test dataset as in Test Case 1, loaded into both source tables.
2.  Ensure both target tables are empty.

**Action:**
1.  Execute the legacy job.
2.  Execute the migrated Airflow DAG.
3.  Query the total row count from Oracle `SOF$TA_ICCID_VERTRAG`.
4.  Query the total row count from BigQuery `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG`.

**Pass/Fail Criterion:**
The row counts from both target tables must be identical.

**Runnable Test Code (SQL Assertion):**

```sql
-- Legacy Oracle
SELECT COUNT(*) FROM SOF$TA_ICCID_VERTRAG;

-- Migrated BigQuery
SELECT COUNT(*) FROM `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG`;

-- Assertions in a testing framework:
# ...
# oracle_count = get_oracle_row_count("SOF$TA_ICCID_VERTRAG")
# bq_count = client.query("SELECT COUNT(*) FROM `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG`").result().total_rows
# assert oracle_count == bq_count, f"Row count mismatch: Oracle={oracle_count}, BigQuery={bq_count}"
```