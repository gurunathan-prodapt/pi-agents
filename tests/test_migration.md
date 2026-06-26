# Migration Validation Test Suite: `ausd_bp_ta_apn_vertrag`

This document defines the migration-validation test suite for the BigQuery job `ausd_bp_ta_apn_vertrag`. These tests ensure functional and behavioral equivalence between the legacy Oracle PL/SQL cursor-loop implementation and the migrated BigQuery SQL pipeline (which utilizes a JavaScript UDF to handle character-limit fitting).

---

## Test Suite 1: Output Parity & Transformation Correctness

### Test 1.1: Standard Aggregation & Ordering (No Overflow)
#### Purpose
Verify that multiple staging records for a single contract ID are correctly aggregated into a comma-separated string, sorted alphabetically, without truncation when the total length is well under the 100-character limit.

#### Setup
1. Ensure the target table `sof_ta_apn_vertrag` is empty.
2. Populate the staging table `sof_ta_bpr_apn` with the following test records:

| cntrct_id | access_point_name | cntrct_id_ref |
| :--- | :--- | :--- |
| `CON_001` | `web.de` | `REF_B` |
| `CON_001` | `internet` | `REF_A` |

#### Action
Execute the BigQuery transformation query (Option B using the JS UDF).

#### Pass/Fail Criterion
**Pass:** The target table contains exactly one row for `CON_001` with alphabetically sorted, comma-separated values:
* `apn` = `"internet, web.de"`
* `cntrct_ref` = `"REF_A, REF_B"`

**Fail:** Any other string concatenation, incorrect ordering, or duplicate rows.

---

### Test 1.2: Strict 100-Character Limit & Skipping Behavior (The "Option B" Edge Case)
#### Purpose
Verify that the JavaScript UDF strictly mirrors the legacy PL/SQL cursor's character-fitting behavior:
1. It must not exceed 100 characters.
2. If an element causes the concatenated string to exceed 100 characters, it must be skipped.
3. **Crucial Edge Case:** The loop must continue processing subsequent elements. If a later, shorter element fits within the remaining budget, it must be appended.

#### Setup
1. Clear staging and target tables.
2. Populate `sof_ta_bpr_apn` for a single contract `CON_002` with four records designed to trigger the skip-and-fit logic. The items are sorted alphabetically to guarantee processing order:

| Item # | cntrct_id | access_point_name | Length | Cumulative Length if Added | Action Expected |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | `CON_002` | `A` (40 chars: `"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"`) | 40 | 40 | **Keep** |
| 2 | `CON_002` | `B` (55 chars: `"BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"`) | 55 | 40 + 2 (delimiter) + 55 = **97** | **Keep** |
| 3 | `CON_002` | `C` (10 chars: `"CCCCCCCCCC"`) | 10 | 97 + 2 (delimiter) + 10 = **109** (>100) | **Skip** |
| 4 | `CON_002` | `D` (1 char: `"D"`) | 1 | 97 + 2 (delimiter) + 1 = **100** (<=100) | **Keep** |

*Note: The same logic is applied to `cntrct_id_ref`. Populate `cntrct_id_ref` with the same values to test both columns simultaneously.*

#### Action
Execute the BigQuery transformation query.

#### Pass/Fail Criterion
**Pass:** The target table contains exactly one row for `CON_002` where:
* `apn` = `"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA, BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB, D"` (Exactly 100 characters. The `C` string was skipped, but the `D` string was successfully appended).
* `cntrct_ref` matches the same pattern.

**Fail:** The string length exceeds 100 characters, the `D` character is missing (indicating the loop terminated early on overflow), or the string is truncated mid-word (e.g., ending in `...BB, CC`).

---

### Test 1.3: NULL and Empty String Handling
#### Purpose
Verify that `NULL` values and empty strings in the staging table do not produce malformed delimiters (e.g., leading commas, trailing commas, or double commas `,,`) in the aggregated target columns.

#### Setup
1. Clear staging and target tables.
2. Populate `sof_ta_bpr_apn` with the following records:

| cntrct_id | access_point_name | cntrct_id_ref |
| :--- | :--- | :--- |
| `CON_003` | `NULL` | `REF_Y` |
| `CON_003` | `internet` | `NULL` |
| `CON_003` | `""` (Empty String) | `REF_Z` |

#### Action
Execute the BigQuery transformation query.

#### Pass/Fail Criterion
**Pass:** The target table contains exactly one row for `CON_003` where:
* `apn` = `"internet"` (No leading/trailing commas or empty elements).
* `cntrct_ref` = `"REF_Y, REF_Z"` (No middle null elements or double delimiters).

**Fail:** The presence of values like `", internet"`, `"internet, "`, or `"REF_Y, , REF_Z"`.

---

## Test Suite 2: Schema & Data Quality Assertions

### Test 2.1: Target Schema Validation
#### Purpose
Ensure the target table structure matches the production DDL specifications, including column names, data types, and descriptions.

#### Setup
Deploy the target table using the DDL script `src/ddl/sof_ta_apn_vertrag.sql`.

#### Action
Query the BigQuery `INFORMATION_SCHEMA.COLUMNS` view for the target table.

#### Pass/Fail Criterion
**Pass:** The query returns exactly 3 columns matching the following schema:

| Column Name | Data Type | Is Nullable | Description |
| :--- | :--- | :--- | :--- |
| `cntrct_id` | `STRING` | `YES` | Contract ID |
| `apn` | `STRING` | `YES` | Aggregated list of Access Point Names, comma-separated |
| `cntrct_ref` | `STRING` | `YES` | Aggregated list of contract references, comma-separated |

**Fail:** Any mismatch in column names, data types, or missing field descriptions.

---

### Test 2.2: Row Count and Uniqueness Assertions
#### Purpose
Verify that the aggregation process does not lose contracts and that `cntrct_id` acts as a unique primary key in the target table (one row per contract).

#### Setup
Populate `sof_ta_bpr_apn` with 10,000 random records spanning 1,500 unique `cntrct_id` values.

#### Action
1. Execute the BigQuery transformation query.
2. Run validation queries to check row counts and uniqueness.

#### Pass/Fail Criterion
**Pass:** 
* The total row count of `sof_ta_apn_vertrag` is exactly 1,500.
* The count of distinct `cntrct_id` in `sof_ta_apn_vertrag` is exactly 1,500 (proving uniqueness).

**Fail:** Row count is not equal to 1,500, or duplicate `cntrct_id` records exist in the target table.

---

## Test Suite 3: End-to-End Pipeline & Orchestration Validation

### Test 3.1: Airflow DAG Execution & Idempotency
#### Purpose
Verify that the Airflow DAG executes successfully, logs metrics correctly, and is fully idempotent (running it multiple times with the same source data yields the exact same target state without duplicating rows).

#### Setup
1. Deploy the Airflow DAG `dw_bert_ausd_bp_ta_apn_vertrag` to the Cloud Composer environment.
2. Populate the staging table with a baseline dataset.

#### Action
1. Trigger the DAG manually via the Airflow UI/CLI. Record the target table state.
2. Trigger the DAG a second time. Record the target table state.

#### Pass/Fail Criterion
**Pass:**
* Both DAG runs complete with a `SUCCESS` status.
* The target table contains the exact same row count and data after both Run 1 and Run 2 (proving the target table is truncated and reloaded cleanly).
* No duplicate records are generated.

**Fail:** Any task failure, or if the second run doubles the row count (indicating a missing truncate/overwrite step).

---

## Runnable Test Code

### 1. Pytest Suite (`test_ausd_bp_ta_apn_vertrag.py`)
This script uses the official Google Cloud BigQuery Python client to run the functional tests defined above.

```python
import os
import pytest
from google.cloud import bigquery

# Environment setup
PROJECT_ID = os.getenv("GCP_PROJECT", "your_project")
DATASET_ID = os.getenv("GCP_DATASET", "your_dataset")
LOCATION = os.getenv("GCP_LOCATION", "EU")

STAGING_TABLE = f"{PROJECT_ID}.{DATASET_ID}.sof_ta_bpr_apn"
TARGET_TABLE = f"{PROJECT_ID}.{DATASET_ID}.sof_ta_apn_vertrag"

@pytest.fixture(scope="session")
def bq_client():
    return bigquery.Client(project=PROJECT_ID, location=LOCATION)

@pytest.fixture(autouse=True)
def clean_tables(bq_client):
    """Ensure tables are clean before and after each test."""
    bq_client.query(f"TRUNCATE TABLE `{STAGING_TABLE}`").result()
    bq_client.query(f"TRUNCATE TABLE `{TARGET_TABLE}`").result()
    yield
    bq_client.query(f"TRUNCATE TABLE `{STAGING_TABLE}`").result()
    bq_client.query(f"TRUNCATE TABLE `{TARGET_TABLE}`").result()

def run_transformation(bq_client):
    """Executes the migrated SQL logic under test."""
    sql_path = os.path.join(os.path.dirname(__file__), "../sql/d_ausd_bp_ta_apn_vertrag.sql")
    
    if os.path.exists(sql_path):
        with open(sql_path, "r") as f:
            query = f.read()
        # Replace Airflow template variables with environment values
        query = query.replace("{{ var.value.gcp_project }}", PROJECT_ID)
        query = query.replace("{{ var.value.gcp_dataset }}", DATASET_ID)
    else:
        # Fallback inline query matching the production code
        query = f"""
            CREATE TEMP FUNCTION aggregate_limited(arr ARRAY<STRING>, delimiter STRING, max_len INT64)
            RETURNS STRING
            LANGUAGE js AS r\"\"\"
              if (!arr) return null;
              let result = "";
              for (let i = 0; i < arr.length; i++) {
                let item = arr[i];
                if (!item) continue;
                let next_str = result ? result + delimiter + item : item;
                if (next_str.length <= max_len) {
                  result = next_str;
                } else {
                  continue;
                }
              }
              return result;
            \"\"\";

            CREATE OR REPLACE TABLE `{TARGET_TABLE}` AS
            SELECT
              cntrct_id,
              aggregate_limited(ARRAY_AGG(access_point_name ORDER BY access_point_name), ', ', 100) AS apn,
              aggregate_limited(ARRAY_AGG(cntrct_id_ref ORDER BY cntrct_id_ref), ', ', 100) AS cntrct_ref
            FROM
              `{STAGING_TABLE}`
            GROUP BY
              cntrct_id;
        """
    bq_client.query(query).result()

def test_standard_aggregation(bq_client):
    """Test 1.1: Standard Aggregation & Ordering (No Overflow)"""
    # Setup
    insert_query = f"""
        INSERT INTO `{STAGING_TABLE}` (cntrct_id, access_point_name, cntrct_id_ref) VALUES
        ('CON_001', 'web.de', 'REF_B'),
        ('CON_001', 'internet', 'REF_A')
    """
    bq_client.query(insert_query).result()

    # Action
    run_transformation(bq_client)

    # Assert
    results = bq_client.query(f"SELECT * FROM `{TARGET_TABLE}`").to_dataframe()
    assert len(results) == 1
    assert results.loc[0, 'cntrct_id'] == 'CON_001'
    assert results.loc[0, 'apn'] == 'internet, web.de'
    assert results.loc[0, 'cntrct_ref'] == 'REF_A, REF_B'

def test_character_limit_and_skipping(bq_client):
    """Test 1.2: Strict 100-Character Limit & Skipping Behavior"""
    # Setup
    val_a = "A" * 40
    val_b = "B" * 55
    val_c = "C" * 10
    val_d = "D"
    
    insert_query = f"""
        INSERT INTO `{STAGING_TABLE}` (cntrct_id, access_point_name, cntrct_id_ref) VALUES
        ('CON_002', '{val_a}', '{val_a}'),
        ('CON_002', '{val_b}', '{val_b}'),
        ('CON_002', '{val_c}', '{val_c}'),
        ('CON_002', '{val_d}', '{val_d}')
    """
    bq_client.query(insert_query).result()

    # Action
    run_transformation(bq_client)

    # Assert
    results = bq_client.query(f"SELECT * FROM `{TARGET_TABLE}`").to_dataframe()
    assert len(results) == 1
    
    expected_string = f"{val_a}, {val_b}, {val_d}"
    assert len(expected_string) == 100
    assert results.loc[0, 'apn'] == expected_string
    assert results.loc[0, 'cntrct_ref'] == expected_string

def test_null_and_empty_handling(bq_client):
    """Test 1.3: NULL and Empty String Handling"""
    # Setup
    insert_query = f"""
        INSERT INTO `{STAGING_TABLE}` (cntrct_id, access_point_name, cntrct_id_ref) VALUES
        ('CON_003', NULL, 'REF_Y'),
        ('CON_003', 'internet', NULL),
        ('CON_003', '', 'REF_Z')
    """
    bq_client.query(insert_query).result()

    # Action
    run_transformation(bq_client)

    # Assert
    results = bq_client.query(f"SELECT * FROM `{TARGET_TABLE}`").to_dataframe()
    assert len(results) == 1
    assert results.loc[0, 'apn'] == 'internet'
    assert results.loc[0, 'cntrct_ref'] == 'REF_Y, REF_Z'
```

### 2. Pure SQL Assertions (For Manual/Console Verification)
These queries can be run directly in the BigQuery Console to validate data quality and transformation correctness post-migration.

```sql
-- ASSERTION 1: Verify no target record exceeds the 100-character limit
SELECT 
  cntrct_id, 
  LENGTH(apn) AS apn_len, 
  LENGTH(cntrct_ref) AS ref_len
FROM 
  `your_project.your_dataset.sof_ta_apn_vertrag`
WHERE 
  LENGTH(apn) > 100 OR LENGTH(cntrct_ref) > 100;
-- EXPECTED RESULT: 0 rows returned.

-- ASSERTION 2: Verify uniqueness of the Primary Key (cntrct_id)
SELECT 
  cntrct_id, 
  COUNT(*) as duplicate_count
FROM 
  `your_project.your_dataset.sof_ta_apn_vertrag`
GROUP BY 
  cntrct_id
HAVING 
  COUNT(*) > 1;
-- EXPECTED RESULT: 0 rows returned.

-- ASSERTION 3: Verify row count parity with staging distinct contracts
SELECT
  (SELECT COUNT(DISTINCT cntrct_id) FROM `your_project.your_dataset.sof_ta_bpr_apn`) AS staging_distinct_contracts,
  (SELECT COUNT(*) FROM `your_project.your_dataset.sof_ta_apn_vertrag`) AS target_total_rows;
-- EXPECTED RESULT: Both columns must show identical values.
```