# Migration Validation Test Suite: `ausd_bp_ta_apn_vertrag`

This document defines the migration-validation test suite to prove behavioral equivalence between the legacy Oracle PL/SQL implementation and the migrated Google Cloud BigQuery / Apache Airflow implementation for the job `ausd_bp_ta_apn_vertrag`.

---

## Test Case 1: Output Parity (Golden Dataset Comparison)

### Purpose
To verify that standard, non-edge-case inputs produce identical outputs in both the legacy Oracle environment and the target BigQuery environment.

### Setup
1. **Legacy Oracle Environment**:
   * Populate `sof$ta_bpr_apn` with a standard set of test records (e.g., 5 contracts, each with 1 to 3 APNs and reference IDs of normal lengths).
   * Run the legacy PL/SQL job.
   * Export the resulting `sof$ta_apn_vertrag` table to a CSV file (`oracle_golden_output.csv`).

2. **BigQuery Environment**:
   * Populate the target source table `sof_ta_bpr_apn` with the exact same input dataset.
   * Ensure the target table `sof_ta_apn_vertrag` is empty before execution.

### Action
Execute the BigQuery SQL script `d_ausd_bp_ta_apn_vertrag.sql` using the BigQuery client or by triggering the Airflow DAG `dw_bert_ausd_bp_ta_apn_vertrag`.

### Pass/Fail Criterion
The test passes if a row-by-row, column-by-column comparison between the BigQuery target table `sof_ta_apn_vertrag` and the Oracle golden export shows 100% parity.

```python
# pytest code for Golden Dataset Comparison
import pandas as pd
from google.cloud import bigquery

def test_golden_dataset_parity():
    # Load Oracle Golden Dataset
    oracle_df = pd.read_csv("tests/golden_data/oracle_golden_output.csv")
    oracle_df = oracle_df.sort_values(by=["cntrct_id"]).reset_index(drop=True)

    # Fetch BigQuery Migrated Dataset
    client = bigquery.Client()
    query = """
        SELECT cntrct_id, access_point_names, cntrct_id_refs 
        FROM `gcp-dwh-prod.isbert_schema.sof_ta_apn_vertrag`
        ORDER BY cntrct_id
    """
    bq_df = client.query(query).to_dataframe()

    # Assert shape and content equivalence
    assert bq_df.shape == oracle_df.shape, f"Shape mismatch: BQ {bq_df.shape} vs Oracle {oracle_df.shape}"
    pd.testing.assert_frame_equal(bq_df, oracle_df, check_dtype=False)
```

---

## Test Case 2: Transformation Correctness — Concatenation Order & Grouping

### Purpose
To verify that the BigQuery SQL correctly groups records by `cntrct_id` and concatenates `access_point_name` and `cntrct_id_ref` in the exact order specified by the partition window: `ORDER BY cntrct_id_ref, access_point_name, bpr_id`.

### Setup
Populate `sof_ta_bpr_apn` with the following test data designed to verify ordering:

| cntrct_id | cntrct_id_ref | bpr_id | access_point_name | Comment |
| :--- | :--- | :--- | :--- | :--- |
| C1 | REF_B | 100 | APN_B | Should be 2nd |
| C1 | REF_A | 101 | APN_A | Should be 1st (REF_A < REF_B) |
| C1 | REF_B | 99  | APN_A | Should be 3rd (REF_B = REF_B, but APN_A < APN_B) |
| C1 | REF_B | 102 | APN_B | Should be 4th (REF_B = REF_B, APN_B = APN_B, but bpr_id 102 > 100) |

### Action
Run the BigQuery transformation query.

### Pass/Fail Criterion
The target table must contain exactly one row for `C1` with the values concatenated in the correct order:
* `access_point_names` = `"APN_A, APN_B, APN_A, APN_B"`
* `cntrct_id_refs` = `"REF_A, REF_B, REF_B, REF_B"`

```sql
-- SQL Assertion Test
SELECT 
  cntrct_id,
  access_point_names,
  cntrct_id_refs
FROM 
  `gcp-dwh-prod.isbert_schema.sof_ta_apn_vertrag`
WHERE 
  cntrct_id = 'C1';

-- EXPECTED OUTPUT:
-- cntrct_id | access_point_names               | cntrct_id_refs
-- C1         | APN_A, APN_B, APN_A, APN_B       | REF_A, REF_B, REF_B, REF_B
```

---

## Test Case 3: Transformation Correctness — Truncation & Edge Cases

### Purpose
To verify that strings exceeding 100 characters are truncated correctly to exactly 100 characters, and to document/assert the behavioral difference between the legacy PL/SQL "skip-on-overflow" logic and the BigQuery standard `SUBSTR` logic.

### Setup
Populate `sof_ta_bpr_apn` with a contract containing long values that will exceed 100 characters when concatenated:

| cntrct_id | cntrct_id_ref | bpr_id | access_point_name | Length |
| :--- | :--- | :--- | :--- | :--- |
| C2 | REF_VERY_LONG_1234567890123456789012345678901234567890 | 1 | APN_VERY_LONG_1234567890123456789012345678901234567890 | 50 chars each |
| C2 | REF_SHORT | 2 | APN_SHORT | 9 chars each |

* Concatenated `access_point_names` before truncation: `"APN_VERY_LONG_1234567890123456789012345678901234567890, APN_SHORT"` (Length: 50 + 2 + 9 = 61 chars)
* Concatenated `cntrct_id_refs` before truncation: `"REF_VERY_LONG_1234567890123456789012345678901234567890, REF_SHORT"` (Length: 50 + 2 + 9 = 61 chars)

Now add a third record to push it over 100 characters:
* `APN_EXTRA_LONG_...` (50 chars)

Total length will be 113 characters.

### Action
Run the BigQuery transformation query.

### Pass/Fail Criterion
1. The length of both `access_point_names` and `cntrct_id_refs` in the target table must be exactly 100 characters.
2. The string must be truncated cleanly at character 100, matching the BigQuery `SUBSTR(..., 1, 100)` logic.

```python
# pytest code for Truncation Verification
def test_truncation_behavior():
    client = bigquery.Client()
    query = """
        SELECT LENGTH(access_point_names) as apn_len, LENGTH(cntrct_id_refs) as ref_len
        FROM `gcp-dwh-prod.isbert_schema.sof_ta_apn_vertrag`
        WHERE cntrct_id = 'C2'
    """
    result = client.query(query).to_dataframe()
    
    assert result.loc[0, 'apn_len'] == 100, f"Expected length 100, got {result.loc[0, 'apn_len']}"
    assert result.loc[0, 'ref_len'] == 100, f"Expected length 100, got {result.loc[0, 'ref_len']}"
```

---

## Test Case 4: Transformation Correctness — NULL and Empty String Handling

### Purpose
To verify that NULL values and empty strings in the source columns `access_point_name` and `cntrct_id_ref` do not cause runtime failures and are handled gracefully (ignored by `STRING_AGG` as per standard SQL behavior).

### Setup
Populate `sof_ta_bpr_apn` with the following records:

| cntrct_id | cntrct_id_ref | bpr_id | access_point_name | Comment |
| :--- | :--- | :--- | :--- | :--- |
| C3 | REF_A | 1 | NULL | NULL APN |
| C3 | NULL | 2 | APN_B | NULL Ref |
| C3 | REF_C | 3 | APN_C | Valid record |

### Action
Run the BigQuery transformation query.

### Pass/Fail Criterion
* `STRING_AGG` must ignore the NULL values.
* The resulting `access_point_names` for `C3` must be `"APN_B, APN_C"`.
* The resulting `cntrct_id_refs` for `C3` must be `"REF_A, REF_C"`.

```sql
-- SQL Assertion Test for NULL Handling
SELECT 
  cntrct_id,
  access_point_names,
  cntrct_id_refs
FROM 
  `gcp-dwh-prod.isbert_schema.sof_ta_apn_vertrag`
WHERE 
  cntrct_id = 'C3';

-- EXPECTED OUTPUT:
-- cntrct_id | access_point_names | cntrct_id_refs
-- C3         | APN_B, APN_C       | REF_A, REF_C
```

---

## Test Case 5: Airflow DAG & Environment Validation

### Purpose
To verify that the Airflow DAG is correctly configured, resolves environment variables, and can execute the BigQuery operator successfully.

### Setup
1. Ensure Airflow Variables are set in the environment:
   * `gcp_project_id` = `gcp-dwh-prod`
   * `bq_dataset` = `isbert_schema`
   * `bq_location` = `EU`
   * `gcp_conn_id` = `google_cloud_default`
2. Place the DAG file `dw_bert_ausd_bp_ta_apn_vertrag.py` in the Airflow DAGs folder.
3. Place the SQL file `d_ausd_bp_ta_apn_vertrag.sql` in the configured template path.

### Action
Trigger the Airflow DAG `dw_bert_ausd_bp_ta_apn_vertrag` manually via the Airflow CLI or UI.

### Pass/Fail Criterion
* The DAG must parse successfully without import errors.
* The task `process_apn_vertrag` must complete with a `SUCCESS` status.
* The SQL template parameters (`{{ var.value.gcp_project_id }}`) must resolve correctly in the rendered query.

```python
# pytest code for Airflow DAG Validation
from airflow.models import DagBag

def test_dag_import_and_structure():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag_id = "dw_bert_ausd_bp_ta_apn_vertrag"
    
    # Assert no import errors
    assert len(dagbag.import_errors) == 0, f"DAG import errors: {dagbag.import_errors}"
    
    # Assert DAG exists
    dag = dagbag.get_dag(dag_id)
    assert dag is not None, f"DAG {dag_id} not found"
    
    # Assert task structure
    task = dag.get_task("process_apn_vertrag")
    assert task is not None
    assert task.write_disposition == "WRITE_TRUNCATE"
```

---

## Test Case 6: Data Quality, Schema, and Row-Count Assertions

### Purpose
To verify that the target table schema matches the design specifications, and that the row counts conform to expected business rules (e.g., target row count must be less than or equal to source row count, and `cntrct_id` must be unique).

### Setup
Run the full migration pipeline on a representative test dataset.

### Action
Execute schema and data-quality validation queries on the target table `sof_ta_apn_vertrag`.

### Pass/Fail Criterion
1. **Schema Check**: Columns must be exactly `cntrct_id` (STRING), `access_point_names` (STRING), and `cntrct_id_refs` (STRING).
2. **Uniqueness Check**: `cntrct_id` must be the primary key (unique) in the target table.
3. **Nullability Check**: `cntrct_id` must not contain NULL values.

```python
# pytest code for Data Quality and Schema Assertions
def test_target_schema_and_dq():
    client = bigquery.Client()
    
    # 1. Schema Check
    table_ref = client.dataset("isbert_schema").table("sof_ta_apn_vertrag")
    table = client.get_table(table_ref)
    
    schema_dict = {field.name: field.field_type for field in table.schema}
    expected_schema = {
        "cntrct_id": "STRING",
        "access_point_names": "STRING",
        "cntrct_id_refs": "STRING"
    }
    assert schema_dict == expected_schema, f"Schema mismatch. Got: {schema_dict}"

    # 2. Uniqueness Check
    dq_query = """
        SELECT 
          COUNT(cntrct_id) as total_rows, 
          COUNT(DISTINCT cntrct_id) as unique_contracts,
          COUNTIF(cntrct_id IS NULL) as null_contracts
        FROM `gcp-dwh-prod.isbert_schema.sof_ta_apn_vertrag`
    """
    dq_result = client.query(dq_query).to_dataframe().iloc[0]
    
    assert dq_result["total_rows"] == dq_result["unique_contracts"], "Duplicate cntrct_id values found in target table!"
    assert dq_result["null_contracts"] == 0, "NULL cntrct_id values found in target table!"
```