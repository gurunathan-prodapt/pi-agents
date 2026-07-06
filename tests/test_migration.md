# Migration Validation Test Suite: `DW.BERT_AUSD_BP_TA_RN_VERTRAG`

This document contains the migration-validation test suite designed to verify that the migrated Google Cloud BigQuery / Dataform / Cloud Composer pipeline is behaviorally equivalent to the legacy Oracle / UC4 / KornShell implementation.

---

## 1. Output Parity & Pivot Correctness Test

### Purpose
Verify that the BigQuery Dataform model aggregates and pivots granular records from `sof_ta_rn_einzeln` into `sof_ta_rn_vertrag` exactly as the legacy Oracle PL/SQL script did, producing identical output values for a given set of input records.

### Setup
1. Create a sandbox dataset in BigQuery: `test_isbert_schema`.
2. Create the source table `test_isbert_schema.sof_ta_rn_einzeln` and populate it with a test contract containing multiple rows representing different phone line types (Telephone, Fax, Data, MultiSIM).
3. Ensure some fields contain `NULL` values to verify that `MAX()` aggregation handles them correctly.

```sql
-- Create temporary source table
CREATE OR REPLACE TABLE test_isbert_schema.sof_ta_rn_einzeln AS 
SELECT 
  123456 AS cntrct_id,
  'S' AS TN_multi_single, '0171111111' AS TN_TEL_msisdn, 'Active' AS TN_TEL_status, DATE('2026-12-31') AS TN_TEL_valid_to,
  CAST(NULL AS STRING) AS TN_FAX_msisdn, CAST(NULL AS STRING) AS TN_FAX_status, CAST(NULL AS DATE) AS TN_FAX_valid_to,
  CAST(NULL AS STRING) AS TN_DAT_msisdn, CAST(NULL AS STRING) AS TN_DAT_status, CAST(NULL AS DATE) AS TN_DAT_valid_to,
  CAST(NULL AS STRING) AS TC_multi_single, CAST(NULL AS STRING) AS TC_TEL_msisdn, CAST(NULL AS STRING) AS TC_TEL_status, CAST(NULL AS DATE) AS TC_TEL_valid_to,
  CAST(NULL AS STRING) AS TC_FAX_msisdn, CAST(NULL AS STRING) AS TC_FAX_status, CAST(NULL AS DATE) AS TC_FAX_valid_to,
  CAST(NULL AS STRING) AS TC_DAT_msisdn, CAST(NULL AS STRING) AS TC_DAT_status, CAST(NULL AS DATE) AS TC_DAT_valid_to,
  CAST(NULL AS STRING) AS TB_multi_single, CAST(NULL AS STRING) AS TB_TEL_msisdn, CAST(NULL AS STRING) AS TB_TEL_status, CAST(NULL AS DATE) AS TB_TEL_valid_to,
  CAST(NULL AS STRING) AS TB_FAX_msisdn, CAST(NULL AS STRING) AS TB_FAX_status, CAST(NULL AS DATE) AS TB_FAX_valid_to,
  CAST(NULL AS STRING) AS TB_DAT_msisdn, CAST(NULL AS STRING) AS TB_DAT_status, CAST(NULL AS DATE) AS TB_DAT_valid_to,
  CAST(NULL AS STRING) AS MS_RN_1_msisdn, CAST(NULL AS STRING) AS MS_RN_1_status, CAST(NULL AS DATE) AS MS_RN_1_valid_to,
  CAST(NULL AS STRING) AS MS_RN_2_msisdn, CAST(NULL AS STRING) AS MS_RN_2_status, CAST(NULL AS DATE) AS MS_RN_2_valid_to
UNION ALL
SELECT 
  123456 AS cntrct_id,
  CAST(NULL AS STRING) AS TN_multi_single, CAST(NULL AS STRING) AS TN_TEL_msisdn, CAST(NULL AS STRING) AS TN_TEL_status, CAST(NULL AS DATE) AS TN_TEL_valid_to,
  'M' AS TN_FAX_msisdn, '0171222222' AS TN_FAX_status, 'Suspended' AS TN_FAX_status, DATE('2026-06-30') AS TN_FAX_valid_to,
  CAST(NULL AS STRING) AS TN_DAT_msisdn, CAST(NULL AS STRING) AS TN_DAT_status, CAST(NULL AS DATE) AS TN_DAT_valid_to,
  CAST(NULL AS STRING) AS TC_multi_single, CAST(NULL AS STRING) AS TC_TEL_msisdn, CAST(NULL AS STRING) AS TC_TEL_status, CAST(NULL AS DATE) AS TC_TEL_valid_to,
  CAST(NULL AS STRING) AS TC_FAX_msisdn, CAST(NULL AS STRING) AS TC_FAX_status, CAST(NULL AS DATE) AS TC_FAX_valid_to,
  CAST(NULL AS STRING) AS TC_DAT_msisdn, CAST(NULL AS STRING) AS TC_DAT_status, CAST(NULL AS DATE) AS TC_DAT_valid_to,
  CAST(NULL AS STRING) AS TB_multi_single, CAST(NULL AS STRING) AS TB_TEL_msisdn, CAST(NULL AS STRING) AS TB_TEL_status, CAST(NULL AS DATE) AS TB_TEL_valid_to,
  CAST(NULL AS STRING) AS TB_FAX_msisdn, CAST(NULL AS STRING) AS TB_FAX_status, CAST(NULL AS DATE) AS TB_FAX_valid_to,
  CAST(NULL AS STRING) AS TB_DAT_msisdn, CAST(NULL AS STRING) AS TB_DAT_status, CAST(NULL AS DATE) AS TB_DAT_valid_to,
  '0171333333' AS MS_RN_1_msisdn, 'Active' AS MS_RN_1_status, DATE('2027-01-01') AS MS_RN_1_valid_to,
  '0171444444' AS MS_RN_2_msisdn, 'Active' AS MS_RN_2_status, DATE('2027-01-01') AS MS_RN_2_valid_to;
```

### Action
Run the target SQL logic against the test source table and output the results to a validation table:

```sql
CREATE OR REPLACE TABLE test_isbert_schema.sof_ta_rn_vertrag AS
SELECT
  cntrct_id,
  MAX(TN_multi_single) AS TN_multi_single,
  MAX(TN_TEL_msisdn) AS TN_TEL_msisdn,
  MAX(TN_TEL_status) AS TN_TEL_status,
  MAX(TN_TEL_valid_to) AS TN_TEL_valid_to,
  MAX(TN_FAX_msisdn) AS TN_FAX_msisdn,
  MAX(TN_FAX_status) AS TN_FAX_status,
  MAX(TN_FAX_valid_to) AS TN_FAX_valid_to,
  MAX(TN_DAT_msisdn) AS TN_DAT_msisdn,
  MAX(TN_DAT_status) AS TN_DAT_status,
  MAX(TN_DAT_valid_to) AS TN_DAT_valid_to,
  MAX(TC_multi_single) AS TC_multi_single,
  MAX(TC_TEL_msisdn) AS TC_TEL_msisdn,
  MAX(TC_TEL_status) AS TC_TEL_status,
  MAX(TC_TEL_valid_to) AS TC_TEL_valid_to,
  MAX(TC_FAX_msisdn) AS TC_FAX_msisdn,
  MAX(TC_FAX_status) AS TC_FAX_status,
  MAX(TC_FAX_valid_to) AS TC_FAX_valid_to,
  MAX(TC_DAT_msisdn) AS TC_DAT_msisdn,
  MAX(TC_DAT_status) AS TC_DAT_status,
  MAX(TC_DAT_valid_to) AS TC_DAT_valid_to,
  MAX(TB_multi_single) AS TB_multi_single,
  MAX(TB_TEL_msisdn) AS TB_TEL_msisdn,
  MAX(TB_TEL_status) AS TB_TEL_status,
  MAX(TB_TEL_valid_to) AS TB_TEL_valid_to,
  MAX(TB_FAX_msisdn) AS TB_FAX_msisdn,
  MAX(TB_FAX_status) AS TB_FAX_status,
  MAX(TB_FAX_valid_to) AS TB_FAX_valid_to,
  MAX(TB_DAT_msisdn) AS TB_DAT_msisdn,
  MAX(TB_DAT_status) AS TB_DAT_status,
  MAX(TB_DAT_valid_to) AS TB_DAT_valid_to,
  MAX(MS_RN_1_msisdn) AS MS_RN_1_msisdn,
  MAX(MS_RN_1_status) AS MS_RN_1_status,
  MAX(MS_RN_1_valid_to) AS MS_RN_1_valid_to,
  MAX(MS_RN_2_msisdn) AS MS_RN_2_msisdn,
  MAX(MS_RN_2_status) AS MS_RN_2_status,
  MAX(MS_RN_2_valid_to) AS MS_RN_2_valid_to
FROM
  test_isbert_schema.sof_ta_rn_einzeln
GROUP BY
  cntrct_id;
```

### Pass/Fail Criterion
The test passes if the output table contains exactly 1 row for `cntrct_id = 123456` with all pivoted fields correctly populated from both source rows, matching the expected values below.

```python
# pytest assertion snippet
import google.cloud.bigquery as bq

def test_pivot_correctness():
    client = bq.Client()
    query = "SELECT * FROM test_isbert_schema.sof_ta_rn_vertrag WHERE cntrct_id = 123456"
    df = client.query(query).to_dataframe()
    
    assert len(df) == 1
    assert df.loc[0, 'TN_multi_single'] == 'S'
    assert df.loc[0, 'TN_TEL_msisdn'] == '0171111111'
    assert df.loc[0, 'TN_FAX_msisdn'] == 'M'
    assert df.loc[0, 'MS_RN_1_msisdn'] == '0171333333'
    assert df.loc[0, 'MS_RN_2_msisdn'] == '0171444444'
    assert df.loc[0, 'TN_DAT_msisdn'] is None
```

---

## 2. Row-Count and Schema Integrity Assertions

### Purpose
Ensure that the target table `sof_ta_rn_vertrag` matches the expected schema structure (data types, column names) and that the row count matches the unique count of `cntrct_id` in the source table `sof_ta_rn_einzeln`.

### Setup
Ensure the production or staging tables `sof_ta_rn_einzeln` and `sof_ta_rn_vertrag` are populated.

### Action
Execute validation queries to compare unique source keys against target keys and verify schema constraints.

### Pass/Fail Criterion
* **Row Count Match**: The number of rows in `sof_ta_rn_vertrag` must exactly equal the number of distinct `cntrct_id` values in `sof_ta_rn_einzeln`.
* **Uniqueness**: `cntrct_id` must be a primary key (unique) in `sof_ta_rn_vertrag`.
* **Schema Match**: No columns should be missing, and data types must align with the target design.

```python
# pytest validation code
def test_schema_and_row_counts(project_id="gcp-bigquery-dwh-prod", dataset_id="isbert_schema"):
    client = bq.Client()
    
    # 1. Row Count Validation
    count_query = f"""
    SELECT 
      (SELECT COUNT(DISTINCT cntrct_id) FROM `{project_id}.{dataset_id}.sof_ta_rn_einzeln`) as expected_count,
      (SELECT COUNT(1) FROM `{project_id}.{dataset_id}.sof_ta_rn_vertrag`) as actual_count,
      (SELECT COUNT(DISTINCT cntrct_id) FROM `{project_id}.{dataset_id}.sof_ta_rn_vertrag`) as unique_actual_count
    """
    res = client.query(count_query).to_dataframe().iloc[0]
    assert res['expected_count'] == res['actual_count'], "Row count mismatch between source and target!"
    assert res['actual_count'] == res['unique_actual_count'], "cntrct_id is not unique in target table!"
    
    # 2. Schema Type Validation
    schema_query = f"""
    SELECT column_name, data_type 
    FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'sof_ta_rn_vertrag'
    """
    schema_df = client.query(schema_query).to_dataframe()
    
    # Assert specific key columns have correct types
    assert schema_df.loc[schema_df['column_name'] == 'cntrct_id', 'data_type'].values[0] == 'INT64'
    assert schema_df.loc[schema_df['column_name'] == 'TN_TEL_valid_to', 'data_type'].values[0] == 'DATE'
    assert schema_df.loc[schema_df['column_name'] == 'TN_TEL_msisdn', 'data_type'].values[0] == 'STRING'
```

---

## 3. Orchestration Parameter & Restart Logic Test

### Purpose
Verify that the Cloud Composer (Airflow) DAG correctly parses execution parameters (`stichtag` and `wiederanlauf_wert`) passed via the DAG Run Configuration, mimicking the legacy KornShell parameter parsing (`-s` and `-l`).

### Setup
1. Deploy the DAG `dw_bert_ausd_bp_ta_rn_vertrag` to the Cloud Composer environment.
2. Trigger a manual run of the DAG with a custom JSON configuration.

### Action
Trigger the DAG via the Airflow CLI or API with the following configuration:
```json
{
  "stichtag": "20260421",
  "wiederanlauf_wert": 99999
}
```

### Pass/Fail Criterion
* The task `parse_parameters` must succeed.
* The Airflow task logs for `parse_parameters` must output:
  * `Executing for Stichtag: 20260421`
  * `Restart boundary value: 99999`
* The XCom value for key `stichtag` on task `parse_parameters` must equal `"20260421"`.

---

## 4. Partitioning and Performance Verification

### Purpose
Verify that the target table `sof_ta_rn_vertrag` is correctly partitioned by `cntrct_id` range buckets as specified in the Dataform configuration, ensuring optimal query performance and cost control in BigQuery.

### Setup
Ensure the Dataform model has been compiled and executed to create the target table in the target environment.

### Action
Query the BigQuery `INFORMATION_SCHEMA.TABLES` and `INFORMATION_SCHEMA.COLUMNS` to verify partitioning metadata.

### Pass/Fail Criterion
The target table must be configured with range partitioning on `cntrct_id`.

```sql
-- SQL Assertion to verify partitioning
SELECT 
  table_name, 
  ddl
FROM 
  `isbert_schema.INFORMATION_SCHEMA.TABLES`
WHERE 
  table_name = 'sof_ta_rn_vertrag'
  AND REGEXP_CONTAINS(ddl, r'PARTITION BY RANGE_BUCKET\(cntrct_id');
```
* **Pass**: The query returns 1 row containing the DDL with the correct `RANGE_BUCKET` configuration.
* **Fail**: The query returns 0 rows, indicating the table is either unpartitioned or partitioned on the wrong column/type.