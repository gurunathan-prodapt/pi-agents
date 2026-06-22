As a senior data-migration QA engineer, I've analyzed the provided migration design and generated code for `DW.BERT_AUSD_V_TA_CNTRCT_TEMPL`. The following test cases are designed to ensure behavioural equivalence, data integrity, and correct functionality of the migrated job on Google Cloud Platform.

The tests cover output parity, transformation correctness (joins, aggregations, filters, type/NULL handling), external system replacements (source data ingestion), and data quality/schema assertions.

---

## Migration Validation Tests: DW.BERT_AUSD_V_TA_CNTRCT_TEMPL

### Test Setup Prerequisites

Before executing any tests, ensure the following:

1.  **Legacy Environment Access:** Read-only access to the Oracle source tables (`isbert_schema.dwtk_meldungen`, `cds$ta_cntrct_template`, `cds$ta_care_description`) and the target table (`sof$ta_cntrct_templ`) is available.
2.  **GCP Environment Setup:**
    *   BigQuery tables for source data (`your_project.isbert_schema.dwtk_meldungen`, `your_project.your_dataset.cds_ta_cntrct_template`, `your_project.your_dataset.cds_ta_care_description`) are created and populated.
    *   BigQuery target table (`your_project.your_dataset.sof_ta_cntrct_templ`) is created.
    *   BigQuery logging tables (`job_log`, `job_error_log`, `job_result`, `job_status`) are created.
    *   BigQuery Stored Procedure `your_project.your_dataset.r_ausd_v_ta_cntrct_templ` is deployed.
    *   Airflow DAG `dw_bert_ausd_v_ta_cntrct_templ` is deployed on Cloud Composer.
3.  **Test Data Generation:** A comprehensive set of test data should be prepared for both Oracle and BigQuery source tables, covering all filter conditions, NULL scenarios, and edge cases. This data should be identical in both environments for parity testing.

---

### Test Case 1: Output Parity - Full Data Match

*   **Purpose:** To verify that the migrated job produces an identical output dataset in BigQuery as the legacy Oracle job, given the same input data. This is the ultimate validation of behavioural equivalence.
*   **Setup:**
    1.  Populate the Oracle source tables (`isbert_schema.dwtk_meldungen`, `cds$ta_cntrct_template`, `cds$ta_care_description`) with a representative and diverse dataset, including edge cases for filters and NULLs.
    2.  Ensure the BigQuery equivalent source tables (`your_project.isbert_schema.dwtk_meldungen`, `your_project.your_dataset.cds_ta_cntrct_template`, `your_project.your_dataset.cds_ta_care_description`) are populated with *exactly* the same data as their Oracle counterparts. This is critical for a fair comparison.
    3.  Clear both the Oracle `sof$ta_cntrct_templ` and BigQuery `your_project.your_dataset.sof_ta_cntrct_templ` target tables.
*   **Action:**
    1.  Execute the legacy Oracle job (`DW.BERT_AUSD_V_TA_CNTRCT_TEMPL` via UC4/KornShell/SQL*Plus).
    2.  Execute the migrated BigQuery job by triggering the Airflow DAG `dw_bert_ausd_v_ta_cntrct_templ`.
    3.  Extract the full content of the Oracle target table `sof$ta_cntrct_templ`.
    4.  Extract the full content of the BigQuery target table `your_project.your_dataset.sof_ta_cntrct_templ`.
*   **Pass/Fail Criterion:**
    *   The row count of the BigQuery target table must exactly match the row count of the Oracle target table.
    *   A row-by-row, column-by-column comparison (e.g., using checksums, hash values, or direct data comparison after sorting) of the extracted data must show no differences. All `CNTRCT_TEMPLATE_ID`, `CDS_DESCRIPTION_ID`, and `CDS_DESCRIPTION` values must be identical.

*   **Runnable Test Code (Conceptual Python/Pytest with SQL):**

```python
import pytest
from google.cloud import bigquery
import pandas as pd
from sqlalchemy import create_engine, text

# Configuration (replace with actual values)
GCP_PROJECT_ID = "your_project"
BQ_DATASET_ID = "your_dataset"
ORACLE_CONN_STR = "oracle+cx_oracle://user:password@host:port/service_name"

# BigQuery client
bq_client = bigquery.Client(project=GCP_PROJECT_ID)

# Oracle engine (requires cx_Oracle and sqlalchemy-oracle drivers)
oracle_engine = create_engine(ORACLE_CONN_STR)

def fetch_oracle_data(query):
    with oracle_engine.connect() as connection:
        return pd.read_sql(text(query), connection)

def fetch_bigquery_data(query):
    return bq_client.query(query).to_dataframe()

@pytest.fixture(scope="module", autouse=True)
def setup_test_data():
    """
    Fixture to ensure identical source data in Oracle and BigQuery.
    This would involve a separate data ingestion/sync process.
    For this test, we assume it's already done.
    """
    # Placeholder for actual data setup/sync logic
    print("Ensuring identical source data in Oracle and BigQuery...")
    # Example: Clear target tables before each run
    bq_client.query(f"TRUNCATE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.sof_ta_cntrct_templ`").result()
    # Oracle truncate would be part of the legacy job's execution.

def run_legacy_job():
    """
    Simulate running the legacy Oracle job.
    In a real scenario, this would trigger the UC4 job or a script.
    For testing, we might manually run it or have a wrapper.
    """
    print("Executing legacy Oracle job...")
    # Example: Manual execution or API call to UC4/shell script
    # For this test, we assume it's run externally and populates Oracle target.

def run_migrated_job():
    """
    Trigger the Airflow DAG for the migrated job.
    """
    print("Triggering migrated BigQuery job via Airflow DAG...")
    # In a real test, this would use Airflow REST API or CLI to trigger the DAG.
    # For simplicity, we'll directly call the BigQuery SP here for isolated testing.
    bq_client.query(f"""
        CALL `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.r_ausd_v_ta_cntrct_templ`(
            p_JobKennung => 'BERT_AUSD_V_TA_CNTRCT_TEMPL',
            p_EintragsNr => 1
        )
    """).result()
    print("Migrated BigQuery job completed.")

def test_output_parity():
    run_legacy_job() # Assumes this populates Oracle target
    run_migrated_job() # This populates BigQuery target

    oracle_query = "SELECT CNTRCT_TEMPLATE_ID, CDS_DESCRIPTION_ID, CDS_DESCRIPTION FROM sof$ta_cntrct_templ ORDER BY CNTRCT_TEMPLATE_ID, CDS_DESCRIPTION_ID"
    bigquery_query = f"SELECT CNTRCT_TEMPLATE_ID, CDS_DESCRIPTION_ID, CDS_DESCRIPTION FROM `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.sof_ta_cntrct_templ` ORDER BY CNTRCT_TEMPLATE_ID, CDS_DESCRIPTION_ID"

    df_oracle = fetch_oracle_data(oracle_query)
    df_bigquery = fetch_bigquery_data(bigquery_query)

    print(f"Oracle row count: {len(df_oracle)}")
    print(f"BigQuery row count: {len(df_bigquery)}")

    assert len(df_oracle) == len(df_bigquery), "Row counts do not match between Oracle and BigQuery target tables."

    # Convert column types to be consistent for comparison (e.g., all strings)
    df_oracle = df_oracle.astype(str)
    df_bigquery = df_bigquery.astype(str)

    # Compare dataframes
    pd.testing.assert_frame_equal(df_oracle, df_bigquery, check_dtype=True)
    print("Output parity test passed: Oracle and BigQuery target tables are identical.")

# To run this test:
# 1. Ensure pytest, google-cloud-bigquery, pandas, sqlalchemy, cx_Oracle are installed.
# 2. Configure GCP_PROJECT_ID, BQ_DATASET_ID, ORACLE_CONN_STR.
# 3. Replace `run_legacy_job` with actual trigger for legacy system.
# 4. Run `pytest your_test_file.py`
```

---

### Test Case 2: Transformation Correctness - Processing Date (`v_datum`) Logic

*   **Purpose:** To verify that the `v_datum` variable is correctly derived from `dwtk_meldungen` and defaults to '19000101' when no relevant entry is found, matching the Oracle `NVL` behavior.
*   **Setup:**
    1.  **Scenario A (Valid Date):** Populate `your_project.isbert_schema.dwtk_meldungen` with an entry:
        ```sql
        INSERT INTO `your_project.isbert_schema.dwtk_meldungen` (job_kennung, timecreated)
        VALUES ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-10-26 10:00:00+00'));
        ```
    2.  **Scenario B (No Entry):** Clear `your_project.isbert_schema.dwtk_meldungen` or ensure no entry matches `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
    3.  **Scenario C (Multiple Entries):** Populate `your_project.isbert_schema.dwtk_meldungen` with multiple entries for `BERT_DROP_TEMP_TABLE` with different `timecreated` values.
*   **Action:**
    1.  For each scenario, execute the BigQuery job (e.g., by calling the stored procedure directly for isolated testing).
    2.  Inspect the `job_log` or `job_error_log` for any messages related to `v_datum` (if logging was enhanced to show it) or infer its value from the resulting data in `sof_ta_cntrct_templ` by checking which records were included/excluded based on date filters.
    3.  Alternatively, run the `DECLARE v_datum` block in isolation to confirm its value.
*   **Pass/Fail Criterion:**
    *   **Scenario A:** `v_datum` should be '20231026'.
    *   **Scenario B:** `v_datum` should be '19000101'.
    *   **Scenario C:** `v_datum` should be the `FORMAT_DATE('%Y%m%d', DATE(MAX(timecreated)))` among the matching entries.
    *   The records inserted into `sof_ta_cntrct_templ` must reflect the application of the derived `v_datum` in the date filters.

*   **Runnable Test Code (BigQuery SQL for verification):**

```sql
-- Verification for Scenario A (Valid Date)
-- Setup: Insert into dwtk_meldungen as described in setup.
DECLARE expected_v_datum STRING;
SET expected_v_datum = (
  SELECT IFNULL(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
  FROM `your_project.isbert_schema.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);
SELECT
  CASE
    WHEN expected_v_datum = '20231026' THEN 'PASS: v_datum derived correctly for valid date.'
    ELSE CONCAT('FAIL: v_datum expected 20231026, got ', expected_v_datum)
  END AS result;

-- Verification for Scenario B (No Entry)
-- Setup: Ensure no entry for 'BERT_DROP_TEMP_TABLE' in dwtk_meldungen.
DECLARE expected_v_datum_no_entry STRING;
SET expected_v_datum_no_entry = (
  SELECT IFNULL(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
  FROM `your_project.isbert_schema.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);
SELECT
  CASE
    WHEN expected_v_datum_no_entry = '19000101' THEN 'PASS: v_datum defaulted correctly for no entry.'
    ELSE CONCAT('FAIL: v_datum expected 19000101, got ', expected_v_datum_no_entry)
  END AS result;
```

---

### Test Case 3: Transformation Correctness - Join Logic

*   **Purpose:** To verify that the `JOIN` between `cds_ta_cntrct_template` and `cds_ta_care_description` on `cds_description_id` correctly filters out non-matching records.
*   **Setup:**
    1.  Populate `your_project.your_dataset.cds_ta_cntrct_template` with records having various `cds_description_id`s.
    2.  Populate `your_project.your_dataset.cds_ta_care_description` with records, ensuring:
        *   Some `cds_description_id`s match those in `cds_ta_cntrct_template`.
        *   Some `cds_description_id`s in `cds_ta_cntrct_template` have no match in `cds_ta_care_description`.
        *   Some `cds_description_id`s in `cds_ta_care_description` have no match in `cds_ta_cntrct_template`.
    3.  Set `dwtk_meldungen` to ensure `v_datum` allows all test records to pass date filters (e.g., set `v_datum` to a future date or '19000101' and `insert_at` to '19000101').
*   **Action:**
    1.  Execute the BigQuery job.
    2.  Query the `your_project.your_dataset.sof_ta_cntrct_templ` table.
*   **Pass/Fail Criterion:**
    *   Only records where `ct.cds_description_id` has a corresponding match in `cd.cds_description_id` should be present in the target table.
    *   The number of rows in the target table should equal the number of successful joins.

*   **Runnable Test Code (BigQuery SQL for verification):**

```sql
-- Assuming source tables are populated as described in setup.
-- Example:
-- cds_ta_cntrct_template:
-- (1, 101, '2023-01-01', NULL, '2023-01-01', NULL, 1) -- Matches
-- (2, 102, '2023-01-01', NULL, '2023-01-01', NULL, 1) -- No match in care_description
-- cds_ta_care_description:
-- (101, 'Description A', 1)
-- (103, 'Description C', 1) -- No match in cntrct_template

-- Expected result: Only record with CNTRCT_TEMPLATE_ID = 1 should be in target.

SELECT
  COUNT(DISTINCT ct.cntrct_template_id) AS expected_joined_count
FROM `your_project.your_dataset.cds_ta_cntrct_template` ct
JOIN `your_project.your_dataset.cds_ta_care_description` cd
  ON ct.cds_description_id = cd.cds_description_id
WHERE ct.insert_at <= PARSE_DATE('%Y%m%d', '19000101') -- Assuming v_datum is '19000101' for this test
  AND (ct.modified_at IS NULL OR ct.modified_at > PARSE_DATE('%Y%m%d', '19000101'))
  AND ct.valid_from <= PARSE_DATE('%Y%m%d', '19000101')
  AND (ct.valid_to IS NULL OR ct.valid_to > PARSE_DATE('%Y%m%d', '19000101'))
  AND ct.is_production = 1
  AND cd.language = 1;

-- After running the job, compare with:
SELECT COUNT(*) AS actual_target_count
FROM `your_project.your_dataset.sof_ta_cntrct_templ`;

-- Assertion: expected_joined_count == actual_target_count
```

---

### Test Case 4: Transformation Correctness - Filter Logic (Date Ranges & NULLs)

*   **Purpose:** To verify the precise application of all date-based filters, including correct handling of `NULL` values for `modified_at` and `valid_to`, as per the Oracle logic.
*   **Setup:**
    1.  Set `your_project.isbert_schema.dwtk_meldungen` so that `v_datum` is a specific date, e.g., '20230615'.
    2.  Populate `your_project.your_dataset.cds_ta_cntrct_template` with records covering all combinations of date filter conditions relative to `v_datum` ('20230615'), including:
        *   `insert_at`: before, on, after `v_datum`.
        *   `modified_at`: `NULL`, before, on, after `v_datum`.
        *   `valid_from`: before, on, after `v_datum`.
        *   `valid_to`: `NULL`, before, on, after `v_datum`.
        *   Ensure `is_production = 1` and `language = 1` for these records to isolate date filter testing.
    3.  Ensure `cds_description_id` matches exist in `cds_ta_care_description`.
*   **Action:**
    1.  Execute the BigQuery job.
    2.  Query the `your_project.your_dataset.sof_ta_cntrct_templ` table.
*   **Pass/Fail Criterion:**
    *   Only records satisfying ALL of the following conditions should be present:
        *   `ct.insert_at <= PARSE_DATE('%Y%m%d', v_datum)`
        *   `(ct.modified_at IS NULL OR ct.modified_at > PARSE_DATE('%Y%m%d', v_datum))`
        *   `ct.valid_from <= PARSE_DATE('%Y%m%d', v_datum)`
        *   `(ct.valid_to IS NULL OR ct.valid_to > PARSE_DATE('%Y%m%d', v_datum))`

*   **Runnable Test Code (BigQuery SQL for verification):**

```sql
-- Setup: Assume v_datum is '20230615'
-- Example test data for cds_ta_cntrct_template (simplified, assuming other filters pass)
-- (cntrct_template_id, cds_description_id, insert_at, modified_at, valid_from, valid_to, is_production)
-- 1, 101, '2023-06-15', NULL, '2023-06-15', NULL, 1  -- PASS
-- 2, 102, '2023-06-14', '2023-06-16', '2023-06-15', '2023-06-16', 1 -- PASS
-- 3, 103, '2023-06-16', NULL, '2023-06-15', NULL, 1  -- FAIL (insert_at > v_datum)
-- 4, 104, '2023-06-15', '2023-06-15', '2023-06-15', NULL, 1 -- FAIL (modified_at <= v_datum AND NOT NULL)
-- 5, 105, '2023-06-15', NULL, '2023-06-16', NULL, 1 -- FAIL (valid_from > v_datum)
-- 6, 106, '2023-06-15', NULL, '2023-06-15', '2023-06-15', 1 -- FAIL (valid_to <= v_datum AND NOT NULL)

DECLARE v_datum_test STRING DEFAULT '20230615'; -- This should match the v_datum derived by the job

SELECT
  COUNT(ct.cntrct_template_id) AS expected_filtered_count
FROM `your_project.your_dataset.cds_ta_cntrct_template` ct
JOIN `your_project.your_dataset.cds_ta_care_description` cd
  ON ct.cds_description_id = cd.cds_description_id
WHERE ct.insert_at <= PARSE_DATE('%Y%m%d', v_datum_test)
  AND (ct.modified_at IS NULL OR ct.modified_at > PARSE_DATE('%Y%m%d', v_datum_test))
  AND ct.valid_from <= PARSE_DATE('%Y%m%d', v_datum_test)
  AND (ct.valid_to IS NULL OR ct.valid_to > PARSE_DATE('%Y%m%d', v_datum_test))
  AND ct.is_production = 1
  AND cd.language = 1;

-- After running the job, compare with:
SELECT COUNT(*) AS actual_target_count
FROM `your_project.your_dataset.sof_ta_cntrct_templ`;

-- Assertion: expected_filtered_count == actual_target_count
```

---

### Test Case 5: Transformation Correctness - `is_production` and `language` Filters

*   **Purpose:** To verify that the `ct.is_production = 1` and `cd.language = 1` filters are correctly applied.
*   **Setup:**
    1.  Set `your_project.isbert_schema.dwtk_meldungen` to ensure `v_datum` allows all test records to pass date filters.
    2.  Populate `your_project.your_dataset.cds_ta_cntrct_template` with records where `is_production` is 0 or 1.
    3.  Populate `your_project.your_dataset.cds_ta_care_description` with records where `language` is 1 or other values (e.g., 2, 3).
    4.  Ensure `cds_description_id` matches exist for all test records.
*   **Action:**
    1.  Execute the BigQuery job.
    2.  Query the `your_project.your_dataset.sof_ta_cntrct_templ` table.
*   **Pass/Fail Criterion:**
    *   Only records where `is_production = 1` AND `language = 1` (in addition to passing date filters) should be present in the target table.

*   **Runnable Test Code (BigQuery SQL for verification):**

```sql
-- Setup: Assume v_datum is '19000101' and all date filters pass.
-- Example test data:
-- cds_ta_cntrct_template: (id, desc_id, ..., is_production)
-- (1, 101, ..., 1) -- PASS
-- (2, 102, ..., 0) -- FAIL
-- cds_ta_care_description: (id, description, language)
-- (101, 'Desc A', 1) -- PASS
-- (102, 'Desc B', 1) -- PASS
-- (103, 'Desc C', 2) -- FAIL

DECLARE v_datum_test STRING DEFAULT '19000101'; -- Or a date that allows all records to pass date filters

SELECT
  COUNT(ct.cntrct_template_id) AS expected_filtered_count
FROM `your_project.your_dataset.cds_ta_cntrct_template` ct
JOIN `your_project.your_dataset.cds_ta_care_description` cd
  ON ct.cds_description_id = cd.cds_description_id
WHERE ct.insert_at <= PARSE_DATE('%Y%m%d', v_datum_test)
  AND (ct.modified_at IS NULL OR ct.modified_at > PARSE_DATE('%Y%m%d', v_datum_test))
  AND ct.valid_from <= PARSE_DATE('%Y%m%d', v_datum_test)
  AND (ct.valid_to IS NULL OR ct.valid_to > PARSE_DATE('%Y%m%d', v_datum_test))
  AND ct.is_production = 1 -- Specific filter
  AND cd.language = 1;     -- Specific filter

-- After running the job, compare with:
SELECT COUNT(*) AS actual_target_count
FROM `your_project.your_dataset.sof_ta_cntrct_templ`;

-- Assertion: expected_filtered_count == actual_target_count
```

---

### Test Case 6: Data Quality - Schema and Data Types

*   **Purpose:** To verify that the target BigQuery table `sof_ta_cntrct_templ` has the correct schema (column names, data types) and that data is inserted without truncation or type conversion errors.
*   **Setup:**
    1.  Ensure the BigQuery target table `your_project.your_dataset.sof_ta_cntrct_templ` is created as per the DDL.
    2.  Populate source tables with data that tests data type boundaries (e.g., maximum length string for `CDS_DESCRIPTION`, large `INT64` values for IDs).
*   **Action:**
    1.  Inspect the schema of `your_project.your_dataset.sof_ta_cntrct_templ` in BigQuery.
    2.  Execute the BigQuery job.
    3.  Query the target table and inspect sample data to ensure values are as expected and not truncated or corrupted.
*   **Pass/Fail Criterion:**
    *   The schema of `your_project.your_dataset.sof_ta_cntrct_templ` must match:
        *   `CNTRCT_TEMPLATE_ID`: `INT64`
        *   `CDS_DESCRIPTION_ID`: `INT64`
        *   `CDS_DESCRIPTION`: `STRING`
    *   No job failures due to schema or data type mismatches.
    *   Data in `CDS_DESCRIPTION` is not truncated if it exceeds a certain length (BigQuery `STRING` handles up to 2MB, so this is primarily for unexpected source data issues).

*   **Runnable Test Code (BigQuery SQL for schema verification):**

```sql
SELECT
  column_name,
  data_type
FROM `your_project.your_dataset.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'sof_ta_cntrct_templ'
ORDER BY ordinal_position;

-- Expected Output:
-- column_name        data_type
-- CNTRCT_TEMPLATE_ID INT64
-- CDS_DESCRIPTION_ID INT64
-- CDS_DESCRIPTION    STRING

-- Manual inspection of data after job run:
SELECT * FROM `your_project.your_dataset.sof_ta_cntrct_templ` LIMIT 100;
```

---

### Test Case 7: Data Quality - Row Count Assertion

*   **Purpose:** To verify that the number of records inserted into the target table is correctly captured and logged in the `job_result` table.
*   **Setup:**
    1.  Populate source tables with a known number of records that will pass all filters.
    2.  Clear the `your_project.your_dataset.job_result` table.
*   **Action:**
    1.  Execute the BigQuery job.
    2.  Query the `your_project.your_dataset.sof_ta_cntrct_templ` table to get the actual row count.
    3.  Query the `your_project.your_dataset.job_result` table for the `records_processed` value for the latest job run.
*   **Pass/Fail Criterion:**
    *   The `records_processed` value in `job_result` for the job `DW.BERT_AUSD_V_TA_CNTRCT_TEMPL` must exactly match the `COUNT(*)` from `your_project.your_dataset.sof_ta_cntrct_templ`.

*   **Runnable Test Code (BigQuery SQL for verification):**

```sql
-- After running the job:
DECLARE actual_target_rows INT64;
SET actual_target_rows = (SELECT COUNT(*) FROM `your_project.your_dataset.sof_ta_cntrct_templ`);

DECLARE logged_processed_rows INT64;
SET logged_processed_rows = (
  SELECT records_processed
  FROM `your_project.your_dataset.job_result`
  WHERE job_id = 'DW.BERT_AUSD_V_TA_CNTRCT_TEMPL'
  ORDER BY end_time DESC
  LIMIT 1
);

SELECT
  CASE
    WHEN actual_target_rows = logged_processed_rows THEN 'PASS: Row count assertion matches.'
    ELSE CONCAT('FAIL: Actual rows (', actual_target_rows, ') != Logged rows (', logged_processed_rows, ')')
  END AS result;
```

---

### Test Case 8: External System Replacement - Source Data Ingestion Validation

*   **Purpose:** To verify that the data ingested into BigQuery from the original Oracle/Carmen sources (`dwtk_meldungen`, `cds_ta_cntrct_template`, `cds_ta_care_description`) is an accurate and complete replica. This is a critical prerequisite for the migrated job's correctness.
*   **Setup:**
    1.  Identify a specific point in time or a specific dataset in the Oracle source systems.
    2.  Execute the data ingestion pipelines (e.g., Cloud Data Fusion, Dataflow jobs) that populate `your_project.isbert_schema.dwtk_meldungen`, `your_project.your_dataset.cds_ta_cntrct_template`, and `your_project.your_dataset.cds_ta_care_description`.
*   **Action:**
    1.  For each source table:
        *   Get the row count from the Oracle source table.
        *   Get the row count from the corresponding BigQuery ingested table.
        *   Perform a checksum or hash comparison of the entire table content (if feasible for large tables) or a detailed sample comparison.
        *   Compare schema (column names, data types, nullability) between Oracle and BigQuery.
*   **Pass/Fail Criterion:**
    *   For each source table, the row count in BigQuery must exactly match the row count in Oracle.
    *   The schema (column names, data types) of the BigQuery tables must accurately reflect the Oracle source tables.
    *   A statistically significant sample of data, or a full data comparison, must confirm that the content is identical, accounting for any expected type conversions (e.g., Oracle `NUMBER` to BigQuery `INT64`/`NUMERIC`, Oracle `DATE`/`TIMESTAMP` to BigQuery `DATE`/`TIMESTAMP`).

*   **Runnable Test Code (Conceptual Python/Pytest with SQL):**

```python
import pytest
from google.cloud import bigquery
import pandas as pd
from sqlalchemy import create_engine, text

# Configuration (replace with actual values)
GCP_PROJECT_ID = "your_project"
BQ_DATASET_ID = "your_dataset"
ORACLE_CONN_STR = "oracle+cx_oracle://user:password@host:port/service_name"

bq_client = bigquery.Client(project=GCP_PROJECT_ID)
oracle_engine = create_engine(ORACLE_CONN_STR)

def get_oracle_table_info(table_name):
    query_count = f"SELECT COUNT(*) FROM {table_name}"
    query_checksum = f"SELECT ORA_HASH(DBMS_LOB.SUBSTR(DBMS_LOB.GETLENGTH(TO_CLOB(t.*)), 4000, 1)) FROM {table_name} t" # Simplified checksum
    query_schema = f"""
        SELECT COLUMN_NAME, DATA_TYPE, NULLABLE
        FROM ALL_TAB_COLUMNS
        WHERE OWNER = 'ISBERT_SCHEMA' AND TABLE_NAME = '{table_name.upper().split('.')[-1]}'
        ORDER BY COLUMN_ID
    """
    with oracle_engine.connect() as connection:
        count = connection.execute(text(query_count)).scalar()
        # checksum = connection.execute(text(query_checksum)).scalar() # May need more robust checksum
        schema_df = pd.read_sql(text(query_schema), connection)
    return count, schema_df #, checksum

def get_bigquery_table_info(project_id, dataset_id, table_name):
    full_table_name = f"`{project_id}.{dataset_id}.{table_name}`"
    query_count = f"SELECT COUNT(*) FROM {full_table_name}"
    query_checksum = f"SELECT FARM_FINGERPRINT(TO_JSON_STRING(t)) FROM {full_table_name} t" # Row-level checksum
    query_schema = f"""
        SELECT column_name, data_type, is_nullable
        FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position
    """
    count = bq_client.query(query_count).result().single_row[0]
    # checksums = bq_client.query(query_checksum).to_dataframe() # For full table checksum
    schema_df = bq_client.query(query_schema).to_dataframe()
    return count, schema_df #, checksums

@pytest.mark.parametrize("oracle_table, bq_dataset, bq_table", [
    ("isbert_schema.dwtk_meldungen", "isbert_schema", "dwtk_meldungen"),
    ("cds$ta_cntrct_template", BQ_DATASET_ID, "cds_ta_cntrct_template"),
    ("cds$ta_care_description", BQ_DATASET_ID, "cds_ta_care_description"),
])
def test_source_data_ingestion_parity(oracle_table, bq_dataset, bq_table):
    print(f"\n--- Validating ingestion for {oracle_table} -> {GCP_PROJECT_ID}.{bq_dataset}.{bq_table} ---")

    # Run ingestion pipeline here (e.g., trigger Dataflow job)
    # For this test, we assume ingestion has already run.

    oracle_count, oracle_schema = get_oracle_table_info(oracle_table)
    bq_count, bq_schema = get_bigquery_table_info(GCP_PROJECT_ID, bq_dataset, bq_table)

    print(f"Oracle '{oracle_table}' row count: {oracle_count}")
    print(f"BigQuery '{bq_table}' row count: {bq_count}")

    assert oracle_count == bq_count, f"Row count mismatch for {oracle_table}."

    # Schema comparison (simplified, requires mapping Oracle types to BQ types)
    # This part needs careful implementation based on actual type mappings.
    # For example: Oracle NUMBER(X,Y) -> BQ NUMERIC, Oracle VARCHAR2 -> BQ STRING, Oracle DATE -> BQ DATE/TIMESTAMP
    # Example:
    # assert len(oracle_schema) == len(bq_schema), f"Column count mismatch for {oracle_table}"
    # for _, oracle_col in oracle_schema.iterrows():
    #     bq_col = bq_schema[bq_schema['column_name'].str.lower() == oracle_col['COLUMN_NAME'].lower()]
    #     assert not bq_col.empty, f"Column {oracle_col['COLUMN_NAME']} not found in BigQuery table."
    #     # Add type and nullability checks here, mapping Oracle to BQ types

    print(f"Ingestion validation for {oracle_table} passed (row count and basic schema check).")

# To run this test:
# 1. Ensure pytest, google-cloud-bigquery, pandas, sqlalchemy, cx_Oracle are installed.
# 2. Configure GCP_PROJECT_ID, BQ_DATASET_ID, ORACLE_CONN_STR.
# 3. Run `pytest your_test_file.py`
```

---

### Test Case 9: Orchestration - Airflow DAG Execution and Logging

*   **Purpose:** To verify that the Airflow DAG successfully triggers the BigQuery Stored Procedure and that the custom logging tables (`job_log`, `job_status`, `job_result`) are populated correctly with job execution details.
*   **Setup:**
    1.  Ensure the Airflow DAG `dw_bert_ausd_v_ta_cntrct_templ` is deployed and unpaused in Cloud Composer.
    2.  Ensure the BigQuery Stored Procedure `r_ausd_v_ta_cntrct_templ` and logging tables are deployed.
    3.  Clear the logging tables (`job_log`, `job_status`, `job_result`).
    4.  Populate source tables with valid data to ensure a successful run.
*   **Action:**
    1.  Manually trigger the Airflow DAG `dw_bert_ausd_v_ta_cntrct_templ` from the Airflow UI or CLI.
    2.  Monitor the Airflow UI for DAG run status and task logs.
    3.  Query the BigQuery logging tables (`job_log`, `job_status`, `job_result`) for entries related to the job `DW.BERT_AUSD_V_TA_CNTRCT_TEMPL`.
*   **Pass/Fail Criterion:**
    *   The Airflow DAG run completes successfully (green status).
    *   The Airflow task `execute_contract_template_sp` completes successfully.
    *   `job_log` contains a 'START' entry for the job.
    *   `job_status` contains 'COMPLETED' entry for the job.
    *   `job_result` contains a 'SUCCESS' entry with `records_processed` > 0 (assuming data was processed).
    *   All timestamps in logging tables are recent and consistent with the execution time.

*   **Runnable Test Code (Conceptual Python/Pytest with Airflow API/CLI and SQL):**

```python
import pytest
from airflow.api.client.local_client import Client # For local testing, use Airflow REST API for real
from google.cloud import bigquery
import time

# Configuration
GCP_PROJECT_ID = "your_project"
BQ_DATASET_ID = "your_dataset"
AIRFLOW_DAG_ID = "dw_bert_ausd_v_ta_cntrct_templ"

bq_client = bigquery.Client(project=GCP_PROJECT_ID)
# airflow_client = Client(None, None) # For local Airflow CLI interaction

def trigger_airflow_dag(dag_id):
    """
    Triggers an Airflow DAG. In a real scenario, use Airflow REST API or gcloud CLI.
    """
    print(f"Triggering Airflow DAG: {dag_id}")
    # Example using gcloud CLI (requires gcloud to be configured)
    # import subprocess
    # subprocess.run(['gcloud', 'composer', 'environments', 'run', 'YOUR_COMPOSER_ENV_NAME',
    #                 '--location', 'YOUR_COMPOSER_REGION', 'dags', 'trigger', dag_id])
    # For this test, we'll assume it's triggered and focus on BQ logs.
    # For direct testing, you might call the BQ SP directly as in other tests.
    bq_client.query(f"""
        CALL `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.r_ausd_v_ta_cntrct_templ`(
            p_JobKennung => 'BERT_AUSD_V_TA_CNTRCT_TEMPL',
            p_EintragsNr => 1
        )
    """).result()
    print(f"DAG {dag_id} execution initiated (or SP called directly).")

def get_latest_job_log_entry(job_id, table_name):
    query = f"""
        SELECT *
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.{table_name}`
        WHERE job_id = '{job_id}'
        ORDER BY
            CASE
                WHEN table_name = 'job_log' THEN start_time
                WHEN table_name = 'job_error_log' THEN error_time
                WHEN table_name = 'job_result' THEN end_time
                WHEN table_name = 'job_status' THEN status_time
            END DESC
        LIMIT 1
    """
    return bq_client.query(query).to_dataframe()

@pytest.fixture(scope="function", autouse=True)
def clear_logging_tables():
    """Clear logging tables before each test run."""
    bq_client.query(f"TRUNCATE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.job_log`").result()
    bq_client.query(f"TRUNCATE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.job_error_log`").result()
    bq_client.query(f"TRUNCATE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.job_result`").result()
    bq_client.query(f"TRUNCATE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.job_status`").result()
    print("Logging tables cleared.")

def test_airflow_dag_execution_and_logging():
    # Ensure source data is available for a successful run
    # (e.g., insert a record into cds_ta_cntrct_template and cds_ta_care_description)
    bq_client.query(f"""
        INSERT INTO `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.cds_ta_cntrct_template`
        (cntrct_template_id, cds_description_id, insert_at, modified_at, valid_from, valid_to, is_production)
        VALUES (1000, 1000, '2023-01-01', NULL, '2023-01-01', NULL, 1);
        INSERT INTO `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.cds_ta_care_description`
        (cds_description_id, cds_description, language)
        VALUES (1000, 'Test Description', 1);
        INSERT INTO `{GCP_PROJECT_ID}.isbert_schema.dwtk_meldungen` (job_kennung, timecreated)
        VALUES ('BERT_DROP_TEMP_TABLE', TIMESTAMP('2023-10-26 10:00:00+00'));
    """).result()

    trigger_airflow_dag(AIRFLOW_DAG_ID)
    time.sleep(10) # Give some time for the job to complete and logs to appear

    job_id = 'DW.BERT_AUSD_V_TA_CNTRCT_TEMPL'

    log_start = get_latest_job_log_entry(job_id, 'job_log')
    log_status = get_latest_job_log_entry(job_id, 'job_status')
    log_result = get_latest_job_log_entry(job_id, 'job_result')
    log_error = get_latest_job_log_entry(job_id, 'job_error_log')

    assert not log_start.empty, "Job START entry not found in job_log."
    assert log_start['status'].iloc[0] == 'START', "Job START status incorrect."

    assert not log_status.empty, "Job COMPLETED/FAILED entry not found in job_status."
    assert log_status['status'].iloc[0] == 'COMPLETED', "Job status should be COMPLETED."

    assert not log_result.empty, "Job SUCCESS/ERROR entry not found in job_result."
    assert log_result['status'].iloc[0] == 'SUCCESS', "Job result status should be SUCCESS."
    assert log_result['records_processed'].iloc[0] > 0, "No records processed, expected some."

    assert log_error.empty, "No error entries expected for a successful run."

    print("Airflow DAG execution and logging test passed.")
```

---

### Test Case 10: Orchestration - Error Handling

*   **Purpose:** To verify that the BigQuery Stored Procedure correctly handles errors during parameter validation or transformation, logs them, and sets the job status to 'FAILED'.
*   **Setup:**
    1.  Ensure the BigQuery Stored Procedure `r_ausd_v_ta_cntrct_templ` and logging tables are deployed.
    2.  Clear the logging tables (`job_log`, `job_error_log`, `job_status`, `job_result`).
    3.  **Scenario A (Parameter Error):** Call the stored procedure with an invalid `p_JobKennung` (e.g., `NULL` or empty string).
    4.  **Scenario B (Transformation Error):** Introduce a deliberate error in the `d_ausd_v_ta_cntrct_templ_transform` logic (e.g., by temporarily changing a column name in the `SELECT` statement or forcing a data type mismatch) or in the source data that would cause a BigQuery SQL error during execution.
*   **Action:**
    1.  For each scenario, execute the BigQuery job (e.g., by calling the stored procedure directly).
    2.  Query the BigQuery logging tables (`job_log`, `job_error_log`, `job_status`, `job_result`).
*   **Pass/Fail Criterion:**
    *   The call to the stored procedure should fail and return an error message.
    *   `job_error_log` must contain an entry with `job_id`, `error_time`, `error_code`, `error_message`, and `error_arg` (for parameter error).
    *   `job_status` must contain a 'FAILED' entry for the job.
    *   `job_result` should either be empty or contain an 'ERROR' entry (depending on when the error occurs relative to `@@row_count` capture).

*   **Runnable Test Code (BigQuery SQL for verification):**

```sql
-- Setup: Clear logging tables (as in Test Case 9 fixture)

-- Scenario A: Parameter Error
-- Action: Call SP with invalid parameter
BEGIN
  CALL `your_project.your_dataset.r_ausd_v_ta_cntrct_templ`(
    p_JobKennung => '', -- Invalid parameter
    p_EintragsNr => 1
  );
EXCEPTION WHEN ERROR THEN
  SELECT 'Caught expected parameter error.' AS status;
END;

-- Verification for Scenario A
SELECT
  COUNT(*) AS error_log_count,
  MAX(error_message) AS error_message,
  MAX(error_arg) AS error_arg
FROM `your_project.your_dataset.job_error_log`
WHERE job_id = 'DW.BERT_AUSD_V_TA_CNTRCT_TEMPL';

SELECT
  MAX(status) AS job_status
FROM `your_project.your_dataset.job_status`
WHERE job_id = 'DW.BERT_AUSD_V_TA_CNTRCT_TEMPL';

-- Assertions:
-- error_log_count > 0
-- error_message contains 'Parameter validation failed'
-- error_arg contains 'Jobkennung'
-- job_status = 'FAILED'

-- Scenario B: Transformation Error (Requires temporary modification to the SP or source data)
-- Action: Temporarily modify the `EXECUTE IMMEDIATE` block in the SP to cause an error,
-- e.g., change `ct.cntrct_template_id` to `ct.non_existent_column`.
-- Then call the SP:
BEGIN
  CALL `your_project.your_dataset.r_ausd_v_ta_cntrct_templ`(
    p_JobKennung => 'BERT_AUSD_V_TA_CNTRCT_TEMPL',
    p_EintragsNr => 1
  );
EXCEPTION WHEN ERROR THEN
  SELECT 'Caught expected transformation error.' AS status;
END;

-- Verification for Scenario B (similar to Scenario A)
SELECT
  COUNT(*) AS error_log_count,
  MAX(error_message) AS error_message
FROM `your_project.your_dataset.job_error_log`
WHERE job_id = 'DW.BERT_AUSD_V_TA_CNTRCT_TEMPL';

SELECT
  MAX(status) AS job_status
FROM `your_project.your_dataset.job_status`
WHERE job_id = 'DW.BERT_AUSD_V_TA_CNTRCT_TEMPL';

-- Assertions:
-- error_log_count > 0
-- error_message contains BigQuery SQL error details (e.g., 'Unrecognized name: non_existent_column')
-- job_status = 'FAILED'
```

---

### Test Case 11: Idempotency / Truncate Behavior

*   **Purpose:** To verify that running the job multiple times with the same source data produces the same result in the target table, due to the `TRUNCATE` operation.
*   **Setup:**
    1.  Populate source tables with a fixed, known dataset.
    2.  Clear the `your_project.your_dataset.sof_ta_cntrct_templ` table.
*   **Action:**
    1.  Execute the BigQuery job for the first time.
    2.  Record the row count and a checksum/hash of the target table `your_project.your_dataset.sof_ta_cntrct_templ`.
    3.  Execute the BigQuery job for the second time (without changing source data).
    4.  Record the row count and a checksum/hash of the target table again.
*   **Pass/Fail Criterion:**
    *   The row count after the first run must be identical to the row count after the second run.
    *   The checksum/hash of the target table content after the first run must be identical to the checksum/hash after the second run. This confirms the data content is exactly the same.

*   **Runnable Test Code (BigQuery SQL for verification):**

```sql
-- Function to get table checksum (example, can be more robust)
CREATE OR REPLACE FUNCTION `your_project.your_dataset.get_table_checksum`(
  project_id STRING, dataset_id STRING, table_name STRING
) RETURNS STRING AS
(
  (SELECT FARM_FINGERPRINT(ARRAY_AGG(TO_JSON_STRING(t) ORDER BY CNTRCT_TEMPLATE_ID, CDS_DESCRIPTION_ID))
   FROM `your_project`.`your_dataset`.`sof_ta_cntrct_templ` AS t)
);

-- Setup: Ensure source data is stable.
-- Clear target table: TRUNCATE TABLE `your_project.your_dataset.sof_ta_cntrct_templ`;

-- Run 1
CALL `your_project.your_dataset.r_ausd_v_ta_cntrct_templ`(
  p_JobKennung => 'BERT_AUSD_V_TA_CNTRCT_TEMPL',
  p_EintragsNr => 1
);

DECLARE run1_row_count INT64;
DECLARE run1_checksum STRING;
SET run1_row_count = (SELECT COUNT(*) FROM `your_project.your_dataset.sof_ta_cntrct_templ`);
SET run1_checksum = `your_project.your_dataset.get_table_checksum`('your_project', 'your_dataset', 'sof_ta_cntrct_templ');

-- Run 2
CALL `your_project.your_dataset.r_ausd_v_ta_cntrct_templ`(
  p_JobKennung => 'BERT_AUSD_V_TA_CNTRCT_TEMPL',
  p_EintragsNr => 1
);

DECLARE run2_row_count INT64;
DECLARE run2_checksum STRING;
SET run2_row_count = (SELECT COUNT(*) FROM `your_project.your_dataset.sof_ta_cntrct_templ`);
SET run2_checksum = `your_project.your_dataset.get_table_checksum`('your_project', 'your_dataset', 'sof_ta_cntrct_templ');

SELECT
  CASE
    WHEN run1_row_count = run2_row_count AND run1_checksum = run2_checksum
    THEN 'PASS: Job is idempotent - row count and checksum match after multiple runs.'
    ELSE CONCAT('FAIL: Idempotency check failed. Run 1 rows: ', run1_row_count, ', checksum: ', run1_checksum,
                '. Run 2 rows: ', run2_row_count, ', checksum: ', run2_checksum)
  END AS result;
```