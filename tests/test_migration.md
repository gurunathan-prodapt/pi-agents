As a senior data-migration QA engineer, I've designed a comprehensive suite of tests to validate the migration of the `r_ausd_v_ta_apn_ve.ksh` job from its legacy Oracle/KornShell implementation to the new Cloud Composer/BigQuery architecture. These tests aim to ensure behavioral equivalence, data integrity, and operational robustness.

---

## Migration Validation Tests: `r_ausd_v_ta_apn_ve.ksh`

### Test Environment Setup (Pre-requisites for all tests)

*   **Legacy Environment**: Access to the original Oracle database and the ability to execute the legacy KornShell job.
*   **Migration Environment**:
    *   A GCP project with BigQuery datasets (`raw`, `staging`, `data_warehouse`).
    *   BigQuery staging tables (`project.dataset.dwtk_meldungen_stg`, `project.dataset.pds_ta_pdp_context_assoc_stg`, `project.dataset.pds_ta_pdp_context_stg`, `project.dataset.pds_ta_access_point_stg`) populated with data ingested from the Oracle sources.
    *   BigQuery target table (`project.dataset.sof_ta_apn_ve`).
    *   A deployed Cloud Composer DAG (`r_ausd_v_ta_apn_ve_dag`) that orchestrates the BigQuery transformation.
    *   Access to Cloud Logging and Cloud Monitoring.
*   **Test Data**: A mechanism to generate and load controlled test data into both Oracle source tables and their corresponding BigQuery staging tables, ensuring identical content for comparative tests. This includes scenarios for various date ranges, NULL values, and join conditions.
*   **Tools**: Python with `pytest`, `google-cloud-bigquery` client, and potentially an Oracle database client library for legacy data extraction.

---

### 1. Output Parity Tests

#### Test Case 1.1: End-to-End Data Parity

*   **Purpose**: To prove that the migrated BigQuery job produces an identical final dataset in the target table as the legacy Oracle job, given the same source data. This is the most critical test for behavioral equivalence.
*   **Setup**:
    1.  Take a snapshot of the Oracle source tables (`dwtk_meldungen`, `pds$ta_pdp_context_assoc`, `pds$ta_pdp_context`, `pds$ta_access_point`).
    2.  Load this exact same data into the corresponding BigQuery staging tables.
    3.  Ensure both the Oracle target table (`sof$ta_apn_ve`) and the BigQuery target table (`project.dataset.sof_ta_apn_ve`) are empty before execution.
*   **Action**:
    1.  Execute the legacy Oracle job (`r_ausd_v_ta_apn_ve.ksh`).
    2.  Execute the migrated BigQuery job via the Cloud Composer DAG (`r_ausd_v_ta_apn_ve_dag`).
    3.  Extract all data from both the legacy Oracle target table and the new BigQuery target table.
*   **Pass/Fail Criterion**:
    *   **Pass**: The full dataset extracted from the BigQuery target table is identical to the full dataset extracted from the Oracle target table, considering column order and data types. This can be verified by comparing row counts and then comparing sorted datasets (e.g., using a hash of each row or a full table hash).
    *   **Fail**: Any discrepancy in row count, column values, or data types between the two target tables.

```python
# Example pytest assertion for data parity
import pandas as pd
from google.cloud import bigquery
import cx_Oracle # Assuming cx_Oracle for legacy DB access

def test_end_to_end_data_parity(bigquery_client, oracle_client, test_data_snapshot_id):
    # Setup: Load identical source data (handled by fixture/pre-test script)
    # Action: Run legacy job (simulated or actual execution)
    # run_legacy_job()
    # Action: Run BigQuery job via Airflow (simulated or actual trigger)
    # trigger_airflow_dag("r_ausd_v_ta_apn_ve_dag", conf={"snapshot_id": test_data_snapshot_id})
    # wait_for_dag_completion("r_ausd_v_ta_apn_ve_dag")

    # Extract data from legacy Oracle
    oracle_query = "SELECT cntrct_id, access_point_name FROM sof$ta_apn_ve ORDER BY cntrct_id, access_point_name"
    oracle_df = pd.read_sql(oracle_query, oracle_client.connection)

    # Extract data from BigQuery
    bq_query = "SELECT cntrct_id, access_point_name FROM `project.dataset.sof_ta_apn_ve` ORDER BY cntrct_id, access_point_name"
    bq_df = bigquery_client.query(bq_query).to_dataframe()

    # Convert column types to be consistent for comparison if needed (e.g., int64 vs object)
    # Ensure column names are identical
    bq_df.columns = [col.upper() for col in bq_df.columns] # Oracle often returns uppercase

    # Pass/Fail: Compare DataFrames
    pd.testing.assert_frame_equal(oracle_df, bq_df, check_dtype=True, check_exact=True)
```

#### Test Case 1.2: Row Count Parity

*   **Purpose**: To quickly verify that the number of records processed and loaded into the target table is consistent between the legacy and migrated jobs.
*   **Setup**: Same as Test Case 1.1.
*   **Action**:
    1.  Execute both jobs as in Test Case 1.1.
    2.  Query the row count from both target tables.
*   **Pass/Fail Criterion**:
    *   **Pass**: The row count in `project.dataset.sof_ta_apn_ve` is exactly equal to the row count in `sof$ta_apn_ve`.
    *   **Fail**: Any difference in row counts.

```sql
-- BigQuery row count
SELECT COUNT(*) FROM `project.dataset.sof_ta_apn_ve`;

-- Oracle row count
SELECT COUNT(*) FROM sof$ta_apn_ve;
```

---

### 2. Transformation Correctness Tests

#### Test Case 2.1: `v_datum` Derivation Parity

*   **Purpose**: To ensure the `v_datum` (watermark date) is derived identically in BigQuery as it would be in the legacy Oracle environment. This value is critical for all date-based filtering.
*   **Setup**:
    1.  Populate `dwtk_meldungen_stg` (BigQuery) and `dwtk_meldungen` (Oracle) with identical data, including various `timecreated` values and `job_kennung` values (some 'BERT_DROP_TEMP_TABLE', some others).
    2.  Ensure there's at least one record with `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
*   **Action**:
    1.  Manually execute the `v_datum` derivation logic in Oracle (if possible, or infer from legacy job logs).
    2.  Execute the `v_datum` derivation task within the Airflow DAG or directly run the BigQuery SQL snippet.
*   **Pass/Fail Criterion**:
    *   **Pass**: The `v_datum` value derived in BigQuery matches the `MAX(DATE(timecreated))` from Oracle for `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
    *   **Fail**: Mismatch in the derived `v_datum`.

```python
# Example pytest assertion for v_datum
def test_v_datum_derivation(bigquery_client, oracle_client):
    # Populate source tables with controlled data
    # ... (e.g., insert into dwtk_meldungen_stg and dwtk_meldungen)

    # Oracle v_datum (assuming direct query for comparison)
    oracle_v_datum_query = """
        SELECT TO_CHAR(MAX(TRUNC(timecreated)), 'YYYY-MM-DD')
        FROM isbert_schema.dwtk_meldungen
        WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
    """
    oracle_v_datum = oracle_client.query(oracle_v_datum_query).fetchone()[0]

    # BigQuery v_datum
    bq_v_datum_query = """
        SELECT FORMAT_DATE('%Y-%m-%d', MAX(DATE(timecreated)))
        FROM `project.dataset.dwtk_meldungen_stg`
        WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
    """
    bq_v_datum = bigquery_client.query(bq_v_datum_query).result().to_dataframe().iloc[0, 0]

    assert oracle_v_datum == bq_v_datum
```

#### Test Case 2.2: Truncate and Load Mechanism

*   **Purpose**: To verify that the target table is correctly truncated before new data is inserted, ensuring a full refresh as per the design.
*   **Setup**:
    1.  Populate `project.dataset.sof_ta_apn_ve` with some dummy data (e.g., 100 rows).
    2.  Populate BigQuery staging tables with data that, when transformed, would result in a different number of rows (e.g., 50 rows).
*   **Action**: Execute the BigQuery job via the Cloud Composer DAG.
*   **Pass/Fail Criterion**:
    *   **Pass**: After the job completes, the `project.dataset.sof_ta_apn_ve` table contains exactly 50 rows (the expected output from the transformation, not the initial 100 dummy rows).
    *   **Fail**: The table still contains the initial dummy data, or the row count is not as expected, indicating the truncate failed or the insert was additive.

```python
# Example pytest assertion for truncate/load
def test_truncate_and_load(bigquery_client):
    target_table = "`project.dataset.sof_ta_apn_ve`"
    staging_table = "`project.dataset.pds_ta_pdp_context_assoc_stg`" # Example staging table

    # 1. Populate target with dummy data
    bigquery_client.query(f"CREATE OR REPLACE TABLE {target_table} (cntrct_id STRING, access_point_name STRING);").result()
    bigquery_client.query(f"INSERT INTO {target_table} (cntrct_id, access_point_name) VALUES ('DUMMY1', 'AP1'), ('DUMMY2', 'AP2');").result()
    initial_count = bigquery_client.query(f"SELECT COUNT(*) FROM {target_table}").result().to_dataframe().iloc[0,0]
    assert initial_count == 2

    # 2. Populate staging with data that will result in 1 row after transformation
    # (Simplified for example, in reality, this would involve all source tables)
    bigquery_client.query(f"CREATE OR REPLACE TABLE {staging_table} (cntrct_id STRING, insert_at DATE, modified_at DATE, valid_from DATE, valid_to DATE);").result()
    bigquery_client.query(f"INSERT INTO {staging_table} (cntrct_id, insert_at, modified_at, valid_from, valid_to) VALUES ('REAL1', '2023-01-01', NULL, '2023-01-01', NULL);").result()
    # Assume v_datum is '2023-01-01' for this test scenario

    # 3. Trigger the BigQuery transformation (simulated or actual)
    # This would involve running the full BigQuery SQL from the DAG
    # For this test, we'll simulate the core logic directly
    bq_sql_transformation = f"""
        DECLARE v_datum DATE DEFAULT '2023-01-01'; -- Hardcode for test
        TRUNCATE TABLE {target_table};
        INSERT INTO {target_table} (cntrct_id, access_point_name)
        SELECT pca.cntrct_id, 'TestAPN' -- Simplified access_point_name for this test
        FROM {staging_table} pca
        WHERE pca.insert_at <= v_datum
        AND (pca.modified_at IS NULL OR pca.modified_at > v_datum)
        AND pca.valid_from <= v_datum
        AND (pca.valid_to IS NULL OR pca.valid_to > v_datum)
        AND pca.cntrct_id IS NOT NULL;
    """
    bigquery_client.query(bq_sql_transformation).result()

    # 4. Verify final count
    final_count = bigquery_client.query(f"SELECT COUNT(*) FROM {target_table}").result().to_dataframe().iloc[0,0]
    assert final_count == 1 # Expected 1 row from staging, not 2 from dummy data
```

#### Test Case 2.3: Join Logic Correctness

*   **Purpose**: To verify that the `JOIN` conditions (`pca.pdp_context_id = pc.pdp_context_id` and `pc.access_point_id = ap.access_point_id`) correctly link records across the three source tables.
*   **Setup**:
    1.  Populate BigQuery staging tables with specific data:
        *   `pds_ta_pdp_context_assoc_stg`: Records with `pdp_context_id` values that exist, don't exist, or are NULL in `pds_ta_pdp_context_stg`.
        *   `pds_ta_pdp_context_stg`: Records with `pdp_context_id` and `access_point_id` values that exist, don't exist, or are NULL in `pds_ta_access_point_stg`.
        *   `pds_ta_access_point_stg`: Records with `access_point_id` values.
    2.  Ensure `v_datum` is set such that all records in the test data would otherwise pass date filters.
*   **Action**: Execute the BigQuery transformation SQL directly or via the DAG.
*   **Pass/Fail Criterion**:
    *   **Pass**: Only records where all three tables successfully join based on the specified `INNER JOIN` conditions are present in the target table. Records with non-matching join keys are correctly excluded.
    *   **Fail**: Records are missing due to incorrect join logic, or extra records are present due to unintended joins.

#### Test Case 2.4: Date Filter Logic (Comprehensive)

*   **Purpose**: To verify all date-based filtering conditions (`insert_at`, `modified_at`, `valid_from`, `valid_to`) are applied correctly relative to `v_datum`.
*   **Setup**:
    1.  Set `v_datum` to a specific date (e.g., '2023-01-15').
    2.  Populate BigQuery staging tables with records having various date combinations for `insert_at`, `modified_at`, `valid_from`, `valid_to` relative to '2023-01-15':
        *   `insert_at` values: `< v_datum`, `= v_datum`, `> v_datum`.
        *   `modified_at` values: `NULL`, `< v_datum`, `= v_datum`, `> v_datum`.
        *   `valid_from` values: `< v_datum`, `= v_datum`, `> v_datum`.
        *   `valid_to` values: `NULL`, `< v_datum`, `= v_datum`, `> v_datum`.
    3.  Ensure all records would otherwise pass join and other non-date filters.
*   **Action**: Execute the BigQuery transformation SQL directly or via the DAG.
*   **Pass/Fail Criterion**:
    *   **Pass**: Only records satisfying ALL of the following conditions are present in the target table:
        *   `pca.insert_at <= v_datum`
        *   `(pca.modified_at IS NULL OR pca.modified_at > v_datum)`
        *   `pca.valid_from <= v_datum`
        *   `(pca.valid_to IS NULL OR pca.valid_to > v_datum)`
        *   `pc.insert_at <= v_datum`
        *   `(pc.modified_at IS NULL OR pc.modified_at > v_datum)`
        *   `ap.insert_at <= v_datum`
        *   `(ap.modified_at IS NULL OR ap.modified_at > v_datum)`
    *   **Fail**: Any record that should have been included is missing, or any record that should have been excluded is present.

#### Test Case 2.5: `pc.is_production` Filter

*   **Purpose**: To verify that only records with `pc.is_production = 1` are included.
*   **Setup**:
    1.  Populate `pds_ta_pdp_context_stg` with records where `is_production` is `0`, `1`, and `NULL`.
    2.  Ensure all records would otherwise pass join and date filters.
*   **Action**: Execute the BigQuery transformation SQL directly or via the DAG.
*   **Pass/Fail Criterion**:
    *   **Pass**: Only records where `pds_ta_pdp_context_stg.is_production = 1` are present in the target table. Records with `0` or `NULL` are correctly excluded.
    *   **Fail**: Records with `is_production` not equal to `1` are included.

#### Test Case 2.6: `pca.cntrct_id IS NOT NULL` Filter

*   **Purpose**: To verify that records with a `NULL` `cntrct_id` in `pds_ta_pdp_context_assoc_stg` are correctly excluded.
*   **Setup**:
    1.  Populate `pds_ta_pdp_context_assoc_stg` with records where `cntrct_id` is `NULL` and where it is a valid string.
    2.  Ensure all records would otherwise pass join and date filters.
*   **Action**: Execute the BigQuery transformation SQL directly or via the DAG.
*   **Pass/Fail Criterion**:
    *   **Pass**: Only records where `pds_ta_pdp_context_assoc_stg.cntrct_id IS NOT NULL` are present in the target table.
    *   **Fail**: Records with `NULL` `cntrct_id` are included.

#### Test Case 2.7: NULL Handling in Output Columns

*   **Purpose**: To verify how `NULL` values in source columns that map to output columns are handled.
*   **Setup**:
    1.  Populate source tables such that `access_point_name` in `pds_ta_access_point_stg` is `NULL` for some records that would otherwise pass all filters.
    2.  Populate source tables such that `cntrct_id` in `pds_ta_pdp_context_assoc_stg` is `NOT NULL` (as per filter) but other columns involved in the join might be `NULL`.
*   **Action**: Execute the BigQuery transformation SQL directly or via the DAG.
*   **Pass/Fail Criterion**:
    *   **Pass**: `access_point_name` in the target table correctly reflects `NULL` if the source `pds_ta_access_point_stg.access_point_name` was `NULL`. `cntrct_id` is never `NULL` in the target due to the explicit filter.
    *   **Fail**: `NULL` values are unexpectedly converted to empty strings, default values, or cause errors.

#### Test Case 2.8: Edge Case - Empty Source Tables

*   **Purpose**: To ensure the job handles scenarios where one or more source tables are empty without error and produces an empty target table.
*   **Setup**:
    1.  Populate `dwtk_meldungen_stg` with data to derive a `v_datum`.
    2.  Leave `pds_ta_pdp_context_assoc_stg`, `pds_ta_pdp_context_stg`, or `pds_ta_access_point_stg` completely empty.
*   **Action**: Execute the BigQuery transformation SQL directly or via the DAG.
*   **Pass/Fail Criterion**:
    *   **Pass**: The job completes successfully without errors, and the target table `project.dataset.sof_ta_apn_ve` is empty.
    *   **Fail**: The job fails or produces unexpected results.

#### Test Case 2.9: Edge Case - No Matching Records

*   **Purpose**: To ensure the job handles scenarios where no records satisfy all join and filter conditions.
*   **Setup**:
    1.  Populate all BigQuery staging tables with data.
    2.  Configure the data such that, for example, all `is_production` values are `0`, or all `insert_at` dates are `> v_datum`.
*   **Action**: Execute the BigQuery transformation SQL directly or via the DAG.
*   **Pass/Fail Criterion**:
    *   **Pass**: The job completes successfully without errors, and the target table `project.dataset.sof_ta_apn_ve` is empty.
    *   **Fail**: The job fails or produces unexpected results.

---

### 3. External-System Replacements & Data Quality

#### Test Case 3.1: Source Ingestion Integrity (Oracle to BigQuery Staging)

*   **Purpose**: To verify that the data ingestion process from Oracle to BigQuery staging tables is accurate and complete. This is a prerequisite for the transformation tests.
*   **Setup**:
    1.  Select a representative set of data from each Oracle source table.
    2.  Trigger the ingestion process (e.g., Datastream, Data Fusion, custom script).
*   **Action**:
    1.  Query row counts from Oracle source tables and BigQuery staging tables.
    2.  Perform checksums or hash comparisons on key columns or entire tables (if feasible) for a sample of data.
*   **Pass/Fail Criterion**:
    *   **Pass**: Row counts match, and a sample of data (or checksums) confirms data integrity (no truncation, data type issues, or corruption) between Oracle sources and BigQuery staging.
    *   **Fail**: Discrepancies in row counts or data content.

#### Test Case 3.2: Target Schema Validation

*   **Purpose**: To ensure the BigQuery target table `project.dataset.sof_ta_apn_ve` has the correct schema (column names, data types, nullability) as expected from the transformation.
*   **Setup**: The BigQuery target table should exist, either pre-created or created by the job.
*   **Action**: Query the schema of `project.dataset.sof_ta_apn_ve` using BigQuery's `INFORMATION_SCHEMA` or client libraries.
*   **Pass/Fail Criterion**:
    *   **Pass**: The schema matches the expected definition (e.g., `cntrct_id` as `STRING`, `access_point_name` as `STRING`).
    *   **Fail**: Mismatch in column names, data types, or nullability.

```sql
-- BigQuery schema validation
SELECT column_name, data_type, is_nullable
FROM `project.dataset.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'sof_ta_apn_ve'
ORDER BY ordinal_position;
```

#### Test Case 3.3: Data Quality - Uniqueness of `cntrct_id`

*   **Purpose**: While not explicitly stated in the design, `cntrct_id` often implies uniqueness. This test checks for potential duplicate `cntrct_id` values in the target table, which could indicate an issue with the join logic or source data.
*   **Setup**: Populate BigQuery staging tables with data that, if transformation logic is flawed, could lead to duplicate `cntrct_id` values in the target.
*   **Action**: Execute the BigQuery transformation SQL directly or via the DAG. Then query for duplicate `cntrct_id` values in the target table.
*   **Pass/Fail Criterion**:
    *   **Pass**: No duplicate `cntrct_id` values are found in `project.dataset.sof_ta_apn_ve`.
    *   **Fail**: Duplicate `cntrct_id` values are found.

```sql
-- BigQuery check for duplicate cntrct_id
SELECT cntrct_id, COUNT(*)
FROM `project.dataset.sof_ta_apn_ve`
GROUP BY cntrct_id
HAVING COUNT(*) > 1;
```

---

### 4. Orchestration & Error Handling Tests

#### Test Case 4.1: Airflow DAG Execution and Task Dependencies

*   **Purpose**: To verify that the Cloud Composer DAG correctly orchestrates the job, with tasks executing in the specified order and dependencies respected.
*   **Setup**: A fully deployed `r_ausd_v_ta_apn_ve_dag` in Cloud Composer.
*   **Action**: Trigger the DAG manually or via a scheduled run. Observe the Airflow UI for task execution.
*   **Pass/Fail Criterion**:
    *   **Pass**: All tasks in the DAG (e.g., `ingest_source_data`, `derive_v_datum`, `transform_and_load`) execute successfully in the correct sequence, and the DAG run completes with a 'success' status.
    *   **Fail**: Any task fails, tasks execute out of order, or the DAG run gets stuck or fails.

#### Test Case 4.2: Error Handling - BigQuery SQL Failure

*   **Purpose**: To verify that if the core BigQuery transformation SQL fails (e.g., due to a syntax error, data type mismatch, or resource exhaustion), the Airflow DAG correctly catches the error, marks the task as failed, and triggers any configured alerts.
*   **Setup**:
    1.  Modify the BigQuery transformation SQL (e.g., introduce a deliberate syntax error, or try to insert a string into an INT column).
    2.  Ensure Cloud Monitoring alerts are configured for Airflow task failures.
*   **Action**: Trigger the Airflow DAG.
*   **Pass/Fail Criterion**:
    *   **Pass**: The BigQuery transformation task fails, the DAG run is marked as 'failed', and an alert is triggered in Cloud Monitoring (e.g., email, Slack notification).
    *   **Fail**: The DAG run completes successfully despite the SQL error, or the error is not properly logged/alerted.

#### Test Case 4.3: Logging and Monitoring Integration

*   **Purpose**: To verify that job execution details, warnings, and errors are correctly captured in Cloud Logging and that key metrics are available in Cloud Monitoring.
*   **Setup**:
    1.  Ensure Cloud Logging is enabled for the Composer environment and BigQuery.
    2.  Ensure Cloud Monitoring dashboards/alerts are configured for Airflow and BigQuery.
*   **Action**: Execute the Airflow DAG (both successful and failed runs).
*   **Pass/Fail Criterion**:
    *   **Pass**:
        *   Detailed logs for each Airflow task are visible in Cloud Logging.
        *   BigQuery job logs (query text, duration, bytes processed) are visible in Cloud Logging.
        *   Airflow DAG/task success/failure metrics are visible in Cloud Monitoring.
        *   Alerts (from Test Case 4.2) are correctly generated and visible.
    *   **Fail**: Logs are missing, incomplete, or metrics are not reported as expected.

---