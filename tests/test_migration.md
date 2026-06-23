As a senior data-migration QA engineer, I've designed a comprehensive suite of validation tests for the `DW.BERT_AUSD_V_TA_CNTRCT_TEMPL` job migration. These tests aim to ensure behavioural equivalence between the legacy Oracle/KornShell system and the new BigQuery/Airflow implementation.

The tests are categorised by the required validation areas: Output Parity, Transformation Correctness, External System Replacements, and Data Quality/Schema Assertions.

---

## Migration Validation Tests: DW.BERT_AUSD_V_TA_CNTRCT_TEMPL

### Test Setup Prerequisites

Before running any tests, ensure the following:

1.  **Legacy Environment:**
    *   Access to the legacy Oracle database.
    *   Ability to execute the legacy `d_ausd_v_ta_cntrct_templ.sql` script (or the full UC4 job) with controlled input data.
    *   Ability to query `sof$ta_cntrct_templ`, `cds$ta_cntrct_template@pcrs1`, `cds$ta_care_description@pcrs1`, and `isbert_schema.dwtk_meldungen` in Oracle.
2.  **Migrated Environment:**
    *   A GCP project with BigQuery enabled.
    *   The target BigQuery tables (`sof_ta_cntrct_templ`, `cds_ta_cntrct_template`, `cds_ta_care_description`, `isbert_schema.dwtk_meldungen`) are created with the DDLs provided.
    *   An Airflow environment (Cloud Composer) where the `dw_bert_ausd_v_ta_cntrct_temmpl.py` DAG is deployed and accessible.
    *   BigQuery connection `google_cloud_default` is configured correctly in Airflow.
3.  **Test Data Management:**
    *   A mechanism to load identical test data into both the legacy Oracle source tables and the BigQuery staging tables (`cds_ta_cntrct_template`, `cds_ta_care_description`, `isbert_schema.dwtk_meldungen`). This is crucial for output parity.
    *   A mechanism to clear/truncate target tables (`sof$ta_cntrct_templ` in Oracle, `sof_ta_cntrct_templ` in BigQuery) before each test run.
4.  **Comparison Tools:**
    *   Python environment with `pytest`, `google-cloud-bigquery` client, and an Oracle database connector (e.g., `cx_Oracle`).
    *   SQL client for direct queries in both environments.

---

### 1. Output Parity Tests

These tests ensure that given the same input data, the migrated job produces an identical output dataset to the legacy job.

#### Test Case 1.1: Full Data Parity (End-to-End)

*   **Purpose:** To verify that the final output table in BigQuery is an exact replica of the output table produced by the legacy Oracle job, covering all transformation logic.
*   **Setup:**
    1.  Prepare a comprehensive set of test data covering various scenarios for `cds$ta_cntrct_template`, `cds$ta_care_description`, and `isbert_schema.dwtk_meldungen`. Include cases for `NULL` values in `modified_at` and `valid_to`, records matching/not matching `is_production` and `language` filters, and records around the `v_datum` calculation.
    2.  Load this identical test data into:
        *   Legacy Oracle tables: `cds$ta_cntrct_template@pcrs1`, `cds$ta_care_description@pcrs1`, `isbert_schema.dwtk_meldungen`.
        *   BigQuery staging tables: `cds_ta_cntrct_template`, `cds_ta_care_description`, `isbert_schema.dwtk_meldungen`.
    3.  Truncate both `sof$ta_cntrct_templ` (Oracle) and `sof_ta_cntrct_templ` (BigQuery).
*   **Action:**
    1.  Execute the legacy job (or its core SQL `d_ausd_v_ta_cntrct_templ.sql`) in the Oracle environment.
    2.  Trigger the `dw_bert_ausd_v_ta_cntrct_temmpl` Airflow DAG in the GCP environment.
    3.  Wait for both jobs to complete successfully.
*   **Pass/Fail Criterion:**
    *   The number of rows in `sof$ta_cntrct_templ` (Oracle) must be exactly equal to the number of rows in `sof_ta_cntrct_templ` (BigQuery).
    *   A full data comparison (e.g., using `MINUS` in SQL or a programmatic comparison) between the two tables must yield no differences.

    ```python
    # Example Python/Pytest assertion
    import pandas as pd
    from google.cloud import bigquery
    import cx_Oracle # Assuming cx_Oracle for Oracle connection

    def test_full_data_parity(oracle_conn, bq_client):
        # 1. Get data from legacy Oracle
        oracle_query = "SELECT CNTRCT_TEMPLATE_ID, CDS_DESCRIPTION_ID, CDS_DESCRIPTION FROM sof$ta_cntrct_templ ORDER BY 1, 2, 3"
        oracle_df = pd.read_sql(oracle_query, oracle_conn)

        # 2. Get data from migrated BigQuery
        bq_query = "SELECT CNTRCT_TEMPLATE_ID, CDS_DESCRIPTION_ID, CDS_DESCRIPTION FROM `your_project.your_dataset.sof_ta_cntrct_templ` ORDER BY 1, 2, 3"
        bq_df = bq_client.query(bq_query).to_dataframe()

        # 3. Compare DataFrames
        pd.testing.assert_frame_equal(oracle_df, bq_df, check_dtype=False) # check_dtype=False if types might differ slightly (e.g., INT vs INT64)

        print("Full data parity test passed: Oracle and BigQuery outputs are identical.")
    ```

---

### 2. Transformation Correctness Tests

These tests focus on specific aspects of the SQL transformation logic, including joins, filters, and data type handling.

#### Test Case 2.1: `v_datum` Calculation Correctness

*   **Purpose:** To verify that the `v_datum` variable, which determines the processing date, is calculated identically in both environments.
*   **Setup:**
    1.  Load specific test data into `isbert_schema.dwtk_meldungen` in both Oracle and BigQuery.
        *   Scenario A: Multiple records for `BERT_DROP_TEMP_TABLE` with varying `timecreated`.
        *   Scenario B: No records for `BERT_DROP_TEMP_TABLE`.
        *   Scenario C: `timecreated` is NULL for `BERT_DROP_TEMP_TABLE` (if possible in schema).
    2.  Ensure `sof$ta_cntrct_templ` and `sof_ta_cntrct_templ` are truncated.
*   **Action:**
    1.  Manually or programmatically execute the `v_datum` calculation logic in both Oracle and BigQuery.
    2.  For the migrated job, trigger the Airflow DAG and inspect logs or use a BigQuery audit log to confirm the `v_datum` used.
*   **Pass/Fail Criterion:**
    *   The `v_datum` derived in Oracle must be identical to the `v_datum` derived and used in BigQuery for each scenario.

    ```sql
    -- Oracle SQL to get v_datum
    SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS v_datum_oracle
    FROM isbert_schema.dwtk_meldungen m
    WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';

    -- BigQuery SQL to get v_datum
    SELECT IFNULL(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101') AS v_datum_bigquery
    FROM `your_project.isbert_schema.dwtk_meldungen` m
    WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
    ```

#### Test Case 2.2: Join Logic

*   **Purpose:** To confirm that the `INNER JOIN` condition `ct.cds_description_id = cd.cds_description_id` behaves identically, correctly linking contract templates to their descriptions.
*   **Setup:**
    1.  Load `cds_ta_cntrct_template` and `cds_ta_care_description` with data that includes:
        *   Matching `cds_description_id` values.
        *   `cds_description_id` values in `ct` that have no match in `cd`.
        *   `cds_description_id` values in `cd` that have no match in `ct`.
    2.  Ensure other filter conditions are set to allow all relevant rows to pass for this test (e.g., set `v_datum` far in the future, `is_production=1`, `language=1`).
*   **Action:**
    1.  Execute both legacy and migrated jobs.
    2.  Query the output tables.
*   **Pass/Fail Criterion:**
    *   Only records where `cds_description_id` exists in both source tables should appear in the output.
    *   The count of joined records should be identical in both `sof$ta_cntrct_templ` and `sof_ta_cntrct_templ`.

#### Test Case 2.3: Filter Logic - `insert_at`

*   **Purpose:** Verify `ct.insert_at <= v_datum` filter works correctly, including boundary conditions.
*   **Setup:**
    1.  Set `v_datum` to a specific date (e.g., '20230115').
    2.  Load `cds_ta_cntrct_template` with records where `insert_at` is:
        *   Before `v_datum` (e.g., '20230114').
        *   Equal to `v_datum` (e.g., '20230115').
        *   After `v_datum` (e.g., '20230116').
    3.  Ensure other filters (`modified_at`, `valid_from`, `valid_to`, `is_production`, `language`) are set to pass all these test records.
*   **Action:** Execute both jobs.
*   **Pass/Fail Criterion:** Only records with `insert_at <= v_datum` should be present in both output tables.

#### Test Case 2.4: Filter Logic - `modified_at` (NULL Handling)

*   **Purpose:** Verify `(ct.modified_at IS NULL OR ct.modified_at > v_datum)` filter works correctly, especially with `NULL` values.
*   **Setup:**
    1.  Set `v_datum` to a specific date (e.g., '20230115').
    2.  Load `cds_ta_cntrct_template` with records where `modified_at` is:
        *   `NULL`.
        *   Before `v_datum` (e.g., '20230114').
        *   Equal to `v_datum` (e.g., '20230115').
        *   After `v_datum` (e.g., '20230116').
    3.  Ensure other filters are set to pass these test records.
*   **Action:** Execute both jobs.
*   **Pass/Fail Criterion:** Records with `modified_at IS NULL` or `modified_at > v_datum` should be present in both output tables, and records with `modified_at <= v_datum` (and not NULL) should be excluded.

#### Test Case 2.5: Filter Logic - `valid_from`

*   **Purpose:** Verify `ct.valid_from <= v_datum` filter works correctly.
*   **Setup:** Similar to Test Case 2.3, but for `valid_from`.
*   **Action:** Execute both jobs.
*   **Pass/Fail Criterion:** Only records with `valid_from <= v_datum` should be present in both output tables.

#### Test Case 2.6: Filter Logic - `valid_to` (NULL Handling)

*   **Purpose:** Verify `(ct.valid_to IS NULL OR ct.valid_to > v_datum)` filter works correctly, especially with `NULL` values.
*   **Setup:** Similar to Test Case 2.4, but for `valid_to`.
*   **Action:** Execute both jobs.
*   **Pass/Fail Criterion:** Records with `valid_to IS NULL` or `valid_to > v_datum` should be present in both output tables, and records with `valid_to <= v_datum` (and not NULL) should be excluded.

#### Test Case 2.7: Filter Logic - `is_production`

*   **Purpose:** Verify `ct.is_production = 1` filter works correctly.
*   **Setup:**
    1.  Load `cds_ta_cntrct_template` with records where `is_production` is `1` (TRUE) and `0` (FALSE).
    2.  Ensure other filters are set to pass these test records.
*   **Action:** Execute both jobs.
*   **Pass/Fail Criterion:** Only records with `is_production = 1` should be present in both output tables.

#### Test Case 2.8: Filter Logic - `language`

*   **Purpose:** Verify `cd.language = 1` filter works correctly.
*   **Setup:**
    1.  Load `cds_ta_care_description` with records where `language` is `1` and other values (e.g., `2`, `3`).
    2.  Ensure other filters are set to pass these test records.
*   **Action:** Execute both jobs.
*   **Pass/Fail Criterion:** Only records with `language = 1` should be present in both output tables.

#### Test Case 2.9: Data Type Handling

*   **Purpose:** To ensure that data types are correctly mapped and handled during the migration, preventing data loss or corruption.
*   **Setup:**
    1.  Load source tables with data that tests the range and precision of each column:
        *   `CNTRCT_TEMPLATE_ID`, `CDS_DESCRIPTION_ID`: Max `INT64` values.
        *   `CDS_DESCRIPTION`: Long strings, strings with special characters.
        *   Date fields (`insert_at`, `modified_at`, `valid_from`, `valid_to`): Dates at boundaries (e.g., '1900-01-01', '2099-12-31').
        *   `is_production`: `TRUE`/`FALSE` (or `1`/`0`).
    2.  Ensure all test records pass the filter conditions.
*   **Action:** Execute both jobs.
*   **Pass/Fail Criterion:**
    *   The data in the corresponding columns of `sof_ta_cntrct_templ` must exactly match the data in `sof$ta_cntrct_templ`.
    *   No truncation or data type conversion errors should occur.
    *   The schema of `sof_ta_cntrct_templ` should match the DDL provided.

#### Test Case 2.10: Edge Case - Empty Source Tables

*   **Purpose:** Verify the job handles scenarios where one or more source tables are empty gracefully.
*   **Setup:**
    1.  Scenario A: `cds_ta_cntrct_template` is empty, others populated.
    2.  Scenario B: `cds_ta_care_description` is empty, others populated.
    3.  Scenario C: Both `cds_ta_cntrct_template` and `cds_ta_care_description` are empty.
    4.  Scenario D: `isbert_schema.dwtk_meldungen` is empty.
    5.  Truncate target tables for each scenario.
*   **Action:** Execute both jobs for each scenario.
*   **Pass/Fail Criterion:**
    *   For Scenarios A, B, C: Both output tables (`sof$ta_cntrct_templ` and `sof_ta_cntrct_templ`) must be empty.
    *   For Scenario D: `v_datum` should default to '19000101' in both environments, and the output should reflect this (likely an empty table unless there's data from 1900). Both jobs should complete without error.

---

### 3. External-System Replacements Tests

These tests focus on the new mechanisms for sourcing data that previously came from external Oracle systems.

#### Test Case 3.1: Oracle Source Data Ingestion Accuracy

*   **Purpose:** To ensure that the BigQuery staging tables (`cds_ta_cntrct_template`, `cds_ta_care_description`, `isbert_schema.dwtk_meldungen`) accurately reflect the data from their respective Oracle source tables. This validates the upstream ingestion pipeline.
*   **Setup:**
    1.  Ensure the data ingestion pipeline from Oracle to BigQuery staging tables has run.
    2.  Identify a specific point in time or a specific dataset snapshot for comparison.
*   **Action:**
    1.  Query the Oracle source tables (`cds$ta_cntrct_template@pcrs1`, `cds$ta_care_description@pcrs1`, `isbert_schema.dwtk_meldungen`).
    2.  Query the corresponding BigQuery staging tables (`cds_ta_cntrct_template`, `cds_ta_care_description`, `isbert_schema.dwtk_meldungen`).
*   **Pass/Fail Criterion:**
    *   Row counts for each corresponding table pair must be identical.
    *   A full data comparison (e.g., using `MINUS` in SQL or programmatic comparison) between each Oracle source table and its BigQuery staging counterpart must yield no differences.
    *   Data types and NULLability in BigQuery staging tables should align with Oracle source tables.

    ```sql
    -- Example SQL for row count comparison (repeat for each table)
    SELECT COUNT(*) FROM cds$ta_cntrct_template@pcrs1;
    SELECT COUNT(*) FROM `your_project.your_dataset.cds_ta_cntrct_template`;

    -- Example SQL for data comparison (repeat for each table and adjust columns)
    SELECT * FROM (
        SELECT cntrct_template_id, cds_description_id, insert_at, modified_at, valid_from, valid_to, is_production FROM cds$ta_cntrct_template@pcrs1
        MINUS
        SELECT cntrct_template_id, cds_description_id, insert_at, modified_at, valid_from, valid_to, is_production FROM `your_project.your_dataset.cds_ta_cntrct_template`
    )
    UNION ALL
    SELECT * FROM (
        SELECT cntrct_template_id, cds_description_id, insert_at, modified_at, valid_from, valid_to, is_production FROM `your_project.your_dataset.cds_ta_cntrct_template`
        MINUS
        SELECT cntrct_template_id, cds_description_id, insert_at, modified_at, valid_from, valid_to, is_production FROM cds$ta_cntrct_template@pcrs1
    );
    -- Pass if both queries return 0 rows.
    ```

#### Test Case 3.2: Airflow DAG Orchestration and Task Execution

*   **Purpose:** To verify that the Airflow DAG correctly triggers the BigQuery transformation and handles basic Airflow features (logging, task status).
*   **Setup:**
    1.  Ensure the Airflow DAG `dw_bert_ausd_v_ta_cntrct_temmpl.py` is deployed.
    2.  Ensure BigQuery staging tables are populated with some valid test data.
    3.  Truncate `sof_ta_cntrct_templ`.
*   **Action:**
    1.  Manually trigger the `dw_bert_ausd_v_ta_cntrct_temmpl` DAG in Airflow.
    2.  Monitor the DAG run in the Airflow UI.
*   **Pass/Fail Criterion:**
    *   The DAG run must complete successfully (green status).
    *   The `mirror_contract_templates` task must complete successfully.
    *   Logs for the `mirror_contract_templates` task should indicate successful BigQuery job execution.
    *   The `sof_ta_cntrct_templ` table in BigQuery should be populated with data.

---

### 4. Data Quality / Row Count / Schema Assertions

These tests focus on the structural integrity and basic quality of the output data.

#### Test Case 4.1: Row Count Parity

*   **Purpose:** To quickly verify that the total number of records processed and inserted into the target table is consistent between legacy and migrated jobs.
*   **Setup:** Identical to Test Case 1.1 (Full Data Parity).
*   **Action:**
    1.  Execute both legacy and migrated jobs.
    2.  Query the row counts of the target tables.
*   **Pass/Fail Criterion:** The `COUNT(*)` from `sof$ta_cntrct_templ` (Oracle) must be exactly equal to `COUNT(*)` from `sof_ta_cntrct_templ` (BigQuery).

    ```sql
    -- Oracle
    SELECT COUNT(*) FROM sof$ta_cntrct_templ;
    -- BigQuery
    SELECT COUNT(*) FROM `your_project.your_dataset.sof_ta_cntrct_templ`;
    ```

#### Test Case 4.2: Schema Parity

*   **Purpose:** To ensure the schema (column names, data types, nullability) of the target BigQuery table matches the expected structure derived from the legacy Oracle table.
*   **Setup:**
    1.  Ensure `sof_ta_cntrct_templ` is created in BigQuery.
    2.  Obtain the schema of the legacy `sof$ta_cntrct_templ` table.
*   **Action:**
    1.  Query the schema of `sof_ta_cntrct_templ` in BigQuery.
*   **Pass/Fail Criterion:**
    *   Column names (`CNTRCT_TEMPLATE_ID`, `CDS_DESCRIPTION_ID`, `CDS_DESCRIPTION`) must match exactly.
    *   Data types (`INT64`, `INT64`, `STRING`) must match the DDL and be compatible with the legacy types.
    *   Nullability constraints should be consistent (e.g., if `CNTRCT_TEMPLATE_ID` is NOT NULL in Oracle, it should be `REQUIRED` in BigQuery, or at least not allow NULLs if the source data never produces them).

    ```python
    # Example Python/Pytest assertion
    from google.cloud import bigquery

    def test_target_schema_parity(bq_client):
        table_id = "your_project.your_dataset.sof_ta_cntrct_templ"
        table = bq_client.get_table(table_id)

        expected_schema = [
            bigquery.SchemaField("CNTRCT_TEMPLATE_ID", "INT64", mode="NULLABLE"), # Or REQUIRED if applicable
            bigquery.SchemaField("CDS_DESCRIPTION_ID", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("CDS_DESCRIPTION", "STRING", mode="NULLABLE"),
        ]

        # Compare field by field, as order might not be guaranteed
        actual_fields = {field.name: field for field in table.schema}
        expected_fields = {field.name: field for field in expected_schema}

        assert actual_fields.keys() == expected_fields.keys(), "Column names mismatch"

        for name, expected_field in expected_fields.items():
            actual_field = actual_fields[name]
            assert actual_field.field_type == expected_field.field_type, f"Type mismatch for {name}"
            assert actual_field.mode == expected_field.mode, f"Mode mismatch for {name}"

        print("Target schema parity test passed.")
    ```

#### Test Case 4.3: Data Integrity - Non-NULLable Columns

*   **Purpose:** To ensure that columns expected to be non-NULL (e.g., primary keys or essential identifiers) do not contain NULL values in the migrated target table.
*   **Setup:**
    1.  Populate source data such that `CNTRCT_TEMPLATE_ID` and `CDS_DESCRIPTION_ID` are always present and non-NULL for records that pass the filters.
    2.  Execute the migrated job.
*   **Action:** Query `sof_ta_cntrct_templ` for NULLs in these columns.
*   **Pass/Fail Criterion:** The count of `CNTRCT_TEMPLATE_ID IS NULL` and `CDS_DESCRIPTION_ID IS NULL` in `sof_ta_cntrct_templ` must be zero.

    ```sql
    SELECT COUNT(*) FROM `your_project.your_dataset.sof_ta_cntrct_templ` WHERE CNTRCT_TEMPLATE_ID IS NULL;
    SELECT COUNT(*) FROM `your_project.your_dataset.sof_ta_cntrct_templ` WHERE CDS_DESCRIPTION_ID IS NULL;
    -- Both queries must return 0.
    ```

#### Test Case 4.4: Data Uniqueness (if applicable)

*   **Purpose:** If `CNTRCT_TEMPLATE_ID` is expected to be unique in the target table, verify this constraint is maintained.
*   **Setup:**
    1.  Populate source data that, after transformation, should result in unique `CNTRCT_TEMPLATE_ID` values.
    2.  Execute the migrated job.
*   **Action:** Query `sof_ta_cntrct_templ` to check for duplicate `CNTRCT_TEMPLATE_ID` values.
*   **Pass/Fail Criterion:** The count of `CNTRCT_TEMPLATE_ID` where `COUNT(*) > 1` must be zero.

    ```sql
    SELECT CNTRCT_TEMPLATE_ID, COUNT(*)
    FROM `your_project.your_dataset.sof_ta_cntrct_templ`
    GROUP BY CNTRCT_TEMPLATE_ID
    HAVING COUNT(*) > 1;
    -- This query must return 0 rows.
    ```