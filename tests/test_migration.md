As a senior data-migration QA engineer, I've analyzed the migration design for `DW.BERT_AUSD_BP_TA_BCP_ICCID` to Google Cloud Platform. The following test cases are designed to ensure the migrated BigQuery/Airflow job is behaviourally equivalent to the legacy Oracle/KornShell job.

---

## Migration Validation Tests: DW.BERT_AUSD_BP_TA_BCP_ICCID

### General Test Setup & Assumptions

*   **Environment Access:** Full read/write access to both the legacy Oracle database (or a representative snapshot) and the target BigQuery environment. Access to the legacy job execution environment and the new Cloud Composer (Airflow) environment.
*   **Data Synchronization:** It is assumed that the source tables (`DWTK_MELDUNGEN`, `SOF$TA_BPR_BCP`, `SOF$TA_ICCID_VERTRAG`) in BigQuery are synchronized with their Oracle counterparts *before* the migrated job runs. This synchronization mechanism (e.g., Datastream) should have its own validation.
*   **Golden Dataset:** For output parity tests, a "golden dataset" approach will be used. This involves preparing specific input data scenarios in both environments and comparing the final output.
*   **Legacy Job Execution:** A mechanism to execute the legacy job on demand and capture its output (e.g., via `SQL*Plus` spooling or direct database query).
*   **Migrated Job Execution:** A mechanism to trigger the Airflow DAG on demand and monitor its execution.
*   **Table Naming:** Legacy Oracle tables are referred to as `isbert_schema.DWTK_MELDUNGEN`, `sof$ta_bpr_bcp`, `sof$ta_iccid_vertrag`, `sof$ta_bcp_iccid`. Migrated BigQuery tables are referred to as `project.dataset.dwtk_meldungen`, `project.dataset.sof_ta_bpr_bcp`, `project.dataset.sof_ta_iccid_vertrag`, `project.dataset.sof_ta_bcp_iccid`.

---

### 1. Output Parity Tests

#### Test Case 1.1: Full Data Output Parity (Golden Dataset)

*   **Purpose:** To verify that the migrated job produces an identical final output table (`sof_ta_bcp_iccid`) compared to the legacy job when given the same input data. This is the primary end-to-end validation.
*   **Setup:**
    1.  Identify a representative "golden dataset" for `sof$ta_bpr_bcp`, `sof$ta_iccid_vertrag`, and `dwtk_meldungen` that covers typical scenarios, including various join conditions, NULLs, and duplicates.
    2.  Load this exact dataset into the legacy Oracle source tables.
    3.  Load this exact dataset into the BigQuery source tables (`project.dataset.sof_ta_bpr_bcp`, `project.dataset.sof_ta_iccid_vertrag`, `project.dataset.dwtk_meldungen`).
    4.  Ensure both target tables (`sof$ta_bcp_iccid` in Oracle and BigQuery) are empty before execution.
*   **Action:**
    1.  Execute the legacy `DW.BERT_AUSD_BP_TA_BCP_ICCID` job.
    2.  Execute the migrated `dw_bert_ausd_bp_ta_bcp_iccid` Airflow DAG.
    3.  Extract all data from the legacy Oracle `sof$ta_bcp_iccid` table.
    4.  Extract all data from the BigQuery `project.dataset.sof_ta_bcp_iccid` table.
*   **Pass/Fail Criterion:**
    *   The row count of the BigQuery target table must exactly match the row count of the Oracle target table.
    *   After sorting both datasets by a unique key (e.g., `CNTRCT_ID`, `BPR_ID`, `CNTRCT_ID_REF`, `TN_ICCID`, `TN_IMSI_HLR`), every row and every column value in the BigQuery output must exactly match the Oracle output.

    ```python
    # Example Python/Pytest assertion for data comparison
    import pandas as pd
    from google.cloud import bigquery
    import cx_Oracle # Assuming Oracle connection

    def test_output_parity_golden_dataset():
        # --- Setup (simplified for example) ---
        # Assume source data is already loaded into both environments
        # Assume target tables are truncated

        # --- Action ---
        # 1. Execute Legacy Job (manual or via automation)
        #    ... wait for legacy job to complete ...

        # 2. Execute Migrated Airflow DAG (trigger via Airflow API or CLI)
        #    ... wait for Airflow DAG to complete ...

        # 3. Extract data from Oracle
        oracle_conn_str = "user/pass@host:port/service_name"
        oracle_query = "SELECT CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_ICCID, TN_IMSI_HLR FROM sof$ta_bcp_iccid ORDER BY 1,2,3,4,5"
        with cx_Oracle.connect(oracle_conn_str) as connection:
            oracle_df = pd.read_sql(oracle_query, connection)

        # 4. Extract data from BigQuery
        bq_client = bigquery.Client()
        bq_query = "SELECT CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_ICCID, TN_IMSI_HLR FROM `project.dataset.sof_ta_bcp_iccid` ORDER BY 1,2,3,4,5"
        bq_df = bq_client.query(bq_query).to_dataframe()

        # --- Pass/Fail Criterion ---
        assert len(oracle_df) == len(bq_df), \
            f"Row count mismatch: Oracle={len(oracle_df)}, BigQuery={len(bq_df)}"

        # Convert columns to string for robust comparison, especially for NULLs/empty strings
        oracle_df = oracle_df.astype(str)
        bq_df = bq_df.astype(str)

        pd.testing.assert_frame_equal(oracle_df, bq_df, check_dtype=True, check_exact=True)
        print("Output parity test passed: Row counts and data content match exactly.")
    ```

### 2. Transformation Correctness Tests

#### Test Case 2.1: Join Logic - Matching Records

*   **Purpose:** To verify that the `JOIN` condition `bp.cntrct_id_ref = ic.cntrct_id` correctly identifies and joins matching records between `sof$ta_bpr_bcp` and `sof$ta_iccid_vertrag`.
*   **Setup:**
    1.  Populate `sof$ta_bpr_bcp` with records where `cntrct_id_ref` has corresponding matches in `sof$ta_iccid_vertrag.cntrct_id`.
    2.  Populate `sof$ta_iccid_vertrag` with records that match `cntrct_id_ref` from `sof$ta_bpr_bcp`.
    3.  Ensure no other records exist that could interfere with the join.
    4.  Ensure both target tables are empty.
*   **Action:** Execute both legacy and migrated jobs.
*   **Pass/Fail Criterion:**
    *   The number of rows in the target table must equal the number of expected joined records.
    *   The `CNTRCT_ID_REF` in the output must match `ic.cntrct_id` from the joined `sof$ta_iccid_vertrag` record.
    *   All other columns (`CNTRCT_ID`, `BPR_ID`, `TN_ICCID`, `TN_IMSI_HLR`) must contain the correct values from the joined source records.
    *   Output parity (row count and content) between legacy and migrated jobs for this specific scenario.

#### Test Case 2.2: Join Logic - Non-Matching Records

*   **Purpose:** To verify that records from `sof$ta_bpr_bcp` with `cntrct_id_ref` values that do *not* have a corresponding match in `sof$ta_iccid_vertrag.cntrct_id` are correctly excluded from the output.
*   **Setup:**
    1.  Populate `sof$ta_bpr_bcp` with records where `cntrct_id_ref` has *no* corresponding matches in `sof$ta_iccid_vertrag.cntrct_id`.
    2.  Populate `sof$ta_iccid_vertrag` with records that are not referenced by `sof$ta_bpr_bcp`.
    3.  Ensure both target tables are empty.
*   **Action:** Execute both legacy and migrated jobs.
*   **Pass/Fail Criterion:**
    *   The target table (`sof_ta_bcp_iccid`) must be empty in both legacy and migrated environments.
    *   Output parity (row count and content) between legacy and migrated jobs for this specific scenario.

#### Test Case 2.3: `DISTINCT` Clause Handling

*   **Purpose:** To verify that the `SELECT DISTINCT` clause correctly eliminates duplicate rows that might result from the join operation.
*   **Setup:**
    1.  Populate `sof$ta_bpr_bcp` and `sof$ta_iccid_vertrag` such that the join operation would produce multiple identical output rows *before* the `DISTINCT` is applied. For example, `sof$ta_bpr_bcp` has `(1, 'BPR1', 100)` and `sof$ta_iccid_vertrag` has `(100, 'ICCID1', 'IMSI1')` and `(100, 'ICCID1', 'IMSI1')` (duplicate `iccid_vertrag` records for the same `cntrct_id`).
    2.  Ensure both target tables are empty.
*   **Action:** Execute both legacy and migrated jobs.
*   **Pass/Fail Criterion:**
    *   The target table must contain only unique rows, and the count should reflect the number of unique combinations after the join.
    *   Output parity (row count and content) between legacy and migrated jobs for this specific scenario.

#### Test Case 2.4: `v_datum` Derivation (Orchestration Logic)

*   **Purpose:** To verify that the `v_datum` variable, derived from `DWTK_MELDUNGEN`, is calculated correctly by the migrated Airflow Python task, matching the legacy KornShell logic. This variable is not part of the final output table but is an internal calculation.
*   **Setup:**
    1.  **Scenario A: `DWTK_MELDUNGEN` has data.** Populate `DWTK_MELDUNGEN` with multiple `timecreated` values, including a `MAX` value.
    2.  **Scenario B: `DWTK_MELDUNGEN` is empty.** Ensure `DWTK_MELDUNGEN` is empty.
    3.  **Scenario C: `DWTK_MELDUNGEN.timecreated` is NULL.** Populate `DWTK_MELDUNGEN` with records where `timecreated` is NULL.
*   **Action:**
    1.  For each scenario, execute the legacy KornShell script up to the point where `v_datum` is determined, and capture its value (e.g., via logging or a temporary file).
    2.  For each scenario, execute the migrated Airflow DAG and capture the `v_datum` value calculated by the Python task (e.g., via Airflow task logs).
*   **Pass/Fail Criterion:**
    *   **Scenario A:** The `v_datum` captured from the migrated job's logs must match `FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated))` from the legacy `DWTK_MELDUNGEN` table.
    *   **Scenario B & C:** The `v_datum` captured from the migrated job's logs must be '19000101', matching the `COALESCE` fallback.

    ```python
    # Example Python/Pytest assertion for v_datum derivation
    import pendulum
    from google.cloud import bigquery

    def get_expected_v_datum(bq_client, project_id, dataset_id):
        query = f"""
        SELECT
          COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101') AS v_datum
        FROM `{project_id}.{dataset_id}.dwtk_meldungen` AS m
        """
        result = bq_client.query(query).to_dataframe()
        return result['v_datum'][0]

    def test_v_datum_derivation():
        bq_client = bigquery.Client()
        project_id = "your-gcp-project"
        dataset_id = "your_dataset"

        # Scenario A: DWTK_MELDUNGEN has data
        # Assume DWTK_MELDUNGEN is populated with specific timecreated values
        # (e.g., max timecreated = '2023-10-26 10:00:00 UTC')
        expected_v_datum_A = get_expected_v_datum(bq_client, project_id, dataset_id)
        # Trigger Airflow DAG and capture v_datum from logs
        # For this example, we'll simulate the captured value
        captured_v_datum_A = "20231026" # This would come from Airflow logs
        assert captured_v_datum_A == expected_v_datum_A, \
            f"v_datum mismatch for Scenario A: Expected {expected_v_datum_A}, Got {captured_v_datum_A}"

        # Scenario B: DWTK_MELDUNGEN is empty
        # Clear DWTK_MELDUNGEN table
        bq_client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.dwtk_meldungen`").result()
        expected_v_datum_B = get_expected_v_datum(bq_client, project_id, dataset_id)
        captured_v_datum_B = "19000101" # This would come from Airflow logs
        assert captured_v_datum_B == expected_v_datum_B, \
            f"v_datum mismatch for Scenario B: Expected {expected_v_datum_B}, Got {captured_v_datum_B}"

        # Scenario C: DWTK_MELDUNGEN.timecreated is NULL
        # Populate DWTK_MELDUNGEN with a row where timecreated is NULL
        bq_client.query(f"INSERT INTO `{project_id}.{dataset_id}.dwtk_meldungen` (timecreated) VALUES (NULL)").result()
        expected_v_datum_C = get_expected_v_datum(bq_client, project_id, dataset_id)
        captured_v_datum_C = "19000101" # This would come from Airflow logs
        assert captured_v_datum_C == expected_v_datum_C, \
            f"v_datum mismatch for Scenario C: Expected {expected_v_datum_C}, Got {captured_v_datum_C}"
    ```

#### Test Case 2.5: NULL Handling in Join Keys

*   **Purpose:** To verify that records with `NULL` values in the join key (`cntrct_id_ref` in `sof$ta_bpr_bcp` or `cntrct_id` in `sof$ta_iccid_vertrag`) are correctly handled (i.e., excluded from the `INNER JOIN`).
*   **Setup:**
    1.  Populate `sof$ta_bpr_bcp` with records where `cntrct_id_ref` is `NULL`.
    2.  Populate `sof$ta_iccid_vertrag` with records where `cntrct_id` is `NULL`.
    3.  Include some valid matching records to ensure the job still processes correctly.
    4.  Ensure both target tables are empty.
*   **Action:** Execute both legacy and migrated jobs.
*   **Pass/Fail Criterion:**
    *   The output table must *not* contain any records that resulted from joining on `NULL` values.
    *   The row count and content of the output table must match between legacy and migrated jobs, reflecting only the valid joins.

#### Test Case 2.6: Empty Source Tables

*   **Purpose:** To verify the job's behavior when one or both source tables (`sof$ta_bpr_bcp`, `sof$ta_iccid_vertrag`) are empty.
*   **Setup:**
    1.  **Scenario A:** `sof$ta_bpr_bcp` is empty, `sof$ta_iccid_vertrag` has data.
    2.  **Scenario B:** `sof$ta_bpr_bcp` has data, `sof$ta_iccid_vertrag` is empty.
    3.  **Scenario C:** Both `sof$ta_bpr_bcp` and `sof$ta_iccid_vertrag` are empty.
    4.  Ensure both target tables are empty for each scenario.
*   **Action:** For each scenario, execute both legacy and migrated jobs.
*   **Pass/Fail Criterion:**
    *   For all scenarios, the target table (`sof_ta_bcp_iccid`) must be empty in both legacy and migrated environments.
    *   The job must complete successfully without errors in both environments.

#### Test Case 2.7: Data Type Handling and Edge Values

*   **Purpose:** To ensure that data types are correctly mapped from Oracle to BigQuery and that edge values (e.g., maximum length strings, minimum/maximum numbers) are handled without truncation or conversion errors.
*   **Setup:**
    1.  Populate `sof$ta_bpr_bcp` and `sof$ta_iccid_vertrag` with records containing:
        *   Maximum allowed length strings for `TN_ICCID`, `TN_IMSI_HLR`.
        *   Numeric values at their boundaries (if applicable for `CNTRCT_ID`, `BPR_ID`, `CNTRCT_ID_REF`).
        *   Special characters in string fields (if allowed by Oracle schema).
    2.  Ensure both target tables are empty.
*   **Action:** Execute both legacy and migrated jobs.
*   **Pass/Fail Criterion:**
    *   The output data in BigQuery must exactly match the Oracle output, including all characters and numeric precision. No data truncation or corruption should occur.
    *   Output parity (row count and content) between legacy and migrated jobs for this specific scenario.

### 3. External-System Replacements Tests

#### Test Case 3.1: Oracle to BigQuery Source Data Synchronization

*   **Purpose:** To verify that the BigQuery source tables (`dwtk_meldungen`, `sof_ta_bpr_bcp`, `sof_ta_iccid_vertrag`) accurately reflect the state of their Oracle counterparts *before* the migrated job runs. This validates the data replication mechanism.
*   **Setup:**
    1.  Introduce a known change (e.g., insert a new record, update an existing record) into one of the Oracle source tables (`sof$ta_bpr_bcp`).
    2.  Wait for the data synchronization mechanism (e.g., Datastream) to complete.
*   **Action:**
    1.  Query the modified Oracle source table.
    2.  Query the corresponding BigQuery source table.
*   **Pass/Fail Criterion:**
    *   The content of the BigQuery source table must exactly match the Oracle source table, including the recent change.
    *   This test should be performed for each source table and for different types of data changes (insert, update, delete).

#### Test Case 3.2: Airflow Orchestration and Parameter Handling

*   **Purpose:** To verify that the Airflow DAG correctly orchestrates the job, including parameter parsing (Stichtag, Wiederanlaufwert), environment setup, and triggering the BigQuery SQL execution.
*   **Setup:**
    1.  Define specific `Stichtag` and `Wiederanlaufwert` parameters for the Airflow DAG run (e.g., via Airflow UI, config).
    2.  Ensure the BigQuery environment is accessible from Airflow.
*   **Action:**
    1.  Trigger the Airflow DAG with the specified parameters.
    2.  Monitor Airflow task logs for parameter values and execution flow.
    3.  Verify the BigQuery job is initiated and completes successfully.
*   **Pass/Fail Criterion:**
    *   The Airflow task logs must show correct parsing and usage of `Stichtag` and `Wiederanlaufwert` (even if not directly used in the final `INSERT`, their correct handling is part of the refactored KornShell logic).
    *   The BigQuery job must be successfully executed by the Airflow task.
    *   No errors related to environment setup or BigQuery connection in Airflow logs.

#### Test Case 3.3: `TRUNCATE` Operation Equivalence

*   **Purpose:** To verify that the BigQuery `TRUNCATE TABLE` operation (or `WRITE_TRUNCATE` disposition) behaves identically to the Oracle `DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_bcp_iccid REUSE STORAGE')`.
*   **Setup:**
    1.  Populate the target table (`sof$ta_bcp_iccid` in Oracle, `project.dataset.sof_ta_bcp_iccid` in BigQuery) with some test data.
    2.  Ensure the source tables are also populated to allow the `INSERT` to run.
*   **Action:**
    1.  Execute the legacy job.
    2.  Execute the migrated job.
    3.  Immediately after the `TRUNCATE` step (but before the `INSERT`), verify the table is empty in both environments.
*   **Pass/Fail Criterion:**
    *   Both the legacy and migrated target tables must be empty after their respective truncation steps, and before the data load.
    *   The job must complete successfully, indicating the truncation did not cause any unexpected errors.

### 4. Data Quality / Row Count / Schema Assertions

#### Test Case 4.1: Row Count Parity

*   **Purpose:** To confirm that the total number of records processed and loaded into the target table is consistent between the legacy and migrated jobs for various data scenarios.
*   **Setup:**
    1.  Prepare multiple datasets for source tables:
        *   A typical, representative dataset.
        *   A dataset with many matching records.
        *   A dataset with few matching records.
        *   A dataset with no matching records.
    2.  Ensure both target tables are empty for each scenario.
*   **Action:** For each dataset, execute both legacy and migrated jobs.
*   **Pass/Fail Criterion:**
    *   The final row count of `project.dataset.sof_ta_bcp_iccid` must exactly match the row count of `sof$ta_bcp_iccid` for every scenario.

    ```sql
    -- SQL Assertion for row count
    SELECT COUNT(*) FROM sof$ta_bcp_iccid; -- Legacy Oracle
    SELECT COUNT(*) FROM `project.dataset.sof_ta_bcp_iccid`; -- Migrated BigQuery
    ```

#### Test Case 4.2: Schema Parity

*   **Purpose:** To verify that the BigQuery target table schema (`project.dataset.sof_ta_bcp_iccid`) precisely matches the legacy Oracle target table schema (`sof$ta_bcp_iccid`) in terms of column names, data types, and nullability.
*   **Setup:**
    1.  Access to Oracle schema metadata (e.g., `ALL_TAB_COLUMNS`).
    2.  Access to BigQuery schema metadata (e.g., `INFORMATION_SCHEMA.COLUMNS`).
*   **Action:**
    1.  Extract the schema definition for `sof$ta_bcp_iccid` from Oracle.
    2.  Extract the schema definition for `project.dataset.sof_ta_bcp_iccid` from BigQuery.
*   **Pass/Fail Criterion:**
    *   All column names must match exactly (case-sensitivity might need to be considered if Oracle is case-insensitive and BigQuery is not, or vice-versa).
    *   Data types must be functionally equivalent (e.g., `NUMBER(10)` in Oracle maps to `INT64` in BigQuery, `VARCHAR2(255)` to `STRING`).
    *   Nullability constraints (NOT NULL vs. NULLABLE) must match for all columns.
    *   Column order should ideally match, though not strictly a functional requirement.

    ```python
    # Example Python/Pytest assertion for schema comparison
    def test_schema_parity():
        # --- Setup (simplified for example) ---
        # Assume functions to get schema from Oracle and BigQuery
        oracle_schema = get_oracle_schema("sof$ta_bcp_iccid") # Returns list of dicts: [{'name': 'COL1', 'type': 'VARCHAR2(10)', 'nullable': False}, ...]
        bq_schema = get_bigquery_schema("project.dataset.sof_ta_bcp_iccid") # Returns list of dicts: [{'name': 'COL1', 'type': 'STRING', 'mode': 'REQUIRED'}, ...]

        # --- Action & Pass/Fail Criterion ---
        assert len(oracle_schema) == len(bq_schema), "Column count mismatch"

        for oracle_col in oracle_schema:
            bq_col = next((c for c in bq_schema if c['name'].upper() == oracle_col['name'].upper()), None)
            assert bq_col is not None, f"Column {oracle_col['name']} not found in BigQuery schema"

            # Map Oracle types to expected BigQuery types
            expected_bq_type = map_oracle_type_to_bq(oracle_col['type'])
            assert bq_col['type'] == expected_bq_type, \
                f"Type mismatch for {oracle_col['name']}: Oracle '{oracle_col['type']}' -> Expected BQ '{expected_bq_type}', Got BQ '{bq_col['type']}'"

            # Map Oracle nullability to BigQuery mode
            expected_bq_mode = 'REQUIRED' if not oracle_col['nullable'] else 'NULLABLE'
            assert bq_col['mode'] == expected_bq_mode, \
                f"Nullability mismatch for {oracle_col['name']}: Oracle '{oracle_col['nullable']}' -> Expected BQ '{expected_bq_mode}', Got BQ '{bq_col['mode']}'"

        print("Schema parity test passed.")
    ```

#### Test Case 4.3: Data Integrity - Non-Nullable Columns

*   **Purpose:** To ensure that columns defined as `NOT NULL` in the target schema (e.g., `CNTRCT_ID`, `BPR_ID`) do not contain `NULL` values after the migration, maintaining data integrity.
*   **Setup:**
    1.  Populate source tables with data that should result in valid, non-NULL values for these columns.
    2.  Also, include edge cases where source data might *potentially* lead to NULLs if the transformation is incorrect (e.g., if `CNTRCT_ID` was nullable in `sof$ta_bpr_bcp`).
*   **Action:** Execute the migrated job.
*   **Pass/Fail Criterion:**
    *   A query against the BigQuery target table for `NULL` values in `CNTRCT_ID`, `BPR_ID`, `CNTRCT_ID_REF`, `TN_ICCID`, `TN_IMSI_HLR` should return zero rows.

    ```sql
    -- SQL Assertion for non-nullable columns
    SELECT COUNT(*)
    FROM `project.dataset.sof_ta_bcp_iccid`
    WHERE CNTRCT_ID IS NULL
       OR BPR_ID IS NULL
       OR CNTRCT_ID_REF IS NULL
       OR TN_ICCID IS NULL
       OR TN_IMSI_HLR IS NULL;
    -- Expected result: 0
    ```

---