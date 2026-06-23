As a senior data-migration QA engineer, I've reviewed the migration design for `DW.BERT_AUSD_BP_TA_BCP_MSISDN`. The following test cases are designed to ensure the migrated job is behaviourally equivalent to its legacy counterpart, covering output parity, transformation correctness, external system replacements, and data quality assertions.

---

## Migration Validation Tests for DW.BERT_AUSD_BP_TA_BCP_MSISDN

### Test Environment Setup (Prerequisites for all tests)

*   **Legacy Environment:** Access to the legacy Oracle database (read-only) where `sof$ta_bpr_bcp`, `sof$ta_rn_vertrag`, `sof$ta_bcp_msisdn`, and `isbert_schema.dwtk_meldungen` reside.
*   **GCP Environment:**
    *   A BigQuery project (`your-gcp-project-id`) with datasets (`your_bigquery_dataset`, `isbert_schema`) and the corresponding tables (`sof_ta_bpr_bcp`, `sof_ta_rn_vertrag`, `sof_ta_bcp_msisdn`, `dwtk_meldungen`) created as per the provided DDL.
    *   A Dataproc cluster (`your-dataproc-cluster-name`) configured and running.
    *   Cloud Composer environment with the `dw_bert_ausd_bp_ta_bcp_msisdn.py` DAG deployed.
    *   The `r_ausd_bp_ta_bcp_msisdn.py` Python script uploaded to the specified GCS path (`gs://your-gcs-bucket/pyspark_scripts/r_ausd_bp_ta_bcp_msisdn.py`).
    *   Service accounts used by Dataproc and Airflow have appropriate IAM permissions for BigQuery read/write, GCS read, and Dataproc job submission.
*   **Test Data:** A set of representative test data (including edge cases like NULLs, duplicates, no matches) prepared for `sof$ta_bpr_bcp`, `sof$ta_rn_vertrag`, and `isbert_schema.dwtk_meldungen` in both legacy Oracle and BigQuery. This data should be identical in both source systems for direct comparison.

---

### 1. Output Parity Tests

#### Test Case 1.1: Full Data Parity (Golden Record Comparison)

*   **Purpose:** To verify that the migrated job produces an identical set of output records in the target BigQuery table as the legacy job produces in its Oracle target table, given the same input data. This is the ultimate end-to-end validation.
*   **Setup:**
    1.  Load a comprehensive set of test data into the legacy Oracle source tables (`sof$ta_bpr_bcp`, `sof$ta_rn_vertrag`, `isbert_schema.dwtk_meldungen`).
    2.  Load the *exact same* test data into the BigQuery source tables (`sof_ta_bpr_bcp`, `sof_ta_rn_vertrag`, `isbert_schema.dwtk_meldungen`).
    3.  Ensure the `isbert_schema.dwtk_meldungen` table in both environments contains a record for `job_kennung = 'BERT_DROP_TEMP_TABLE'` with a `timecreated` value that will be picked up by the `MAX` function.
    4.  Record the `v_datum` value that would be derived from `dwtk_meldungen` for this test run (or explicitly pass it if testing `--stichtag`).
*   **Action:**
    1.  Execute the legacy UC4 job `DW.BERT_AUSD_BP_TA_BCP_MSISDN`.
    2.  Execute the migrated Airflow DAG `dw_bert_ausd_bp_ta_bcp_msisdn`.
*   **Pass/Fail Criterion:**
    *   The number of rows in the legacy Oracle `sof$ta_bcp_msisdn` table must be exactly equal to the number of rows in the BigQuery `sof_ta_bcp_msisdn` table.
    *   A row-by-row comparison of the data in both target tables must show 100% equivalence across all columns.

    ```python
    # Example Python (pytest) assertion for data parity
    import pandas as pd
    from google.cloud import bigquery
    import cx_Oracle # Assuming cx_Oracle for Oracle connection

    def test_full_data_parity(gcp_project_id, bq_dataset, oracle_conn_str):
        bq_client = bigquery.Client(project=gcp_project_id)
        oracle_conn = cx_Oracle.connect(oracle_conn_str)

        # Fetch data from BigQuery
        bq_query = f"""
            SELECT CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_TEL_MSISDN
            FROM `{gcp_project_id}.{bq_dataset}.sof_ta_bcp_msisdn`
            ORDER BY CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_TEL_MSISDN
        """
        bq_df = bq_client.query(bq_query).to_dataframe()

        # Fetch data from Oracle
        oracle_query = """
            SELECT CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_TEL_MSISDN
            FROM sof$ta_bcp_msisdn
            ORDER BY CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_TEL_MSISDN
        """
        oracle_df = pd.read_sql(oracle_query, oracle_conn)

        # Close Oracle connection
        oracle_conn.close()

        # Convert column names to be case-insensitive for comparison if needed
        bq_df.columns = bq_df.columns.str.upper()
        oracle_df.columns = oracle_df.columns.str.upper()

        # Assert row counts
        assert len(bq_df) == len(oracle_df), \
            f"Row count mismatch: BigQuery has {len(bq_df)} rows, Oracle has {len(oracle_df)} rows."

        # Assert data content
        pd.testing.assert_frame_equal(
            bq_df.reset_index(drop=True),
            oracle_df.reset_index(drop=True),
            check_dtype=False, # Data types might differ slightly (e.g., STRING vs VARCHAR2)
            check_exact=False, # Allow for floating point differences if applicable (not here)
            obj="Target table data"
        )
    ```

---

### 2. Transformation Correctness Tests

#### Test Case 2.1: `v_datum` Retrieval Logic

*   **Purpose:** To verify that the Python script correctly retrieves the `v_datum` (processing date) from the `dwtk_meldungen` table, handling `MAX`, `COALESCE`, and the default value '19000101'.
*   **Setup:**
    1.  **Scenario A (Normal):** Insert multiple records into `isbert_schema.dwtk_meldungen` with `job_kennung = 'BERT_DROP_TEMP_TABLE'` and varying `timecreated` values. Ensure one is the latest.
    2.  **Scenario B (No matching job_kennung):** Ensure `isbert_schema.dwtk_meldungen` contains no records for `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
    3.  **Scenario C (NULL timecreated):** Insert a record into `isbert_schema.dwtk_meldungen` with `job_kennung = 'BERT_DROP_TEMP_TABLE'` and `timecreated = NULL` (if BigQuery schema allows, otherwise simulate by having only NULLs if possible, or skip this specific sub-scenario if not applicable).
*   **Action:**
    1.  For each scenario, execute the Python script `r_ausd_bp_ta_bcp_msisdn.py` directly (or via Airflow) with no `--stichtag` argument.
    2.  Capture the `v_datum` value logged by the script.
*   **Pass/Fail Criterion:**
    *   **Scenario A:** The logged `v_datum` must match `FORMAT_DATE('%Y%m%d', MAX(timecreated))` for `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
    *   **Scenario B & C:** The logged `v_datum` must be '19000101'.

    ```python
    # Example Python (pytest) assertion for v_datum retrieval
    import subprocess
    import json
    from datetime import datetime

    def setup_dwtk_meldungen(bq_client, gcp_project_id, metadata_dataset, records):
        """Helper to set up dwtk_meldungen table."""
        table_id = f"{gcp_project_id}.{metadata_dataset}.dwtk_meldungen"
        bq_client.query(f"TRUNCATE TABLE `{table_id}`").result()
        if records:
            rows_to_insert = [
                {"timecreated": r["timecreated"].isoformat() if r["timecreated"] else None, "job_kennung": r["job_kennung"]}
                for r in records
            ]
            bq_client.insert_rows_json(table_id, rows_to_insert)

    def test_v_datum_retrieval(bq_client, gcp_project_id, bq_dataset, metadata_dataset, python_script_path):
        # Scenario A: Normal retrieval
        setup_dwtk_meldungen(bq_client, gcp_project_id, metadata_dataset, [
            {"timecreated": datetime(2023, 1, 1, 10, 0, 0), "job_kennung": "OTHER_JOB"},
            {"timecreated": datetime(2023, 1, 5, 12, 30, 0), "job_kennung": "BERT_DROP_TEMP_TABLE"},
            {"timecreated": datetime(2023, 1, 3, 8, 0, 0), "job_kennung": "BERT_DROP_TEMP_TABLE"},
        ])
        cmd_a = [
            "python", python_script_path,
            f"--gcp_project={gcp_project_id}",
            f"--bigquery_dataset={bq_dataset}",
            f"--metadata_dataset={metadata_dataset}"
        ]
        result_a = subprocess.run(cmd_a, capture_output=True, text=True, check=False)
        assert "Determined v_datum: 20230105" in result_a.stdout, f"Scenario A failed: {result_a.stdout}"

        # Scenario B: No matching job_kennung
        setup_dwtk_meldungen(bq_client, gcp_project_id, metadata_dataset, [
            {"timecreated": datetime(2023, 1, 1, 10, 0, 0), "job_kennung": "OTHER_JOB"},
        ])
        cmd_b = [
            "python", python_script_path,
            f"--gcp_project={gcp_project_id}",
            f"--bigquery_dataset={bq_dataset}",
            f"--metadata_dataset={metadata_dataset}"
        ]
        result_b = subprocess.run(cmd_b, capture_output=True, text=True, check=False)
        assert "Determined v_datum: 19000101" in result_b.stdout, f"Scenario B failed: {result_b.stdout}"

        # Scenario C: Empty table
        setup_dwtk_meldungen(bq_client, gcp_project_id, metadata_dataset, [])
        cmd_c = [
            "python", python_script_path,
            f"--gcp_project={gcp_project_id}",
            f"--bigquery_dataset={bq_dataset}",
            f"--metadata_dataset={metadata_dataset}"
        ]
        result_c = subprocess.run(cmd_c, capture_output=True, text=True, check=False)
        assert "Determined v_datum: 19000101" in result_c.stdout, f"Scenario C failed: {result_c.stdout}"
    ```

#### Test Case 2.2: Truncate Behavior

*   **Purpose:** To confirm that the target table `sof_ta_bcp_msisdn` is always truncated before new data is inserted. This replicates the Oracle `TRUNCATE` behavior.
*   **Setup:**
    1.  Insert some dummy data into `your_gcp_project.your_bigquery_dataset.sof_ta_bcp_msisdn`.
    2.  Ensure source tables (`sof_ta_bpr_bcp`, `sof_ta_rn_vertrag`) are populated such that the `INSERT` query will produce at least one row.
    3.  Ensure `dwtk_meldungen` is set up to provide a valid `v_datum`.
*   **Action:**
    1.  Execute the Airflow DAG `dw_bert_ausd_bp_ta_bcp_msisdn`.
*   **Pass/Fail Criterion:**
    *   The final row count of `sof_ta_bcp_msisdn` must be equal to the number of rows inserted by the `SELECT DISTINCT` statement, not the sum of dummy data + new data.
    *   The content of `sof_ta_bcp_msisdn` must *only* contain the newly inserted data, with no remnants of the dummy data.

    ```sql
    -- BigQuery SQL assertion
    -- Before running the job:
    -- INSERT INTO `your_gcp_project.your_bigquery_dataset.sof_ta_bcp_msisdn`
    -- (CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_TEL_MSISDN) VALUES ('DUMMY1', 'DUMMY_BPR1', 'DUMMY_REF1', '111');
    -- INSERT INTO `your_gcp_project.your_bigquery_dataset.sof_ta_bcp_msisdn`
    -- (CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_TEL_MSISDN) VALUES ('DUMMY2', 'DUMMY_BPR2', 'DUMMY_REF2', '222');

    -- After running the job, verify:
    SELECT COUNT(*) FROM `your_gcp_project.your_bigquery_dataset.sof_ta_bcp_msisdn`
    WHERE CNTRCT_ID LIKE 'DUMMY%'; -- Should return 0
    ```

#### Test Case 2.3: Join Logic

*   **Purpose:** To verify that the `JOIN` condition `bp.cntrct_id_ref = rn.cntrct_id` correctly links records, including cases with one-to-many relationships.
*   **Setup:**
    1.  Populate `sof_ta_bpr_bcp` and `sof_ta_rn_vertrag` with data that includes:
        *   Matching `cntrct_id_ref` and `cntrct_id` values.
        *   `cntrct_id_ref` values in `sof_ta_bpr_bcp` that have no match in `sof_ta_rn_vertrag`.
        *   `cntrct_id` values in `sof_ta_rn_vertrag` that have no match in `sof_ta_bpr_bcp`.
        *   One `cntrct_id_ref` in `sof_ta_bpr_bcp` matching multiple `cntrct_id` in `sof_ta_rn_vertrag` (if `cntrct_id` is not unique in `sof_ta_rn_vertrag`).
        *   One `cntrct_id` in `sof_ta_rn_vertrag` matching multiple `cntrct_id_ref` in `sof_ta_bpr_bcp`.
*   **Action:**
    1.  Execute the Airflow DAG `dw_bert_ausd_bp_ta_bcp_msisdn`.
*   **Pass/Fail Criterion:**
    *   The records in `sof_ta_bcp_msisdn` must accurately reflect an `INNER JOIN` on `bp.cntrct_id_ref = rn.cntrct_id`.
    *   Records from `sof_ta_bpr_bcp` without a matching `cntrct_id` in `sof_ta_rn_vertrag` must *not* appear in the target.
    *   Records from `sof_ta_rn_vertrag` without a matching `cntrct_id_ref` in `sof_ta_bpr_bcp` must *not* appear in the target.
    *   One-to-many and many-to-one relationships must produce the correct Cartesian product before `DISTINCT` is applied.

#### Test Case 2.4: `DISTINCT` Clause

*   **Purpose:** To ensure that the `DISTINCT` clause correctly removes duplicate rows from the result set before insertion into the target table.
*   **Setup:**
    1.  Populate `sof_ta_bpr_bcp` and `sof_ta_rn_vertrag` such that the `JOIN` operation would produce identical rows for `(CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_TEL_MSISDN)` if `DISTINCT` were not applied. For example:
        *   `sof_ta_bpr_bcp`: `('C1', 'B1', 'R1')`
        *   `sof_ta_rn_vertrag`: `('R1', 'MSISDN1')`, `('R1', 'MSISDN1')` (duplicate `rn` record)
        *   Or, `sof_ta_bpr_bcp`: `('C1', 'B1', 'R1')`, `('C1', 'B1', 'R1')` (duplicate `bp` record)
        *   Or, `sof_ta_bpr_bcp`: `('C1', 'B1', 'R1')`, `('C2', 'B2', 'R1')` and `sof_ta_rn_vertrag`: `('R1', 'MSISDN1')` where `C1, B1, R1, MSISDN1` is the same as `C2, B2, R1, MSISDN1` (unlikely but possible if `CNTRCT_ID` and `BPR_ID` are not unique in `sof_ta_bpr_bcp` for a given `CNTRCT_ID_REF`).
*   **Action:**
    1.  Execute the Airflow DAG `dw_bert_ausd_bp_ta_bcp_msisdn`.
*   **Pass/Fail Criterion:**
    *   The `sof_ta_bcp_msisdn` table must contain only unique combinations of `(CNTRCT_ID, BPR_ID, CNTRCT_ID_REF, TN_TEL_MSISDN)`.
    *   The number of rows in the target table must match the expected count after `DISTINCT` is applied to the joined result set.

#### Test Case 2.5: NULL Handling in Join Keys

*   **Purpose:** To verify that NULL values in the join keys (`cntrct_id_ref` or `cntrct_id`) are handled correctly, specifically that `NULL = NULL` evaluates to false, consistent with SQL standards for `INNER JOIN`.
*   **Setup:**
    1.  Populate `sof_ta_bpr_bcp` with records where `cntrct_id_ref` is `NULL`.
    2.  Populate `sof_ta_rn_vertrag` with records where `cntrct_id` is `NULL`.
    3.  Include records where `cntrct_id_ref` is `NULL` and `cntrct_id` is `NULL` in respective tables.
    4.  Include records with valid (non-NULL) join keys.
*   **Action:**
    1.  Execute the Airflow DAG `dw_bert_ausd_bp_ta_bcp_msisdn`.
*   **Pass/Fail Criterion:**
    *   No records where `cntrct_id_ref` or `cntrct_id` were `NULL` in the source tables (and thus could not satisfy the `INNER JOIN` condition) should appear in the target `sof_ta_bcp_msisdn` table.
    *   Only records with non-NULL, matching join keys should be present.

#### Test Case 2.6: Data Type Handling

*   **Purpose:** To confirm that data types are correctly handled during migration and transformation, ensuring no data loss or corruption for `STRING` types.
*   **Setup:**
    1.  Populate `sof_ta_bpr_bcp` and `sof_ta_rn_vertrag` with data that includes:
        *   Maximum length strings for each column.
        *   Strings with special characters (e.g., `-, _, /, `, etc.).
        *   Numeric strings (e.g., `CNTRCT_ID = '12345'`).
        *   Empty strings (`''`).
    2.  Ensure the BigQuery DDL for `sof_ta_bcp_msisdn` uses `STRING` for all columns.
*   **Action:**
    1.  Execute the Airflow DAG `dw_bert_ausd_bp_ta_bcp_msisdn`.
*   **Pass/Fail Criterion:**
    *   The data in `sof_ta_bcp_msisdn` must exactly match the source data after transformation, with no truncation, corruption, or unexpected type conversions.
    *   Empty strings in source should remain empty strings in target.

#### Test Case 2.7: Empty Source Tables

*   **Purpose:** To verify the job handles scenarios where one or both source tables are empty gracefully.
*   **Setup:**
    1.  **Scenario A:** `sof_ta_bpr_bcp` is empty, `sof_ta_rn_vertrag` has data.
    2.  **Scenario B:** `sof_ta_bpr_bcp` has data, `sof_ta_rn_vertrag` is empty.
    3.  **Scenario C:** Both `sof_ta_bpr_bcp` and `sof_ta_rn_vertrag` are empty.
    4.  Ensure `dwtk_meldungen` is set up to provide a valid `v_datum`.
*   **Action:**
    1.  For each scenario, execute the Airflow DAG `dw_bert_ausd_bp_ta_bcp_msisdn`.
*   **Pass/Fail Criterion:**
    *   For all scenarios (A, B, C), the target table `sof_ta_bcp_msisdn` must be empty (contain 0 rows).
    *   The job must complete successfully without errors.

#### Test Case 2.8: No Matching Records

*   **Purpose:** To verify the job handles scenarios where source tables have data, but the join condition yields no matches.
*   **Setup:**
    1.  Populate `sof_ta_bpr_bcp` with data (e.g., `cntrct_id_ref = 'A'`).
    2.  Populate `sof_ta_rn_vertrag` with data (e.g., `cntrct_id = 'B'`), ensuring no `cntrct_id_ref` in `sof_ta_bpr_bcp` matches any `cntrct_id` in `sof_ta_rn_vertrag`.
    3.  Ensure `dwtk_meldungen` is set up to provide a valid `v_datum`.
*   **Action:**
    1.  Execute the Airflow DAG `dw_bert_ausd_bp_ta_bcp_msisdn`.
*   **Pass/Fail Criterion:**
    *   The target table `sof_ta_bcp_msisdn` must be empty (contain 0 rows).
    *   The job must complete successfully without errors.

#### Test Case 2.9: `stichtag` Parameter Override

*   **Purpose:** To verify that the `--stichtag` argument, when provided to the Python script, correctly overrides the `v_datum` retrieval logic from `dwtk_meldungen`.
*   **Setup:**
    1.  Populate `isbert_schema.dwtk_meldungen` with records such that the `MAX(timecreated)` for `job_kennung = 'BERT_DROP_TEMP_TABLE'` would result in a specific `v_datum` (e.g., '20230101').
    2.  Prepare a `--stichtag` value that is different (e.g., '20230315').
*   **Action:**
    1.  Modify the Airflow DAG to pass the `--stichtag` argument to the `DataprocSubmitJobOperator`.
    2.  Execute the Airflow DAG `dw_bert_ausd_bp_ta_bcp_msisdn`.
    3.  Review the Dataproc job logs for the Python script's output.
*   **Pass/Fail Criterion:**
    *   The Dataproc job logs must show `Using Stichtag from arguments: 20230315` (or the provided value) and *not* `Retrieving v_datum from BigQuery metadata...`.
    *   The job must complete successfully.

---

### 3. External-System Replacements Tests

#### Test Case 3.1: Airflow Orchestration and Dataproc Job Submission

*   **Purpose:** To verify that the Airflow DAG successfully triggers and monitors the Dataproc job, replacing the UC4 orchestration.
*   **Setup:**
    1.  Ensure the Airflow DAG `dw_bert_ausd_bp_ta_bcp_msisdn.py` is deployed to Cloud Composer.
    2.  Ensure the Dataproc cluster is available and configured correctly in the DAG.
    3.  Ensure the Python script `r_ausd_bp_ta_bcp_msisdn.py` is accessible in GCS.
*   **Action:**
    1.  Manually trigger the `dw_bert_ausd_bp_ta_bcp_msisdn` DAG from the Airflow UI.
*   **Pass/Fail Criterion:**
    *   The Airflow DAG run must complete successfully (all tasks green).
    *   A corresponding Dataproc job must be visible in the GCP Console (Dataproc -> Jobs) and show a "SUCCEEDED" status.
    *   Airflow task logs for `run_dw_bert_ausd_bp_ta_bcp_msisdn` must show output from the Dataproc job.

#### Test Case 3.2: BigQuery Client Interaction

*   **Purpose:** To verify that the Python script correctly uses the `google-cloud-bigquery` client library to interact with BigQuery for DDL (TRUNCATE), DML (INSERT), and data retrieval (`v_datum`).
*   **Setup:**
    1.  Ensure the Python script `r_ausd_bp_ta_bcp_msisdn.py` is deployed.
    2.  Ensure the Dataproc cluster has network access to BigQuery APIs.
    3.  Ensure the service account running the Dataproc job has `bigquery.dataEditor` (or equivalent) permissions on the target dataset and `bigquery.dataViewer` on the metadata dataset.
*   **Action:**
    1.  Execute the Airflow DAG `dw_bert_ausd_bp_ta_bcp_msisdn`.
    2.  Review the Dataproc job logs in Cloud Logging.
*   **Pass/Fail Criterion:**
    *   The logs must contain messages indicating successful BigQuery operations:
        *   `Retrieving v_datum from BigQuery metadata...` (if `--stichtag` not used)
        *   `BigQuery query executed successfully. Job ID: ...` for `get_datum_query`.
        *   `Truncating target table: ...`
        *   `BigQuery query executed successfully. Job ID: ...` for `truncate_query`.
        *   `Successfully truncated ...`
        *   `Inserting data into target table. Query: ...`
        *   `BigQuery query executed successfully. Job ID: ...` for `insert_query`.
        *   `Successfully inserted data into ...`
    *   No BigQuery API errors should be present in the logs.

---

### 4. Data Quality / Row Count / Schema Assertions

#### Test Case 4.1: Row Count Parity

*   **Purpose:** To verify that the total number of rows in the target BigQuery table matches the legacy Oracle table after a full run.
*   **Setup:**
    1.  Perform a full run of both the legacy Oracle job and the migrated BigQuery job with identical input data (as in Test Case 1.1).
*   **Action:**
    1.  Query the row count from the legacy Oracle `sof$ta_bcp_msisdn`.
    2.  Query the row count from the BigQuery `sof_ta_bcp_msisdn`.
*   **Pass/Fail Criterion:**
    *   The `COUNT(*)` from `sof$ta_bcp_msisdn` in Oracle must be equal to `COUNT(*)` from `sof_ta_bcp_msisdn` in BigQuery.

    ```sql
    -- BigQuery SQL assertion
    SELECT COUNT(*) FROM `your_gcp_project.your_bigquery_dataset.sof_ta_bcp_msisdn`;

    -- Oracle SQL assertion
    SELECT COUNT(*) FROM sof$ta_bcp_msisdn;
    ```

#### Test Case 4.2: Schema Parity

*   **Purpose:** To verify that the schema (column names, data types, order) of the target BigQuery table matches the legacy Oracle table.
*   **Setup:**
    1.  Ensure the BigQuery DDL for `sof_ta_bcp_msisdn` has been applied.
*   **Action:**
    1.  Retrieve the schema definition for `sof$ta_bcp_msisdn` from Oracle (e.g., `DESC sof$ta_bcp_msisdn`).
    2.  Retrieve the schema definition for `sof_ta_bcp_msisdn` from BigQuery (e.g., `bq show --schema --format=prettyjson your_gcp_project:your_bigquery_dataset.sof_ta_bcp_msisdn`).
*   **Pass/Fail Criterion:**
    *   All column names must match (case-insensitivity might need to be considered, but ideally, they are identical).
    *   The logical data types must be equivalent (e.g., Oracle `VARCHAR2` maps to BigQuery `STRING`).
    *   The order of columns should ideally be the same, though BigQuery is column-order agnostic for queries, it's good for consistency.

#### Test Case 4.3: Logging and Error Handling

*   **Purpose:** To verify that the Python script logs appropriate messages for successful operations and captures errors effectively.
*   **Setup:**
    1.  **Scenario A (Success):** Configure source data for a successful run.
    2.  **Scenario B (Failure - BigQuery error):** Configure source data or BigQuery permissions to intentionally cause a BigQuery error (e.g., revoke `bigquery.dataEditor` on the target table for the Dataproc service account).
    3.  **Scenario C (Failure - Python error):** Introduce a syntax error or runtime error in the Python script.
*   **Action:**
    1.  For each scenario, execute the Airflow DAG `dw_bert_ausd_bp_ta_bcp_msisdn`.
    2.  Review the Dataproc job logs in Cloud Logging.
*   **Pass/Fail Criterion:**
    *   **Scenario A:** Logs must show `INFO` level messages for each major step (retrieving `v_datum`, truncating, inserting) and end with `Data processing completed successfully.`.
    *   **Scenario B:** Logs must show `ERROR` level messages detailing the BigQuery error (e.g., permission denied) and the script should exit with a non-zero status. The Airflow task should fail.
    *   **Scenario C:** Logs must show `ERROR` level messages with Python stack traces for the introduced error, and the script should exit with a non-zero status. The Airflow task should fail.

---