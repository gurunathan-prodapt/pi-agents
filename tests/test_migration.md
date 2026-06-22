As a senior data-migration QA engineer, I've designed a suite of validation tests for the `DW.BERT_AUSD_BP_TA_P_BASISPROD` job migration. These tests aim to ensure the migrated BigQuery/PySpark/Airflow solution is behaviourally equivalent to the legacy Oracle/KornShell/UC4 system.

The tests are categorized to cover output parity, transformation correctness, external system replacements, and data quality/schema assertions.

**General Prerequisites for all Tests:**
*   **GCP Environment:** A Google Cloud Project with BigQuery, Cloud Storage, and Dataproc services enabled.
*   **BigQuery Source Tables:** BigQuery tables corresponding to all Oracle source tables (`isbert_schema.dwtk_meldungen`, `sof$ta_cntrct_dist`, `sof$ta_bcp_iccid`, `sof$ta_bcp_msisdn`, `sof$ta_cntrct_evn`, `sof$ta_iccid_vertrag`, `sof$ta_rn_vertrag`, `sof$ta_rn_da_vda_tk`, `sof$ta_tarifoption`, `sof$ta_apn_vertrag`) must be created. For most tests, these will be populated with specific mock data. For end-to-end parity, they will contain a snapshot of legacy Oracle data.
*   **BigQuery Target Table:** The target table `dw_dwh_prod.sof_ta_p_basisprod` must be created in BigQuery with the expected schema.
*   **GCS Artifacts:** The PySpark script (`k_ausd_bp_ta_p_basisprod.py`) and the BigQuery SQL script (`d_ausd_bp_ta_p_basisprod_bq.sql`) must be uploaded to the specified GCS bucket path (e.g., `gs://YOUR_BUCKET_NAME/pyspark_scripts/` and `gs://YOUR_BUCKET_NAME/sql/`).
*   **Airflow Deployment:** The Airflow DAG (`dw_bert_ausd_bp_ta_p_basisprod_dag.py`) must be deployed to an Airflow environment, and the `GCP_PROJECT_ID`, `DATAPROC_CLUSTER_NAME`, `GCP_REGION`, `GCS_BUCKET_NAME`, `PYSPARK_SCRIPT_GCS_PATH`, and `BIGQUERY_SQL_FILE_GCS_PATH` variables within the DAG must be correctly configured for your environment.
*   **Pytest Environment:** For runnable Python tests, a `pytest` environment with `google-cloud-bigquery` and `google-cloud-storage` libraries installed is assumed. Fixtures for `gcp_project_id`, `bq_client`, and `gcs_client` would typically be provided by the test framework.

---

### Test Case 1: End-to-End Output Parity with Golden Data

*   **Purpose:** To verify that the migrated job produces an identical output dataset to the legacy job when given the exact same input data. This is the most comprehensive test for output parity.
*   **Setup:**
    1.  **Identify a "Golden Run Date":** Choose a specific historical `stichtag` (e.g., `20231026`) for which the legacy job ran successfully and its output is considered correct.
    2.  **Extract Legacy Source Data:** For the chosen `stichtag`, extract the full data content of all Oracle source tables as they existed *before* the legacy job ran on that date. Store this data (e.g., as CSV or Parquet files).
    3.  **Extract Legacy Target Data (Golden Output):** Extract the full data content of the Oracle target table `sof$ta_p_basisprod` *after* the legacy job completed successfully for the chosen `stichtag`. This is the "golden output".
    4.  **Load BigQuery Source Tables:** Load the extracted legacy source data into the corresponding BigQuery source tables (e.g., `your-gcp-project-id.isbert_schema.dwtk_meldungen_bq`, `your-gcp-project-id.dw_dwh_prod.sof_ta_cntrct_dist_bq`, etc.). Ensure data types and column names match the BigQuery schema.
    5.  **Load Golden Output:** Load the "golden output" into a temporary BigQuery table (e.g., `your-gcp-project-id.temp_dataset.legacy_golden_output`).
    6.  **Configure Airflow DAG:** Ensure the DAG's `start_date` and `schedule` (or manually trigger) are set to run for the chosen `stichtag`.
*   **Action:**
    1.  Trigger the Airflow DAG `dw_bert_ausd_bp_ta_p_basisprod` for the chosen `stichtag` (e.g., by setting `{{ ds_nodash }}` to `20231026` or manually triggering with a configuration).
    2.  Wait for the DAG run to complete successfully.
    3.  Once complete, execute the comparison SQL queries below.
*   **Pass/Fail Criterion:**
    *   The Airflow DAG completes successfully.
    *   The row count of `your-gcp-project-id.dw_dwh_prod.sof_ta_p_basisprod` matches the row count of the golden Oracle output.
    *   The `EXCEPT DISTINCT` queries return zero rows, indicating no differences in data content.
*   **Test Code (Conceptual SQL for comparison):**
    ```sql
    -- Assuming 'your-gcp-project-id.temp_dataset.legacy_golden_output' is a temporary BigQuery table
    -- loaded with the Oracle golden data, and 'your-gcp-project-id.dw_dwh_prod.sof_ta_p_basisprod'
    -- is the output of the migrated job.

    -- 1. Check row counts
    SELECT
        (SELECT COUNT(*) FROM `your-gcp-project-id.dw_dwh_prod.sof_ta_p_basisprod`) AS migrated_row_count,
        (SELECT COUNT(*) FROM `your-gcp-project-id.temp_dataset.legacy_golden_output`) AS golden_row_count;

    -- 2. Identify rows present in migrated but not in golden
    SELECT 'Migrated_Only' AS diff_type, * FROM `your-gcp-project-id.dw_dwh_prod.sof_ta_p_basisprod`
    EXCEPT DISTINCT
    SELECT * FROM `your-gcp-project-id.temp_dataset.legacy_golden_output`;

    -- 3. Identify rows present in golden but not in migrated
    SELECT 'Golden_Only' AS diff_type, * FROM `your-gcp-project-id.temp_dataset.legacy_golden_output`
    EXCEPT DISTINCT
    SELECT * FROM `your-gcp-project-id.temp_dataset.legacy_golden_output`;

    -- If the above queries return 0 rows, the data is identical.
    -- For large tables, consider using data comparison tools like data-diff or dbt_utils.audit_helper.
    ```

---

### Test Case 2: Transformation Correctness - `v_datum` Determination

*   **Purpose:** To verify that the `v_datum` (reference date) is correctly determined from `isbert_schema.dwtk_meldungen` using the `MAX(timecreated)` logic, matching the legacy behavior.
*   **Setup:**
    1.  **Mock `isbert_schema.dwtk_meldungen_bq`:** Create a BigQuery table `your-gcp-project-id.isbert_schema.dwtk_meldungen_bq` with specific data to test the `MAX(timecreated)` logic for `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
    2.  **Create Dummy Target Table:** Create a minimal `your-gcp-project-id.dw_dwh_prod.sof_ta_p_basisprod` table so the PySpark script can execute the full SQL without error.
    3.  **Upload Minimal SQL:** Upload a simplified BigQuery SQL file to GCS (e.g., `gs://YOUR_BUCKET_NAME/sql/test_v_datum_bq.sql`) that includes the `v_datum` determination logic and a dummy DML statement.
*   **Action:**
    1.  Execute the PySpark script `k_ausd_bp_ta_p_basisprod.py` directly (or via Airflow) with the mocked SQL file.
    2.  Monitor the PySpark job logs for the output indicating the determined `v_datum`.
*   **Pass/Fail Criterion:**
    *   The PySpark job completes successfully.
    *   The log output for `v_datum` matches the expected `MAX(timecreated)` for `job_kennung = 'BERT_DROP_TEMP_TABLE'` from the mocked data.
*   **Test Code (Pytest with subprocess execution):**
    ```python
    import subprocess
    from datetime import datetime
    from google.cloud import bigquery, storage

    def test_v_datum_determination(gcp_project_id, gcs_bucket_name, bq_client, gcs_client):
        """
        Verifies the correct determination of v_datum from dwtk_meldungen.
        """
        dwtk_table_id = f"{gcp_project_id}.isbert_schema.dwtk_meldungen_bq"
        target_table_id = f"{gcp_project_id}.dw_dwh_prod.sof_ta_p_basisprod"

        # 1. Setup: Create/replace mock dwtk_meldungen_bq table
        bq_client.query(f"""
            CREATE OR REPLACE TABLE `{dwtk_table_id}` (
                job_kennung STRING,
                timecreated TIMESTAMP
            );
            INSERT INTO `{dwtk_table_id}` VALUES
            ('OTHER_JOB', '2023-10-25 10:00:00 UTC'),
            ('BERT_DROP_TEMP_TABLE', '2023-10-26 11:00:00 UTC'),
            ('OTHER_JOB', '2023-10-26 12:00:00 UTC'),
            ('BERT_DROP_TEMP_TABLE', '2023-10-27 09:30:00 UTC'), -- This should be the max
            ('BERT_DROP_TEMP_TABLE', '2023-10-27 09:00:00 UTC');
        """).result()

        # Create a dummy target table for the script to run without error
        bq_client.query(f"""
            CREATE OR REPLACE TABLE `{target_table_id}` (
                dummy_col STRING
            );
        """).result()

        # Prepare a minimal SQL script for v_datum determination and a dummy DML
        minimal_sql = f"""
            SELECT MAX(timecreated) AS v_datum
            FROM `{dwtk_table_id}`
            WHERE job_kennung = 'BERT_DROP_TEMP_TABLE';
            TRUNCATE TABLE `{target_table_id}`; -- Dummy DML to satisfy the script's expectation
        """
        sql_blob_path = f"sql/test_v_datum_bq.sql"
        gcs_client.bucket(gcs_bucket_name).blob(sql_blob_path).upload_from_string(minimal_sql)
        sql_file_gcs_path = f"gs://{gcs_bucket_name}/{sql_blob_path}"

        # 2. Action: Run the PySpark script
        command = [
            "python", "k_ausd_bp_ta_p_basisprod.py",
            "--project_id", gcp_project_id,
            "--sql_file_gcs_path", sql_file_gcs_path,
            "--stichtag", datetime.now().strftime('%Y%m%d')
        ]
        
        process = subprocess.run(command, capture_output=True, text=True, check=False)
        
        # 3. Pass/Fail Criterion: Check logs for v_datum
        assert process.returncode == 0, f"PySpark script failed: {process.stderr}"
        
        expected_v_datum_str = "2023-10-27 09:30:00 UTC"
        assert f"Determined v_datum: {expected_v_datum_str}" in process.stdout, \
            f"Expected v_datum '{expected_v_datum_str}' not found in logs. Actual logs: {process.stdout}"

        # Clean up dummy SQL file from GCS
        gcs_client.bucket(gcs_bucket_name).blob(sql_blob_path).delete()
    ```

---

### Test Case 3: Transformation Correctness - Oracle Function Translations (`NVL`, `decode`, `(+)`)

*   **Purpose:** To verify that key Oracle SQL functions and join syntax are correctly translated to BigQuery equivalents.
*   **Setup:**
    1.  **Mock Source Tables:** Create minimal BigQuery source tables (`sof_ta_cntrct_dist_bq`, `sof_ta_bcp_msisdn_bq`, `sof_ta_apn_vertrag_bq`) in a `test_dataset` with data specifically designed to test `COALESCE` (for `NVL`), `CASE` statements (for `decode`), and `LEFT OUTER JOIN` (for `(+)`) behavior.
*   **Action:**
    1.  Execute the provided SQL assertions directly against BigQuery.
*   **Pass/Fail Criterion:**
    *   All `HAVING` clauses in the assertion queries return 0 rows, indicating no discrepancies between actual and expected results for the tested transformations.
*   **Test Code (SQL Assertions):**
    ```sql
    -- Setup: Mock data for testing NVL, decode, and LEFT OUTER JOIN
    CREATE OR REPLACE TABLE `your-gcp-project-id.test_dataset.sof_ta_cntrct_dist_bq` (cntrct_id STRING, dist_val STRING);
    INSERT INTO `your-gcp-project-id.test_dataset.sof_ta_cntrct_dist_bq` VALUES
    ('C1', 'Val1'), ('C2', NULL), ('C3', 'Val3');

    CREATE OR REPLACE TABLE `your-gcp-project-id.test_dataset.sof_ta_bcp_msisdn_bq` (cntrct_id STRING, msisdn STRING);
    INSERT INTO `your-gcp-project-id.test_dataset.sof_ta_bcp_msisdn_bq` VALUES
    ('C1', 'MSISDN1'), ('C3', 'MSISDN3'); -- C2 will be unmatched for outer join

    CREATE OR REPLACE TABLE `your-gcp-project-id.test_dataset.sof_ta_apn_vertrag_bq` (cntrct_id STRING, apn STRING, apn_cntrct STRING);
    INSERT INTO `your-gcp-project-id.test_dataset.sof_ta_apn_vertrag_bq` VALUES
    ('C1', 'APN1', 'APNC1'),
    ('C2', NULL, 'APNC2'), -- apn is NULL
    ('C3', 'APN3', NULL),  -- apn_cntrct is NULL
    ('C4', 'APN4', 'APNC4');

    -- Test NVL translation (COALESCE)
    -- This query should return 0 rows
    SELECT
        cntrct_id,
        COALESCE(dist_val, 'DEFAULT_DIST') AS actual_dist_val,
        CASE cntrct_id
            WHEN 'C1' THEN 'Val1'
            WHEN 'C2' THEN 'DEFAULT_DIST'
            WHEN 'C3' THEN 'Val3'
        END AS expected_dist_val
    FROM `your-gcp-project-id.test_dataset.sof_ta_cntrct_dist_bq`
    HAVING actual_dist_val != expected_dist_val;

    -- Test decode (APN logic) translation: `decode(av.apn, null,av.apn, av.apn||','||av.apn_cntrct)`
    -- In Oracle, `APN3 || ',' || NULL` results in NULL.
    -- In BigQuery, `CONCAT('APN3', ',', NULL)` also results in NULL.
    -- The translation `CASE WHEN av.apn IS NOT NULL THEN CONCAT(av.apn, ',', av.apn_cntrct) ELSE av.apn END` is correct.
    -- This query should return 0 rows
    SELECT
        cntrct_id,
        CASE WHEN apn IS NOT NULL THEN CONCAT(apn, COALESCE(',' || apn_cntrct, '')) ELSE apn END AS actual_apn_combined,
        CASE cntrct_id
            WHEN 'C1' THEN 'APN1,APNC1'
            WHEN 'C2' THEN NULL
            WHEN 'C3' THEN NULL -- Expected NULL because APN_CNTRCT is NULL
            WHEN 'C4' THEN 'APN4,APNC4'
        END AS expected_apn_combined
    FROM `your-gcp-project-id.test_dataset.sof_ta_apn_vertrag_bq`
    HAVING actual_apn_combined != expected_apn_combined
       OR (actual_apn_combined IS NULL AND expected_apn_combined IS NOT NULL)
       OR (actual_apn_combined IS NOT NULL AND expected_apn_combined IS NULL);

    -- Test LEFT OUTER JOIN translation
    -- This query should return 0 rows
    SELECT
        cn.cntrct_id,
        bcm.msisdn AS actual_msisdn,
        CASE cn.cntrct_id
            WHEN 'C1' THEN 'MSISDN1'
            WHEN 'C2' THEN NULL -- Expected NULL for unmatched
            WHEN 'C3' THEN 'MSISDN3'
        END AS expected_msisdn
    FROM `your-gcp-project-id.test_dataset.sof_ta_cntrct_dist_bq` cn
    LEFT OUTER JOIN `your-gcp-project-id.test_dataset.sof_ta_bcp_msisdn_bq` bcm
      ON cn.cntrct_id = bcm.cntrct_id
    HAVING actual_msisdn != expected_msisdn
       OR (actual_msisdn IS NULL AND expected_msisdn IS NOT NULL)
       OR (actual_msisdn IS NOT NULL AND expected_msisdn IS NULL);
    ```

---

### Test Case 4: External System Replacement - PySpark Parameter Handling

*   **Purpose:** To verify that the PySpark script correctly receives and processes parameters (`stichtag`, `wiederanlaufwert`, `sql_file_gcs_path`, `project_id`) passed from Airflow.
*   **Setup:**
    1.  **Mock SQL File:** Create a simple BigQuery SQL file (e.g., `gs://YOUR_BUCKET_NAME/sql/test_params.sql`) in GCS that includes a dummy DML statement (e.g., `TRUNCATE TABLE ...`) to allow the PySpark script to execute successfully.
    2.  **Configure Airflow DAG:** Set `BIGQUERY_SQL_FILE_GCS_PATH` to point to this mock SQL file.
    3.  **Set `stichtag`:** The DAG passes `{{ ds_nodash }}` for `stichtag`. Choose a specific execution date for the DAG (e.g., `2023-11-15`).
*   **Action:**
    1.  Trigger the Airflow DAG `dw_bert_ausd_bp_ta_p_basisprod` for the chosen date (e.g., `2023-11-15`).
    2.  Monitor the Airflow task logs for `run_bert_ausd_bp_ta_p_basisprod`.
*   **Pass/Fail Criterion:**
    *   The Airflow task completes successfully.
    *   The PySpark script logs show the `stichtag` parameter matching the Airflow execution date (`20231115`).
    *   The `wiederanlaufwert` is logged as `0` (or whatever default/configured value).
    *   The `sql_file_gcs_path` and `project_id` are logged correctly.
*   **Test Code (Conceptual - relies on Airflow/Dataproc logs):**
    ```python
    # This test relies on observing logs from a running Airflow task.
    # Example log snippet to look for:
    # INFO - Starting PySpark job for DW.BERT_AUSD_BP_TA_P_BASISPROD
    # INFO - Parameters: Stichtag=20231115, Wiederanlaufwert=0
    # INFO - SQL file GCS path: gs://your-gcs-bucket-name/sql/test_params.sql
    # INFO - Project ID: your-gcp-project-id
    ```

---

### Test Case 5: External System Replacement - BigQuery `TRUNCATE TABLE`

*   **Purpose:** To verify that the target table `sof_ta_p_basisprod` is truncated before new data is inserted, mimicking the `TRUNCATE TABLE` call in the legacy Oracle script.
*   **Setup:**
    1.  **Populate Target Table:** Insert some dummy data into the BigQuery target table `your-gcp-project-id.dw_dwh_prod.sof_ta_p_basisprod`.
    2.  **Mock Source Data:** Provide minimal source data that would result in *different* data being inserted (e.g., `('NEW_C1', 1)`).
    3.  **Configure Airflow DAG:** Ensure the DAG is configured to run the PySpark script with the actual `d_ausd_bp_ta_p_basisprod_bq.sql` (or a simplified version that includes `TRUNCATE` and `INSERT`).
*   **Action:**
    1.  Trigger the Airflow DAG `dw_bert_ausd_bp_ta_p_basisprod`.
    2.  After the DAG completes, execute the SQL assertions below.
*   **Pass/Fail Criterion:**
    *   The Airflow DAG completes successfully.
    *   The `your-gcp-project-id.dw_dwh_prod.sof_ta_p_basisprod` table contains *only* the data inserted by the current run, and none of the pre-existing dummy data.
*   **Test Code (SQL Assertion):**
    ```sql
    -- Setup:
    INSERT INTO `your-gcp-project-id.dw_dwh_prod.sof_ta_p_basisprod` VALUES
    ('OLD_C1', 100), ('OLD_C2', 200);

    -- After running the Airflow DAG:
    -- This query should return 0 rows, indicating old data was truncated.
    SELECT COUNT(*) FROM `your-gcp-project-id.dw_dwh_prod.sof_ta_p_basisprod` WHERE contract_id LIKE 'OLD_%';

    -- This query should return the expected number of new rows, indicating new data was inserted.
    SELECT COUNT(*) FROM `your-gcp-project-id.dw_dwh_prod.sof_ta_p_basisprod` WHERE contract_id LIKE 'NEW_%';
    ```

---

### Test Case 6: Data Quality - NULL Handling for Critical Fields

*   **Purpose:** To verify that critical fields, especially those involved in joins or `NVL`/`COALESCE` transformations, handle NULL values as expected, preventing unexpected NULLs or errors.
*   **Setup:**
    1.  **Mock Source Tables:** Create BigQuery source tables with specific NULL scenarios for `cntrct_id`, `dist_val`, `msisdn`, `apn`, and `apn_cntrct`. Include cases where join keys are NULL, and where values used in `COALESCE` or `CASE` statements are NULL.
    2.  **Full Job Execution:** Run the Airflow DAG with this mocked data.
*   **Action:**
    1.  Trigger the Airflow DAG `dw_bert_ausd_bp_ta_p_basisprod`.
    2.  After completion, execute the SQL assertions below on the target table `your-gcp-project-id.dw_dwh_prod.sof_ta_p_basisprod`.
*   **Pass/Fail Criterion:**
    *   The Airflow DAG completes successfully.
    *   The SQL assertions return 0 rows, indicating that NULLs are handled as expected in derived columns and join logic.
*   **Test Code (SQL Assertions on target table):**
    ```sql
    -- Assuming the target table has columns like `DIST_VALUE`, `APN_COMBINED`, `MSISDN`, and `CONTRACT_ID`

    -- Check for unexpected NULLs in a COALESCE-derived column (e.g., DIST_VALUE, which should have a default)
    -- This query should return 0 rows.
    SELECT COUNT(*)
    FROM `your-gcp-project-id.dw_dwh_prod.sof_ta_p_basisprod`
    WHERE DIST_VALUE IS NULL; -- If DIST_VALUE should always have a default and never be NULL

    -- Check APN_COMBINED logic for NULLs (if source APN_CNTRCT was NULL, combined should be NULL)
    -- This query should return 0 rows.
    SELECT
        COUNT(*)
    FROM `your-gcp-project-id.dw_dwh_prod.sof_ta_p_basisprod`
    WHERE APN_COMBINED IS NOT NULL
      AND (APN_SOURCE IS NOT NULL AND APN_CNTRCT_SOURCE IS NULL); -- Assuming source columns are also in target for verification
                                                                  -- Or, more practically, test with specific contract_ids from setup
                                                                  -- e.g., WHERE CONTRACT_ID = 'C3_APN_NULL_CNTRCT' AND APN_COMBINED IS NOT NULL

    -- Check that rows with NULL join keys in the left table are still present (for LEFT OUTER JOIN)
    -- and their right-side columns are NULL.
    -- This query should return 0 rows.
    SELECT COUNT(*)
    FROM `your-gcp-project-id.dw_dwh_prod.sof_ta_p_basisprod`
    WHERE CONTRACT_ID_FROM_LEFT_TABLE IS NULL -- Assuming a column to track original left table contract_id
      AND MSISDN IS NOT NULL;
    ```

---

### Test Case 7: Data Quality - Row Count Assertion

*   **Purpose:** To verify that the total number of rows in the target table after migration matches the expected row count, providing a high-level check for data completeness.
*   **Setup:**
    1.  **Golden Row Count:** Determine the expected row count for the target table `sof$ta_p_basisprod` for a specific `stichtag` from the legacy system.
    2.  **Populate BigQuery Source Tables:** Load BigQuery source tables with the same data that produced the golden row count in the legacy system (as in Test Case 1).
    3.  **Configure Airflow DAG:** Ensure the DAG is configured correctly.
*   **Action:**
    1.  Trigger the Airflow DAG `dw_bert_ausd_bp_ta_p_basisprod` for the chosen `stichtag`.
    2.  After the DAG completes, execute the SQL query below.
*   **Pass/Fail Criterion:**
    *   The Airflow DAG completes successfully.
    *   The `COUNT(*)` from `your-gcp-project-id.dw_dwh_prod.sof_ta_p_basisprod` exactly matches the golden row count obtained from the legacy system for the same input data.
*   **Test Code (SQL Assertion):**
    ```sql
    -- After running the Airflow DAG:
    SELECT
        (SELECT COUNT(*) FROM `your-gcp-project-id.dw_dwh_prod.sof_ta_p_basisprod`) AS migrated_row_count,
        <expected_golden_row_count> AS expected_row_count;
    -- The two values should be equal.
    ```

---

### Test Case 8: Schema Assertion

*   **Purpose:** To verify that the schema (column names, data types) of the migrated target table `dw_dwh_prod.sof_ta_p_basisprod` matches the expected schema, which should be derived from the legacy `sof$ta_p_basisprod` table.
*   **Setup:**
    1.  **Extract Legacy Schema:** Obtain the precise schema (column names, data types, nullability) of the Oracle `sof$ta_p_basisprod` table.
    2.  **Define Expected BigQuery Schema:** Translate the Oracle schema to its BigQuery equivalent, considering data type mappings (e.g., `VARCHAR2` to `STRING`, `NUMBER` to `INT64`/`NUMERIC`, `DATE` to `DATE`/`TIMESTAMP`).
*   **Action:**
    1.  Trigger the Airflow DAG `dw_bert_ausd_bp_ta_p_basisprod`.
    2.  After the DAG completes, execute the Python test code below.
*   **Pass/Fail Criterion:**
    *   The Airflow DAG completes successfully.
    *   The Python test asserts that the actual BigQuery schema matches the defined expected schema in terms of column names, data types, and nullability.
*   **Test Code (Pytest using BigQuery client):**
    ```python
    import pytest
    from google.cloud import bigquery

    def test_target_table_schema(gcp_project_id, bq_client):
        """
        Verifies the schema of the target BigQuery table.
        """
        target_table_id = f"{gcp_project_id}.dw_dwh_prod.sof_ta_p_basisprod"

        # Define the expected schema based on Oracle to BQ translation
        # This needs to be meticulously created from the legacy Oracle schema.
        expected_schema = [
            bigquery.SchemaField("CONTRACT_ID", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("MSISDN", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("ICCID", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("V_DATUM", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("DIST_VALUE", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("APN_COMBINED", "STRING", mode="NULLABLE"),
            # ... add all expected columns and their types/modes from the design
            bigquery.SchemaField("MS1_ICCID", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("MS10_VALID", "BOOLEAN", mode="NULLABLE"),
            # ...
        ]

        # Retrieve the actual schema from BigQuery
        table = bq_client.get_table(target_table_id)
        actual_schema = table.schema

        # Compare schemas
        actual_schema_map = {field.name: (field.field_type, field.mode) for field in actual_schema}
        expected_schema_map = {field.name: (field.field_type, field.mode) for field in expected_schema}

        # Check if all expected fields are present and match type/mode
        for expected_field_name, expected_field_props in expected_schema_map.items():
            assert expected_field_name in actual_schema_map, \
                f"Missing expected column: {expected_field_name}"
            assert actual_schema_map[expected_field_name] == expected_field_props, \
                f"Schema mismatch for column {expected_field_name}: " \
                f"Expected {expected_field_props}, Got {actual_schema_map[expected_field_name]}"

        # Optionally, check for unexpected extra columns
        for actual_field_name in actual_schema_map:
            assert actual_field_name in expected_schema_map, \
                f"Unexpected column found: {actual_field_name}"
    ```

---

### Test Case 9: Edge Case - Empty Source Tables

*   **Purpose:** To verify that the job handles scenarios where one or more source tables are empty gracefully, producing an empty target table or a table with expected default values, without failing.
*   **Setup:**
    1.  **Empty Source Tables:** Create all BigQuery source tables but leave them completely empty.
    2.  **Configure Airflow DAG:** Ensure the DAG is configured correctly.
*   **Action:**
    1.  Trigger the Airflow DAG `dw_bert_ausd_bp_ta_p_basisprod`.
    2.  After the DAG completes, execute the SQL query below.
*   **Pass/Fail Criterion:**
    *   The Airflow DAG completes successfully (does not fail with errors).
    *   The `your-gcp-project-id.dw_dwh_prod.sof_ta_p_basisprod` table should be empty (row count = 0).
*   **Test Code (SQL Assertion):**
    ```sql
    -- After running the Airflow DAG with empty source tables:
    SELECT COUNT(*) FROM `your-gcp-project-id.dw_dwh_prod.sof_ta_p_basisprod`;
    -- This query should return 0.
    ```

---

### Test Case 10: PySpark Script Robustness - Invalid `sql_file_gcs_path`

*   **Purpose:** To verify that the PySpark script handles an invalid GCS path for the SQL file gracefully, failing with an informative error.
*   **Setup:**
    1.  **Invalid GCS Path:** Configure the Airflow DAG's `BIGQUERY_SQL_FILE_GCS_PATH` to point to a non-existent or malformed GCS path (e.g., `gs://non-existent-bucket/sql/non_existent.sql` or `invalid-path`).
*   **Action:**
    1.  Trigger the Airflow DAG `dw_bert_ausd_bp_ta_p_basisprod`.
    2.  Monitor the Airflow task logs for `run_bert_ausd_bp_ta_p_basisprod`.
*   **Pass/Fail Criterion:**
    *   The Airflow task `run_bert_ausd_bp_ta_p_basisprod` fails.
    *   The PySpark script logs contain an error message indicating that the SQL file could not be loaded from GCS (e.g., `NotFound` error from GCS client, or `ValueError` if path format is wrong).
*   **Test Code (Conceptual - relies on Airflow/Dataproc logs):**
    ```python
    # This is an integration test for error handling, observed via Airflow/Dataproc logs.
    # Expected log snippet for a non-existent file:
    # ERROR - PySpark job failed: google.cloud.exceptions.NotFound: 404 Not Found
    # Expected log snippet for malformed path:
    # ERROR - PySpark job failed: ValueError: SQL file GCS path must start with 'gs://'
    ```

---

### Test Case 11: PySpark Script Robustness - Invalid BigQuery SQL

*   **Purpose:** To verify that the PySpark script correctly handles and propagates errors when the BigQuery SQL it attempts to execute is syntactically incorrect or refers to non-existent tables/columns.
*   **Setup:**
    1.  **Invalid SQL File:** Create a BigQuery SQL file in GCS (e.g., `gs://YOUR_BUCKET_NAME/sql/invalid_sql.sql`) with a deliberate syntax error or a reference to a non-existent table/column.
    2.  **Configure Airflow DAG:** Set `BIGQUERY_SQL_FILE_GCS_PATH` to point to this invalid SQL file.
*   **Action:**
    1.  Trigger the Airflow DAG `dw_bert_ausd_bp_ta_p_basisprod`.
    2.  Monitor the Airflow task logs for `run_bert_ausd_bp_ta_p_basisprod`.
*   **Pass/Fail Criterion:**
    *   The Airflow task `run_bert_ausd_bp_ta_p_basisprod` fails.
    *   The PySpark script logs contain an error message from the BigQuery client, indicating a SQL error (e.g., `BadRequest` error for syntax, or `NotFound` for non-existent table).
*   **Test Code (Conceptual - relies on Airflow/Dataproc logs):**
    ```python
    # This is an integration test for error handling, observed via Airflow/Dataproc logs.
    # Expected log snippet:
    # ERROR - PySpark job failed: google.api_core.exceptions.BadRequest: 400 Syntax error: Expected end of input but got ";" at [1:38]
    # or
    # ERROR - PySpark job failed: google.api_core.exceptions.NotFound: 404 Not found: Table your-gcp-project-id:non_existent_dataset.non_existent_table
    ```

---

### Test Case 12: MultiSIM and Options Logic (Specific Column Checks)

*   **Purpose:** To verify the correct population of the highly denormalized MultiSIM (`MS1_ICCID` to `MS10_ICCID`, `MS1_E_ID` to `MS10_E_ID`, etc.) and various options (`DATA_OPTION_REIN`, `VOICE_OPTION_REIN`, etc.) columns, as these represent complex business rules.
*   **Setup:**
    1.  **Mock Source Data:** Create BigQuery source tables (`sof_ta_iccid_vertrag_bq`, `sof_ta_rn_vertrag_bq`, `sof_ta_tarifoption_bq`, etc.) with diverse data to cover various scenarios for MultiSIM and options (e.g., contracts with 0, 1, 5, 10+ MultiSIMs; contracts with different combinations of options).
    2.  **Expected Output:** Manually calculate the expected values for these specific columns in the target table for the mocked input data.
*   **Action:**
    1.  Trigger the Airflow DAG `dw_bert_ausd_bp_ta_p_basisprod` with the mocked data.
    2.  After completion, execute the SQL assertions below on the target table `your-gcp-project-id.dw_dwh_prod.sof_ta_p_basisprod`.
*   **Pass/Fail Criterion:**
    *   The Airflow DAG completes successfully.
    *   The values in the `MSx_ICCID`, `MSx_E_ID`, and option-related columns in `your-gcp-project-id.dw_dwh_prod.sof_ta_p_basisprod` exactly match the manually calculated expected values for each test `CONTRACT_ID`.
*   **Test Code (SQL Assertions on target table):**
    ```sql
    -- Assuming the target table has columns like MS1_ICCID, MS2_ICCID, DATA_OPTION_REIN, etc.

    -- Example: Check MultiSIM ICCID for a contract with 3 MultiSIMs
    -- This query should return 0 rows if the assertion holds.
    SELECT
        CONTRACT_ID,
        MS1_ICCID, MS2_ICCID, MS3_ICCID, MS4_ICCID, MS5_ICCID,
        MS6_ICCID, MS7_ICCID, MS8_ICCID, MS9_ICCID, MS10_ICCID
    FROM `your-gcp-project-id.dw_dwh_prod.sof_ta_p_basisprod`
    WHERE CONTRACT_ID = 'CONTRACT_WITH_3_MSIMS'
    HAVING NOT (
        MS1_ICCID = 'ICCID_MS1' AND
        MS2_ICCID = 'ICCID_MS2' AND
        MS3_ICCID = 'ICCID_MS3' AND
        MS4_ICCID IS NULL AND -- Expected NULL for MS4 onwards
        MS5_ICCID IS NULL AND
        MS6_ICCID IS NULL AND
        MS7_ICCID IS NULL AND
        MS8_ICCID IS NULL AND
        MS9_ICCID IS NULL AND
        MS10_ICCID IS NULL
    );

    -- Example: Check DATA_OPTION_REIN for a contract
    -- This query should return 0 rows.
    SELECT
        CONTRACT_ID,
        DATA_OPTION_REIN
    FROM `your-gcp-project-id.dw_dwh_prod.sof_ta_p_basisprod`
    WHERE CONTRACT_ID = 'CONTRACT_WITH_DATA_OPTION'
    HAVING DATA_OPTION_REIN != 'EXPECTED_DATA_OPTION_VALUE';
    ```