As a senior data-migration QA engineer, I've developed a comprehensive suite of validation tests for the migration of `r_ausd_v_ta_p_vertrag.ksh` to Google Cloud Platform. These tests are designed to ensure behavioral equivalence across output parity, transformation correctness, external system replacements, and data quality assertions.

---

## Migration Validation Tests: `r_ausd_v_ta_p_vertrag.ksh`

### 1. Output Parity - Full Data Set Comparison

*   **Purpose:** To confirm that the migrated BigQuery job produces an identical final output dataset in `sof_dwh.ta_p_vertrag` as the legacy Oracle job produces in `sof$ta_p_vertrag`, given the same input data. This is the most critical test for overall behavioral equivalence.
*   **Setup:**
    1.  **Data Migration:** Ensure a full, representative dataset from Oracle's `sof$ta_vertrag_tmp` and `isbert_schema.dwtk_meldungen` has been accurately migrated to BigQuery's `sof_dwh.ta_vertrag_tmp` and `isbert_dwh.dwtk_meldungen` respectively.
    2.  **Clean State:** Truncate both `sof$ta_p_vertrag` (Oracle) and `sof_dwh.ta_p_vertrag` (BigQuery) to ensure a clean starting point.
*   **Action:**
    1.  Execute the legacy job (`r_ausd_v_ta_p_vertrag.ksh`) in the Oracle environment.
    2.  Execute the migrated Airflow DAG (`dag_ta_p_vertrag_sync`) in the BigQuery environment.
    3.  Extract the entire content of `sof$ta_p_vertrag` from Oracle and `sof_dwh.ta_p_vertrag` from BigQuery.
*   **Pass/Fail Criterion:**
    *   The row count of `sof$ta_p_vertrag` (Oracle) must be exactly equal to the row count of `sof_dwh.ta_p_vertrag` (BigQuery).
    *   A deep comparison (e.g., using `EXCEPT DISTINCT` in SQL or a programmatic row-by-row comparison after sorting by a unique key) must show no differences between the two tables.

    ```sql
    -- BigQuery: Check for rows in BigQuery that are NOT in Oracle's snapshot
    -- (Assuming 'oracle_snapshot_ta_p_vertrag' is a BigQuery table containing data extracted from Oracle)
    SELECT 'Rows in BigQuery but not in Oracle' AS comparison_type, COUNT(*) AS diff_count
    FROM (
        SELECT * FROM `your-gcp-project-id.sof_dwh.ta_p_vertrag`
        EXCEPT DISTINCT
        SELECT * FROM `your-gcp-project-id.test_snapshots.oracle_snapshot_ta_p_vertrag`
    );

    -- BigQuery: Check for rows in Oracle's snapshot that are NOT in BigQuery
    SELECT 'Rows in Oracle but not in BigQuery' AS comparison_type, COUNT(*) AS diff_count
    FROM (
        SELECT * FROM `your-gcp-project-id.test_snapshots.oracle_snapshot_ta_p_vertrag`
        EXCEPT DISTINCT
        SELECT * FROM `your-gcp-project-id.sof_dwh.ta_p_vertrag`
    );
    ```
    *   **Pass:** Both `diff_count` queries return `0`.
    *   **Fail:** Any `diff_count` query returns a value greater than `0`.

### 2. Transformation Correctness - Join Logic & NULL Handling

*   **Purpose:** To specifically validate the correct translation of Oracle's outer join syntax (`(+)`) to BigQuery's `LEFT JOIN` and to ensure consistent handling of `NULL` values in join conditions and projected columns.
*   **Setup:**
    1.  **Targeted Test Data:** Populate `sof$ta_vertrag_tmp` (Oracle) and `sof_dwh.ta_vertrag_tmp` (BigQuery) with specific test cases for `twin_vertrag_id` and `vertrag_id_carmen`:
        *   Rows where `v.twin_vertrag_id` has a direct match in `pv.vertrag_id_carmen`.
        *   Rows where `v.twin_vertrag_id` has no match in `pv.vertrag_id_carmen` (expect `NULL`s for `pv` columns).
        *   Rows where `v.twin_vertrag_id` is `NULL`.
        *   Rows where `pv.vertrag_id_carmen` is `NULL` (should not affect `LEFT JOIN` from `v`'s perspective).
    2.  **Clean State:** Truncate both target tables.
*   **Action:**
    1.  Execute the legacy job on the Oracle environment.
    2.  Execute the migrated Airflow DAG on the BigQuery environment.
    3.  Extract the resulting data from both `sof$ta_p_vertrag` tables.
*   **Pass/Fail Criterion:**
    *   The output data in `sof$ta_p_vertrag` (Oracle) must be identical to `sof_dwh.ta_p_vertrag` (BigQuery) for all test cases, specifically verifying correct `NULL` propagation for non-matching `pv` records and accurate join behavior.
    *   Use the same deep comparison method as in Test Case 1.

### 3. Transformation Correctness - `v_datum` Derivation

*   **Purpose:** To verify that the logic for deriving the `v_datum` variable (used for logging in legacy, but its derivation is a key part of the migration) is functionally equivalent in BigQuery to its Oracle counterpart, including handling of `NULL` or missing source data.
*   **Setup:**
    1.  **Test Data:** Populate `isbert_schema.dwtk_meldungen` (Oracle) and `isbert_dwh.dwtk_meldungen` (BigQuery) with identical data, including:
        *   Rows with valid `timecreated` for `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
        *   Cases where `MAX(timecreated)` for `job_kennung = 'BERT_DROP_TEMP_TABLE'` would be `NULL` (e.g., no matching rows).
        *   Cases with different `timecreated` values to test `MAX` function.
*   **Action:**
    1.  Execute the Oracle SQL snippet to get `v_datum` from the legacy system.
        ```sql
        -- Oracle SQL*Plus equivalent logic
        SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS v_datum
        FROM isbert_schema.dwtk_meldungen m
        WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
        ```
    2.  Execute the BigQuery SQL from the `get_v_datum_task` to obtain the BigQuery `v_datum`.
        ```sql
        -- BigQuery SQL from get_v_datum_task
        SELECT COALESCE(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101') AS v_datum
        FROM `your-gcp-project-id.isbert_dwh.dwtk_meldungen` m
        WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
        ```
*   **Pass/Fail Criterion:** The `v_datum` value obtained from Oracle must exactly match the `v_datum` value obtained from BigQuery for all test data scenarios.

### 4. External-System Replacements - Truncation Operations

*   **Purpose:** To verify that all tables that were truncated in the legacy Oracle job (via `TRUNCATE TABLE` or `DWPA_UTIL_SKRIPT.runstatement`) are correctly truncated in the BigQuery environment using `TRUNCATE TABLE` statements.
*   **Setup:**
    1.  **Populate Tables:** Populate `sof$ta_p_vertrag` (Oracle) and `sof_dwh.ta_p_vertrag` (BigQuery) with dummy data.
    2.  **Populate Temp Tables:** Populate all `sof$ta_*` temporary tables listed in the `truncate_temp_tables_group` (Oracle and BigQuery) with dummy data.
*   **Action:**
    1.  Execute the legacy job on the Oracle environment.
    2.  Execute the migrated Airflow DAG on the BigQuery environment.
    3.  After execution, query the row counts for `sof$ta_p_vertrag` and all `sof$ta_*` temporary tables in Oracle.
    4.  After execution, query the row counts for `sof_dwh.ta_p_vertrag` and all `sof_dwh.ta_*` temporary tables in BigQuery.
*   **Pass/Fail Criterion:**
    *   All specified tables (target and temporary) in both Oracle and BigQuery environments must have a row count of `0` after the job execution.
    *   The list of tables truncated in BigQuery must precisely match the list of tables truncated in Oracle.

    ```python
    # Example pytest assertion for BigQuery truncation
    import pytest
    from google.cloud import bigquery

    @pytest.fixture(scope="module")
    def bq_client():
        return bigquery.Client(project="your-gcp-project-id")

    def test_all_temp_tables_truncated(bq_client):
        temp_tables_to_truncate = [
            "sof_dwh.ta_disc_zusgf",
            "sof_dwh.ta_discount",
            # ... all other tables from the Airflow DAG's list
            "sof_dwh.ta_vertrag_tmp",
            "sof_dwh.ta_action_assoc",
            "sof_dwh.ta_p_vertrag", # Target table is also truncated
        ]
        
        # Run the Airflow DAG here (e.g., via Airflow API or CLI in a test setup)
        # ...

        for table_name in temp_tables_to_truncate:
            dataset_id, table_id = table_name.split('.')
            query = f"SELECT COUNT(*) FROM `{dataset_id}.{table_id}`;"
            query_job = bq_client.query(query)
            rows = list(query_job.result())
            assert rows[0][0] == 0, f"Table {table_name} was not truncated. Row count: {rows[0][0]}"
    ```

### 5. Data-Quality / Row-Count / Schema Assertions - Row Count

*   **Purpose:** To verify that the total number of rows inserted into the target table (`sof_dwh.ta_p_vertrag`) is consistent with the legacy system, ensuring no rows are lost or duplicated during the transformation.
*   **Setup:**
    1.  **Identical Input:** Ensure `sof$ta_vertrag_tmp` (Oracle) and `sof_dwh.ta_vertrag_tmp` (BigQuery) contain identical data.
    2.  **Clean State:** Truncate both target tables.
*   **Action:**
    1.  Execute the legacy job on the Oracle environment.
    2.  Execute the migrated Airflow DAG on the BigQuery environment.
    3.  Query the row count from `sof$ta_p_vertrag` (Oracle) and `sof_dwh.ta_p_vertrag` (BigQuery).
*   **Pass/Fail Criterion:** The row count from Oracle's `sof$ta_p_vertrag` must exactly match the row count from BigQuery's `sof_dwh.ta_p_vertrag`.

    ```python
    # Example pytest assertion for row count
    import pytest
    from google.cloud import bigquery
    # Assume oracle_db_connector is a fixture for Oracle connection

    def test_target_table_row_count_parity(bq_client, oracle_db_connector):
        # Assume DAG has been run and Oracle job has been run
        # ...

        # Get BigQuery row count
        bq_query = "SELECT COUNT(*) FROM `your-gcp-project-id.sof_dwh.ta_p_vertrag`;"
        bq_job = bq_client.query(bq_query)
        bq_row_count = list(bq_job.result())[0][0]

        # Get Oracle row count
        with oracle_db_connector.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM sof$ta_p_vertrag;")
            oracle_row_count = cursor.fetchone()[0]

        assert bq_row_count == oracle_row_count, \
            f"Row count mismatch: BigQuery has {bq_row_count} rows, Oracle has {oracle_row_count} rows."
    ```

### 6. Data-Quality / Row-Count / Schema Assertions - Schema Validation

*   **Purpose:** To verify that the schema of the target table in BigQuery (`sof_dwh.ta_p_vertrag`) matches the expected schema, including column names, data types, and nullability, as derived from the Oracle source and migration design.
*   **Setup:**
    1.  Ensure the Airflow DAG has run successfully at least once to create the target table.
    2.  Have a documented expected BigQuery schema for `sof_dwh.ta_p_vertrag`.
*   **Action:**
    1.  Query the schema of `sof_dwh.ta_p_vertrag` in BigQuery using BigQuery's information schema or client libraries.
    2.  Compare this retrieved schema against the expected schema.
*   **Pass/Fail Criterion:** The BigQuery table schema (column names, data types, and nullability) for `sof_dwh.ta_p_vertrag` must exactly match the documented target schema.

    ```python
    # Example pytest assertion for schema validation
    import pytest
    from google.cloud import bigquery

    def test_target_table_schema(bq_client):
        expected_schema = [
            bigquery.SchemaField("vertrag_id_carmen", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("partner_id_carmen", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("rechdef_id_carmen", "STRING", mode="NULLABLE"),
            # ... define all 30+ expected fields with their types and modes
            bigquery.SchemaField("cntrct_validity_id", "STRING", mode="NULLABLE"),
        ]

        table_ref = bq_client.dataset("sof_dwh").table("ta_p_vertrag")
        table = bq_client.get_table(table_ref)

        # Convert actual schema to a comparable format (e.g., list of dicts or tuples)
        actual_schema_fields = sorted([(f.name, f.field_type, f.mode) for f in table.schema])
        expected_schema_fields = sorted([(f.name, f.field_type, f.mode) for f in expected_schema])

        assert actual_schema_fields == expected_schema_fields, "BigQuery table schema does not match expected schema."
    ```

### 7. Orchestration - Parameter Handling

*   **Purpose:** To verify that parameters (`JobKennung`, `EintragsNr`) passed to the Airflow DAG are correctly received and accessible within the DAG's tasks, mirroring the KornShell script's parameter parsing.
*   **Setup:**
    1.  Modify the Airflow DAG temporarily to include a PythonOperator that logs the received parameters.
    2.  Trigger the Airflow DAG with specific, non-default values for `JobKennung` and `EintragsNr` (e.g., via Airflow UI or CLI).
*   **Action:**
    1.  Trigger the Airflow DAG with custom parameters.
    2.  Review the Airflow task logs for the parameter-logging task.
*   **Pass/Fail Criterion:** The Airflow logs must clearly show the exact `JobKennung` and `EintragsNr` values that were passed when triggering the DAG.

    ```python
    # Example PythonOperator in DAG for testing
    from airflow.operators.python import PythonOperator

    def _check_dag_params(**kwargs):
        job_kennung = kwargs["params"].get("JobKennung")
        eintrags_nr = kwargs["params"].get("EintragsNr")
        print(f"DAG received JobKennung: {job_kennung}")
        print(f"DAG received EintragsNr: {eintrags_nr}")
        assert job_kennung == "TEST_JOB_KENNUNG"
        assert eintrags_nr == "TEST_EINTRAGS_NR"

    check_params_task = PythonOperator(
        task_id="check_dag_params",
        python_callable=_check_dag_params,
    )

    # Add this task at the beginning of the DAG:
    # check_params_task >> get_v_datum_task
    ```

### 8. Orchestration - Error Handling and Logging

*   **Purpose:** To verify that the Airflow DAG correctly identifies and handles errors during execution, providing appropriate logging and marking the DAG run as failed, similar to the legacy KornShell's `trap` mechanism.
*   **Setup:**
    1.  **Introduce Error:** Create a controlled error condition in the BigQuery environment:
        *   Option A: Revoke `bigquery.dataEditor` permissions for the service account on `sof_dwh.ta_p_vertrag` to cause the `main_insert_task` to fail.
        *   Option B: Temporarily rename `isbert_dwh.dwtk_meldungen` to make `get_v_datum_task` fail.
        *   Option C: Introduce a syntax error into `bq_d_ausd_v_ta_p_vertrag.sql` for a specific test run.
*   **Action:**
    1.  Execute the Airflow DAG with the introduced error condition.
    2.  Monitor the Airflow UI and Cloud Logging for the DAG run.
*   **Pass/Fail Criterion:**
    *   The Airflow DAG run must fail at the expected task.
    *   Airflow logs for the failed task must contain clear, meaningful error messages indicating the cause of the failure.
    *   No downstream tasks (e.g., `truncate_temp_tables_group` if `main_insert_task` failed) should execute.
    *   The overall DAG run status in Airflow must be marked as `failed`.

### 9. Idempotency

*   **Purpose:** To ensure that running the migrated job multiple times with the same input data produces the same result in the target table, without duplicates or unintended side effects, due to the `TRUNCATE` then `INSERT` pattern.
*   **Setup:**
    1.  Ensure `sof_dwh.ta_vertrag_tmp` contains a consistent set of input data.
    2.  Ensure `sof_dwh.ta_p_vertrag` is empty initially.
*   **Action:**
    1.  Run the Airflow DAG once.
    2.  Record the row count and a checksum/hash of the data in `sof_dwh.ta_p_vertrag`.
    3.  Run the Airflow DAG a second time immediately after the first.
    4.  Record the row count and a checksum/hash of the data in `sof_dwh.ta_p_vertrag` again.
*   **Pass/Fail Criterion:**
    *   The row count after the first run must be identical to the row count after the second run.
    *   The checksum/hash of the data after the first run must be identical to the checksum/hash after the second run.

    ```sql
    -- BigQuery: Calculate a checksum/hash for a table
    -- This query generates a single hash value for the entire table content,
    -- useful for quick idempotency checks.
    SELECT FARM_FINGERPRINT(TO_JSON_STRING(t)) AS table_hash
    FROM (
        SELECT * FROM `your-gcp-project-id.sof_dwh.ta_p_vertrag`
        ORDER BY vertrag_id_carmen, partner_id_carmen -- Order for consistent hashing
    ) AS t;
    ```