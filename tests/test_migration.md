As a senior data-migration QA engineer, I've analyzed the migration design for `DW.BERT_AUSD_V_TA_P_DISCOUNT_RR` and developed a suite of validation tests. These tests aim to ensure behavioral equivalence, data integrity, and correct system integration post-migration.

The tests are categorized to cover output parity, transformation correctness, external system replacements, and data quality assertions. Each test case includes its purpose, setup, action, and concrete pass/fail criteria, with runnable code examples where applicable.

---

### Test Case 1: End-to-End Data Parity (Golden Record Comparison)

*   **Purpose:** To ensure that the migrated job, when executed with a representative set of source data, produces an identical output in the BigQuery target table (`dw.ta_p_discount_rr`) as the legacy job produced in its Oracle target table (`sof$ta_p_discount_rr`). This is the primary behavioral equivalence test.
*   **Setup:**
    1.  **Golden Record Selection:** Identify a comprehensive "golden record" dataset from the legacy Oracle environment. This dataset should cover various scenarios, including:
        *   Rows where all join conditions are met.
        *   Rows in `ta_discount_rr` that have no matching `cntrct_id`/`obj_version` in `ta_cntrct_crs` (expected to be excluded).
        *   Rows in `ta_discount_rr` that have no matching `cntrct_template_id` in `ta_cntrct_templ` (expected to be excluded).
        *   Rows with `NULL` values in source columns that are selected into the target.
        *   Data in `dwtk_meldungen` for `BERT_DROP_TEMP_TABLE` to test date derivation, including cases for `MAX(timecreated)` and the `19000101` default.
    2.  **Legacy Run & Export:** Execute the legacy Oracle job with this golden record data. Export the resulting `sof$ta_p_discount_rr` table's data into a canonical format (e.g., CSV, Parquet) or load it into a temporary BigQuery table (`legacy_output_table`) to serve as the "expected output".
    3.  **BigQuery Source Setup:** Load the exact same golden record data into the corresponding BigQuery source tables: `dw.ta_discount_rr`, `dw.ta_cntrct_crs`, `dw.ta_cntrct_templ`, and `dw.dwtk_meldungen`.
    4.  **Target Table Clean-up:** Ensure the BigQuery target table `dw.ta_p_discount_rr` is empty before running the migrated job.
*   **Action:**
    1.  Trigger the Airflow DAG `dw_bert_ausd_v_ta_p_discount_rr` in the BigQuery environment.
    2.  Monitor the DAG execution and wait for it to complete successfully.
*   **Pass/Fail Criterion:**
    *   The Airflow DAG completes successfully.
    *   The data in the BigQuery target table `dw.ta_p_discount_rr` is *exactly identical* to the "expected output" from the legacy Oracle job (`legacy_output_table`). This includes row count, column values, and data types.
    *   **SQL Assertion (BigQuery):**
        ```sql
        -- Assuming 'legacy_output_table' is a temporary BigQuery table loaded with the Oracle golden output
        -- And 'dw.ta_p_discount_rr' is the actual output from the migrated job

        SELECT
          (SELECT COUNT(*) FROM `your-gcp-project-id.dw.ta_p_discount_rr`) = (SELECT COUNT(*) FROM `your-gcp-project-id.test_dataset.legacy_output_table`) AS row_count_match,
          (SELECT COUNT(*) FROM `your-gcp-project-id.dw.ta_p_discount_rr` EXCEPT DISTINCT SELECT * FROM `your-gcp-project-id.test_dataset.legacy_output_table`) = 0 AS no_extra_rows,
          (SELECT COUNT(*) FROM `your-gcp-project-id.test_dataset.legacy_output_table` EXCEPT DISTINCT SELECT * FROM `your-gcp-project-id.dw.ta_p_discount_rr`) = 0 AS no_missing_rows;
        ```
        *Pass if all three boolean results (`row_count_match`, `no_extra_rows`, `no_missing_rows`) are `TRUE`.*

---

### Test Case 2: Transformation Correctness - Join Logic (Inner Join Behavior)

*   **Purpose:** To verify that the `INNER JOIN` logic in the BigQuery SQL correctly filters out rows from `dw.ta_discount_rr` that do not have matching entries in `dw.ta_cntrct_crs` or `dw.ta_cntrct_templ`.
*   **Setup:**
    1.  **BigQuery Source Data:** Populate BigQuery source tables with specific test data to isolate join behavior:
        *   `dw.ta_discount_rr`:
            ```sql
            INSERT INTO `dw.ta_discount_rr` (cntrct_id, discount_id, disc_vector_ty, cntrct_obj_version, cntrct_template_id, disc_invoice_item_id, rabatt, rabatthoehe, rabattierte_rech_pos) VALUES
            (1, 101, 'TYPE_A', 1, 10, 1001, 10.0, 5.0, 100.0), -- All matches
            (2, 102, 'TYPE_B', 2, 20, 1002, 20.0, 10.0, 200.0), -- No match in ta_cntrct_crs (cntrct_id=2, obj_version=2)
            (3, 103, 'TYPE_C', 3, 30, 1003, 30.0, 15.0, 300.0), -- No match in ta_cntrct_templ (cntrct_template_id=30)
            (4, 104, 'TYPE_D', 4, 40, 1004, 40.0, 20.0, 400.0); -- All matches
            ```
        *   `dw.ta_cntrct_crs`:
            ```sql
            INSERT INTO `dw.ta_cntrct_crs` (cntrct_id, obj_version, contract_number) VALUES
            (1, 1, 'CN-001'),
            (4, 4, 'CN-004');
            ```
        *   `dw.ta_cntrct_templ`:
            ```sql
            INSERT INTO `dw.ta_cntrct_templ` (cntrct_template_id, cds_description) VALUES
            (10, 'Standard Template A'),
            (40, 'Standard Template D');
            ```
    2.  **Date Derivation Setup:** Ensure `dw.dwtk_meldungen` contains at least one record for `job_kennung = 'BERT_DROP_TEMP_TABLE'` so `v_datum` is derived (e.g., `INSERT INTO dw.dwtk_meldungen (timecreated, job_kennung) VALUES ('2023-10-27 10:00:00 UTC', 'BERT_DROP_TEMP_TABLE');`).
    3.  **Target Table Clean-up:** Ensure `dw.ta_p_discount_rr` is empty.
*   **Action:**
    1.  Trigger the Airflow DAG `dw_bert_ausd_v_ta_p_discount_rr`.
    2.  Monitor the DAG execution and wait for it to complete successfully.
*   **Pass/Fail Criterion:**
    *   The Airflow DAG completes successfully.
    *   The `dw.ta_p_discount_rr` table contains exactly 2 rows.
    *   The `contract_number` and `std_vertrag` columns for these rows are correctly populated based on the joins.
    *   **SQL Assertion (BigQuery):**
        ```sql
        SELECT
          COUNT(*) AS row_count,
          SUM(CASE WHEN cntrct_id = 1 AND contract_number = 'CN-001' AND std_vertrag = 'Standard Template A' THEN 1 ELSE 0 END) AS row1_match,
          SUM(CASE WHEN cntrct_id = 4 AND contract_number = 'CN-004' AND std_vertrag = 'Standard Template D' THEN 1 ELSE 0 END) AS row4_match
        FROM `your-gcp-project-id.dw.ta_p_discount_rr`;
        ```
        *Pass if `row_count = 2`, `row1_match = 1`, and `row4_match = 1`.*

---

### Test Case 3: Transformation Correctness - Date Derivation and Default Value

*   **Purpose:** To verify that the `v_datum` variable is correctly derived from `dw.dwtk_meldungen` (using `MAX(timecreated)` for `job_kennung = 'BERT_DROP_TEMP_TABLE'`) and defaults to `19000101` when no matching record is found.
*   **Setup:**
    1.  **Common Setup:** Populate `dw.ta_discount_rr`, `dw.ta_cntrct_crs`, and `dw.ta_cntrct_templ` with at least one set of matching rows to ensure an insert occurs if the `DECLARE/SET` block is successful.
    2.  **Scenario A (Date Found):**
        *   Clear `dw.dwtk_meldungen`.
        *   Populate `dw.dwtk_meldungen` with:
            ```sql
            INSERT INTO `dw.dwtk_meldungen` (timecreated, job_kennung) VALUES
            ('2023-10-26 10:00:00 UTC', 'BERT_DROP_TEMP_TABLE'),
            ('2023-10-25 10:00:00 UTC', 'OTHER_JOB'),
            ('2023-10-27 10:00:00 UTC', 'BERT_DROP_TEMP_TABLE'); -- Max timecreated
            ```
        *   Ensure `dw.ta_p_discount_rr` is empty.
    3.  **Scenario B (Default Date):**
        *   Clear `dw.dwtk_meldungen` (or ensure no records with `job_kennung = 'BERT_DROP_TEMP_TABLE'` exist).
        *   Ensure `dw.ta_p_discount_rr` is empty.
*   **Action:**
    1.  Run Scenario A: Trigger the Airflow DAG `dw_bert_ausd_v_ta_p_discount_rr`.
    2.  Run Scenario B: Reset `dw.dwtk_meldungen` and `dw.ta_p_discount_rr` as per setup, then trigger the Airflow DAG again.
*   **Pass/Fail Criterion:**
    *   The Airflow DAG completes successfully for both scenarios.
    *   Since `v_datum` is not directly inserted into the target table, this test primarily verifies the BigQuery SQL syntax and runtime behavior of the `DECLARE/SET` block. Successful completion of the job under these conditions indicates the date derivation logic (including the `COALESCE` to default) is syntactically correct and doesn't cause runtime errors.
    *   **Pytest (Conceptual, verifying job success):**
        ```python
        import pytest
        from airflow.models import DagBag
        from airflow.utils.state import State
        from datetime import datetime
        from google.cloud import bigquery

        @pytest.fixture(scope="module")
        def bigquery_client():
            return bigquery.Client(project="your-gcp-project-id")

        @pytest.fixture(scope="module")
        def dag_bag():
            return DagBag(dag_folder='dags/', include_examples=False)

        def _run_dag_task(dag_bag, dag_id, run_id_suffix):
            dag = dag_bag.get_dag(dag_id=dag_id)
            execution_date = datetime.now()
            dr = dag.create_dagrun(
                run_id=f"test_{run_id_suffix}_{execution_date.isoformat()}",
                state=State.RUNNING,
                execution_date=execution_date,
                start_date=execution_date,
                external_trigger=False,
            )
            # Assuming 'run_transformation_task' is the main task
            task = dr.get_task_instances()[0]
            task.run(start_date=execution_date, end_date=execution_date)
            return task.current_state()

        def test_date_derivation_scenario_a(dag_bag, bigquery_client):
            # Setup BigQuery source data for Scenario A (date found)
            bigquery_client.query("TRUNCATE TABLE `dw.dwtk_meldungen`").result()
            bigquery_client.query("INSERT INTO `dw.dwtk_meldungen` (timecreated, job_kennung) VALUES ('2023-10-27 10:00:00 UTC', 'BERT_DROP_TEMP_TABLE')").result()
            # ... setup other source tables with minimal data for successful insert ...
            bigquery_client.query("TRUNCATE TABLE `dw.ta_p_discount_rr`").result()

            assert _run_dag_task(dag_bag, 'dw_bert_ausd_v_ta_p_discount_rr', 'date_a') == State.SUCCESS

        def test_date_derivation_scenario_b(dag_bag, bigquery_client):
            # Setup BigQuery source data for Scenario B (no matching meldungen, default date)
            bigquery_client.query("TRUNCATE TABLE `dw.dwtk_meldungen`").result()
            # ... setup other source tables with minimal data for successful insert ...
            bigquery_client.query("TRUNCATE TABLE `dw.ta_p_discount_rr`").result()

            assert _run_dag_task(dag_bag, 'dw_bert_ausd_v_ta_p_discount_rr', 'date_b') == State.SUCCESS
        ```
        *Pass if both `test_date_derivation_scenario_a` and `test_date_derivation_scenario_b` pass.*

---

### Test Case 4: Transformation Correctness - NULL Handling

*   **Purpose:** To verify that `NULL` values in source columns are correctly propagated or handled according to BigQuery's default behavior, matching Oracle's behavior for `INNER JOIN` and column selection.
*   **Setup:**
    1.  **BigQuery Source Data with NULLs:** Populate BigQuery source tables with data including `NULL`s in various selected columns:
        *   `dw.ta_discount_rr`:
            ```sql
            INSERT INTO `dw.ta_discount_rr` (cntrct_id, discount_id, disc_vector_ty, cntrct_obj_version, cntrct_template_id, disc_invoice_item_id, rabatt, rabatthoehe, rabattierte_rech_pos) VALUES
            (1, 101, 'TYPE_A', 1, 10, NULL, 10.5, NULL, 100.0),
            (2, 102, NULL, 2, 20, 201, NULL, 20.0, NULL);
            ```
        *   `dw.ta_cntrct_crs`:
            ```sql
            INSERT INTO `dw.ta_cntrct_crs` (cntrct_id, obj_version, contract_number) VALUES
            (1, 1, 'CN-001'),
            (2, 2, NULL);
            ```
        *   `dw.ta_cntrct_templ`:
            ```sql
            INSERT INTO `dw.ta_cntrct_templ` (cntrct_template_id, cds_description) VALUES
            (10, 'Template A'),
            (20, NULL);
            ```
    2.  **Date Derivation Setup:** Ensure `dw.dwtk_meldungen` has a record for `BERT_DROP_TEMP_TABLE`.
    3.  **Target Table Clean-up:** Ensure `dw.ta_p_discount_rr` is empty.
*   **Action:**
    1.  Trigger the Airflow DAG `dw_bert_ausd_v_ta_p_discount_rr`.
    2.  Monitor the DAG execution and wait for it to complete successfully.
*   **Pass/Fail Criterion:**
    *   The Airflow DAG completes successfully.
    *   The `dw.ta_p_discount_rr` table contains 2 rows.
    *   `NULL` values are correctly present in the output for the corresponding columns as expected from the source data.
    *   **SQL Assertion (BigQuery):**
        ```sql
        SELECT
          COUNT(*) AS row_count,
          SUM(CASE WHEN cntrct_id = 1 AND disc_invoice_item_id IS NULL AND rabatthoehe IS NULL AND contract_number = 'CN-001' AND std_vertrag = 'Template A' THEN 1 ELSE 0 END) AS row1_null_check,
          SUM(CASE WHEN cntrct_id = 2 AND disc_vector_ty IS NULL AND rabatt IS NULL AND rabattierte_rech_pos IS NULL AND contract_number IS NULL AND std_vertrag IS NULL THEN 1 ELSE 0 END) AS row2_null_check
        FROM `your-gcp-project-id.dw.ta_p_discount_rr`;
        ```
        *Pass if `row_count = 2`, `row1_null_check = 1`, and `row2_null_check = 1`.*

---

### Test Case 5: Data Quality - Row Count Parity

*   **Purpose:** To ensure that the total number of rows inserted into the BigQuery target table matches the number of rows inserted by the legacy Oracle job for a given set of source data. This is a quick check for major discrepancies.
*   **Setup:**
    1.  **Production-like Data:** Populate BigQuery source tables (`dw.ta_discount_rr`, `dw.ta_cntrct_crs`, `dw.ta_cntrct_templ`, `dw.dwtk_meldungen`) with a large, representative dataset (e.g., a full day's or week's worth of production data). This dataset should be identical to what was used for a legacy run.
    2.  **Legacy Row Count:** Obtain the `COUNT(*)` from the legacy Oracle `sof$ta_p_discount_rr` table after it has been populated by the legacy job with the chosen dataset. Record this as `expected_row_count_oracle`.
    3.  **Target Table Clean-up:** Ensure `dw.ta_p_discount_rr` is empty.
*   **Action:**
    1.  Trigger the Airflow DAG `dw_bert_ausd_v_ta_p_discount_rr`.
    2.  Monitor the DAG execution and wait for it to complete successfully.
*   **Pass/Fail Criterion:**
    *   The Airflow DAG completes successfully.
    *   The `COUNT(*)` from `dw.ta_p_discount_rr` matches `expected_row_count_oracle`.
    *   **SQL Assertion (BigQuery):**
        ```sql
        -- Replace <expected_row_count_oracle> with the actual count from the legacy system
        SELECT COUNT(*) = <expected_row_count_oracle> AS row_count_match
        FROM `your-gcp-project-id.dw.ta_p_discount_rr`;
        ```
        *Pass if `row_count_match` is `TRUE`.*

---

### Test Case 6: Schema Parity and Data Type Handling

*   **Purpose:** To verify that the BigQuery target table `dw.ta_p_discount_rr` has the correct schema (column names, data types, nullability) as inferred from the Oracle source and the provided DDL, and that data types are handled without loss or error during insertion.
*   **Setup:**
    1.  **DDL Execution:** Ensure the `ddl_tables.sql.bq` has been executed to create `dw.ta_p_discount_rr` with the specified schema.
    2.  **Source Data:** Populate source tables with data that covers the range of expected values for each data type (e.g., large integers, decimals with precision, long strings, `NULL`s) to test type compatibility.
    3.  **Legacy Schema Reference:** Obtain the precise schema of the legacy Oracle table `sof$ta_p_discount_rr` (column name, Oracle data type, precision/scale, nullability).
*   **Action:**
    1.  Trigger the Airflow DAG `dw_bert_ausd_v_ta_p_discount_rr`.
    2.  Monitor the DAG execution and wait for it to complete successfully.
    3.  Inspect the schema of `dw.ta_p_discount_rr` in BigQuery using the BigQuery UI or information schema queries.
*   **Pass/Fail Criterion:**
    *   The Airflow DAG completes successfully.
    *   The column names and their ordinal position in `dw.ta_p_discount_rr` match the legacy table.
    *   The BigQuery data types (`INT64`, `STRING`, `NUMERIC`) are appropriate mappings for the Oracle types and can accommodate the data without truncation, overflow, or conversion errors.
    *   Nullability constraints (if explicitly defined in Oracle and intended to be migrated) are respected. BigQuery columns are nullable by default unless `NOT NULL` is specified.
    *   **SQL Assertion (BigQuery Information Schema):**
        ```sql
        SELECT
          column_name,
          data_type,
          is_nullable,
          ordinal_position
        FROM `your-gcp-project-id.dw.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'ta_p_discount_rr'
        ORDER BY ordinal_position;
        ```
        *Pass if the output matches the expected BigQuery schema derived from the Oracle legacy schema, considering BigQuery's type system and default nullability.*
        *Example Expected Output (based on provided DDL):*
        ```
        column_name           data_type   is_nullable  ordinal_position
        ----------------------------------------------------------------
        cntrct_id             INT64       YES          1
        discount_id           INT64       YES          2
        disc_vector_ty        STRING      YES          3
        cntrct_obj_version    INT64       YES          4
        cntrct_template_id    INT64       YES          5
        disc_invoice_item_id  INT64       YES          6
        rabatt                NUMERIC     YES          7
        rabatthoehe           NUMERIC     YES          8
        rabattierte_rech_pos  NUMERIC     YES          9
        contract_number       STRING      YES          10
        std_vertrag           STRING      YES          11
        ```

---

### Test Case 7: External System Replacement - Airflow/PySpark Orchestration and Error Handling

*   **Purpose:** To verify that the Airflow DAG correctly triggers the PySpark job, the PySpark job executes the BigQuery SQL, and that errors originating from the BigQuery SQL execution are caught and reported by the PySpark wrapper, leading to a failed Airflow task. This validates the new orchestration and error propagation mechanism.
*   **Setup:**
    1.  Ensure the Airflow DAG, PySpark script, and BigQuery SQL are deployed to the GCP environment.
    2.  **Scenario A (Successful Run):** Populate source tables with valid data that will allow the BigQuery SQL to execute without error.
    3.  **Scenario B (SQL Error Simulation):** Temporarily modify the `sql_scripts/d_ausd_v_ta_p_discount_rr.sql.bq` file (or a copy used for testing) to introduce a deliberate syntax error (e.g., `SELECT FROM dw.ta_discount_rr` instead of `SELECT * FROM ...`) or a semantic error (e.g., attempting to insert a `STRING` into an `INT64` column). Deploy this erroneous SQL to the GCS bucket where the PySpark job expects to find it.
*   **Action:**
    1.  Run Scenario A: Trigger the Airflow DAG `dw_bert_ausd_v_ta_p_discount_rr`.
    2.  Run Scenario B: After deploying the erroneous SQL, trigger the Airflow DAG `dw_bert_ausd_v_ta_p_discount_rr` again.
*   **Pass/Fail Criterion:**
    *   **Scenario A:** The Airflow DAG completes successfully, and the `run_transformation_task` shows a `success` status.
    *   **Scenario B:** The Airflow DAG's `run_transformation_task` fails. The logs for the PySpark job (accessible via Dataproc or Cloud Logging) should clearly show the BigQuery SQL error message, and the PySpark script should exit with a non-zero status code, which Airflow correctly interprets as a task failure.
    *   **Pytest (Conceptual, verifying task status):**
        ```python
        import pytest
        from airflow.models import DagBag
        from airflow.utils.state import State
        from datetime import datetime
        from google.cloud import bigquery
        import os

        # Assume helper functions for setting up source data and running DAG tasks from Test Case 3

        def test_orchestration_success(dag_bag, bigquery_client):
            # Setup valid source data for a successful run
            bigquery_client.query("TRUNCATE TABLE `dw.dwtk_meldungen`").result()
            bigquery_client.query("INSERT INTO `dw.dwtk_meldungen` (timecreated, job_kennung) VALUES ('2023-10-27 10:00:00 UTC', 'BERT_DROP_TEMP_TABLE')").result()
            bigquery_client.query("TRUNCATE TABLE `dw.ta_discount_rr`").result()
            bigquery_client.query("INSERT INTO `dw.ta_discount_rr` (cntrct_id, discount_id, cntrct_obj_version, cntrct_template_id) VALUES (1, 101, 1, 10)").result()
            bigquery_client.query("TRUNCATE TABLE `dw.ta_cntrct_crs`").result()
            bigquery_client.query("INSERT INTO `dw.ta_cntrct_crs` (cntrct_id, obj_version, contract_number) VALUES (1, 1, 'CN1')").result()
            bigquery_client.query("TRUNCATE TABLE `dw.ta_cntrct_templ`").result()
            bigquery_client.query("INSERT INTO `dw.ta_cntrct_templ` (cntrct_template_id, cds_description) VALUES (10, 'Template A')").result()
            bigquery_client.query("TRUNCATE TABLE `dw.ta_p_discount_rr`").result()

            assert _run_dag_task(dag_bag, 'dw_bert_ausd_v_ta_p_discount_rr', 'orchestration_success') == State.SUCCESS

        def test_orchestration_failure_handling(dag_bag, bigquery_client):
            # This test requires deploying a BigQuery SQL file with a deliberate error
            # to the GCS bucket that the PySpark job reads from.
            # For a real automated test, this would involve:
            # 1. Creating a temporary erroneous SQL file.
            # 2. Uploading it to a test GCS path.
            # 3. Modifying the DataprocSubmitJobOperator's `file_uris` to point to this erroneous file.
            # 4. Running the DAG.
            # 5. Asserting failure and checking logs.
            # 6. Cleaning up the erroneous file.

            # For this example, we'll assume the setup for an erroneous SQL file is done
            # and focus on asserting the task failure.
            # In a local Airflow test, the DataprocSubmitJobOperator might not actually
            # submit to Dataproc, so mocking might be necessary.
            # If running against a real Composer/Dataproc, the actual deployment of bad SQL
            # is the key setup step.

            # Setup minimal valid source data, but expect the SQL to fail
            bigquery_client.query("TRUNCATE TABLE `dw.dwtk_meldungen`").result()
            bigquery_client.query("INSERT INTO `dw.dwtk_meldungen` (timecreated, job_kennung) VALUES ('2023-10-27 10:00:00 UTC', 'BERT_DROP_TEMP_TABLE')").result()
            bigquery_client.query("TRUNCATE TABLE `dw.ta_discount_rr`").result()
            bigquery_client.query("INSERT INTO `dw.ta_discount_rr` (cntrct_id, discount_id, cntrct_obj_version, cntrct_template_id) VALUES (1, 101, 1, 10)").result()
            bigquery_client.query("TRUNCATE TABLE `dw.ta_cntrct_crs`").result()
            bigquery_client.query("INSERT INTO `dw.ta_cntrct_crs` (cntrct_id, obj_version, contract_number) VALUES (1, 1, 'CN1')").result()
            bigquery_client.query("TRUNCATE TABLE `dw.ta_cntrct_templ`").result()
            bigquery_client.query("INSERT INTO `dw.ta_cntrct_templ` (cntrct_template_id, cds_description) VALUES (10, 'Template A')").result()
            bigquery_client.query("TRUNCATE TABLE `dw.ta_p_discount_rr`").result()

            # The actual assertion for failure:
            # This will typically involve catching an AirflowException from task.run()
            # or asserting the state after a simulated run.
            try:
                _run_dag_task(dag_bag, 'dw_bert_ausd_v_ta_p_discount_rr', 'orchestration_failure')
                pytest.fail("Expected task to fail, but it succeeded.")
            except Exception as e: # DataprocSubmitJobOperator raises AirflowException on job failure
                # Check if the exception indicates a task failure
                assert "Dataproc job failed" in str(e) or "Task failed" in str(e) # Simplified check
                # In a real scenario, you'd check the actual task state from Airflow DB
                # For this conceptual test, we assume the exception indicates failure.
                pass # Expected failure
        ```
        *Pass if `test_orchestration_success` passes and `test_orchestration_failure_handling` correctly asserts a failure.*

---

### Test Case 8: Idempotency (Truncate Behavior)

*   **Purpose:** To verify that running the job multiple times with the same source data produces the exact same result in the target table, due to the `TRUNCATE TABLE` operation ensuring a clean slate before each insertion. This confirms the `TRUNCATE` replacement for `DWPA_UTIL_SKRIPT.runstatement` works as expected.
*   **Setup:**
    1.  **Consistent Source Data:** Populate BigQuery source tables (`dw.ta_discount_rr`, `dw.ta_cntrct_crs`, `dw.ta_cntrct_templ`, `dw.dwtk_meldungen`) with a consistent and unchanging set of test data.
    2.  **Target Table Clean-up:** Ensure `dw.ta_p_discount_rr` is empty.
*   **Action:**
    1.  **First Run:** Trigger the Airflow DAG `dw_bert_ausd_v_ta_p_discount_rr`.
    2.  Monitor the DAG execution and wait for it to complete successfully.
    3.  Record the `COUNT(*)` and a deterministic hash/checksum of the data in `dw.ta_p_discount_rr`.
    4.  **Second Run:** Trigger the Airflow DAG `dw_bert_ausd_v_ta_p_discount_rr` *again* with the exact same source data (without modifying source tables between runs).
    5.  Monitor the DAG execution and wait for the second run to complete successfully.
*   **Pass/Fail Criterion:**
    *   Both Airflow DAG runs complete successfully.
    *   The `COUNT(*)` and the deterministic hash/checksum of the data in `dw.ta_p_discount_rr` after the second run are *identical* to those recorded after the first run. This confirms the `TRUNCATE` and subsequent `INSERT` behave idempotently.
    *   **SQL Assertion (BigQuery):**
        ```sql
        -- After first run, capture count and a data hash (e.g., array of row fingerprints)
        -- Example:
        -- SELECT COUNT(*) FROM `your-gcp-project-id.dw.ta_p_discount_rr`;
        -- SELECT ARRAY_AGG(FARM_FINGERPRINT(TO_JSON_STRING(t)) ORDER BY 1) FROM `your-gcp-project-id.dw.ta_p_discount_rr` AS t;

        -- Let's assume these values are stored in variables:
        -- first_run_count = <count_from_first_run>
        -- first_run_data_hash_array = <hash_array_from_first_run>

        -- After second run, compare:
        SELECT
          (SELECT COUNT(*) FROM `your-gcp-project-id.dw.ta_p_discount_rr`) = first_run_count AS count_match,
          (SELECT ARRAY_AGG(FARM_FINGERPRINT(TO_JSON_STRING(t)) ORDER BY 1) FROM `your-gcp-project-id.dw.ta_p_discount_rr` AS t) = first_run_data_hash_array AS data_match;
        ```
        *Pass if both `count_match` and `data_match` are `TRUE`.*

---