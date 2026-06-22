As a senior data-migration QA engineer, I've analyzed the provided Migration Design Document and the Generated Migration Code for `DW.BERT_AUSD_BP_TA_TARIFOPTION`. The migration involves re-platforming from Oracle/KornShell/UC4 to BigQuery/PySpark/Airflow.

A critical observation is the `concat_placeholder_udf` which explicitly states its placeholder nature. This is a **high-risk item** and means that full output parity for transformations relying on these functions cannot be guaranteed until the actual logic is implemented and re-tested. The tests below will validate the placeholder's behavior but emphasize the need for re-validation.

Another key area is the `LEAD` analytic function's `ORDER BY` clause. The Oracle `ORDER BY NULL` was translated to `ORDER BY cntrct_id, pds_description` in BigQuery. This change introduces a deterministic order where the original might have been non-deterministic or implicitly ordered. This needs careful validation with business stakeholders to ensure the new deterministic order aligns with business requirements.

The tests are designed to cover output parity, transformation correctness, external system replacements, and data quality/schema assertions.

---

## Migration Validation Tests: DW.BERT_AUSD_BP_TA_TARIFOPTION

### Prerequisites for all Tests:

*   **Golden Dataset**: A representative dataset from the legacy Oracle environment has been extracted and loaded into BigQuery. This dataset includes:
    *   `isbert_schema.dwtk_meldungen`
    *   `isbert_schema.sof_ta_l_bpr_optionen_filter`
    *   `isbert_schema.sof_ta_bpr_opt_text_YYYYMMDD` (for a specific `YYYYMMDD` date, e.g., `20231026`)
    *   All necessary source tables for the legacy job are available in BigQuery with identical data.
*   **Legacy Output Snapshots**: The output tables (`SOF$TA_BPR_OPT_FILTER`, `SOF$TA_TARIFOPTION`) from a successful run of the legacy Oracle job using the golden dataset have been captured and stored for comparison.
*   **GCP Environment Setup**:
    *   BigQuery dataset `isbert_schema` exists.
    *   `concat_placeholder_udf` is deployed to BigQuery.
    *   PySpark script (`r_ausd_bp_ta_tarifoption_main.py`) is uploaded to GCS.
    *   BigQuery SQL script (`d_ausd_bp_ta_tarifoption.sql`) is uploaded to GCS.
    *   Airflow DAG (`dw_bert_ausd_bp_ta_tarifoption.py`) is deployed to Cloud Composer.
    *   Dataproc cluster (`your-dataproc-cluster`) is available and configured correctly.
*   **Configuration**: Replace placeholders like `your-gcp-project-id`, `your-gcp-region`, `your-dataproc-cluster`, `gs://your-gcs-bucket` with actual values in the DAG definition.

---

### Test Case 1: End-to-End Output Parity (Golden Dataset)

*   **Purpose**: To verify that the migrated job, when executed with the same input data as the legacy job, produces identical final output data in `SOF$TA_TARIFOPTION`. This is the primary validation for behavioral equivalence.
*   **Setup**:
    1.  Load the "golden dataset" into the BigQuery source tables (`isbert_schema.dwtk_meldungen`, `isbert_schema.sof_ta_l_bpr_optionen_filter`, `isbert_schema.sof_ta_bpr_opt_text_YYYYMMDD`).
    2.  Ensure the `timecreated` in `dwtk_meldungen` for `BERT_DROP_TEMP_TABLE` is set such that `v_datum` resolves to the `YYYYMMDD` of the dynamic table (`sof_ta_bpr_opt_text_YYYYMMDD`).
    3.  Have the legacy job's final output (`SOF$TA_TARIFOPTION`) available as a reference (e.g., in a BigQuery table `isbert_schema.legacy_sof_ta_tarifoption_golden`).
*   **Action**:
    1.  Manually trigger the `dw_bert_ausd_bp_ta_tarifoption` Airflow DAG in Cloud Composer.
    2.  Monitor the DAG run to ensure successful completion of the Dataproc PySpark job and subsequent BigQuery SQL execution.
    3.  Once complete, query the target BigQuery table `isbert_schema.sof_ta_tarifoption`.
*   **Pass/Fail Criterion**:
    *   The Airflow DAG completes successfully.
    *   The row count of `isbert_schema.sof_ta_tarifoption` matches the row count of `isbert_schema.legacy_sof_ta_tarifoption_golden`.
    *   A full data comparison (e.g., using `EXCEPT DISTINCT`) between `isbert_schema.sof_ta_tarifoption` and `isbert_schema.legacy_sof_ta_tarifoption_golden` yields zero differences.

    ```sql
    -- Pass/Fail Criterion SQL
    -- Check row counts
    SELECT
        (SELECT COUNT(*) FROM `your-gcp-project-id.isbert_schema.sof_ta_tarifoption`) AS migrated_row_count,
        (SELECT COUNT(*) FROM `your-gcp-project-id.isbert_schema.legacy_sof_ta_tarifoption_golden`) AS legacy_row_count;

    -- Check for data differences (should return 0 rows)
    SELECT 'Differences found in Migrated vs Legacy' AS status, * FROM (
        (SELECT * FROM `your-gcp-project-id.isbert_schema.sof_ta_tarifoption`
         EXCEPT DISTINCT
         SELECT * FROM `your-gcp-project-id.isbert_schema.legacy_sof_ta_tarifoption_golden`)
        UNION ALL
        (SELECT * FROM `your-gcp-project-id.isbert_schema.legacy_sof_ta_tarifoption_golden`
         EXCEPT DISTINCT
         SELECT * FROM `your-gcp-project-id.isbert_schema.sof_ta_tarifoption`)
    ) AS diffs;
    ```
    **Note**: This test is highly dependent on the `concat_placeholder_udf` being correctly implemented. If the UDF is still a placeholder, this test will likely fail or pass incorrectly if the placeholder logic happens to match the legacy logic for the golden dataset. **Re-run this test once the actual `concatX` logic is implemented.**

---

### Test Case 2: `v_datum` Determination and Dynamic Table Naming

*   **Purpose**: To verify that the `v_datum` variable is correctly calculated based on `dwtk_meldungen` and that the dynamic table name for `sof_ta_bpr_opt_text_&v_datum` is correctly constructed and used.
*   **Setup**:
    1.  Populate `isbert_schema.dwtk_meldungen` with various `timecreated` values for `job_kennung = 'BERT_DROP_TEMP_TABLE'`, including NULLs and a specific maximum date (e.g., `2023-10-26 10:00:00 UTC`).
    2.  Ensure a table `isbert_schema.sof_ta_bpr_opt_text_20231026` exists with some test data.
    3.  Ensure no table `isbert_schema.sof_ta_bpr_opt_text_19000101` exists, or if it does, it's empty.
*   **Action**:
    1.  Execute the BigQuery SQL script `d_ausd_bp_ta_tarifoption.sql` directly in BigQuery (or trigger the DAG and inspect logs/intermediate tables).
    2.  Observe the `v_datum` value and the table used for `sof_ta_bpr_opt_filter`.
*   **Pass/Fail Criterion**:
    *   The `v_datum` variable, as determined by the `DECLARE/SET` block in `d_ausd_bp_ta_tarifoption.sql`, correctly reflects `IFNULL(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')` from `isbert_schema.dwtk_meldungen`.
    *   The `CREATE TABLE isbert_schema.sof_ta_bpr_opt_filter` statement successfully executes, joining with the dynamically named table (e.g., `isbert_schema.sof_ta_bpr_opt_text_20231026`).
    *   If `dwtk_meldungen` is empty or `MAX(timecreated)` is NULL, `v_datum` should be `19000101`, and the job should attempt to use `isbert_schema.sof_ta_bpr_opt_text_19000101`.

    ```sql
    -- Example SQL to verify v_datum calculation (run manually or via test framework)
    DECLARE v_datum_test STRING;
    SET v_datum_test = (
      SELECT
        IFNULL(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
      FROM
        `isbert_schema.dwtk_meldungen` m
      WHERE
        m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    );
    SELECT v_datum_test AS expected_v_datum;

    -- After running the full job, check if the intermediate table was created correctly
    -- This implicitly verifies the dynamic table naming.
    SELECT COUNT(*) FROM `your-gcp-project-id.isbert_schema.sof_ta_bpr_opt_filter`;
    ```

---

### Test Case 3: Intermediate Table (`SOF$TA_BPR_OPT_FILTER`) Transformation Correctness

*   **Purpose**: To validate the join logic, column selection, and data integrity when populating the intermediate table `isbert_schema.sof_ta_bpr_opt_filter`.
*   **Setup**:
    1.  Populate `isbert_schema.sof_ta_l_bpr_optionen_filter` and `isbert_schema.sof_ta_bpr_opt_text_YYYYMMDD` with diverse test data, including:
        *   Matching `bpr_id` values.
        *   Non-matching `bpr_id` values (to test inner join behavior).
        *   NULLs in `bpr_id` in one or both tables.
        *   Duplicate `bpr_id` values.
    2.  Have the legacy job's intermediate output (`SOF$TA_BPR_OPT_FILTER`) available as a reference (e.g., `isbert_schema.legacy_sof_ta_bpr_opt_filter_golden`).
*   **Action**:
    1.  Execute the Airflow DAG.
    2.  Query the `isbert_schema.sof_ta_bpr_opt_filter` table.
*   **Pass/Fail Criterion**:
    *   The row count of `isbert_schema.sof_ta_bpr_opt_filter` matches `isbert_schema.legacy_sof_ta_bpr_opt_filter_golden`.
    *   A full data comparison (e.g., using `EXCEPT DISTINCT`) between `isbert_schema.sof_ta_bpr_opt_filter` and `isbert_schema.legacy_sof_ta_bpr_opt_filter_golden` yields zero differences.
    *   Verify that `NULL` values in `bpr_id` are correctly handled by the inner join (i.e., rows with `NULL` `bpr_id` are excluded).

    ```sql
    -- Pass/Fail Criterion SQL
    -- Check row counts
    SELECT
        (SELECT COUNT(*) FROM `your-gcp-project-id.isbert_schema.sof_ta_bpr_opt_filter`) AS migrated_row_count,
        (SELECT COUNT(*) FROM `your-gcp-project-id.isbert_schema.legacy_sof_ta_bpr_opt_filter_golden`) AS legacy_row_count;

    -- Check for data differences (should return 0 rows)
    SELECT 'Differences found in Migrated vs Legacy Intermediate' AS status, * FROM (
        (SELECT * FROM `your-gcp-project-id.isbert_schema.sof_ta_bpr_opt_filter`
         EXCEPT DISTINCT
         SELECT * FROM `your-gcp-project-id.isbert_schema.legacy_sof_ta_bpr_opt_filter_golden`)
        UNION ALL
        (SELECT * FROM `your-gcp-project-id.isbert_schema.legacy_sof_ta_bpr_opt_filter_golden`
         EXCEPT DISTINCT
         SELECT * FROM `your-gcp-project-id.isbert_schema.sof_ta_bpr_opt_filter`)
    ) AS diffs;
    ```

---

### Test Case 4: `LEAD` Analytic Function and `ORDER BY` Behavior

*   **Purpose**: To verify the correct translation and behavior of the `LEAD` analytic function, especially considering the change from Oracle's `ORDER BY NULL` to BigQuery's explicit `ORDER BY cntrct_id, pds_description`.
*   **Setup**:
    1.  Populate `isbert_schema.sof_ta_bpr_opt_filter` with data that specifically highlights `LEAD` function behavior:
        *   Multiple rows with the same `cntrct_id` but different `pds_description`.
        *   Rows where `cntrct_id` changes.
        *   Edge cases for `LEAD` (e.g., first/last rows in a partition, or in this case, the entire dataset as there's no `PARTITION BY`).
    2.  Have the legacy job's `LEAD` output (specifically the `lagi` column and its impact on the final `WHERE` clause) available for comparison.
*   **Action**:
    1.  Execute the Airflow DAG.
    2.  Query the `isbert_schema.sof_ta_tarifoption` table and, if possible, the intermediate results of the `LEAD` calculation.
*   **Pass/Fail Criterion**:
    *   The `lagi` column values in the migrated `sof_ta_tarifoption` match the legacy `lagi` values (if available, or derived from the legacy logic).
    *   The final filtering `WHERE lagi > cntrct_id OR lagi = -1` produces the same set of rows as the legacy job.
    *   **Crucially**: Confirm with business stakeholders that the deterministic ordering introduced by `ORDER BY cntrct_id, pds_description` is acceptable and does not alter any implicit business logic that might have relied on the original `ORDER BY NULL` behavior. If the original was truly non-deterministic, then any deterministic order is technically "different" but might be acceptable.

    ```sql
    -- Example SQL to inspect the LEAD function's output (run after job completion)
    -- This requires recreating the inner query structure to see 'lagi' directly.
    SELECT
           bpr_opt.cntrct_id,
           bpr_opt.bpr_id,
           bpr_opt.pds_description,
           bpr_opt.opt_kategorie,
           LEAD(bpr_opt.cntrct_id, 1, -1) OVER () AS lagi_migrated,
           -- Compare with legacy_lagi if available from legacy output
           -- legacy_lagi
    FROM
    (
           SELECT
                  bpr_id,
                  cntrct_id,
                  pds_description,
                  opt_kategorie
           FROM
                  `your-gcp-project-id.isbert_schema.sof_ta_bpr_opt_filter`
           ORDER BY cntrct_id, pds_description
    ) AS bpr_opt
    ORDER BY cntrct_id, pds_description;
    ```

---

### Test Case 5: Custom UDF (`concat_placeholder_udf`) Behavior

*   **Purpose**: To verify that the placeholder UDF `isbert_schema.concat_placeholder_udf` is correctly invoked and produces its defined output. This test will need to be re-run once the actual `sof$ab_con.concatX` logic is implemented.
*   **Setup**:
    1.  Ensure `isbert_schema.concat_placeholder_udf` is deployed.
    2.  Populate `isbert_schema.sof_ta_bpr_opt_filter` with data that exercises the `CASE` statements and the UDF calls, including:
        *   Rows where `opt_kategorie` is 'BUDGET', 'SONST', 'GPRS'.
        *   Rows where `opt_kategorie` is none of the above.
        *   NULL values for `pds_description` and `cntrct_id`.
*   **Action**:
    1.  Execute the Airflow DAG.
    2.  Query the `isbert_schema.sof_ta_tarifoption` table and inspect the `business_option`, `sonstige_option`, `gprs_option` columns.
*   **Pass/Fail Criterion**:
    *   For all rows, the `business_option`, `sonstige_option`, `gprs_option` columns correctly reflect the output of `CONCAT(str1, ', ', str2)` (the placeholder logic) for the respective `pds_description` and `cntrct_id` inputs.
    *   NULL inputs to the UDF should result in NULL outputs for the concatenated string (BigQuery `CONCAT` handles NULLs by returning NULL if any argument is NULL, unless `CONCAT(IFNULL(str1, ''), IFNULL(str2, ''))` is used).
    *   **Critical**: Once the actual `sof$ab_con.concatX` logic is implemented in the UDF, this test must be re-executed, and the pass/fail criterion will be based on comparing the output of the *actual* UDF with the legacy Oracle function's output.

    ```sql
    -- Example SQL to verify placeholder UDF behavior (run after job completion)
    SELECT
        cntrct_id,
        business_option,
        sonstige_option,
        gprs_option
    FROM
        `your-gcp-project-id.isbert_schema.sof_ta_tarifoption`
    WHERE
        -- Example: Check a specific row or pattern
        business_option LIKE '%, %' OR sonstige_option LIKE '%, %' OR gprs_option LIKE '%, %';

    -- Test NULL handling of the placeholder UDF directly
    SELECT
        `isbert_schema`.concat_placeholder_udf('Hello', 'World') AS test1, -- Expected: 'Hello, World'
        `isbert_schema`.concat_placeholder_udf('Hello', NULL) AS test2,    -- Expected: NULL
        `isbert_schema`.concat_placeholder_udf(NULL, 'World') AS test3,    -- Expected: NULL
        `isbert_schema`.concat_placeholder_udf(NULL, NULL) AS test4;       -- Expected: NULL
    ```

---

### Test Case 6: String Functions (`RTRIM`, `LTRIM`, `SUBSTR`) and `CASE` Statements

*   **Purpose**: To verify the correct translation and behavior of standard SQL string functions and `CASE` statements.
*   **Setup**:
    1.  Populate `isbert_schema.sof_ta_bpr_opt_filter` with data that specifically tests these functions:
        *   `pds_description` values with leading/trailing spaces, commas, and varying lengths.
        *   `opt_kategorie` values that trigger different branches of the `CASE` statements.
        *   NULL values for relevant columns.
*   **Action**:
    1.  Execute the Airflow DAG.
    2.  Query the `isbert_schema.sof_ta_tarifoption` table and inspect the `business_option`, `sonstige_option`, `gprs_option` columns.
*   **Pass/Fail Criterion**:
    *   `RTRIM(SUBSTR(LTRIM(pds_des1, ', '), 1, 500))` correctly processes strings:
        *   Leading commas are removed by `LTRIM`.
        *   The string is truncated to 500 characters by `SUBSTR`.
        *   Trailing spaces are removed by `RTRIM`.
        *   NULL inputs result in NULL outputs.
    *   The `CASE` statements correctly route to the appropriate UDF calls based on `opt_kategorie`.

    ```sql
    -- Example SQL to verify string function behavior (run after job completion)
    SELECT
        t.cntrct_id,
        t.business_option,
        t.sonstige_option,
        t.gprs_option,
        -- Reconstruct expected values based on the logic for comparison
        RTRIM(SUBSTR(LTRIM(
            CASE WHEN bpr_opt.opt_kategorie = 'BUDGET' THEN `isbert_schema`.concat_placeholder_udf(bpr_opt.pds_description, bpr_opt.cntrct_id)
                 ELSE `isbert_schema`.concat_placeholder_udf(bpr_opt.pds_description, bpr_opt.cntrct_id) END
        , ', '), 1, 500)) AS expected_business_option_logic
    FROM
        `your-gcp-project-id.isbert_schema.sof_ta_tarifoption` t
    JOIN (
        SELECT bpr_id, cntrct_id, pds_description, opt_kategorie
        FROM `your-gcp-project-id.isbert_schema.sof_ta_bpr_opt_filter`
        ORDER BY cntrct_id, pds_description
    ) AS bpr_opt ON t.cntrct_id = bpr_opt.cntrct_id -- Simplified join for example, actual logic is more complex
    WHERE
        t.business_option IS NOT NULL
    LIMIT 10; -- Inspect a few rows manually or build a full comparison query.
    ```

---

### Test Case 7: Data Quality - Row Counts and Schema Assertions

*   **Purpose**: To ensure that the migrated job maintains expected row counts and that the target table schemas are correct.
*   **Setup**:
    1.  Ensure source tables are populated.
    2.  Know the expected row counts for the intermediate and final tables from legacy runs or design specifications.
    3.  Have the expected schema definitions for `isbert_schema.sof_ta_bpr_opt_filter` and `isbert_schema.sof_ta_tarifoption`.
*   **Action**:
    1.  Execute the Airflow DAG.
    2.  Query row counts and schema information for the target tables.
*   **Pass/Fail Criterion**:
    *   **Row Counts**:
        *   `COUNT(*)` from `isbert_schema.sof_ta_bpr_opt_filter` matches the expected count.
        *   `COUNT(*)` from `isbert_schema.sof_ta_tarifoption` matches the expected count.
    *   **Schema**:
        *   The column names, data types, and nullability of `isbert_schema.sof_ta_bpr_opt_filter` match the design/legacy.
        *   The column names, data types, and nullability of `isbert_schema.sof_ta_tarifoption` match the design/legacy.

    ```sql
    -- Pass/Fail Criterion SQL for Row Counts
    SELECT
        (SELECT COUNT(*) FROM `your-gcp-project-id.isbert_schema.sof_ta_bpr_opt_filter`) AS sof_ta_bpr_opt_filter_rows,
        (SELECT COUNT(*) FROM `your-gcp-project-id.isbert_schema.sof_ta_tarifoption`) AS sof_ta_tarifoption_rows;

    -- Pass/Fail Criterion SQL for Schema (example for sof_ta_tarifoption)
    -- This query retrieves schema information. Manual comparison or automated script needed.
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM
        `your-gcp-project-id.isbert_schema.INFORMATION_SCHEMA.COLUMNS`
    WHERE
        table_name = 'sof_ta_tarifoption'
    ORDER BY
        ordinal_position;

    -- Expected Schema for sof_ta_tarifoption (example based on code)
    -- Column Name      | Data Type | Nullable
    -- -----------------|-----------|----------
    -- cntrct_id        | INT64     | YES (or based on source)
    -- business_option  | STRING    | YES
    -- sonstige_option  | STRING    | YES
    -- gprs_option      | STRING    | YES
    ```

---

### Test Case 8: Airflow Orchestration and Parameter Passing

*   **Purpose**: To verify that the Airflow DAG correctly triggers the PySpark job, and that the PySpark job receives and processes parameters as expected.
*   **Setup**:
    1.  Ensure the Airflow DAG is deployed.
    2.  Ensure the PySpark script is deployed to GCS.
    3.  Configure the DAG with a specific `start_date` and `schedule_interval`.
*   **Action**:
    1.  Manually trigger the `dw_bert_ausd_bp_ta_tarifoption` Airflow DAG with a specific `logical_date` (which will become `ds`).
    2.  Observe the Airflow task logs for `run_dw_bert_ausd_bp_ta_tarifoption`.
*   **Pass/Fail Criterion**:
    *   The Airflow DAG runs successfully without errors.
    *   The DataprocSubmitJobOperator task successfully launches the PySpark job.
    *   The PySpark job logs (`logging.info` messages) confirm that `stichtag` and `wiederanlaufwert` parameters were received correctly from Airflow macros (`{{ ds }}` and `{{ dag_run.run_id }}`).
    *   The PySpark job successfully calls `execute_bigquery_sql_template` with the correct path to the BigQuery SQL script.

    ```python
    # Example Pytest for Airflow DAG (conceptual, requires Airflow testing framework)
    from airflow.models.dagbag import DagBag
    from airflow.utils.state import State
    from datetime import datetime

    def test_dag_structure():
        dag_bag = DagBag(dag_folder='dags/', include_examples=False)
        assert 'dw_bert_ausd_bp_ta_tarifoption' in dag_bag.dags
        dag = dag_bag.dags['dw_bert_ausd_bp_ta_tarifoption']
        assert len(dag.tasks) == 1
        task = dag.tasks[0]
        assert task.task_id == 'run_dw_bert_ausd_bp_ta_tarifoption'
        assert isinstance(task, DataprocSubmitJobOperator)

    def test_dag_execution_and_params(mocker):
        # Mock DataprocSubmitJobOperator to prevent actual Dataproc calls
        mocker.patch('airflow.providers.google.cloud.operators.dataproc.DataprocSubmitJobOperator.execute')

        dag_bag = DagBag(dag_folder='dags/', include_examples=False)
        dag = dag_bag.dags['dw_bert_ausd_bp_ta_tarifoption']

        # Simulate a DAG run
        execution_date = datetime(2023, 10, 26)
        dag_run = dag.create_dagrun(
            run_id=f"test_run_{execution_date.isoformat()}",
            state=State.RUNNING,
            execution_date=execution_date,
            start_date=execution_date,
            external_trigger=True,
        )

        task_instance = dag_run.get_task_instance(task_id='run_dw_bert_ausd_bp_ta_tarifoption')
        task_instance.run(start_date=execution_date, end_date=execution_date)

        # Assert that DataprocSubmitJobOperator.execute was called with correct arguments
        DataprocSubmitJobOperator.execute.assert_called_once()
        call_args = DataprocSubmitJobOperator.execute.call_args[0][0] # context
        pyspark_job_args = call_args['job']['pyspark_job']['args']

        assert '--stichtag' in pyspark_job_args
        assert execution_date.strftime('%Y-%m-%d') in pyspark_job_args # {{ ds }}
        assert '--wiederanlaufwert' in pyspark_job_args
        assert dag_run.run_id in pyspark_job_args # {{ dag_run.run_id }}
        assert '--sql_template_path' in pyspark_job_args
        assert 'gs://your-gcs-bucket/bigquery_sql/d_ausd_bp_ta_tarifoption.sql' in pyspark_job_args
    ```

---

### Test Case 9: Error Handling and Robustness

*   **Purpose**: To verify that the migrated job handles expected error conditions gracefully (e.g., missing source tables, invalid data) and logs errors appropriately.
*   **Setup**:
    1.  **Scenario A (Missing Dynamic Table)**: Rename or drop `isbert_schema.sof_ta_bpr_opt_text_YYYYMMDD` so it's not found.
    2.  **Scenario B (Invalid Data)**: Introduce data that might cause type conversion errors or unexpected behavior (e.g., extremely long strings where `SUBSTR` might behave differently, or non-numeric `cntrct_id` if it's expected to be numeric).
    3.  **Scenario C (Empty Source)**: Make `isbert_schema.dwtk_meldungen` empty or without the specific `job_kennung`.
*   **Action**:
    1.  For each scenario, execute the Airflow DAG.
    2.  Monitor Airflow task logs and Dataproc driver logs.
*   **Pass/Fail Criterion**:
    *   **Scenario A**: The job should fail with a clear error message indicating the missing table.
    *   **Scenario B**: The job should either fail with a clear error message (if data is truly invalid) or process the data according to BigQuery's type coercion rules, which should be documented and understood.
    *   **Scenario C**: `v_datum` should correctly default to `19000101`, and the job should attempt to use `isbert_schema.sof_ta_bpr_opt_text_19000101`. If this table doesn't exist, it should fail gracefully.
    *   All errors should be logged with sufficient detail to diagnose the issue.
    *   Airflow task should be marked as `failed`.

    ```python
    # Example Pytest for error handling (conceptual)
    import pytest
    from unittest.mock import patch
    from google.cloud import bigquery.exceptions

    @patch('google.cloud.bigquery.Client.query')
    def test_missing_dynamic_table_error(mock_bq_query):
        # Simulate BigQuery error for missing table
        mock_bq_query.side_effect = bigquery.exceptions.NotFound('Table not found: isbert_schema.sof_ta_bpr_opt_text_20231026')

        with pytest.raises(Exception, match='Error executing BigQuery SQL'):
            # Call the PySpark main function directly or via a wrapper
            # main_pyspark_function(sql_template_path='src/bigquery/sql/d_ausd_bp_ta_tarifoption.sql', ...)
            pass # Placeholder for actual PySpark execution call

        # Assert that logging captured the error
        # assert "Error executing BigQuery SQL: Table not found" in caplog.text
    ```