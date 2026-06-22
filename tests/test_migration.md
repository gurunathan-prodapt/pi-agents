The migration of `r_ausd_austausch.ksh` to Google BigQuery and Cloud Composer requires comprehensive validation to ensure behavioral equivalence. The following test cases cover output parity, transformation correctness, external system replacements, and data quality assertions.

**Assumptions:**
*   You have access to both the legacy Unix environment to execute `r_ausd_austausch.ksh` and the Google Cloud Platform environment to interact with BigQuery and Cloud Composer.
*   A mechanism exists to reset/repopulate source tables (`contract_cache`) and target tables (`fos_table`, `job_log`, `job_status`) to a known state before each test run.
*   The legacy `fos_table` (or equivalent output) can be extracted and made available for comparison (e.g., as a CSV file or loaded into a temporary BigQuery table).
*   The placeholder columns (`some_data_column_1`, etc.) in `k_ausd_austausch` will be replaced with actual columns from `contract_cache` in the final implementation. Tests should be adapted accordingly.
*   `your_gcp_project` and `your_bq_dataset` are placeholders for your actual project and dataset names.

---

## Test Case 1: Default Parameter Handling and Core Logic Execution (Success Path)

*   **Purpose:** Verify that when no `Stichtag` or `Wiederanlaufwert` is explicitly provided, the migrated job correctly defaults `Stichtag` to the current date and `Wiederanlaufwert` to `0`, and then executes the core data preparation logic successfully, producing the same output as the legacy job.
*   **Setup:**
    1.  **Populate `contract_cache`:** Insert a diverse set of test data into `your_gcp_project.your_bq_dataset.contract_cache`. Include rows that should be selected by `CURRENT_DATE()` and `DWH_VERTRAG_ID > 0`, and some that should be filtered out by the `Gueltig_von`, `Gueltig_bis`, and `LADEDATUM` conditions relative to the current date.
        ```sql
        -- Example: Assuming today is '2023-10-27' (DDMMYYYY = '27102023')
        TRUNCATE TABLE `your_gcp_project.your_bq_dataset.contract_cache`;
        INSERT INTO `your_gcp_project.your_bq_dataset.contract_cache` (dwh_vertrag_id, gueltig_von, gueltig_bis, ladedatum, some_data_column_1, some_data_column_2, some_data_column_3) VALUES
        (101, '2023-01-01', '2024-01-01', '2023-10-26', 'data1', 'val1', 'info1'), -- Selected (DWH_VERTRAG_ID > 0, conditions met)
        (102, '2023-10-27', '2024-10-27', '2023-10-26', 'data2', 'val2', 'info2'), -- Selected (Gueltig_von <= Stichtag)
        (103, '2023-01-01', '2023-10-27', '2023-10-26', 'data3', 'val3', 'info3'), -- Filtered (Stichtag < Gueltig_bis is false)
        (104, '2023-01-01', '2024-01-01', '2023-10-27', 'data4', 'val4', 'info4'), -- Filtered (LADEDATUM < Stichtag is false)
        (105, '2023-01-01', '2024-01-01', '2023-10-26', 'data5', 'val5', 'info5'), -- Selected
        (0, '2023-01-01', '2024-01-01', '2023-10-26', 'data6', 'val6', 'info6');  -- Filtered (DWH_VERTRAG_ID > 0 is false)
        ```
    2.  **Clear Target Tables:** Ensure `your_gcp_project.your_bq_dataset.fos_table`, `job_log`, and `job_status` tables are empty.
    3.  **Note Current Date:** Record the current system date in `DDMMYYYY` format (e.g., `27102023`).
*   **Action:**
    1.  **Legacy System:** Execute `r_ausd_austausch.ksh` without any parameters:
        ```bash
        ./r_ausd_austausch.ksh
        ```
        Capture the exit code and the content of the generated log file (`$LogDatei`). Extract the final data from the legacy `fos_table` (or equivalent output) into a temporary file or table for comparison.
    2.  **Migrated System:** Call the BigQuery stored procedure without parameters:
        ```sql
        CALL `your_gcp_project.your_bq_dataset.BERT_AUSTAUSCH_KSH`();
        ```
*   **Pass/Fail Criterion:**
    *   **Output Parity (Data):**
        *   The number of rows in `your_gcp_project.your_bq_dataset.fos_table` must be identical to the number of rows inserted by the legacy script.
        *   The content of `your_gcp_project.your_bq_dataset.fos_table` must be identical to the data produced by the legacy script.
        ```python
        import pandas as pd
        from google.cloud import bigquery

        def test_default_params_output_parity(bq_client, legacy_fos_data_df):
            # legacy_fos_data_df should be a Pandas DataFrame loaded from the legacy output
            migrated_fos_data_df = bq_client.query(
                "SELECT * FROM `your_gcp_project.your_bq_dataset.fos_table` ORDER BY dwh_vertrag_id"
            ).to_dataframe()

            assert len(migrated_fos_data_df) == len(legacy_fos_data_df), "Row count mismatch"
            pd.testing.assert_frame_equal(
                migrated_fos_data_df, legacy_fos_data_df,
                check_dtype=False, check_column_type=False,
                obj="fos_table content"
            )
        ```
    *   **Transformation Correctness (Parameter Handling):**
        *   The `job_log` table should contain an entry for `BERT_AUSTAUSCH_KSH` where `stichtag_param` matches the current system date (DDMMYYYY format) and `wiederanlaufwert_param` is `0`.
        *   The `job_status` table should show `BERT_AUSTAUSCH_KSH` with `last_run_status = 'SUCCESS'` and `last_stichtag` matching the current date.
        ```sql
        SELECT stichtag_param, wiederanlaufwert_param, status
        FROM `your_gcp_project.your_bq_dataset.job_log`
        WHERE job_name = 'BERT_AUSTAUSCH_KSH'
        ORDER BY start_time DESC
        LIMIT 1;
        -- Expected: stichtag_param = '<CURRENT_DATE_DDMMYYYY>', wiederanlaufwert_param = 0, status = 'SUCCESS'

        SELECT last_run_status, last_stichtag
        FROM `your_gcp_project.your_bq_dataset.job_status`
        WHERE job_name = 'BERT_AUSTAUSCH_KSH';
        -- Expected: last_run_status = 'SUCCESS', last_stichtag = '<CURRENT_DATE_DDMMYYYY>'
        ```
    *   **Logging:**
        *   The `job_log` table should contain entries for both `BERT_AUSTAUSCH_KSH` and `k_ausd_austausch` with `status = 'RUNNING'` and `status = 'SUCCESS'`. The messages should reflect successful execution.
        *   The legacy script's log file should indicate successful completion (exit code 0).

---

## Test Case 2: Explicit Parameter Handling and Core Logic Execution

*   **Purpose:** Verify that the migrated job correctly processes explicitly provided `Stichtag` and `Wiederanlaufwert` parameters, including the deletion logic, and produces behaviorally equivalent results to the legacy job.
*   **Setup:**
    1.  **Populate `contract_cache`:** Insert data that will be filtered by specific `Stichtag` (`15062023`) and `Wiederanlaufwert` (`100`).
        ```sql
        TRUNCATE TABLE `your_gcp_project.your_bq_dataset.contract_cache`;
        INSERT INTO `your_gcp_project.your_bq_dataset.contract_cache` (dwh_vertrag_id, gueltig_von, gueltig_bis, ladedatum, some_data_column_1, some_data_column_2, some_data_column_3) VALUES
        (99, '2023-01-01', '2024-01-01', '2023-06-14', 'data_A', 'valA', 'infoA'), -- Filtered by DWH_VERTRAG_ID <= 100
        (100, '2023-01-01', '2024-01-01', '2023-06-14', 'data_D', 'valD', 'infoD'), -- Filtered by DWH_VERTRAG_ID <= 100
        (101, '2023-01-01', '2024-01-01', '2023-06-14', 'data_G', 'valG', 'infoG'), -- Selected
        (102, '2023-06-15', '2024-06-15', '2023-06-14', 'data_J', 'valJ', 'infoJ'), -- Selected
        (103, '2023-01-01', '2023-06-15', '2023-06-14', 'data_M', 'valM', 'infoM'), -- Filtered (Stichtag < Gueltig_bis is false)
        (104, '2023-01-01', '2024-01-01', '2023-06-15', 'data_P', 'valP', 'infoP'); -- Filtered (LADEDATUM < Stichtag is false)
        ```
    2.  **Populate `fos_table`:** Insert existing data, including rows that should be deleted by the `Wiederanlaufwert` logic (`dwh_vertrag_id >= 100`).
        ```sql
        TRUNCATE TABLE `your_gcp_project.your_bq_dataset.fos_table`;
        INSERT INTO `your_gcp_project.your_bq_dataset.fos_table` (dwh_vertrag_id, gueltig_von, gueltig_bis, ladedatum, some_data_column_1, some_data_column_2, some_data_column_3) VALUES
        (90, '2023-01-01', '2024-01-01', '2023-06-10', 'old_data_1', 'old_val1', 'old_info1'), -- Should remain
        (100, '2023-01-01', '2024-01-01', '2023-06-10', 'old_data_2', 'old_val2', 'old_info2'), -- Should be deleted
        (105, '2023-01-01', '2024-01-01', '2023-06-10', 'old_data_3', 'old_val3', 'old_info3'); -- Should be deleted
        ```
    3.  **Clear Logging Tables:** Ensure `job_log` and `job_status` tables are empty.
*   **Action:**
    1.  **Legacy System:** Execute `r_ausd_austausch.ksh` with specific parameters:
        ```bash
        ./r_ausd_austausch.ksh -s 15062023 -l 100
        ```
        Capture the exit code and the content of the generated log file. Extract the final data from the legacy `fos_table` for comparison.
    2.  **Migrated System:** Call the BigQuery stored procedure with explicit parameters:
        ```sql
        CALL `your_gcp_project.your_bq_dataset.BERT_AUSTAUSCH_KSH`(
            p_stichtag_in => '15062023',
            p_wiederanlaufWert_in => 100
        );
        ```
*   **Pass/Fail Criterion:**
    *   **Output Parity (Data):**
        *   The number of rows in `your_gcp_project.your_bq_dataset.fos_table` must be identical to the number of rows in the legacy `fos_table` after execution.
        *   The content of `your_gcp_project.your_bq_dataset.fos_table` must be identical to the data produced by the legacy script.
        ```python
        import pandas as pd
        from google.cloud import bigquery

        def test_explicit_params_output_parity(bq_client, legacy_fos_data_df):
            migrated_fos_data_df = bq_client.query(
                "SELECT * FROM `your_gcp_project.your_bq_dataset.fos_table` ORDER BY dwh_vertrag_id"
            ).to_dataframe()

            assert len(migrated_fos_data_df) == len(legacy_fos_data_df), "Row count mismatch"
            pd.testing.assert_frame_equal(
                migrated_fos_data_df, legacy_fos_data_df,
                check_dtype=False, check_column_type=False,
                obj="fos_table content"
            )
        ```
    *   **Transformation Correctness (Filtering & Deletion):**
        *   Verify that `dwh_vertrag_id` values less than `100` (from `fos_table` initial data) are retained, and those greater than or equal to `100` are deleted and replaced by new inserts where `dwh_vertrag_id > 100`.
        *   The `job_log` table should contain an entry for `BERT_AUSTAUSCH_KSH` where `stichtag_param` is `'15062023'` and `wiederanlaufwert_param` is `100`.
        *   The `job_status` table should show `BERT_AUSTAUSCH_KSH` with `last_run_status = 'SUCCESS'` and `last_stichtag = '15062023'`.
    *   **Logging:**
        *   The `job_log` table should contain entries for both `BERT_AUSTAUSCH_KSH` and `k_ausd_austausch` with `status = 'RUNNING'` and `status = 'SUCCESS'`.
        *   The legacy script's log file should indicate successful completion (exit code 0) and reflect the passed parameters.

---

## Test Case 3: Error Handling - Invalid Stichtag Format

*   **Purpose:** Verify that the migrated job correctly handles invalid `Stichtag` input, logs the error, updates job status, and propagates the failure, mirroring the legacy script's error behavior.
*   **Setup:**
    1.  **Populate `contract_cache`:** Insert some valid data.
    2.  **Clear Target Tables:** Ensure `your_gcp_project.your_bq_dataset.fos_table`, `job_log`, and `job_status` tables are empty.
*   **Action:**
    1.  **Legacy System:** Execute `r_ausd_austausch.ksh` with an invalid `Stichtag` format:
        ```bash
        ./r_ausd_austausch.ksh -s 2023-10-27
        ```
        Capture the exit code and the content of the generated log file.
    2.  **Migrated System:** Call the BigQuery stored procedure with an invalid `Stichtag` format:
        ```sql
        CALL `your_gcp_project.your_bq_dataset.BERT_AUSTAUSCH_KSH`(
            p_stichtag_in => '2023-10-27' -- Invalid DDMMYYYY format
        );
        ```
*   **Pass/Fail Criterion:**
    *   **Error Propagation:**
        *   **Legacy:** The script must exit with a non-zero error code (e.g., `192` or `193` if parameter parsing fails, or a different code if date parsing fails later). The log file must contain an error message indicating a problem with the `Stichtag`.
        *   **Migrated:** The `CALL` statement must raise an error (e.g., `Invalid date format`), preventing successful completion.
        ```python
        import pytest
        from google.cloud import bigquery

        def test_invalid_stichtag_error_propagation(bq_client):
            with pytest.raises(bigquery.exceptions.GoogleCloudError, match="Invalid date format"):
                bq_client.query("CALL `your_gcp_project.your_bq_dataset.BERT_AUSTAUSCH_KSH`(p_stichtag_in => '2023-10-27');").result()
        ```
    *   **Logging and Status:**
        *   The `job_log` table should contain an entry for `BERT_AUSTAUSCH_KSH` with `status = 'FAILED'`. The `message` column should contain details about the invalid date format (e.g., "Invalid date format: '2023-10-27'").
        *   The `job_status` table should show `BERT_AUSTAUSCH_KSH` with `last_run_status = 'FAILED'`.
        *   `fos_table` must remain unchanged (no data inserted or deleted).

---

## Test Case 4: External System Replacement - Cloud Composer Orchestration

*   **Purpose:** Verify that the Cloud Composer DAG successfully triggers the `BERT_AUSTAUSCH_KSH` stored procedure and correctly passes parameters from Airflow's `dag_run.conf`.
*   **Setup:**
    1.  Deploy the `dags/bert_austausch_ksh_dag.py` DAG to a Cloud Composer environment.
    2.  **Populate `contract_cache`:** Insert test data that will be filtered by the chosen parameters.
    3.  **Clear Target Tables:** Ensure `your_gcp_project.your_bq_dataset.fos_table`, `job_log`, and `job_status` tables are empty.
*   **Action:**
    1.  Trigger the `bert_austausch_ksh_dag` from the Airflow UI, providing configuration parameters:
        ```json
        {
            "stichtag_in": "01012023",
            "wiederanlaufwert_in": 50
        }
        ```
    2.  Monitor the DAG run in the Airflow UI until completion.
*   **Pass/Fail Criterion:**
    *   The Airflow DAG run must complete successfully (green status).
    *   The `call_bert_austausch_ksh_sp` task in the DAG must succeed.
    *   **Parameter Passing:** Query `job_log` for the latest run of `BERT_AUSTAUSCH_KSH`. The `stichtag_param` should be `'01012023'` and `wiederanlaufwert_param` should be `50`.
        ```sql
        SELECT stichtag_param, wiederanlaufwert_param, status
        FROM `your_gcp_project.your_bq_dataset.job_log`
        WHERE job_name = 'BERT_AUSTAUSCH_KSH'
        ORDER BY start_time DESC
        LIMIT 1;
        -- Expected: stichtag_param = '01012023', wiederanlaufwert_param = 50, status = 'SUCCESS'
        ```
    *   **Job Status Update:** The `job_status` table should show `BERT_AUSTAUSCH_KSH` with `last_run_status = 'SUCCESS'` and `last_stichtag = '01012023'`.
    *   **Data Output:** `fos_table` should contain data filtered according to `Stichtag = '2023-01-01'` and `DWH_VERTRAG_ID > 50`.

---

## Test Case 5: Data Quality - Row Count and Content Parity

*   **Purpose:** Verify that for a given set of inputs, the total number of rows and the actual data content in the target `fos_table` are identical between the legacy and migrated systems.
*   **Setup:**
    1.  **Populate `contract_cache`:** Insert a comprehensive, representative dataset covering various scenarios for `Gueltig_von`, `Gueltig_bis`, `LADEDATUM`, and `DWH_VERTRAG_ID`. This dataset should be identical for both legacy and migrated runs.
    2.  **Populate `fos_table`:** Insert some initial data that will be affected by the `Wiederanlaufwert` deletion logic. This initial state must be identical for both runs.
    3.  Choose a specific `Stichtag` (e.g., `31122022`) and `Wiederanlaufwert` (e.g., `200`) for the test.
    4.  **Clear Logging Tables:** Ensure `job_log` and `job_status` tables are empty.
*   **Action:**
    1.  **Legacy System:** Execute `r_ausd_austausch.ksh` with the chosen parameters.
        ```bash
        ./r_ausd_austausch.ksh -s 31122022 -l 200
        ```
        Extract the final data from the legacy `fos_table` (or equivalent target) into a structured format (e.g., CSV, JSON). Load this into a temporary BigQuery table, e.g., `your_gcp_project.your_bq_dataset.legacy_fos_table_snapshot`.
    2.  **Migrated System:** Call the BigQuery stored procedure with the same parameters.
        ```sql
        CALL `your_gcp_project.your_bq_dataset.BERT_AUSTAUSCH_KSH`(
            p_stichtag_in => '31122022',
            p_wiederanlaufWert_in => 200
        );
        ```
*   **Pass/Fail Criterion:**
    *   **Row Count Parity:** The `COUNT(*)` from `your_gcp_project.your_bq_dataset.fos_table` must be exactly equal to the row count from the `legacy_fos_table_snapshot`.
        ```sql
        SELECT COUNT(*) FROM `your_gcp_project.your_bq_dataset.fos_table`;
        SELECT COUNT(*) FROM `your_gcp_project.your_bq_dataset.legacy_fos_table_snapshot`;
        -- Both counts must be equal.
        ```
    *   **Data Content Parity:** A deep comparison of the data in `your_gcp_project.your_bq_dataset.fos_table` against the `legacy_fos_table_snapshot` must show no differences.
        ```sql
        -- Query 1: Rows in migrated but not in legacy
        SELECT * FROM `your_gcp_project.your_bq_dataset.fos_table`
        EXCEPT DISTINCT
        SELECT * FROM `your_gcp_project.your_bq_dataset.legacy_fos_table_snapshot`;

        -- Query 2: Rows in legacy but not in migrated
        SELECT * FROM `your_gcp_project.your_bq_dataset.legacy_fos_table_snapshot`
        EXCEPT DISTINCT
        SELECT * FROM `your_gcp_project.your_bq_dataset.fos_table`;
        -- Both queries must return 0 rows.
        ```
        *(Note: Ensure column order and data types are consistent between the two tables for `EXCEPT DISTINCT` to work correctly. You might need to explicitly cast or reorder columns in the SELECT statements.)*

---

## Test Case 6: Schema Validation of Target Tables

*   **Purpose:** Verify that the schemas of the target `fos_table`, `job_log`, and `job_status` tables in BigQuery conform to the expected structure and data types as defined in the migration design.
*   **Setup:**
    1.  Ensure the BigQuery tables (`fos_table`, `job_log`, `job_status`) have been created according to the DDL in the build plan.
*   **Action:**
    1.  Inspect the schema of each table using BigQuery's `INFORMATION_SCHEMA` views or the BigQuery UI/CLI.
*   **Pass/Fail Criterion:**
    *   **`fos_table` Schema:**
        *   Must contain `dwh_vertrag_id` (INT64), `gueltig_von` (DATE), `gueltig_bis` (DATE), `ladedatum` (DATE), and the placeholder `some_data_column_X` columns (e.g., STRING, INT64) with appropriate types. All columns should be NULLABLE unless explicitly defined as NOT NULL in the design.
        ```sql
        SELECT column_name, data_type, is_nullable
        FROM `your_gcp_project.your_bq_dataset.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'fos_table'
        ORDER BY ordinal_position;
        /* Expected output (adjust placeholder columns):
        column_name        data_type   is_nullable
        dwh_vertrag_id     INT64       YES
        gueltig_von        DATE        YES
        gueltig_bis        DATE        YES
        ladedatum          DATE        YES
        some_data_column_1 STRING      YES
        some_data_column_2 STRING      YES
        some_data_column_3 STRING      YES
        */
        ```
    *   **`job_log` Schema:**
        *   Must contain `job_id` (STRING), `job_name` (STRING), `start_time` (TIMESTAMP), `end_time` (TIMESTAMP), `status` (STRING), `message` (STRING), `stichtag_param` (STRING), `wiederanlaufwert_param` (INT64).
        ```sql
        SELECT column_name, data_type, is_nullable
        FROM `your_gcp_project.your_bq_dataset.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'job_log'
        ORDER BY ordinal_position;
        /* Expected output:
        column_name           data_type   is_nullable
        job_id                STRING      YES
        job_name              STRING      YES
        start_time            TIMESTAMP   YES
        end_time              TIMESTAMP   YES
        status                STRING      YES
        message               STRING      YES
        stichtag_param        STRING      YES
        wiederanlaufwert_param INT64       YES
        */
        ```
    *   **`job_status` Schema:**
        *   Must contain `job_id` (STRING), `job_name` (STRING), `last_run_status` (STRING), `last_run_time` (TIMESTAMP), `last_success_time` (TIMESTAMP), `last_stichtag` (STRING).
        ```sql
        SELECT column_name, data_type, is_nullable
        FROM `your_gcp_project.your_bq_dataset.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'job_status'
        ORDER BY ordinal_position;
        /* Expected output:
        column_name         data_type   is_nullable
        job_id              STRING      YES
        job_name            STRING      YES
        last_run_status     STRING      YES
        last_run_time       TIMESTAMP   YES
        last_success_time   TIMESTAMP   YES
        last_stichtag       STRING      YES
        */
        ```

---

## Test Case 7: Empty Source Data Handling

*   **Purpose:** Verify that the migrated job handles an empty `contract_cache` gracefully, resulting in an empty `fos_table` and a successful job status, mirroring the legacy behavior.
*   **Setup:**
    1.  **Empty `contract_cache`:** Ensure `your_gcp_project.your_bq_dataset.contract_cache` is completely empty.
    2.  **Clear Target Tables:** Ensure `your_gcp_project.your_bq_dataset.fos_table`, `job_log`, and `job_status` tables are empty.
*   **Action:**
    1.  **Legacy System:** Execute `r_ausd_austausch.ksh` (e.g., with default parameters):
        ```bash
        ./r_ausd_austausch.ksh
        ```
        Capture the exit code and log.
    2.  **Migrated System:** Call the BigQuery stored procedure (e.g., with default parameters):
        ```sql
        CALL `your_gcp_project.your_bq_dataset.BERT_AUSTAUSCH_KSH`();
        ```
*   **Pass/Fail Criterion:**
    *   **Output Parity:**
        *   `your_gcp_project.your_bq_dataset.fos_table` must remain empty.
        *   The legacy system's target output should also be empty.
        ```sql
        SELECT COUNT(*) FROM `your_gcp_project.your_bq_dataset.fos_table`;
        -- Expected: 0
        ```
    *   **Job Status:**
        *   Both legacy and migrated jobs should complete successfully (exit code 0 for legacy, `status = 'SUCCESS'` in `job_log` and `job_status` for migrated).
    *   **Logging:**
        *   `job_log` should show successful completion messages for both `BERT_AUSTAUSCH_KSH` and `k_ausd_austausch`.

---

## Test Case 8: No Data to Insert After Filtering

*   **Purpose:** Verify that the migrated job handles scenarios where `contract_cache` contains data, but all rows are filtered out by the `Stichtag` or `Wiederanlaufwert` conditions, resulting in an empty `fos_table` and a successful job status.
*   **Setup:**
    1.  **Populate `contract_cache`:** Insert data into `your_gcp_project.your_bq_dataset.contract_cache`, but ensure that *all* rows will be filtered out by the chosen `Stichtag` and `Wiederanlaufwert` conditions.
        *   Example: `contract_cache` has `dwh_vertrag_id` values 1-10. Set `Wiederanlaufwert = 10`.
        ```sql
        TRUNCATE TABLE `your_gcp_project.your_bq_dataset.contract_cache`;
        INSERT INTO `your_gcp_project.your_bq_dataset.contract_cache` (dwh_vertrag_id, gueltig_von, gueltig_bis, ladedatum, some_data_column_1, some_data_column_2, some_data_column_3) VALUES
        (1, '2023-01-01', '2023-01-02', '2022-12-31', 'a', 'b', 'c'),
        (5, '2023-01-01', '2023-01-02', '2022-12-31', 'd', 'e', 'f');
        -- All rows will be filtered out if Stichtag = '01012023' and Wiederanlaufwert = 10 (since DWH_VERTRAG_ID > 10 is false for all).
        ```
    2.  **Clear Target Tables:** Ensure `your_gcp_project.your_bq_dataset.fos_table`, `job_log`, and `job_status` tables are empty.
*   **Action:**
    1.  **Legacy System:** Execute `r_ausd_austausch.ksh` with parameters that filter out all data:
        ```bash
        ./r_ausd_austausch.ksh -s 01012023 -l 10
        ```
        Capture the exit code and log.
    2.  **Migrated System:** Call the BigQuery stored procedure with the same parameters:
        ```sql
        CALL `your_gcp_project.your_bq_dataset.BERT_AUSTAUSCH_KSH`(
            p_stichtag_in => '01012023',
            p_wiederanlaufWert_in => 10
        );
        ```
*   **Pass/Fail Criterion:**
    *   **Output Parity:**
        *   `your_gcp_project.your_bq_dataset.fos_table` must remain empty.
        *   The legacy system's target output should also be empty.
        ```sql
        SELECT COUNT(*) FROM `your_gcp_project.your_bq_dataset.fos_table`;
        -- Expected: 0
        ```
    *   **Job Status:**
        *   Both legacy and migrated jobs should complete successfully.
    *   **Logging:**
        *   `job_log` should show successful completion messages for both `BERT_AUSTAUSCH_KSH` and `k_ausd_austausch`.