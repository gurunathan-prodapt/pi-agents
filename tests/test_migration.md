Here are the migration validation tests for the `DW.BERT_AUSD_BP_TA_BCP_MSISDN` job, structured as requested.

---

## Migration Validation Tests: DW.BERT_AUSD_BP_TA_BCP_MSISDN

### Test Case 1: End-to-End Output Parity

*   **Purpose**: To verify that the migrated BigQuery pipeline produces an identical final output dataset in `sof.ta_bcp_msisdn` as the legacy Oracle job produced in `sof$ta_bcp_msisdn`, given the same input data. This test covers the entire transformation logic, including joins, filters, distinct operations, and data types.

*   **Setup**:
    1.  **Legacy Data Preparation**: Populate the legacy Oracle source tables (`isbert_schema.dwtk_meldungen`, `sof$ta_bpr_bcp`, `sof$ta_rn_vertrag`) with a comprehensive test dataset. This dataset should include:
        *   Typical matching `cntrct_id_ref` values.
        *   `cntrct_id_ref` values in `sof$ta_bpr_bcp` that have no corresponding `cntrct_id` in `sof$ta_rn_vertrag`.
        *   `cntrct_id` values in `sof$ta_rn_vertrag` that have no corresponding `cntrct_id_ref` in `sof$ta_bpr_bcp`.
        *   Scenarios that would result in duplicate rows *before* the `DISTINCT` clause, to ensure `DISTINCT` is handled correctly.
        *   NULL values in `cntrct_id_ref`, `cntrct_id`, and `tn_tel_msisdn`.
        *   `isbert_schema.dwtk_meldungen` entries covering various `timecreated` values and `job_kennung` values (including `BERT_DROP_TEMP_TABLE` and other values, and NULL `timecreated`).
    2.  **Legacy Execution**: Execute the legacy UC4 job (or its underlying Oracle SQL script `d_ausd_bp_ta_bcp_msisdn.sql` via SQL*Plus) to populate the legacy target table `sof$ta_bcp_msisdn`.
    3.  **Data Export**: Export the data from the populated `sof$ta_bcp_msisdn` table into a temporary BigQuery table (e.g., `temp_legacy_sof_ta_bcp_msisdn`). Ensure data types are mapped correctly during export.
    4.  **Migrated Data Preparation**: Load the *exact same* test dataset used in step 1 into the corresponding BigQuery source tables (`isbert_schema.dwtk_meldungen`, `sof.ta_bpr_bc`, `sof.ta_rn_vertrag`).
    5.  **Target Table State**: Ensure the BigQuery target table `sof.ta_bcp_msisdn` is empty before running the migrated job.

*   **Action**:
    1.  Trigger the migrated Airflow DAG `dw_bert_ausd_bp_ta_bcp_msisdn` in the BigQuery environment.
    2.  Wait for the DAG to complete successfully.

*   **Pass/Fail Criterion**:
    *   The row count of `your_project.sof.ta_bcp_msisdn` in BigQuery must be exactly equal to the row count of `your_project.temp_legacy_sof_ta_bcp_msisdn`.
    *   A full data comparison between `your_project.sof.ta_bcp_msisdn` and `your_project.temp_legacy_sof_ta_bcp_msisdn` (after ensuring consistent data types and ordering) must yield zero differences.

    ```sql
    -- BigQuery SQL for Row Count Comparison
    SELECT
        (SELECT COUNT(*) FROM `your_project.sof.ta_bcp_msisdn`) AS migrated_row_count,
        (SELECT COUNT(*) FROM `your_project.temp_legacy_sof_ta_bcp_msisdn`) AS legacy_row_count;

    -- BigQuery SQL for Data Difference (should return 0 rows)
    SELECT 'Migrated_Only' AS source, * FROM `your_project.sof.ta_bcp_msisdn`
    EXCEPT DISTINCT
    SELECT 'Migrated_Only' AS source, * FROM `your_project.temp_legacy_sof_ta_bcp_msisdn`
    UNION ALL
    SELECT 'Legacy_Only' AS source, * FROM `your_project.temp_legacy_sof_ta_bcp_msisdn`
    EXCEPT DISTINCT
    SELECT 'Legacy_Only' AS source, * FROM `your_project.sof.ta_bcp_msisdn`;
    ```

### Test Case 2: Transformation Correctness - `v_datum` Derivation and NULL Handling

*   **Purpose**: To verify that the logic for deriving the `v_datum` variable (using `MAX(m.timecreated)` and `COALESCE/NVL`) behaves identically in BigQuery as it did in Oracle, specifically covering NULL handling and date formatting.

*   **Setup**:
    1.  **Oracle Data**: Populate `isbert_schema.dwtk_meldungen` in Oracle with the following scenarios:
        *   **Scenario A**: Multiple entries for `job_kennung = 'BERT_DROP_TEMP_TABLE'` with valid, distinct `timecreated` values.
        *   **Scenario B**: No entries for `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
        *   **Scenario C**: One entry for `job_kennung = 'BERT_DROP_TEMP_TABLE'` with `timecreated` as NULL.
        *   **Scenario D**: One entry for `job_kennung = 'BERT_DROP_TEMP_TABLE'` with a valid `timecreated`.
        *   **Scenario E**: Entries for `job_kennung` *other than* `BERT_DROP_TEMP_TABLE`.
    2.  **BigQuery Data**: Load the *exact same* data into `your_project.isbert_schema.dwtk_meldungen` in BigQuery.

*   **Action**:
    1.  For each scenario (A-E), execute the Oracle SQL snippet to derive `v_datum`.
    2.  For each scenario (A-E), execute the BigQuery SQL snippet to derive `v_datum`.

*   **Pass/Fail Criterion**:
    *   The `v_datum` value derived from BigQuery must exactly match the `v_datum` value derived from Oracle for all scenarios.
    *   Specifically:
        *   Scenarios A, D: Should return the `YYYYMMDD` format of the maximum `timecreated`.
        *   Scenarios B, C: Should return `'19000101'` due to `COALESCE`/`NVL` handling of `NULL` `MAX(timecreated)`.
        *   Scenario E: Should not affect the `v_datum` calculation for `BERT_DROP_TEMP_TABLE`.

    ```sql
    -- Oracle SQL (for v_datum derivation)
    -- Replace with actual data for each scenario
    SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS v_datum
    FROM isbert_schema.dwtk_meldungen m
    WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';

    -- BigQuery SQL (for v_datum derivation)
    -- Replace with actual data for each scenario
    SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101') AS v_datum
    FROM `your_project.isbert_schema.dwtk_meldungen` AS m
    WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
    ```

### Test Case 3: Transformation Correctness - Join Logic and `DISTINCT`

*   **Purpose**: To isolate and verify the correctness of the `JOIN` condition (`bp.cntrct_id_ref = rn.cntrct_id`) and the `DISTINCT` clause, ensuring they produce identical results in BigQuery compared to Oracle.

*   **Setup**:
    1.  **Oracle Data**: Populate `sof$ta_bpr_bcp` and `sof$ta_rn_vertrag` in Oracle with data covering:
        *   **Scenario A**: Exact one-to-one matches for `cntrct_id_ref = cntrct_id`.
        *   **Scenario B**: One `bp.cntrct_id_ref` matching multiple `rn.cntrct_id` (should not happen with `DISTINCT` on the selected columns, but good to test).
        *   **Scenario C**: Multiple `bp` rows having the same `cntrct_id_ref` that matches a single `rn.cntrct_id` (tests `DISTINCT` on `bp.cntrct_id`, `bp.bpr_id`, `bp.cntrct_id_ref`, `rn.tn_tel_msisdn`).
        *   **Scenario D**: `bp.cntrct_id_ref` values that do not exist in `rn.cntrct_id` (should be excluded by `JOIN`).
        *   **Scenario E**: `rn.cntrct_id` values that do not exist in `bp.cntrct_id_ref` (should be excluded by `JOIN`).
        *   **Scenario F**: NULL values in `bp.cntrct_id_ref` or `rn.cntrct_id` (should be excluded by `JOIN`).
        *   **Scenario G**: Duplicate rows in `sof$ta_bpr_bcp` or `sof$ta_rn_vertrag` that, when joined, would produce duplicates *before* `DISTINCT`.
    2.  **BigQuery Data**: Load the *exact same* data into `your_project.sof.ta_bpr_bcp` and `your_project.sof.ta_rn_vertrag` in BigQuery.

*   **Action**:
    1.  Execute the core `SELECT DISTINCT` query from `d_ausd_bp_ta_bcp_msisdn.sql` in Oracle for each scenario.
    2.  Execute the core `SELECT DISTINCT` query from `d_ausd_bp_ta_bcp_msisdn_bq.sql` in BigQuery for each scenario.

*   **Pass/Fail Criterion**:
    *   The result sets (row count and content, ordered consistently) from both Oracle and BigQuery queries must be identical for all scenarios.
    *   Specifically, `DISTINCT` should correctly remove any duplicate output rows based on the selected columns.

    ```sql
    -- Oracle SQL (core select for comparison)
    SELECT DISTINCT
        bp.cntrct_id,
        bp.bpr_id,
        bp.cntrct_id_ref,
        rn.tn_tel_msisdn
    FROM sof$ta_bpr_bcp bp
    JOIN sof$ta_rn_vertrag rn ON bp.cntrct_id_ref = rn.cntrct_id
    ORDER BY 1,2,3,4; -- Order for consistent comparison

    -- BigQuery SQL (core select for comparison)
    SELECT DISTINCT
        bp.cntrct_id,
        bp.bpr_id,
        bp.cntrct_id_ref,
        rn.tn_tel_msisdn
    FROM `your_project.sof.ta_bpr_bcp` AS bp
    JOIN `your_project.sof.ta_rn_vertrag` AS rn ON bp.cntrct_id_ref = rn.cntrct_id
    ORDER BY 1,2,3,4; -- Order for consistent comparison
    ```

### Test Case 4: External System Replacement - Airflow Orchestration and Parameter Handling

*   **Purpose**: To verify that the Airflow DAG correctly orchestrates the tasks, and the Python `prepare_parameters_task` accurately replicates the KornShell logic for parameter parsing, default value assignment, and validation, passing results via XComs.

*   **Setup**:
    1.  Deploy the Airflow DAG `dw_bert_ausd_bp_ta_bcp_msisdn_dag.py` and the utility module `bert_bcp_msisdn_utils.py` to a Cloud Composer environment.
    2.  Ensure Airflow's logging is configured to capture task output.

*   **Action**:
    1.  **Scenario A (Default `stichtag`)**: Trigger the Airflow DAG manually without providing any `dag_run.conf` parameters.
    2.  **Scenario B (Valid `stichtag` and `wiederanlaufwert`)**: Trigger the Airflow DAG manually with `dag_run.conf = {'stichtag': '20231026', 'wiederanlaufwert': '1'}`.
    3.  **Scenario C (Invalid `stichtag`)**: Trigger the Airflow DAG manually with `dag_run.conf = {'stichtag': '2023-10-26'}`.
    4.  **Scenario D (Invalid `stichtag` format)**: Trigger the Airflow DAG manually with `dag_run.conf = {'stichtag': 'ABC'}`.

*   **Pass/Fail Criterion**:
    *   **Scenarios A & B**:
        *   The `prepare_parameters` task must complete successfully.
        *   The XCom value pushed by `prepare_parameters` (key `processed_job_parameters`) must contain the expected `job_execution_date` (current date for A, '20231026' for B) and `wiederanlaufwert` (None for A, '1' for B).
        *   The `log_job_status` task must execute and log the correct `job_execution_date` from XComs.
        *   The overall DAG run must succeed.
    *   **Scenarios C & D**:
        *   The `prepare_parameters` task must fail with a `ValueError` (as implemented in `bert_bcp_msisdn_utils.py`).
        *   The Airflow task logs for `prepare_parameters` must contain an error message indicating an invalid date format.
        *   The overall DAG run must be marked as failed.

    ```python
    # Example Pytest unit tests for bert_bcp_msisdn_utils.py (run locally)
    import pytest
    from datetime import datetime
    from unittest.mock import MagicMock
    from bert_bcp_msisdn_utils import validate_date_format, prepare_parameters_task, log_job_status_task

    def test_validate_date_format_valid():
        assert validate_date_format("20231026") == True

    def test_validate_date_format_invalid():
        assert validate_date_format("2023-10-26") == False
        assert validate_date_format("ABC") == False
        assert validate_date_format(None) == False

    def test_prepare_parameters_task_default_stichtag():
        mock_ti = MagicMock()
        mock_ti.xcom_push = MagicMock()
        with pytest.MonkeyPatch.context() as m:
            m.setattr(datetime, 'now', lambda: datetime(2023, 1, 1, 10, 0, 0)) # Mock current time
            result = prepare_parameters_task(ti=mock_ti, dag_run={'conf': {}})
            assert result['job_execution_date'] == "20230101"
            mock_ti.xcom_push.assert_called_once()

    def test_prepare_parameters_task_custom_stichtag():
        mock_ti = MagicMock()
        mock_ti.xcom_push = MagicMock()
        result = prepare_parameters_task(ti=mock_ti, dag_run={'conf': {'stichtag': '20221231', 'wiederanlaufwert': '5'}})
        assert result['job_execution_date'] == '20221231'
        assert result['wiederanlaufwert'] == '5'
        mock_ti.xcom_push.assert_called_once()

    def test_prepare_parameters_task_invalid_stichtag_raises_error():
        mock_ti = MagicMock()
        with pytest.raises(ValueError, match="Invalid date format for stichtag"):
            prepare_parameters_task(ti=mock_ti, dag_run={'conf': {'stichtag': '2022-12-31'}})

    def test_log_job_status_task_logs_info():
        mock_ti = MagicMock()
        mock_ti.xcom_pull.side_effect = [
            {'job_execution_date': '20231026'}, # For processed_params
            {'num_inserted_rows': 100, 'job_id': 'bq_job_123'} # For bq_query_results
        ]
        # Mock the logger to capture output
        mock_log = MagicMock()
        with pytest.MonkeyPatch.context() as m:
            m.setattr('bert_bcp_msisdn_utils.log', mock_log)
            log_job_status_task(ti=mock_ti)
            mock_log.info.assert_any_call("Job DW.BERT_AUSD_BP_TA_BCP_MSISDN completed successfully.")
            mock_log.info.assert_any_call("Number of rows inserted into sof.ta_bcp_msisdn: 100")
    ```

### Test Case 5: Data Quality - Schema and Truncate Behavior

*   **Purpose**: To verify that the target BigQuery table `sof.ta_bcp_msisdn` has the correct schema (column names, data types, nullability) and that the `TRUNCATE` operation (implemented via `write_disposition='WRITE_TRUNCATE'`) correctly clears the table before new data is inserted.

*   **Setup**:
    1.  **Oracle Schema**: Obtain the schema definition for `sof$ta_bcp_msisdn` from the legacy Oracle database.
    2.  **BigQuery Initial Data**: Populate `your_project.sof.ta_bcp_msisdn` in BigQuery with some dummy data (e.g., 5-10 rows).
    3.  **BigQuery Source Data**: Populate `your_project.sof.ta_bpr_bcp` and `your_project.sof.ta_rn_vertrag` with a small, known dataset that will result in a predictable number of rows (e.g., 3 rows) being inserted into `sof.ta_bcp_msisdn`.

*   **Action**:
    1.  Execute the migrated Airflow DAG `dw_bert_ausd_bp_ta_bcp_msisdn`.
    2.  After the DAG completes, query the schema of `your_project.sof.ta_bcp_msisdn` in BigQuery.
    3.  Query the row count of `your_project.sof.ta_bcp_msisdn` in BigQuery.

*   **Pass/Fail Criterion**:
    *   The BigQuery table `your_project.sof.ta_bcp_msisdn` must have the same column names, equivalent data types (e.g., `NUMBER` to `INT64`, `VARCHAR2` to `STRING`), and nullability constraints as the Oracle `sof$ta_bcp_msisdn` table.
    *   The row count of `your_project.sof.ta_bcp_msisdn` must be exactly equal to the number of rows expected from the `INSERT` statement (e.g., 3 rows), confirming that the initial dummy data was truncated.

    ```sql
    -- Oracle SQL (to retrieve schema)
    SELECT COLUMN_NAME, DATA_TYPE, NULLABLE
    FROM ALL_TAB_COLUMNS
    WHERE OWNER = 'SOF' AND TABLE_NAME = 'TA_BCP_MSISDN'
    ORDER BY COLUMN_ID;

    -- BigQuery SQL (to retrieve schema)
    SELECT column_name, data_type, is_nullable
    FROM `your_project.sof.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'ta_bcp_msisdn'
    ORDER BY ordinal_position;

    -- BigQuery SQL (to verify row count after run)
    SELECT COUNT(*) FROM `your_project.sof.ta_bcp_msisdn`;
    ```

### Test Case 6: Data Quality - Referential Integrity (Implicit)

*   **Purpose**: To verify that the `CNTRCT_ID_REF` and `CNTRCT_ID` values in the output table `sof.ta_bcp_msisdn` maintain referential integrity with their respective source tables, as implied by the `JOIN` condition.

*   **Setup**:
    1.  Ensure `your_project.sof.ta_bpr_bcp` and `your_project.sof.ta_rn_vertrag` contain a representative dataset, including cases where `cntrct_id_ref` in `ta_bpr_bcp` might not have a match in `ta_rn_vertrag` (these should be filtered out).
    2.  Execute the migrated Airflow DAG `dw_bert_ausd_bp_ta_bcp_msisdn`.

*   **Action**:
    1.  After the DAG completes, execute BigQuery queries to check for orphaned `CNTRCT_ID_REF` and `CNTRCT_ID` values in the target table.

*   **Pass/Fail Criterion**:
    *   The query checking for `CNTRCT_ID_REF` values in `sof.ta_bcp_msisdn` that do not exist in `sof.ta_rn_vertrag.CNTRCT_ID` must return 0 rows.
    *   The query checking for `CNTRCT_ID` values in `sof.ta_bcp_msisdn` that do not exist in `sof.ta_bpr_bcp.CNTRCT_ID` must return 0 rows.

    ```sql
    -- BigQuery SQL (Check for orphaned CNTRCT_ID_REF)
    SELECT COUNT(DISTINCT t1.CNTRCT_ID_REF)
    FROM `your_project.sof.ta_bcp_msisdn` AS t1
    LEFT JOIN `your_project.sof.ta_rn_vertrag` AS t2 ON t1.CNTRCT_ID_REF = t2.CNTRCT_ID
    WHERE t2.CNTRCT_ID IS NULL;
    -- Expected result: 0

    -- BigQuery SQL (Check for orphaned CNTRCT_ID)
    SELECT COUNT(DISTINCT t1.CNTRCT_ID)
    FROM `your_project.sof.ta_bcp_msisdn` AS t1
    LEFT JOIN `your_project.sof.ta_bpr_bcp` AS t2 ON t1.CNTRCT_ID = t2.CNTRCT_ID
    WHERE t2.CNTRCT_ID IS NULL;
    -- Expected result: 0
    ```