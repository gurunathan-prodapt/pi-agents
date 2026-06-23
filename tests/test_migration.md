The migration of `DW.BERT_AUSD_BP_TA_APN_VERTRAG` from Oracle/KornShell/UC4 to BigQuery/Python/Airflow requires comprehensive testing to ensure behavioral equivalence. The following test cases are designed to validate output parity, transformation correctness, external system replacements, and data quality.

---

## Migration Validation Tests: DW.BERT_AUSD_BP_TA_APN_VERTRAG

### 1. Output Parity

#### Test Case 1.1: Full Data Parity Check

*   **Purpose**: To verify that the migrated job produces an identical output table (`sof.sof_ta_apn_vertrag` in BigQuery) compared to the legacy job's output (`sof$ta_apn_vertrag` in Oracle) when given the same input data. This is the ultimate behavioral equivalence test.
*   **Setup**:
    1.  **Legacy System**: Capture a snapshot of the `sof$ta_bpr_apn` and `isbert_schema.dwtk_meldungen` tables from the Oracle source system. Execute the legacy `DW.BERT_AUSD_BP_TA_APN_VERTRAG` job with this input data. Capture the final state of `sof$ta_apn_vertrag`. This will serve as the "golden standard" output.
    2.  **Migrated System**: Create corresponding BigQuery tables (`sof.sof_ta_bpr_apn`, `isbert_schema.dwtk_meldungen`, `sof.sof_ta_apn_vertrag`) in a test BigQuery project. Load the *exact same* input data (from step 1) into `sof.sof_ta_bpr_apn` and `isbert_schema.dwtk_meldungen`.
    3.  Deploy the Airflow DAG `dw_bert_ausd_bp_ta_apn_vertrag` to a test Airflow environment.
*   **Action**:
    1.  Execute the legacy job `DW.BERT_AUSD_BP_TA_APN_VERTRAG` in the Oracle environment.
    2.  Execute the Airflow DAG `dw_bert_ausd_bp_ta_apn_vertrag` in the test Airflow environment.
    3.  After both jobs complete, extract the data from the legacy `sof$ta_apn_vertrag` and the migrated `sof.sof_ta_apn_vertrag` tables.
*   **Pass/Fail Criterion**: The data in the migrated `sof.sof_ta_apn_vertrag` BigQuery table must be *byte-for-byte identical* to the data in the legacy `sof$ta_apn_vertrag` Oracle table, considering column order and appropriate data type mappings (e.g., Oracle `VARCHAR2` to BigQuery `STRING`).

    ```python
    # Example using pandas for comparison (requires BigQuery and Oracle clients)
    import pandas as pd
    from google.cloud import bigquery
    import cx_Oracle # Assuming Oracle client is installed and configured

    def compare_apn_vertrag_tables(legacy_conn_str: str, bq_project_id: str, bq_dataset_id: str, bq_table_id: str, legacy_table_name: str):
        """
        Compares the content of the legacy Oracle table with the migrated BigQuery table.
        """
        print(f"Comparing legacy Oracle table {legacy_table_name} with BigQuery table {bq_dataset_id}.{bq_table_id}...")

        # 1. Fetch data from legacy Oracle
        try:
            oracle_conn = cx_Oracle.connect(legacy_conn_str)
            oracle_query = f"SELECT CNTRCT_ID, APN_LIST, CNTRCT_REF_LIST FROM {legacy_table_name} ORDER BY CNTRCT_ID"
            df_legacy = pd.read_sql(oracle_query, oracle_conn)
            oracle_conn.close()
            print(f"Fetched {len(df_legacy)} rows from legacy Oracle.")
        except Exception as e:
            raise RuntimeError(f"Failed to fetch data from Oracle: {e}")

        # 2. Fetch data from BigQuery
        try:
            bq_client = bigquery.Client(project=bq_project_id)
            bq_query = f"SELECT cntrct_id, apn_list, cntrct_ref_list FROM `{bq_dataset_id}.{bq_table_id}` ORDER BY cntrct_id"
            df_migrated = bq_client.query(bq_query).to_dataframe()
            print(f"Fetched {len(df_migrated)} rows from BigQuery.")
        except Exception as e:
            raise RuntimeError(f"Failed to fetch data from BigQuery: {e}")

        # 3. Standardize column names and types for comparison
        df_legacy.columns = df_legacy.columns.str.lower() # Convert to lowercase for consistency
        # Ensure string columns are of object type for exact comparison if they might be mixed types
        for col in ['cntrct_id', 'apn_list', 'cntrct_ref_list']:
            if col in df_legacy.columns:
                df_legacy[col] = df_legacy[col].astype(str).replace('None', pd.NA) # Handle Oracle NULLs as None, then to pd.NA
            if col in df_migrated.columns:
                df_migrated[col] = df_migrated[col].astype(str).replace('None', pd.NA) # BigQuery NULLs are usually None in pandas

        # 4. Compare dataframes
        try:
            pd.testing.assert_frame_equal(df_legacy, df_migrated, check_dtype=True, check_exact=True)
            print("Output parity test PASSED: Dataframes are identical.")
        except AssertionError as e:
            print(f"Output parity test FAILED: Dataframes differ.\n{e}")
            # Optionally, print differences for debugging
            # diff = df_legacy.compare(df_migrated)
            # print("Differences:\n", diff)
            raise

    # Example usage (replace with actual connection details and table names)
    # if __name__ == "__main__":
    #     try:
    #         compare_apn_vertrag_tables(
    #             legacy_conn_str="user/pass@host:port/service",
    #             bq_project_id="your-gcp-project",
    #             bq_dataset_id="sof",
    #             bq_table_id="sof_ta_apn_vertrag",
    #             legacy_table_name="SOF$TA_APN_VERTRAG"
    #         )
    #     except Exception as e:
    #         print(f"Test failed: {e}")
    ```

---

### 2. Transformation Correctness

#### Test Case 2.1: Python Wrapper - Parameter Validation

*   **Purpose**: Verify that the `k_ausd_bp_ta_apn_vertrag_wrapper.py` correctly validates input parameters, especially `stichtag`.
*   **Setup**: None (unit test environment).
*   **Action**: Call `ksh_wrapper_main` with various valid and invalid `stichtag` values.
*   **Pass/Fail Criterion**:
    *   Valid `stichtag` (e.g., "20231026") should execute without raising an exception.
    *   Missing `stichtag` (e.g., `None` or empty string) should raise a `ValueError`.
    *   Invalid `stichtag` format (e.g., "2023-10-26", "20231301", "abc") should raise a `ValueError` with a descriptive message.

    ```python
    # File: tests/unit/test_k_ausd_bp_ta_apn_vertrag_wrapper.py
    import pytest
    from unittest.mock import MagicMock
    from dags.k_ausd_bp_ta_apn_vertrag_wrapper import main as ksh_wrapper_main

    # Mock Airflow's TaskInstance for local testing
    class MockTaskInstance:
        def xcom_push(self, key, value):
            pass # Do nothing for these tests

    mock_context = {"ti": MockTaskInstance()}

    def test_ksh_wrapper_valid_stichtag():
        """Test with a correctly formatted stichtag."""
        try:
            ksh_wrapper_main(job_kennung="TEST", stichtag="20231026", eintragsnr=1, wiederanlaufwert=0, **mock_context)
            assert True # If no exception, test passes
        except Exception as e:
            pytest.fail(f"Valid stichtag raised an unexpected exception: {e}")

    def test_ksh_wrapper_missing_stichtag():
        """Test with a missing stichtag parameter."""
        with pytest.raises(ValueError, match="Stichtag \\(date\\) parameter is missing."):
            ksh_wrapper_main(job_kennung="TEST", stichtag="", eintragsnr=1, wiederanlaufwert=0, **mock_context)

    @pytest.mark.parametrize("invalid_stichtag", ["2023-10-26", "20231301", "abc", "20231032"])
    def test_ksh_wrapper_invalid_stichtag_format(invalid_stichtag):
        """Test with various invalid stichtag formats."""
        with pytest.raises(ValueError, match=f"Invalid Stichtag format: {invalid_stichtag}. Expected YYYYMMDD."):
            ksh_wrapper_main(job_kennung="TEST", stichtag=invalid_stichtag, eintragsnr=1, wiederanlaufwert=0, **mock_context)
    ```

#### Test Case 2.2: Python Wrapper - SQL Template Loading and XCom Push

*   **Purpose**: Verify that the Python wrapper correctly loads the BigQuery SQL template file and pushes its content to Airflow XComs for subsequent tasks.
*   **Setup**:
    1.  Create a dummy `d_ausd_bp_ta_apn_vertrag_bq.sql` file in the same directory as `k_ausd_bp_ta_apn_vertrag_wrapper.py` with known content.
    2.  Mock Airflow's `TaskInstance` to capture the pushed XCom value.
*   **Action**: Call `ksh_wrapper_main` with valid parameters.
*   **Pass/Fail Criterion**: The `mock_context["ti"].xcom_pushed_value` (captured by the mock) should contain the exact content of the dummy `d_ausd_bp_ta_apn_vertrag_bq.sql` file.

    ```python
    # File: tests/unit/test_k_ausd_bp_ta_apn_vertrag_wrapper.py (continued)
    import os

    def test_ksh_wrapper_loads_and_pushes_sql_template():
        """Test that the wrapper loads the SQL template and pushes it to XComs."""
        # Create a dummy SQL file for testing
        dummy_sql_content = "-- This is a test SQL content\nSELECT 'test_value';"
        sql_file_name = "d_ausd_bp_ta_apn_vertrag_bq.sql"
        # Ensure the dummy file is created in the expected location relative to the wrapper
        sql_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'dags', sql_file_name)
        
        # Create parent directory if it doesn't exist
        os.makedirs(os.path.dirname(sql_file_path), exist_ok=True)

        with open(sql_file_path, "w") as f:
            f.write(dummy_sql_content)

        class MockTaskInstance:
            def __init__(self):
                self.xcom_pushed_value = None
            def xcom_push(self, key, value):
                self.xcom_pushed_value = value

        mock_ti = MockTaskInstance()
        mock_context = {"ti": mock_ti}

        try:
            ksh_wrapper_main(job_kennung="TEST", stichtag="20231026", eintragsnr=1, wiederanlaufwert=0, **mock_context)
            assert mock_ti.xcom_pushed_value == dummy_sql_content
        finally:
            os.remove(sql_file_path) # Clean up dummy file
    ```

#### Test Case 2.3: BigQuery SQL - `v_datum` Declaration Logic

*   **Purpose**: Verify that the `v_datum` variable is correctly declared and derived from `isbert_schema.dwtk_meldungen` using `MAX(timecreated)` and `COALESCE` logic.
*   **Setup**:
    1.  Create a test BigQuery table `isbert_schema.dwtk_meldungen` in a test project.
    2.  Populate it with various `timecreated` values, including `NULL`s, and different `job_kennung` values, ensuring some match `'BERT_DROP_TEMP_TABLE'`.
*   **Action**: Execute a BigQuery query that declares `v_datum` using the provided logic and then selects `v_datum`.
*   **Pass/Fail Criterion**: The selected `v_datum` must match the expected value based on the `MAX(m.timecreated)` for `job_kennung = 'BERT_DROP_TEMP_TABLE'` and the `COALESCE` logic (defaulting to '19000101' if no matching rows or all `timecreated` are `NULL`).

    ```sql
    -- Setup: Create and populate test table `isbert_schema.dwtk_meldungen`
    CREATE SCHEMA IF NOT EXISTS isbert_schema;
    CREATE OR REPLACE TABLE `isbert_schema.dwtk_meldungen` (
        job_kennung STRING,
        timecreated TIMESTAMP
    );

    -- Scenario 1: Normal case with valid max timecreated
    INSERT INTO `isbert_schema.dwtk_meldungen` (job_kennung, timecreated) VALUES
    ('OTHER_JOB', '2023-01-01 10:00:00 UTC'),
    ('BERT_DROP_TEMP_TABLE', '2023-01-05 12:00:00 UTC'),
    ('BERT_DROP_TEMP_TABLE', '2023-01-03 09:00:00 UTC'),
    ('ANOTHER_JOB', '2023-01-06 15:00:00 UTC');

    -- Action & Assertion for Scenario 1:
    DECLARE v_datum STRING DEFAULT (
      SELECT
        COALESCE(
          FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))),
          '19000101'
        )
      FROM `isbert_schema.dwtk_meldungen` m
      WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    );
    SELECT v_datum AS expected_20230105; -- Expected result: '20230105'

    -- Scenario 2: All matching timecreated are NULL
    TRUNCATE TABLE `isbert_schema.dwtk_meldungen`;
    INSERT INTO `isbert_schema.dwtk_meldungen` (job_kennung, timecreated) VALUES
    ('OTHER_JOB', '2023-01-01 10:00:00 UTC'),
    ('BERT_DROP_TEMP_TABLE', NULL),
    ('BERT_DROP_TEMP_TABLE', NULL);

    -- Action & Assertion for Scenario 2:
    DECLARE v_datum STRING DEFAULT (
      SELECT
        COALESCE(
          FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))),
          '19000101'
        )
      FROM `isbert_schema.dwtk_meldungen` m
      WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    );
    SELECT v_datum AS expected_19000101_null_times; -- Expected result: '19000101'

    -- Scenario 3: No rows for 'BERT_DROP_TEMP_TABLE'
    TRUNCATE TABLE `isbert_schema.dwtk_meldungen`;
    INSERT INTO `isbert_schema.dwtk_meldungen` (job_kennung, timecreated) VALUES
    ('OTHER_JOB', '2023-01-01 10:00:00 UTC');

    -- Action & Assertion for Scenario 3:
    DECLARE v_datum STRING DEFAULT (
      SELECT
        COALESCE(
          FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))),
          '19000101'
        )
      FROM `isbert_schema.dwtk_meldungen` m
      WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    );
    SELECT v_datum AS expected_19000101_no_rows; -- Expected result: '19000101'
    ```

#### Test Case 2.4: BigQuery SQL - Core Aggregation Logic

*   **Purpose**: Verify the correctness of the `STRING_AGG` aggregation, `GROUP BY`, `SUBSTR`, and `TRIM` functions, including ordering, NULL handling, and length constraints.
*   **Setup**:
    1.  Create a test BigQuery table `sof.sof_ta_bpr_apn` in a test project.
    2.  Populate it with diverse data to cover various scenarios:
        *   Multiple APNs/refs for the same `cntrct_id`.
        *   Single APN/ref for a `cntrct_id`.
        *   `NULL` values for `access_point_name` or `cntrct_id_ref`.
        *   Very long `access_point_name` or `cntrct_id_ref` values to test `SUBSTR` (100 char limit).
        *   Empty `sof.sof_ta_bpr_apn` table (for an edge case).
    3.  Create an empty target table `sof.sof_ta_apn_vertrag`.
*   **Action**: Execute the core `INSERT INTO ... SELECT ... FROM ... GROUP BY ...` part of the `d_ausd_bp_ta_apn_vertrag_bq.sql` script.
*   **Pass/Fail Criterion**: The data in `sof.sof_ta_apn_vertrag` must match the manually calculated expected output for each scenario.

    ```sql
    -- Setup: Create and populate test table `sof.sof_ta_bpr_apn`
    CREATE SCHEMA IF NOT EXISTS sof;
    CREATE OR REPLACE TABLE `sof.sof_ta_bpr_apn` (
        cntrct_id STRING,
        access_point_name STRING,
        cntrct_id_ref STRING
    );
    INSERT INTO `sof.sof_ta_bpr_apn` (cntrct_id, access_point_name, cntrct_id_ref) VALUES
    ('C1', 'apn_a', 'ref_1'),
    ('C1', 'apn_b', 'ref_2'),
    ('C1', 'apn_c', 'ref_3'),
    ('C2', 'apn_x', 'ref_x'),
    ('C3', 'apn_y', NULL), -- NULL ref
    ('C4', NULL, 'ref_z'), -- NULL apn
    ('C5', 'long_apn_name_123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890_extra', 'ref_long'), -- APN > 100 chars
    ('C6', 'apn_d', 'long_ref_name_123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890_extra'), -- REF > 100 chars
    ('C7', 'apn_e', 'ref_e');

    CREATE OR REPLACE TABLE `sof.sof_ta_apn_vertrag` (
        cntrct_id STRING,
        apn_list STRING,
        cntrct_ref_list STRING
    );

    -- Action: Execute the core SQL logic
    INSERT INTO `sof.sof_ta_apn_vertrag` (cntrct_id, apn_list, cntrct_ref_list)
    SELECT
      cntrct_id,
      SUBSTR(TRIM(TRAILING ', ' FROM STRING_AGG(access_point_name, ', ' ORDER BY access_point_name)), 1, 100) AS apn_list,
      SUBSTR(TRIM(TRAILING ', ' FROM STRING_AGG(cntrct_id_ref, ', ' ORDER BY cntrct_id_ref)), 1, 100) AS cntrct_ref_list
    FROM `sof.sof_ta_bpr_apn`
    GROUP BY cntrct_id
    ORDER BY cntrct_id;

    -- Assertion: Query the target table and compare with expected results
    SELECT * FROM `sof.sof_ta_apn_vertrag` ORDER BY cntrct_id;
    /* Expected Results:
    cntrct_id | apn_list                                                                                             | cntrct_ref_list
    ----------|------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------
    C1        | apn_a, apn_b, apn_c                                                                                  | ref_1, ref_2, ref_3
    C2        | apn_x                                                                                                | ref_x
    C3        | apn_y                                                                                                | NULL
    C4        | NULL                                                                                                 | ref_z
    C5        | long_apn_name_12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789 | ref_long
    C6        | apn_d                                                                                                | long_ref_name_12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
    C7        | apn_e                                                                                                | ref_e
    */

    -- Edge Case: Empty Source Table
    TRUNCATE TABLE `sof.sof_ta_bpr_apn`;
    TRUNCATE TABLE `sof.sof_ta_apn_vertrag`;

    INSERT INTO `sof.sof_ta_apn_vertrag` (cntrct_id, apn_list, cntrct_ref_list)
    SELECT
      cntrct_id,
      SUBSTR(TRIM(TRAILING ', ' FROM STRING_AGG(access_point_name, ', ' ORDER BY access_point_name)), 1, 100) AS apn_list,
      SUBSTR(TRIM(TRAILING ', ' FROM STRING_AGG(cntrct_id_ref, ', ' ORDER BY cntrct_id_ref)), 1, 100) AS cntrct_ref_list
    FROM `sof.sof_ta_bpr_apn`
    GROUP BY cntrct_id
    ORDER BY cntrct_id;

    SELECT COUNT(*) FROM `sof.sof_ta_apn_vertrag`; -- Expected result: 0 rows
    ```

---

### 3. External-System Replacements

#### Test Case 3.1: BigQuery Table Accessibility and Schema Mapping

*   **Purpose**: Verify that the BigQuery tables replacing Oracle sources and targets are correctly created, accessible, and have the expected schema (column names, types).
*   **Setup**: Ensure the BigQuery datasets (`isbert_schema`, `sof`) and tables (`dwtk_meldungen`, `sof_ta_bpr_apn`, `sof_ta_apn_vertrag`) exist in the target GCP project.
*   **Action**: Query the BigQuery information schema for the tables involved in the migration.
*   **Pass/Fail Criterion**: The schema (column names, data types) of the BigQuery tables must match the conceptual schema of the original Oracle tables, with appropriate BigQuery type mappings (e.g., Oracle `VARCHAR2` to BigQuery `STRING`, `NUMBER` to `INT64`/`BIGNUMERIC`, `DATE`/`TIMESTAMP` to `DATE`/`TIMESTAMP`).

    ```sql
    -- Verify schema for `sof.sof_ta_bpr_apn` (source table)
    SELECT
      column_name,
      data_type
    FROM
      `your-gcp-project.sof.INFORMATION_SCHEMA.COLUMNS`
    WHERE
      table_name = 'sof_ta_bpr_apn'
    ORDER BY
      ordinal_position;
    /* Expected Schema for sof.sof_ta_bpr_apn:
    column_name       | data_type
    ------------------|----------
    cntrct_id         | STRING
    access_point_name | STRING
    cntrct_id_ref     | STRING
    */

    -- Verify schema for `sof.sof_ta_apn_vertrag` (target table)
    SELECT
      column_name,
      data_type
    FROM
      `your-gcp-project.sof.INFORMATION_SCHEMA.COLUMNS`
    WHERE
      table_name = 'sof_ta_apn_vertrag'
    ORDER BY
      ordinal_position;
    /* Expected Schema for sof.sof_ta_apn_vertrag:
    column_name     | data_type
    ----------------|----------
    cntrct_id       | STRING
    apn_list        | STRING
    cntrct_ref_list | STRING
    */

    -- Verify schema for `isbert_schema.dwtk_meldungen` (for v_datum derivation)
    SELECT
      column_name,
      data_type
    FROM
      `your-gcp-project.isbert_schema.INFORMATION_SCHEMA.COLUMNS`
    WHERE
      table_name = 'dwtk_meldungen'
    ORDER BY
      ordinal_position;
    /* Expected Schema for isbert_schema.dwtk_meldungen (based on usage):
    column_name | data_type
    ------------|----------
    job_kennung | STRING
    timecreated | TIMESTAMP
    */
    ```

#### Test Case 3.2: Airflow BigQuery Connection

*   **Purpose**: Verify that the Airflow `BigQueryOperator` can successfully connect to BigQuery using the configured `bigquery_conn_id` and execute a query.
*   **Setup**:
    1.  Ensure the `google_cloud_default` connection (or the specified `bigquery_conn_id`) is correctly configured in Airflow with appropriate service account credentials and permissions to the BigQuery project.
    2.  Deploy the Airflow DAG `dw_bert_ausd_bp_ta_apn_vertrag`.
*   **Action**: Trigger a run of the Airflow DAG.
*   **Pass/Fail Criterion**: The `execute_bq_sql` task in Airflow must complete successfully without connection or authentication errors. This is implicitly tested by any successful DAG run, but a dedicated simple `SELECT 1` task could be added for explicit connection testing if desired.

    ```python
    # This is an operational check, not typically a unit test.
    # A simple Airflow DAG for explicit connection testing:
    from airflow.models.dag import DAG
    from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator
    import pendulum

    with DAG(
        dag_id="test_bq_connection",
        start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
        schedule=None,
        catchup=False,
        tags=["test", "bigquery"],
    ) as dag:
        test_bq_connection_task = BigQueryOperator(
            task_id="test_bq_connection",
            sql="SELECT 1;",
            use_legacy_sql=False,
            bigquery_conn_id="google_cloud_default", # Must be configured in Airflow UI
        )
    ```

---

### 4. Data-Quality / Row-Count / Schema Assertions

#### Test Case 4.1: Row Count Parity

*   **Purpose**: Verify that the number of rows inserted into the target table by the migrated job is consistent with the legacy system.
*   **Setup**: Same as Test Case 1.1 (Full Data Parity Check), ensuring identical input data in both legacy Oracle and migrated BigQuery source tables.
*   **Action**:
    1.  Run the legacy job and record the row count in `sof$ta_apn_vertrag`.
    2.  Run the migrated DAG and record the row count in `sof.sof_ta_apn_vertrag`.
*   **Pass/Fail Criterion**: The row count in the migrated `sof.sof_ta_apn_vertrag` table must be exactly equal to the row count in the legacy `sof$ta_apn_vertrag` table.

    ```sql
    -- BigQuery assertion after DAG run
    SELECT COUNT(*) FROM `your-gcp-project.sof.sof_ta_apn_vertrag`;

    -- Oracle assertion after legacy job run
    SELECT COUNT(*) FROM SOF$TA_APN_VERTRAG;
    ```

#### Test Case 4.2: Data Quality - NULL Handling in Aggregation

*   **Purpose**: Verify that `NULL` values in `access_point_name` or `cntrct_id_ref` are correctly handled by `STRING_AGG` (i.e., they are ignored and do not result in "NULL" strings in the aggregated list, and the resulting aggregated column is `NULL` if all source values are `NULL`).
*   **Setup**: Use the `sof.sof_ta_bpr_apn` test data from Test Case 2.4, specifically rows for `C3` (NULL `cntrct_id_ref`) and `C4` (NULL `access_point_name`).
*   **Action**: Execute the full `d_ausd_bp_ta_apn_vertrag_bq.sql` script (or just the `INSERT` statement after populating source data).
*   **Pass/Fail Criterion**:
    *   For `cntrct_id = 'C3'`, `apn_list` should be 'apn_y' and `cntrct_ref_list` should be `NULL`.
    *   For `cntrct_id = 'C4'`, `apn_list` should be `NULL` and `cntrct_ref_list` should be 'ref_z'.

    ```sql
    -- Query to verify specific rows after running the aggregation (assuming setup from 2.4)
    SELECT cntrct_id, apn_list, cntrct_ref_list
    FROM `your-gcp-project.sof.sof_ta_apn_vertrag`
    WHERE cntrct_id IN ('C3', 'C4')
    ORDER BY cntrct_id;

    /* Expected:
    cntrct_id | apn_list | cntrct_ref_list
    ----------|----------|----------------
    C3        | apn_y    | NULL
    C4        | NULL     | ref_z
    */
    ```

#### Test Case 4.3: Data Quality - Length Truncation

*   **Purpose**: Verify that `SUBSTR` correctly truncates aggregated strings to a maximum of 100 characters, matching the legacy Oracle behavior.
*   **Setup**: Use the `sof.sof_ta_bpr_apn` test data from Test Case 2.4, specifically rows for `C5` (long `access_point_name`) and `C6` (long `cntrct_id_ref`).
*   **Action**: Execute the full `d_ausd_bp_ta_apn_vertrag_bq.sql` script (or just the `INSERT` statement after populating source data).
*   **Pass/Fail Criterion**: The `LENGTH(apn_list)` for `cntrct_id = 'C5'` should be exactly 100. The `LENGTH(cntrct_ref_list)` for `cntrct_id = 'C6'` should be exactly 100.

    ```sql
    -- Query to verify specific rows after running the aggregation (assuming setup from 2.4)
    SELECT
      cntrct_id,
      LENGTH(apn_list) AS apn_list_length,
      LENGTH(cntrct_ref_list) AS cntrct_ref_list_length,
      apn_list,
      cntrct_ref_list
    FROM `your-gcp-project.sof.sof_ta_apn_vertrag`
    WHERE cntrct_id IN ('C5', 'C6')
    ORDER BY cntrct_id;

    /* Expected:
    cntrct_id | apn_list_length | cntrct_ref_list_length | apn_list                                                                                             | cntrct_ref_list
    ----------|-----------------|------------------------|------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------
    C5        | 100             | 8                      | long_apn_name_12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789 | ref_long
    C6        | 5               | 100                    | apn_d                                                                                                | long_ref_name_12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
    */
    ```

#### Test Case 4.4: Idempotency Check

*   **Purpose**: Verify that running the job multiple times with the same input produces the same output, due to the `TRUNCATE` operation ensuring a clean state before each run.
*   **Setup**:
    1.  Populate `sof.sof_ta_bpr_apn` and `isbert_schema.dwtk_meldungen` with sample data.
    2.  Ensure `sof.sof_ta_apn_vertrag` is empty initially.
*   **Action**:
    1.  Run the Airflow DAG once. Record the row count and a checksum/hash of the `sof.sof_ta_apn_vertrag` table.
    2.  Run the Airflow DAG a second time.
*   **Pass/Fail Criterion**: The row count and checksum/hash of `sof.sof_ta_apn_vertrag` after the second run must be identical to those recorded after the first run.

    ```sql
    -- After first run, capture state:
    SELECT COUNT(*) AS row_count FROM `your-gcp-project.sof.sof_ta_apn_vertrag`;
    -- Generate a stable checksum for the table content (order matters for this, so ORDER BY is crucial)
    SELECT FARM_FINGERPRINT(ARRAY_AGG(TO_JSON_STRING(t) ORDER BY cntrct_id, apn_list, cntrct_ref_list)) AS table_checksum
    FROM `your-gcp-project.sof.sof_ta_apn_vertrag` AS t;

    -- After second run, re-capture and compare.
    -- The row_count and table_checksum should be identical to the first run's results.
    ```