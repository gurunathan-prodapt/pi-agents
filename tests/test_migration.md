The migration of `DW.BERT_AUSD_BP_TA_TARIFOPTION` involves significant changes in technology stack and transformation logic. The following tests are designed to ensure the migrated BigQuery pipeline is functionally equivalent to the legacy Oracle/KornShell job.

**Pre-requisites for all tests:**

1.  **Legacy Environment Access**: Access to the Oracle database where the legacy `d_ausd_bp_ta_tarifoption.sql` can be executed, along with its source tables (`isbert_schema.dwtk_meldungen`, `isbert_schema.sof$ta_l_bpr_optionen_filter`, `sof$ta_bpr_opt_text_<dynamic_date_variable>`) and target tables (`sof$ta_bpr_opt_filter`, `sof$ta_tarifoption`).
2.  **Migrated Environment Access**: Access to the Google Cloud project where the Airflow DAG `dw_bert_ausd_bp_ta_tarifoption` is deployed, and its BigQuery source tables (`bert_staging.dwtk_meldungen`, `bert_master.sof_l_bpr_optionen_filter`, `bert_raw.sof_ta_bpr_opt_YYYYMMDD`) and target tables (`bert_staging.bpr_opt_filter`, `bert_reporting.tarifoption`) exist.
3.  **Data Synchronization**: For each test run, ensure that the BigQuery source tables contain an exact, point-in-time replica of the data from their corresponding Oracle source tables. This is crucial for accurate output parity comparisons.
4.  **Test Data Set**: A "golden" test data set should be prepared, ideally a snapshot of production data from the legacy system, covering typical scenarios and known edge cases. This data set will be loaded into both legacy Oracle and migrated BigQuery source tables.

---

### Test Case 1: End-to-End Output Parity (Final Table)

*   **Purpose**: Verify that the final output table in BigQuery (`bert_reporting.tarifoption`) is identical to the final output table in the legacy Oracle system (`sof$ta_tarifoption`) for a given set of inputs. This is the most critical test for behavioral equivalence.
*   **Setup**:
    1.  Load the "golden" test data set into the Oracle source tables (`isbert_schema.dwtk_meldungen`, `isbert_schema.sof$ta_l_bpr_optionen_filter`, `sof$ta_bpr_opt_text_<dynamic_date_variable>`).
    2.  Load the *exact same* "golden" test data set into the corresponding BigQuery source tables (`bert_staging.dwtk_meldungen`, `bert_master.sof_l_bpr_optionen_filter`, `bert_raw.sof_ta_bpr_opt_YYYYMMDD`).
    3.  Ensure the `v_datum` derived from `dwtk_meldungen` is consistent across both environments.
*   **Action**:
    1.  Execute the legacy Oracle job (`d_ausd_bp_ta_tarifoption.sql`) to populate `sof$ta_tarifoption`.
    2.  Trigger the Airflow DAG `dw_bert_ausd_bp_ta_tarifoption` to populate `bert_reporting.tarifoption`.
*   **Pass/Fail Criterion**:
    *   The row count of `bert_reporting.tarifoption` must exactly match the row count of `sof$ta_tarifoption`.
    *   A full data comparison (e.g., using `MINUS` in SQL or a data diff tool) between `bert_reporting.tarifoption` and `sof$ta_tarifoption` must yield no differences. All columns (`cntrct_id`, `business_option`, `sonstige_option`, `gprs_option`) must match exactly, including NULLs and empty strings.

*   **Test Code (SQL Assertion - BigQuery vs. Oracle)**:

    ```sql
    -- BigQuery side:
    SELECT
        cntrct_id,
        business_option,
        sonstige_option,
        gprs_option
    FROM
        `your_gcp_project.bert_reporting.tarifoption`
    ORDER BY
        cntrct_id;

    -- Oracle side:
    SELECT
        cntrct_id,
        business_option,
        sonstige_option,
        gprs_option
    FROM
        sof$ta_tarifoption
    ORDER BY
        cntrct_id;

    -- Automated comparison (conceptual, requires data export/import or a data diff tool):
    -- Example using Python with pandas (assuming dataframes df_bq and df_oracle are loaded):
    # import pandas as pd
    # pd.testing.assert_frame_equal(df_bq, df_oracle, check_dtype=False, check_like=True)
    ```

---

### Test Case 2: Intermediate Table Output Parity

*   **Purpose**: Verify that the intermediate table in BigQuery (`bert_staging.bpr_opt_filter`) is identical to its legacy Oracle counterpart (`sof$ta_bpr_opt_filter`). This helps pinpoint issues if the final output parity test fails.
*   **Setup**: Same as Test Case 1.
*   **Action**:
    1.  Execute the relevant part of the legacy Oracle job that populates `sof$ta_bpr_opt_filter`.
    2.  Trigger the Airflow DAG `dw_bert_ausd_bp_ta_tarifoption`. The `bert_staging.bpr_opt_filter` table will be populated by the `Create Intermediate Filter Table Task`.
*   **Pass/Fail Criterion**:
    *   The row count of `bert_staging.bpr_opt_filter` must exactly match the row count of `sof$ta_bpr_opt_filter`.
    *   A full data comparison between `bert_staging.bpr_opt_filter` and `sof$ta_bpr_opt_filter` must yield no differences. All columns (`bpr_id`, `cntrct_id`, `pds_description`, `opt_kategorie`) must match exactly.

*   **Test Code (SQL Assertion - BigQuery vs. Oracle)**:

    ```sql
    -- BigQuery side:
    SELECT
        bpr_id,
        cntrct_id,
        pds_description,
        opt_kategorie
    FROM
        `your_gcp_project.bert_staging.bpr_opt_filter`
    ORDER BY
        bpr_id, cntrct_id, pds_description, opt_kategorie;

    -- Oracle side:
    SELECT
        bpr_id,
        cntrct_id,
        pds_description,
        opt_kategorie
    FROM
        sof$ta_bpr_opt_filter
    ORDER BY
        bpr_id, cntrct_id, pds_description, opt_kategorie;
    ```

---

### Test Case 3: Dynamic Date Variable (`v_datum`) Correctness

*   **Purpose**: Verify that the `_get_v_datum` PythonOperator correctly determines the `v_datum` based on the maximum `timecreated` for `job_kennung = 'BERT_DROP_TEMP_TABLE'` from `bert_staging.dwtk_meldungen`, and that this value is correctly passed to the BigQuery SQL.
*   **Setup**:
    1.  Populate `bert_staging.dwtk_meldungen` with various `timecreated` values and `job_kennung` entries, including at least one `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
    2.  Ensure the Oracle `isbert_schema.dwtk_meldungen` has corresponding data to determine the expected `v_datum`.
*   **Action**:
    1.  Manually determine the expected `v_datum` from the Oracle `isbert_schema.dwtk_meldungen` table using the logic `FORMAT_DATE('%Y%m%d', MAX(m.timecreated))` where `m.job_kennung = 'BERT_DROP_TEMP_TABLE'`.
    2.  Trigger the Airflow DAG `dw_bert_ausd_bp_ta_tarifoption`.
    3.  Inspect the Airflow task logs for `get_v_datum` to see the pushed XCom value, or query the Airflow metadata database.
*   **Pass/Fail Criterion**:
    *   The `v_datum` value pushed to XCom by `get_v_datum_task` must exactly match the manually determined expected `v_datum`.
    *   The BigQuery SQL executed by `full_sql_transformation_task` must correctly use this `v_datum` to construct the dynamic table name (e.g., `bert_raw.sof_ta_bpr_opt_YYYYMMDD`). This can be verified by inspecting BigQuery job history or logs.

*   **Test Code (Python/SQL for expected value)**:

    ```python
    # Python code to get expected v_datum (can be run locally or in a test harness)
    from google.cloud import bigquery
    client = bigquery.Client()
    query = """
    SELECT
        COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
    FROM
        `your_gcp_project.bert_staging.dwtk_meldungen` AS m
    WHERE
        m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    """
    query_job = client.query(query)
    expected_v_datum = [row[0] for row in query_job.result()][0]
    print(f"Expected v_datum: {expected_v_datum}")

    # After DAG run, retrieve XCom value (conceptual)
    # actual_v_datum = airflow_api.get_xcom_value(dag_id, task_id='get_v_datum', key='v_datum')
    # assert actual_v_datum == expected_v_datum
    ```

---

### Test Case 4: Transformation Correctness - Aggregation and String Handling

*   **Purpose**: Verify the `STRING_AGG` logic, `CASE` statements, `TRIM`, and `SUBSTR` functions correctly categorize, concatenate, and truncate `pds_description` values. This specifically addresses the replacement of Oracle's `LEAD` (with `ORDER BY NULL`) and custom `concatX` functions.
*   **Setup**:
    1.  Prepare a synthetic data set for `bert_master.sof_l_bpr_optionen_filter` and `bert_raw.sof_ta_bpr_opt_YYYYMMDD` that includes:
        *   Multiple `pds_description` values for the same `cntrct_id` and `opt_kategorie`.
        *   `pds_description` values that, when concatenated, exceed 500 characters.
        *   `cntrct_id`s with no `pds_description` for certain `opt_kategorie`s.
        *   `pds_description` values with leading/trailing spaces.
    2.  Ensure `bert_staging.dwtk_meldungen` is set up such that `_get_v_datum` returns the `YYYYMMDD` corresponding to your synthetic `sof_ta_bpr_opt_YYYYMMDD` table.
*   **Action**:
    1.  Manually calculate the expected output for `bert_reporting.tarifoption` based on the synthetic data and the BigQuery SQL logic.
    2.  Trigger the Airflow DAG `dw_bert_ausd_bp_ta_tarifoption`.
    3.  Query `bert_reporting.tarifoption`.
*   **Pass/Fail Criterion**:
    *   The `business_option`, `sonstige_option`, and `gprs_option` columns in `bert_reporting.tarifoption` must exactly match the manually calculated expected values for all `cntrct_id`s. This includes correct concatenation order (alphabetical by `pds_description`), truncation at 500 characters, and handling of NULLs/empty strings for missing categories.

*   **Test Code (Pytest with BigQuery query)**:

    ```python
    import pytest
    from google.cloud import bigquery

    @pytest.fixture(scope="module")
    def bq_client():
        return bigquery.Client()

    def test_aggregation_and_string_handling(bq_client):
        # Setup: Assume synthetic data is loaded and DAG has run
        # Example:
        # bert_master.sof_l_bpr_optionen_filter:
        # (1, 'OPT_A', 'BUDGET'), (2, 'OPT_B', 'SONST'), (3, 'OPT_C', 'GPRS'), (4, 'OPT_D', 'BUDGET')
        # bert_raw.sof_ta_bpr_opt_20230101:
        # (1, 'C1', 'Option A'), (2, 'C1', 'Option B'), (3, 'C1', 'Option C'), (4, 'C2', 'Option D Long String...')

        # Expected results for a specific cntrct_id (e.g., 'C1')
        expected_results = {
            'C1': {
                'business_option': 'Option A',
                'sonstige_option': 'Option B',
                'gprs_option': 'Option C'
            },
            'C2': {
                'business_option': 'Option D Long String... (truncated if >500)',
                'sonstige_option': None,
                'gprs_option': None
            }
        }

        query = """
        SELECT
            cntrct_id,
            business_option,
            sonstige_option,
            gprs_option
        FROM
            `your_gcp_project.bert_reporting.tarifoption`
        WHERE
            cntrct_id IN ('C1', 'C2') -- Filter for relevant test data
        ORDER BY
            cntrct_id
        """
        rows = bq_client.query(query).result()

        actual_results = {}
        for row in rows:
            actual_results[row.cntrct_id] = {
                'business_option': row.business_option,
                'sonstige_option': row.sonstige_option,
                'gprs_option': row.gprs_option
            }

        for cntrct_id, expected_data in expected_results.items():
            assert cntrct_id in actual_results, f"Missing cntrct_id: {cntrct_id}"
            actual_data = actual_results[cntrct_id]
            assert actual_data['business_option'] == expected_data['business_option'], \
                f"Mismatch for {cntrct_id} business_option"
            assert actual_data['sonstige_option'] == expected_data['sonstige_option'], \
                f"Mismatch for {cntrct_id} sonstige_option"
            assert actual_data['gprs_option'] == expected_data['gprs_option'], \
                f"Mismatch for {cntrct_id} gprs_option"
    ```

---

### Test Case 5: External System Replacement - Oracle Source Tables to BigQuery

*   **Purpose**: Verify that the BigQuery source tables (`bert_staging.dwtk_meldungen`, `bert_master.sof_l_bpr_optionen_filter`, `bert_raw.sof_ta_bpr_opt_YYYYMMDD`) are correctly ingested and accessible, effectively replacing the Oracle reads.
*   **Setup**:
    1.  Ensure the BigQuery source tables are populated with data. This is typically handled by upstream ingestion jobs (e.g., Dataflow, Datastream).
    2.  For this test, specifically verify the data in these BigQuery tables matches their Oracle counterparts.
*   **Action**:
    1.  Run a data comparison query between `bert_staging.dwtk_meldungen` and `isbert_schema.dwtk_meldungen` (Oracle).
    2.  Run a data comparison query between `bert_master.sof_l_bpr_optionen_filter` and `isbert_schema.sof$ta_l_bpr_optionen_filter` (Oracle).
    3.  Run a data comparison query between `bert_raw.sof_ta_bpr_opt_YYYYMMDD` and `sof$ta_bpr_opt_text_<dynamic_date_variable>` (Oracle).
    4.  Trigger the Airflow DAG `dw_bert_ausd_bp_ta_tarifoption` and observe its successful completion, indicating it could read from these tables.
*   **Pass/Fail Criterion**:
    *   All data comparison queries must show no differences between the BigQuery and Oracle source tables.
    *   The Airflow DAG must complete successfully without errors related to table access or data availability for these source tables.

*   **Test Code (SQL Assertion - Example for `dwtk_meldungen`)**:

    ```sql
    -- BigQuery:
    SELECT COUNT(*) FROM `your_gcp_project.bert_staging.dwtk_meldungen`;
    SELECT COUNT(*) FROM `your_gcp_project.bert_staging.dwtk_meldungen` EXCEPT DISTINCT SELECT * FROM `your_gcp_project.bert_staging.dwtk_meldungen_oracle_replica`; -- Assuming a replica for comparison

    -- Oracle:
    SELECT COUNT(*) FROM isbert_schema.dwtk_meldungen;
    ```
    *(Note: Direct `EXCEPT DISTINCT` between BigQuery and Oracle is not possible. This implies exporting data or using a tool for comparison.)*

---

### Test Case 6: Data Quality - Row Counts and Schema Assertions

*   **Purpose**: Verify basic data quality aspects like row counts and schema integrity for the target tables.
*   **Setup**: Same as Test Case 1 (golden data set).
*   **Action**:
    1.  Execute the legacy Oracle job.
    2.  Trigger the Airflow DAG.
    3.  Query row counts for `sof$ta_bpr_opt_filter`, `sof$ta_tarifoption` (Oracle) and `bert_staging.bpr_opt_filter`, `bert_reporting.tarifoption` (BigQuery).
    4.  Inspect the schema of `bert_reporting.tarifoption` and `bert_staging.bpr_opt_filter` in BigQuery.
*   **Pass/Fail Criterion**:
    *   **Row Counts**:
        *   `COUNT(*)` from `bert_staging.bpr_opt_filter` must match `COUNT(*)` from `sof$ta_bpr_opt_filter`.
        *   `COUNT(*)` from `bert_reporting.tarifoption` must match `COUNT(*)` from `sof$ta_tarifoption`.
    *   **Schema**:
        *   The column names and data types of `bert_reporting.tarifoption` must match the expected schema (e.g., `cntrct_id` as STRING, `business_option` as STRING, etc.).
        *   The column names and data types of `bert_staging.bpr_opt_filter` must match the expected schema.
    *   **No Duplicates**: `SELECT COUNT(DISTINCT cntrct_id) FROM bert_reporting.tarifoption` must equal `SELECT COUNT(*) FROM bert_reporting.tarifoption` (as `cntrct_id` should be unique after `GROUP BY`).

*   **Test Code (SQL Assertions - BigQuery)**:

    ```sql
    -- Row Count Comparison (assuming Oracle counts are known from Test Case 1 & 2)
    SELECT COUNT(*) FROM `your_gcp_project.bert_staging.bpr_opt_filter`;
    -- Expected: <Oracle_sof$ta_bpr_opt_filter_count>

    SELECT COUNT(*) FROM `your_gcp_project.bert_reporting.tarifoption`;
    -- Expected: <Oracle_sof$ta_tarifoption_count>

    -- Schema Check (manual inspection or automated via BigQuery API)
    -- Example:
    SELECT column_name, data_type
    FROM `your_gcp_project.bert_reporting.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'tarifoption'
    ORDER BY ordinal_position;
    /* Expected Output:
    cntrct_id, STRING
    business_option, STRING
    sonstige_option, STRING
    gprs_option, STRING
    */

    -- Uniqueness Check for final table
    SELECT
        CASE
            WHEN COUNT(*) = COUNT(DISTINCT cntrct_id) THEN 'PASS'
            ELSE 'FAIL'
        END AS uniqueness_check
    FROM
        `your_gcp_project.bert_reporting.tarifoption`;
    ```

---

### Test Case 7: Edge Case - Empty Source Tables

*   **Purpose**: Verify the job handles scenarios where source tables are empty or yield no data.
*   **Setup**:
    1.  Load empty data sets into `bert_staging.dwtk_meldungen`, `bert_master.sof_l_bpr_optionen_filter`, and `bert_raw.sof_ta_bpr_opt_YYYYMMDD`.
    2.  Ensure `v_datum` will default to '19000101' (or a similar fallback as per `_get_v_datum` logic).
*   **Action**:
    1.  Execute the legacy Oracle job with empty source tables.
    2.  Trigger the Airflow DAG `dw_bert_ausd_bp_ta_tarifoption`.
*   **Pass/Fail Criterion**:
    *   Both `bert_staging.bpr_opt_filter` and `bert_reporting.tarifoption` must be created successfully and contain 0 rows.
    *   The Airflow DAG must complete successfully without errors.
    *   The `v_datum` should correctly default to '19000101' as per the `_get_v_datum` logic.

*   **Test Code (SQL Assertion)**:

    ```sql
    SELECT COUNT(*) FROM `your_gcp_project.bert_staging.bpr_opt_filter`; -- Expected: 0
    SELECT COUNT(*) FROM `your_gcp_project.bert_reporting.tarifoption`; -- Expected: 0
    ```

---

### Test Case 8: Edge Case - Long String Truncation

*   **Purpose**: Verify that `SUBSTR(..., 1, 500)` correctly truncates concatenated strings that exceed 500 characters.
*   **Setup**:
    1.  Create a synthetic data set for `bert_master.sof_l_bpr_optionen_filter` and `bert_raw.sof_ta_bpr_opt_YYYYMMDD` such that for a specific `cntrct_id` and `opt_kategorie`, the `STRING_AGG` result (before `SUBSTR`) is longer than 500 characters.
    2.  Ensure `v_datum` is set correctly.
*   **Action**:
    1.  Trigger the Airflow DAG `dw_bert_ausd_bp_ta_tarifoption`.
    2.  Query `bert_reporting.tarifoption` for the specific `cntrct_id`.
*   **Pass/Fail Criterion**:
    *   The length of the `business_option`, `sonstige_option`, or `gprs_option` column for the test `cntrct_id` must be exactly 500 characters.
    *   The content of the truncated string must match the first 500 characters of the expected concatenated string.

*   **Test Code (SQL Assertion)**:

    ```sql
    SELECT
        LENGTH(business_option) AS business_option_len,
        LENGTH(sonstige_option) AS sonstige_option_len,
        LENGTH(gprs_option) AS gprs_option_len
    FROM
        `your_gcp_project.bert_reporting.tarifoption`
    WHERE
        cntrct_id = 'TEST_LONG_STRING_ID'; -- Replace with your test ID

    -- Expected: One of the _len columns should be 500, others might be less or NULL.
    ```