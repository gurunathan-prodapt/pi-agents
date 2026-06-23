Here are migration validation tests for the `DW.BERT_AUSD_BP_TA_RN_DA_VDA_TK` job, structured as requested.

---

## Migration Validation Tests: DW.BERT_AUSD_BP_TA_RN_DA_VDA_TK

### 1. Output Parity - End-to-End Data Comparison

*   **Purpose:** To verify that the migrated BigQuery job produces an identical target table (`sof_ta_rn_da_vda_tk`) as the legacy Oracle job when given the same input data. This is the ultimate behavioral equivalence test.
*   **Setup:**
    1.  **Legacy Environment:**
        *   Ensure the Oracle source tables (`sof$ta_rn_einzeln`, `isbert_schema.dwtk_meldungen`) are populated with a representative, static dataset. This dataset should include various scenarios: rows that should be filtered out, rows that should be included, NULL values, and valid/invalid dates.
        *   Ensure the Oracle target table (`sof$ta_rn_da_vda_tk`) is empty before execution.
    2.  **Migrated Environment (GCP BigQuery):**
        *   Create BigQuery tables `your_dataset.sof_ta_rn_einzeln`, `your_dataset.sof_ta_rn_da_vda_tk`, and `your_metadata_dataset.dwtk_meldungen` using the provided DDLs.
        *   Load the *exact same* static dataset into `your_dataset.sof_ta_rn_einzeln` and `your_metadata_dataset.dwtk_meldungen` as used in the Oracle environment.
        *   Ensure `your_dataset.sof_ta_rn_da_vda_tk` is empty before execution.
*   **Action:**
    1.  Execute the legacy `DW.BERT_AUSD_BP_TA_RN_DA_VDA_TK` job in the Oracle/UC4 environment.
    2.  After the legacy job completes, extract all data from the Oracle `sof$ta_rn_da_vda_tk` table into a canonical format (e.g., CSV, JSON, or a temporary table in a comparison database).
    3.  Execute the migrated Airflow DAG `dw_bert_ausd_bp_ta_rn_da_vda_tk` in the GCP environment.
    4.  After the migrated job completes, extract all data from the BigQuery `your_dataset.sof_ta_rn_da_vda_tk` table.
    5.  Compare the extracted datasets.
*   **Pass/Fail Criterion:** The data extracted from the BigQuery target table must be *byte-for-byte identical* (after accounting for potential data type representation differences, e.g., floating point precision, or date/timestamp string formats if applicable) to the data extracted from the Oracle target table. This includes row count, column values, and order (if a stable sort is applied before comparison).

    ```python
    # Example Python (pytest) assertion for data comparison
    import pandas as pd
    from google.cloud import bigquery
    import cx_Oracle # Assuming cx_Oracle for legacy extraction

    def test_output_parity_sof_ta_rn_da_vda_tk():
        # --- Legacy Data Extraction ---
        # Replace with actual Oracle connection details and query
        oracle_conn_str = "user/password@host:port/service_name"
        oracle_query = "SELECT CNTRCT_ID, DA_RN_MSISDN, DA_RN_STATUS, TO_CHAR(DA_RN_VALID_TO, 'YYYY-MM-DD') AS DA_RN_VALID_TO, VDA_RN_MSISDN, VDA_RN_STATUS, TO_CHAR(VDA_RN_VALID_TO, 'YYYY-MM-DD') AS VDA_RN_VALID_TO, TK_RN_MSISDN, TK_RN_STATUS, TO_CHAR(TK_RN_VALID_TO, 'YYYY-MM-DD') AS TK_RN_VALID_TO FROM sof$ta_rn_da_vda_tk ORDER BY CNTRCT_ID, DA_RN_MSISDN"
        
        with cx_Oracle.connect(oracle_conn_str) as connection:
            oracle_df = pd.read_sql(oracle_query, connection)

        # --- Migrated Data Extraction ---
        bq_client = bigquery.Client()
        bq_query = """
        SELECT
          CNTRCT_ID,
          DA_RN_MSISDN,
          DA_RN_STATUS,
          FORMAT_DATE('%Y-%m-%d', DA_RN_VALID_TO) AS DA_RN_VALID_TO,
          VDA_RN_MSISDN,
          VDA_RN_STATUS,
          FORMAT_DATE('%Y-%m-%d', VDA_RN_VALID_TO) AS VDA_RN_VALID_TO,
          TK_RN_MSISDN,
          TK_RN_STATUS,
          FORMAT_DATE('%Y-%m-%d', TK_RN_VALID_TO) AS TK_RN_VALID_TO
        FROM `your_dataset.sof_ta_rn_da_vda_tk`
        ORDER BY CNTRCT_ID, DA_RN_MSISDN
        """
        bq_df = bq_client.query(bq_query).to_dataframe()

        # --- Comparison ---
        pd.testing.assert_frame_equal(oracle_df, bq_df, check_dtype=False, check_index=False)
    ```

### 2. Transformation Correctness - Filtering Logic

*   **Purpose:** To specifically verify that the `WHERE DA_RN_msisdn IS NOT NULL OR VDA_RN_msisdn IS NOT NULL OR TK_RN_msisdn IS NOT NULL` clause correctly filters rows.
*   **Setup:**
    1.  Populate `your_dataset.sof_ta_rn_einzeln` with a controlled dataset including:
        *   Rows where `DA_RN_msisdn` is NOT NULL (others can be NULL or NOT NULL).
        *   Rows where `VDA_RN_msisdn` is NOT NULL (others can be NULL or NOT NULL).
        *   Rows where `TK_RN_msisdn` is NOT NULL (others can be NULL or NOT NULL).
        *   Rows where ALL three (`DA_RN_msisdn`, `VDA_RN_msisdn`, `TK_RN_msisdn`) are NULL.
        *   Rows where ALL three are NOT NULL.
    2.  Ensure `your_metadata_dataset.dwtk_meldungen` has at least one entry for `BERT_DROP_TEMP_TABLE` to avoid the default `v_datum` (though `v_datum` is not used in the `INSERT` statement, it's good practice for a complete run).
    3.  Ensure `your_dataset.sof_ta_rn_da_vda_tk` is empty.
*   **Action:** Execute the migrated Airflow DAG `dw_bert_ausd_bp_ta_rn_da_vda_tk`.
*   **Pass/Fail Criterion:**
    *   Rows where at least one of `DA_RN_msisdn`, `VDA_RN_msisdn`, `TK_RN_msisdn` was NOT NULL in the source must be present in `your_dataset.sof_ta_rn_da_vda_tk`.
    *   Rows where all three (`DA_RN_msisdn`, `VDA_RN_msisdn`, `TK_RN_msisdn`) were NULL in the source must *not* be present in `your_dataset.sof_ta_rn_da_vda_tk`.
    *   The total count of rows in the target table should match the count of filtered rows from the source.

    ```sql
    -- BigQuery SQL assertion after job execution
    -- Count rows that should have been included
    SELECT COUNT(*)
    FROM `your_dataset.sof_ta_rn_einzeln`
    WHERE DA_RN_msisdn IS NOT NULL
       OR VDA_RN_msisdn IS NOT NULL
       OR TK_RN_msisdn IS NOT NULL;
    -- Expected: N (e.g., 5)

    -- Count rows actually in target
    SELECT COUNT(*) FROM `your_dataset.sof_ta_rn_da_vda_tk`;
    -- Expected: N (must match the above)

    -- Verify no rows with all three MSISDNs NULL made it through
    SELECT COUNT(*)
    FROM `your_dataset.sof_ta_rn_da_vda_tk` target
    JOIN `your_dataset.sof_ta_rn_einzeln` source
      ON target.CNTRCT_ID = source.CNTRCT_ID -- Assuming CNTRCT_ID is unique/sufficient for join
    WHERE source.DA_RN_msisdn IS NULL
      AND source.VDA_RN_msisdn IS NULL
      AND source.TK_RN_msisdn IS NULL;
    -- Expected: 0
    ```

### 3. Transformation Correctness - `v_datum` Derivation and `COALESCE`

*   **Purpose:** To verify the correct derivation of the `v_datum` variable, including `MAX(timecreated)` and `COALESCE` logic, even though it's not directly used in the final `INSERT` statement. This ensures the BigQuery SQL's `DECLARE` block behaves as expected.
*   **Setup:**
    1.  **Scenario A: Multiple `BERT_DROP_TEMP_TABLE` entries:**
        *   Insert multiple rows into `your_metadata_dataset.dwtk_meldungen` with `job_kennung = 'BERT_DROP_TEMP_TABLE'` and varying `timecreated` values (e.g., '2023-01-01 10:00:00 UTC', '2023-01-01 11:00:00 UTC', '2023-01-01 09:00:00 UTC').
        *   Also include rows with different `job_kennung` values.
    2.  **Scenario B: No `BERT_DROP_TEMP_TABLE` entries:**
        *   Ensure `your_metadata_dataset.dwtk_meldungen` is empty or contains only rows with `job_kennung != 'BERT_DROP_TEMP_TABLE'`.
    3.  **Scenario C: `timecreated` is NULL for `BERT_DROP_TEMP_TABLE`:**
        *   Insert a row into `your_metadata_dataset.dwtk_meldungen` with `job_kennung = 'BERT_DROP_TEMP_TABLE'` and `timecreated = NULL`.
*   **Action:**
    1.  For each scenario, execute only the `DECLARE v_datum` part of the BigQuery SQL (e.g., by running it in the BigQuery console or a test script).
*   **Pass/Fail Criterion:**
    *   **Scenario A:** `v_datum` must be `FORMAT_TIMESTAMP('%Y%m%d', MAX(timecreated))` for `job_kennung = 'BERT_DROP_TEMP_TABLE'` (e.g., '20230101' for the example above).
    *   **Scenario B:** `v_datum` must be '19000101'.
    *   **Scenario C:** `v_datum` must be '19000101' (as `FORMAT_TIMESTAMP(NULL, ...)` returns NULL, and `COALESCE` picks the default).

    ```sql
    -- BigQuery SQL for Scenario A (example)
    -- Setup:
    -- INSERT INTO `your_metadata_dataset.dwtk_meldungen` (job_kennung, timecreated) VALUES
    -- ('BERT_DROP_TEMP_TABLE', '2023-01-01 10:00:00 UTC'),
    -- ('BERT_DROP_TEMP_TABLE', '2023-01-01 11:00:00 UTC'),
    -- ('OTHER_JOB', '2023-01-02 12:00:00 UTC');

    DECLARE v_datum STRING DEFAULT (
      SELECT COALESCE(
        FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)),
        '19000101'
      )
      FROM `your_metadata_dataset.dwtk_meldungen` m
      WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    );
    SELECT v_datum;
    -- Expected output: '20230101'

    -- BigQuery SQL for Scenario B (example)
    -- Setup:
    -- TRUNCATE TABLE `your_metadata_dataset.dwtk_meldungen`;
    -- INSERT INTO `your_metadata_dataset.dwtk_meldungen` (job_kennung, timecreated) VALUES
    -- ('OTHER_JOB', '2023-01-02 12:00:00 UTC');

    DECLARE v_datum STRING DEFAULT (
      SELECT COALESCE(
        FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)),
        '19000101'
      )
      FROM `your_metadata_dataset.dwtk_meldungen` m
      WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    );
    SELECT v_datum;
    -- Expected output: '19000101'

    -- BigQuery SQL for Scenario C (example)
    -- Setup:
    -- TRUNCATE TABLE `your_metadata_dataset.dwtk_meldungen`;
    -- INSERT INTO `your_metadata_dataset.dwtk_meldungen` (job_kennung, timecreated) VALUES
    -- ('BERT_DROP_TEMP_TABLE', NULL);

    DECLARE v_datum STRING DEFAULT (
      SELECT COALESCE(
        FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)),
        '19000101'
      )
      FROM `your_metadata_dataset.dwtk_meldungen` m
      WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    );
    SELECT v_datum;
    -- Expected output: '19000101'
    ```

### 4. External-System Replacement - `TRUNCATE TABLE` Operation

*   **Purpose:** To verify that the `TRUNCATE TABLE` command correctly clears the target table before new data is inserted, mimicking the legacy Oracle `TRUNCATE` or `DELETE` behavior.
*   **Setup:**
    1.  Populate `your_dataset.sof_ta_rn_da_vda_tk` with a significant amount of dummy data (e.g., 1000 rows).
    2.  Populate `your_dataset.sof_ta_rn_einzeln` with a small, known number of rows (e.g., 5 rows) that *will* pass the filter.
    3.  Ensure `your_metadata_dataset.dwtk_meldungen` has valid data for `v_datum` derivation.
*   **Action:** Execute the migrated Airflow DAG `dw_bert_ausd_bp_ta_rn_da_vda_tk`.
*   **Pass/Fail Criterion:**
    *   The final row count in `your_dataset.sof_ta_rn_da_vda_tk` must be exactly the number of rows inserted from `your_dataset.sof_ta_rn_einzeln` that satisfy the `WHERE` clause, and *not* the sum of initial dummy data plus new data.
    *   The content of `your_dataset.sof_ta_rn_da_vda_tk` must only contain the newly inserted rows, with no trace of the initial dummy data.

    ```sql
    -- BigQuery SQL assertion after job execution
    SELECT COUNT(*) FROM `your_dataset.sof_ta_rn_da_vda_tk`;
    -- Expected: 5 (based on the example setup of 5 filtered rows)

    -- Verify content is only the new data
    SELECT COUNT(*)
    FROM `your_dataset.sof_ta_rn_da_vda_tk`
    WHERE CNTRCT_ID NOT IN ('new_contract_id_1', 'new_contract_id_2', ...); -- Check against known new data
    -- Expected: 0
    ```

### 5. Data Quality - Schema and Type Handling

*   **Purpose:** To ensure that the BigQuery target table schema (`your_dataset.sof_ta_rn_da_vda_tk`) correctly reflects the legacy Oracle schema, and that data types (especially `DATE` and `STRING`) are handled without loss or corruption during insertion.
*   **Setup:**
    1.  **Schema Definition:** Verify the DDL for `your_dataset.sof_ta_rn_da_vda_tk` matches the Oracle schema (column names, data types, nullability).
    2.  **Test Data:** Populate `your_dataset.sof_ta_rn_einzeln` with data covering:
        *   Valid dates for `DA_RN_VALID_TO`, `VDA_RN_VALID_TO`, `TK_RN_VALID_TO`.
        *   NULL values for all columns.
        *   String values that might contain special characters or varying lengths.
*   **Action:** Execute the migrated Airflow DAG `dw_bert_ausd_bp_ta_rn_da_vda_tk`.
*   **Pass/Fail Criterion:**
    *   **Schema:** The BigQuery table schema (e.g., obtained via `bq show --schema your_dataset.sof_ta_rn_da_vda_tk`) must match the expected schema from the DDL.
    *   **Data Types:** All columns in `your_dataset.sof_ta_rn_da_vda_tk` must have the correct BigQuery data types (e.g., `STRING` for MSISDNs, `DATE` for valid_to fields).
    *   **Value Integrity:** Sample data from the target table and verify that:
        *   Dates are correctly stored as `DATE` type and match the source values.
        *   NULL values in the source are correctly represented as NULL in the target.
        *   String values are identical to the source, without truncation or corruption.

    ```python
    # Example Python (pytest) assertion for schema and data type check
    from google.cloud import bigquery

    def test_schema_and_type_integrity():
        bq_client = bigquery.Client()
        table_id = "your_project.your_dataset.sof_ta_rn_da_vda_tk"
        table = bq_client.get_table(table_id)

        expected_schema = {
            "CNTRCT_ID": "STRING",
            "DA_RN_MSISDN": "STRING",
            "DA_RN_STATUS": "STRING",
            "DA_RN_VALID_TO": "DATE",
            "VDA_RN_MSISDN": "STRING",
            "VDA_RN_STATUS": "STRING",
            "VDA_RN_VALID_TO": "DATE",
            "TK_RN_MSISDN": "STRING",
            "TK_RN_STATUS": "STRING",
            "TK_RN_VALID_TO": "DATE",
        }

        actual_schema = {field.name: field.field_type for field in table.schema}

        assert actual_schema == expected_schema, f"Schema mismatch: {actual_schema} vs {expected_schema}"

        # Optional: Add data sampling and value integrity checks
        # query = "SELECT CNTRCT_ID, DA_RN_VALID_TO FROM `your_dataset.sof_ta_rn_da_vda_tk` LIMIT 10"
        # df = bq_client.query(query).to_dataframe()
        # assert df['DA_RN_VALID_TO'].dtype == 'datetime64[ns]' # Pandas represents BQ DATE as datetime
        # assert df['CNTRCT_ID'].iloc[0] == 'expected_value'
    ```

### 6. Data Quality - Row Count Assertion

*   **Purpose:** To confirm that the total number of rows processed and inserted into the target table matches the expected count based on the source data and filtering logic. This is a quick sanity check for data completeness.
*   **Setup:**
    1.  Populate `your_dataset.sof_ta_rn_einzeln` with a known number of rows, some of which will pass the filter and some will not.
    2.  Calculate the *expected* number of rows that should be inserted into `your_dataset.sof_ta_rn_da_vda_tk` based on the filtering logic.
    3.  Ensure `your_metadata_dataset.dwtk_meldungen` has valid data for `v_datum` derivation.
*   **Action:** Execute the migrated Airflow DAG `dw_bert_ausd_bp_ta_rn_da_vda_TK`.
*   **Pass/Fail Criterion:** The `COUNT(*)` from `your_dataset.sof_ta_rn_da_vda_tk` must exactly match the pre-calculated expected row count.

    ```sql
    -- BigQuery SQL assertion after job execution
    SELECT COUNT(*) FROM `your_dataset.sof_ta_rn_da_vda_tk`;
    -- Expected: [Pre-calculated count based on source data and filter]
    ```

### 7. External-System Replacement - Airflow Orchestration & Horizon Python Execution

*   **Purpose:** To verify that the Airflow DAG correctly triggers the Horizon Python script, and that the Python script executes the BigQuery SQL successfully. This tests the orchestration and control layer.
*   **Setup:**
    1.  Deploy the Airflow DAG `dw_bert_ausd_bp_ta_rn_da_vda_tk.py` to the Airflow environment.
    2.  Deploy the Horizon Python script `dw_bert_ausd_bp_ta_rn_da_vda_tk.py` to its designated location (e.g., GCS bucket for Dataproc).
    3.  Ensure all necessary BigQuery tables (`sof_ta_rn_einzeln`, `dwtk_meldungen`, `sof_ta_rn_da_vda_tk`) exist and are accessible.
*   **Action:** Manually trigger the `dw_bert_ausd_bp_ta_rn_da_vda_tk` DAG in Airflow.
*   **Pass/Fail Criterion:**
    *   The Airflow DAG run must complete successfully (green status).
    *   The logs for the `DataprocSubmitJobOperator` task should show the Horizon Python script starting and completing without errors.
    *   BigQuery audit logs should show the `TRUNCATE` and `INSERT` statements being executed by the service account associated with the Dataproc job.
    *   The `your_dataset.sof_ta_rn_da_vda_tk` table should be populated with data (verify with a simple `SELECT COUNT(*)`).

### 8. External-System Replacement - Horizon Python Parameter Handling & Date Logic

*   **Purpose:** To verify that the Horizon Python script correctly parses input parameters (e.g., `stichtag`, `wiederanlauf_wert`) and derives internal date variables (e.g., `p_datum_heute`, `p_datum_gestern`) as per the legacy KornShell logic.
*   **Setup:**
    1.  Create a test version of the Horizon Python script that includes print statements or mock functions to capture parsed parameters and derived dates, without actually executing BigQuery SQL.
    2.  Prepare a set of test cases for command-line arguments, including valid dates, invalid dates, and missing required parameters.
*   **Action:** Execute the test Horizon Python script directly from the command line with various parameter combinations.
*   **Pass/Fail Criterion:**
    *   The script must correctly parse all expected parameters (`job_kennung`, `eintrags_nr`, `stichtag`, `wiederanlauf_wert`).
    *   Date validation logic must correctly identify invalid date formats and raise appropriate errors (or log warnings).
    *   Derived date variables (`p_datum_heute`, `p_datum_gestern`) must be calculated correctly based on the `stichtag` parameter, matching the logic of `gestern.ksh`.
    *   Error handling (e.g., for missing parameters) should trigger the expected Python exceptions or logging behavior, replacing `DWMSG_MeldeFehler`.

    ```python
    # Example Python (pytest) for parameter parsing and date logic (mocking func_execute_bq)
    import pytest
    from unittest.mock import patch
    from datetime import date, timedelta

    # Assuming your Horizon Python script is structured like:
    # def main(job_kennung, eintrags_nr, stichtag, wiederanlauf_wert):
    #     # ... parameter parsing and date logic ...
    #     today = date.today() # or derived from stichtag
    #     yesterday = today - timedelta(days=1)
    #     # ... call script.func_execute_bq ...
    #     return parsed_stichtag, today, yesterday # For testing purposes

    @patch('your_horizon_script_module.script.func_execute_bq')
    def test_horizon_script_parameter_and_date_logic(mock_execute_bq):
        # Test case 1: Valid parameters
        stichtag_param = "2023-10-26"
        parsed_stichtag, derived_today, derived_yesterday = your_horizon_script_module.main(
            job_kennung="BERT", eintrags_nr="123", stichtag=stichtag_param, wiederanlauf_wert="N"
        )
        assert parsed_stichtag == date(2023, 10, 26)
        assert derived_today == date(2023, 10, 26) # Assuming stichtag dictates "today"
        assert derived_yesterday == date(2023, 10, 25)

        # Test case 2: Invalid date format
        with pytest.raises(ValueError, match="Invalid date format"):
            your_horizon_script_module.main(
                job_kennung="BERT", eintrags_nr="123", stichtag="2023/10/26", wiederanlauf_wert="N"
            )

        # Test case 3: Missing required parameter (e.g., stichtag)
        with pytest.raises(TypeError): # Or specific custom exception
            your_horizon_script_module.main(
                job_kennung="BERT", eintrags_nr="123", wiederanlauf_wert="N"
            )
    ```

---