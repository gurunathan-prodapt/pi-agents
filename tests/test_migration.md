As a senior data-migration QA engineer, I've analyzed the migration design for `r_ausd_geschaeftspartner.ksh` to Google Cloud Platform. The core logic, originally in `k_ausd_geschaeftspartner.ksh`, is now translated into a PySpark job executing BigQuery SQL. The orchestration is handled by an Airflow DAG.

A critical discrepancy has been identified: the legacy script's `Wiederanlaufwert` parameter, used for filtering `DWH_VERTRAG_ID`, is not present in the provided migrated BigQuery SQL. This represents a potential behavioral change and is highlighted in a dedicated test case.

The tests below are designed to prove behavioral equivalence, covering output parity, transformation correctness, external system replacements, and data quality.

---

## Migration Validation Tests: `r_ausd_geschaeftspartner.ksh`

### Test Environment Setup

To execute these tests, the following environments must be prepared:

*   **Legacy Environment:**
    *   A system capable of running KornShell scripts.
    *   Access to the original Oracle database containing `isbert_source_ds` and `isbert_target_ds` tables (or their Oracle equivalents).
    *   The original `r_ausd_geschaeftspartner.ksh` and `k_ausd_geschaeftspartner.ksh` scripts, along with all their utility script dependencies (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`).
*   **Migrated Environment (GCP):**
    *   A Google Cloud Project with BigQuery, Cloud Composer (Airflow), and Dataproc enabled.
    *   BigQuery datasets `isbert_source_ds` and `isbert_target_ds` created.
    *   All DDLs for `isbert_target_ds` tables (e.g., `sof_ta_segm_prem`, `sof_ta_p_gesch_part`, etc.) and `FOS_Tabelle` applied in BigQuery.
    *   The PySpark script (`k_ausd_geschaeftspartner.py`) and the BigQuery SQL script (`d_ausd_geschaeftspartner_bq.sql`) uploaded to a GCS bucket accessible by Dataproc.
    *   The Airflow DAG (`r_ausd_geschaeftspartner_dag.py`) deployed to Cloud Composer.
    *   A Dataproc cluster configured and available for the Airflow DAG to submit jobs.

**Data Synchronization:**
Crucially, the source data in the Oracle `isbert_source_ds` tables must be *identically replicated* to the BigQuery `isbert_source_ds` tables before running any comparison tests. This ensures a fair "apples-to-apples" comparison.

---

### Test Case 1: End-to-End Output Parity (Core Tables)

*   **Purpose:** Verify that the migrated job produces the exact same data in its primary output tables as the legacy job, given identical inputs and parameters. This is the most critical test for behavioral equivalence.
*   **Setup:**
    1.  Populate all `isbert_source_ds` tables (Oracle and BigQuery) with a comprehensive test dataset, including:
        *   Various `BP_ID`s, `CNTRCT_ID`s, `BPR_ID`s.
        *   Data covering all `CASE` statement branches (e.g., `segment_id` values 11-16 and others).
        *   Rows with `NULL` values in fields like `surname_s`, `first_name_g`, `corp_unit`, `street`, `pobox`, `modified_at`, `valid_to`.
        *   Rows with dates (`VALID_FROM`, `VALID_TO`, `INSERT_AT`, `MODIFIED_AT`) both before, on, and after a chosen `Stichtag`.
        *   Rows where `is_production` is 0 and 1.
    2.  Ensure target tables (`sof_ta_segm_prem`, `sof_ta_bpr_dn_evn`, `sof_ta_bpr_dn_evn_his`, `sof_ta_p_gesch_part`, `sof_ta_p_dn_nutzer`, `sof_ta_p_evn_empf`) are empty in both environments.
    3.  Choose a `Stichtag` (e.g., `20230315`) and a `Wiederanlaufwert` (e.g., `0` for initial run, or a specific `DWH_VERTRAG_ID` if the logic were present in the migrated code). For this test, we'll use `Wiederanlaufwert=0` to avoid the identified discrepancy for now.
*   **Action:**
    1.  **Legacy:** Execute `r_ausd_geschaeftspartner.ksh -s 15032023 -l 0` in the legacy environment.
    2.  **Migrated:** Trigger the Airflow DAG `r_ausd_geschaeftspartner_dag` with a manual configuration: `{"stichtag": "20230315"}`.
    3.  After both jobs complete, extract the full content of the following target tables from both environments:
        *   `sof_ta_segm_prem`
        *   `sof_ta_bpr_dn_evn`
        *   `sof_ta_bpr_dn_evn_his`
        *   `sof_ta_p_gesch_part`
        *   `sof_ta_p_dn_nutzer`
        *   `sof_ta_p_evn_empf`
*   **Pass/Fail Criteria:**
    *   The row counts for each corresponding target table must be identical between legacy (Oracle) and migrated (BigQuery).
    *   The full dataset content (all columns, all rows, order-agnostic comparison after sorting by a unique key or all columns) for each corresponding target table must be identical.
    *   No errors reported in either job's logs.

    ```python
    # Example Python (pytest) assertion for output parity
    import pandas as pd
    from google.cloud import bigquery
    import cx_Oracle # Assuming Oracle client for legacy data extraction

    def get_oracle_data(table_name, connection_string):
        # Implement logic to connect to Oracle and fetch data
        # Example:
        # conn = cx_Oracle.connect(connection_string)
        # cursor = conn.cursor()
        # cursor.execute(f"SELECT * FROM {table_name} ORDER BY 1, 2, ...") # Order by all columns for consistent comparison
        # df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
        # conn.close()
        # return df
        pass # Placeholder

    def get_bigquery_data(table_name, project_id):
        client = bigquery.Client(project=project_id)
        query = f"SELECT * FROM {table_name} ORDER BY 1, 2, ..." # Order by all columns for consistent comparison
        df = client.query(query).to_dataframe()
        return df

    def test_output_parity_sof_ta_segm_prem(oracle_conn_str, bq_project_id):
        legacy_df = get_oracle_data("ISBERT_TARGET_DS.SOF_TA_SEGM_PREM", oracle_conn_str)
        migrated_df = get_bigquery_data("isbert_target_ds.sof_ta_segm_prem", bq_project_id)
        pd.testing.assert_frame_equal(legacy_df, migrated_df, check_dtype=True, check_exact=False) # check_exact=False for float precision
        assert len(legacy_df) > 0, "Legacy table is empty, check test data setup."

    # Repeat similar tests for other target tables:
    # test_output_parity_sof_ta_bpr_dn_evn
    # test_output_parity_sof_ta_bpr_dn_evn_his
    # test_output_parity_sof_ta_p_gesch_part
    # test_output_parity_sof_ta_p_dn_nutzer
    # test_output_parity_sof_ta_p_evn_empf
    ```

### Test Case 2: Transformation Correctness - `sof_ta_p_gesch_part` Logic

*   **Purpose:** Verify the complex `CASE` statements, `COALESCE` functions, and string concatenations in the `sof_ta_p_gesch_part` insertion logic.
*   **Setup:**
    1.  Populate `isbert_source_ds.sof_ta_e_reach_gp`, `isbert_source_ds.sof_ta_e_business_gp`, and `isbert_target_ds.sof_ta_segm_prem` with specific test data covering:
        *   `rg.surname_s IS NULL` and `bp.title` present.
        *   `rg.surname_s` present.
        *   `rg.corp_unit` present and `bp.organisation_name` present.
        *   `rg.corp_unit` NULL and `bp.organisation_name` present.
        *   `rg.street IS NULL` and `rg.pobox IS NULL`.
        *   `rg.street IS NULL` and `rg.pobox` present.
        *   `rg.street` present.
        *   All `pr.segment_id` values (11-16) and other values.
        *   Rows where `rg.bp_id` matches `bp.bp_id` and `pr.bp_id`, and rows where `pr.bp_id` is not matched (LEFT JOIN).
    2.  Ensure `isbert_target_ds.sof_ta_p_gesch_part` is empty.
    3.  Choose a `Stichtag` (e.g., `20230315`).
*   **Action:**
    1.  **Legacy:** Execute `r_ausd_geschaeftspartner.ksh -s 15032023 -l 0`.
    2.  **Migrated:** Trigger the Airflow DAG `r_ausd_geschaeftspartner_dag` with `{"stichtag": "20230315"}`.
    3.  Query `sof_ta_p_gesch_part` from both environments.
*   **Pass/Fail Criteria:**
    *   Compare specific rows and columns that exercise the `CASE` and `COALESCE` logic. For example:
        *   `AKAD_TITEL`: Should be `bp.title` when `rg.surname_s` is NULL, else empty.
        *   `FIRMENNAME`: Should be `rg.corp_unit` if not NULL, else `bp.organisation_name`.
        *   `STRASSE`: Should be `CONCAT('Postfach ', rg.pobox)` if `rg.street` is NULL and `rg.pobox` is not NULL.
        *   `KUNDE_SEGMENT_ID`: Should map 11-16 to 'SP', 'RV', etc., and others to their `CAST`ed string value.
    *   All assertions from Test Case 1 for this table must pass.

    ```sql
    -- Example BigQuery SQL assertion for specific transformation logic
    -- This would be run after the migrated job completes.
    SELECT
        CNTRCT_ID,
        NAMENSZUSATZ,
        ADRESSZUSATZ,
        FIRMENNAME,
        AKAD_TITEL,
        NACHNAME,
        VORNAME,
        LAND,
        PLZ,
        WOHNORT,
        STRASSE,
        KUNDE_SEGMENT_ID,
        PREM_SEGMENT_ID,
        TM_KUNDENNUMMER,
        MWST_KENNZEICHEN,
        ORGANISATIONSEINHEIT
    FROM
        isbert_target_ds.sof_ta_p_gesch_part
    WHERE
        -- Filter for a specific test case row that exercises complex logic
        CNTRCT_ID = 'TEST_CNTRCT_ID_123'
    QUALIFY ROW_NUMBER() OVER (ORDER BY CNTRCT_ID) = 1; -- Ensure deterministic order if multiple rows match

    -- Expected output for this row should match the legacy output exactly.
    -- This can be compared programmatically or manually for specific complex rows.
    ```

### Test Case 3: Transformation Correctness - `sof_ta_bpr_dn_evn_his` and `sof_ta_bpr_dn_evn` Date Filtering and Window Function

*   **Purpose:** Verify the date filtering conditions (`INSERT_AT`, `MODIFIED_AT`, `VALID_FROM`) and the `MAX(COALESCE(valid_to, ...)) OVER (PARTITION BY ...)` window function logic.
*   **Setup:**
    1.  Populate `isbert_source_ds.pds_ta_bpri_com` with data that specifically tests:
        *   `bpr_id` values both in and out of the `IN` list (31, 2759, ...).
        *   `insert_at` dates before, on, and after the `Stichtag`.
        *   `modified_at` being `NULL` or after `Stichtag`.
        *   `valid_from` dates before, on, and after the `Stichtag`.
        *   `is_production` being 0 and 1.
        *   Multiple entries for the same `cntrct_id`, `bpr_id` with varying `valid_to` dates to test the `MAX` window function, including `NULL` `valid_to` values.
    2.  Ensure `isbert_target_ds.sof_ta_bpr_dn_evn_his` and `isbert_target_ds.sof_ta_bpr_dn_evn` are empty.
    3.  Choose a `Stichtag` (e.g., `20230601`).
*   **Action:**
    1.  **Legacy:** Execute `r_ausd_geschaeftspartner.ksh -s 01062023 -l 0`.
    2.  **Migrated:** Trigger the Airflow DAG `r_ausd_geschaeftspartner_dag` with `{"stichtag": "20230601"}`.
    3.  Query `sof_ta_bpr_dn_evn_his` and `sof_ta_bpr_dn_evn` from both environments.
*   **Pass/Fail Criteria:**
    *   The `sof_ta_bpr_dn_evn_his` table should contain only records matching all `WHERE` clause conditions.
    *   The `sof_ta_bpr_dn_evn` table should contain only the "latest" valid record (based on `valid_to` or `47121231`) for each `(cntrct_id, bpr_id)` partition.
    *   `COLUMN_5VALID_TO` should correctly handle `COALESCE(bp.valid_to, PARSE_DATE('%Y%m%d', '47121231'))`.
    *   All assertions from Test Case 1 for these tables must pass.

    ```sql
    -- Example BigQuery SQL assertion for date filtering and window function
    -- Verify rows in sof_ta_bpr_dn_evn_his
    SELECT COUNT(*) FROM isbert_target_ds.sof_ta_bpr_dn_evn_his
    WHERE
        NOT (bpr_id IN (31, 2759, 2800, 2835, 2836, 2837, 2839, 2840, 3056)
        AND insert_at <= PARSE_DATE('%Y%m%d', '20230601')
        AND (modified_at IS NULL OR modified_at > PARSE_DATE('%Y%m%d', '20230601'))
        AND valid_from <= PARSE_DATE('%Y%m%d', '20230601')
        AND is_production = 1);
    -- Expected result: 0 (no rows should violate the filter)

    -- Verify the window function logic in sof_ta_bpr_dn_evn
    -- Select a specific contract_id and bpr_id with multiple historical entries
    SELECT
        t1.CNTRCT_ID,
        t1.BPR_ID,
        t1.BPR_INSTANCE_ID,
        t1.COLUMN_5VALID_TO,
        t2.max_valid_to
    FROM
        isbert_target_ds.sof_ta_bpr_dn_evn AS t1
    JOIN
        (SELECT CNTRCT_ID, BPR_ID, MAX(COALESCE(VALID_TO, PARSE_DATE('%Y%m%d', '47121231'))) AS max_valid_to
         FROM isbert_target_ds.sof_ta_bpr_dn_evn_his GROUP BY CNTRCT_ID, BPR_ID) AS t2
    ON t1.CNTRCT_ID = t2.CNTRCT_ID AND t1.BPR_ID = t2.BPR_ID
    WHERE t1.COLUMN_5VALID_TO != t2.max_valid_to;
    -- Expected result: 0 (all entries in sof_ta_bpr_dn_evn should correspond to the max_valid_to from history)
    ```

### Test Case 4: Parameter Handling and Default Values

*   **Purpose:** Verify that the Airflow DAG correctly handles `Stichtag` parameter passing, including its default value (`ds_nodash`), and that the PySpark script correctly substitutes it into the SQL.
*   **Setup:**
    1.  Ensure source data is available.
    2.  Ensure target tables are empty.
*   **Action:**
    1.  **Legacy:** Execute `r_ausd_geschaeftspartner.ksh` *without* the `-s` parameter. Note the `Stichtag` used in the logs (it should be `sysdate` or `MIN(sysdate,maxladedatum)` as per script logic, but the provided script defaults to `sysdate` if `-s` is not set). Let's assume `sysdate` is `20230701`.
    2.  **Migrated:** Trigger the Airflow DAG `r_ausd_geschaeftspartner_dag` *without* a `stichtag` in the DAG run configuration. The `ds_nodash` for this run should be `20230701`.
    3.  Verify the `stichtag_yyyymmdd` value logged by the PySpark job in Cloud Logging.
    4.  Query the target tables (e.g., `sof_ta_bpr_dn_evn_his`) and check if the date filters (`PARSE_DATE('%Y%m%d', @p_stichtag_yyyymmdd)`) were applied using `20230701`.
*   **Pass/Fail Criteria:**
    *   The `Stichtag` used by the legacy job (from its logs) must match the `stichtag_yyyymmdd` passed to the PySpark job (from Cloud Logging).
    *   The data in the target tables (e.g., `sof_ta_bpr_dn_evn_his`) must reflect the filtering based on the correct `Stichtag` value in both environments.
    *   The Airflow DAG should complete successfully without errors related to missing parameters.

    ```python
    # Pytest assertion for Airflow DAG run configuration
    # This would be part of an Airflow DAG testing framework (e.g., using `airflow dags test`)
    # or by inspecting logs after a manual trigger.
    def test_airflow_stichtag_default_behavior(mock_dag_run):
        # Simulate a DAG run without 'stichtag' in conf
        # Check that the rendered template for stichtag_yyyymmdd resolves to ds_nodash
        # This requires mocking Airflow context or inspecting rendered templates.
        # For actual execution, check Cloud Logging for the PySpark job's arguments.
        pass
    ```

### Test Case 5: Data Quality - Row Counts and Nullability

*   **Purpose:** Verify basic data quality assertions, specifically row counts and the absence of unexpected NULLs in critical fields, after migration.
*   **Setup:**
    1.  Run both legacy and migrated jobs with a representative dataset (as in Test Case 1).
*   **Action:**
    1.  Query row counts for all target tables in both environments.
    2.  Query for `NULL` values in columns that are expected to be non-NULL (e.g., `CNTRCT_ID`, `BP_ID` in `sof_ta_segm_prem`).
    3.  Query for `NULL` values in columns where `COALESCE` was applied, to ensure the fallback worked as expected (e.g., `FIRMENNAME`, `NACHNAME`, `VORNAME`, `STRASSE` in `sof_ta_p_gesch_part`).
*   **Pass/Fail Criteria:**
    *   Row counts for each target table must be identical between legacy and migrated.
    *   No unexpected `NULL` values should be found in critical columns in the migrated BigQuery tables.
    *   For columns where `COALESCE` was used, if the primary source was `NULL`, the fallback value should be present (e.g., `bp.organisation_name` for `FIRMENNAME` if `rg.corp_unit` was NULL).

    ```sql
    -- Example BigQuery SQL assertions for data quality
    -- Row count check (should match legacy)
    SELECT COUNT(*) FROM isbert_target_ds.sof_ta_p_gesch_part;

    -- Nullability check for a critical column
    SELECT COUNT(*) FROM isbert_target_ds.sof_ta_p_gesch_part WHERE CNTRCT_ID IS NULL;
    -- Expected result: 0

    -- Check COALESCE logic for FIRMENNAME
    SELECT COUNT(*) FROM isbert_target_ds.sof_ta_p_gesch_part
    WHERE FIRMENNAME IS NULL;
    -- Expected result: 0 (assuming either rg.corp_unit or bp.organisation_name is always present)

    -- Check KUNDE_SEGMENT_ID transformation
    SELECT DISTINCT KUNDE_SEGMENT_ID FROM isbert_target_ds.sof_ta_p_gesch_part
    WHERE KUNDE_SEGMENT_ID NOT IN ('SP', 'RV', 'MA', 'SO', 'VJ', 'IN')
    AND NOT REGEXP_CONTAINS(KUNDE_SEGMENT_ID, r'^[0-9]+$');
    -- Expected result: 0 (all values should be one of the mapped strings or a numeric string)
    ```

### Test Case 6: External System Replacement - Airflow Orchestration & PySpark Execution

*   **Purpose:** Verify that the Airflow DAG correctly triggers the PySpark job on Dataproc, and that the PySpark job successfully executes the BigQuery SQL script.
*   **Setup:**
    1.  Ensure the Airflow DAG is deployed.
    2.  Ensure the PySpark script and SQL file are in the specified GCS paths.
    3.  Ensure the Dataproc cluster is running and configured correctly.
*   **Action:**
    1.  Trigger the Airflow DAG `r_ausd_geschaeftspartner_dag` with a valid `stichtag` (e.g., `{"stichtag": "20230101"}`).
    2.  Monitor the Airflow UI for task status.
    3.  Check Cloud Logging for the `run_contract_cache_initial_load` task and the PySpark job logs.
*   **Pass/Fail Criteria:**
    *   The `run_contract_cache_initial_load` task in Airflow completes successfully (green status).
    *   Cloud Logging shows the PySpark job starting, reading the SQL file, substituting the `stichtag`, and executing all BigQuery statements without errors.
    *   No `DataprocSubmitPySparkJobOperator` specific errors (e.g., cluster connection issues, GCS file access issues).

    ```python
    # This test is primarily observational via Airflow UI and Cloud Logging.
    # Automated testing would involve Airflow's own testing utilities or
    # integration tests that trigger the DAG and check its state and logs.
    # Example (conceptual):
    # from airflow.models.dagrun import DagRun
    # from airflow.utils.state import State
    #
    # def test_dag_execution_success(dag_id="r_ausd_geschaefts_partner_dag"):
    #     dag = DAG(dag_id, start_date=pendulum.datetime(2023, 1, 1))
    #     # ... (define tasks as in the actual DAG)
    #     dr = dag.create_dagrun(
    #         state=State.RUNNING,
    #         execution_date=pendulum.now(),
    #         conf={"stichtag": "20230101"}
    #     )
    #     # Simulate execution or wait for actual execution
    #     # Assert dr.get_state() == State.SUCCESS
    #     # Further assertions would involve checking Cloud Logging for PySpark output.
    ```

### Test Case 7: Schema Validation

*   **Purpose:** Verify that the BigQuery target table schemas match the expected DDLs and are compatible with the data being inserted.
*   **Setup:**
    1.  Ensure all target table DDLs are applied in BigQuery.
    2.  Run the migrated job with a full dataset.
*   **Action:**
    1.  Inspect the schema of each target table in BigQuery (e.g., `isbert_target_ds.sof_ta_p_gesch_part`).
    2.  Compare the column names, data types, and nullability properties against the provided DDLs and the expected schema from the legacy Oracle tables.
*   **Pass/Fail Criteria:**
    *   All column names and their corresponding BigQuery data types must match the DDLs.
    *   Data types should be appropriate for the data (e.g., `DATE` for dates, `STRING` for text, `INT64` for integers).
    *   No data type conversion errors should occur during job execution.

    ```python
    # Example Python (pytest) assertion for schema validation
    from google.cloud import bigquery

    def get_bq_schema(table_id, project_id):
        client = bigquery.Client(project=project_id)
        table = client.get_table(table_id)
        return {field.name: field.field_type for field in table.schema}

    def test_schema_sof_ta_p_gesch_part(bq_project_id):
        expected_schema = {
            "CNTRCT_ID": "STRING",
            "NAMENSZUSATZ": "STRING",
            "ADRESSZUSATZ": "STRING",
            "FIRMENNAME": "STRING",
            "AKAD_TITEL": "STRING",
            "NACHNAME": "STRING",
            "VORNAME": "STRING",
            "LAND": "STRING",
            "PLZ": "STRING",
            "WOHNORT": "STRING",
            "STRASSE": "STRING",
            "KUNDE_SEGMENT_ID": "STRING",
            "PREM_SEGMENT_ID": "INT64",
            "TM_KUNDENNUMMER": "STRING",
            "MWST_KENNZEICHEN": "STRING",
            "ORGANISATIONSEINHEIT": "STRING"
        }
        actual_schema = get_bq_schema("isbert_target_ds.sof_ta_p_gesch_part", bq_project_id)
        assert actual_schema == expected_schema, "Schema mismatch for sof_ta_p_gesch_part"

    # Repeat for all other target tables.
    ```

### Test Case 8: **CRITICAL DISCREPANCY** - `Wiederanlaufwert` Handling

*   **Purpose:** Highlight and verify the behavioral difference regarding the `Wiederanlaufwert` parameter. The legacy script uses it to filter `DWH_VERTRAG_ID > Wiederanlaufwert`. This logic is *missing* in the migrated BigQuery SQL.
*   **Setup:**
    1.  Populate `DWH_VERTRAG_ID` (or its equivalent source table in `isbert_source_ds` that `k_ausd_geschaeftspartner.ksh` would have read from) with a range of numeric IDs.
    2.  Ensure some `DWH_VERTRAG_ID` values are less than, equal to, and greater than a chosen `Wiederanlaufwert`.
    3.  Ensure target tables are empty.
    4.  Choose a `Stichtag` (e.g., `20230315`) and a `Wiederanlaufwert` (e.g., `1000`).
*   **Action:**
    1.  **Legacy:** Execute `r_ausd_geschaeftspartner.ksh -s 15032023 -l 1000`. Observe the output (e.g., `FOS-Tabelle` or intermediate tables) to confirm only records with `DWH_VERTRAG_ID > 1000` are processed.
    2.  **Migrated:** Trigger the Airflow DAG `r_ausd_geschaeftspartner_dag` with `{"stichtag": "20230315"}`. (Note: `wiederanlaufwert` is not passed to the PySpark script as per the provided code).
    3.  Query the target tables (e.g., `sof_ta_p_gesch_part`) in BigQuery.
*   **Pass/Fail Criteria:**
    *   **FAIL (Expected):** The migrated job will *not* filter records based on `Wiederanlaufwert`. The output in BigQuery will contain records that would have been excluded by the legacy job when `Wiederanlaufwert` was set to a non-zero value.
    *   This test case is designed to *fail* to highlight the missing logic. A successful outcome would require the `Wiederanlaufwert` logic to be implemented in the PySpark/BigQuery SQL.
    *   **Recommendation:** The `Wiederanlaufwert` logic needs to be analyzed from the original `k_ausd_geschaeftspartner.ksh` and implemented in the PySpark/BigQuery SQL. If `DWH_VERTRAG_ID` is not directly used in the provided SQL, its mapping to the current `isbert_source_ds` tables needs clarification.

    ```sql
    -- Example BigQuery SQL assertion to demonstrate the missing filter
    -- This query would be run after the migrated job completes.
    -- Assuming 'DWH_VERTRAG_ID' maps to 'CNTRCT_ID' in sof_ta_p_gesch_part for this example.
    SELECT COUNT(*)
    FROM isbert_target_ds.sof_ta_p_gesch_part
    WHERE CAST(CNTRCT_ID AS INT64) <= 1000; -- Assuming CNTRCT_ID is numeric and maps to DWH_VERTRAG_ID
    -- Expected result (if logic were implemented): 0
    -- Actual result (with current migrated code): > 0 (if such records exist in source)
    ```

### Test Case 9: Error Handling and Logging

*   **Purpose:** Verify that the migrated job's error handling and logging mechanisms (Airflow, Cloud Logging) are robust and provide sufficient information for debugging, similar to the legacy script's `f_alis_msgerr.ksh`.
*   **Setup:**
    1.  Introduce a controlled error condition in the source data or BigQuery environment (e.g., a schema mismatch, a division by zero in a hypothetical calculation, or make a source table temporarily inaccessible).
    2.  Ensure target tables are empty.
*   **Action:**
    1.  **Legacy:** Execute `r_ausd_geschaeftspartner.ksh` with the error condition. Observe the output to `LogDatei` and the exit code.
    2.  **Migrated:** Trigger the Airflow DAG `r_ausd_geschaeftspartner_dag` with the error condition.
    3.  Monitor the Airflow UI for task status and check Cloud Logging for the PySpark job.
*   **Pass/Fail Criteria:**
    *   **Legacy:** The script should log the error message to `LogDatei` and exit with a non-zero error code (e.g., `ErrNr`).
    *   **Migrated:**
        *   The `run_contract_cache_initial_load` task in Airflow should fail (red status).
        *   Cloud Logging should contain detailed error messages from the PySpark job, including stack traces if applicable, clearly indicating the cause of the failure.
        *   Airflow's native alerting (if configured) should trigger.
    *   The level of detail and clarity of error messages should be comparable to or better than the legacy system.

---

### Test Case 10: `FOS-Tabelle` Output Parity (Conceptual)

*   **Purpose:** Verify the final `FOS-Tabelle` output, assuming it's a derived view or table from the intermediate `sof_ta_p_...` tables.
*   **Setup:**
    1.  Complete Test Case 1 successfully, ensuring all intermediate `sof_ta_p_...` tables are identical.
    2.  Define the `FOS-Tabelle` logic in both legacy (Oracle view/script) and migrated (BigQuery view/script) environments. For this test, let's assume `FOS-Tabelle` is a simple `UNION ALL` of key fields from `sof_ta_p_gesch_part`, `sof_ta_p_dn_nutzer`, and `sof_ta_p_evn_empf`.
*   **Action:**
    1.  After running both legacy and migrated jobs (as in Test Case 1), query the `FOS-Tabelle` (or its equivalent derived output) from both environments.
*   **Pass/Fail Criteria:**
    *   The row counts for `FOS-Tabelle` must be identical.
    *   The full dataset content (all columns, all rows, order-agnostic comparison) for `FOS-Tabelle` must be identical.

    ```sql
    -- Example BigQuery SQL for a conceptual FOS_Tabelle view
    -- This view would need to be created in BigQuery for the test.
    CREATE OR REPLACE VIEW isbert_target_ds.FOS_Tabelle_View AS
    SELECT
        CNTRCT_ID AS contract_id,
        FIRMENNAME AS customer_name,
        WOHNORT AS city,
        KUNDE_SEGMENT_ID AS segment_id,
        PARSE_DATE('%Y%m%d', @p_stichtag_yyyymmdd) AS processing_date -- Stichtag needs to be passed or derived
    FROM isbert_target_ds.sof_ta_p_gesch_part
    UNION ALL
    SELECT
        CNTRCT_ID AS contract_id,
        FIRMENNAME AS customer_name,
        WOHNORT AS city,
        NULL AS segment_id, -- Assuming this field might not exist in all source tables
        PARSE_DATE('%Y%m%d', @p_stichtag_yyyymmdd) AS processing_date
    FROM isbert_target_ds.sof_ta_p_dn_nutzer
    UNION ALL
    SELECT
        CNTRCT_ID AS contract_id,
        FIRMENNAME AS customer_name,
        WOHNORT AS city,
        NULL AS segment_id,
        PARSE_DATE('%Y%m%d', @p_stichtag_yyyymmdd) AS processing_date
    FROM isbert_target_ds.sof_ta_p_evn_empf;

    -- Then, compare the content of this view between legacy and migrated.
    ```