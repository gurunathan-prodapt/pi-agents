As a senior data-migration QA engineer, I've analyzed the provided migration design and legacy KornShell script (`k_ausd_v_ta_vvl_dwh.ksh`) to BigQuery. The migration involves re-platforming a control script and its invoked SQL logic into BigQuery stored procedures, with external orchestration via Cloud Composer.

The following test cases are designed to ensure behavioral equivalence, transformation correctness, proper handling of external system replacements, and data quality in the migrated BigQuery solution.

---

## Migration Validation Tests for `k_ausd_v_ta_vvl_dwh.ksh`

**Assumptions:**
*   `your_gcp_project_id` and `your_bq_dataset_id` are placeholders for actual BigQuery project and dataset IDs.
*   `legacy_oracle_schema` is the schema name for legacy Oracle tables.
*   Test data can be consistently loaded into both legacy Oracle and BigQuery environments.
*   A Python `pytest` framework is used for test execution, with helper functions/fixtures for interacting with BigQuery (`bq_client`) and executing legacy KornShell scripts (`run_legacy_ksh_script`, `get_oracle_data`).
*   The actual content of `d_ausd_v_ta_vvl_dwh.sql` and `DWPA_UTIL_SKRIPT` is unknown, so tests for core transformation logic will be generalized or marked as placeholders requiring specific details.

---

### 1. Output Parity Tests

These tests ensure that given the same input data, the migrated BigQuery job produces identical output data and record counts as the legacy KornShell job.

#### Test Case 1.1: End-to-End Data Parity - `SOF_TA_VVL_DWH`

*   **Purpose:** Verify that the data written to the `SOF_TA_VVL_DWH` target table by the migrated BigQuery job is byte-for-byte identical to the data written by the legacy job to its corresponding Oracle table.
*   **Setup:**
    1.  Load a predefined, representative dataset into both the legacy Oracle `DWTK_MELDUNGEN` and `DWH$TA_F_VVL_EREIGNISSE` tables, and their BigQuery counterparts (`your_bq_dataset_id.DWTK_MELDUNGEN`, `your_bq_dataset_id.DWH_TA_F_VVL_EREIGNISSE`).
    2.  Ensure both legacy Oracle `SOF$TA_VVL_DWH` and BigQuery `your_bq_dataset_id.SOF_TA_VVL_DWH` tables are empty before execution.
    3.  Define `p_JobKennung` and `p_EintragsNr` parameters (e.g., `TEST_JOB_1`, `20231027`).
*   **Action:**
    1.  Execute the legacy `k_ausd_v_ta_vvl_dwh.ksh` script with the defined parameters.
    2.  Execute the migrated `r_ausd_vertrag_control` BigQuery stored procedure with the same parameters.
    3.  Extract all data from the legacy Oracle `SOF$TA_VVL_DWH` table.
    4.  Extract all data from the BigQuery `your_bq_dataset_id.SOF_TA_VVL_DWH` table.
*   **Pass/Fail Criterion:**
    *   The number of rows in both extracted datasets must be equal.
    *   After sorting both datasets by a common key (or all columns if no natural key), the content of all columns must be identical.
    *   The job execution for both legacy and migrated systems must complete successfully (exit code 0 for legacy, no unhandled exceptions for BigQuery).

*   **Runnable Test Code (Python/Pytest with SQL Assertions):**

    ```python
    import pytest
    from google.cloud import bigquery
    import subprocess
    import pandas as pd
    from pandas.testing import assert_frame_equal

    # Assume bq_client and oracle_client are pytest fixtures providing BigQuery and Oracle connections
    # Assume helper functions for data loading and legacy script execution

    PROJECT_ID = "your_gcp_project_id"
    DATASET_ID = "your_bq_dataset_id"
    LEGACY_ORACLE_SCHEMA = "legacy_oracle_schema"

    def load_test_data(bq_client, oracle_client, source_data_path):
        """Helper to load test data into both environments."""
        # This function would read from source_data_path (e.g., CSV, JSON)
        # and insert into DWTK_MELDUNGEN and DWH_TA_F_VVL_EREIGNISSE in both Oracle and BigQuery.
        # For brevity, actual implementation is omitted.
        print(f"Loading test data from {source_data_path} into Oracle and BigQuery...")
        # Example:
        # bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.DWTK_MELDUNGEN`").result()
        # bq_client.query(f"INSERT INTO `{PROJECT_ID}.{DATASET_ID}.DWTK_MELDUNGEN` ...").result()
        # oracle_client.execute("TRUNCATE TABLE DWTK_MELDUNGEN")
        # oracle_client.execute("INSERT INTO DWTK_MELDUNGEN ...")
        pass

    def run_legacy_ksh_script(job_kennung, eintrags_nr):
        """Executes the legacy ksh script and returns stdout, stderr, exit_code."""
        script_path = "vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_dwh.ksh"
        command = [script_path, "-j", job_kennung, "-f", eintrags_nr]
        result = subprocess.run(command, capture_output=True, text=True, shell=True)
        return result.stdout, result.stderr, result.returncode

    def get_oracle_data(oracle_client, table_name):
        """Fetches all data from a given Oracle table as a Pandas DataFrame."""
        query = f"SELECT * FROM {LEGACY_ORACLE_SCHEMA}.{table_name} ORDER BY 1" # Order by first column for consistency
        df = pd.read_sql(query, oracle_client)
        return df

    def get_bigquery_data(bq_client, table_name):
        """Fetches all data from a given BigQuery table as a Pandas DataFrame."""
        query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}` ORDER BY 1" # Order by first column for consistency
        df = bq_client.query(query).to_dataframe()
        return df

    @pytest.fixture(scope="module", autouse=True)
    def setup_test_environment(bq_client, oracle_client):
        """Fixture to set up and tear down test data and environments."""
        # Clear target tables before tests
        bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.SOF_TA_VVL_DWH`").result()
        bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.VIA`").result()
        # oracle_client.execute(f"TRUNCATE TABLE {LEGACY_ORACLE_SCHEMA}.SOF$TA_VVL_DWH")
        # oracle_client.execute(f"TRUNCATE TABLE {LEGACY_ORACLE_SCHEMA}.VIA")
        # oracle_client.commit() # Assuming DDL/DML needs commit

        # Load common test data
        load_test_data(bq_client, oracle_client, "path/to/test_data_scenario_1.csv")

        yield # Run tests

        # Teardown (optional, depending on test strategy)
        # bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.SOF_TA_VVL_DWH`").result()
        # bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.VIA`").result()
        # bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.DWTK_MELDUNGEN`").result()
        # bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.DWH_TA_F_VVL_EREIGNISSE`").result()
        # oracle_client.execute(f"TRUNCATE TABLE {LEGACY_ORACLE_SCHEMA}.SOF$TA_VVL_DWH")
        # oracle_client.execute(f"TRUNCATE TABLE {LEGACY_ORACLE_SCHEMA}.VIA")
        # oracle_client.execute(f"TRUNCATE TABLE {LEGACY_ORACLE_SCHEMA}.DWTK_MELDUNGEN")
        # oracle_client.execute(f"TRUNCATE TABLE {LEGACY_ORACLE_SCHEMA}.DWH$TA_F_VVL_EREIGNISSE")
        # oracle_client.commit()

    def test_sof_ta_vvl_dwh_data_parity(bq_client, oracle_client):
        job_kennung = "TEST_JOB_1"
        eintrags_nr = "20231027"

        # 1. Run legacy job
        stdout_legacy, stderr_legacy, exit_code_legacy = run_legacy_ksh_script(job_kennung, eintrags_nr)
        assert exit_code_legacy == 0, f"Legacy job failed: {stderr_legacy}"

        # 2. Run migrated job
        try:
            bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_vertrag_control`('{job_kennung}', '{eintrags_nr}')").result()
        except Exception as e:
            pytest.fail(f"Migrated job failed: {e}")

        # 3. Extract and compare data
        legacy_df = get_oracle_data(oracle_client, "SOF$TA_VVL_DWH")
        migrated_df = get_bigquery_data(bq_client, "SOF_TA_VVL_DWH")

        assert len(legacy_df) > 0, "Legacy SOF$TA_VVL_DWH table is empty, check test data or legacy job."
        assert_frame_equal(legacy_df, migrated_df, check_dtype=False, check_like=True) # check_like for column order/names

    ```

#### Test Case 1.2: End-to-End Data Parity - `VIA`

*   **Purpose:** Verify that the data written to the `VIA` target table by the migrated BigQuery job is identical to the data written by the legacy job to its corresponding Oracle table.
*   **Setup:** Same as Test Case 1.1.
*   **Action:** Same as Test Case 1.1, but focusing on the `VIA` table.
*   **Pass/Fail Criterion:**
    *   The number of rows in both extracted datasets must be equal.
    *   After sorting both datasets by a common key, the content of all columns must be identical.
    *   The job execution for both legacy and migrated systems must complete successfully.

*   **Runnable Test Code (Python/Pytest with SQL Assertions):**

    ```python
    # ... (imports and helper functions from Test Case 1.1) ...

    def test_via_data_parity(bq_client, oracle_client):
        job_kennung = "TEST_JOB_1"
        eintrags_nr = "20231027"

        # 1. Run legacy job
        stdout_legacy, stderr_legacy, exit_code_legacy = run_legacy_ksh_script(job_kennung, eintrags_nr)
        assert exit_code_legacy == 0, f"Legacy job failed: {stderr_legacy}"

        # 2. Run migrated job
        try:
            bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_vertrag_control`('{job_kennung}', '{eintrags_nr}')").result()
        except Exception as e:
            pytest.fail(f"Migrated job failed: {e}")

        # 3. Extract and compare data
        legacy_df = get_oracle_data(oracle_client, "VIA")
        migrated_df = get_bigquery_data(bq_client, "VIA")

        assert len(legacy_df) > 0, "Legacy VIA table is empty, check test data or legacy job."
        assert_frame_equal(legacy_df, migrated_df, check_dtype=False, check_like=True)
    ```

#### Test Case 1.3: Processed Record Count Parity

*   **Purpose:** Verify that the count of processed records reported by the migrated BigQuery job matches the count reported by the legacy KornShell job.
*   **Setup:** Same as Test Case 1.1.
*   **Action:**
    1.  Execute the legacy `k_ausd_v_ta_vvl_dwh.ksh` script. Capture the `v_records` variable value from its output or temporary file.
    2.  Execute the migrated `r_ausd_vertrag_control` BigQuery stored procedure.
    3.  Query the `job_run_log` table for the `processed_records` value for the executed job.
*   **Pass/Fail Criterion:**
    *   The `v_records` value from the legacy job must be equal to the `processed_records` value in the `job_run_log` for the corresponding migrated job run.

*   **Runnable Test Code (Python/Pytest with SQL Assertions):**

    ```python
    import re
    # ... (imports and helper functions from Test Case 1.1) ...

    def test_processed_record_count_parity(bq_client, oracle_client):
        job_kennung = "TEST_JOB_2"
        eintrags_nr = "20231028"
        load_test_data(bq_client, oracle_client, "path/to/test_data_scenario_2.csv") # Use different data for distinct run

        # 1. Run legacy job and capture record count
        stdout_legacy, stderr_legacy, exit_code_legacy = run_legacy_ksh_script(job_kennung, eintrags_nr)
        assert exit_code_legacy == 0, f"Legacy job failed: {stderr_legacy}"

        # The legacy script uses `eval "v_records=`cat $tmpFile`"`.
        # We need to simulate or capture the content of $tmpFile.
        # For testing, we might need to modify the legacy script to print v_records,
        # or inspect the temporary file directly if accessible.
        # Assuming for this test, the script prints "v_records=XYZ" at the end.
        match = re.search(r"v_records=(\d+)", stdout_legacy)
        assert match, "Could not find 'v_records' in legacy script output."
        legacy_record_count = int(match.group(1))

        # 2. Run migrated job
        try:
            bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_vertrag_control`('{job_kennung}', '{eintrags_nr}')").result()
        except Exception as e:
            pytest.fail(f"Migrated job failed: {e}")

        # 3. Query migrated job_run_log for record count
        query = f"""
            SELECT processed_records
            FROM `{PROJECT_ID}.{DATASET_ID}.job_run_log`
            WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'
            ORDER BY start_timestamp DESC
            LIMIT 1
        """
        migrated_record_count_df = bq_client.query(query).to_dataframe()
        assert not migrated_record_count_df.empty, "No entry found in job_run_log for migrated job."
        migrated_record_count = migrated_record_count_df['processed_records'].iloc[0]

        assert legacy_record_count == migrated_record_count, \
            f"Record count mismatch: Legacy={legacy_record_count}, Migrated={migrated_record_count}"
    ```

---

### 2. Transformation Correctness Tests

These tests focus on specific aspects of the transformation logic, including parameter handling, job state management, and the core SQL transformations.

#### Test Case 2.1: Parameter Validation - Missing `p_JobKennung`

*   **Purpose:** Verify that the migrated job correctly handles missing `p_JobKennung` parameters, raising an error and logging it, similar to the legacy script's `pruefeParameterGesetzt` and `DWMSG_MeldeFehler`.
*   **Setup:** Ensure `job_error_log` is empty.
*   **Action:**
    1.  Attempt to execute the legacy `k_ausd_v_ta_vvl_dwh.ksh` script without the `-j` parameter.
    2.  Attempt to execute the migrated `r_ausd_vertrag_control` BigQuery stored procedure with `p_JobKennung` as `NULL` or an empty string.
    3.  Query `job_error_log` for the error entry.
*   **Pass/Fail Criterion:**
    *   Legacy script exits with a non-zero status (specifically, `ErrNr=193` or `192` as per script logic).
    *   Migrated stored procedure raises an `BQ.INVALID_ARGUMENT_TYPE` error.
    *   An entry exists in `your_bq_dataset_id.job_error_log` with `job_kennung` (potentially NULL/empty), `eintrags_nr`, and an error message indicating `p_JobKennung` is missing/invalid.

*   **Runnable Test Code (Python/Pytest with SQL Assertions):**

    ```python
    # ... (imports and helper functions) ...

    def test_missing_jobkennung_parameter(bq_client):
        eintrags_nr = "20231029_missing_jobkennung"

        # Clear error log for this specific test
        bq_client.query(f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log` WHERE eintrags_nr = '{eintrags_nr}'").result()

        # 1. Test legacy script behavior (expect failure)
        stdout_legacy, stderr_legacy, exit_code_legacy = run_legacy_ksh_script(None, eintrags_nr) # Pass None for missing param
        assert exit_code_legacy != 0, f"Legacy job unexpectedly succeeded without -j: {stdout_legacy}"
        assert "Notwendiges Argument fehlt" in stderr_legacy or "Parameter unbekannt" in stderr_legacy, \
            f"Legacy error message not as expected: {stderr_legacy}"

        # 2. Test migrated procedure behavior (expect failure)
        with pytest.raises(bigquery.exceptions.GoogleBadRequest) as excinfo:
            bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_vertrag_control`(NULL, '{eintrags_nr}')").result()
        assert "p_JobKennung cannot be NULL or empty" in str(excinfo.value), \
            f"Migrated procedure error message not as expected: {excinfo.value}"

        # 3. Verify error log entry
        query = f"""
            SELECT error_message, severity
            FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`
            WHERE eintrags_nr = '{eintrags_nr}'
            ORDER BY error_timestamp DESC
            LIMIT 1
        """
        error_log_df = bq_client.query(query).to_dataframe()
        assert not error_log_df.empty, "No error log entry found for missing p_JobKennung."
        assert "p_JobKennung cannot be NULL or empty" in error_log_df['error_message'].iloc[0]
        assert error_log_df['severity'].iloc[0] == 'ERROR'
    ```

#### Test Case 2.2: Parameter Validation - Missing `p_EintragsNr`

*   **Purpose:** Verify that the migrated job correctly handles missing `p_EintragsNr` parameters, raising an error and logging it.
*   **Setup:** Ensure `job_error_log` is empty.
*   **Action:**
    1.  Attempt to execute the legacy `k_ausd_v_ta_vvl_dwh.ksh` script without the `-f` parameter.
    2.  Attempt to execute the migrated `r_ausd_vertrag_control` BigQuery stored procedure with `p_EintragsNr` as `NULL` or an empty string.
    3.  Query `job_error_log` for the error entry.
*   **Pass/Fail Criterion:**
    *   Legacy script exits with a non-zero status.
    *   Migrated stored procedure raises an `BQ.INVALID_ARGUMENT_TYPE` error.
    *   An entry exists in `your_bq_dataset_id.job_error_log` with `job_kennung`, `eintrags_nr` (potentially NULL/empty), and an error message indicating `p_EintragsNr` is missing/invalid.

*   **Runnable Test Code (Python/Pytest with SQL Assertions):**

    ```python
    # ... (imports and helper functions) ...

    def test_missing_eintragsnr_parameter(bq_client):
        job_kennung = "TEST_JOB_3_missing_eintragsnr"

        # Clear error log for this specific test
        bq_client.query(f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log` WHERE job_kennung = '{job_kennung}'").result()

        # 1. Test legacy script behavior (expect failure)
        stdout_legacy, stderr_legacy, exit_code_legacy = run_legacy_ksh_script(job_kennung, None) # Pass None for missing param
        assert exit_code_legacy != 0, f"Legacy job unexpectedly succeeded without -f: {stdout_legacy}"
        assert "Notwendiges Argument fehlt" in stderr_legacy or "Parameter unbekannt" in stderr_legacy, \
            f"Legacy error message not as expected: {stderr_legacy}"

        # 2. Test migrated procedure behavior (expect failure)
        with pytest.raises(bigquery.exceptions.GoogleBadRequest) as excinfo:
            bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_vertrag_control`('{job_kennung}', NULL)").result()
        assert "p_EintragsNr cannot be NULL or empty" in str(excinfo.value), \
            f"Migrated procedure error message not as expected: {excinfo.value}"

        # 3. Verify error log entry
        query = f"""
            SELECT error_message, severity
            FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`
            WHERE job_kennung = '{job_kennung}'
            ORDER BY error_timestamp DESC
            LIMIT 1
        """
        error_log_df = bq_client.query(query).to_dataframe()
        assert not error_log_df.empty, "No error log entry found for missing p_EintragsNr."
        assert "p_EintragsNr cannot be NULL or empty" in error_log_df['error_message'].iloc[0]
        assert error_log_df['severity'].iloc[0] == 'ERROR'
    ```

#### Test Case 2.3: Job Registration - Initial Run

*   **Purpose:** Verify that the `register_job_start` procedure correctly marks a new job as active and creates an entry in `job_run_log`.
*   **Setup:** Ensure `job_table` and `job_run_log` are clean for the specific `job_kennung`/`eintrags_nr` pair.
*   **Action:**
    1.  Call `register_job_start` with a new `p_JobKennung` and `p_EintragsNr`.
    2.  Query `job_table` and `job_run_log`.
*   **Pass/Fail Criterion:**
    *   `job_table` contains an entry for the `job_kennung`/`eintrags_nr` with `is_active = TRUE`.
    *   `job_run_log` contains an entry for the `job_kennung`/`eintrags_nr` with `status = 'RUNNING'` and a valid `start_timestamp`.

*   **Runnable Test Code (Python/Pytest with SQL Assertions):**

    ```python
    # ... (imports and helper functions) ...

    def test_job_registration_initial_run(bq_client):
        job_kennung = "TEST_JOB_4_INITIAL"
        eintrags_nr = "20231030_01"

        # Clean up previous runs for this test case
        bq_client.query(f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.job_table` WHERE job_kennung = '{job_kennung}'").result()
        bq_client.query(f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.job_run_log` WHERE job_kennung = '{job_kennung}'").result()

        # Action: Call register_job_start
        bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.register_job_start`('{job_kennung}', '{eintrags_nr}')").result()

        # Assertions for job_table
        job_table_query = f"""
            SELECT is_active, last_update_timestamp
            FROM `{PROJECT_ID}.{DATASET_ID}.job_table`
            WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'
        """
        job_table_df = bq_client.query(job_table_query).to_dataframe()
        assert not job_table_df.empty, "No entry found in job_table after initial registration."
        assert job_table_df['is_active'].iloc[0] is True
        assert job_table_df['last_update_timestamp'].iloc[0] is not None

        # Assertions for job_run_log
        job_run_log_query = f"""
            SELECT status, start_timestamp
            FROM `{PROJECT_ID}.{DATASET_ID}.job_run_log`
            WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'
        """
        job_run_log_df = bq_client.query(job_run_log_query).to_dataframe()
        assert not job_run_log_df.empty, "No entry found in job_run_log after initial registration."
        assert job_run_log_df['status'].iloc[0] == 'RUNNING'
        assert job_run_log_df['start_timestamp'].iloc[0] is not None
    ```

#### Test Case 2.4: Job Registration - Deactivation of Old Active Job

*   **Purpose:** Verify that `register_job_start` correctly deactivates any previously active job for the same `job_kennung` before activating the new one. This mimics the legacy `starteSQLSkript` behavior.
*   **Setup:**
    1.  Insert an entry into `job_table` for `job_kennung='TEST_JOB_5_DEACTIVATE'`, `eintrags_nr='OLD_RUN'`, `is_active=TRUE`.
    2.  Ensure `job_run_log` is clean for the new `job_kennung`/`eintrags_nr` pair.
*   **Action:**
    1.  Call `register_job_start` with `job_kennung='TEST_JOB_5_DEACTIVATE'` and `eintrags_nr='NEW_RUN'`.
    2.  Query `job_table` for both `OLD_RUN` and `NEW_RUN`.
*   **Pass/Fail Criterion:**
    *   The `job_table` entry for `OLD_RUN` must have `is_active = FALSE`.
    *   The `job_table` entry for `NEW_RUN` must have `is_active = TRUE`.
    *   `job_run_log` contains an entry for `NEW_RUN` with `status = 'RUNNING'`.

*   **Runnable Test Code (Python/Pytest with SQL Assertions):**

    ```python
    # ... (imports and helper functions) ...

    def test_job_registration_deactivates_old_job(bq_client):
        job_kennung = "TEST_JOB_5_DEACTIVATE"
        old_eintrags_nr = "20231030_OLD"
        new_eintrags_nr = "20231030_NEW"

        # Clean up and set up old active job
        bq_client.query(f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.job_table` WHERE job_kennung = '{job_kennung}'").result()
        bq_client.query(f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.job_run_log` WHERE job_kennung = '{job_kennung}'").result()
        bq_client.query(f"""
            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.job_table` (job_kennung, eintrags_nr, is_active, last_update_timestamp)
            VALUES ('{job_kennung}', '{old_eintrags_nr}', TRUE, CURRENT_TIMESTAMP())
        """).result()

        # Action: Call register_job_start for the new run
        bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.register_job_start`('{job_kennung}', '{new_eintrags_nr}')").result()

        # Assertions for job_table
        job_table_query = f"""
            SELECT eintrags_nr, is_active
            FROM `{PROJECT_ID}.{DATASET_ID}.job_table`
            WHERE job_kennung = '{job_kennung}'
            ORDER BY eintrags_nr
        """
        job_table_df = bq_client.query(job_table_query).to_dataframe()

        assert len(job_table_df) == 2, "Expected two entries in job_table for this job_kennung."
        old_job_status = job_table_df[job_table_df['eintrags_nr'] == old_eintrags_nr]['is_active'].iloc[0]
        new_job_status = job_table_df[job_table_df['eintrags_nr'] == new_eintrags_nr]['is_active'].iloc[0]

        assert old_job_status is False, f"Old job '{old_eintrags_nr}' was not deactivated."
        assert new_job_status is True, f"New job '{new_eintrags_nr}' was not activated."

        # Assertions for job_run_log (new entry)
        job_run_log_query = f"""
            SELECT status
            FROM `{PROJECT_ID}.{DATASET_ID}.job_run_log`
            WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{new_eintrags_nr}'
        """
        job_run_log_df = bq_client.query(job_run_log_query).to_dataframe()
        assert not job_run_log_df.empty, "No entry found in job_run_log for new run."
        assert job_run_log_df['status'].iloc[0] == 'RUNNING'
    ```

#### Test Case 2.5: Core Transformation Logic - Specific Join/Filter (Placeholder)

*   **Purpose:** Validate that specific join conditions and filter clauses within `d_ausd_v_ta_vvl_dwh_proc` behave identically to `d_ausd_v_ta_vvl_dwh.sql`.
*   **Setup:**
    1.  Load specific test data into `DWTK_MELDUNGEN` and `DWH_TA_F_VVL_EREIGNISSE` designed to test a particular join condition (e.g., matching/non-matching IDs, NULL IDs) or filter (e.g., boundary values, specific date ranges).
    2.  Ensure target tables are empty.
*   **Action:**
    1.  Execute the legacy `k_ausd_v_ta_vvl_dwh.ksh` job.
    2.  Execute the migrated `r_ausd_vertrag_control` job.
    3.  Query the target tables (`SOF_TA_VVL_DWH`, `VIA`) and compare results.
*   **Pass/Fail Criterion:**
    *   The number of rows and the content of relevant columns in the target tables must be identical between legacy and migrated.
    *   **Specific Assertion Example (if `d_ausd_v_ta_vvl_dwh.sql` had `WHERE t1.status = 'ACTIVE'`):** Verify that only 'ACTIVE' records are processed.

*   **Note:** This test requires detailed knowledge of `d_ausd_v_ta_vvl_dwh.sql`. Without it, this remains a conceptual test.

#### Test Case 2.6: Core Transformation Logic - Data Type/NULL Handling (Placeholder)

*   **Purpose:** Validate that data type conversions, NULL handling, and specific column transformations (e.g., date formatting, string manipulation, arithmetic operations) within `d_ausd_v_ta_vvl_dwh_proc` are correct.
*   **Setup:**
    1.  Load test data into source tables including various data types, NULL values in critical columns, edge cases for transformations (e.g., empty strings, zero values, max/min dates).
    2.  Ensure target tables are empty.
*   **Action:**
    1.  Execute the legacy `k_ausd_v_ta_vvl_dwh.ksh` job.
    2.  Execute the migrated `r_ausd_vertrag_control` job.
    3.  Query the target tables (`SOF_TA_VVL_DWH`, `VIA`) and compare results, paying close attention to data types and NULL values.
*   **Pass/Fail Criterion:**
    *   All data types in target tables match the expected types.
    *   NULL values are handled consistently (e.g., propagated, defaulted, or excluded as per logic).
    *   Transformed column values are identical between legacy and migrated.

*   **Note:** This test requires detailed knowledge of `d_ausd_v_ta_vvl_dwh.sql`.

#### Test Case 2.7: `DWPA_UTIL_SKRIPT` Function Re-implementation (Placeholder)

*   **Purpose:** Verify that any functions or procedures from the legacy Oracle `DWPA_UTIL_SKRIPT` package, which have been re-implemented as BigQuery UDFs or helper procedures, produce identical results for given inputs.
*   **Setup:**
    1.  Identify specific functions from `DWPA_UTIL_SKRIPT` that are used in `d_ausd_v_ta_vvl_dwh.sql`.
    2.  Create a set of input values for these functions, including edge cases.
*   **Action:**
    1.  Execute the original Oracle `DWPA_UTIL_SKRIPT` functions with the test inputs and record outputs.
    2.  Execute the corresponding BigQuery UDFs/procedures with the same test inputs and record outputs.
*   **Pass/Fail Criterion:**
    *   The outputs from the Oracle functions and their BigQuery re-implementations must be identical for all test inputs.

*   **Runnable Test Code (Python/Pytest with SQL Assertions):**

    ```python
    # ... (imports and helper functions) ...

    def test_dwpa_util_skript_function_parity(bq_client, oracle_client):
        # This is a placeholder. Replace with actual function calls and expected values.
        # Example: Assuming a function `DWPA_UTIL_SKRIPT.FORMAT_DATE(input_date)`
        test_cases = [
            ("2023-01-01", "01.01.2023"),
            ("2023-12-31", "31.12.2023"),
            (None, None), # Test NULL handling
            # Add more test cases based on actual function logic
        ]

        for input_val, expected_output in test_cases:
            # 1. Call legacy Oracle function
            # query_oracle = f"SELECT {LEGACY_ORACLE_SCHEMA}.DWPA_UTIL_SKRIPT.FORMAT_DATE('{input_val}') FROM DUAL"
            # oracle_result = oracle_client.execute(query_oracle).fetchone()[0]
            # For now, assume a direct comparison if we know the expected output
            oracle_result = expected_output # Placeholder

            # 2. Call migrated BigQuery UDF/procedure
            bq_udf_name = f"`{PROJECT_ID}.{DATASET_ID}.DWPA_UTIL_SKRIPT_FORMAT_DATE_UDF`"
            bq_input_val = f"'{input_val}'" if input_val is not None else "NULL"
            query_bq = f"SELECT {bq_udf_name}({bq_input_val})"
            bq_result_df = bq_client.query(query_bq).to_dataframe()
            bq_result = bq_result_df.iloc[0, 0]

            assert oracle_result == bq_result, \
                f"DWPA_UTIL_SKRIPT_FORMAT_DATE_UDF mismatch for input '{input_val}': " \
                f"Oracle='{oracle_result}', BigQuery='{bq_result}'"
    ```

#### Test Case 2.8: Error Handling during Data Transformation

*   **Purpose:** Verify that if an error occurs within the `d_ausd_v_ta_vvl_dwh_proc` (e.g., due to bad data, constraint violation), the `r_ausd_vertrag_control` procedure correctly catches it, logs it, and updates the job status to 'FAILED'.
*   **Setup:**
    1.  Load test data into source tables that will intentionally cause an error in `d_ausd_v_ta_vvl_dwh_proc` (e.g., inserting a duplicate primary key if the target table has one, or data that violates a type conversion).
    2.  Ensure `job_error_log` and `job_run_log` are clean for the specific `job_kennung`/`eintrags_nr`.
*   **Action:**
    1.  Execute the migrated `r_ausd_vertrag_control` BigQuery stored procedure.
    2.  Query `job_error_log` and `job_run_log`.
*   **Pass/Fail Criterion:**
    *   The `r_ausd_vertrag_control` procedure raises an error to the caller.
    *   An entry exists in `your_bq_dataset_id.job_error_log` with `severity = 'CRITICAL'` and an appropriate error message.
    *   The `job_run_log` entry for the job shows `status = 'FAILED'` and `end_timestamp` is populated.
    *   The transaction within `d_ausd_v_ta_vvl_dwh_proc` is rolled back, meaning no partial data is committed to target tables.

*   **Runnable Test Code (Python/Pytest with SQL Assertions):**

    ```python
    # ... (imports and helper functions) ...

    def test_error_handling_during_transformation(bq_client):
        job_kennung = "TEST_JOB_6_ERROR"
        eintrags_nr = "20231031"

        # Clean up
        bq_client.query(f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log` WHERE job_kennung = '{job_kennung}'").result()
        bq_client.query(f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.job_run_log` WHERE job_kennung = '{job_kennung}'").result()
        bq_client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.SOF_TA_VVL_DWH`").result()

        # Load data that will cause an error (e.g., duplicate ID if SOF_TA_VVL_DWH has unique constraint)
        # For this example, let's assume d_ausd_v_ta_vvl_dwh_proc has a bug or a specific data pattern causes an error.
        # We'll simulate this by having d_ausd_v_ta_vvl_dwh_proc explicitly raise an error for a specific job_kennung.
        # In a real scenario, you'd load data that naturally triggers an error in the BQSQL.
        # For now, let's assume the d_ausd_v_ta_vvl_dwh_proc is modified for this test to:
        # IF p_JobKennung = 'TEST_JOB_6_ERROR' THEN RAISE 'Simulated transformation error'; END IF;
        # Or, load data that causes a real error, e.g., a string into an INT64 column.
        bq_client.query(f"""
            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.DWTK_MELDUNGEN` (id, data, load_timestamp)
            VALUES ('error_id', JSON '{{ "value": "not_an_int" }}', CURRENT_TIMESTAMP())
        """).result()
        bq_client.query(f"""
            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.DWH_TA_F_VVL_EREIGNISSE` (id, event_data, load_timestamp)
            VALUES ('error_id', JSON '{{ "event": "test" }}', CURRENT_TIMESTAMP())
        """).result()


        # Action: Execute migrated procedure (expect failure)
        with pytest.raises(bigquery.exceptions.GoogleBadRequest) as excinfo: # Or other specific BQ error type
            bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_vertrag_control`('{job_kennung}', '{eintrags_nr}')").result()
        assert "Simulated transformation error" in str(excinfo.value) or "invalid" in str(excinfo.value).lower(), \
            f"Migrated procedure error message not as expected: {excinfo.value}"

        # Verify error log entry
        error_log_query = f"""
            SELECT error_message, severity
            FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`
            WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'
            ORDER BY error_timestamp DESC
            LIMIT 1
        """
        error_log_df = bq_client.query(error_log_query).to_dataframe()
        assert not error_log_df.empty, "No error log entry found."
        assert error_log_df['severity'].iloc[0] == 'CRITICAL'
        assert "Simulated transformation error" in error_log_df['error_message'].iloc[0] or "invalid" in error_log_df['error_message'].iloc[0].lower()

        # Verify job_run_log status
        run_log_query = f"""
            SELECT status, end_timestamp
            FROM `{PROJECT_ID}.{DATASET_ID}.job_run_log`
            WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'
            ORDER BY start_timestamp DESC
            LIMIT 1
        """
        run_log_df = bq_client.query(run_log_query).to_dataframe()
        assert not run_log_df.empty, "No run log entry found."
        assert run_log_df['status'].iloc[0] == 'FAILED'
        assert run_log_df['end_timestamp'].iloc[0] is not None

        # Verify rollback (target tables should be empty)
        sof_count_query = f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.SOF_TA_VVL_DWH`"
        sof_count = bq_client.query(sof_count_query).to_dataframe().iloc[0,0]
        assert sof_count == 0, "SOF_TA_VVL_DWH should be empty due to transaction rollback."
    ```

---

### 3. External-System Replacements Tests

These tests confirm that the migration correctly replaces Oracle dependencies with BigQuery and that no unintended external system interactions occur.

#### Test Case 3.1: Source Table Reads (Implicit)

*   **Purpose:** Confirm that `d_ausd_v_ta_vvl_dwh_proc` correctly reads from BigQuery tables `DWTK_MELDUNGEN` and `DWH_TA_F_VVL_EREIGNISSE` as intended, replacing the Oracle reads.
*   **Setup:** Load distinct data into `DWTK_MELDUNGEN` and `DWH_TA_F_VVL_EREIGNISSE`.
*   **Action:** Execute `r_ausd_vertrag_control`.
*   **Pass/Fail Criterion:** This is implicitly covered by the "Output Parity" tests (1.1 and 1.2). If the output data matches, it confirms the source reads were successful. Additionally, BigQuery query logs can be inspected to confirm reads from the correct BigQuery tables.

#### Test Case 3.2: Target Table Writes (Implicit)

*   **Purpose:** Confirm that `d_ausd_v_ta_vvl_dwh_proc` correctly writes to BigQuery tables `SOF_TA_VVL_DWH` and `VIA`, replacing the Oracle writes.
*   **Setup:** Ensure target tables are empty.
*   **Action:** Execute `r_ausd_vertrag_control`.
*   **Pass/Fail Criterion:** This is implicitly covered by the "Output Parity" tests (1.1 and 1.2). If the output data matches, it confirms the target writes were successful. BigQuery query logs can also confirm writes to the correct BigQuery tables.

#### Test Case 3.3: Absence of Legacy External System Calls (e.g., Oracle SQL*Plus)

*   **Purpose:** Verify that the migrated BigQuery solution does not attempt to connect to or interact with the legacy Oracle database or any other external systems (SFTP, S3, etc.) that were not part of the migration scope.
*   **Setup:**
    1.  Configure BigQuery environment with no Oracle connection details.
    2.  (Optional but recommended) Monitor network traffic or BigQuery audit logs during execution.
*   **Action:**
    1.  Execute the migrated `r_ausd_vertrag_control` BigQuery stored procedure.
*   **Pass/Fail Criterion:**
    *   The BigQuery job completes successfully without any errors related to external database connections (e.g., Oracle connection failures).
    *   Audit logs or network monitoring confirm no outbound connections to Oracle or other un-migrated external systems.

*   **Runnable Test Code (Conceptual / Manual Verification):**

    ```python
    # This test is primarily conceptual and relies on environment configuration
    # and monitoring rather than direct code assertions.

    def test_no_legacy_oracle_connections(bq_client):
        job_kennung = "TEST_JOB_NO_ORACLE"
        eintrags_nr = "20231101"

        # Ensure BigQuery environment is configured WITHOUT any Oracle connection details
        # (e.g., no external connections defined, no JDBC drivers, etc.)

        # Action: Run the migrated job
        try:
            bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_vertrag_control`('{job_kennung}', '{eintrags_nr}')").result()
        except Exception as e:
            # If an unexpected external connection attempt fails, it might raise an error here.
            # This would be a FAIL.
            pytest.fail(f"Migrated job failed, possibly due to unexpected external connection: {e}")

        # Pass criterion: Job completes successfully without external connection errors.
        # Further verification:
        # - Manually inspect BigQuery audit logs for any external connection attempts.
        # - If possible, run network monitoring tools during execution to detect outbound traffic.
        print("Migrated BigQuery job completed successfully without apparent legacy Oracle connections.")
        assert True # Placeholder for successful execution and manual verification
    ```

---

### 4. Data Quality / Row Count / Schema Assertions

These tests validate the structural integrity and basic quality of the data in the target BigQuery tables.

#### Test Case 4.1: Target Table Schema Validation

*   **Purpose:** Verify that the schema (column names, data types, nullability) of the migrated `SOF_TA_VVL_DWH` and `VIA` tables matches the expected schema derived from the legacy Oracle tables.
*   **Setup:** None.
*   **Action:**
    1.  Retrieve the schema of the legacy Oracle `SOF$TA_VVL_DWH` and `VIA` tables.
    2.  Retrieve the schema of the BigQuery `your_bq_dataset_id.SOF_TA_VVL_DWH` and `your_bq_dataset_id.VIA` tables.
*   **Pass/Fail Criterion:**
    *   All column names, their corresponding BigQuery data types, and nullability properties must match the defined target schema.

*   **Runnable Test Code (Python/Pytest with SQL Assertions):**

    ```python
    # ... (imports) ...

    def get_oracle_schema(oracle_client, table_name):
        """Fetches schema details from Oracle."""
        query = f"""
            SELECT COLUMN_NAME, DATA_TYPE, NULLABLE
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = UPPER('{LEGACY_ORACLE_SCHEMA}') AND TABLE_NAME = UPPER('{table_name}')
            ORDER BY COLUMN_ID
        """
        df = pd.read_sql(query, oracle_client)
        return df

    def get_bigquery_schema(bq_client, table_name):
        """Fetches schema details from BigQuery."""
        table_ref = bq_client.dataset(DATASET_ID).table(table_name)
        table = bq_client.get_table(table_ref)
        schema_list = []
        for field in table.schema:
            schema_list.append({
                'COLUMN_NAME': field.name.upper(), # Oracle usually uppercase
                'DATA_TYPE': field.field_type,
                'NULLABLE': 'Y' if field.mode == 'NULLABLE' else 'N'
            })
        return pd.DataFrame(schema_list)

    def test_target_table_schema_validation(bq_client, oracle_client):
        # Define expected schema mappings (Oracle_Type -> BQ_Type)
        type_mapping = {
            'VARCHAR2': 'STRING',
            'NUMBER': 'INT64', # Or FLOAT64, BIGNUMERIC depending on precision
            'DATE': 'TIMESTAMP', # Or DATE, DATETIME
            # ... add more mappings
        }

        # For SOF_TA_VVL_DWH
        legacy_sof_schema = get_oracle_schema(oracle_client, "SOF$TA_VVL_DWH")
        migrated_sof_schema = get_bigquery_schema(bq_client, "SOF_TA_VVL_DWH")

        # Apply type mapping to legacy schema for comparison
        legacy_sof_schema['DATA_TYPE_BQ'] = legacy_sof_schema['DATA_TYPE'].apply(lambda x: type_mapping.get(x, x))
        # Compare column names, BQ data types, and nullability
        assert_frame_equal(
            legacy_sof_schema[['COLUMN_NAME', 'DATA_TYPE_BQ', 'NULLABLE']],
            migrated_sof_schema[['COLUMN_NAME', 'DATA_TYPE', 'NULLABLE']],
            check_dtype=False, check_like=True
        )

        # Repeat for VIA table
        legacy_via_schema = get_oracle_schema(oracle_client, "VIA")
        migrated_via_schema = get_bigquery_schema(bq_client, "VIA")
        legacy_via_schema['DATA_TYPE_BQ'] = legacy_via_schema['DATA_TYPE'].apply(lambda x: type_mapping.get(x, x))
        assert_frame_equal(
            legacy_via_schema[['COLUMN_NAME', 'DATA_TYPE_BQ', 'NULLABLE']],
            migrated_via_schema[['COLUMN_NAME', 'DATA_TYPE', 'NULLABLE']],
            check_dtype=False, check_like=True
        )
    ```

#### Test Case 4.2: Target Table Row Count Validation

*   **Purpose:** Verify that the total number of rows in the target tables after a successful run matches the expected count.
*   **Setup:** Load a known dataset into source tables.
*   **Action:**
    1.  Execute the migrated `r_ausd_vertrag_control` job.
    2.  Count rows in `your_bq_dataset_id.SOF_TA_VVL_DWH` and `your_bq_dataset_id.VIA`.
*   **Pass/Fail Criterion:**
    *   The row counts in `SOF_TA_VVL_DWH` and `VIA` must match the expected counts based on the input data and transformation logic. (This is also covered by Test Case 1.3, but can be a standalone check for specific scenarios).

*   **Runnable Test Code (Python/Pytest with SQL Assertions):**

    ```python
    # ... (imports and helper functions) ...

    def test_target_table_row_count_validation(bq_client, oracle_client):
        job_kennung = "TEST_JOB_ROW_COUNT"
        eintrags_nr = "20231102"
        load_test_data(bq_client, oracle_client, "path/to/test_data_for_row_counts.csv") # Load specific data

        # Expected counts based on the specific test_data_for_row_counts.csv and transformation logic
        expected_sof_rows = 100
        expected_via_rows = 50

        # Run the migrated job
        try:
            bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_vertrag_control`('{job_kennung}', '{eintrags_nr}')").result()
        except Exception as e:
            pytest.fail(f"Migrated job failed: {e}")

        # Count rows in target tables
        sof_count_query = f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.SOF_TA_VVL_DWH`"
        actual_sof_rows = bq_client.query(sof_count_query).to_dataframe().iloc[0,0]

        via_count_query = f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.VIA`"
        actual_via_rows = bq_client.query(via_count_query).to_dataframe().iloc[0,0]

        assert actual_sof_rows == expected_sof_rows, \
            f"SOF_TA_VVL_DWH row count mismatch: Expected {expected_sof_rows}, Got {actual_sof_rows}"
        assert actual_via_rows == expected_via_rows, \
            f"VIA row count mismatch: Expected {expected_via_rows}, Got {actual_via_rows}"
    ```

#### Test Case 4.3: Data Quality - No Unexpected NULLs in Key Fields

*   **Purpose:** Verify that critical columns (e.g., primary keys, foreign keys, mandatory business identifiers) in the target tables do not contain unexpected NULL values.
*   **Setup:** Load test data, including some with potential NULLs in source tables that should be handled (e.g., filtered out, defaulted).
*   **Action:**
    1.  Execute the migrated `r_ausd_vertrag_control` job.
    2.  Query target tables for NULLs in key fields.
*   **Pass/Fail Criterion:**
    *   Count of NULLs in specified critical columns (e.g., `id` in `SOF_TA_VVL_DWH`) must be zero, or match the expected count if NULLs are intentionally allowed/generated.

*   **Runnable Test Code (Python/Pytest with SQL Assertions):**

    ```python
    # ... (imports and helper functions) ...

    def test_no_unexpected_nulls_in_key_fields(bq_client, oracle_client):
        job_kennung = "TEST_JOB_NULL_CHECK"
        eintrags_nr = "20231103"
        # Load data where 'id' is always present in source, so it should be present in target
        load_test_data(bq_client, oracle_client, "path/to/test_data_no_null_keys.csv")

        # Run the migrated job
        try:
            bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_vertrag_control`('{job_kennung}', '{eintrags_nr}')").result()
        except Exception as e:
            pytest.fail(f"Migrated job failed: {e}")

        # Check for NULLs in 'id' column of SOF_TA_VVL_DWH
        sof_null_id_query = f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.SOF_TA_VVL_DWH` WHERE id IS NULL"
        null_ids_sof = bq_client.query(sof_null_id_query).to_dataframe().iloc[0,0]
        assert null_ids_sof == 0, f"Found {null_ids_sof} unexpected NULLs in SOF_TA_VVL_DWH.id"

        # Check for NULLs in 'id' column of VIA
        via_null_id_query = f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.VIA` WHERE id IS NULL"
        null_ids_via = bq_client.query(via_null_id_query).to_dataframe().iloc[0,0]
        assert null_ids_via == 0, f"Found {null_ids_via} unexpected NULLs in VIA.id"
    ```

#### Test Case 4.4: Data Quality - No Duplicate Primary Keys

*   **Purpose:** Verify that target tables with defined primary keys (or unique constraints) do not contain duplicate entries for those keys.
*   **Setup:** Load test data, including some that might *attempt* to create duplicates if the transformation logic is flawed.
*   **Action:**
    1.  Execute the migrated `r_ausd_vertrag_control` job.
    2.  Query target tables to identify duplicate primary keys.
*   **Pass/Fail Criterion:**
    *   Count of duplicate primary keys in `SOF_TA_VVL_DWH` (assuming `id` is PK) and `VIA` (assuming `id` is PK) must be zero.

*   **Runnable Test Code (Python/Pytest with SQL Assertions):**

    ```python
    # ... (imports and helper functions) ...

    def test_no_duplicate_primary_keys(bq_client, oracle_client):
        job_kennung = "TEST_JOB_DUPLICATE_CHECK"
        eintrags_nr = "20231104"
        # Load data that, if processed incorrectly, could lead to duplicates
        load_test_data(bq_client, oracle_client, "path/to/test_data_potential_duplicates.csv")

        # Run the migrated job
        try:
            bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_vertrag_control`('{job_kennung}', '{eintrags_nr}')").result()
        except Exception as e:
            pytest.fail(f"Migrated job failed: {e}")

        # Check for duplicate 'id' in SOF_TA_VVL_DWH
        sof_duplicate_id_query = f"""
            SELECT id, COUNT(*)
            FROM `{PROJECT_ID}.{DATASET_ID}.SOF_TA_VVL_DWH`
            GROUP BY id
            HAVING COUNT(*) > 1
        """
        duplicate_ids_sof_df = bq_client.query(sof_duplicate_id_query).to_dataframe()
        assert duplicate_ids_sof_df.empty, \
            f"Found unexpected duplicate IDs in SOF_TA_VVL_DWH: {duplicate_ids_sof_df['id'].tolist()}"

        # Check for duplicate 'id' in VIA
        via_duplicate_id_query = f"""
            SELECT id, COUNT(*)
            FROM `{PROJECT_ID}.{DATASET_ID}.VIA`
            GROUP BY id
            HAVING COUNT(*) > 1
        """
        duplicate_ids_via_df = bq_client.query(via_duplicate_id_query).to_dataframe()
        assert duplicate_ids_via_df.empty, \
            f"Found unexpected duplicate IDs in VIA: {duplicate_ids_via_df['id'].tolist()}"
    ```

#### Test Case 4.5: Job Run Log Assertions (Success)

*   **Purpose:** Verify that successful job runs are correctly logged in `job_run_log` with accurate status and metadata.
*   **Setup:** None.
*   **Action:**
    1.  Execute the migrated `r_ausd_vertrag_control` job with valid parameters and data that leads to success.
    2.  Query `job_run_log` for the specific job entry.
*   **Pass/Fail Criterion:**
    *   An entry exists for the `job_kennung`/`eintrags_nr` in `job_run_log`.
    *   `status` column is 'SUCCESS'.
    *   `start_timestamp`, `end_timestamp`, and `processed_records` are populated correctly.

*   **Runnable Test Code (Python/Pytest with SQL Assertions):**

    ```python
    # ... (imports and helper functions) ...

    def test_job_run_log_success_entry(bq_client, oracle_client):
        job_kennung = "TEST_JOB_LOG_SUCCESS"
        eintrags_nr = "20231105"
        load_test_data(bq_client, oracle_client, "path/to/simple_success_data.csv")

        # Clean up previous runs
        bq_client.query(f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.job_run_log` WHERE job_kennung = '{job_kennung}'").result()

        # Run the migrated job
        try:
            bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_vertrag_control`('{job_kennung}', '{eintrags_nr}')").result()
        except Exception as e:
            pytest.fail(f"Migrated job failed: {e}")

        # Query job_run_log
        run_log_query = f"""
            SELECT status, start_timestamp, end_timestamp, processed_records
            FROM `{PROJECT_ID}.{DATASET_ID}.job_run_log`
            WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'
            ORDER BY start_timestamp DESC
            LIMIT 1
        """
        run_log_df = bq_client.query(run_log_query).to_dataframe()

        assert not run_log_df.empty, "No job_run_log entry found for successful run."
        assert run_log_df['status'].iloc[0] == 'SUCCESS'
        assert run_log_df['start_timestamp'].iloc[0] is not None
        assert run_log_df['end_timestamp'].iloc[0] is not None
        assert run_log_df['processed_records'].iloc[0] > 0 # Assuming some records were processed
    ```

#### Test Case 4.6: Job Error Log Assertions (Failure)

*   **Purpose:** Verify that failed job runs are correctly logged in `job_error_log` with accurate error messages and severity.
*   **Setup:** Load test data designed to cause a failure (e.g., as in Test Case 2.8).
*   **Action:**
    1.  Execute the migrated `r_ausd_vertrag_control` job with parameters/data that leads to failure.
    2.  Query `job_error_log` for the specific job entry.
*   **Pass/Fail Criterion:**
    *   An entry exists for the `job_kennung`/`eintrags_nr` in `job_error_log`.
    *   `severity` column is 'CRITICAL' (or 'ERROR' for parameter validation).
    *   `error_message` contains relevant details about the failure.
    *   `error_timestamp` is populated.

*   **Runnable Test Code (Python/Pytest with SQL Assertions):**

    ```python
    # ... (imports and helper functions) ...

    def test_job_error_log_failure_entry(bq_client, oracle_client):
        job_kennung = "TEST_JOB_LOG_FAILURE"
        eintrags_nr = "20231106"
        # Load data that will cause an error (e.g., as in Test Case 2.8)
        bq_client.query(f"""
            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.DWTK_MELDUNGEN` (id, data, load_timestamp)
            VALUES ('fail_id', JSON '{{ "value": "trigger_error" }}', CURRENT_TIMESTAMP())
        """).result()
        bq_client.query(f"""
            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.DWH_TA_F_VVL_EREIGNISSE` (id, event_data, load_timestamp)
            VALUES ('fail_id', JSON '{{ "event": "test" }}', CURRENT_TIMESTAMP())
        """).result()

        # Clean up previous runs
        bq_client.query(f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log` WHERE job_kennung = '{job_kennung}'").result()
        bq_client.query(f"DELETE FROM `{PROJECT_ID}.{DATASET_ID}.job_run_log` WHERE job_kennung = '{job_kennung}'").result()

        # Run the migrated job (expect failure)
        with pytest.raises(bigquery.exceptions.GoogleBadRequest): # Or other specific BQ error type
            bq_client.query(f"CALL `{PROJECT_ID}.{DATASET_ID}.r_ausd_vertrag_control`('{job_kennung}', '{eintrags_nr}')").result()

        # Query job_error_log
        error_log_query = f"""
            SELECT error_message, severity, error_code
            FROM `{PROJECT_ID}.{DATASET_ID}.job_error_log`
            WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'
            ORDER BY error_timestamp DESC
            LIMIT 1
        """
        error_log_df = bq_client.query(error_log_query).to_dataframe()

        assert not error_log_df.empty, "No job_error_log entry found for failed run."
        assert error_log_df['severity'].iloc[0] == 'CRITICAL'
        assert "trigger_error" in error_log_df['error_message'].iloc[0] # Or specific error message
        assert error_log_df['error_code'].iloc[0] is not None # Assuming error_code is captured
    ```