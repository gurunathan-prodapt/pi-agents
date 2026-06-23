As a senior data-migration QA engineer, I've analyzed the migration design and the generated BigQuery code for `k_ausd_v_ta_apn_ve.ksh`. The migration involves translating KornShell orchestration and an external SQL script (`d_ausd_v_ta_apn_ve.sql`) into BigQuery Stored Procedures and audit/logging tables.

The core challenge is that `d_ausd_v_ta_apn_ve.sql`'s content is unknown and its BigQuery counterpart (`project.dataset.d_ausd_v_ta_apn_ve`) is a placeholder. Therefore, tests for transformation correctness within `d_ausd_v_ta_apn_ve` will be conceptual until its actual logic is migrated. For the purpose of testing the *orchestration* and *job control*, I will assume `project.dataset.d_ausd_v_ta_apn_ve` is implemented to perform some predictable data manipulation and return a non-zero `records_processed` value.

The tests below cover the orchestration, error handling, job control, and data flow aspects of the migration.

---

## Migration Validation Tests for `k_ausd_v_ta_apn_ve.ksh`

**Assumptions for Testing:**
*   All BigQuery tables (`error_log`, `job_table`, `job_run_audit`, `target_table_for_ta_apn_ve`) and stored procedures (`d_ausd_v_ta_apn_ve`, `starte_sql_skript`, `r_ausd_vertrag_control`) have been successfully deployed to `project.dataset`.
*   For tests involving `d_ausd_v_ta_apn_ve`, we assume a temporary implementation that inserts a known number of rows (e.g., 5 rows) into `target_table_for_ta_apn_ve` and returns this count via `records_processed`. This allows us to test the `records_processed` propagation.
*   A testing framework (e.g., Pytest with `google-cloud-bigquery` client) is used to execute BigQuery SQL and stored procedures, and assert results.

---

### Test Case 1: Successful End-to-End Execution (Happy Path)

*   **Purpose:** Verify that the entire migrated job runs successfully from start to finish, processing data, updating job status, and logging audit information correctly when all conditions are met.
*   **Setup:**
    1.  Ensure `project.dataset.job_table`, `project.dataset.job_run_audit`, `project.dataset.error_log`, and `project.dataset.target_table_for_ta_apn_ve` are empty.
    2.  Ensure `project.dataset.d_ausd_v_ta_apn_ve` is implemented to insert a predictable number of rows (e.g., 5) into `target_table_for_ta_apn_ve` and return that count.
*   **Action:**
    Execute the main control procedure with valid parameters.
    ```python
    # pytest-style test code
    from google.cloud import bigquery

    client = bigquery.Client()
    project_id = "project"
    dataset_id = "dataset"
    job_kennung = "TEST_JOB_001"
    eintrags_nr = "ENTRY_001"

    # Clean up previous test data
    client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.job_table").result()
    client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.job_run_audit").result()
    client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.error_log").result()
    client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.target_table_for_ta_apn_ve").result()

    # Execute the main stored procedure
    query = f"""
    CALL {project_id}.{dataset_id}.r_ausd_vertrag_control(
        p_JobKennung => '{job_kennung}',
        p_EintragsNr => '{eintrags_nr}'
    );
    """
    client.query(query).result()
    ```
*   **Pass/Fail Criterion:**
    1.  `project.dataset.job_table` contains one entry for `(job_kennung, eintrags_nr)` with `status = 'COMPLETED'`, `start_timestamp` and `end_timestamp` populated.
    2.  `project.dataset.job_run_audit` contains one entry for `(job_kennung, eintrags_nr)` with `status = 'COMPLETED'`, `records_processed` matching the expected count from `d_ausd_v_ta_apn_ve` (e.g., 5), and `start_timestamp`/`end_timestamp` populated.
    3.  `project.dataset.error_log` is empty.
    4.  `project.dataset.target_table_for_ta_apn_ve` contains the expected number of rows (e.g., 5) inserted by `d_ausd_v_ta_apn_ve` for the given `p_EintragsNr` and `p_JobKennung`.

    ```python
    # pytest-style assertions
    # Assert job_table state
    job_table_rows = list(client.query(f"SELECT status, start_timestamp IS NOT NULL, end_timestamp IS NOT NULL FROM {project_id}.{dataset_id}.job_table WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'").result())
    assert len(job_table_rows) == 1
    assert job_table_rows[0][0] == 'COMPLETED'
    assert job_table_rows[0][1] is True  # start_timestamp is not NULL
    assert job_table_rows[0][2] is True  # end_timestamp is not NULL

    # Assert job_run_audit state
    audit_rows = list(client.query(f"SELECT status, records_processed, start_timestamp IS NOT NULL, end_timestamp IS NOT NULL FROM {project_id}.{dataset_id}.job_run_audit WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'").result())
    assert len(audit_rows) == 1
    assert audit_rows[0][0] == 'COMPLETED'
    assert audit_rows[0][1] == 5  # Assuming d_ausd_v_ta_apn_ve inserts 5 rows
    assert audit_rows[0][2] is True
    assert audit_rows[0][3] is True

    # Assert error_log is empty
    error_log_count = client.query(f"SELECT COUNT(*) FROM {project_id}.{dataset_id}.error_log").result().total_rows
    assert error_log_count == 0

    # Assert target_table_for_ta_apn_ve content
    target_table_count = client.query(f"SELECT COUNT(*) FROM {project_id}.{dataset_id}.target_table_for_ta_apn_ve WHERE eintrags_nr = '{eintrags_nr}' AND job_kennung = '{job_kennung}'").result().total_rows
    assert target_table_count == 5
    ```

---

### Test Case 2: Parameter Validation - Missing `p_JobKennung`

*   **Purpose:** Verify the main procedure correctly handles the absence of the required `p_JobKennung` parameter, logging an error and terminating.
*   **Setup:**
    1.  Ensure `project.dataset.job_table`, `project.dataset.job_run_audit`, `project.dataset.error_log`, and `project.dataset.target_table_for_ta_apn_ve` are empty.
*   **Action:**
    Execute the main control procedure with `p_JobKennung` as `NULL` or an empty string.
    ```python
    # pytest-style test code
    from google.cloud import bigquery
    import pytest

    client = bigquery.Client()
    project_id = "project"
    dataset_id = "dataset"
    eintrags_nr = "ENTRY_002"

    # Clean up previous test data
    client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.job_table").result()
    client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.job_run_audit").result()
    client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.error_log").result()
    client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.target_table_for_ta_apn_ve").result()

    # Execute the main stored procedure with missing p_JobKennung
    query = f"""
    CALL {project_id}.{dataset_id}.r_ausd_vertrag_control(
        p_JobKennung => NULL, -- Or ''
        p_EintragsNr => '{eintrags_nr}'
    );
    """
    with pytest.raises(Exception) as excinfo: # Expecting an error to be raised
        client.query(query).result()
    assert "FEHLER: Notwendiges Argument fehlt - Jobkennung" in str(excinfo.value)
    ```
*   **Pass/Fail Criterion:**
    1.  The `r_ausd_vertrag_control` procedure terminates with an error.
    2.  `project.dataset.error_log` contains one entry with `error_code = 193`, `error_message` indicating "Notwendiges Argument fehlt - Jobkennung", and `severity = 'E'`.
    3.  `project.dataset.job_run_audit` contains one entry for the attempted run with `status = 'FAILED'` and `error_message` matching the raised error.
    4.  `project.dataset.job_table` remains empty (no job was started).
    5.  `project.dataset.target_table_for_ta_apn_ve` remains empty.

    ```python
    # pytest-style assertions
    # Assert error_log state
    error_log_rows = list(client.query(f"SELECT error_code, error_message, error_arg, severity FROM {project_id}.{dataset_id}.error_log").result())
    assert len(error_log_rows) == 1
    assert error_log_rows[0][0] == 193
    assert "Notwendiges Argument fehlt - Jobkennung" in error_log_rows[0][1]
    assert error_log_rows[0][2] == 'Jobkennung'
    assert error_log_rows[0][3] == 'E'

    # Assert job_run_audit state
    audit_rows = list(client.query(f"SELECT status, error_message FROM {project_id}.{dataset_id}.job_run_audit WHERE eintrags_nr = '{eintrags_nr}'").result())
    assert len(audit_rows) == 1
    assert audit_rows[0][0] == 'FAILED'
    assert "FEHLER: Notwendiges Argument fehlt - Jobkennung" in audit_rows[0][1]

    # Assert job_table is empty
    job_table_count = client.query(f"SELECT COUNT(*) FROM {project_id}.{dataset_id}.job_table").result().total_rows
    assert job_table_count == 0

    # Assert target_table_for_ta_apn_ve is empty
    target_table_count = client.query(f"SELECT COUNT(*) FROM {project_id}.{dataset_id}.target_table_for_ta_apn_ve").result().total_rows
    assert target_table_count == 0
    ```

---

### Test Case 3: Job Control - Ignore Already Active Job

*   **Purpose:** Verify that `starte_sql_skript` correctly identifies an already active job (same `p_JobKennung` and `p_EintragsNr`) and exits without processing, logging an 'IGNORED' status.
*   **Setup:**
    1.  Ensure `project.dataset.job_table`, `project.dataset.job_run_audit`, `project.dataset.error_log`, and `project.dataset.target_table_for_ta_apn_ve` are empty.
    2.  Pre-populate `project.dataset.job_table` with an 'ACTIVE' entry for `(job_kennung, eintrags_nr)`.
*   **Action:**
    Execute the main control procedure with the same `p_JobKennung` and `p_EintragsNr` as the pre-existing active job.
    ```python
    # pytest-style test code
    from google.cloud import bigquery

    client = bigquery.Client()
    project_id = "project"
    dataset_id = "dataset"
    job_kennung = "TEST_JOB_003"
    eintrags_nr = "ENTRY_003"

    # Clean up previous test data
    client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.job_table").result()
    client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.job_run_audit").result()
    client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.error_log").result()
    client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.target_table_for_ta_apn_ve").result()

    # Pre-populate job_table with an active job
    client.query(f"""
    INSERT INTO {project_id}.{dataset_id}.job_table (job_kennung, eintrags_nr, status, start_timestamp, last_updated)
    VALUES ('{job_kennung}', '{eintrags_nr}', 'ACTIVE', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
    """).result()

    # Execute the main stored procedure
    query = f"""
    CALL {project_id}.{dataset_id}.r_ausd_vertrag_control(
        p_JobKennung => '{job_kennung}',
        p_EintragsNr => '{eintrags_nr}'
    );
    """
    client.query(query).result()
    ```
*   **Pass/Fail Criterion:**
    1.  `project.dataset.job_table` still contains one entry for `(job_kennung, eintrags_nr)` with `status = 'ACTIVE'`. Its `start_timestamp` should be the original one, and `last_updated` might be updated or not depending on exact implementation (but status should not change to 'RUNNING' or 'COMPLETED').
    2.  `project.dataset.job_run_audit` contains one entry for `(job_kennung, eintrags_nr)` with `status = 'IGNORED'` and an appropriate `error_message` (e.g., "Job already active, ignored execution."). `records_processed` should be 0.
    3.  `project.dataset.error_log` is empty.
    4.  `project.dataset.target_table_for_ta_apn_ve` remains empty (no data processing occurred).

    ```python
    # pytest-style assertions
    # Assert job_table state (status remains ACTIVE)
    job_table_rows = list(client.query(f"SELECT status FROM {project_id}.{dataset_id}.job_table WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'").result())
    assert len(job_table_rows) == 1
    assert job_table_rows[0][0] == 'ACTIVE'

    # Assert job_run_audit state
    audit_rows = list(client.query(f"SELECT status, records_processed, error_message FROM {project_id}.{dataset_id}.job_run_audit WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'").result())
    assert len(audit_rows) == 1
    assert audit_rows[0][0] == 'IGNORED'
    assert audit_rows[0][1] == 0
    assert "Job already active, ignored execution." in audit_rows[0][2]

    # Assert error_log is empty
    error_log_count = client.query(f"SELECT COUNT(*) FROM {project_id}.{dataset_id}.error_log").result().total_rows
    assert error_log_count == 0

    # Assert target_table_for_ta_apn_ve is empty
    target_table_count = client.query(f"SELECT COUNT(*) FROM {project_id}.{dataset_id}.target_table_for_ta_apn_ve").result().total_rows
    assert target_table_count == 0
    ```

---

### Test Case 4: Job Control - Deactivate Older Active Jobs

*   **Purpose:** Verify that `starte_sql_skript` correctly deactivates other active jobs for the same `p_JobKennung` but different `p_EintragsNr` values, as per the design.
*   **Setup:**
    1.  Ensure `project.dataset.job_table`, `project.dataset.job_run_audit`, `project.dataset.error_log`, and `project.dataset.target_table_for_ta_apn_ve` are empty.
    2.  Pre-populate `project.dataset.job_table` with multiple 'ACTIVE' entries for the same `p_JobKennung` but different `p_EintragsNr` values.
*   **Action:**
    Execute the main control procedure with a *new* `p_EintragsNr` for the existing `p_JobKennung`.
    ```python
    # pytest-style test code
    from google.cloud import bigquery

    client = bigquery.Client()
    project_id = "project"
    dataset_id = "dataset"
    job_kennung = "TEST_JOB_004"
    old_eintrags_nr_1 = "OLD_ENTRY_001"
    old_eintrags_nr_2 = "OLD_ENTRY_002"
    new_eintrags_nr = "NEW_ENTRY_003"

    # Clean up previous test data
    client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.job_table").result()
    client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.job_run_audit").result()
    client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.error_log").result()
    client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.target_table_for_ta_apn_ve").result()

    # Pre-populate job_table with old active jobs
    client.query(f"""
    INSERT INTO {project_id}.{dataset_id}.job_table (job_kennung, eintrags_nr, status, start_timestamp, last_updated)
    VALUES
        ('{job_kennung}', '{old_eintrags_nr_1}', 'ACTIVE', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)),
        ('{job_kennung}', '{old_eintrags_nr_2}', 'ACTIVE', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE));
    """).result()

    # Execute the main stored procedure with a new EintragsNr
    query = f"""
    CALL {project_id}.{dataset_id}.r_ausd_vertrag_control(
        p_JobKennung => '{job_kennung}',
        p_EintragsNr => '{new_eintrags_nr}'
    );
    """
    client.query(query).result()
    ```
*   **Pass/Fail Criterion:**
    1.  `project.dataset.job_table` shows `old_eintrags_nr_1` and `old_eintrags_nr_2` entries with `status = 'INACTIVE'` and `end_timestamp` populated.
    2.  `project.dataset.job_table` contains a new entry for `(job_kennung, new_eintrags_nr)` with `status = 'COMPLETED'`.
    3.  `project.dataset.job_run_audit` contains one entry for `(job_kennung, new_eintrags_nr)` with `status = 'COMPLETED'` and `records_processed` matching the expected count.
    4.  `project.dataset.error_log` is empty.
    5.  `project.dataset.target_table_for_ta_apn_ve` contains the expected number of rows for `(job_kennung, new_eintrags_nr)`.

    ```python
    # pytest-style assertions
    # Assert old jobs are INACTIVE
    inactive_jobs = list(client.query(f"SELECT eintrags_nr, status FROM {project_id}.{dataset_id}.job_table WHERE job_kennung = '{job_kennung}' AND eintrags_nr IN ('{old_eintrags_nr_1}', '{old_eintrags_nr_2}')").result())
    assert len(inactive_jobs) == 2
    for row in inactive_jobs:
        assert row[1] == 'INACTIVE'

    # Assert new job is COMPLETED
    new_job_status = list(client.query(f"SELECT status FROM {project_id}.{dataset_id}.job_table WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{new_eintrags_nr}'").result())
    assert len(new_job_status) == 1
    assert new_job_status[0][0] == 'COMPLETED'

    # Assert job_run_audit for new job
    audit_rows = list(client.query(f"SELECT status, records_processed FROM {project_id}.{dataset_id}.job_run_audit WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{new_eintrags_nr}'").result())
    assert len(audit_rows) == 1
    assert audit_rows[0][0] == 'COMPLETED'
    assert audit_rows[0][1] == 5 # Assuming d_ausd_v_ta_apn_ve inserts 5 rows

    # Assert target_table_for_ta_apn_ve content
    target_table_count = client.query(f"SELECT COUNT(*) FROM {project_id}.{dataset_id}.target_table_for_ta_apn_ve WHERE eintrags_nr = '{new_eintrags_nr}' AND job_kennung = '{job_kennung}'").result().total_rows
    assert target_table_count == 5
    ```

---

### Test Case 5: Error During Core SQL Processing (`d_ausd_v_ta_apn_ve`)

*   **Purpose:** Verify robust error handling and logging when the underlying data processing procedure (`d_ausd_v_ta_apn_ve`) encounters an error.
*   **Setup:**
    1.  Ensure `project.dataset.job_table`, `project.dataset.job_run_audit`, `project.dataset.error_log`, and `project.dataset.target_table_for_ta_apn_ve` are empty.
    2.  **Temporarily modify `project.dataset.d_ausd_v_ta_apn_ve`** to always raise an error (e.g., `SELECT ERROR('Simulated processing error');`).
*   **Action:**
    Execute the main control procedure with valid parameters.
    ```python
    # pytest-style test code
    from google.cloud import bigquery
    import pytest

    client = bigquery.Client()
    project_id = "project"
    dataset_id = "dataset"
    job_kennung = "TEST_JOB_005"
    eintrags_nr = "ENTRY_005"

    # Clean up previous test data
    client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.job_table").result()
    client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.job_run_audit").result()
    client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.error_log").result()
    client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.target_table_for_ta_apn_ve").result()

    # NOTE: Manual step: Modify project.dataset.d_ausd_v_ta_apn_ve to raise an error
    # Example modification:
    # CREATE OR REPLACE PROCEDURE project.dataset.d_ausd_v_ta_apn_ve(...)
    # BEGIN
    #   SELECT ERROR('Simulated processing error in d_ausd_v_ta_apn_ve');
    # END;

    # Execute the main stored procedure
    query = f"""
    CALL {project_id}.{dataset_id}.r_ausd_vertrag_control(
        p_JobKennung => '{job_kennung}',
        p_EintragsNr => '{eintrags_nr}'
    );
    """
    with pytest.raises(Exception) as excinfo: # Expecting an error to be raised
        client.query(query).result()
    assert "Simulated processing error" in str(excinfo.value)
    ```
*   **Pass/Fail Criterion:**
    1.  The `r_ausd_vertrag_control` procedure terminates with an error.
    2.  `project.dataset.job_table` contains one entry for `(job_kennung, eintrags_nr)` with `status = 'FAILED'`, and `start_timestamp`/`end_timestamp` populated.
    3.  `project.dataset.job_run_audit` contains one entry for `(job_kennung, eintrags_nr)` with `status = 'FAILED'`, `records_processed = 0`, and `error_message` reflecting the error from `d_ausd_v_ta_apn_ve`.
    4.  `project.dataset.error_log` contains one entry with `error_code = 500` (or specific code if `d_ausd_v_ta_apn_ve` provides one), `error_message` reflecting the error, and `severity = 'E'`.
    5.  `project.dataset.target_table_for_ta_apn_ve` remains empty or reflects a rolled-back state, depending on the transactional nature of `d_ausd_v_ta_apn_ve`'s implementation.

    ```python
    # pytest-style assertions
    # Assert job_table state
    job_table_rows = list(client.query(f"SELECT status FROM {project_id}.{dataset_id}.job_table WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'").result())
    assert len(job_table_rows) == 1
    assert job_table_rows[0][0] == 'FAILED'

    # Assert job_run_audit state
    audit_rows = list(client.query(f"SELECT status, records_processed, error_message FROM {project_id}.{dataset_id}.job_run_audit WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}'").result())
    assert len(audit_rows) == 1
    assert audit_rows[0][0] == 'FAILED'
    assert audit_rows[0][1] == 0
    assert "Simulated processing error" in audit_rows[0][2]

    # Assert error_log state
    error_log_rows = list(client.query(f"SELECT error_code, error_message, severity FROM {project_id}.{dataset_id}.error_log WHERE job_id = '{job_kennung}'").result())
    assert len(error_log_rows) == 1
    assert error_log_rows[0][0] == 500 # Generic BQ error code
    assert "Simulated processing error" in error_log_rows[0][1]
    assert error_log_rows[0][2] == 'E'

    # Assert target_table_for_ta_apn_ve is empty (assuming rollback)
    target_table_count = client.query(f"SELECT COUNT(*) FROM {project_id}.{dataset_id}.target_table_for_ta_apn_ve WHERE eintrags_nr = '{eintrags_nr}' AND job_kennung = '{job_kennung}'").result().total_rows
    assert target_table_count == 0
    ```

---

### Test Case 6: Output Parity - Data Comparison

*   **Purpose:** Prove that the migrated `d_ausd_v_ta_apn_ve` procedure, when invoked by the control script, produces the exact same output data as the legacy `d_ausd_v_ta_apn_ve.sql` script. This is the most critical test for transformation correctness and output parity.
*   **Setup:**
    1.  **Crucially, `project.dataset.d_ausd_v_ta_apn_ve` must be fully implemented** with the translated logic from the original `d_ausd_v_ta_apn_ve.sql`.
    2.  Identify and prepare a representative set of source data that the original `d_ausd_v_ta_apn_ve.sql` would read from (e.g., Oracle tables).
    3.  Load this *exact same* source data into corresponding BigQuery source tables.
    4.  Run the **legacy** `k_ausd_v_ta_apn_ve.ksh` with specific `p_JobKennung` and `p_EintragsNr` using the prepared source data. Capture the final state of its target table (e.g., `ta_apn_ve` in Oracle). Store this as a "golden dataset".
    5.  Ensure `project.dataset.job_table`, `project.dataset.job_run_audit`, `project.dataset.error_log`, and `project.dataset.target_table_for_ta_apn_ve` are empty before running the migrated job.
*   **Action:**
    Execute the migrated `r_ausd_vertrag_control` procedure with the *same* `p_JobKennung` and `p_EintragsNr` as used for the legacy run, using the BigQuery source tables.
    ```python
    # pytest-style test code
    from google.cloud import bigquery
    import pandas as pd

    client = bigquery.Client()
    project_id = "project"
    dataset_id = "dataset"
    job_kennung = "GOLDEN_TEST_JOB"
    eintrags_nr = "GOLDEN_ENTRY"

    # Clean up previous test data
    client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.job_table").result()
    client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.job_run_audit").result()
    client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.error_log").result()
    client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.target_table_for_ta_apn_ve").result()

    # NOTE: Manual step: Load the "golden dataset" (legacy output) into a temporary BigQuery table
    # For example: project.dataset.golden_target_table_for_ta_apn_ve

    # NOTE: Manual step: Ensure BigQuery source tables are populated with the exact same data
    # as used for the legacy run.

    # Execute the main stored procedure
    query = f"""
    CALL {project_id}.{dataset_id}.r_ausd_vertrag_control(
        p_JobKennung => '{job_kennung}',
        p_EintragsNr => '{eintrags_nr}'
    );
    """
    client.query(query).result()

    # Fetch results from migrated target table
    migrated_df = client.query(f"SELECT * FROM {project_id}.{dataset_id}.target_table_for_ta_apn_ve WHERE eintrags_nr = '{eintrags_nr}' AND job_kennung = '{job_kennung}' ORDER BY 1,2,3").to_dataframe()

    # Fetch results from golden dataset
    golden_df = client.query(f"SELECT * FROM {project_id}.{dataset_id}.golden_target_table_for_ta_apn_ve ORDER BY 1,2,3").to_dataframe()
    ```
*   **Pass/Fail Criterion:**
    1.  The schema (column names, data types, nullability) of `project.dataset.target_table_for_ta_apn_ve` matches the schema of the legacy target table.
    2.  The row count of `project.dataset.target_table_for_ta_apn_ve` matches the row count of the legacy target table.
    3.  All data values in `project.dataset.target_table_for_ta_apn_ve` are identical to the data values in the legacy target table for the corresponding `p_EintragsNr` and `p_JobKennung`. This often requires sorting both datasets by primary key(s) before comparison.

    ```python
    # pytest-style assertions
    # Assert schema (conceptual, would involve comparing INFORMATION_SCHEMA or pandas dtypes)
    # assert list(migrated_df.columns) == list(golden_df.columns)
    # assert all(migrated_df.dtypes == golden_df.dtypes)

    # Assert row counts
    assert len(migrated_df) == len(golden_df)

    # Assert data equality (after sorting to handle potential order differences)
    pd.testing.assert_frame_equal(migrated_df, golden_df, check_dtype=True, check_exact=True)
    ```

---

### Test Case 7: Data Quality - Record Count Accuracy

*   **Purpose:** Verify that the `records_processed` value logged in `job_run_audit` accurately reflects the actual number of rows processed/inserted/updated by `d_ausd_v_ta_apn_ve`.
*   **Setup:**
    1.  Ensure `project.dataset.d_ausd_v_ta_apn_ve` is implemented to perform data manipulation and return the *actual* count of affected rows.
    2.  Ensure `project.dataset.job_table`, `project.dataset.job_run_audit`, `project.dataset.error_log`, and `project.dataset.target_table_for_ta_apn_ve` are empty.
*   **Action:**
    Execute the main control procedure with valid parameters.
    ```python
    # pytest-style test code
    from google.cloud import bigquery

    client = bigquery.Client()
    project_id = "project"
    dataset_id = "dataset"
    job_kennung = "TEST_JOB_007"
    eintrags_nr = "ENTRY_007"

    # Clean up previous test data
    client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.job_table").result()
    client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.job_run_audit").result()
    client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.error_log").result()
    client.query(f"TRUNCATE TABLE {project_id}.{dataset_id}.target_table_for_ta_apn_ve").result()

    # NOTE: Ensure d_ausd_v_ta_apn_ve is implemented to return an accurate count.
    # For example, if it inserts 10 rows, it should return 10.
    # Let's assume it inserts 7 rows for this test.

    # Execute the main stored procedure
    query = f"""
    CALL {project_id}.{dataset_id}.r_ausd_vertrag_control(
        p_JobKennung => '{job_kennung}',
        p_EintragsNr => '{eintrags_nr}'
    );
    """
    client.query(query).result()
    ```
*   **Pass/Fail Criterion:**
    1.  The `records_processed` value in `project.dataset.job_run_audit` for the completed run matches the `COUNT(*)` of rows in `project.dataset.target_table_for_ta_apn_ve` that were processed by this specific job run.

    ```python
    # pytest-style assertions
    # Get actual count from target table
    actual_processed_count = client.query(f"SELECT COUNT(*) FROM {project_id}.{dataset_id}.target_table_for_ta_apn_ve WHERE eintrags_nr = '{eintrags_nr}' AND job_kennung = '{job_kennung}'").result().total_rows

    # Get logged count from audit table
    logged_processed_count = list(client.query(f"SELECT records_processed FROM {project_id}.{dataset_id}.job_run_audit WHERE job_kennung = '{job_kennung}' AND eintrags_nr = '{eintrags_nr}' AND status = 'COMPLETED'").result())[0][0]

    assert actual_processed_count == logged_processed_count
    ```

---

### Test Case 8: Schema Assertions for Audit/Log Tables

*   **Purpose:** Verify that the schemas of the audit and logging tables (`error_log`, `job_table`, `job_run_audit`) conform to the design specifications.
*   **Setup:**
    1.  Ensure the tables `project.dataset.error_log`, `project.dataset.job_table`, and `project.dataset.job_run_audit` exist.
*   **Action:**
    Query BigQuery's `INFORMATION_SCHEMA` to retrieve table and column details.
    ```python
    # pytest-style test code
    from google.cloud import bigquery

    client = bigquery.Client()
    project_id = "project"
    dataset_id = "dataset"

    expected_schemas = {
        "error_log": {
            "job_id": "STRING", "run_id": "STRING", "error_code": "INT64",
            "error_message": "STRING", "error_arg": "STRING", "severity": "STRING",
            "timestamp": "TIMESTAMP"
        },
        "job_table": {
            "job_kennung": "STRING", "eintrags_nr": "STRING", "status": "STRING",
            "start_timestamp": "TIMESTAMP", "end_timestamp": "TIMESTAMP", "last_updated": "TIMESTAMP"
        },
        "job_run_audit": {
            "job_kennung": "STRING", "eintrags_nr": "STRING", "run_timestamp": "TIMESTAMP",
            "status": "STRING", "records_processed": "INT64", "start_timestamp": "TIMESTAMP",
            "end_timestamp": "TIMESTAMP", "error_message": "STRING"
        }
    }

    def get_table_schema(table_name):
        query = f"""
        SELECT column_name, data_type
        FROM {project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position;
        """
        rows = client.query(query).result()
        return {row.column_name: row.data_type for row in rows}

    # Retrieve actual schemas
    actual_error_log_schema = get_table_schema("error_log")
    actual_job_table_schema = get_table_schema("job_table")
    actual_job_run_audit_schema = get_table_schema("job_run_audit")
    ```
*   **Pass/Fail Criterion:**
    1.  The column names and their corresponding BigQuery data types for each table (`error_log`, `job_table`, `job_run_audit`) exactly match the expected schemas defined in the migration design.

    ```python
    # pytest-style assertions
    assert actual_error_log_schema == expected_schemas["error_log"]
    assert actual_job_table_schema == expected_schemas["job_table"]
    assert actual_job_run_audit_schema == expected_schemas["job_run_audit"]
    ```

---

### Test Case 9: External System Replacement - Oracle to BigQuery SQL

*   **Purpose:** Verify that the replacement of `h_alis_sqlplus.ksh` (implying Oracle interaction) with direct BigQuery SQL execution within stored procedures functions correctly. This is covered implicitly by the data parity test (Test Case 6) but explicitly calls out the replacement.
*   **Setup:**
    1.  `project.dataset.d_ausd_v_ta_apn_ve` is fully implemented in BQSQL, replacing the original Oracle SQL.
    2.  BigQuery source tables are populated with data equivalent to the original Oracle source tables.
*   **Action:**
    Execute `r_ausd_vertrag_control` as in a normal successful run (Test Case 1 or 6).
*   **Pass/Fail Criterion:**
    1.  The job completes successfully.
    2.  No errors related to database connectivity or SQL dialect conversion are observed.
    3.  The output data in `project.dataset.target_table_for_ta_apn_ve` is correct and matches the legacy Oracle output (as verified by Test Case 6).
    4.  The `job_run_audit` and `job_table` reflect a successful run, indicating the BigQuery SQL execution was seamless.

    *(This test case primarily relies on the success of Test Case 6 and the absence of specific errors related to the database interaction. No additional code is needed beyond what's in Test Case 6.)*

---

These tests provide a comprehensive validation strategy for the migrated `k_ausd_v_ta_apn_ve.ksh` job, focusing on behavioral equivalence and adherence to the migration design. The placeholder nature of `d_ausd_v_ta_apn_ve` means that its internal logic requires separate, detailed unit and integration tests once its content is fully migrated.