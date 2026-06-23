As a senior data-migration QA engineer, I've designed a suite of validation tests for the migration of `r_ausd_austausch.ksh` to Google Cloud BigQuery. These tests aim to ensure behavioral equivalence, covering output parity, transformation correctness, external system interactions, and data quality.

A critical challenge highlighted in the design document is the *unknown content* of the core script `k_ausd_austausch.ksh`. Therefore, the tests for data transformation are based on the inferred logic from the wrapper script's comments and the provided BigQuery DDL. Any discrepancies found during test execution will necessitate a review of the actual legacy `k_ausd_austausch.ksh` script.

The tests are organized into sections covering the main aspects of the migration.

---

## Migration Validation Tests for `r_ausd_austausch.ksh`

### Assumptions for Test Data & Logic (due to unknown `k_ausd_austausch.ksh` content)

1.  **Source Tables**: The `contract_cache_source` mentioned in the legacy script is represented by `bert_reporting.sof_ta_p_vertrag` and other `bert_reporting.sof_ta_p_*` tables.
2.  **Target Tables**: The FOS table is primarily `bert_reporting.rpt_ta_s_d1_vertrag`, with other `bert_reporting.rpt_ta_s_d1_*` tables being populated as part of the overall process.
3.  **`DWH_VERTRAG_ID`**: This legacy identifier for restart logic is assumed to map to `vertrag_id_carmen` in the `rpt_ta_s_d1_vertrag` table. For numeric comparisons (e.g., `DWH_VERTRAG_ID >= Wiederanlaufwert`), `vertrag_id_carmen` is assumed to be castable to an integer or handled as a string comparison if the legacy system did so.
4.  **`LADEDATUM`**: A `LADEDATUM DATE` column is assumed to exist in `bert_reporting.sof_ta_p_vertrag` for filtering purposes, as it's mentioned in the legacy script's comments but not explicitly in the provided `sof_ta_p_vertrag` DDL.
5.  **Core Logic (`k_ausd_austausch.ksh`)**: The core script is assumed to perform `DELETE` and `INSERT` operations as described in the migration design, involving joins between `sof_ta_p_*` tables to populate `rpt_ta_s_d1_*` tables. The wide schema of `rpt_ta_s_d1_vertrag` implies complex joins and data derivations.

---

### Test Case 1.1: Full Load - Default Stichtag (Output Parity & Transformation)

*   **Purpose**: Verify that the migrated job correctly processes data and produces identical output to the legacy job when no `Stichtag` or `Wiederanlaufwert` is explicitly provided, thus defaulting `Stichtag` to the current system date and `Wiederanlaufwert` to `0`.
*   **Setup**:
    1.  Ensure all target tables (`bert_reporting.rpt_ta_s_d1_*`) are empty.
    2.  Populate source tables (`bert_reporting.sof_ta_p_*`) with a comprehensive dataset. This dataset should include:
        *   Records where `Gueltig_von <= CURRENT_DATE()` and `CURRENT_DATE() < Gueltig_bis`.
        *   Records where `LADEDATUM < CURRENT_DATE()`.
        *   Records that should be excluded based on these date filters.
        *   Records with various `vertrag_id_carmen` values.
    3.  Record the `CURRENT_DATE()` for the test run.
*   **Action**:
    1.  Execute the legacy `r_ausd_austausch.ksh` script without any command-line arguments.
    2.  Execute the migrated BigQuery Stored Procedure (e.g., `CALL bert_reporting.r_ausd_austausch_sp(NULL, 0);`).
*   **Pass/Fail Criteria**:
    1.  The `job_audit_log` table in BigQuery must show a `status = 'SUCCESS'` entry for the migrated job.
    2.  The row counts of all target tables (`bert_reporting.rpt_ta_s_d1_*`) must be identical between the legacy and migrated systems.
    3.  The data content of the primary target table (`bert_reporting.rpt_ta_s_d1_vertrag`) must be identical. A checksum or row-by-row comparison should yield no differences.
    4.  (Optional) Compare other `rpt_ta_s_d1_*` tables for full parity.

*   **Runnable Test Code (Python with Pytest & BigQuery Client)**:

    ```python
    import pytest
    from google.cloud import bigquery
    from datetime import date, timedelta

    # Assume client and project_id are configured
    client = bigquery.Client()
    project_id = "your-gcp-project-id"
    dataset_id = "bert_reporting"

    def _run_legacy_job():
        """Simulates running the legacy ksh script and capturing its output."""
        # This would typically involve SSHing to the legacy server and executing the script,
        # then extracting data from the legacy database.
        # For this example, we'll assume a function that returns the data.
        print("Running legacy job without parameters...")
        # Placeholder for actual legacy execution and data extraction
        # Example: legacy_data = get_data_from_legacy_db("rpt_ta_s_d1_vertrag")
        # For testing, we might load a pre-generated "golden" dataset.
        return {
            "rpt_ta_s_d1_vertrag": [
                {"kund_nr_dpps": "C1", "vertrag_id_carmen": "V1", "vertragsbeginn": date(2023, 1, 1), ...},
                {"kund_nr_dpps": "C2", "vertrag_id_carmen": "V2", "vertragsbeginn": date(2023, 1, 5), ...},
            ],
            "rpt_ta_s_d1_rech_empf": [...],
            # ... other target tables
        }

    def _run_migrated_job(stichtag_str=None, wiederanlauf_wert=0):
        """Executes the BigQuery Stored Procedure."""
        stichtag_param = f"'{stichtag_str}'" if stichtag_str else "NULL"
        query = f"""
            CALL `{project_id}.{dataset_id}.r_ausd_austausch_sp`({stichtag_param}, {wiederanlauf_wert});
        """
        print(f"Running migrated job with: stichtag={stichtag_param}, wiederanlauf={wiederanlauf_wert}")
        job = client.query(query)
        job.result() # Wait for the job to complete
        print("Migrated job completed.")

    def _get_bq_table_data(table_name):
        """Fetches all data from a BigQuery table."""
        query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_name}` ORDER BY 1, 2, 3" # Order for consistent comparison
        rows = client.query(query).result()
        return [dict(row) for row in rows]

    def _get_bq_row_count(table_name):
        """Fetches row count from a BigQuery table."""
        query = f"SELECT COUNT(*) FROM `{project_id}.{dataset_id}.{table_name}`"
        return client.query(query).result().single_value

    def _clear_bq_target_tables():
        """Clears data from target tables for a clean test run."""
        target_tables = ["rpt_ta_s_d1_vertrag", "rpt_ta_s_d1_rech_empf", "rpt_ta_s_d1_rech_kunde",
                         "rpt_ta_s_d1_discount", "rpt_ta_s_d1_discount_rr", "rpt_ta_s_d1_vpn"]
        for table in target_tables:
            client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.{table}`").result()
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_audit_log`").result()
        print("Cleared BigQuery target tables.")

    def _populate_bq_source_tables(stichtag_for_data):
        """Populates BigQuery source tables with test data."""
        # This is a simplified example. In a real scenario, you'd have a more robust
        # data generation or loading mechanism.
        print(f"Populating source tables for stichtag: {stichtag_for_data}")
        # Example for sof_ta_p_vertrag
        source_table = f"`{project_id}.{dataset_id}.sof_ta_p_vertrag`"
        client.query(f"TRUNCATE TABLE {source_table}").result()
        insert_query = f"""
        INSERT INTO {source_table} (
            cntrct_ty, vertrag_id_carmen, partner_id_carmen, rechdef_id_carmen, kundenkonto,
            vertragsbeginn, geplant_kuend, LADEDATUM -- LADEDATUM is an assumed column
        ) VALUES
            (1, 'V001', 'P001', 'R001', 'K001', '{stichtag_for_data - timedelta(days=1)}', '{stichtag_for_data + timedelta(days=10)}', '{stichtag_for_data - timedelta(days=2)}'), -- Included
            (2, 'V002', 'P002', 'R002', 'K002', '{stichtag_for_data}', '{stichtag_for_data + timedelta(days=5)}', '{stichtag_for_data - timedelta(days=1)}'),     -- Included
            (3, 'V003', 'P003', 'R003', 'K003', '{stichtag_for_data + timedelta(days=1)}', '{stichtag_for_data + timedelta(days=10)}', '{stichtag_for_data - timedelta(days=2)}'), -- Excluded (Gueltig_von > Stichtag)
            (4, 'V004', 'P004', 'R004', 'K004', '{stichtag_for_data - timedelta(days=5)}', '{stichtag_for_data}', '{stichtag_for_data - timedelta(days=2)}'),     -- Excluded (Stichtag not < Gueltig_bis)
            (5, 'V005', 'P005', 'R005', 'K005', '{stichtag_for_data - timedelta(days=1)}', '{stichtag_for_data + timedelta(days=10)}', '{stichtag_for_data}'); -- Excluded (LADEDATUM not < Stichtag)
        """
        client.query(insert_query).result()
        # ... populate other sof_ta_p_* tables similarly
        print("Source tables populated.")

    @pytest.fixture(scope="module", autouse=True)
    def setup_module():
        """Module-level setup to ensure tables exist and are clean."""
        # This would run DDL scripts to ensure tables exist before tests
        # For this example, we assume DDL has been run.
        _clear_bq_target_tables()
        yield
        _clear_bq_target_tables() # Clean up after tests

    def test_full_load_default_stichtag():
        current_test_date = date.today() # Simulate CURRENT_DATE() for consistency
        _populate_bq_source_tables(current_test_date)
        _clear_bq_target_tables() # Ensure target is clean before run

        # --- Legacy Run Simulation ---
        # In a real scenario, this would involve running the actual ksh script
        # and then extracting its results from the legacy database.
        # For this example, we'll define expected_legacy_data based on our _populate_bq_source_tables logic.
        expected_legacy_vertrag_data = [
            {'cntrct_ty': 1, 'vertrag_id_carmen': 'V001', 'partner_id_carmen': 'P001', 'rechdef_id_carmen': 'R001', 'kundenkonto': 'K001', 'vertragsbeginn': current_test_date - timedelta(days=1), 'geplant_kuend': current_test_date + timedelta(days=10), 'LADEDATUM': current_test_date - timedelta(days=2)},
            {'cntrct_ty': 2, 'vertrag_id_carmen': 'V002', 'partner_id_carmen': 'P002', 'rechdef_id_carmen': 'R002', 'kundenkonto': 'K002', 'vertragsbeginn': current_test_date, 'geplant_kuend': current_test_date + timedelta(days=5), 'LADEDATUM': current_test_date - timedelta(days=1)},
        ]
        # Assume other tables would also have corresponding expected data.
        # For simplicity, we'll focus on rpt_ta_s_d1_vertrag for data content.
        expected_legacy_vertrag_count = len(expected_legacy_vertrag_data)
        # --- End Legacy Run Simulation ---

        # --- Migrated Run ---
        _run_migrated_job(stichtag_str=None, wiederanlauf_wert=0)

        # --- Assertions ---
        # 1. Check audit log
        audit_log_entries = _get_bq_table_data("job_audit_log")
        assert len(audit_log_entries) == 1
        assert audit_log_entries[0]["status"] == "SUCCESS"
        assert audit_log_entries[0]["stichtag"] == current_test_date

        # 2. Check row counts
        migrated_vertrag_count = _get_bq_row_count("rpt_ta_s_d1_vertrag")
        assert migrated_vertrag_count == expected_legacy_vertrag_count, \
            f"Row count mismatch for rpt_ta_s_d1_vertrag. Expected: {expected_legacy_vertrag_count}, Got: {migrated_vertrag_count}"

        # 3. Check data content (for primary target table)
        migrated_vertrag_data = _get_bq_table_data("rpt_ta_s_d1_vertrag")
        # This comparison needs to be robust, handling column order, types, etc.
        # For simple dicts, direct comparison works if keys/values match.
        # In a real scenario, you'd compare sorted lists of dicts or use a data diffing library.
        assert sorted(migrated_vertrag_data, key=lambda x: x['vertrag_id_carmen']) == \
               sorted(expected_legacy_vertrag_data, key=lambda x: x['vertrag_id_carmen']), \
               "Data content mismatch for rpt_ta_s_d1_vertrag."

        # 4. (Optional) Check other target tables similarly
        # migrated_rech_empf_count = _get_bq_row_count("rpt_ta_s_d1_rech_empf")
        # assert migrated_rech_empf_count == expected_legacy_rech_empf_count
        # ...
    ```

### Test Case 1.2: Full Load - Specific Stichtag (Output Parity & Transformation)

*   **Purpose**: Verify the job correctly processes data for a *specified* `Stichtag` when no restart value is provided.
*   **Setup**:
    1.  Ensure all target tables (`bert_reporting.rpt_ta_s_d1_*`) are empty.
    2.  Populate source tables (`bert_reporting.sof_ta_p_*`) with data relevant to a specific `Stichtag` (e.g., `2023-01-15`). Include records that should be included/excluded based on `Gueltig_von`, `Gueltig_bis`, `LADEDATUM` relative to this specific `Stichtag`.
*   **Action**:
    1.  Execute the legacy `r_ausd_austausch.ksh` script with `-s 15012023`.
    2.  Execute the migrated BigQuery Stored Procedure: `CALL bert_reporting.r_ausd_austausch_sp('15012023', 0);`.
*   **Pass/Fail Criteria**:
    1.  The `job_audit_log` table in BigQuery must show a `status = 'SUCCESS'` entry for the migrated job, with `stichtag = '2023-01-15'`.
    2.  Row counts of all target tables must be identical between legacy and migrated systems.
    3.  Data content of `bert_reporting.rpt_ta_s_d1_vertrag` (and other relevant target tables) must be identical.

### Test Case 1.3: Restart Logic (Partial Update) (Output Parity & Transformation)

*   **Purpose**: Verify the `Wiederanlaufwert` logic correctly deletes and inserts records, ensuring idempotency and partial processing. This tests the `DELETE ... WHERE DWH_VERTRAG_ID >= Wiederanlaufwert` and `INSERT ... WHERE DWH_VERTRAG_ID > Wiederanlaufwert` behavior.
*   **Setup**:
    1.  Choose a `Stichtag` (e.g., `2023-02-01`).
    2.  **Initial Load**: Populate source tables and run a full load (e.g., using Test Case 1.2 logic) with `Stichtag = 2023-02-01` and `Wiederanlaufwert = 0`. This populates the target tables with an initial state.
    3.  **Data Modification for Restart**:
        *   Modify some source records in `sof_ta_p_vertrag` where `vertrag_id_carmen` is above a chosen `Wiederanlaufwert` (e.g., `100`).
        *   Add new source records with `vertrag_id_carmen > 100`.
        *   Ensure some records with `vertrag_id_carmen < 100` remain unchanged in the source.
*   **Action**:
    1.  Execute the legacy `r_ausd_austausch.ksh` script with `-s 01022023 -l 100`.
    2.  Execute the migrated BigQuery Stored Procedure: `CALL bert_reporting.r_ausd_austausch_sp('01022023', 100);`.
*   **Pass/Fail Criteria**:
    1.  The `job_audit_log` table must show a `status = 'SUCCESS'` entry, with `stichtag = '2023-02-01'` and `wiederanlauf_wert = 100`.
    2.  Records in `bert_reporting.rpt_ta_s_d1_vertrag` (and other relevant target tables) where `vertrag_id_carmen` (or its numeric equivalent) is less than `100` must be identical to their state *before* the restart run.
    3.  Records where `vertrag_id_carmen` (or its numeric equivalent) is greater than or equal to `100` must reflect the state derived from the *modified* source data, matching the legacy output.
    4.  Row counts for the entire target table must be identical between legacy and migrated systems.

*   **Runnable Test Code (SQL for comparison)**:

    ```sql
    -- Assuming 'legacy_rpt_ta_s_d1_vertrag' is a snapshot of the legacy output
    -- and 'migrated_rpt_ta_s_d1_vertrag' is the output from the BigQuery job.

    -- Check for records that should be untouched (vertrag_id_carmen < 100)
    SELECT
        COUNT(*)
    FROM
        (SELECT * FROM `your-gcp-project-id.bert_reporting.rpt_ta_s_d1_vertrag` WHERE CAST(vertrag_id_carmen AS INT64) < 100) AS migrated
    EXCEPT DISTINCT
        (SELECT * FROM `your-gcp-project-id.legacy_snapshots.rpt_ta_s_d1_vertrag_before_restart` WHERE CAST(vertrag_id_carmen AS INT64) < 100) AS legacy_before_restart;
    -- Expected result: 0 rows

    -- Check for records that should be updated/inserted (vertrag_id_carmen >= 100)
    SELECT
        COUNT(*)
    FROM
        (SELECT * FROM `your-gcp-project-id.bert_reporting.rpt_ta_s_d1_vertrag` WHERE CAST(vertrag_id_carmen AS INT64) >= 100) AS migrated
    EXCEPT DISTINCT
        (SELECT * FROM `your-gcp-project-id.legacy_snapshots.rpt_ta_s_d1_vertrag_after_restart`) AS legacy_after_restart;
    -- Expected result: 0 rows

    -- Check total row count
    SELECT
        (SELECT COUNT(*) FROM `your-gcp-project-id.bert_reporting.rpt_ta_s_d1_vertrag`) =
        (SELECT COUNT(*) FROM `your-gcp-project-id.legacy_snapshots.rpt_ta_s_d1_vertrag_after_restart`);
    -- Expected result: TRUE
    ```

### Test Case 1.4: Filtering Conditions - Edge Cases (Transformation Correctness)

*   **Purpose**: Verify the exact boundaries of the `Gueltig_von`, `Gueltig_bis`, and `LADEDATUM` filters (`Gueltig_von <= Stichtag`, `Stichtag < Gueltig_bis`, `LADEDATUM < Stichtag`).
*   **Setup**:
    1.  Choose a `Stichtag` (e.g., `2023-03-10`).
    2.  Populate `bert_reporting.sof_ta_p_vertrag` with records specifically designed to test boundaries:
        *   `Gueltig_von = '2023-03-10'`, `Gueltig_bis = '2023-03-11'`, `LADEDATUM = '2023-03-09'` (Should be INCLUDED)
        *   `Gueltig_von = '2023-03-09'`, `Gueltig_bis = '2023-03-11'`, `LADEDATUM = '2023-03-09'` (Should be INCLUDED)
        *   `Gueltig_von = '2023-03-11'`, `Gueltig_bis = '2023-03-12'`, `LADEDATUM = '2023-03-09'` (Should be EXCLUDED: `Gueltig_von > Stichtag`)
        *   `Gueltig_von = '2023-03-09'`, `Gueltig_bis = '2023-03-10'`, `LADEDATUM = '2023-03-09'` (Should be EXCLUDED: `Stichtag` not `< Gueltig_bis`)
        *   `Gueltig_von = '2023-03-09'`, `Gueltig_bis = '2023-03-11'`, `LADEDATUM = '2023-03-10'` (Should be EXCLUDED: `LADEDATUM` not `< Stichtag`)
*   **Action**:
    1.  Execute legacy job with `-s 10032023`.
    2.  Execute migrated job with `CALL bert_reporting.r_ausd_austausch_sp('10032023', 0);`.
*   **Pass/Fail Criteria**:
    1.  The `job_audit_log` table must show a `status = 'SUCCESS'`.
    2.  The set of `vertrag_id_carmen` values (or full data content) in `bert_reporting.rpt_ta_s_d1_vertrag` must be identical between legacy and migrated systems, containing only the expected INCLUDED records.

### Test Case 1.5: NULL Handling (Transformation Correctness)

*   **Purpose**: Verify how NULL values in critical filter columns (`Gueltig_von`, `Gueltig_bis`, `LADEDATUM`, `vertrag_id_carmen`) or join keys are handled during data transformation.
*   **Setup**:
    1.  Populate `bert_reporting.sof_ta_p_vertrag` with records containing NULLs in `vertragsbeginn`, `geplant_kuend`, `LADEDATUM`, and `vertrag_id_carmen`.
    2.  Populate other `sof_ta_p_*` tables with NULLs in their join keys.
*   **Action**:
    1.  Execute legacy job with a valid `Stichtag`.
    2.  Execute migrated job with `CALL bert_reporting.r_ausd_austausch_sp('<Stichtag>', 0);`.
*   **Pass/Fail Criteria**:
    1.  The `job_audit_log` table must show a `status = 'SUCCESS'`.
    2.  The data content of `bert_reporting.rpt_ta_s_d1_vertrag` (and other relevant target tables) must be identical between legacy and migrated systems. Specifically, records with NULLs in filter columns should be consistently excluded, and records with NULLs in join keys should be consistently handled (e.g., excluded from joins if `INNER JOIN` is used).

### Test Case 1.6: Data Type Handling & Implicit Conversions (Transformation Correctness)

*   **Purpose**: Verify that data types are correctly mapped and any implicit conversions (e.g., date strings to dates, numeric strings to numbers) behave identically.
*   **Setup**:
    1.  Populate source tables with data that might challenge type conversions:
        *   Dates in slightly non-standard but parseable formats (if the legacy system was lenient).
        *   Numeric fields in source that might be strings and need conversion to INT64/FLOAT64 in BigQuery.
        *   Strings with leading/trailing spaces, special characters.
    2.  Ensure target table DDL (`rpt_ta_s_d1_vertrag`) has appropriate BigQuery types.
*   **Action**:
    1.  Execute legacy job with a valid `Stichtag`.
    2.  Execute migrated job with `CALL bert_reporting.r_ausd_austausch_sp('<Stichtag>', 0);`.
*   **Pass/Fail Criteria**:
    1.  The `job_audit_log` table must show a `status = 'SUCCESS'`.
    2.  The data content of `bert_reporting.rpt_ta_s_d1_vertrag` (and other relevant target tables) must be identical between legacy and migrated systems. Pay close attention to fields that underwent type conversion. For example, a `STRING` in legacy becoming a `DATE` in BigQuery should have the correct date value.

### Test Case 2.1: Schema Parity (Data Quality / Schema Assertions)

*   **Purpose**: Verify that the schema of the target tables in BigQuery matches the expected schema (based on the provided DDL and any known legacy schema).
*   **Setup**: N/A (schema is static).
*   **Action**: Inspect the BigQuery table schemas using `INFORMATION_SCHEMA`.
*   **Pass/Fail Criteria**:
    1.  All target tables (`bert_reporting.rpt_ta_s_d1_vertrag`, `rpt_ta_s_d1_rech_empf`, etc.) must exist.
    2.  Column names, data types, and nullability constraints in BigQuery must precisely match the provided DDL and any documented legacy schema.

*   **Runnable Test Code (SQL)**:

    ```sql
    -- Example for rpt_ta_s_d1_vertrag
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM
        `your-gcp-project-id.bert_reporting.INFORMATION_SCHEMA.COLUMNS`
    WHERE
        table_name = 'rpt_ta_s_d1_vertrag'
    ORDER BY
        ordinal_position;

    -- Compare this output to the expected DDL.
    -- In a pytest, you'd fetch this and compare against a hardcoded expected schema dictionary.
    ```

### Test Case 2.2: Data Integrity - Non-NULLable Columns (Data Quality / Schema Assertions)

*   **Purpose**: Verify that columns defined as `NOT NULL` in the target BigQuery schema actually contain no NULLs after migration, indicating correct transformation logic.
*   **Setup**:
    1.  Populate source data that, if transformations are incorrect, *could* produce NULLs for columns defined as `NOT NULL` in the target.
    2.  Run a full load of the migrated job (e.g., using Test Case 1.1 or 1.2).
*   **Action**: Query the target tables for NULL values in columns marked as `NOT NULL` in their DDL.
*   **Pass/Fail Criteria**: No rows should be returned for any of these queries.

*   **Runnable Test Code (SQL)**:

    ```sql
    -- Example for rpt_ta_s_d1_vertrag, assuming 'kund_nr_dpps' is NOT NULL
    SELECT
        'rpt_ta_s_d1_vertrag' AS table_name,
        'kund_nr_dpps' AS column_name,
        COUNT(*) AS null_count
    FROM
        `your-gcp-project-id.bert_reporting.rpt_ta_s_d1_vertrag`
    WHERE
        kund_nr_dpps IS NULL
    HAVING
        null_count > 0;

    -- Repeat for all NOT NULL columns across all target tables.
    -- In a pytest, you'd iterate through a list of (table, column) pairs.
    ```

### Test Case 3.1: Parameter Validation - Invalid Stichtag Format (Orchestration & Error Handling)

*   **Purpose**: Verify that the job correctly handles an invalid `Stichtag` format, failing gracefully and logging the error.
*   **Setup**: N/A.
*   **Action**:
    1.  Execute legacy job with `-s 2023-01-01` (an invalid `DDMMYYYY` format).
    2.  Execute migrated BigQuery Stored Procedure: `CALL bert_reporting.r_ausd_austausch_sp('2023-01-01', 0);`.
*   **Pass/Fail Criteria**:
    1.  Both the legacy and migrated jobs must fail.
    2.  The legacy job's log file should contain an error message related to date parsing.
    3.  The `job_audit_log` table in BigQuery must contain an entry with `status = 'FAILED'`, `error_details` indicating a date parsing error, and `message` reflecting the failure.
    4.  No data should be inserted or modified in the target tables.

*   **Runnable Test Code (Python with Pytest)**:

    ```python
    import pytest
    from google.cloud import bigquery

    # ... (client, project_id, dataset_id, _clear_bq_target_tables, _get_bq_table_data functions from above) ...

    def _run_migrated_job_expect_failure(stichtag_str=None, wiederanlauf_wert=0):
        """Executes the BigQuery Stored Procedure and expects it to fail."""
        stichtag_param = f"'{stichtag_str}'" if stichtag_str else "NULL"
        query = f"""
            CALL `{project_id}.{dataset_id}.r_ausd_austausch_sp`({stichtag_param}, {wiederanlauf_wert});
        """
        print(f"Running migrated job (expecting failure) with: stichtag={stichtag_param}, wiederanlauf={wiederanlauf_wert}")
        with pytest.raises(Exception) as excinfo: # BigQuery client will raise an exception on SP error
            job = client.query(query)
            job.result()
        assert "Stichtag is missing or invalid" in str(excinfo.value) or \
               "Failed to parse date" in str(excinfo.value) # Specific error message from SP

    def test_invalid_stichtag_format_failure():
        _clear_bq_target_tables() # Ensure clean state

        # --- Legacy Run Simulation ---
        # Assume legacy job fails with specific error message for '-s 2023-01-01'
        # --- End Legacy Run Simulation ---

        # --- Migrated Run ---
        _run_migrated_job_expect_failure(stichtag_str='2023-01-01', wiederanlauf_wert=0)

        # --- Assertions ---
        # 1. Check audit log for failure
        audit_log_entries = _get_bq_table_data("job_audit_log")
        assert len(audit_log_entries) == 1
        assert audit_log_entries[0]["status"] == "FAILED"
        assert "invalid date" in audit_log_entries[0]["error_details"].lower() or \
               "stichtag is missing or invalid" in audit_log_entries[0]["error_details"].lower()

        # 2. Ensure no data was written to target tables
        assert _get_bq_row_count("rpt_ta_s_d1_vertrag") == 0
        # ... check other target tables
    ```

### Test Case 3.2: Error Handling - Core Logic Failure (Orchestration & Error Handling)

*   **Purpose**: Verify that the wrapper/orchestrator correctly logs and reports errors originating from the core data transformation logic (e.g., the `k_ausd_austausch_sp` in BigQuery).
*   **Setup**:
    1.  Create a scenario where the core logic will intentionally fail. This could be:
        *   Introducing a data type mismatch in source data that causes an `INSERT` to fail (e.g., inserting a string into an INT64 column).
        *   Making a required source table temporarily unavailable (e.g., by revoking permissions for the service account running the job).
        *   Modifying the `k_ausd_austausch_sp` to explicitly `RAISE` an error under certain conditions.
    2.  Ensure target tables are initially empty or in a known state.
*   **Action**:
    1.  Execute legacy job with the failing condition.
    2.  Execute migrated BigQuery Stored Procedure with the failing condition.
*   **Pass/Fail Criteria**:
    1.  Both the legacy and migrated jobs must terminate with an error.
    2.  The legacy job's log file should contain detailed error messages from the core script.
    3.  The `job_audit_log` table in BigQuery must contain an entry with `status = 'FAILED'`, and `error_details` providing insights into the core logic failure.
    4.  The state of the target tables should reflect the point of failure (e.g., partial data if the error occurred mid-way, or no data if it failed early). This behavior should be consistent between legacy and migrated.

### Test Case 3.3: Audit Logging (Orchestration & Error Handling)

*   **Purpose**: Verify that the `job_audit_log` table is correctly populated for both successful and failed runs, capturing all required metadata.
*   **Setup**: N/A.
*   **Action**:
    1.  Run a successful job (e.g., Test Case 1.1).
    2.  Run a failed job (e.g., Test Case 3.1).
*   **Pass/Fail Criteria**:
    1.  **Successful Run**:
        *   `job_audit_log` must have one entry with `status = 'SUCCESS'`.
        *   `job_name`, `start_time`, `end_time`, `stichtag`, `wiederanlauf_wert`, and `message` fields must be correctly populated. `error_details` should be NULL or empty.
    2.  **Failed Run**:
        *   `job_audit_log` must have one entry with `status = 'FAILED'`.
        *   `job_name`, `start_time`, `end_time` (if applicable), `stichtag`, `wiederanlauf_wert`, `message`, and `error_details` fields must be correctly populated. `error_details` should contain relevant error information.
    3.  The `start_time` and `end_time` should reflect the actual execution duration.

*   **Runnable Test Code (SQL)**:

    ```sql
    -- After running a successful job:
    SELECT
        job_name,
        start_time,
        end_time,
        status,
        message,
        stichtag,
        wiederanlauf_wert,
        error_details
    FROM
        `your-gcp-project-id.bert_reporting.job_audit_log`
    WHERE
        status = 'SUCCESS';
    -- Assert that all fields are as expected.

    -- After running a failed job:
    SELECT
        job_name,
        start_time,
        end_time,
        status,
        message,
        stichtag,
        wiederanlauf_wert,
        error_details
    FROM
        `your-gcp-project-id.bert_reporting.job_audit_log`
    WHERE
        status = 'FAILED';
    -- Assert that status is 'FAILED' and error_details is populated.
    ```