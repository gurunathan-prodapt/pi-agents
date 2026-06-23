As a senior data-migration QA engineer, I have prepared a comprehensive suite of migration validation tests for the `r_ausd_geschaeftspartner.ksh` job. These tests are designed to ensure the BigQuery implementation (`sp_initial_befuellung_vertrags_cache_fos` and `sp_ausd_geschaeftspartner`) is behaviourally equivalent to the legacy KornShell script.

The tests cover output parity, transformation correctness, external system replacements (logging, control tables), and data quality assertions.

**Assumptions for Testing:**
*   A BigQuery project and dataset (`project_id.dataset_id`) are configured.
*   All DDLs for `job_log`, `job_control`, `fos_vertrags_cache`, and `dwh_vertrag_cache_source` have been executed.
*   The stored procedures `sp_ausd_geschaeftspartner` and `sp_initial_befuellung_vertrags_cache_fos` have been deployed.
*   A mechanism exists to execute the legacy `r_ausd_geschaeftspartner.ksh` script and capture its output into a "legacy" target table (e.g., `project_id.dataset_id.legacy_fos_vertrags_cache`) for direct comparison. For the purpose of these tests, we will assume such a `legacy_fos_vertrags_cache` table can be populated with the expected output of the legacy system.
*   Test data for `dwh_vertrag_cache_source` is carefully crafted to cover various scenarios.

---

## Test Case 1: Happy Path - Full Load with Default Stichtag and No Restart

*   **Purpose**: Verify the migrated job correctly performs a full load when no `Stichtag` or `Wiederanlaufwert` is provided, using the default `CURRENT_DATE()` for `Stichtag` and `0` for `Wiederanlaufwert`. This tests the basic data extraction and loading logic.
*   **Setup**:
    1.  Clear `project_id.dataset_id.fos_vertrags_cache`, `job_log`, and `job_control` tables.
    2.  Populate `project_id.dataset_id.dwh_vertrag_cache_source` with diverse test data, including records that should and should not be selected based on `gueltig_von`, `gueltig_bis`, and `ladedatum` relative to `CURRENT_DATE()`.
        *   Example data:
            *   `dwh_vertrag_id=1, vertrags_nummer='V001', gueltig_von='2023-01-01', gueltig_bis='2024-01-01', betrag=100.00, ladedatum='2023-10-01'` (Should be selected if `CURRENT_DATE()` is e.g., '2023-11-15')
            *   `dwh_vertrag_id=2, vertrags_nummer='V002', gueltig_von='2023-01-01', gueltig_bis='2023-11-15', betrag=200.00, ladedatum='2023-10-01'` (Should NOT be selected if `CURRENT_DATE()` is '2023-11-15' because `Stichtag < gueltig_bis` is false)
            *   `dwh_vertrag_id=3, vertrags_nummer='V003', gueltig_von='2023-01-01', gueltig_bis='2024-01-01', betrag=300.00, ladedatum='2023-11-15'` (Should NOT be selected if `CURRENT_DATE()` is '2023-11-15' because `ladedatum < Stichtag` is false)
    3.  Execute the legacy `r_ausd_geschaeftspartner.ksh` script without parameters to populate `project_id.dataset_id.legacy_fos_vertrags_cache` with the expected output. Ensure the legacy script runs on the same `CURRENT_DATE()` as the BigQuery job will use.
*   **Action**:
    Execute the BigQuery stored procedure without parameters:
    ```sql
    CALL `project_id.dataset_id.sp_initial_befuellung_vertrags_cache_fos`(NULL, NULL);
    ```
*   **Pass/Fail Criterion**:
    1.  The `project_id.dataset_id.fos_vertrags_cache` table must contain the exact same data (same rows, same values for all columns) as `project_id.dataset_id.legacy_fos_vertrags_cache`.
    2.  The `job_control` table must have one entry for the job with `status = 'SUCCESS'`.
    3.  The `job_log` table must contain `INFO` level messages indicating job start and successful completion, and the `Stichtag` logged should match `CURRENT_DATE()`.
*   **Runnable Test Code (Pytest with SQL assertions)**:
    ```python
    import pytest
    from google.cloud import bigquery
    from datetime import date

    client = bigquery.Client()
    project_id = "your-gcp-project-id"
    dataset_id = "your_dataset_id"

    def setup_tables():
        # Clear tables
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.fos_vertrags_cache`").result()
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_log`").result()
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_control`").result()
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.dwh_vertrag_cache_source`").result()
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.legacy_fos_vertrags_cache`").result()

        # Populate dwh_vertrag_cache_source
        current_date = date.today()
        source_data = [
            (1, 'V001', '2023-01-01', '2024-01-01', 100.00, '2023-10-01'), # Should be selected
            (2, 'V002', '2023-01-01', current_date.strftime('%Y-%m-%d'), 200.00, '2023-10-01'), # Not selected (Stichtag < gueltig_bis is false)
            (3, 'V003', '2023-01-01', '2024-01-01', 300.00, current_date.strftime('%Y-%m-%d')), # Not selected (ladedatum < Stichtag is false)
            (4, 'V004', '2023-01-01', '2024-01-01', 400.00, '2023-01-01'), # Should be selected
            (5, 'V005', '2023-01-01', '2023-01-01', 500.00, '2022-12-31'), # Not selected (Stichtag < gueltig_bis is false)
        ]
        insert_source_sql = f"""
            INSERT INTO `{project_id}.{dataset_id}.dwh_vertrag_cache_source`
            (dwh_vertrag_id, vertrags_nummer, gueltig_von, gueltig_bis, betrag, ladedatum) VALUES
        """ + ",".join([f"({d[0]}, '{d[1]}', '{d[2]}', '{d[3]}', {d[4]}, '{d[5]}')" for d in source_data])
        client.query(insert_source_sql).result()

        # Simulate legacy output (replace with actual legacy script execution if possible)
        # For this test, assume legacy would select 1 and 4
        insert_legacy_sql = f"""
            INSERT INTO `{project_id}.{dataset_id}.legacy_fos_vertrags_cache`
            (dwh_vertrag_id, vertrags_nummer, gueltig_von, gueltig_bis, betrag, ladedatum) VALUES
            (1, 'V001', '2023-01-01', '2024-01-01', 100.00, '{current_date.strftime('%Y-%m-%d')}'),
            (4, 'V004', '2023-01-01', '2024-01-01', 400.00, '{current_date.strftime('%Y-%m-%d')}')
        """
        client.query(insert_legacy_sql).result()

    def test_happy_path_default_stichtag():
        setup_tables()
        current_date = date.today()

        # Action: Execute the BigQuery SP
        client.query(f"CALL `{project_id}.{dataset_id}.sp_initial_befuellung_vertrags_cache_fos`(NULL, NULL)").result()

        # Pass/Fail Criterion 1: Output parity
        query_parity = f"""
            SELECT
                (SELECT COUNT(*) FROM `{project_id}.{dataset_id}.fos_vertrags_cache`) =
                (SELECT COUNT(*) FROM `{project_id}.{dataset_id}.legacy_fos_vertrags_cache`)
            AND
                (SELECT COUNT(*) FROM `{project_id}.{dataset_id}.fos_vertrags_cache` EXCEPT DISTINCT SELECT * FROM `{project_id}.{dataset_id}.legacy_fos_vertrags_cache`) = 0
            AND
                (SELECT COUNT(*) FROM `{project_id}.{dataset_id}.legacy_fos_vertrags_cache` EXCEPT DISTINCT SELECT * FROM `{project_id}.{dataset_id}.fos_vertrags_cache`) = 0
        """
        result_parity = client.query(query_parity).result().to_dataframe()
        assert result_parity.iloc[0][0] is True, "Output tables do not match legacy output."

        # Pass/Fail Criterion 2: Job control status
        query_job_control = f"""
            SELECT status, stichtag, resume_value FROM `{project_id}.{dataset_id}.job_control`
            WHERE job_kennung = 'r_ausd_geschaeftspartner' ORDER BY created_at DESC LIMIT 1
        """
        job_control_df = client.query(query_job_control).result().to_dataframe()
        assert not job_control_df.empty, "No job control entry found."
        assert job_control_df.iloc[0]['status'] == 'SUCCESS', f"Job status is not SUCCESS: {job_control_df.iloc[0]['status']}"
        assert job_control_df.iloc[0]['stichtag'] == current_date, f"Stichtag in job_control is incorrect: {job_control_df.iloc[0]['stichtag']}"
        assert job_control_df.iloc[0]['resume_value'] == 0, f"Resume value in job_control is incorrect: {job_control_df.iloc[0]['resume_value']}"

        # Pass/Fail Criterion 3: Job log messages
        query_job_log = f"""
            SELECT message, log_level FROM `{project_id}.{dataset_id}.job_log`
            WHERE job_kennung = 'r_ausd_geschaeftspartner' ORDER BY created_at
        """
        job_log_df = client.query(query_job_log).result().to_dataframe()
        assert "Job started with Stichtag:" in job_log_df.iloc[0]['message'], "Missing job start log message."
        assert f"Stichtag: {current_date.strftime('%Y-%m-%d')}" in job_log_df.iloc[0]['message'], "Incorrect Stichtag in job start log."
        assert "Job completed successfully." in job_log_df.iloc[-1]['message'], "Missing job success log message."
        assert all(log_level == 'INFO' for log_level in job_log_df['log_level']), "Unexpected log levels found."
    ```

---

## Test Case 2: Specified Stichtag and No Restart

*   **Purpose**: Verify the job correctly processes data for a specific `Stichtag` provided as input, with `Wiederanlaufwert` defaulting to `0`. This tests parameter handling and date filtering.
*   **Setup**:
    1.  Clear `project_id.dataset_id.fos_vertrags_cache`, `job_log`, and `job_control` tables.
    2.  Populate `project_id.dataset_id.dwh_vertrag_cache_source` with test data.
        *   Example `Stichtag`: `2023-06-15` (formatted as `15062023`)
        *   Data:
            *   `dwh_vertrag_id=10, vertrags_nummer='V010', gueltig_von='2023-01-01', gueltig_bis='2023-07-01', betrag=100.00, ladedatum='2023-05-01'` (Selected)
            *   `dwh_vertrag_id=11, vertrags_nummer='V011', gueltig_von='2023-06-15', gueltig_bis='2023-07-15', betrag=110.00, ladedatum='2023-06-14'` (Selected - `gueltig_von <= Stichtag`)
            *   `dwh_vertrag_id=12, vertrags_nummer='V012', gueltig_von='2023-06-16', gueltig_bis='2023-07-16', betrag=120.00, ladedatum='2023-06-01'` (Not selected - `gueltig_von <= Stichtag` is false)
            *   `dwh_vertrag_id=13, vertrags_nummer='V013', gueltig_von='2023-01-01', gueltig_bis='2023-06-15', betrag=130.00, ladedatum='2023-05-01'` (Not selected - `Stichtag < gueltig_bis` is false)
            *   `dwh_vertrag_id=14, vertrags_nummer='V014', gueltig_von='2023-01-01', gueltig_bis='2023-07-01', betrag=140.00, ladedatum='2023-06-15'` (Not selected - `ladedatum < Stichtag` is false)
    3.  Execute the legacy `r_ausd_geschaeftspartner.ksh` script with `-s 15062023` to populate `project_id.dataset_id.legacy_fos_vertrags_cache`.
*   **Action**:
    Execute the BigQuery stored procedure with a specific `Stichtag`:
    ```sql
    CALL `project_id.dataset_id.sp_initial_befuellung_vertrags_cache_fos`('15062023', NULL);
    ```
*   **Pass/Fail Criterion**:
    1.  The `project_id.dataset_id.fos_vertrags_cache` table must contain the exact same data as `project_id.dataset_id.legacy_fos_vertrags_cache`.
    2.  The `job_control` table must have one entry with `status = 'SUCCESS'` and `stichtag = '2023-06-15'`.
    3.  The `job_log` table must contain `INFO` level messages, and the `Stichtag` logged should match `2023-06-15`.
*   **Runnable Test Code (Pytest with SQL assertions)**:
    ```python
    # ... (client, project_id, dataset_id definitions) ...

    def setup_tables_specific_stichtag():
        # Clear tables
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.fos_vertrags_cache`").result()
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_log`").result()
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_control`").result()
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.dwh_vertrag_cache_source`").result()
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.legacy_fos_vertrags_cache`").result()

        specific_stichtag_date = date(2023, 6, 15)
        specific_stichtag_str = specific_stichtag_date.strftime('%Y-%m-%d')

        # Populate dwh_vertrag_cache_source
        source_data = [
            (10, 'V010', '2023-01-01', '2023-07-01', 100.00, '2023-05-01'), # Selected
            (11, 'V011', '2023-06-15', '2023-07-15', 110.00, '2023-06-14'), # Selected
            (12, 'V012', '2023-06-16', '2023-07-16', 120.00, '2023-06-01'), # Not selected (gueltig_von > Stichtag)
            (13, 'V013', '2023-01-01', '2023-06-15', 130.00, '2023-05-01'), # Not selected (Stichtag < gueltig_bis is false)
            (14, 'V014', '2023-01-01', '2023-07-01', 140.00, '2023-06-15'), # Not selected (ladedatum < Stichtag is false)
        ]
        insert_source_sql = f"""
            INSERT INTO `{project_id}.{dataset_id}.dwh_vertrag_cache_source`
            (dwh_vertrag_id, vertrags_nummer, gueltig_von, gueltig_bis, betrag, ladedatum) VALUES
        """ + ",".join([f"({d[0]}, '{d[1]}', '{d[2]}', '{d[3]}', {d[4]}, '{d[5]}')" for d in source_data])
        client.query(insert_source_sql).result()

        # Simulate legacy output (records 10 and 11 should be selected)
        insert_legacy_sql = f"""
            INSERT INTO `{project_id}.{dataset_id}.legacy_fos_vertrags_cache`
            (dwh_vertrag_id, vertrags_nummer, gueltig_von, gueltig_bis, betrag, ladedatum) VALUES
            (10, 'V010', '2023-01-01', '2023-07-01', 100.00, '{specific_stichtag_str}'),
            (11, 'V011', '2023-06-15', '2023-07-15', 110.00, '{specific_stichtag_str}')
        """
        client.query(insert_legacy_sql).result()

    def test_specific_stichtag_no_restart():
        setup_tables_specific_stichtag()
        specific_stichtag_date = date(2023, 6, 15)

        # Action: Execute the BigQuery SP
        client.query(f"CALL `{project_id}.{dataset_id}.sp_initial_befuellung_vertrags_cache_fos`('15062023', NULL)").result()

        # Pass/Fail Criterion 1: Output parity
        query_parity = f"""
            SELECT
                (SELECT COUNT(*) FROM `{project_id}.{dataset_id}.fos_vertrags_cache`) =
                (SELECT COUNT(*) FROM `{project_id}.{dataset_id}.legacy_fos_vertrags_cache`)
            AND
                (SELECT COUNT(*) FROM `{project_id}.{dataset_id}.fos_vertrags_cache` EXCEPT DISTINCT SELECT * FROM `{project_id}.{dataset_id}.legacy_fos_vertrags_cache`) = 0
            AND
                (SELECT COUNT(*) FROM `{project_id}.{dataset_id}.legacy_fos_vertrags_cache` EXCEPT DISTINCT SELECT * FROM `{project_id}.{dataset_id}.fos_vertrags_cache`) = 0
        """
        result_parity = client.query(query_parity).result().to_dataframe()
        assert result_parity.iloc[0][0] is True, "Output tables do not match legacy output for specific Stichtag."

        # Pass/Fail Criterion 2: Job control status
        query_job_control = f"""
            SELECT status, stichtag, resume_value FROM `{project_id}.{dataset_id}.job_control`
            WHERE job_kennung = 'r_ausd_geschaeftspartner' ORDER BY created_at DESC LIMIT 1
        """
        job_control_df = client.query(query_job_control).result().to_dataframe()
        assert not job_control_df.empty, "No job control entry found."
        assert job_control_df.iloc[0]['status'] == 'SUCCESS', f"Job status is not SUCCESS: {job_control_df.iloc[0]['status']}"
        assert job_control_df.iloc[0]['stichtag'] == specific_stichtag_date, f"Stichtag in job_control is incorrect: {job_control_df.iloc[0]['stichtag']}"
        assert job_control_df.iloc[0]['resume_value'] == 0, f"Resume value in job_control is incorrect: {job_control_df.iloc[0]['resume_value']}"

        # Pass/Fail Criterion 3: Job log messages
        query_job_log = f"""
            SELECT message, log_level FROM `{project_id}.{dataset_id}.job_log`
            WHERE job_kennung = 'r_ausd_geschaeftspartner' ORDER BY created_at
        """
        job_log_df = client.query(query_job_log).result().to_dataframe()
        assert "Job started with Stichtag:" in job_log_df.iloc[0]['message'], "Missing job start log message."
        assert f"Stichtag: {specific_stichtag_date.strftime('%Y-%m-%d')}" in job_log_df.iloc[0]['message'], "Incorrect Stichtag in job start log."
        assert "Job completed successfully." in job_log_df.iloc[-1]['message'], "Missing job success log message."
    ```

---

## Test Case 3: Restart Logic with Wiederanlaufwert > 0

*   **Purpose**: Verify the `Wiederanlaufwert` logic, ensuring that existing records in `fos_vertrags_cache` with `dwh_vertrag_id >= p_wiederanlaufWert` are deleted, and new records are inserted only if `dwh_vertrag_id > p_wiederanlaufWert`.
*   **Setup**:
    1.  Clear `job_log` and `job_control` tables.
    2.  Populate `project_id.dataset_id.fos_vertrags_cache` with some initial data.
        *   Example: `dwh_vertrag_id`s `1, 2, 3, 4, 5`.
    3.  Populate `project_id.dataset_id.dwh_vertrag_cache_source` with new data, some of which would be selected by the `Stichtag` and some with `dwh_vertrag_id`s both above and below the `Wiederanlaufwert`.
        *   Example `Stichtag`: `2023-06-15` (`15062023`)
        *   Example `Wiederanlaufwert`: `3`
        *   Initial `fos_vertrags_cache`: `(1, 'A', ...), (2, 'B', ...), (3, 'C', ...), (4, 'D', ...), (5, 'E', ...)`
        *   `dwh_vertrag_cache_source`:
            *   `dwh_vertrag_id=2, vertrags_nummer='V002_new', ...` (Should NOT be inserted due to `dwh_vertrag_id > Wiederanlaufwert` filter)
            *   `dwh_vertrag_id=3, vertrags_nummer='V003_new', ...` (Should NOT be inserted due to `dwh_vertrag_id > Wiederanlaufwert` filter)
            *   `dwh_vertrag_id=6, vertrags_nummer='V006', ...` (Should be inserted)
            *   `dwh_vertrag_id=7, vertrags_nummer='V007', ...` (Should be inserted)
    4.  Execute the legacy `r_ausd_geschaeftspartner.ksh` script with `-s 15062023 -l 3` after setting up the initial `fos_vertrags_cache` and `dwh_vertrag_cache_source` to populate `project_id.dataset_id.legacy_fos_vertrags_cache`.
*   **Action**:
    Execute the BigQuery stored procedure with `Stichtag` and `Wiederanlaufwert`:
    ```sql
    CALL `project_id.dataset_id.sp_initial_befuellung_vertrags_cache_fos`('15062023', 3);
    ```
*   **Pass/Fail Criterion**:
    1.  The `project_id.dataset_id.fos_vertrags_cache` table must contain the exact same data as `project_id.dataset_id.legacy_fos_vertrags_cache`. Specifically, records with `dwh_vertrag_id` 3, 4, 5 from the initial load should be gone, and only new records with `dwh_vertrag_id > 3` (and meeting date criteria) should be present, along with original records with `dwh_vertrag_id < 3`.
    2.  The `job_control` table must have one entry with `status = 'SUCCESS'` and `resume_value = 3`.
    3.  The `job_log` table must contain a message indicating deletion of records due to `Wiederanlaufwert`.
*   **Runnable Test Code (Pytest with SQL assertions)**:
    ```python
    # ... (client, project_id, dataset_id definitions) ...

    def setup_tables_restart_logic():
        # Clear tables
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.fos_vertrags_cache`").result()
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_log`").result()
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_control`").result()
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.dwh_vertrag_cache_source`").result()
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.legacy_fos_vertrags_cache`").result()

        specific_stichtag_date = date(2023, 6, 15)
        specific_stichtag_str = specific_stichtag_date.strftime('%Y-%m-%d')
        wiederanlauf_value = 3

        # Populate fos_vertrags_cache with initial data
        initial_target_data = [
            (1, 'V_init_1', '2023-01-01', '2023-07-01', 100.00, '2023-06-01'),
            (2, 'V_init_2', '2023-01-01', '2023-07-01', 200.00, '2023-06-01'),
            (3, 'V_init_3', '2023-01-01', '2023-07-01', 300.00, '2023-06-01'),
            (4, 'V_init_4', '2023-01-01', '2023-07-01', 400.00, '2023-06-01'),
            (5, 'V_init_5', '2023-01-01', '2023-07-01', 500.00, '2023-06-01'),
        ]
        insert_initial_target_sql = f"""
            INSERT INTO `{project_id}.{dataset_id}.fos_vertrags_cache`
            (dwh_vertrag_id, vertrags_nummer, gueltig_von, gueltig_bis, betrag, ladedatum) VALUES
        """ + ",".join([f"({d[0]}, '{d[1]}', '{d[2]}', '{d[3]}', {d[4]}, '{d[5]}')" for d in initial_target_data])
        client.query(insert_initial_target_sql).result()

        # Populate dwh_vertrag_cache_source with new data
        source_data = [
            (2, 'V002_new', '2023-01-01', '2023-07-01', 250.00, '2023-06-01'), # dwh_vertrag_id <= wiederanlauf_value, not inserted
            (3, 'V003_new', '2023-01-01', '2023-07-01', 350.00, '2023-06-01'), # dwh_vertrag_id <= wiederanlauf_value, not inserted
            (6, 'V006', '2023-01-01', '2023-07-01', 600.00, '2023-06-01'), # dwh_vertrag_id > wiederanlauf_value, inserted
            (7, 'V007', '2023-01-01', '2023-07-01', 700.00, '2023-06-01'), # dwh_vertrag_id > wiederanlauf_value, inserted
            (8, 'V008', '2023-01-01', '2023-07-01', 800.00, '2023-06-15'), # dwh_vertrag_id > wiederanlauf_value, but ladedatum not < Stichtag, not inserted
        ]
        insert_source_sql = f"""
            INSERT INTO `{project_id}.{dataset_id}.dwh_vertrag_cache_source`
            (dwh_vertrag_id, vertrags_nummer, gueltig_von, gueltig_bis, betrag, ladedatum) VALUES
        """ + ",".join([f"({d[0]}, '{d[1]}', '{d[2]}', '{d[3]}', {d[4]}, '{d[5]}')" for d in source_data])
        client.query(insert_source_sql).result()

        # Simulate legacy output:
        # Initial (1,2) remain. Initial (3,4,5) are deleted. New (6,7) are inserted.
        insert_legacy_sql = f"""
            INSERT INTO `{project_id}.{dataset_id}.legacy_fos_vertrags_cache`
            (dwh_vertrag_id, vertrags_nummer, gueltig_von, gueltig_bis, betrag, ladedatum) VALUES
            (1, 'V_init_1', '2023-01-01', '2023-07-01', 100.00, '2023-06-01'),
            (2, 'V_init_2', '2023-01-01', '2023-07-01', 200.00, '2023-06-01'),
            (6, 'V006', '2023-01-01', '2023-07-01', 600.00, '{specific_stichtag_str}'),
            (7, 'V007', '2023-01-01', '2023-07-01', 700.00, '{specific_stichtag_str}')
        """
        client.query(insert_legacy_sql).result()

    def test_restart_logic_wiederanlaufwert():
        setup_tables_restart_logic()
        specific_stichtag_date = date(2023, 6, 15)
        wiederanlauf_value = 3

        # Action: Execute the BigQuery SP
        client.query(f"CALL `{project_id}.{dataset_id}.sp_initial_befuellung_vertrags_cache_fos`('15062023', {wiederanlauf_value})").result()

        # Pass/Fail Criterion 1: Output parity
        query_parity = f"""
            SELECT
                (SELECT COUNT(*) FROM `{project_id}.{dataset_id}.fos_vertrags_cache`) =
                (SELECT COUNT(*) FROM `{project_id}.{dataset_id}.legacy_fos_vertrags_cache`)
            AND
                (SELECT COUNT(*) FROM `{project_id}.{dataset_id}.fos_vertrags_cache` EXCEPT DISTINCT SELECT * FROM `{project_id}.{dataset_id}.legacy_fos_vertrags_cache`) = 0
            AND
                (SELECT COUNT(*) FROM `{project_id}.{dataset_id}.legacy_fos_vertrags_cache` EXCEPT DISTINCT SELECT * FROM `{project_id}.{dataset_id}.fos_vertrags_cache`) = 0
        """
        result_parity = client.query(query_parity).result().to_dataframe()
        assert result_parity.iloc[0][0] is True, "Output tables do not match legacy output for restart logic."

        # Pass/Fail Criterion 2: Job control status
        query_job_control = f"""
            SELECT status, stichtag, resume_value FROM `{project_id}.{dataset_id}.job_control`
            WHERE job_kennung = 'r_ausd_geschaeftspartner' ORDER BY created_at DESC LIMIT 1
        """
        job_control_df = client.query(query_job_control).result().to_dataframe()
        assert not job_control_df.empty, "No job control entry found."
        assert job_control_df.iloc[0]['status'] == 'SUCCESS', f"Job status is not SUCCESS: {job_control_df.iloc[0]['status']}"
        assert job_control_df.iloc[0]['stichtag'] == specific_stichtag_date, f"Stichtag in job_control is incorrect: {job_control_df.iloc[0]['stichtag']}"
        assert job_control_df.iloc[0]['resume_value'] == wiederanlauf_value, f"Resume value in job_control is incorrect: {job_control_df.iloc[0]['resume_value']}"

        # Pass/Fail Criterion 3: Job log messages for restart
        query_job_log = f"""
            SELECT message FROM `{project_id}.{dataset_id}.job_log`
            WHERE job_kennung = 'r_ausd_geschaeftspartner' AND message LIKE '%Deleted records from fos_vertrags_cache with dwh_vertrag_id >= %'
        """
        job_log_df = client.query(query_job_log).result().to_dataframe()
        assert not job_log_df.empty, "Missing log message for restart deletion."
        assert f"Deleted records from fos_vertrags_cache with dwh_vertrag_id >= {wiederanlauf_value}" in job_log_df.iloc[0]['message'], "Incorrect restart deletion log message."
    ```

---

## Test Case 4: Transformation Correctness - Date Filtering Edge Cases

*   **Purpose**: Verify the precise behaviour of date filtering conditions: `Gueltig_von <= Stichtag < Gueltig_bis AND LADEDATUM < Stichtag`. This includes boundary conditions.
*   **Setup**:
    1.  Clear `project_id.dataset_id.fos_vertrags_cache`, `job_log`, and `job_control` tables.
    2.  Set `Stichtag` to `2023-07-01` (`01072023`).
    3.  Populate `project_id.dataset_id.dwh_vertrag_cache_source` with data specifically testing date boundaries:
        *   `dwh_vertrag_id=1, gueltig_von='2023-07-01', gueltig_bis='2023-07-02', ladedatum='2023-06-30'` (Selected: `gueltig_von == Stichtag`, `Stichtag < gueltig_bis`, `ladedatum < Stichtag`)
        *   `dwh_vertrag_id=2, gueltig_von='2023-06-30', gueltig_bis='2023-07-01', ladedatum='2023-06-29'` (Not Selected: `Stichtag < gueltig_bis` is false)
        *   `dwh_vertrag_id=3, gueltig_von='2023-07-02', gueltig_bis='2023-07-03', ladedatum='2023-07-01'` (Not Selected: `gueltig_von <= Stichtag` is false)
        *   `dwh_vertrag_id=4, gueltig_von='2023-06-01', gueltig_bis='2023-08-01', ladedatum='2023-07-01'` (Not Selected: `ladedatum < Stichtag` is false)
        *   `dwh_vertrag_id=5, gueltig_von='2023-06-01', gueltig_bis='2023-08-01', ladedatum='2023-06-30'` (Selected)
    4.  Execute the legacy `r_ausd_geschaeftspartner.ksh` script with `-s 01072023` to populate `project_id.dataset_id.legacy_fos_vertrags_cache`.
*   **Action**:
    Execute the BigQuery stored procedure:
    ```sql
    CALL `project_id.dataset_id.sp_initial_befuellung_vertrags_cache_fos`('01072023', 0);
    ```
*   **Pass/Fail Criterion**:
    1.  The `project_id.dataset_id.fos_vertrags_cache` table must contain the exact same data as `project_id.dataset_id.legacy_fos_vertrags_cache`.
    2.  The `job_control` table must show `status = 'SUCCESS'`.
    3.  The `job_log` table must show successful execution.
*   **Runnable Test Code (Pytest with SQL assertions)**:
    ```python
    # ... (client, project_id, dataset_id definitions) ...

    def setup_tables_date_edge_cases():
        # Clear tables
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.fos_vertrags_cache`").result()
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_log`").result()
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_control`").result()
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.dwh_vertrag_cache_source`").result()
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.legacy_fos_vertrags_cache`").result()

        specific_stichtag_date = date(2023, 7, 1)
        specific_stichtag_str = specific_stichtag_date.strftime('%Y-%m-%d')

        # Populate dwh_vertrag_cache_source
        source_data = [
            (1, 'V001', '2023-07-01', '2023-07-02', 100.00, '2023-06-30'), # Selected
            (2, 'V002', '2023-06-30', '2023-07-01', 200.00, '2023-06-29'), # Not Selected (Stichtag < gueltig_bis is false)
            (3, 'V003', '2023-07-02', '2023-07-03', 300.00, '2023-07-01'), # Not Selected (gueltig_von <= Stichtag is false)
            (4, 'V004', '2023-06-01', '2023-08-01', 400.00, '2023-07-01'), # Not Selected (ladedatum < Stichtag is false)
            (5, 'V005', '2023-06-01', '2023-08-01', 500.00, '2023-06-30'), # Selected
        ]
        insert_source_sql = f"""
            INSERT INTO `{project_id}.{dataset_id}.dwh_vertrag_cache_source`
            (dwh_vertrag_id, vertrags_nummer, gueltig_von, gueltig_bis, betrag, ladedatum) VALUES
        """ + ",".join([f"({d[0]}, '{d[1]}', '{d[2]}', '{d[3]}', {d[4]}, '{d[5]}')" for d in source_data])
        client.query(insert_source_sql).result()

        # Simulate legacy output (records 1 and 5 should be selected)
        insert_legacy_sql = f"""
            INSERT INTO `{project_id}.{dataset_id}.legacy_fos_vertrags_cache`
            (dwh_vertrag_id, vertrags_nummer, gueltig_von, gueltig_bis, betrag, ladedatum) VALUES
            (1, 'V001', '2023-07-01', '2023-07-02', 100.00, '{specific_stichtag_str}'),
            (5, 'V005', '2023-06-01', '2023-08-01', 500.00, '{specific_stichtag_str}')
        """
        client.query(insert_legacy_sql).result()

    def test_transformation_date_edge_cases():
        setup_tables_date_edge_cases()
        specific_stichtag_date = date(2023, 7, 1)

        # Action: Execute the BigQuery SP
        client.query(f"CALL `{project_id}.{dataset_id}.sp_initial_befuellung_vertrags_cache_fos`('01072023', 0)").result()

        # Pass/Fail Criterion 1: Output parity
        query_parity = f"""
            SELECT
                (SELECT COUNT(*) FROM `{project_id}.{dataset_id}.fos_vertrags_cache`) =
                (SELECT COUNT(*) FROM `{project_id}.{dataset_id}.legacy_fos_vertrags_cache`)
            AND
                (SELECT COUNT(*) FROM `{project_id}.{dataset_id}.fos_vertrags_cache` EXCEPT DISTINCT SELECT * FROM `{project_id}.{dataset_id}.legacy_fos_vertrags_cache`) = 0
            AND
                (SELECT COUNT(*) FROM `{project_id}.{dataset_id}.legacy_fos_vertrags_cache` EXCEPT DISTINCT SELECT * FROM `{project_id}.{dataset_id}.fos_vertrags_cache`) = 0
        """
        result_parity = client.query(query_parity).result().to_dataframe()
        assert result_parity.iloc[0][0] is True, "Output tables do not match legacy output for date edge cases."

        # Pass/Fail Criterion 2: Job control status
        query_job_control = f"""
            SELECT status, stichtag FROM `{project_id}.{dataset_id}.job_control`
            WHERE job_kennung = 'r_ausd_geschaeftspartner' ORDER BY created_at DESC LIMIT 1
        """
        job_control_df = client.query(query_job_control).result().to_dataframe()
        assert job_control_df.iloc[0]['status'] == 'SUCCESS', f"Job status is not SUCCESS: {job_control_df.iloc[0]['status']}"
        assert job_control_df.iloc[0]['stichtag'] == specific_stichtag_date, f"Stichtag in job_control is incorrect: {job_control_df.iloc[0]['stichtag']}"
    ```

---

## Test Case 5: Error Handling - Invalid Stichtag Format

*   **Purpose**: Verify that the job correctly handles an invalid `Stichtag` format, logs the error, updates `job_control` with a `FAILED` status, and raises an appropriate error.
*   **Setup**:
    1.  Clear `project_id.dataset_id.fos_vertrags_cache`, `job_log`, and `job_control` tables.
    2.  No specific data needed in `dwh_vertrag_cache_source` as the error occurs during parameter parsing.
*   **Action**:
    Execute the BigQuery stored procedure with an invalid `Stichtag` string:
    ```sql
    CALL `project_id.dataset_id.sp_initial_befuellung_vertrags_cache_fos`('2023-12-31', NULL); -- Invalid format
    ```
*   **Pass/Fail Criterion**:
    1.  The stored procedure execution must fail with an error message indicating invalid `Stichtag` format.
    2.  The `job_control` table must contain one entry with `status = 'FAILED'`. The `stichtag` column might be `NULL` or reflect the parsing attempt.
    3.  The `job_log` table must contain an `ERROR` level message detailing the invalid `Stichtag` format.
    4.  The `fos_vertrags_cache` table must remain empty (no data should be inserted).
*   **Runnable Test Code (Pytest with SQL assertions)**:
    ```python
    # ... (client, project_id, dataset_id definitions) ...

    def setup_tables_error_handling():
        # Clear tables
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.fos_vertrags_cache`").result()
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_log`").result()
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_control`").result()
        # No source data needed for this test

    def test_error_handling_invalid_stichtag():
        setup_tables_error_handling()

        # Action: Execute the BigQuery SP with invalid Stichtag
        with pytest.raises(Exception) as excinfo:
            client.query(f"CALL `{project_id}.{dataset_id}.sp_initial_befuellung_vertrags_cache_fos`('2023-12-31', NULL)").result()

        # Pass/Fail Criterion 1: Procedure fails with specific error
        assert "Invalid Stichtag format" in str(excinfo.value), "Procedure did not fail with expected invalid Stichtag error."

        # Pass/Fail Criterion 2: Job control status
        query_job_control = f"""
            SELECT status, stichtag FROM `{project_id}.{dataset_id}.job_control`
            WHERE job_kennung = 'r_ausd_geschaeftspartner' ORDER BY created_at DESC LIMIT 1
        """
        job_control_df = client.query(query_job_control).result().to_dataframe()
        assert not job_control_df.empty, "No job control entry found after error."
        assert job_control_df.iloc[0]['status'] == 'FAILED', f"Job status is not FAILED: {job_control_df.iloc[0]['status']}"
        assert job_control_df.iloc[0]['stichtag'] is None, "Stichtag in job_control should be NULL for invalid input."

        # Pass/Fail Criterion 3: Job log messages
        query_job_log = f"""
            SELECT message, log_level FROM `{project_id}.{dataset_id}.job_log`
            WHERE job_kennung = 'r_ausd_geschaeftspartner' AND log_level = 'ERROR'
            ORDER BY created_at DESC LIMIT 1
        """
        job_log_df = client.query(query_job_log).result().to_dataframe()
        assert not job_log_df.empty, "No ERROR log message found."
        assert "Invalid Stichtag format" in job_log_df.iloc[0]['message'], "Error log message does not contain expected text."

        # Pass/Fail Criterion 4: Target table remains empty
        query_target_count = f"SELECT COUNT(*) FROM `{project_id}.{dataset_id}.fos_vertrags_cache`"
        target_count = client.query(query_target_count).result().to_dataframe().iloc[0][0]
        assert target_count == 0, "Target table should be empty after failed job."
    ```

---

## Test Case 6: Data Quality - NULL Handling and Schema Integrity

*   **Purpose**: Verify that `NULL` values in `betrag` are handled correctly (i.e., preserved) and that the target table schema matches expectations.
*   **Setup**:
    1.  Clear `project_id.dataset_id.fos_vertrags_cache`, `job_log`, and `job_control` tables.
    2.  Populate `project_id.dataset_id.dwh_vertrag_cache_source` with records, including some where `betrag` is `NULL`.
        *   Example `Stichtag`: `2023-06-15` (`15062023`)
        *   Data:
            *   `dwh_vertrag_id=1, vertrags_nummer='V001', gueltig_von='2023-01-01', gueltig_bis='2023-07-01', betrag=100.00, ladedatum='2023-05-01'`
            *   `dwh_vertrag_id=2, vertrags_nummer='V002', gueltig_von='2023-01-01', gueltig_bis='2023-07-01', betrag=NULL, ladedatum='2023-05-01'`
    3.  Execute the legacy `r_ausd_geschaeftspartner.ksh` script with `-s 15062023` to populate `project_id.dataset_id.legacy_fos_vertrags_cache`.
*   **Action**:
    Execute the BigQuery stored procedure:
    ```sql
    CALL `project_id.dataset_id.sp_initial_befuellung_vertrags_cache_fos`('15062023', NULL);
    ```
*   **Pass/Fail Criterion**:
    1.  The `project_id.dataset_id.fos_vertrags_cache` table must contain the exact same data as `project_id.dataset_id.legacy_fos_vertrags_cache`, specifically verifying that `NULL` values in `betrag` are correctly transferred.
    2.  The schema of `fos_vertrags_cache` must match the defined DDL, including correct data types and nullability (e.g., `betrag` should be nullable).
*   **Runnable Test Code (Pytest with SQL assertions)**:
    ```python
    # ... (client, project_id, dataset_id definitions) ...

    def setup_tables_null_handling():
        # Clear tables
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.fos_vertrags_cache`").result()
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_log`").result()
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_control`").result()
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.dwh_vertrag_cache_source`").result()
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.legacy_fos_vertrags_cache`").result()

        specific_stichtag_date = date(2023, 6, 15)
        specific_stichtag_str = specific_stichtag_date.strftime('%Y-%m-%d')

        # Populate dwh_vertrag_cache_source with NULL betrag
        source_data = [
            (1, 'V001', '2023-01-01', '2023-07-01', 100.00, '2023-05-01'),
            (2, 'V002', '2023-01-01', '2023-07-01', None, '2023-05-01'), # NULL betrag
            (3, 'V003', '2023-01-01', '2023-07-01', 300.00, '2023-06-15'), # Not selected
        ]
        insert_source_sql = f"""
            INSERT INTO `{project_id}.{dataset_id}.dwh_vertrag_cache_source`
            (dwh_vertrag_id, vertrags_nummer, gueltig_von, gueltig_bis, betrag, ladedatum) VALUES
        """ + ",".join([f"({d[0]}, '{d[1]}', '{d[2]}', '{d[3]}', {d[4] if d[4] is not None else 'NULL'}, '{d[5]}')" for d in source_data])
        client.query(insert_source_sql).result()

        # Simulate legacy output
        insert_legacy_sql = f"""
            INSERT INTO `{project_id}.{dataset_id}.legacy_fos_vertrags_cache`
            (dwh_vertrag_id, vertrags_nummer, gueltig_von, gueltig_bis, betrag, ladedatum) VALUES
            (1, 'V001', '2023-01-01', '2023-07-01', 100.00, '{specific_stichtag_str}'),
            (2, 'V002', '2023-01-01', '2023-07-01', NULL, '{specific_stichtag_str}')
        """
        client.query(insert_legacy_sql).result()

    def test_null_handling_and_schema():
        setup_tables_null_handling()
        specific_stichtag_date = date(2023, 6, 15)

        # Action: Execute the BigQuery SP
        client.query(f"CALL `{project_id}.{dataset_id}.sp_initial_befuellung_vertrags_cache_fos`('15062023', NULL)").result()

        # Pass/Fail Criterion 1: Output parity (including NULLs)
        query_parity = f"""
            SELECT
                (SELECT COUNT(*) FROM `{project_id}.{dataset_id}.fos_vertrags_cache`) =
                (SELECT COUNT(*) FROM `{project_id}.{dataset_id}.legacy_fos_vertrags_cache`)
            AND
                (SELECT COUNT(*) FROM `{project_id}.{dataset_id}.fos_vertrags_cache` EXCEPT DISTINCT SELECT * FROM `{project_id}.{dataset_id}.legacy_fos_vertrags_cache`) = 0
            AND
                (SELECT COUNT(*) FROM `{project_id}.{dataset_id}.legacy_fos_vertrags_cache` EXCEPT DISTINCT SELECT * FROM `{project_id}.{dataset_id}.fos_vertrags_cache`) = 0
        """
        result_parity = client.query(query_parity).result().to_dataframe()
        assert result_parity.iloc[0][0] is True, "Output tables do not match legacy output, especially with NULL values."

        # Pass/Fail Criterion 2: Schema validation
        table = client.get_table(f"{project_id}.{dataset_id}.fos_vertrags_cache")
        schema_fields = {field.name: (field.field_type, field.is_nullable) for field in table.schema}
        
        expected_schema = {
            'dwh_vertrag_id': ('INT64', False),
            'vertrags_nummer': ('STRING', False),
            'gueltig_von': ('DATE', False),
            'gueltig_bis': ('DATE', False),
            'betrag': ('NUMERIC', True), # betrag should be nullable
            'ladedatum': ('DATE', False),
            'created_at': ('TIMESTAMP', True) # created_at has a default, so it's nullable
        }
        
        for field_name, (field_type, is_nullable) in expected_schema.items():
            assert field_name in schema_fields, f"Field {field_name} missing from target schema."
            assert schema_fields[field_name][0] == field_type, f"Field {field_name} has incorrect type: {schema_fields[field_name][0]} vs {field_type}"
            assert schema_fields[field_name][1] == is_nullable, f"Field {field_name} has incorrect nullability: {schema_fields[field_name][1]} vs {is_nullable}"
    ```

---

## Test Case 7: Empty Source Table

*   **Purpose**: Verify the job handles an empty source table gracefully, resulting in an empty target table and successful job completion.
*   **Setup**:
    1.  Clear `project_id.dataset_id.fos_vertrags_cache`, `job_log`, and `job_control` tables.
    2.  Ensure `project_id.dataset_id.dwh_vertrag_cache_source` is empty.
    3.  Execute the legacy `r_ausd_geschaeftspartner.ksh` script with a valid `Stichtag` against an empty source to populate `project_id.dataset_id.legacy_fos_vertrags_cache` (which should also be empty).
*   **Action**:
    Execute the BigQuery stored procedure with a valid `Stichtag`:
    ```sql
    CALL `project_id.dataset_id.sp_initial_befuellung_vertrags_cache_fos`('01012024', NULL);
    ```
*   **Pass/Fail Criterion**:
    1.  The `project_id.dataset_id.fos_vertrags_cache` table must be empty.
    2.  The `job_control` table must show `status = 'SUCCESS'`.
    3.  The `job_log` table must show successful execution.
*   **Runnable Test Code (Pytest with SQL assertions)**:
    ```python
    # ... (client, project_id, dataset_id definitions) ...

    def setup_tables_empty_source():
        # Clear tables
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.fos_vertrags_cache`").result()
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_log`").result()
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.job_control`").result()
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.dwh_vertrag_cache_source`").result() # Ensure empty
        client.query(f"TRUNCATE TABLE `{project_id}.{dataset_id}.legacy_fos_vertrags_cache`").result() # Ensure empty

        # Simulate legacy output (should be empty)
        # No insert needed for legacy_fos_vertrags_cache as it should be empty

    def test_empty_source_table():
        setup_tables_empty_source()
        specific_stichtag_date = date(2024, 1, 1)

        # Action: Execute the BigQuery SP
        client.query(f"CALL `{project_id}.{dataset_id}.sp_initial_befuellung_vertrags_cache_fos`('01012024', NULL)").result()

        # Pass/Fail Criterion 1: Target table is empty
        query_target_count = f"SELECT COUNT(*) FROM `{project_id}.{dataset_id}.fos_vertrags_cache`"
        target_count = client.query(query_target_count).result().to_dataframe().iloc[0][0]
        assert target_count == 0, "Target table should be empty when source is empty."

        # Pass/Fail Criterion 2: Job control status
        query_job_control = f"""
            SELECT status FROM `{project_id}.{dataset_id}.job_control`
            WHERE job_kennung = 'r_ausd_geschaeftspartner' ORDER BY created_at DESC LIMIT 1
        """
        job_control_df = client.query(query_job_control).result().to_dataframe()
        assert job_control_df.iloc[0]['status'] == 'SUCCESS', f"Job status is not SUCCESS: {job_control_df.iloc[0]['status']}"

        # Pass/Fail Criterion 3: Job log messages
        query_job_log = f"""
            SELECT message FROM `{project_id}.{dataset_id}.job_log`
            WHERE job_kennung = 'r_ausd_geschaeftspartner' AND message LIKE '%Job completed successfully%'
        """
        job_log_df = client.query(query_job_log).result().to_dataframe()
        assert not job_log_df.empty, "Missing job success log message."
    ```

---

These test cases provide a robust framework for validating the migration. Remember to adapt `project_id.dataset_id` placeholders and ensure your legacy test harness accurately populates `legacy_fos_vertrags_cache` for true output parity comparisons.