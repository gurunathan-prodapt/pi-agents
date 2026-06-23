The migration of `k_ausd_geschaeftspartner.ksh` to BigQuery involves a significant shift from shell scripting and Oracle SQL*Plus to BigQuery Stored Procedures and SQL. The validation tests below aim to ensure that this migration maintains behavioral equivalence across all critical aspects.

---

## Migration Validation Tests for `k_ausd_geschaeftspartner.ksh`

### Common Setup for All Tests

Before running any tests, the following setup is required:

1.  **BigQuery Project and Dataset:** Ensure a BigQuery project and dataset (`project.dataset`) are available for testing.
2.  **Table Creation:** All source, intermediate, and target tables referenced in the BigQuery Stored Procedures must be created with their respective schemas.
    *   `project.dataset.job_log` (DDL provided in migration design)
    *   **Source Tables (Example DDLs - actual types/lengths should match legacy):**
        ```sql
        CREATE TABLE IF NOT EXISTS `project.dataset.bpd_ta_bp_valueseg_assoc` (BP_ID STRING, SEGMENT_ID INT64);
        CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_e_reach_gp` (cntrct_cp2_id STRING, for_the_attention_of STRING, address_attachment STRING, corp_unit STRING, surname_s STRING, first_name_g STRING, land_sd STRING, zip_code STRING, city STRING, street STRING, house_nr STRING, pobox STRING, address_attachment_org STRING, bp_id STRING);
        CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_e_business_gp` (bp_id STRING, organisation_name STRING, title STRING, surname STRING, first_name STRING, tm_customerid STRING, sales_tax_freed STRING);
        CREATE TABLE IF NOT EXISTS `project.dataset.pds_ta_bpri_com` (cntrct_id STRING, bpr_id INT64, bpri_com_id STRING, cntrct_id_ref STRING, valid_from DATE, valid_to DATE, modified_at DATE, insert_at DATE, is_production INT64);
        CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_e_reach_dn` (for_the_attention_of STRING, address_attachment STRING, corp_unit STRING, surname_s STRING, first_name_g STRING, land_sd STRING, zip_code STRING, city STRING, street STRING, house_nr STRING, pobox STRING, address_attachment_org STRING, bp_id STRING, bpr_inst_srvusr_id STRING);
        CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_e_business_dn` (bp_id STRING, organisation_name STRING, title STRING, surname STRING, first_name STRING, sales_tax_freed STRING);
        CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_e_reach_ev` (for_the_attention_of STRING, address_attachment STRING, corp_unit STRING, surname_s STRING, first_name_g STRING, land_sd STRING, zip_code STRING, city STRING, street STRING, house_nr STRING, pobox STRING, address_attachment_org STRING, bp_id STRING, bpr_inst_evnrec_id STRING);
        CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_e_business_ev` (bp_id STRING, organisation_name STRING, title STRING, surname STRING, first_name STRING, sales_tax_freed STRING);
        ```
    *   **Intermediate/Target Tables (DDLs provided in migration design or derived):**
        ```sql
        CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_segm_prem` (BP_ID STRING, SEGMENT_ID INT64);
        CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_bpr_dn_evn` (CNTRCT_ID STRING, BPR_ID INT64, BPR_INSTANCE_ID STRING, CNTRCT_ID_REF STRING, COLUMN_5VALID_TO DATE);
        CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_bpr_dn_evn_his` (CNTRCT_ID STRING, BPR_ID INT64, BPRI_COM_ID STRING, CNTRCT_ID_REF STRING, VALID_FROM DATE, VALID_TO DATE, MODIFIED_AT DATE, INSERT_AT DATE);
        CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_p_gesch_part` (CNTRCT_ID STRING, NAMENSZUSATZ STRING, ADRESSZUSATZ STRING, FIRMENNAME STRING, AKAD_TITEL STRING, NACHNAME STRING, VORNAME STRING, LAND STRING, PLZ STRING, WOHNORT STRING, STRASSE STRING, KUNDE_SEGMENT_ID STRING, PREM_SEGMENT_ID INT64, TM_KUNDENNUMMER STRING, MWST_KENNZEICHEN STRING, ORGANISATIONSEINHEIT STRING);
        CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_p_dn_nutzer` (CNTRCT_ID STRING, NAMENSZUSATZ STRING, ADRESSZUSATZ STRING, FIRMENNAME STRING, AKAD_TITEL STRING, NACHNAME STRING, VORNAME STRING, LAND STRING, PLZ STRING, WOHNORT STRING, STRASSE STRING, ORGANISATIONSEINHEIT STRING, MWST_KENNZEICHEN STRING);
        CREATE TABLE IF NOT EXISTS `project.dataset.sof_ta_p_evn_empf` (CNTRCT_ID STRING, NAMENSZUSATZ STRING, ADRESSZUSATZ STRING, FIRMENNAME STRING, AKAD_TITEL STRING, NACHNAME STRING, VORNAME STRING, LAND STRING, PLZ STRING, WOHNORT STRING, STRASSE STRING, ORGANISATIONSEINHEIT STRING, MWST_KENNZEICHEN STRING);
        ```
3.  **Stored Procedure Deployment:** Deploy `project.dataset.d_ausd_geschaeftspartner_sp` and `project.dataset.r_ausd_vertrag_control` to the BigQuery dataset.
4.  **Legacy Data Snapshot:** For output parity tests, ensure a mechanism to capture the exact state of all relevant Oracle tables (source and target) after a legacy job run. This can be CSV exports, database dumps, or direct queries.

### Test Case 1: Successful Execution - Output Parity & Transformation Correctness (Happy Path)

*   **Purpose:** Verify that with valid and representative inputs, the migrated job produces identical data in all target tables and the correct record count, matching the legacy system's output. This covers the core transformation logic, joins, aggregations, and data flow.
*   **Setup:**
    1.  Populate all source tables (`bpd_ta_bp_valueseg_assoc`, `sof_ta_e_reach_gp`, `sof_ta_e_business_gp`, `pds_ta_bpri_com`, `sof_ta_e_reach_dn`, `sof_ta_e_business_dn`, `sof_ta_e_reach_ev`, `sof_ta_e_business_ev`) with a diverse set of valid data. Include various scenarios for `COALESCE`, `CASE` statements, and date filters.
    2.  Clear all target tables (`sof_ta_segm_prem`, `sof_ta_bpr_dn_evn`, `sof_ta_bpr_dn_evn_his`, `sof_ta_p_gesch_part`, `sof_ta_p_dn_nutzer`, `sof_ta_p_evn_empf`) and `job_log`.
    3.  Capture the expected output data for all target tables and the final record count from a successful run of the **legacy** `k_ausd_geschaeftspartner.ksh` with the same input data and parameters. Store this as `expected_legacy_output_data`.
*   **Action:**
    1.  Execute the migrated control procedure:
        ```sql
        CALL `project.dataset.r_ausd_vertrag_control`(
            'TEST_JOB_HAPPY',
            '12345',
            '01012023', -- p_Stichtag in DDMMYYYY
            0           -- p_wiederanlaufWert
        );
        ```
    2.  Query all target tables and the `job_log` table.
*   **Pass/Fail Criterion:**
    1.  **Output Parity:** The data in `project.dataset.sof_ta_p_gesch_part`, `project.dataset.sof_ta_p_dn_nutzer`, and `project.dataset.sof_ta_p_evn_empf` must be identical (row-by-row and column-by-column) to the `expected_legacy_output_data`.
    2.  **Record Count:** The `record_count` in the `job_log` entry for `TEST_JOB_HAPPY` must match the record count captured from the legacy `tmpFile` or the main output table.
    3.  **Job Status:** The `job_log` entry for `TEST_JOB_HAPPY` must have `status = 'COMPLETED'` and a descriptive success `message`.
    4.  **Intermediate Tables:** (Optional but recommended for detailed debugging) Data in `project.dataset.sof_ta_segm_prem`, `project.dataset.sof_ta_bpr_dn_evn_his`, and `project.dataset.sof_ta_bpr_dn_evn` should also match their legacy counterparts if they were captured.

    ```python
    import pytest
    from google.cloud import bigquery

    # Assume bigquery_client is initialized and points to the test project/dataset
    # Assume expected_legacy_output_data is loaded from a file or database
    # Example: expected_legacy_output_data = {
    #     'sof_ta_p_gesch_part': [{'CNTRCT_ID': 'C1', ...}],
    #     'sof_ta_p_dn_nutzer': [...],
    #     'sof_ta_p_evn_empf': [...],
    #     'record_count': 100
    # }

    @pytest.fixture(scope="module")
    def bigquery_client():
        return bigquery.Client()

    @pytest.fixture(autouse=True)
    def cleanup_tables(bigquery_client):
        # Truncate all relevant tables before each test
        tables_to_clear = [
            "`project.dataset.job_log`",
            "`project.dataset.sof_ta_segm_prem`",
            "`project.dataset.sof_ta_bpr_dn_evn`",
            "`project.dataset.sof_ta_bpr_dn_evn_his`",
            "`project.dataset.sof_ta_p_gesch_part`",
            "`project.dataset.sof_ta_p_dn_nutzer`",
            "`project.dataset.sof_ta_p_evn_empf`",
            # Add all source tables if they are modified by tests or need fresh data
        ]
        for table in tables_to_clear:
            bigquery_client.query(f"TRUNCATE TABLE {table}").result()
        yield

    def test_happy_path_output_parity(bigquery_client, expected_legacy_output_data):
        # 1. Populate source tables with test data
        # Example:
        bigquery_client.query("""
            INSERT INTO `project.dataset.bpd_ta_bp_valueseg_assoc` (BP_ID, SEGMENT_ID) VALUES ('BP1', 11), ('BP2', 15);
            INSERT INTO `project.dataset.sof_ta_e_reach_gp` (cntrct_cp2_id, bp_id, corp_unit, surname_s, first_name_g, street, house_nr, pobox) VALUES ('C1', 'BP1', 'Corp A', 'Doe', 'John', 'Main St', '10', NULL);
            INSERT INTO `project.dataset.sof_ta_e_business_gp` (bp_id, organisation_name, surname, first_name) VALUES ('BP1', 'Org A', 'D', 'J');
            -- ... populate all other source tables similarly
        """).result()

        # 2. Execute the migrated SP
        bigquery_client.query("""
            CALL `project.dataset.r_ausd_vertrag_control`('TEST_JOB_HAPPY', '12345', '01012023', 0);
        """).result()

        # 3. Assertions
        # Fetch actual results from target tables
        query_job_part = bigquery_client.query("SELECT * FROM `project.dataset.sof_ta_p_gesch_part` ORDER BY CNTRCT_ID")
        actual_gesch_part = [dict(row) for row in query_job_part.result()]

        query_job_log = bigquery_client.query("SELECT status, record_count, message FROM `project.dataset.job_log` WHERE job_kennung = 'TEST_JOB_HAPPY'")
        job_log_entry = [dict(row) for row in query_job_log.result()]

        assert len(job_log_entry) == 1
        assert job_log_entry[0]['status'] == 'COMPLETED'
        assert job_log_entry[0]['record_count'] == expected_legacy_output_data['record_count']
        assert "Job completed successfully" in job_log_entry[0]['message']

        # Deep comparison of data (requires careful handling of data types and order)
        # For simplicity, assuming expected_legacy_output_data['sof_ta_p_gesch_part'] is sorted
        assert actual_gesch_part == expected_legacy_output_data['sof_ta_p_gesch_part']
        # ... repeat for other target tables
    ```

### Test Case 2: Parameter Validation - Missing `JobKennung`

*   **Purpose:** Verify that the job fails gracefully when `p_JobKennung` is missing or empty, matching legacy behavior (`pruefeParameterGesetzt Jobkennung p_JobKennung`).
*   **Setup:** Clear `job_log` and target tables.
*   **Action:** Attempt to call `r_ausd_vertrag_control` with `p_JobKennung` as `NULL` or an empty string.
    ```sql
    -- Attempt 1: NULL JobKennung
    CALL `project.dataset.r_ausd_vertrag_control`(
        NULL,
        '12345',
        '01012023',
        0
    );

    -- Attempt 2: Empty string JobKennung
    CALL `project.dataset.r_ausd_vertrag_control`(
        '',
        '12345',
        '01012023',
        0
    );
    ```
*   **Pass/Fail Criterion:**
    1.  Both calls must fail with a BigQuery error (e.g., `Bad request: Invalid procedure call: JobKennung parameter is missing or empty.`).
    2.  No data should be inserted into any target tables.
    3.  No entry (or a minimal, failed entry if the error handling allowed it) should be present in `project.dataset.job_log` for these specific calls, as the `SIGNAL SQLSTATE` occurs before the initial `INSERT` into `job_log`.

    ```python
    import pytest
    from google.cloud import bigquery

    def test_missing_jobkennung_parameter(bigquery_client):
        with pytest.raises(bigquery.exceptions.BadRequest) as excinfo:
            bigquery_client.query("CALL `project.dataset.r_ausd_vertrag_control`(NULL, '12345', '01012023', 0);").result()
        assert "JobKennung parameter is missing or empty." in str(excinfo.value)

        with pytest.raises(bigquery.exceptions.BadRequest) as excinfo:
            bigquery_client.query("CALL `project.dataset.r_ausd_vertrag_control`('', '12345', '01012023', 0);").result()
        assert "JobKennung parameter is missing or empty." in str(excinfo.value)

        # Verify no job_log entry was created for these failed attempts
        query_job_log = bigquery_client.query("SELECT COUNT(*) FROM `project.dataset.job_log` WHERE job_kennung IS NULL OR job_kennung = ''")
        assert query_job_log.result().to_dataframe().iloc[0, 0] == 0
    ```

### Test Case 3: Parameter Validation - Missing `Stichtag`

*   **Purpose:** Verify that the job fails gracefully when `p_Stichtag` is missing or empty, matching legacy behavior (`pruefeParameterGesetzt Stichtag p_Stichtag`).
*   **Setup:** Clear `job_log` and target tables.
*   **Action:** Attempt to call `r_ausd_vertrag_control` with `p_Stichtag` as `NULL` or an empty string.
    ```sql
    CALL `project.dataset.r_ausd_vertrag_control`(
        'TEST_JOB_MISSING_STICHTAG',
        '12345',
        NULL, -- p_Stichtag
        0
    );
    ```
*   **Pass/Fail Criterion:**
    1.  The call must fail with a BigQuery error indicating missing `Stichtag`.
    2.  No data should be inserted into any target tables.
    3.  No entry should be present in `project.dataset.job_log` for this specific call.

    ```python
    import pytest
    from google.cloud import bigquery

    def test_missing_stichtag_parameter(bigquery_client):
        with pytest.raises(bigquery.exceptions.BadRequest) as excinfo:
            bigquery_client.query("CALL `project.dataset.r_ausd_vertrag_control`('TEST_JOB_MISSING_STICHTAG', '12345', NULL, 0);").result()
        assert "Stichtag parameter is missing or empty." in str(excinfo.value)

        with pytest.raises(bigquery.exceptions.BadRequest) as excinfo:
            bigquery_client.query("CALL `project.dataset.r_ausd_vertrag_control`('TEST_JOB_MISSING_STICHTAG', '12345', '', 0);").result()
        assert "Stichtag parameter is missing or empty." in str(excinfo.value)

        query_job_log = bigquery_client.query("SELECT COUNT(*) FROM `project.dataset.job_log` WHERE job_kennung = 'TEST_JOB_MISSING_STICHTAG'")
        assert query_job_log.result().to_dataframe().iloc[0, 0] == 0
    ```

### Test Case 4: Parameter Validation - Invalid `Stichtag` Format

*   **Purpose:** Verify that the job fails gracefully when `p_Stichtag` is in an invalid `DDMMYYYY` format, matching legacy behavior (`DWDate_Datum_Check`).
*   **Setup:** Clear `job_log` and target tables.
*   **Action:** Call `r_ausd_vertrag_control` with `p_Stichtag` in an incorrect format (e.g., 'YYYY-MM-DD', 'DD.MM.YYYY', '20230101').
    ```sql
    CALL `project.dataset.r_ausd_vertrag_control`(
        'TEST_JOB_INVALID_STICHTAG',
        '12345',
        '2023-01-01', -- Invalid format
        0
    );
    ```
*   **Pass/Fail Criterion:**
    1.  The call must fail with a BigQuery error indicating an invalid `Stichtag` date format.
    2.  No data should be inserted into any target tables.
    3.  No entry should be present in `project.dataset.job_log` for this specific call, as the error occurs during `PARSE_DATE` before the initial `INSERT`.

    ```python
    import pytest
    from google.cloud import bigquery

    def test_invalid_stichtag_format(bigquery_client):
        invalid_dates = ['2023-01-01', '01.01.2023', '20230101', '32012023'] # DDMMYYYY, 32 is invalid day
        for invalid_date in invalid_dates:
            with pytest.raises(bigquery.exceptions.BadRequest) as excinfo:
                bigquery_client.query(f"CALL `project.dataset.r_ausd_vertrag_control`('TEST_JOB_INVALID_STICHTAG', '12345', '{invalid_date}', 0);").result()
            assert "Invalid Stichtag date format" in str(excinfo.value)

        query_job_log = bigquery_client.query("SELECT COUNT(*) FROM `project.dataset.job_log` WHERE job_kennung = 'TEST_JOB_INVALID_STICHTAG'")
        assert query_job_log.result().to_dataframe().iloc[0, 0] == 0
    ```

### Test Case 5: Active Job Handling (Concurrency/Idempotency)

*   **Purpose:** Verify that the migrated job correctly identifies and skips execution if an identical job (same `JobKennung`, `EintragsNr`, `Stichtag`) is already marked as 'STARTED' in `job_log`. This replaces the commented-out `FOSJobDeaktivate` and implicit "ignore active jobs" logic.
*   **Setup:**
    1.  Clear `job_log` and target tables.
    2.  Insert a 'STARTED' entry into `project.dataset.job_log` for a specific `JobKennung`, `EintragsNr`, `Stichtag`.
        ```sql
        INSERT INTO `project.dataset.job_log`
        (job_kennung, eintrags_nr, tab_name, stichtag, status, message)
        VALUES
        ('TEST_JOB_ACTIVE', '67890', 'sof_ta_p_gesch_part', '2023-01-01', 'STARTED', 'Initial start of job');
        ```
*   **Action:** Call `r_ausd_vertrag_control` with the same `JobKennung`, `EintragsNr`, `Stichtag`.
    ```sql
    CALL `project.dataset.r_ausd_vertrag_control`(
        'TEST_JOB_ACTIVE',
        '67890',
        '01012023', -- Same Stichtag as the 'STARTED' entry
        0
    );
    ```
*   **Pass/Fail Criterion:**
    1.  The call must complete successfully (not raise an error).
    2.  A new entry must be added to `project.dataset.job_log` with `status = 'SKIPPED'` and a message indicating that the job was skipped due to an active instance.
    3.  The original 'STARTED' entry in `job_log` must remain unchanged.
    4.  No data should be inserted into any of the core target tables (`sof_ta_p_gesch_part`, etc.) by this skipped run.

    ```python
    import pytest
    from google.cloud import bigquery

    def test_active_job_handling(bigquery_client):
        # Setup: Insert a 'STARTED' job_log entry
        bigquery_client.query("""
            INSERT INTO `project.dataset.job_log`
            (job_kennung, eintrags_nr, tab_name, stichtag, status, message)
            VALUES
            ('TEST_JOB_ACTIVE', '67890', 'sof_ta_p_gesch_part', '2023-01-01', 'STARTED', 'Initial start of job');
        """).result()

        # Action: Call the SP with same parameters
        bigquery_client.query("CALL `project.dataset.r_ausd_vertrag_control`('TEST_JOB_ACTIVE', '67890', '01012023', 0);").result()

        # Assertions
        query_job_log = bigquery_client.query("""
            SELECT status, message FROM `project.dataset.job_log`
            WHERE job_kennung = 'TEST_JOB_ACTIVE' AND eintrags_nr = '67890' AND stichtag = '2023-01-01'
            ORDER BY created_at
        """)
        log_entries = [dict(row) for row in query_job_log.result()]

        assert len(log_entries) == 2 # Original 'STARTED' and new 'SKIPPED'
        assert log_entries[0]['status'] == 'STARTED'
        assert log_entries[1]['status'] == 'SKIPPED'
        assert "is already active. Skipping execution." in log_entries[1]['message']

        # Verify no data was processed by checking a target table
        query_target_table = bigquery_client.query("SELECT COUNT(*) FROM `project.dataset.sof_ta_p_gesch_part`")
        assert query_target_table.result().to_dataframe().iloc[0, 0] == 0
    ```

### Test Case 6: Error Handling during `d_ausd_geschaeftspartner_sp` Execution

*   **Purpose:** Verify that errors occurring within the core data transformation (`d_ausd_geschaeftspartner_sp`) are caught by the orchestrator, logged as 'FAILED', and re-raised to the caller.
*   **Setup:**
    1.  Clear `job_log` and target tables.
    2.  Temporarily modify `project.dataset.d_ausd_geschaeftspartner_sp` to introduce a deliberate error (e.g., `SELECT 1/0;` or reference a non-existent table). *Alternatively, populate source data in a way that causes a known error, e.g., an invalid cast if a column was not correctly typed.*
*   **Action:** Call `r_ausd_vertrag_control` with valid parameters.
    ```sql
    CALL `project.dataset.r_ausd_vertrag_control`(
        'TEST_JOB_FAIL',
        '99999',
        '01012023',
        0
    );
    ```
*   **Pass/Fail Criterion:**
    1.  The call to `r_ausd_vertrag_control` must fail and raise a BigQuery error.
    2.  The `job_log` entry for `TEST_JOB_FAIL` must exist, have `status = 'FAILED'`, and its `message` should contain details about the error from `d_ausd_geschaeftspartner_sp`.
    3.  Any partial data written by `d_ausd_geschaeftspartner_sp` before the error should ideally be rolled back (BigQuery SPs are transactional by default).

    ```python
    import pytest
    from google.cloud import bigquery

    def test_error_during_transformation(bigquery_client):
        # Setup: Temporarily modify d_ausd_geschaeftspartner_sp to cause an error
        # This is a conceptual step. In a real scenario, you might deploy a test version
        # of the SP or use data that triggers an error.
        # Example:
        # bigquery_client.query("ALTER PROCEDURE `project.dataset.d_ausd_geschaeftspartner_sp`(...) BEGIN SELECT 1/0; END;").result()

        # Action: Call the orchestrator SP
        with pytest.raises(bigquery.exceptions.BadRequest) as excinfo:
            bigquery_client.query("CALL `project.dataset.r_ausd_vertrag_control`('TEST_JOB_FAIL', '99999', '01012023', 0);").result()

        # Assertions
        assert "Job failed during processing" in str(excinfo.value)

        query_job_log = bigquery_client.query("""
            SELECT status, message FROM `project.dataset.job_log`
            WHERE job_kennung = 'TEST_JOB_FAIL' AND eintrags_nr = '99999' AND stichtag = '2023-01-01'
        """)
        log_entry = [dict(row) for row in query_job_log.result()]

        assert len(log_entry) == 1
        assert log_entry[0]['status'] == 'FAILED'
        assert "Job failed during processing" in log_entry[0]['message']
        # Further assert that the message contains details of the specific error (e.g., "division by zero")
    ```

### Test Case 7: Data Quality - NULL Handling & Default Values

*   **Purpose:** Verify that `NULL` values in source columns are handled correctly by `COALESCE` and `CASE` statements, and that default values (e.g., empty strings, 'Postfach' prefix) are applied as expected in the target tables.
*   **Setup:**
    1.  Clear all tables.
    2.  Populate source tables with data specifically designed to test NULL scenarios:
        *   `sof_ta_e_reach_gp`: `corp_unit` is NULL, `surname_s` is NULL, `first_name_g` is NULL, `street` is NULL but `pobox` is present, both `street` and `pobox` are NULL.
        *   `sof_ta_e_business_gp`: `organisation_name`, `surname`, `first_name` are populated to be picked up by `COALESCE`.
        *   `pds_ta_bpri_com`: `valid_to` is NULL.
*   **Action:** Run `r_ausd_vertrag_control` with valid parameters.
*   **Pass/Fail Criterion:**
    1.  Query the target tables (`sof_ta_p_gesch_part`, `sof_ta_p_dn_nutzer`, `sof_ta_p_evn_empf`, `sof_ta_bpr_dn_evn`) and assert that:
        *   `FIRMENNAME` correctly uses `organisation_name` when `corp_unit` is NULL.
        *   `AKAD_TITEL` is an empty string when `surname_s` is NULL.
        *   `NACHNAME` and `VORNAME` use `bp.surname`/`bp.first_name` when `rg.surname_s`/`rg.first_name_g` are NULL.
        *   `STRASSE` correctly forms 'Postfach ' + `pobox` when `street` is NULL, or is an empty string if both `street` and `pobox` are NULL.
        *   `COLUMN_5VALID_TO` in `sof_ta_bpr_dn_evn` correctly defaults to `DATE '4712-12-31'` when `valid_to` was NULL in `pds_ta_bpri_com`.

    ```python
    import pytest
    from google.cloud import bigquery

    def test_null_handling_and_defaults(bigquery_client):
        # Setup: Populate source tables with NULLs and specific values
        bigquery_client.query("""
            INSERT INTO `project.dataset.bpd_ta_bp_valueseg_assoc` (BP_ID, SEGMENT_ID) VALUES ('BP_NULL_TEST', 11);
            INSERT INTO `project.dataset.sof_ta_e_reach_gp` (cntrct_cp2_id, bp_id, corp_unit, surname_s, first_name_g, street, house_nr, pobox, address_attachment_org) VALUES
                ('C_NULL1', 'BP_NULL_TEST', NULL, NULL, NULL, NULL, NULL, '123', 'Org1'), -- Street NULL, Pobox present
                ('C_NULL2', 'BP_NULL_TEST', NULL, 'S_NULL2', 'F_NULL2', 'Street2', '20', NULL, 'Org2'), -- Corp_unit NULL
                ('C_NULL3', 'BP_NULL_TEST', 'Corp3', NULL, NULL, NULL, NULL, NULL, 'Org3'); -- Street, Pobox, Surname_s, First_name_g NULL
            INSERT INTO `project.dataset.sof_ta_e_business_gp` (bp_id, organisation_name, title, surname, first_name) VALUES
                ('BP_NULL_TEST', 'OrgName_Fallback', 'Mr', 'Surname_Fallback', 'Firstname_Fallback');
            INSERT INTO `project.dataset.pds_ta_bpri_com` (cntrct_id, bpr_id, bpri_com_id, cntrct_id_ref, valid_from, valid_to, modified_at, insert_at, is_production) VALUES
                ('C_VALID_TO_NULL', 31, 'BPRICOM1', 'REF1', '2023-01-01', NULL, NULL, '2023-01-01', 1);
            -- ... populate other source tables as needed for full coverage
        """).result()

        # Action: Execute the migrated SP
        bigquery_client.query("CALL `project.dataset.r_ausd_vertrag_control`('TEST_NULL_HANDLING', '11111', '01012023', 0);").result()

        # Assertions for sof_ta_p_gesch_part
        query_gesch_part = bigquery_client.query("""
            SELECT CNTRCT_ID, FIRMENNAME, AKAD_TITEL, NACHNAME, VORNAME, STRASSE
            FROM `project.dataset.sof_ta_p_gesch_part` WHERE CNTRCT_ID LIKE 'C_NULL%' ORDER BY CNTRCT_ID
        """)
        results_gesch_part = [dict(row) for row in query_gesch_part.result()]

        assert len(results_gesch_part) == 3
        assert results_gesch_part[0]['CNTRCT_ID'] == 'C_NULL1'
        assert results_gesch_part[0]['FIRMENNAME'] == 'OrgName_Fallback' # corp_unit was NULL
        assert results_gesch_part[0]['AKAD_TITEL'] == '' # surname_s was NULL
        assert results_gesch_part[0]['NACHNAME'] == 'Surname_Fallback' # surname_s was NULL
        assert results_gesch_part[0]['VORNAME'] == 'Firstname_Fallback' # first_name_g was NULL
        assert results_gesch_part[0]['STRASSE'] == 'Postfach 123' # street NULL, pobox present

        assert results_gesch_part[1]['CNTRCT_ID'] == 'C_NULL2'
        assert results_gesch_part[1]['FIRMENNAME'] == 'OrgName_Fallback' # corp_unit was NULL
        assert results_gesch_part[1]['AKAD_TITEL'] == '' # surname_s was NOT NULL, so title is empty
        assert results_gesch_part[1]['NACHNAME'] == 'S_NULL2'
        assert results_gesch_part[1]['VORNAME'] == 'F_NULL2'
        assert results_gesch_part[1]['STRASSE'] == 'Street2 20'

        assert results_gesch_part[2]['CNTRCT_ID'] == 'C_NULL3'
        assert results_gesch_part[2]['FIRMENNAME'] == 'Corp3' # corp_unit was NOT NULL
        assert results_gesch_part[2]['AKAD_TITEL'] == '' # surname_s was NULL
        assert results_gesch_part[2]['NACHNAME'] == 'Surname_Fallback' # surname_s was NULL
        assert results_gesch_part[2]['VORNAME'] == 'Firstname_Fallback' # first_name_g was NULL
        assert results_gesch_part[2]['STRASSE'] == '' # street and pobox were NULL

        # Assertions for sof_ta_bpr_dn_evn (COLUMN_5VALID_TO)
        query_bpr_dn_evn = bigquery_client.query("""
            SELECT CNTRCT_ID, COLUMN_5VALID_TO FROM `project.dataset.sof_ta_bpr_dn_evn`
            WHERE CNTRCT_ID = 'C_VALID_TO_NULL'
        """)
        results_bpr_dn_evn = [dict(row) for row in query_bpr_dn_evn.result()]
        assert len(results_bpr_dn_evn) == 1
        assert results_bpr_dn_evn[0]['COLUMN_5VALID_TO'] == '4712-12-31' # COALESCE(bp.valid_to, PARSE_DATE('%Y%m%d', '47121231'))
    ```

### Test Case 8: Transformation Correctness - Date Filtering in `sof_ta_bpr_dn_evn_his`

*   **Purpose:** Verify the complex date filtering logic (`insert_at <= v_datum` AND (`modified_at IS NULL` OR `modified_at > v_datum`) AND `valid_from <= v_datum` AND `is_production = 1`) is correctly applied when populating `sof_ta_bpr_dn_evn_his`.
*   **Setup:**
    1.  Clear all tables.
    2.  Populate `pds_ta_bpri_com` with various combinations of `insert_at`, `modified_at`, `valid_from`, and `is_production` dates, some within the `stichtag` range, some outside.
        *   Record 1: All dates match `stichtag`, `is_production = 1` (should be included)
        *   Record 2: `insert_at` > `stichtag` (should be excluded)
        *   Record 3: `modified_at` <= `stichtag` (should be excluded)
        *   Record 4: `valid_from` > `stichtag` (should be excluded)
        *   Record 5: `is_production = 0` (should be excluded)
        *   Record 6: `modified_at` IS NULL, other dates match `stichtag`, `is_production = 1` (should be included)
*   **Action:** Run `r_ausd_vertrag_control` with a specific `p_Stichtag` (e.g., '15012023').
*   **Pass/Fail Criterion:**
    1.  Query `project.dataset.sof_ta_bpr_dn_evn_his`.
    2.  Only records matching the specified date criteria and `is_production = 1` should be present. The count and content of these records must be as expected.

    ```python
    import pytest
    from google.cloud import bigquery

    def test_date_filtering_bpr_dn_evn_his(bigquery_client):
        stichtag_str = '15012023'
        stichtag_date = '2023-01-15'

        # Setup: Populate pds_ta_bpri_com with various date scenarios
        bigquery_client.query(f"""
            INSERT INTO `project.dataset.pds_ta_bpri_com` (cntrct_id, bpr_id, bpri_com_id, cntrct_id_ref, valid_from, valid_to, modified_at, insert_at, is_production) VALUES
            ('C_INCL_1', 31, 'BPRICOM_1', 'REF_1', '2023-01-10', NULL, NULL, '2023-01-12', 1), -- Included: all conditions met
            ('C_INCL_2', 31, 'BPRICOM_2', 'REF_2', '2023-01-01', NULL, '2023-01-20', '2023-01-05', 1), -- Included: modified_at > stichtag
            ('C_EXCL_1', 31, 'BPRICOM_3', 'REF_3', '2023-01-10', NULL, NULL, '2023-01-16', 1), -- Excluded: insert_at > stichtag
            ('C_EXCL_2', 31, 'BPRICOM_4', 'REF_4', '2023-01-01', NULL, '2023-01-10', '2023-01-05', 1), -- Excluded: modified_at <= stichtag
            ('C_EXCL_3', 31, 'BPRICOM_5', 'REF_5', '2023-01-20', NULL, NULL, '2023-01-10', 1), -- Excluded: valid_from > stichtag
            ('C_EXCL_4', 31, 'BPRICOM_6', 'REF_6', '2023-01-10', NULL, NULL, '2023-01-12', 0); -- Excluded: is_production = 0
        """).result()

        # Action: Execute the migrated SP
        bigquery_client.query(f"CALL `project.dataset.r_ausd_vertrag_control`('TEST_DATE_FILTER', '22222', '{stichtag_str}', 0);").result()

        # Assertions
        query_his = bigquery_client.query("SELECT cntrct_id FROM `project.dataset.sof_ta_bpr_dn_evn_his` ORDER BY cntrct_id")
        actual_ids = [row['cntrct_id'] for row in query_his.result()]

        expected_ids = ['C_INCL_1', 'C_INCL_2']
        assert sorted(actual_ids) == sorted(expected_ids)
    ```

### Test Case 9: Transformation Correctness - `MAX(COALESCE(valid_to, '47121231')) OVER (PARTITION BY ...)`

*   **Purpose:** Verify the window function logic for selecting the latest valid record in `sof_ta_bpr_dn_evn` is correct, handling `NULL` `valid_to` values as the maximum possible date.
*   **Setup:**
    1.  Clear all tables.
    2.  Populate `sof_ta_bpr_dn_evn_his` with multiple records for the same `cntrct_id`, `bpr_id` but with different `valid_to` dates (including `NULL`s) to simulate historical changes.
        *   Group 1: `valid_to` dates: '2023-01-01', '2023-01-10', '2023-01-05' -> Max should be '2023-01-10'
        *   Group 2: `valid_to` dates: '2023-01-01', NULL -> Max should be '4712-12-31' (from COALESCE)
        *   Group 3: Single record -> Should be included
*   **Action:** Run `r_ausd_vertrag_control` with valid parameters.
*   **Pass/Fail Criterion:**
    1.  Query `project.dataset.sof_ta_bpr_dn_evn`.
    2.  For each `(cntrct_id, bpr_id)` group, only one record should be present, corresponding to the maximum `valid_to` (or `DATE '4712-12-31'` if `valid_to` was `NULL`).

    ```python
    import pytest
    from google.cloud import bigquery

    def test_max_valid_to_partition_logic(bigquery_client):
        # Setup: Populate sof_ta_bpr_dn_evn_his with overlapping valid_to dates
        bigquery_client.query("""
            INSERT INTO `project.dataset.sof_ta_bpr_dn_evn_his` (CNTRCT_ID, BPR_ID, BPRI_COM_ID, CNTRCT_ID_REF, VALID_FROM, VALID_TO, MODIFIED_AT, INSERT_AT) VALUES
            -- Group 1
            ('C_GRP1', 31, 'BPRICOM_G1_1', 'REF_G1', '2022-01-01', '2023-01-01', NULL, '2022-12-01'),
            ('C_GRP1', 31, 'BPRICOM_G1_2', 'REF_G1', '2022-01-01', '2023-01-10', NULL, '2022-12-05'), -- Max
            ('C_GRP1', 31, 'BPRICOM_G1_3', 'REF_G1', '2022-01-01', '2023-01-05', NULL, '2022-12-10'),
            -- Group 2 (with NULL valid_to)
            ('C_GRP2', 31, 'BPRICOM_G2_1', 'REF_G2', '2022-01-01', '2023-01-01', NULL, '2022-12-01'),
            ('C_GRP2', 31, 'BPRICOM_G2_2', 'REF_G2', '2022-01-01', NULL, NULL, '2022-12-05'), -- Max (coalesced to 4712-12-31)
            -- Group 3 (single record)
            ('C_GRP3', 31, 'BPRICOM_G3_1', 'REF_G3', '2022-01-01', '2023-01-01', NULL, '2022-12-01');
        """).result()

        # Action: Execute the migrated SP
        bigquery_client.query("CALL `project.dataset.r_ausd_vertrag_control`('TEST_MAX_VALID_TO', '33333', '01022023', 0);").result()

        # Assertions
        query_bpr_dn_evn = bigquery_client.query("""
            SELECT CNTRCT_ID, BPR_INSTANCE_ID, COLUMN_5VALID_TO FROM `project.dataset.sof_ta_bpr_dn_evn` ORDER BY CNTRCT_ID
        """)
        results = [dict(row) for row in query_bpr_dn_evn.result()]

        assert len(results) == 3
        assert results[0]['CNTRCT_ID'] == 'C_GRP1'
        assert results[0]['BPR_INSTANCE_ID'] == 'BPRICOM_G1_2'
        assert results[0]['COLUMN_5VALID_TO'] == '2023-01-10'

        assert results[1]['CNTRCT_ID'] == 'C_GRP2'
        assert results[1]['BPR_INSTANCE_ID'] == 'BPRICOM_G2_2'
        assert results[1]['COLUMN_5VALID_TO'] == '4712-12-31'

        assert results[2]['CNTRCT_ID'] == 'C_GRP3'
        assert results[2]['BPR_INSTANCE_ID'] == 'BPRICOM_G3_1'
        assert results[2]['COLUMN_5VALID_TO'] == '2023-01-01'
    ```

### Test Case 10: Schema and Data Type Integrity

*   **Purpose:** Verify that the schema and data types of the target tables match the expected structure and that no data truncation or type conversion errors occur during the migration process.
*   **Setup:**
    1.  Clear all tables.
    2.  Populate source tables with data that pushes the boundaries of data types (e.g., maximum length strings for `STRING` columns, values that might cause implicit type conversions if not handled correctly).
*   **Action:** Run `r_ausd_vertrag_control` with valid parameters.
*   **Pass/Fail Criterion:**
    1.  The job completes successfully without any BigQuery schema or data type errors.
    2.  Query the BigQuery information schema to verify the DDL of all target tables (`sof_ta_p_gesch_part`, `sof_ta_p_dn_nutzer`, `sof_ta_p_evn_empf`, `sof_ta_segm_prem`, `sof_ta_bpr_dn_evn`, `sof_ta_bpr_dn_evn_his`) matches the expected DDL (e.g., `STRING` for text, `INT64` for integers, `DATE` for dates).
    3.  Perform spot checks on inserted data to ensure no truncation occurred for long strings and that numeric/date values are stored correctly.

    ```python
    import pytest
    from google.cloud import bigquery

    def test_schema_and_data_type_integrity(bigquery_client):
        # Setup: Populate source tables with max-length strings, various numbers, dates
        long_string = "A" * 250 # Example max length for a STRING column
        bigquery_client.query(f"""
            INSERT INTO `project.dataset.bpd_ta_bp_valueseg_assoc` (BP_ID, SEGMENT_ID) VALUES ('BP_SCHEMA_TEST', 11);
            INSERT INTO `project.dataset.sof_ta_e_reach_gp` (cntrct_cp2_id, bp_id, corp_unit, surname_s, first_name_g, land_sd, zip_code, city, street, house_nr, pobox, address_attachment_org) VALUES
                ('C_SCHEMA_TEST', 'BP_SCHEMA_TEST', '{long_string}', '{long_string}', '{long_string}', 'DE', '12345', 'City', '{long_string}', '1A', 'Pobox', '{long_string}');
            INSERT INTO `project.dataset.sof_ta_e_business_gp` (bp_id, organisation_name, title, surname, first_name, tm_customerid, sales_tax_freed) VALUES
                ('BP_SCHEMA_TEST', '{long_string}', 'Dr', '{long_string}', '{long_string}', 'TM1234567890', 'X');
            INSERT INTO `project.dataset.pds_ta_bpri_com` (cntrct_id, bpr_id, bpri_com_id, cntrct_id_ref, valid_from, valid_to, modified_at, insert_at, is_production) VALUES
                ('C_SCHEMA_TEST', 31, '{long_string}', '{long_string}', '2023-01-01', '2023-12-31', '2023-06-15', '2023-01-01', 1);
            -- ... populate other source tables with similar boundary data
        """).result()

        # Action: Execute the migrated SP
        bigquery_client.query("CALL `project.dataset.r_ausd_vertrag_control`('TEST_SCHEMA_INTEGRITY', '44444', '01012023', 0);").result()

        # Assertions
        # 1. Check table schemas
        expected_schema_gesch_part = {
            'CNTRCT_ID': 'STRING', 'NAMENSZUSATZ': 'STRING', 'ADRESSZUSATZ': 'STRING',
            'FIRMENNAME': 'STRING', 'AKAD_TITEL': 'STRING', 'NACHNAME': 'STRING',
            'VORNAME': 'STRING', 'LAND': 'STRING', 'PLZ': 'STRING', 'WOHNORT': 'STRING',
            'STRASSE': 'STRING', 'KUNDE_SEGMENT_ID': 'STRING', 'PREM_SEGMENT_ID': 'INT64',
            'TM_KUNDENNUMMER': 'STRING', 'MWST_KENNZEICHEN': 'STRING', 'ORGANISATIONSEINHEIT': 'STRING'
        }
        query_schema = bigquery_client.query(f"""
            SELECT column_name, data_type FROM `project.dataset.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = 'sof_ta_p_gesch_part'
        """)
        actual_schema_gesch_part = {row['column_name']: row['data_type'] for row in query_schema.result()}
        assert actual_schema_gesch_part == expected_schema_gesch_part
        # ... repeat for other target tables

        # 2. Check for data truncation (spot check a long string)
        query_data = bigquery_client.query(f"""
            SELECT FIRMENNAME, NACHNAME, VORNAME, STRASSE FROM `project.dataset.sof_ta_p_gesch_part`
            WHERE CNTRCT_ID = 'C_SCHEMA_TEST'
        """)
        result_data = [dict(row) for row in query_data.result()]
        assert len(result_data) == 1
        assert len(result_data[0]['FIRMENNAME']) == len(long_string) # Check if full string was inserted
        assert len(result_data[0]['NACHNAME']) == len(long_string)
        assert len(result_data[0]['VORNAME']) == len(long_string)
        assert len(result_data[0]['STRASSE']) > len(long_string) # Should be long_string + ' 1A'
    ```

### Test Case 11: Record Count Parity for All Tables

*   **Purpose:** Verify that the record counts in all intermediate and final target tables match the legacy system's output for a given input. This ensures that no rows are unexpectedly dropped or duplicated during any step of the transformation.
*   **Setup:**
    1.  Clear all tables.
    2.  Populate source tables with a known, representative dataset.
    3.  Capture the expected row counts for all tables populated by `d_ausd_geschaeftspartner.sql` (e.g., `sof_ta_segm_prem`, `sof_ta_bpr_dn_evn_his`, `sof_ta_bpr_dn_evn`, `sof_ta_p_gesch_part`, `sof_ta_p_dn_nutzer`, `sof_ta_p_evn_empf`) from a legacy run. Store this as `expected_legacy_row_counts`.
*   **Action:** Run `r_ausd_vertrag_control` with the same parameters used for the legacy run.
*   **Pass/Fail Criterion:**
    1.  Query the `COUNT(*)` from each of the target tables in BigQuery.
    2.  Each BigQuery table's row count must exactly match its corresponding `expected_legacy_row_counts`.

    ```python
    import pytest
    from google.cloud import bigquery

    # Assume expected_legacy_row_counts = {
    #     'sof_ta_segm_prem': 5,
    #     'sof_ta_bpr_dn_evn_his': 12,
    #     'sof_ta_bpr_dn_evn': 8,
    #     'sof_ta_p_gesch_part': 10,
    #     'sof_ta_p_dn_nutzer': 7,
    #     'sof_ta_p_evn_empf': 3
    # }

    def test_record_count_parity_all_tables(bigquery_client, expected_legacy_row_counts):
        # 1. Populate source tables with test data (same as happy path, but ensure it's consistent)
        # ... (insert data into source tables) ...

        # 2. Execute the migrated SP
        bigquery_client.query("CALL `project.dataset.r_ausd_vertrag_control`('TEST_ROW_COUNT_PARITY', '55555', '01012023', 0);").result()

        # 3. Assertions for all target tables
        tables_to_check = [
            'sof_ta_segm_prem', 'sof_ta_bpr_dn_evn_his', 'sof_ta_bpr_dn_evn',
            'sof_ta_p_gesch_part', 'sof_ta_p_dn_nutzer', 'sof_ta_p_evn_empf'
        ]

        for table_name in tables_to_check:
            query_count = bigquery_client.query(f"SELECT COUNT(*) FROM `project.dataset.{table_name}`")
            actual_count = query_count.result().to_dataframe().iloc[0, 0]
            assert actual_count == expected_legacy_row_counts[table_name], \
                f"Row count mismatch for table {table_name}: Expected {expected_legacy_row_counts[table_name]}, Got {actual_count}"

        # Also check the record_count logged in job_log
        query_job_log = bigquery_client.query("SELECT record_count FROM `project.dataset.job_log` WHERE job_kennung = 'TEST_ROW_COUNT_PARITY'")
        logged_record_count = query_job_log.result().to_dataframe().iloc[0, 0]
        assert logged_record_count == expected_legacy_row_counts['sof_ta_p_gesch_part'], \
            "Logged record count mismatch with expected main output table count."
    ```

### Test Case 12: Empty Source Tables

*   **Purpose:** Verify the job handles empty source tables gracefully and produces empty target tables, and logs a record count of 0.
*   **Setup:**
    1.  Clear all tables.
    2.  Ensure all source tables are empty.
*   **Action:** Run `r_ausd_vertrag_control` with valid parameters.
*   **Pass/Fail Criterion:**
    1.  The job completes successfully (`job_log` status 'COMPLETED').
    2.  The `record_count` in `job_log` is 0.
    3.  All target tables (`sof_ta_segm_prem`, `sof_ta_bpr_dn_evn`, `sof_ta_bpr_dn_evn_his`, `sof_ta_p_gesch_part`, `sof_ta_p_dn_nutzer`, `sof_ta_p_evn_empf`) must be empty (contain 0 rows).

    ```python
    import pytest
    from google.cloud import bigquery

    def test_empty_source_tables(bigquery_client):
        # Setup: Ensure all source tables are empty (cleanup_tables fixture handles this)

        # Action: Execute the migrated SP
        bigquery_client.query("CALL `project.dataset.r_ausd_vertrag_control`('TEST_EMPTY_SOURCES', '66666', '01012023', 0);").result()

        # Assertions
        query_job_log = bigquery_client.query("SELECT status, record_count FROM `project.dataset.job_log` WHERE job_kennung = 'TEST_EMPTY_SOURCES'")
        log_entry = [dict(row) for row in query_job_log.result()]

        assert len(log_entry) == 1
        assert log_entry[0]['status'] == 'COMPLETED'
        assert log_entry[0]['record_count'] == 0

        tables_to_check = [
            'sof_ta_segm_prem', 'sof_ta_bpr_dn_evn_his', 'sof_ta_bpr_dn_evn',
            'sof_ta_p_gesch_part', 'sof_ta_p_dn_nutzer', 'sof_ta_p_evn_empf'
        ]
        for table_name in tables_to_check:
            query_count = bigquery_client.query(f"SELECT COUNT(*) FROM `project.dataset.{table_name}`")
            actual_count = query_count.result().to_dataframe().iloc[0, 0]
            assert actual_count == 0, f"Table {table_name} should be empty but contains {actual_count} rows."
    ```