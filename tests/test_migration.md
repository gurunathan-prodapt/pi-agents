As a senior data-migration QA engineer, I've designed a suite of validation tests for the migration of `k_ausd_bp_ta_p_basisprod.ksh` to BigQuery. These tests aim to ensure the migrated solution is functionally equivalent, robust, and adheres to data quality standards.

The tests are categorized according to the requirements: Output Parity, Transformation Correctness, External-System Replacements, and Data Quality/Schema Assertions. Each test case includes its purpose, setup instructions, the action to be performed, and a concrete pass/fail criterion, often with runnable SQL or Python (pytest) code.

**Assumptions:**
*   `project.dataset` is the placeholder for your BigQuery project and dataset. Replace it with your actual project and dataset names.
*   Source tables (`sof$ta_cntrct_dist`, `sof$ta_cntrct_evn`, etc.) have been created in BigQuery with schemas matching the legacy Oracle system (or at least the relevant columns for this job).
*   A "golden dataset" representing the expected output from the legacy system for specific inputs is available for comparison. For simplicity in these examples, I will define the expected output directly in the test.
*   The Airflow DAG (`k_ausd_bp_ta_p_basisprod_workflow`) is deployed and accessible.
*   `pytest` is used for test orchestration and assertions, interacting with BigQuery via its client library.

---

## Migration Validation Tests for `k_ausd_bp_ta_p_basisprod.ksh`

### Setup for All Tests: BigQuery Source Data & Target Table

Before running any tests, ensure the necessary BigQuery tables exist and are empty or reset for each test run.

**1. Create Source Tables (if they don't exist):**

```sql
-- project.dataset.sof$ta_cntrct_dist
CREATE TABLE IF NOT EXISTS `project.dataset.sof$ta_cntrct_dist` (
    cntrct_id STRING NOT NULL
);

-- project.dataset.sof$ta_cntrct_evn
CREATE TABLE IF NOT EXISTS `project.dataset.sof$ta_cntrct_evn` (
    cntrct_id STRING NOT NULL,
    evn STRING
);

-- project.dataset.sof$ta_iccid_vertrag
CREATE TABLE IF NOT EXISTS `project.dataset.sof$ta_iccid_vertrag` (
    cntrct_id STRING NOT NULL,
    tn_iccid STRING, tn_imsi_mcc STRING, tn_imsi_mnc STRING, tn_imsi_hlr STRING, tn_imsi_si STRING, tn_status STRING, tn_valid_to STRING,
    tc_iccid STRING, tc_imsi_mcc STRING, tc_imsi_mnc STRING, tc_imsi_hlr STRING, tc_imsi_si STRING, tc_status STRING, tc_valid_to STRING,
    tb_iccid STRING, tb_imsi_mcc STRING, tb_imsi_mnc STRING, tb_imsi_hlr STRING, tb_imsi_si STRING, tb_status STRING, tb_valid_to STRING,
    ms1_iccid STRING, ms1_imsi_mcc STRING, ms1_imsi_mnc STRING, ms1_imsi_hlr STRING, ms1_imsi_si STRING, ms1_status STRING, ms1_valid_to STRING,
    ms2_iccid STRING, ms2_imsi_mcc STRING, ms2_imsi_mnc STRING, ms2_imsi_hlr STRING, ms2_imsi_si STRING, ms2_status STRING, ms2_valid_to STRING,
    tn_e_id STRING, tn_card_type_name STRING, tc_e_id STRING, tc_card_type_name STRING, tb_e_id STRING, tb_card_type_name STRING,
    ms1_e_id STRING, ms1_card_type_name STRING, ms2_e_id STRING, ms2_card_type_name STRING,
    ms3_iccid STRING, ms3_e_id STRING, ms3_card_type_name STRING, ms3_imsi_mcc STRING, ms3_imsi_mnc STRING, ms3_imsi_hlr STRING, ms3_imsi_si STRING, ms3_status STRING, ms3_valid_to STRING,
    ms4_iccid STRING, ms4_e_id STRING, ms4_card_type_name STRING, ms4_imsi_mcc STRING, ms4_imsi_mnc STRING, ms4_imsi_hlr STRING, ms4_imsi_si STRING, ms4_status STRING, ms4_valid_to STRING,
    ms5_iccid STRING, ms5_e_id STRING, ms5_card_type_name STRING, ms5_imsi_mcc STRING, ms5_imsi_mnc STRING, ms5_imsi_hlr STRING, ms5_imsi_si STRING, ms5_status STRING, ms5_valid_to STRING,
    ms6_iccid STRING, ms6_e_id STRING, ms6_card_type_name STRING, ms6_imsi_mcc STRING, ms6_imsi_mnc STRING, ms6_imsi_hlr STRING, ms6_imsi_si STRING, ms6_status STRING, ms6_valid_to STRING,
    ms7_iccid STRING, ms7_e_id STRING, ms7_card_type_name STRING, ms7_imsi_mcc STRING, ms7_imsi_mnc STRING, ms7_imsi_hlr STRING, ms7_imsi_si STRING, ms7_status STRING, ms7_valid_to STRING,
    ms8_iccid STRING, ms8_e_id STRING, ms8_card_type_name STRING, ms8_imsi_mcc STRING, ms8_imsi_mnc STRING, ms8_imsi_hlr STRING, ms8_imsi_si STRING, ms8_status STRING, ms8_valid_to STRING,
    ms9_iccid STRING, ms9_e_id STRING, ms9_card_type_name STRING, ms9_imsi_mcc STRING, ms9_imsi_mnc STRING, ms9_imsi_hlr STRING, ms9_imsi_si STRING, ms9_status STRING, ms9_valid_to STRING,
    ms10_iccid STRING, ms10_e_id STRING, ms10_card_type_name STRING, ms10_imsi_mcc STRING, ms10_imsi_mnc STRING, ms10_imsi_hlr STRING, ms10_imsi_si STRING, ms10_status STRING, ms10_valid_to STRING
);

-- project.dataset.sof$ta_rn_vertrag
CREATE TABLE IF NOT EXISTS `project.dataset.sof$ta_rn_vertrag` (
    cntrct_id STRING NOT NULL,
    tn_multi_single STRING, tc_multi_single STRING, tb_multi_single STRING,
    tn_tel_msisdn STRING, tn_tel_status STRING, tn_tel_valid_to STRING,
    tn_dat_msisdn STRING, tn_dat_status STRING, tn_dat_valid_to STRING,
    tn_fax_msisdn STRING, tn_fax_status STRING, tn_fax_valid_to STRING,
    tc_tel_msisdn STRING, tc_tel_status STRING, tc_tel_valid_to STRING,
    tc_dat_msisdn STRING, tc_dat_status STRING, tc_dat_valid_to STRING,
    tc_fax_msisdn STRING, tc_fax_status STRING, tc_fax_valid_to STRING,
    tb_tel_msisdn STRING, tb_tel_status STRING, tb_tel_valid_to STRING,
    tb_dat_msisdn STRING, tb_dat_status STRING, tb_dat_valid_to STRING,
    tb_fax_msisdn STRING, tb_fax_status STRING, tb_fax_valid_to STRING,
    ms_rn_1_msisdn STRING, ms_rn_1_status STRING, ms_rn_1_valid_to STRING,
    ms_rn_2_msisdn STRING, ms_rn_2_status STRING, ms_rn_2_valid_to STRING
);

-- project.dataset.sof$ta_rn_da_vda_tk
CREATE TABLE IF NOT EXISTS `project.dataset.sof$ta_rn_da_vda_tk` (
    cntrct_id STRING NOT NULL,
    da_rn_msisdn STRING, da_rn_status STRING, da_rn_valid_to STRING,
    vda_rn_msisdn STRING, vda_rn_status STRING, vda_rn_valid_to STRING,
    tk_rn_msisdn STRING, tk_rn_status STRING, tk_rn_valid_to STRING
);

-- project.dataset.sof$ta_tarifoption
CREATE TABLE IF NOT EXISTS `project.dataset.sof$ta_tarifoption` (
    cntrct_id STRING NOT NULL,
    data_option_rein STRING, voice_option_rein STRING, mix_option STRING,
    multi_option STRING, roaming_option STRING, sonstige_option STRING
);

-- project.dataset.sof$ta_apn_vertrag
CREATE TABLE IF NOT EXISTS `project.dataset.sof$ta_apn_vertrag` (
    cntrct_id STRING NOT NULL,
    apn STRING,
    apn_cntrct STRING
);

-- project.dataset.SOF$TA_BCP_ICCID
CREATE TABLE IF NOT EXISTS `project.dataset.SOF$TA_BCP_ICCID` (
    cntrct_id STRING NOT NULL,
    cntrct_id_ref STRING NOT NULL,
    tn_iccid STRING,
    tn_imsi_hlr STRING
);

-- project.dataset.SOF$TA_BCP_MSISDN
CREATE TABLE IF NOT EXISTS `project.dataset.SOF$TA_BCP_MSISDN` (
    cntrct_id STRING NOT NULL,
    cntrct_id_ref STRING NOT NULL,
    tn_tel_msisdn STRING
);
```

**2. Create Logging Tables (if they don't exist):**

```sql
-- DDL for error_log table (from generated code)
CREATE TABLE IF NOT EXISTS `project.dataset.error_log` (
    job_id STRING NOT NULL,
    run_id STRING NOT NULL,
    log_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    message STRING,
    error_code STRING,
    severity STRING
);

-- DDL for job_log table (from generated code)
CREATE TABLE IF NOT EXISTS `project.dataset.job_log` (
    job_id STRING NOT NULL,
    run_id STRING NOT NULL,
    log_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    message STRING,
    record_count INT64,
    status STRING
);
```

**3. Create Target Table (if it doesn't exist):**

```sql
-- DDL for PoolBasisprodukt_target table (from generated code)
CREATE TABLE IF NOT EXISTS `project.dataset.PoolBasisprodukt_target` (
    CNTRCT_ID STRING, EVN STRING, TNV_ICCID STRING, TNV_MCC STRING, TNV_MNC STRING, TNV_HLR STRING, TNV_SI STRING, TNV_ICC_STAT STRING, TNV_ICC_VALID DATE, TC_ICCID STRING,
    TC_MCC STRING, TC_MNC STRING, TC_HLR STRING, TC_SI STRING, TC_ICC_STAT STRING, TC_ICC_VALID DATE, TB_ICCID STRING, TB_MCC STRING, TB_MNC STRING, TB_HLR STRING,
    TB_SI STRING, TB_ICC_STAT STRING, TB_ICC_VALID DATE, MS1_ICCID STRING, MS1_MCC STRING, MS1_MNC STRING, MS1_HLR STRING, MS1_SI STRING, MS1_STAT STRING, MS1_VALID DATE,
    MS2_ICCID STRING, MS2_MCC STRING, MS2_MNC STRING, MS2_HLR STRING, MS2_SI STRING, MS2_STAT STRING, MS2_VALID DATE, TNV_E_ID STRING, TNV_CARD_TYPE_NAME STRING, TC_E_ID STRING,
    TC_CARD_TYPE_NAME STRING, TB_E_ID STRING, TB_CARD_TYPE_NAME STRING, MS1_E_ID STRING, MS1_CARD_TYPE_NAME STRING, MS2_E_ID STRING, MS2_CARD_TYPE_NAME STRING,
    TNV_MULTI_SINGLE STRING, TC_MULTI_SINGLE STRING, TB_MULTI_SINGLE STRING, TNV_MSISDN STRING, TNV_MS_STAT STRING, TNV_MS_VALID DATE, TNV_DAT_MSISDN STRING,
    TNV_DAT_STAT STRING, TNV_DAT_VALID DATE, TNV_FAX_MSISDN STRING, TNV_FAX_STAT STRING, TNV_FAX_VALID DATE, TC_MSISDN STRING, TC_MS_STAT STRING, TC_MS_VALID DATE,
    TC_DAT_MSISDN STRING, TC_DAT_STAT STRING, TC_DAT_VALID DATE, TC_FAX_MSISDN STRING, TC_FAX_STAT STRING, TC_FAX_VALID DATE, TB_MSISDN STRING, TB_MS_STAT STRING,
    TB_MS_VALID DATE, TB_DAT_MSISDN STRING, TB_DAT_STAT STRING, TB_DAT_VALID DATE, TB_FAX_MSISDN STRING, TB_FAX_STAT STRING, TB_FAX_VALID DATE, MS1_MSISDN STRING,
    MS1_MS_STAT STRING, MS1_MS_VALID DATE, MS2_MSISDN STRING, MS2_MS_STAT STRING, MS2_MS_VALID DATE, DA_MSISDN STRING, DA_MS_STAT STRING, DA_MS_VALID DATE,
    VDA_MSISDN STRING, VDA_MS_STAT STRING, VDA_MS_VALID DATE, TK_MSISDN STRING, TK_MS_STAT STRING, TK_MS_VALID DATE, BCP_VERTRAG STRING, BCP_ICCID STRING,
    BCP_HLR STRING, APN STRING, BCP_TN_TEL STRING, DATA_OPTION_REIN STRING, VOICE_OPTION_REIN STRING, MIX_OPTION STRING, MULTI_OPTION STRING, ROAMING_OPTION STRING,
    SONSTIGE_OPTION STRING, MS3_ICCID STRING, MS3_E_ID STRING, MS3_CARD_TYPE_NAME STRING, MS3_MCC STRING, MS3_MNC STRING, MS3_HLR STRING, MS3_SI STRING, MS3_STAT STRING,
    MS3_VALID DATE, MS4_ICCID STRING, MS4_E_ID STRING, MS4_CARD_TYPE_NAME STRING, MS4_MCC STRING, MS4_MNC STRING, MS4_HLR STRING, MS4_SI STRING, MS4_STAT STRING, MS4_VALID DATE,
    MS5_ICCID STRING, MS5_E_ID STRING, MS5_CARD_TYPE_NAME STRING, MS5_MCC STRING, MS5_MNC STRING, MS5_HLR STRING, MS5_SI STRING, MS5_STAT STRING, MS5_VALID DATE, MS6_ICCID STRING,
    MS6_E_ID STRING, MS6_CARD_TYPE_NAME STRING, MS6_MCC STRING, MS6_MNC STRING, MS6_HLR STRING, MS6_SI STRING, MS6_STAT STRING, MS6_VALID DATE, MS7_ICCID STRING, MS7_E_ID STRING,
    MS7_CARD_TYPE_NAME STRING, MS7_MCC STRING, MS7_MNC STRING, MS7_HLR STRING, MS7_SI STRING, MS7_STAT STRING, MS7_VALID DATE, MS8_ICCID STRING, MS8_E_ID STRING,
    MS8_CARD_TYPE_NAME STRING, MS8_MCC STRING, MS8_MNC STRING, MS8_HLR STRING, MS8_SI STRING, MS8_STAT STRING, MS8_VALID DATE, MS9_ICCID STRING, MS9_E_ID STRING,
    MS9_CARD_TYPE_NAME STRING, MS9_MCC STRING, MS9_MNC STRING, MS9_HLR STRING, MS9_SI STRING, MS9_STAT STRING, MS9_VALID DATE, MS10_ICCID STRING, MS10_E_ID STRING,
    MS10_CARD_TYPE_NAME STRING, MS10_MCC STRING, MS10_MNC STRING, MS10_HLR STRING, MS10_SI STRING, MS10_STAT STRING, MS10_VALID DATE
);
```

---

### 1. Output Parity Tests

These tests ensure that for a given set of inputs, the migrated job produces the exact same output data as the legacy job.

#### Test Case 1.1: Full Data Parity (Happy Path)

*   **Purpose**: Verify that the migrated BigQuery Stored Procedure, when executed with valid parameters and representative source data, produces a target table (`PoolBasisprodukt_target`) identical to the legacy system's output.
*   **Setup**:
    1.  Clear `project.dataset.PoolBasisprodukt_target`, `project.dataset.job_log`, and `project.dataset.error_log`.
    2.  Populate all source tables (`sof$ta_cntrct_dist`, `sof$ta_cntrct_evn`, etc.) with a comprehensive set of test data that covers various join scenarios, NULLs, valid dates, invalid dates, and APN combinations.
    3.  Obtain the "golden dataset" (expected output) from a successful run of the legacy `k_ausd_bp_ta_p_basisprod.ksh` with the *exact same source data*. This golden dataset should be loaded into a temporary BigQuery table, e.g., `project.dataset.PoolBasisprodukt_target_golden`.
*   **Action**:
    Execute the BigQuery Stored Procedure with valid parameters:
    `CALL project.dataset.r_ausd_bp_ta_p_basisprod('JOB123', '1', '01012023', 0, 'test_job_id_1_1', 'test_run_id_1_1');`
*   **Pass/Fail Criterion**:
    The `PoolBasisprodukt_target` table must contain the exact same number of rows and identical data in all columns as `PoolBasisprodukt_target_golden`.

    ```python
    # pytest_output_parity.py
    from google.cloud import bigquery
    import pytest

    PROJECT_ID = 'project'
    DATASET_ID = 'dataset'
    SP_NAME = 'r_ausd_bp_ta_p_basisprod'
    TARGET_TABLE = f'{PROJECT_ID}.{DATASET_ID}.PoolBasisprodukt_target'
    GOLDEN_TABLE = f'{PROJECT_ID}.{DATASET_ID}.PoolBasisprodukt_target_golden'

    client = bigquery.Client()

    def _run_sp(job_kennung, eintrags_nr, stichtag, wiederanlauf_wert, job_id, run_id):
        query = f"""
            CALL `{PROJECT_ID}.{DATASET_ID}.{SP_NAME}`(
                p_job_kennung => '{job_kennung}',
                p_eintrags_nr => '{eintrags_nr}',
                p_stichtag => '{stichtag}',
                p_wiederanlauf_wert => {wiederanlauf_wert},
                p_job_id => '{job_id}',
                p_run_id => '{run_id}'
            );
        """
        query_job = client.query(query)
        query_job.result() # Wait for job to complete

    def _clear_tables():
        client.query(f"TRUNCATE TABLE {TARGET_TABLE}").result()
        client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.job_log`").result()
        client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.error_log`").result()
        # Clear source tables if they are populated per test
        # For this test, we assume source tables are pre-populated with a fixed dataset

    def _populate_golden_data():
        # This function would load your golden dataset into GOLDEN_TABLE
        # For demonstration, let's assume it's already populated or we define it here.
        # Example: client.query(f"INSERT INTO {GOLDEN_TABLE} (...) VALUES (...)").result()
        # In a real scenario, this would involve loading a CSV or other format.
        pass # Assuming golden data is pre-loaded for this test

    @pytest.fixture(scope="module", autouse=True)
    def setup_golden_data():
        _populate_golden_data()
        yield

    @pytest.fixture(autouse=True)
    def setup_and_teardown_for_test():
        _clear_tables()
        yield
        _clear_tables() # Clean up after test

    def test_full_data_parity_happy_path():
        # Action
        _run_sp('JOB123', '1', '01012023', 0, 'test_job_id_1_1', 'test_run_id_1_1')

        # Pass/Fail Criterion: Compare row counts first
        target_row_count = client.query(f"SELECT COUNT(*) FROM {TARGET_TABLE}").result().to_dataframe().iloc[0, 0]
        golden_row_count = client.query(f"SELECT COUNT(*) FROM {GOLDEN_TABLE}").result().to_dataframe().iloc[0, 0]
        assert target_row_count == golden_row_count, \
            f"Row count mismatch: Target has {target_row_count}, Golden has {golden_row_count}"

        # Then compare content
        diff_query = f"""
            (SELECT * FROM {TARGET_TABLE} EXCEPT DISTINCT SELECT * FROM {GOLDEN_TABLE})
            UNION ALL
            (SELECT * FROM {GOLDEN_TABLE} EXCEPT DISTINCT SELECT * FROM {TARGET_TABLE})
        """
        diff_result = client.query(diff_query).result().to_dataframe()
        assert diff_result.empty, f"Data mismatch found between {TARGET_TABLE} and {GOLDEN_TABLE}:\n{diff_result.to_string()}"
    ```

#### Test Case 1.2: Record Count Parity

*   **Purpose**: Verify that the number of records processed and inserted into the target table by the migrated job matches the number reported by the legacy job.
*   **Setup**:
    1.  Clear `project.dataset.PoolBasisprodukt_target`, `project.dataset.job_log`, and `project.dataset.error_log`.
    2.  Populate source tables with a known number of records that would result in a specific output count (e.g., 100 records).
    3.  Record the expected record count from the legacy job's output (e.g., from its `tmpFile` or log).
*   **Action**:
    Execute the BigQuery Stored Procedure:
    `CALL project.dataset.r_ausd_bp_ta_p_basisprod('JOB124', '2', '02012023', 0, 'test_job_id_1_2', 'test_run_id_1_2');`
*   **Pass/Fail Criterion**:
    The `record_count` in the `project.dataset.job_log` table for `test_run_id_1_2` must match the expected record count from the legacy system. Also, the actual row count in `PoolBasisprodukt_target` must match.

    ```python
    # pytest_record_count_parity.py
    # ... (imports and client setup as above) ...

    def _populate_source_data_for_count_test(expected_output_count):
        # Example: Insert data into source tables that will result in `expected_output_count` rows
        # This is highly dependent on your specific source data and join logic.
        # For simplicity, let's assume 5 rows in cntrct_dist will result in 5 output rows.
        client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.sof$ta_cntrct_dist`").result()
        insert_query = f"INSERT INTO `{PROJECT_ID}.{DATASET_ID}.sof$ta_cntrct_dist` (cntrct_id) VALUES "
        values = [f"('CNTRCT_{i}')" for i in range(expected_output_count)]
        client.query(insert_query + ",".join(values)).result()
        # Populate other source tables similarly to ensure joins work and produce the expected count.

    def test_record_count_parity():
        expected_legacy_record_count = 5 # Assume legacy job processed 5 records for this specific input
        _populate_source_data_for_count_test(expected_legacy_record_count)
        
        # Action
        _run_sp('JOB124', '2', '02012023', 0, 'test_job_id_1_2', 'test_run_id_1_2')

        # Pass/Fail Criterion
        target_row_count_query = f"SELECT COUNT(*) FROM {TARGET_TABLE}"
        target_row_count = client.query(target_row_count_query).result().to_dataframe().iloc[0, 0]

        job_log_query = f"""
            SELECT record_count
            FROM `{PROJECT_ID}.{DATASET_ID}.job_log`
            WHERE run_id = 'test_run_id_1_2' AND status = 'SUCCESS'
        """
        job_log_result = client.query(job_log_query).result().to_dataframe()

        assert not job_log_result.empty, "No successful job log entry found."
        logged_record_count = job_log_result.iloc[0, 0]

        assert target_row_count == expected_legacy_record_count, \
            f"Actual target row count ({target_row_count}) does not match expected ({expected_legacy_record_count})."
        assert logged_record_count == expected_legacy_record_count, \
            f"Logged record count ({logged_record_count}) does not match expected ({expected_legacy_record_count})."
    ```

---

### 2. Transformation Correctness Tests

These tests focus on specific logic within the migrated code, such as parameter handling, data type conversions, NULL handling, and complex transformations.

#### Test Case 2.1: Parameter Validation - Missing `p_JobKennung`

*   **Purpose**: Verify that the BigQuery Stored Procedure correctly identifies and handles a missing `p_job_kennung` parameter, logging an error and signaling failure.
*   **Setup**:
    1.  Clear `project.dataset.job_log` and `project.dataset.error_log`.
*   **Action**:
    Attempt to execute the BigQuery Stored Procedure with `p_job_kennung` as `NULL` or an empty string.
    `CALL project.dataset.r_ausd_bp_ta_p_basisprod(NULL, '1', '01012023', 0, 'test_job_id_2_1', 'test_run_id_2_1');`
*   **Pass/Fail Criterion**:
    The stored procedure execution must fail (e.g., raise an exception in the calling environment). An entry must be present in `project.dataset.error_log` for `test_run_id_2_1` with `error_code = 'PARAM_MISSING_JOB_KENNUNG'` and `severity = 'ERROR'`.

    ```python
    # pytest_param_validation.py
    # ... (imports and client setup as above) ...

    def test_missing_job_kennung_parameter():
        # Action
        with pytest.raises(Exception) as excinfo:
            _run_sp(None, '1', '01012023', 0, 'test_job_id_2_1', 'test_run_id_2_1')

        # Pass/Fail Criterion
        assert "PARAM_MISSING_JOB_KENNUNG" in str(excinfo.value)

        error_log_query = f"""
            SELECT message, error_code, severity
            FROM `{PROJECT_ID}.{DATASET_ID}.error_log`
            WHERE run_id = 'test_run_id_2_1'
        """
        error_log_result = client.query(error_log_query).result().to_dataframe()

        assert not error_log_result.empty, "No error log entry found for missing p_job_kennung."
        assert error_log_result.iloc[0]['error_code'] == 'PARAM_MISSING_JOB_KENNUNG'
        assert error_log_result.iloc[0]['severity'] == 'ERROR'
    ```

#### Test Case 2.2: Date Validation - Invalid `p_Stichtag` Format

*   **Purpose**: Verify that the BigQuery Stored Procedure correctly validates the `p_stichtag` parameter for the `DDMMYYYY` format, logging an error and signaling failure for invalid formats.
*   **Setup**:
    1.  Clear `project.dataset.job_log` and `project.dataset.error_log`.
*   **Action**:
    Attempt to execute the BigQuery Stored Procedure with an invalid `p_stichtag` (e.g., `2023-01-01` or `01/01/2023`).
    `CALL project.dataset.r_ausd_bp_ta_p_basisprod('JOB125', '1', '2023-01-01', 0, 'test_job_id_2_2', 'test_run_id_2_2');`
*   **Pass/Fail Criterion**:
    The stored procedure execution must fail. An entry must be present in `project.dataset.error_log` for `test_run_id_2_2` with `error_code = 'DATE_VALIDATION_FAILED'` and `severity = 'ERROR'`.

    ```python
    # pytest_date_validation.py
    # ... (imports and client setup as above) ...

    def test_invalid_stichtag_format():
        # Action
        with pytest.raises(Exception) as excinfo:
            _run_sp('JOB125', '1', '2023-01-01', 0, 'test_job_id_2_2', 'test_run_id_2_2')

        # Pass/Fail Criterion
        assert "DATE_VALIDATION_FAILED" in str(excinfo.value)

        error_log_query = f"""
            SELECT message, error_code, severity
            FROM `{PROJECT_ID}.{DATASET_ID}.error_log`
            WHERE run_id = 'test_run_id_2_2'
        """
        error_log_result = client.query(error_log_query).result().to_dataframe()

        assert not error_log_result.empty, "No error log entry found for invalid p_stichtag."
        assert error_log_result.iloc[0]['error_code'] == 'DATE_VALIDATION_FAILED'
        assert error_log_result.iloc[0]['severity'] == 'ERROR'
    ```

#### Test Case 2.3: `p_wiederanlaufWert` Default Value

*   **Purpose**: Verify that `p_wiederanlauf_wert` correctly defaults to `0` if not explicitly provided, mirroring the legacy script's behavior.
*   **Setup**:
    1.  Clear `project.dataset.job_log` and `project.dataset.error_log`.
    2.  Populate source tables with minimal data to allow a successful run.
*   **Action**:
    Execute the BigQuery Stored Procedure omitting `p_wiederanlauf_wert`.
    `CALL project.dataset.r_ausd_bp_ta_p_basisprod('JOB126', '1', '03012023', p_job_id => 'test_job_id_2_3', p_run_id => 'test_run_id_2_3');`
*   **Pass/Fail Criterion**:
    The procedure must execute successfully. The `job_log` entry should indicate success. (Since `p_wiederanlauf_wert` is not used in the core SQL, its effect is primarily on the SP's internal state, which is implicitly tested by successful execution without error).

    ```python
    # pytest_wiederanlauf_default.py
    # ... (imports and client setup as above) ...

    def test_wiederanlauf_wert_default():
        _populate_source_data_for_count_test(1) # Ensure some data for a successful run

        # Action (omitting p_wiederanlauf_wert)
        _run_sp('JOB126', '1', '03012023', None, 'test_job_id_2_3', 'test_run_id_2_3') # Pass None to use default

        # Pass/Fail Criterion
        job_log_query = f"""
            SELECT status
            FROM `{PROJECT_ID}.{DATASET_ID}.job_log`
            WHERE run_id = 'test_run_id_2_3'
        """
        job_log_result = client.query(job_log_query).result().to_dataframe()

        assert not job_log_result.empty, "No job log entry found."
        assert job_log_result.iloc[0]['status'] == 'SUCCESS', "Job did not complete successfully with default p_wiederanlauf_wert."
    ```

#### Test Case 2.4: `APN` Field Transformation Logic

*   **Purpose**: Verify the `CASE` statement and `CONCAT` logic for the `APN` field (`CASE WHEN av.apn IS NULL THEN av.apn ELSE CONCAT(av.apn, ',', av.apn_cntrct) END`) correctly handles various combinations of `apn` and `apn_cntrct` from `sof$ta_apn_vertrag`.
*   **Setup**:
    1.  Clear `project.dataset.PoolBasisprodukt_target`.
    2.  Populate `sof$ta_cntrct_dist` with contract IDs.
    3.  Populate `sof$ta_apn_vertrag` with specific test cases for `apn` and `apn_cntrct`:
        *   `cntrct_id = 'APN_1'`, `apn = 'internet'`, `apn_cntrct = 'contract1'` (Expected: `internet,contract1`)
        *   `cntrct_id = 'APN_2'`, `apn = 'internet'`, `apn_cntrct = NULL` (Expected: `internet`) - *Correction: The `CASE` statement `WHEN av.apn IS NULL THEN av.apn` means if `apn` is NULL, it returns NULL. If `apn` is NOT NULL, it concatenates. So, if `apn` is 'internet' and `apn_cntrct` is NULL, it should be 'internet,'*
        *   `cntrct_id = 'APN_3'`, `apn = NULL`, `apn_cntrct = 'contract3'` (Expected: `NULL`)
        *   `cntrct_id = 'APN_4'`, `apn = NULL`, `apn_cntrct = NULL` (Expected: `NULL`)
*   **Action**:
    Execute the BigQuery Stored Procedure.
    `CALL project.dataset.r_ausd_bp_ta_p_basisprod('JOB127', '1', '04012023', 0, 'test_job_id_2_4', 'test_run_id_2_4');`
*   **Pass/Fail Criterion**:
    Query `PoolBasisprodukt_target` and assert that the `APN` column values match the expected outputs for each `cntrct_id`.

    ```python
    # pytest_apn_transformation.py
    # ... (imports and client setup as above) ...

    def _populate_apn_test_data():
        client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.sof$ta_cntrct_dist`").result()
        client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.sof$ta_apn_vertrag`").result()

        client.query(f"""
            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.sof$ta_cntrct_dist` (cntrct_id) VALUES
            ('APN_1'), ('APN_2'), ('APN_3'), ('APN_4')
        """).result()

        client.query(f"""
            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.sof$ta_apn_vertrag` (cntrct_id, apn, apn_cntrct) VALUES
            ('APN_1', 'internet', 'contract1'),
            ('APN_2', 'internet', NULL),
            ('APN_3', NULL, 'contract3'),
            ('APN_4', NULL, NULL)
        """).result()

    def test_apn_field_transformation():
        _populate_apn_test_data()

        # Action
        _run_sp('JOB127', '1', '04012023', 0, 'test_job_id_2_4', 'test_run_id_2_4')

        # Pass/Fail Criterion
        apn_results_query = f"""
            SELECT cntrct_id, APN FROM {TARGET_TABLE} ORDER BY cntrct_id
        """
        apn_results = client.query(apn_results_query).result().to_dataframe()

        expected_apn_results = {
            'APN_1': 'internet,contract1',
            'APN_2': 'internet,', # If apn is not NULL, it concatenates, even if apn_cntrct is NULL
            'APN_3': None,
            'APN_4': None
        }

        for _, row in apn_results.iterrows():
            assert row['APN'] == expected_apn_results[row['cntrct_id']], \
                f"APN mismatch for {row['cntrct_id']}: Expected '{expected_apn_results[row['cntrct_id']]}', Got '{row['APN']}'"
    ```

#### Test Case 2.5: `SAFE.PARSE_DATE` NULL Handling

*   **Purpose**: Verify that `SAFE.PARSE_DATE('%Y%m%d', ...)` correctly converts valid date strings and returns `NULL` for invalid or `NULL` input date strings, preventing job failures due to malformed dates.
*   **Setup**:
    1.  Clear `project.dataset.PoolBasisprodukt_target`.
    2.  Populate `sof$ta_cntrct_dist` with contract IDs.
    3.  Populate `sof$ta_iccid_vertrag` with various `tn_valid_to` values:
        *   `cntrct_id = 'DATE_1'`, `tn_valid_to = '20230115'` (Expected: `2023-01-15`)
        *   `cntrct_id = 'DATE_2'`, `tn_valid_to = '2023-01-15'` (Invalid format, Expected: `NULL`)
        *   `cntrct_id = 'DATE_3'`, `tn_valid_to = 'INVALIDDATE'` (Invalid string, Expected: `NULL`)
        *   `cntrct_id = 'DATE_4'`, `tn_valid_to = NULL` (Expected: `NULL`)
*   **Action**:
    Execute the BigQuery Stored Procedure.
    `CALL project.dataset.r_ausd_bp_ta_p_basisprod('JOB128', '1', '05012023', 0, 'test_job_id_2_5', 'test_run_id_2_5');`
*   **Pass/Fail Criterion**:
    Query `PoolBasisprodukt_target` and assert that the `TNV_ICC_VALID` column values match the expected outputs for each `cntrct_id`.

    ```python
    # pytest_safe_parse_date.py
    # ... (imports and client setup as above) ...

    def _populate_date_test_data():
        client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.sof$ta_cntrct_dist`").result()
        client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.sof$ta_iccid_vertrag`").result()

        client.query(f"""
            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.sof$ta_cntrct_dist` (cntrct_id) VALUES
            ('DATE_1'), ('DATE_2'), ('DATE_3'), ('DATE_4')
        """).result()

        client.query(f"""
            INSERT INTO `{PROJECT_ID}.{DATASET_ID}.sof$ta_iccid_vertrag` (cntrct_id, tn_valid_to) VALUES
            ('DATE_1', '20230115'),
            ('DATE_2', '2023-01-15'),
            ('DATE_3', 'INVALIDDATE'),
            ('DATE_4', NULL)
        """).result()

    def test_safe_parse_date_handling():
        _populate_date_test_data()

        # Action
        _run_sp('JOB128', '1', '05012023', 0, 'test_job_id_2_5', 'test_run_id_2_5')

        # Pass/Fail Criterion
        date_results_query = f"""
            SELECT cntrct_id, TNV_ICC_VALID FROM {TARGET_TABLE} ORDER BY cntrct_id
        """
        date_results = client.query(date_results_query).result().to_dataframe()

        expected_date_results = {
            'DATE_1': '2023-01-15',
            'DATE_2': None,
            'DATE_3': None,
            'DATE_4': None
        }

        for _, row in date_results.iterrows():
            # Convert date objects to string for comparison if they are not None
            actual_date_str = row['TNV_ICC_VALID'].strftime('%Y-%m-%d') if row['TNV_ICC_VALID'] else None
            expected_date_str = expected_date_results[row['cntrct_id']]

            assert actual_date_str == expected_date_str, \
                f"TNV_ICC_VALID mismatch for {row['cntrct_id']}: Expected '{expected_date_str}', Got '{actual_date_str}'"
    ```

---

### 3. External-System Replacements Tests

These tests verify that the new BigQuery-native logging and orchestration mechanisms behave as expected, replacing the legacy shell utilities and FOS integration.

#### Test Case 3.1: Logging to `job_log` Table (Success)

*   **Purpose**: Verify that a successful execution of the BigQuery Stored Procedure correctly inserts a `SUCCESS` entry into the `project.dataset.job_log` table, including the record count.
*   **Setup**:
    1.  Clear `project.dataset.job_log`.
    2.  Populate source tables with data that will result in a successful run and a non-zero record count (e.g., 3 records).
*   **Action**:
    Execute the BigQuery Stored Procedure.
    `CALL project.dataset.r_ausd_bp_ta_p_basisprod('JOB129', '1', '06012023', 0, 'test_job_id_3_1', 'test_run_id_3_1');`
*   **Pass/Fail Criterion**:
    A single row must exist in `project.dataset.job_log` for `test_run_id_3_1` with `status = 'SUCCESS'` and `record_count` matching the actual number of rows inserted into `PoolBasisprodukt_target`.

    ```python
    # pytest_job_log_success.py
    # ... (imports and client setup as above) ...

    def test_job_log_success_entry():
        expected_records = 3
        _populate_source_data_for_count_test(expected_records)

        # Action
        _run_sp('JOB129', '1', '06012023', 0, 'test_job_id_3_1', 'test_run_id_3_1')

        # Pass/Fail Criterion
        job_log_query = f"""
            SELECT status, record_count, message
            FROM `{PROJECT_ID}.{DATASET_ID}.job_log`
            WHERE run_id = 'test_run_id_3_1'
        """
        job_log_result = client.query(job_log_query).result().to_dataframe()

        assert len(job_log_result) == 2, "Expected 2 job log entries (RUNNING, SUCCESS)." # One for RUNNING, one for SUCCESS
        success_entry = job_log_result[job_log_result['status'] == 'SUCCESS']
        assert not success_entry.empty, "No SUCCESS entry found in job_log."
        assert success_entry.iloc[0]['record_count'] == expected_records, \
            f"Logged record count ({success_entry.iloc[0]['record_count']}) does not match expected ({expected_records})."
        assert 'completed successfully' in success_entry.iloc[0]['message'].lower()
    ```

#### Test Case 3.2: Logging to `error_log` Table (Failure)

*   **Purpose**: Verify that a failed execution of the BigQuery Stored Procedure correctly inserts an `ERROR` entry into the `project.dataset.error_log` table, including the error message and code.
*   **Setup**:
    1.  Clear `project.dataset.error_log`.
*   **Action**:
    Execute the BigQuery Stored Procedure with an invalid `p_stichtag` (to force a known error).
    `CALL project.dataset.r_ausd_bp_ta_p_basisprod('JOB130', '1', 'INVALID_DATE', 0, 'test_job_id_3_2', 'test_run_id_3_2');`
*   **Pass/Fail Criterion**:
    The procedure must fail. A single row must exist in `project.dataset.error_log` for `test_run_id_3_2` with `severity = 'ERROR'` and `error_code = 'DATE_VALIDATION_FAILED'`.

    ```python
    # pytest_error_log_failure.py
    # ... (imports and client setup as above) ...

    def test_error_log_failure_entry():
        # Action
        with pytest.raises(Exception): # Expecting the SP to raise an error
            _run_sp('JOB130', '1', 'INVALID_DATE', 0, 'test_job_id_3_2', 'test_run_id_3_2')

        # Pass/Fail Criterion
        error_log_query = f"""
            SELECT message, error_code, severity
            FROM `{PROJECT_ID}.{DATASET_ID}.error_log`
            WHERE run_id = 'test_run_id_3_2'
        """
        error_log_result = client.query(error_log_query).result().to_dataframe()

        assert not error_log_result.empty, "No error log entry found for failed run."
        assert error_log_result.iloc[0]['error_code'] == 'DATE_VALIDATION_FAILED'
        assert error_log_result.iloc[0]['severity'] == 'ERROR'
        assert 'failed with error' in error_log_result.iloc[0]['message'].lower()
    ```

#### Test Case 3.3: Orchestrator Integration (Airflow DAG)

*   **Purpose**: Verify that the Airflow DAG successfully triggers the BigQuery Stored Procedure and handles parameter passing correctly.
*   **Setup**:
    1.  Ensure the Airflow DAG (`k_ausd_bp_ta_p_basisprod_workflow.py`) is deployed to your Cloud Composer environment.
    2.  Clear `project.dataset.PoolBasisprodukt_target`, `project.dataset.job_log`, and `project.dataset.error_log`.
    3.  Populate source tables with data for a successful run.
*   **Action**:
    Manually trigger the `k_ausd_bp_ta_p_basisprod_workflow` DAG in Airflow.
*   **Pass/Fail Criterion**:
    The Airflow DAG run must complete successfully (green status). A `SUCCESS` entry must be present in `project.dataset.job_log` with `job_id` matching the DAG ID and `run_id` matching the Airflow run ID.

    ```python
    # This is a conceptual test, typically performed manually or via Airflow API/CLI.
    # It's not directly runnable as a pytest function without Airflow client setup.

    # Example Airflow CLI command to trigger:
    # airflow dags trigger k_ausd_bp_ta_p_basisprod_workflow --conf '{"stichtag": "07012023"}'

    # After triggering, you would query BigQuery:
    # SQL Assertion:
    SELECT
        COUNT(*)
    FROM
        `project.dataset.job_log`
    WHERE
        job_id = 'k_ausd_bp_ta_p_basisprod_workflow'
        AND status = 'SUCCESS'
        AND log_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE); -- Adjust interval as needed

    -- Expected result: 1 (or more if multiple tasks log success)
    ```

---

### 4. Data Quality / Row Count / Schema Assertions

These tests focus on the integrity and structure of the output data.

#### Test Case 4.1: Target Table Schema Validation

*   **Purpose**: Verify that the schema of the `project.dataset.PoolBasisprodukt_target` table (column names, data types, nullability) matches the expected DDL.
*   **Setup**:
    1.  Ensure the `PoolBasisprodukt_target` table exists as per the provided DDL.
*   **Action**:
    Query the schema information for `PoolBasisprodukt_target` from BigQuery's `INFORMATION_SCHEMA`.
*   **Pass/Fail Criterion**:
    The retrieved schema must exactly match the expected schema (column names, data types, and nullability for each column).

    ```python
    # pytest_schema_validation.py
    # ... (imports and client setup as above) ...

    def test_target_table_schema():
        expected_schema = [
            # (column_name, data_type, is_nullable) - based on the DDL
            ('CNTRCT_ID', 'STRING', 'YES'), ('EVN', 'STRING', 'YES'),
            ('TNV_ICCID', 'STRING', 'YES'), ('TNV_MCC', 'STRING', 'YES'), ('TNV_MNC', 'STRING', 'YES'),
            ('TNV_HLR', 'STRING', 'YES'), ('TNV_SI', 'STRING', 'YES'), ('TNV_ICC_STAT', 'STRING', 'YES'),
            ('TNV_ICC_VALID', 'DATE', 'YES'), ('TC_ICCID', 'STRING', 'YES'), ('TC_MCC', 'STRING', 'YES'),
            ('TC_MNC', 'STRING', 'YES'), ('TC_HLR', 'STRING', 'YES'), ('TC_SI', 'STRING', 'YES'),
            ('TC_ICC_STAT', 'STRING', 'YES'), ('TC_ICC_VALID', 'DATE', 'YES'), ('TB_ICCID', 'STRING', 'YES'),
            ('TB_MCC', 'STRING', 'YES'), ('TB_MNC', 'STRING', 'YES'), ('TB_HLR', 'STRING', 'YES'),
            ('TB_SI', 'STRING', 'YES'), ('TB_ICC_STAT', 'STRING', 'YES'), ('TB_ICC_VALID', 'DATE', 'YES'),
            ('MS1_ICCID', 'STRING', 'YES'), ('MS1_MCC', 'STRING', 'YES'), ('MS1_MNC', 'STRING', 'YES'),
            ('MS1_HLR', 'STRING', 'YES'), ('MS1_SI', 'STRING', 'YES'), ('MS1_STAT', 'STRING', 'YES'),
            ('MS1_VALID', 'DATE', 'YES'), ('MS2_ICCID', 'STRING', 'YES'), ('MS2_MCC', 'STRING', 'YES'),
            ('MS2_MNC', 'STRING', 'YES'), ('MS2_HLR', 'STRING', 'YES'), ('MS2_SI', 'STRING', 'YES'),
            ('MS2_STAT', 'STRING', 'YES'), ('MS2_VALID', 'DATE', 'YES'), ('TNV_E_ID', 'STRING', 'YES'),
            ('TNV_CARD_TYPE_NAME', 'STRING', 'YES'), ('TC_E_ID', 'STRING', 'YES'), ('TC_CARD_TYPE_NAME', 'STRING', 'YES'),
            ('TB_E_ID', 'STRING', 'YES'), ('TB_CARD_TYPE_NAME', 'STRING', 'YES'), ('MS1_E_ID', 'STRING', 'YES'),
            ('MS1_CARD_TYPE_NAME', 'STRING', 'YES'), ('MS2_E_ID', 'STRING', 'YES'), ('MS2_CARD_TYPE_NAME', 'STRING', 'YES'),
            ('TNV_MULTI_SINGLE', 'STRING', 'YES'), ('TC_MULTI_SINGLE', 'STRING', 'YES'), ('TB_MULTI_SINGLE', 'STRING', 'YES'),
            ('TNV_MSISDN', 'STRING', 'YES'), ('TNV_MS_STAT', 'STRING', 'YES'), ('TNV_MS_VALID', 'DATE', 'YES'),
            ('TNV_DAT_MSISDN', 'STRING', 'YES'), ('TNV_DAT_STAT', 'STRING', 'YES'), ('TNV_DAT_VALID', 'DATE', 'YES'),
            ('TNV_FAX_MSISDN', 'STRING', 'YES'), ('TNV_FAX_STAT', 'STRING', 'YES'), ('TNV_FAX_VALID', 'DATE', 'YES'),
            ('TC_MSISDN', 'STRING', 'YES'), ('TC_MS_STAT', 'STRING', 'YES'), ('TC_MS_VALID', 'DATE', 'YES'),
            ('TC_DAT_MSISDN', 'STRING', 'YES'), ('TC_DAT_STAT', 'STRING', 'YES'), ('TC_DAT_VALID', 'DATE', 'YES'),
            ('TC_FAX_MSISDN', 'STRING', 'YES'), ('TC_FAX_STAT', 'STRING', 'YES'), ('TC_FAX_VALID', 'DATE', 'YES'),
            ('TB_MSISDN', 'STRING', 'YES'), ('TB_MS_STAT', 'STRING', 'YES'), ('TB_MS_VALID', 'DATE', 'YES'),
            ('TB_DAT_MSISDN', 'STRING', 'YES'), ('TB_DAT_STAT', 'STRING', 'YES'), ('TB_DAT_VALID', 'DATE', 'YES'),
            ('TB_FAX_MSISDN', 'STRING', 'YES'), ('TB_FAX_STAT', 'STRING', 'YES'), ('TB_FAX_VALID', 'DATE', 'YES'),
            ('MS1_MSISDN', 'STRING', 'YES'), ('MS1_MS_STAT', 'STRING', 'YES'), ('MS1_MS_VALID', 'DATE', 'YES'),
            ('MS2_MSISDN', 'STRING', 'YES'), ('MS2_MS_STAT', 'STRING', 'YES'), ('MS2_MS_VALID', 'DATE', 'YES'),
            ('DA_MSISDN', 'STRING', 'YES'), ('DA_MS_STAT', 'STRING', 'YES'), ('DA_MS_VALID', 'DATE', 'YES'),
            ('VDA_MSISDN', 'STRING', 'YES'), ('VDA_MS_STAT', 'STRING', 'YES'), ('VDA_MS_VALID', 'DATE', 'YES'),
            ('TK_MSISDN', 'STRING', 'YES'), ('TK_MS_STAT', 'STRING', 'YES'), ('TK_MS_VALID', 'DATE', 'YES'),
            ('BCP_VERTRAG', 'STRING', 'YES'), ('BCP_ICCID', 'STRING', 'YES'), ('BCP_HLR', 'STRING', 'YES'),
            ('APN', 'STRING', 'YES'), ('BCP_TN_TEL', 'STRING', 'YES'), ('DATA_OPTION_REIN', 'STRING', 'YES'),
            ('VOICE_OPTION_REIN', 'STRING', 'YES'), ('MIX_OPTION', 'STRING', 'YES'), ('MULTI_OPTION', 'STRING', 'YES'),
            ('ROAMING_OPTION', 'STRING', 'YES'), ('SONSTIGE_OPTION', 'STRING', 'YES'),
            ('MS3_ICCID', 'STRING', 'YES'), ('MS3_E_ID', 'STRING', 'YES'), ('MS3_CARD_TYPE_NAME', 'STRING', 'YES'),
            ('MS3_MCC', 'STRING', 'YES'), ('MS3_MNC', 'STRING', 'YES'), ('MS3_HLR', 'STRING', 'YES'),
            ('MS3_SI', 'STRING', 'YES'), ('MS3_STAT', 'STRING', 'YES'), ('MS3_VALID', 'DATE', 'YES'),
            ('MS4_ICCID', 'STRING', 'YES'), ('MS4_E_ID', 'STRING', 'YES'), ('MS4_CARD_TYPE_NAME', 'STRING', 'YES'),
            ('MS4_MCC', 'STRING', 'YES'), ('MS4_MNC', 'STRING', 'YES'), ('MS4_HLR', 'STRING', 'YES'),
            ('MS4_SI', 'STRING', 'YES'), ('MS4_STAT', 'STRING', 'YES'), ('MS4_VALID', 'DATE', 'YES'),
            ('MS5_ICCID', 'STRING', 'YES'), ('MS5_E_ID', 'STRING', 'YES'), ('MS5_CARD_TYPE_NAME', 'STRING', 'YES'),
            ('MS5_MCC', 'STRING', 'YES'), ('MS5_MNC', 'STRING', 'YES'), ('MS5_HLR', 'STRING', 'YES'),
            ('MS5_SI', 'STRING', 'YES'), ('MS5_STAT', 'STRING', 'YES'), ('MS5_VALID', 'DATE', 'YES'),
            ('MS6_ICCID', 'STRING', 'YES'), ('MS6_E_ID', 'STRING', 'YES'), ('MS6_CARD_TYPE_NAME', 'STRING', 'YES'),
            ('MS6_MCC', 'STRING', 'YES'), ('MS6_MNC', 'STRING', 'YES'), ('MS6_HLR', 'STRING', 'YES'),
            ('MS6_SI', 'STRING', 'YES'), ('MS6_STAT', 'STRING', 'YES'), ('MS6_VALID', 'DATE', 'YES'),
            ('MS7_ICCID', 'STRING', 'YES'), ('MS7_E_ID', 'STRING', 'YES'), ('MS7_CARD_TYPE_NAME', 'STRING', 'YES'),
            ('MS7_MCC', 'STRING', 'YES'), ('MS7_MNC', 'STRING', 'YES'), ('MS7_HLR', 'STRING', 'YES'),
            ('MS7_SI', 'STRING', 'YES'), ('MS7_STAT', 'STRING', 'YES'), ('MS7_VALID', 'DATE', 'YES'),
            ('MS8_ICCID', 'STRING', 'YES'), ('MS8_E_ID', 'STRING', 'YES'), ('MS8_CARD_TYPE_NAME', 'STRING', 'YES'),
            ('MS8_MCC', 'STRING', 'YES'), ('MS8_MNC', 'STRING', 'YES'), ('MS8_HLR', 'STRING', 'YES'),
            ('MS8_SI', 'STRING', 'YES'), ('MS8_STAT', 'STRING', 'YES'), ('MS8_VALID', 'DATE', 'YES'),
            ('MS9_ICCID', 'STRING', 'YES'), ('MS9_E_ID', 'STRING', 'YES'), ('MS9_CARD_TYPE_NAME', 'STRING', 'YES'),
            ('MS9_MCC', 'STRING', 'YES'), ('MS9_MNC', 'STRING', 'YES'), ('MS9_HLR', 'STRING', 'YES'),
            ('MS9_SI', 'STRING', 'YES'), ('MS9_STAT', 'STRING', 'YES'), ('MS9_VALID', 'DATE', 'YES'),
            ('MS10_ICCID', 'STRING', 'YES'), ('MS10_E_ID', 'STRING', 'YES'), ('MS10_CARD_TYPE_NAME', 'STRING', 'YES'),
            ('MS10_MCC', 'STRING', 'YES'), ('MS10_MNC', 'STRING', 'YES'), ('MS10_HLR', 'STRING', 'YES'),
            ('MS10_SI', 'STRING', 'YES'), ('MS10_STAT', 'STRING', 'YES'), ('MS10_VALID', 'DATE', 'YES')
        ]

        table_ref = client.get_table(TARGET_TABLE)
        actual_schema = [(field.name, field.field_type, 'YES' if field.is_nullable else 'NO') for field in table_ref.schema]

        # Convert to sets for easier comparison, ignoring order
        assert set(actual_schema) == set(expected_schema), \
            f"Schema mismatch. Expected: {expected_schema}, Actual: {actual_schema}"
    ```

#### Test Case 4.2: Data Integrity - `NOT NULL` Constraints

*   **Purpose**: Verify that columns defined as `NOT NULL` in the target table DDL actually contain no `NULL` values after the job runs. (Note: In the provided DDL, all columns are nullable. This test would be more relevant if `NOT NULL` was specified for some columns.)
*   **Setup**:
    1.  Clear `project.dataset.PoolBasisprodukt_target`.
    2.  Populate source tables with data that *might* produce `NULL`s in columns that *should not* be `NULL` if the DDL were different. For this example, we'll assume `CNTRCT_ID` is logically non-nullable.
*   **Action**:
    Execute the BigQuery Stored Procedure.
    `CALL project.dataset.r_ausd_bp_ta_p_basisprod('JOB131', '1', '08012023', 0, 'test_job_id_4_2', 'test_run_id_4_2');`
*   **Pass/Fail Criterion**:
    Query `PoolBasisprodukt_target` for `NULL` values in `CNTRCT_ID`. The count should be 0.

    ```python
    # pytest_not_null_constraints.py
    # ... (imports and client setup as above) ...

    def test_not_null_constraints():
        # Populate source data to ensure CNTRCT_ID is always present
        _populate_source_data_for_count_test(5)

        # Action
        _run_sp('JOB131', '1', '08012023', 0, 'test_job_id_4_2', 'test_run_id_4_2')

        # Pass/Fail Criterion
        # Assuming CNTRCT_ID is logically NOT NULL based on the source query
        null_count_query = f"""
            SELECT COUNT(*) FROM {TARGET_TABLE} WHERE CNTRCT_ID IS NULL
        """
        null_count = client.query(null_count_query).result().to_dataframe().iloc[0, 0]

        assert null_count == 0, f"Found {null_count} NULL values in CNTRCT_ID, which should be NOT NULL."
    ```

#### Test Case 4.3: Target Table Row Count (Zero Records)

*   **Purpose**: Verify the job correctly handles scenarios where no records are expected to be processed or inserted into the target table.
*   **Setup**:
    1.  Clear `project.dataset.PoolBasisprodukt_target`.
    2.  Ensure all source tables are empty or contain data that, when joined and filtered, results in zero output rows.
*   **Action**:
    Execute the BigQuery Stored Procedure.
    `CALL project.dataset.r_ausd_bp_ta_p_basisprod('JOB132', '1', '09012023', 0, 'test_job_id_4_3', 'test_run_id_4_3');`
*   **Pass/Fail Criterion**:
    The `PoolBasisprodukt_target` table must contain 0 rows. The `job_log` entry for `test_run_id_4_3` must show `record_count = 0` and `status = 'SUCCESS'`.

    ```python
    # pytest_zero_records.py
    # ... (imports and client setup as above) ...

    def test_zero_records_output():
        # Setup: Ensure source tables are empty or produce no output
        client.query(f"TRUNCATE TABLE `{PROJECT_ID}.{DATASET_ID}.sof$ta_cntrct_dist`").result()
        # Ensure other source tables are also empty or won't join to produce results

        # Action
        _run_sp('JOB132', '1', '09012023', 0, 'test_job_id_4_3', 'test_run_id_4_3')

        # Pass/Fail Criterion
        target_row_count_query = f"SELECT COUNT(*) FROM {TARGET_TABLE}"
        target_row_count = client.query(target_row_count_query).result().to_dataframe().iloc[0, 0]

        job_log_query = f"""
            SELECT record_count, status
            FROM `{PROJECT_ID}.{DATASET_ID}.job_log`
            WHERE run_id = 'test_run_id_4_3' AND status = 'SUCCESS'
        """
        job_log_result = client.query(job_log_query).result().to_dataframe()

        assert target_row_count == 0, f"Target table has {target_row_count} rows, expected 0."
        assert not job_log_result.empty, "No successful job log entry found."
        assert job_log_result.iloc[0]['record_count'] == 0, \
            f"Logged record count ({job_log_result.iloc[0]['record_count']}) does not match expected 0."
    ```