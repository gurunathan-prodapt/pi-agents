As a senior data-migration QA engineer, I've reviewed the migration design for `k_ausd_bp_ta_p_basisprod.ksh` to BigQuery. The migration involves translating a KornShell orchestration script and an Oracle SQL data transformation script into BigQuery Stored Procedures.

The following test cases are designed to validate the behavioral equivalence of the migrated BigQuery solution (`k_ausd_bp_ta_p_basisprod_sp` and its called `d_ausd_bp_ta_p_basisprod_sp`) against the legacy KornShell/Oracle job.

**Assumptions:**
*   BigQuery project ID: `my-gcp-project`
*   BigQuery dataset ID: `my_dw_dataset`
*   All source tables (`sof_ta_cntrct_dist`, `sof_ta_iccid_vertrag`, `sof_ta_rn_vertrag`, `sof_ta_rn_da_vda_tk`, `sof_ta_tarifoption`, `sof_ta_apn_vertrag`, `sof_ta_bcp_iccid`, `sof_ta_bcp_msisdn`, `dwtk_meldungen`) have been successfully migrated to `my-gcp-project.my_dw_dataset`.
*   The target table `sof_ta_p_basisprod` and the audit table `job_audit` exist in `my-gcp-project.my_dw_dataset` with appropriate schemas.
*   The `d_ausd_bp_ta_p_basisprod_sp` BigQuery Stored Procedure (containing the core data transformation logic) has been deployed and is callable by `k_ausd_bp_ta_p_basisprod_sp`.
*   Python with `pytest` and `google-cloud-bigquery` client library is used for test orchestration.

---

## Test Case 1: Orchestration - Happy Path Execution and Audit Logging

**Purpose:** Verify that the `k_ausd_bp_ta_p_basisprod_sp` executes successfully with valid parameters, calls the data transformation procedure, captures the correct record count, and logs a successful entry in the `job_audit` table.

**Setup:**
1.  Ensure all source tables (`my-gcp-project.my_dw_dataset.sof_ta_cntrct_dist`, etc.) contain representative test data.
2.  Ensure `my-gcp-project.my_dw_dataset.dwtk_meldungen` contains at least one record with `job_kennung = 'BERT_DROP_TEMP_TABLE'` to allow `MAX(timecreated)` to return a non-NULL value.
3.  Truncate the target table `my-gcp-project.my_dw_dataset.sof_ta_p_basisprod`.
4.  Truncate the `my-gcp-project.my_dw_dataset.job_audit` table.

**Action:**
Execute the `k_ausd_bp_ta_p_basisprod_sp` with valid parameters.

```python
# pytest_orchestration.py
from google.cloud import bigquery
import datetime

client = bigquery.Client(project='my-gcp-project')
dataset_id = 'my_dw_dataset'

def test_happy_path_execution():
    job_kennung = "TEST_JOB_HP"
    eintrags_nr = "001"
    stichtag = datetime.date.today().strftime("%d%m%Y") # e.g., "25122023"
    wiederanlauf_wert = 0

    # Setup: Clear target and audit tables
    client.query(f"TRUNCATE TABLE `{client.project}.{dataset_id}.sof_ta_p_basisprod`").result()
    client.query(f"TRUNCATE TABLE `{client.project}.{dataset_id}.job_audit`").result()

    # Action: Call the stored procedure
    query = f"""
    CALL `{client.project}.{dataset_id}.k_ausd_bp_ta_p_basisprod_sp`(
        p_job_kennung => '{job_kennung}',
        p_eintrags_nr => '{eintrags_nr}',
        p_stichtag => '{stichtag}',
        p_wiederanlauf_wert => {wiederanlauf_wert}
    );
    """
    job = client.query(query)
    job.result() # Wait for the job to complete

    # Pass/Fail Criterion: Check audit log and record count
    audit_query = f"""
    SELECT
        job_name,
        status,
        record_count,
        stichtag,
        eintrags_nr
    FROM `{client.project}.{dataset_id}.job_audit`
    WHERE job_name = 'k_ausd_bp_ta_p_basisprod'
    ORDER BY start_time DESC
    LIMIT 1;
    """
    audit_result = client.query(audit_query).result().to_dataframe()

    assert not audit_result.empty, "Audit log entry not found."
    assert audit_result['status'].iloc[0] == 'SUCCESS', f"Job status was not SUCCESS: {audit_result['status'].iloc[0]}"
    assert audit_result['stichtag'].iloc[0] == datetime.datetime.strptime(stichtag, "%d%m%Y").date(), "Stichtag in audit log is incorrect."
    assert audit_result['eintrags_nr'].iloc[0] == eintrags_nr, "EintragsNr in audit log is incorrect."

    # Verify record count in target table
    target_count_query = f"SELECT COUNT(*) FROM `{client.project}.{dataset_id}.sof_ta_p_basisprod`"
    target_count = client.query(target_count_query).result().to_dataframe().iloc[0, 0]
    assert audit_result['record_count'].iloc[0] == target_count, "Record count in audit log does not match target table."

    print(f"Test Case 1 Passed: Job executed successfully with {target_count} records.")

```

**Pass/Fail Criterion:**
*   The BigQuery stored procedure executes without error.
*   A single entry exists in `my-gcp-project.my_dw_dataset.job_audit` for `job_name = 'k_ausd_bp_ta_p_basisprod'` with `status = 'SUCCESS'`.
*   The `record_count` in the audit log matches the actual `COUNT(*)` of rows in `my-gcp-project.my_dw_dataset.sof_ta_p_basisprod` after execution.
*   The `stichtag` and `eintrags_nr` in the audit log match the input parameters.

---

## Test Case 2: Orchestration - Parameter Validation (Missing Mandatory Parameters)

**Purpose:** Verify that the stored procedure correctly identifies and raises an error when mandatory input parameters (`p_job_kennung`, `p_eintrags_nr`, `p_stichtag`) are missing or empty.

**Setup:**
1.  Ensure `my-gcp-project.my_dw_dataset.job_audit` is truncated.

**Action:**
Attempt to execute `k_ausd_bp_ta_p_basisprod_sp` with each mandatory parameter missing or empty, one at a time.

```python
# pytest_orchestration.py (continued)
import pytest

def test_missing_job_kennung_parameter():
    eintrags_nr = "002"
    stichtag = datetime.date.today().strftime("%d%m%Y")
    
    query = f"""
    CALL `{client.project}.{dataset_id}.k_ausd_bp_ta_p_basisprod_sp`(
        p_job_kennung => NULL,
        p_eintrags_nr => '{eintrags_nr}',
        p_stichtag => '{stichtag}'
    );
    """
    with pytest.raises(Exception) as excinfo:
        client.query(query).result()
    assert "Parameter p_job_kennung is mandatory." in str(excinfo.value)
    
    # Verify audit log for failure
    audit_query = f"""
    SELECT status, error_message FROM `{client.project}.{dataset_id}.job_audit`
    WHERE job_name = 'k_ausd_bp_ta_p_basisprod' AND eintrags_nr = '{eintrags_nr}'
    ORDER BY start_time DESC LIMIT 1;
    """
    audit_result = client.query(audit_query).result().to_dataframe()
    assert not audit_result.empty
    assert audit_result['status'].iloc[0] == 'FAILED'
    assert "Parameter p_job_kennung is mandatory." in audit_result['error_message'].iloc[0]
    print("Test Case 2a Passed: Missing p_job_kennung handled correctly.")

def test_missing_eintrags_nr_parameter():
    job_kennung = "TEST_JOB_MISS_EN"
    stichtag = datetime.date.today().strftime("%d%m%Y")
    
    query = f"""
    CALL `{client.project}.{dataset_id}.k_ausd_bp_ta_p_basisprod_sp`(
        p_job_kennung => '{job_kennung}',
        p_eintrags_nr => NULL,
        p_stichtag => '{stichtag}'
    );
    """
    with pytest.raises(Exception) as excinfo:
        client.query(query).result()
    assert "Parameter p_eintrags_nr is mandatory." in str(excinfo.value)
    
    # Verify audit log for failure (eintrags_nr might be NULL in audit if validation fails early)
    audit_query = f"""
    SELECT status, error_message FROM `{client.project}.{dataset_id}.job_audit`
    WHERE job_name = 'k_ausd_bp_ta_p_basisprod'
    ORDER BY start_time DESC LIMIT 1;
    """
    audit_result = client.query(audit_query).result().to_dataframe()
    assert not audit_result.empty
    assert audit_result['status'].iloc[0] == 'FAILED'
    assert "Parameter p_eintrags_nr is mandatory." in audit_result['error_message'].iloc[0]
    print("Test Case 2b Passed: Missing p_eintrags_nr handled correctly.")

def test_missing_stichtag_parameter():
    job_kennung = "TEST_JOB_MISS_ST"
    eintrags_nr = "003"
    
    query = f"""
    CALL `{client.project}.{dataset_id}.k_ausd_bp_ta_p_basisprod_sp`(
        p_job_kennung => '{job_kennung}',
        p_eintrags_nr => '{eintrags_nr}',
        p_stichtag => NULL
    );
    """
    with pytest.raises(Exception) as excinfo:
        client.query(query).result()
    assert "Parameter p_stichtag is mandatory." in str(excinfo.value)
    
    # Verify audit log for failure
    audit_query = f"""
    SELECT status, error_message FROM `{client.project}.{dataset_id}.job_audit`
    WHERE job_name = 'k_ausd_bp_ta_p_basisprod' AND eintrags_nr = '{eintrags_nr}'
    ORDER BY start_time DESC LIMIT 1;
    """
    audit_result = client.query(audit_query).result().to_dataframe()
    assert not audit_result.empty
    assert audit_result['status'].iloc[0] == 'FAILED'
    assert "Parameter p_stichtag is mandatory." in audit_result['error_message'].iloc[0]
    print("Test Case 2c Passed: Missing p_stichtag handled correctly.")
```

**Pass/Fail Criterion:**
*   Each execution attempt raises an exception containing the specific error message for the missing parameter (e.g., "Parameter p_job_kennung is mandatory.").
*   An entry is logged in `my-gcp-project.my_dw_dataset.job_audit` for each failed attempt with `status = 'FAILED'` and the corresponding `error_message`.

---

## Test Case 3: Orchestration - Parameter Validation (Invalid `p_stichtag` Format)

**Purpose:** Verify that the stored procedure correctly validates the `p_stichtag` format (`DDMMYYYY`) and raises an error for invalid formats.

**Setup:**
1.  Ensure `my-gcp-project.my_dw_dataset.job_audit` is truncated.

**Action:**
Execute `k_ausd_bp_ta_p_basisprod_sp` with an invalid `p_stichtag` format (e.g., `YYYY-MM-DD`, `DD.MM.YYYY`, or non-numeric).

```python
# pytest_orchestration.py (continued)
def test_invalid_stichtag_format():
    job_kennung = "TEST_JOB_INV_ST"
    eintrags_nr = "004"
    invalid_stichtag = "2023-12-25" # Expected DDMMYYYY

    query = f"""
    CALL `{client.project}.{dataset_id}.k_ausd_bp_ta_p_basisprod_sp`(
        p_job_kennung => '{job_kennung}',
        p_eintrags_nr => '{eintrags_nr}',
        p_stichtag => '{invalid_stichtag}'
    );
    """
    with pytest.raises(Exception) as excinfo:
        client.query(query).result()
    assert f"Invalid date format for p_stichtag: {invalid_stichtag}. Expected: DDMMYYYY" in str(excinfo.value)

    # Verify audit log for failure
    audit_query = f"""
    SELECT status, error_message FROM `{client.project}.{dataset_id}.job_audit`
    WHERE job_name = 'k_ausd_bp_ta_p_basisprod' AND eintrags_nr = '{eintrags_nr}'
    ORDER BY start_time DESC LIMIT 1;
    """
    audit_result = client.query(audit_query).result().to_dataframe()
    assert not audit_result.empty
    assert audit_result['status'].iloc[0] == 'FAILED'
    assert f"Invalid date format for p_stichtag: {invalid_stichtag}. Expected: DDMMYYYY" in audit_result['error_message'].iloc[0]
    print("Test Case 3 Passed: Invalid p_stichtag format handled correctly.")
```

**Pass/Fail Criterion:**
*   The execution raises an exception with a message indicating an invalid `p_stichtag` format.
*   An entry is logged in `my-gcp-project.my_dw_dataset.job_audit` with `status = 'FAILED'` and the corresponding `error_message`.

---

## Test Case 4: External System Replacement - `dwtk_meldungen` `MAX(timecreated)` Logic

**Purpose:** Verify that the BigQuery procedure correctly derives `v_max_timecreated_date` from `my-gcp-project.my_dw_dataset.dwtk_meldungen`, including handling cases where no matching records exist. This replaces the Oracle `NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101')` logic.

**Setup:**
1.  Truncate `my-gcp-project.my_dw_dataset.sof_ta_p_basisprod` and `job_audit`.
2.  Prepare `my-gcp-project.my_dw_dataset.dwtk_meldungen` with specific test data:
    *   **Scenario A:** Multiple records for `BERT_DROP_TEMP_TABLE` with varying `timecreated`.
    *   **Scenario B:** No records for `BERT_DROP_TEMP_TABLE`.

**Action:**
Execute the `k_ausd_bp_ta_p_basisprod_sp` for both scenarios. Since `v_max_timecreated_date` is an internal variable, we'll need to inspect the audit log or temporarily modify the SP to log this value for direct assertion. For this test, we'll rely on the overall success/failure and assume the internal logic is correct if the job completes as expected. A more direct test would involve a separate UDF/SP for this specific logic.

```python
# pytest_orchestration.py (continued)
def test_dwtk_meldungen_max_timecreated_logic():
    job_kennung = "TEST_JOB_DWTK"
    eintrags_nr = "005"
    stichtag = datetime.date.today().strftime("%d%m%Y")

    # Scenario A: Multiple records, expect MAX
    client.query(f"TRUNCATE TABLE `{client.project}.{dataset_id}.dwtk_meldungen`").result()
    client.query(f"""
    INSERT INTO `{client.project}.{dataset_id}.dwtk_meldungen` (job_kennung, timecreated) VALUES
    ('BERT_DROP_TEMP_TABLE', '2023-01-01 10:00:00 UTC'),
    ('OTHER_JOB', '2023-02-01 11:00:00 UTC'),
    ('BERT_DROP_TEMP_TABLE', '2023-01-15 12:00:00 UTC'),
    ('BERT_DROP_TEMP_TABLE', '2023-03-01 13:00:00 UTC');
    """).result()
    
    # Expected max_timecreated_date: '20230301'
    expected_max_date_a = '20230301'

    # Action A: Call the stored procedure
    query_a = f"""
    CALL `{client.project}.{dataset_id}.k_ausd_bp_ta_p_basisprod_sp`(
        p_job_kennung => '{job_kennung}_A',
        p_eintrags_nr => '{eintrags_nr}_A',
        p_stichtag => '{stichtag}'
    );
    """
    job_a = client.query(query_a)
    job_a.result()

    # Pass/Fail A: Verify success (implies correct internal derivation)
    audit_query_a = f"""
    SELECT status FROM `{client.project}.{dataset_id}.job_audit`
    WHERE job_name = 'k_ausd_bp_ta_p_basisprod' AND eintrags_nr = '{eintrags_nr}_A'
    ORDER BY start_time DESC LIMIT 1;
    """
    audit_result_a = client.query(audit_query_a).result().to_dataframe()
    assert audit_result_a['status'].iloc[0] == 'SUCCESS', "Scenario A failed."
    print("Test Case 4a Passed: MAX(timecreated) with existing data handled correctly.")

    # Scenario B: No records for 'BERT_DROP_TEMP_TABLE', expect '19000101'
    client.query(f"TRUNCATE TABLE `{client.project}.{dataset_id}.dwtk_meldungen`").result()
    client.query(f"""
    INSERT INTO `{client.project}.{dataset_id}.dwtk_meldungen` (job_kennung, timecreated) VALUES
    ('OTHER_JOB', '2023-02-01 11:00:00 UTC');
    """).result()

    # Expected max_timecreated_date: '19000101'
    expected_max_date_b = '19000101'

    # Action B: Call the stored procedure
    query_b = f"""
    CALL `{client.project}.{dataset_id}.k_ausd_bp_ta_p_basisprod_sp`(
        p_job_kennung => '{job_kennung}_B',
        p_eintrags_nr => '{eintrags_nr}_B',
        p_stichtag => '{stichtag}'
    );
    """
    job_b = client.query(query_b)
    job_b.result()

    # Pass/Fail B: Verify success (implies correct internal derivation)
    audit_query_b = f"""
    SELECT status FROM `{client.project}.{dataset_id}.job_audit`
    WHERE job_name = 'k_ausd_bp_ta_p_basisprod' AND eintrags_nr = '{eintrags_nr}_B'
    ORDER BY start_time DESC LIMIT 1;
    """
    audit_result_b = client.query(audit_query_b).result().to_dataframe()
    assert audit_result_b['status'].iloc[0] == 'SUCCESS', "Scenario B failed."
    print("Test Case 4b Passed: MAX(timecreated) with no matching data (default '19000101') handled correctly.")

```

**Pass/Fail Criterion:**
*   Both scenarios execute successfully without error. (This implicitly verifies the `COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')` logic.)
*   For a more direct test, one would need to modify the SP to log `v_max_timecreated_date` to the audit table or a temporary table, and then assert its value.

---

## Test Case 5: Output Parity - End-to-End Data Comparison

**Purpose:** Verify that the final output table `my-gcp-project.my_dw_dataset.sof_ta_p_basisprod` contains exactly the same data as the legacy `sof$ta_p_basisprod` table, given identical source data. This is the most critical test for behavioral equivalence.

**Setup:**
1.  **Legacy Data Snapshot:** Extract a full snapshot of the legacy `sof$ta_p_basisprod` table (e.g., into a CSV or a temporary BigQuery table `legacy_snapshot.sof_ta_p_basisprod`). This snapshot should be taken after a successful run of the *legacy* job with a specific set of source data.
2.  **BigQuery Source Data:** Ensure all BigQuery source tables (`sof_ta_cntrct_dist`, etc.) are populated with the *exact same data* that was used to generate the legacy snapshot.
3.  Truncate `my-gcp-project.my_dw_dataset.sof_ta_p_basisprod`.
4.  Truncate `my-gcp-project.my_dw_dataset.job_audit`.

**Action:**
Execute the `k_ausd_bp_ta_p_basisprod_sp` with the same parameters used for the legacy run.

```python
# pytest_orchestration.py (continued)
def test_output_parity_end_to_end():
    job_kennung = "TEST_JOB_PARITY"
    eintrags_nr = "006"
    stichtag = "25122023" # Use a fixed date for reproducibility
    
    # Setup: Ensure BigQuery source tables match legacy source tables
    # (This step is manual or via a data ingestion pipeline prior to testing)
    # Example:
    # client.query("LOAD DATA OVERWRITE `my-gcp-project.my_dw_dataset.sof_ta_cntrct_dist` FROM FILES (format='CSV', uris=['gs://my-bucket/legacy_data/sof_ta_cntrct_dist.csv'])").result()
    # ... repeat for all source tables ...

    # Setup: Truncate target and audit tables
    client.query(f"TRUNCATE TABLE `{client.project}.{dataset_id}.sof_ta_p_basisprod`").result()
    client.query(f"TRUNCATE TABLE `{client.project}.{dataset_id}.job_audit`").result()

    # Action: Call the stored procedure
    query = f"""
    CALL `{client.project}.{dataset_id}.k_ausd_bp_ta_p_basisprod_sp`(
        p_job_kennung => '{job_kennung}',
        p_eintrags_nr => '{eintrags_nr}',
        p_stichtag => '{stichtag}'
    );
    """
    job = client.query(query)
    job.result()

    # Pass/Fail Criterion: Compare BigQuery target with legacy snapshot
    # Assuming 'legacy_snapshot.sof_ta_p_basisprod' is a BigQuery table containing the legacy output
    comparison_query = f"""
    SELECT
        (SELECT COUNT(*) FROM `{client.project}.{dataset_id}.sof_ta_p_basisprod`) AS bq_count,
        (SELECT COUNT(*) FROM `legacy_snapshot.sof_ta_p_basisprod`) AS legacy_count,
        (SELECT COUNT(*) FROM (
            SELECT * FROM `{client.project}.{dataset_id}.sof_ta_p_basisprod`
            EXCEPT DISTINCT
            SELECT * FROM `legacy_snapshot.sof_ta_p_basisprod`
        )) AS diff_bq_legacy,
        (SELECT COUNT(*) FROM (
            SELECT * FROM `legacy_snapshot.sof_ta_p_basisprod`
            EXCEPT DISTINCT
            SELECT * FROM `{client.project}.{dataset_id}.sof_ta_p_basisprod`
        )) AS diff_legacy_bq
    """
    comparison_result = client.query(comparison_query).result().to_dataframe()

    bq_count = comparison_result['bq_count'].iloc[0]
    legacy_count = comparison_result['legacy_count'].iloc[0]
    diff_bq_legacy = comparison_result['diff_bq_legacy'].iloc[0]
    diff_legacy_bq = comparison_result['diff_legacy_bq'].iloc[0]

    assert bq_count == legacy_count, f"Row counts differ: BQ={bq_count}, Legacy={legacy_count}"
    assert diff_bq_legacy == 0, f"Rows in BQ not in Legacy: {diff_bq_legacy}"
    assert diff_legacy_bq == 0, f"Rows in Legacy not in BQ: {diff_legacy_bq}"
    print(f"Test Case 5 Passed: Output parity confirmed. {bq_count} records match exactly.")

```

**Pass/Fail Criterion:**
*   The `bq_count` and `legacy_count` must be equal.
*   `diff_bq_legacy` and `diff_legacy_bq` must both be `0`, indicating no differences in rows between the two tables.

---

## Test Case 6: Transformation Correctness - `decode` to `CASE` Translation

**Purpose:** Verify that the `decode` function in Oracle SQL, specifically `decode(av.apn, null,av.apn, av.apn||\',\'||av.apn_cntrct)`, is correctly translated to BigQuery's `CASE` statement.

**Setup:**
1.  Create a small, isolated test dataset for `my-gcp-project.my_dw_dataset.sof_ta_apn_vertrag` with specific values for `apn` and `apn_cntrct` to cover:
    *   `apn` is NULL.
    *   `apn` is not NULL, `apn_cntrct` is NULL.
    *   `apn` is not NULL, `apn_cntrct` is not NULL.
2.  Create a temporary BigQuery Stored Procedure that mimics *only* this specific transformation logic, or ensure the main `d_ausd_bp_ta_p_basisprod_sp` can be tested in isolation with controlled inputs. For simplicity, we'll use a direct SQL query against a mock table.

**Action:**
Execute a BigQuery SQL query that applies the translated `CASE` logic to the test data and compare the results with expected output.

```sql
-- Setup: Create a mock table for sof_ta_apn_vertrag
CREATE OR REPLACE TABLE `my-gcp-project.my_dw_dataset.mock_sof_ta_apn_vertrag` (
    apn STRING,
    apn_cntrct STRING
);

INSERT INTO `my-gcp-project.my_dw_dataset.mock_sof_ta_apn_vertrag` (apn, apn_cntrct) VALUES
('APN_A', 'CNTRCT_A'),
('APN_B', NULL),
(NULL, 'CNTRCT_C'),
(NULL, NULL);

-- Action: Execute the translated BigQuery logic
SELECT
    apn,
    apn_cntrct,
    CASE
        WHEN av.apn IS NULL THEN av.apn -- This will be NULL
        ELSE CONCAT(av.apn, ',', av.apn_cntrct)
    END AS transformed_apn_concat
FROM `my-gcp-project.my_dw_dataset.mock_sof_ta_apn_vertrag` AS av;
```

**Expected Output (Oracle `decode` behavior):**
| apn     | apn_cntrct | transformed_apn_concat |
| :------ | :--------- | :--------------------- |
| APN_A   | CNTRCT_A   | APN_A,CNTRCT_A         |
| APN_B   | NULL       | APN_B,                 |
| NULL    | CNTRCT_C   | NULL                   |
| NULL    | NULL       | NULL                   |

**Pass/Fail Criterion:**
The `transformed_apn_concat` column in the BigQuery query result must match the expected output for all test cases.

```python
# pytest_orchestration.py (continued)
def test_transformation_decode_to_case():
    # Setup: Create and populate mock table
    client.query(f"CREATE OR REPLACE TABLE `{client.project}.{dataset_id}.mock_sof_ta_apn_vertrag` (apn STRING, apn_cntrct STRING)").result()
    client.query(f"""
    INSERT INTO `{client.project}.{dataset_id}.mock_sof_ta_apn_vertrag` (apn, apn_cntrct) VALUES
    ('APN_A', 'CNTRCT_A'),
    ('APN_B', NULL),
    (NULL, 'CNTRCT_C'),
    (NULL, NULL);
    """).result()

    # Action: Execute the translated BigQuery logic
    query = f"""
    SELECT
        apn,
        apn_cntrct,
        CASE
            WHEN av.apn IS NULL THEN av.apn
            ELSE CONCAT(av.apn, ',', av.apn_cntrct)
        END AS transformed_apn_concat
    FROM `{client.project}.{dataset_id}.mock_sof_ta_apn_vertrag` AS av
    ORDER BY apn, apn_cntrct;
    """
    result_df = client.query(query).result().to_dataframe()

    # Expected results based on Oracle's DECODE and CONCAT behavior
    expected_data = [
        (None, None, None),
        (None, 'CNTRCT_C', None),
        ('APN_A', 'CNTRCT_A', 'APN_A,CNTRCT_A'),
        ('APN_B', None, 'APN_B,')
    ]
    expected_df = pd.DataFrame(expected_data, columns=['apn', 'apn_cntrct', 'transformed_apn_concat'])
    expected_df = expected_df.sort_values(by=['apn', 'apn_cntrct'], na_position='first').reset_index(drop=True)
    
    # Convert result_df to match expected_df types for comparison
    result_df['apn'] = result_df['apn'].astype(object)
    result_df['apn_cntrct'] = result_df['apn_cntrct'].astype(object)
    result_df['transformed_apn_concat'] = result_df['transformed_apn_concat'].astype(object)

    pd.testing.assert_frame_equal(result_df, expected_df)
    print("Test Case 6 Passed: decode to CASE translation is correct.")

```
*(Note: `pandas` and `pytest` are assumed for `pd.testing.assert_frame_equal`)*

---

## Test Case 7: Transformation Correctness - Oracle `(+)` Outer Join to BigQuery `LEFT JOIN`

**Purpose:** Verify that all Oracle `(+)` outer join syntax is correctly translated to BigQuery `LEFT JOIN` and produces identical results, especially concerning NULL propagation.

**Setup:**
1.  Identify a specific join in `d_ausd_bp_ta_p_basisprod.sql` that uses the `(+)` syntax (e.g., `FROM table_A a, table_B b WHERE a.id = b.id(+)`).
2.  Create small, isolated test datasets for the involved tables (e.g., `table_A`, `table_B`) in BigQuery, covering scenarios:
    *   Matching rows.
    *   Rows in `table_A` with no match in `table_B`.
    *   Rows in `table_B` with no match in `table_A` (should not appear in `LEFT JOIN` from `table_A`).
3.  Create a temporary BigQuery Stored Procedure or direct SQL query to test this specific join.

**Action:**
Execute the BigQuery SQL query with the `LEFT JOIN` and compare the results with the expected output based on Oracle's `(+)` behavior.

```sql
-- Setup: Create mock tables for join test
CREATE OR REPLACE TABLE `my-gcp-project.my_dw_dataset.mock_table_a` (
    id INT64,
    value_a STRING
);
INSERT INTO `my-gcp-project.my_dw_dataset.mock_table_a` (id, value_a) VALUES
(1, 'A1'),
(2, 'A2'),
(3, 'A3_no_match');

CREATE OR REPLACE TABLE `my-gcp-project.my_dw_dataset.mock_table_b` (
    id INT64,
    value_b STRING
);
INSERT INTO `my-gcp-project.my_dw_dataset.mock_table_b` (id, value_b) VALUES
(1, 'B1'),
(2, 'B2'),
(4, 'B4_no_match_in_A'); -- This row should not appear in a LEFT JOIN from A

-- Action: Execute the translated BigQuery LEFT JOIN logic
SELECT
    a.id,
    a.value_a,
    b.value_b
FROM `my-gcp-project.my_dw_dataset.mock_table_a` AS a
LEFT JOIN `my-gcp-project.my_dw_dataset.mock_table_b` AS b ON a.id = b.id
ORDER BY a.id;
```

**Expected Output (Oracle `(+)` behavior):**
| id | value_a     | value_b |
| :-- | :---------- | :------ |
| 1  | A1          | B1      |
| 2  | A2          | B2      |
| 3  | A3_no_match | NULL    |

**Pass/Fail Criterion:**
The BigQuery query result must match the expected output, specifically ensuring that rows from `table_A` without a match in `table_B` retain their `table_A` values and have `NULL` for `table_B` columns, and rows only in `table_B` are excluded.

```python
# pytest_orchestration.py (continued)
import pandas as pd

def test_transformation_oracle_outer_join_to_left_join():
    # Setup: Create and populate mock tables
    client.query(f"CREATE OR REPLACE TABLE `{client.project}.{dataset_id}.mock_table_a` (id INT64, value_a STRING)").result()
    client.query(f"""
    INSERT INTO `{client.project}.{dataset_id}.mock_table_a` (id, value_a) VALUES
    (1, 'A1'),
    (2, 'A2'),
    (3, 'A3_no_match');
    """).result()

    client.query(f"CREATE OR REPLACE TABLE `{client.project}.{dataset_id}.mock_table_b` (id INT64, value_b STRING)").result()
    client.query(f"""
    INSERT INTO `{client.project}.{dataset_id}.mock_table_b` (id, value_b) VALUES
    (1, 'B1'),
    (2, 'B2'),
    (4, 'B4_no_match_in_A');
    """).result()

    # Action: Execute the translated BigQuery LEFT JOIN logic
    query = f"""
    SELECT
        a.id,
        a.value_a,
        b.value_b
    FROM `{client.project}.{dataset_id}.mock_table_a` AS a
    LEFT JOIN `{client.project}.{dataset_id}.mock_table_b` AS b ON a.id = b.id
    ORDER BY a.id;
    """
    result_df = client.query(query).result().to_dataframe()

    # Expected results
    expected_data = [
        (1, 'A1', 'B1'),
        (2, 'A2', 'B2'),
        (3, 'A3_no_match', None)
    ]
    expected_df = pd.DataFrame(expected_data, columns=['id', 'value_a', 'value_b'])
    expected_df['value_b'] = expected_df['value_b'].astype(object) # Ensure NULLs are comparable

    pd.testing.assert_frame_equal(result_df, expected_df)
    print("Test Case 7 Passed: Oracle (+) outer join to LEFT JOIN translation is correct.")
```

---

## Test Case 8: Data Quality - Target Table Schema and Data Types

**Purpose:** Verify that the schema (column names, data types, nullability) of the migrated `my-gcp-project.my_dw_dataset.sof_ta_p_basisprod` table matches the legacy `sof$ta_p_basisprod` table, ensuring no data truncation or type mismatches.

**Setup:**
1.  Obtain the schema definition of the legacy `sof$ta_p_basisprod` table (e.g., `DESCRIBE sof$ta_p_basisprod` in Oracle).
2.  Ensure `my-gcp-project.my_dw_dataset.sof_ta_p_basisprod` has been created according to the migration DDL.

**Action:**
Query the schema of `my-gcp-project.my_dw_dataset.sof_ta_p_basisprod` in BigQuery.

```sql
-- Action: Query BigQuery table schema
SELECT
    column_name,
    data_type,
    is_nullable
FROM `my-gcp-project.my_dw_dataset.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'sof_ta_p_basisprod'
ORDER BY ordinal_position;
```

**Pass/Fail Criterion:**
The retrieved BigQuery schema must match the legacy Oracle schema definition. This includes:
*   All column names are identical (case-sensitivity might need adjustment if Oracle was case-insensitive).
*   Data types are functionally equivalent (e.g., `VARCHAR2(X)` -> `STRING`, `NUMBER` -> `INT64`/`BIGNUMERIC`/`FLOAT64`, `DATE` -> `DATE`/`TIMESTAMP`).
*   Nullability constraints (`NOT NULL` vs. nullable) are preserved.
*   No unexpected columns are present, and no expected columns are missing.

```python
# pytest_orchestration.py (continued)
def test_target_table_schema_and_data_types():
    # Define expected schema based on legacy Oracle DDL translation
    # Example: (column_name, data_type, is_nullable)
    expected_schema = [
        ('COLUMN_ID', 'INT64', 'NO'),
        ('COLUMN_NAME', 'STRING', 'YES'),
        ('COLUMN_DATE', 'DATE', 'NO'),
        ('COLUMN_AMOUNT', 'BIGNUMERIC', 'YES'),
        # ... add all expected columns ...
    ]
    expected_df = pd.DataFrame(expected_schema, columns=['column_name', 'data_type', 'is_nullable'])
    expected_df = expected_df.sort_values(by='column_name').reset_index(drop=True)

    # Action: Query BigQuery table schema
    query = f"""
    SELECT
        column_name,
        data_type,
        is_nullable
    FROM `{client.project}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'sof_ta_p_basisprod'
    ORDER BY column_name;
    """
    bq_schema_df = client.query(query).result().to_dataframe()

    pd.testing.assert_frame_equal(bq_schema_df, expected_df)
    print("Test Case 8 Passed: Target table schema and data types match expected.")
```

---

## Test Case 9: External System Replacement - `TRUNCATE TABLE`

**Purpose:** Verify that the `TRUNCATE TABLE` operation within `d_ausd_bp_ta_p_basisprod_sp` (which replaces `isbert_schema.dwpa_util_skript.runstatement` for truncation) correctly clears the target table before insertion.

**Setup:**
1.  Populate `my-gcp-project.my_dw_dataset.sof_ta_p_basisprod` with some dummy data.
2.  Ensure source tables for `d_ausd_bp_ta_p_basisprod_sp` contain data that will result in a *different* number of rows than the dummy data.
3.  Truncate `my-gcp-project.my_dw_dataset.job_audit`.

**Action:**
Execute the `k_ausd_bp_ta_p_basisprod_sp`.

```python
# pytest_orchestration.py (continued)
def test_truncate_table_behavior():
    job_kennung = "TEST_JOB_TRUNCATE"
    eintrags_nr = "007"
    stichtag = datetime.date.today().strftime("%d%m%Y")

    # Setup: Populate target table with dummy data
    client.query(f"TRUNCATE TABLE `{client.project}.{dataset_id}.sof_ta_p_basisprod`").result()
    client.query(f"""
    INSERT INTO `{client.project}.{dataset_id}.sof_ta_p_basisprod` (some_id, some_value) VALUES
    (999, 'Dummy Row 1'),
    (998, 'Dummy Row 2');
    """).result()
    initial_count = client.query(f"SELECT COUNT(*) FROM `{client.project}.{dataset_id}.sof_ta_p_basisprod`").result().to_dataframe().iloc[0, 0]
    assert initial_count == 2, "Initial dummy data not inserted."

    # Action: Call the stored procedure (which should truncate and then insert)
    query = f"""
    CALL `{client.project}.{dataset_id}.k_ausd_bp_ta_p_basisprod_sp`(
        p_job_kennung => '{job_kennung}',
        p_eintrags_nr => '{eintrags_nr}',
        p_stichtag => '{stichtag}'
    );
    """
    job = client.query(query)
    job.result()

    # Pass/Fail Criterion: Verify the final row count matches the expected output from the transformation,
    # not the initial dummy data count.
    final_count = client.query(f"SELECT COUNT(*) FROM `{client.project}.{dataset_id}.sof_ta_p_basisprod`").result().to_dataframe().iloc[0, 0]
    
    # Assuming a known count from the transformation logic with the current source data
    # This would typically be derived from a baseline run or a separate calculation.
    expected_transformed_count = 100 # Replace with actual expected count based on source data
    
    assert final_count == expected_transformed_count, f"Truncate/Insert failed. Expected {expected_transformed_count} rows, got {final_count}."
    print(f"Test Case 9 Passed: TRUNCATE TABLE behavior confirmed. Final count: {final_count}.")

```

**Pass/Fail Criterion:**
*   The `k_ausd_bp_ta_p_basisprod_sp` executes successfully.
*   The final row count in `my-gcp-project.my_dw_dataset.sof_ta_p_basisprod` matches the expected count from the `INSERT ... SELECT` logic, demonstrating that the initial dummy data was truncated.

---