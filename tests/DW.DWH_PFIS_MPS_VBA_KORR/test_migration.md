# Migration Validation Test Suite: `DW.DWH_PFIS_MPS_VBA_KORR`

This document defines the comprehensive QA test suite to validate the migration of the UC4 job `DW.DWH_PFIS_MPS_VBA_KORR` (and its associated KSH wrapper `r_pfis_mps_vba_korrektur` and SQL script `d_pfis_mps_vba_korrektur.sql`) to Google Cloud Composer (Airflow) and BigQuery.

---

## Test Case 1: SQL Transformation Correctness (Level 6 & 7 ID Resolution)

### Purpose
Verify that the migrated BigQuery SQL script `d_pfis_mps_vba_korrektur.sql` correctly updates the Level 6 and Level 7 VBA IDs in the fact table `dwh$ta_f_mps_nutzung` based on case-insensitive matches against the lookup view `dwh$vi_l_m2_vba`. It must also verify that:
1. Unmatched descriptions retain their default IDs.
2. Successfully matched descriptions have their text columns cleared (`NULL`).
3. Descriptions matched to `'UNBEKANNT'` retain their text columns (not cleared).

### Setup
1. Create temporary test tables in a QA dataset mimicking the production schema.
2. Populate the lookup view mock `dwh$vi_l_m2_vba` with reference data.
3. Populate the fact table mock `dwh$ta_f_mps_nutzung` with test scenarios:
   * **Row 1 (Exact Match L6)**: Text matches lookup exactly.
   * **Row 2 (Mixed Case Match L6)**: Text matches lookup with different casing.
   * **Row 3 (No Match L6)**: Text does not exist in lookup.
   * **Row 4 (Unbekannt L6)**: Text matches 'UNBEKANNT' lookup.
   * **Row 5 (Exact Match L7)**: Text matches lookup exactly.
   * **Row 6 (Mixed Case Match L7)**: Text matches lookup with different casing.
   * **Row 7 (No Match L7)**: Text does not exist in lookup.
   * **Row 8 (Unbekannt L7)**: Text matches 'UNBEKANNT' lookup.

```sql
-- Create Mock Schema
CREATE OR REPLACE TABLE `your_qa_project.dwh.dwh$vi_l_m2_vba` (
  m2_vba_ebene6_text STRING,
  m2_vba_ebene7_text STRING,
  m2_vba_ebene7_id INT64
);

CREATE OR REPLACE TABLE `your_qa_project.dwh.dwh$ta_f_mps_nutzung` (
  row_id INT64, -- Used to track test cases
  m2_vba_ebene6_id INT64,
  m2_vba_ebene6_text STRING,
  m2_vba_ebene7_id INT64,
  m2_vba_ebene7_text STRING
);

-- Populate Lookup Mock
INSERT INTO `your_qa_project.dwh.dwh$vi_l_m2_vba` (m2_vba_ebene6_text, m2_vba_ebene7_text, m2_vba_ebene7_id)
VALUES 
  ('Vertrieb_A', 'Sub_Vertrieb_A', 101),
  ('Vertrieb_B', 'Sub_Vertrieb_B', 102),
  ('UNBEKANNT', 'UNBEKANNT', 999);

-- Populate Fact Mock
INSERT INTO `your_qa_project.dwh.dwh$ta_f_mps_nutzung` (row_id, m2_vba_ebene6_id, m2_vba_ebene6_text, m2_vba_ebene7_id, m2_vba_ebene7_text)
VALUES
  (1, 999, 'Vertrieb_A', 999, NULL),          -- Case 1: Exact Match L6
  (2, 999, 'vertrieb_b', 999, NULL),          -- Case 2: Mixed Case Match L6
  (3, 999, 'NonExistent', 999, NULL),         -- Case 3: No Match L6
  (4, 999, 'UNBEKANNT', 999, NULL),           -- Case 4: Unbekannt L6
  (5, 999, NULL, 999, 'Sub_Vertrieb_A'),      -- Case 5: Exact Match L7
  (6, 999, NULL, 999, 'sub_vertrieb_b'),      -- Case 6: Mixed Case Match L7
  (7, 999, NULL, 999, 'NonExistentSub'),      -- Case 7: No Match L7
  (8, 999, NULL, 999, 'UNBEKANNT');           -- Case 8: Unbekannt L7
```

### Action
Execute the migrated BigQuery SQL script `d_pfis_mps_vba_korrektur.sql` targeting the QA dataset, passing a mock parameter `@p_eintrags_nr = '99999'`.

### Pass/Fail Criterion
Run the following assertion query. It must return **0 rows** for the test to pass.

```sql
SELECT 
  row_id, 
  m2_vba_ebene6_id, 
  m2_vba_ebene6_text, 
  m2_vba_ebene7_id, 
  m2_vba_ebene7_text,
  expected_ebene6_id,
  expected_ebene6_text,
  expected_ebene7_id,
  expected_ebene7_text
FROM (
  SELECT *,
    CASE 
      WHEN row_id = 1 THEN 101
      WHEN row_id = 2 THEN 102
      WHEN row_id = 3 THEN 999
      WHEN row_id = 4 THEN 999
      ELSE 999
    END AS expected_ebene6_id,
    CASE 
      WHEN row_id = 1 THEN CAST(NULL AS STRING)
      WHEN row_id = 2 THEN CAST(NULL AS STRING)
      WHEN row_id = 3 THEN 'NonExistent'
      WHEN row_id = 4 THEN 'UNBEKANNT'
      ELSE CAST(NULL AS STRING)
    END AS expected_ebene6_text,
    CASE 
      WHEN row_id = 5 THEN 101
      WHEN row_id = 6 THEN 102
      WHEN row_id = 7 THEN 999
      WHEN row_id = 8 THEN 999
      ELSE 999
    END AS expected_ebene7_id,
    CASE 
      WHEN row_id = 5 THEN CAST(NULL AS STRING)
      WHEN row_id = 6 THEN CAST(NULL AS STRING)
      WHEN row_id = 7 THEN 'NonExistentSub'
      WHEN row_id = 8 THEN 'UNBEKANNT'
      ELSE CAST(NULL AS STRING)
    END AS expected_ebene7_text
  FROM `your_qa_project.dwh.dwh$ta_f_mps_nutzung`
)
WHERE 
  COALESCE(m2_vba_ebene6_id, -1) != COALESCE(expected_ebene6_id, -1)
  OR COALESCE(m2_vba_ebene6_text, 'NULL_VAL') != COALESCE(expected_ebene6_text, 'NULL_VAL')
  OR COALESCE(m2_vba_ebene7_id, -1) != COALESCE(expected_ebene7_id, -1)
  OR COALESCE(m2_vba_ebene7_text, 'NULL_VAL') != COALESCE(expected_ebene7_text, 'NULL_VAL');
```

---

## Test Case 2: SQL Transaction Rollback and Error Logging

### Purpose
Verify that if any statement within the BigQuery Scripting block fails, the entire transaction rolls back (no partial updates are committed to `dwh$ta_f_mps_nutzung`), and the error logging procedure `dwh_utility.dwpa_meldung_fehler` is executed with the correct error details.

### Setup
1. Deploy a mock version of the logging procedure `dwh_utility.dwpa_meldung_fehler` that writes logs to an audit table.
2. Populate the fact table mock with baseline data.
3. Force an error during execution. To simulate this cleanly, we can temporarily alter the lookup view mock to contain duplicate rows for a single text value, which will cause a "Scalar subquery produced more than one element" runtime error during the `UPDATE` statement.

```sql
-- Create Audit Table for Mock Logger
CREATE OR REPLACE TABLE `your_qa_project.dwh.audit_log` (
  msg_type STRING,
  eintrags_nr INT64,
  fehler_nr INT64,
  err_text STRING,
  err_code STRING,
  log_timestamp TIMESTAMP
);

-- Create Mock Logger Procedure
CREATE OR REPLACE PROCEDURE `your_qa_project.dwh_utility.dwpa_meldung_fehler`(
  p_msg_type STRING, p_eintrags_nr INT64, p_fehler_nr INT64, p_err_text STRING, p_err_code STRING
)
BEGIN
  INSERT INTO `your_qa_project.dwh.audit_log` (msg_type, eintrags_nr, fehler_nr, err_text, err_code, log_timestamp)
  VALUES (p_msg_type, p_eintrags_nr, p_fehler_nr, p_err_text, p_err_code, CURRENT_TIMESTAMP());
END;

-- Force duplicate in lookup to trigger scalar subquery failure
INSERT INTO `your_qa_project.dwh.dwh$vi_l_m2_vba` (m2_vba_ebene6_text, m2_vba_ebene7_text, m2_vba_ebene7_id)
VALUES ('Vertrieb_A', 'Sub_Vertrieb_A_DUP', 9999); -- Duplicate text 'Vertrieb_A'
```

### Action
Execute the migrated BigQuery SQL script `d_pfis_mps_vba_korrektur.sql` with parameter `@p_eintrags_nr = '12345'`.

### Pass/Fail Criterion
The test passes if:
1. The script execution fails (raises an exception).
2. The fact table `dwh$ta_f_mps_nutzung` remains completely unchanged (transaction rolled back).
3. The `audit_log` table contains exactly one entry for `eintrags_nr = 12345` with the captured error message.

```python
# Pytest assertion block to verify transaction rollback and logging
import pytest
from google.cloud import bigquery

def test_transaction_rollback_on_error():
    client = bigquery.Client()
    
    # 1. Capture baseline state of fact table
    baseline_query = "SELECT COUNT(*) as cnt FROM `your_qa_project.dwh.dwh$ta_f_mps_nutzung` WHERE m2_vba_ebene6_id = 101"
    baseline_cnt = list(client.query(baseline_query).result())[0].cnt
    assert baseline_cnt == 0, "Baseline should not have resolved IDs yet"

    # 2. Execute script and expect failure
    with open("d_pfis_mps_vba_korrektur.sql", "r") as f:
        sql_script = f.read()
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("p_eintrags_nr", "STRING", "12345"),
        ]
    )
    
    with pytest.raises(Exception) as excinfo:
        query_job = client.query(sql_script, job_config=job_config)
        query_job.result()
    
    assert "Scalar subquery produced more than one element" in str(excinfo.value)

    # 3. Verify rollback (count remains 0)
    post_query_cnt = list(client.query(baseline_query).result())[0].cnt
    assert post_query_cnt == 0, "Transaction failed to roll back! Partial updates were committed."

    # 4. Verify audit log entry
    audit_query = "SELECT * FROM `your_qa_project.dwh.audit_log` WHERE eintrags_nr = 12345"
    audit_rows = list(client.query(audit_query).result())
    assert len(audit_rows) == 1
    assert "Scalar subquery produced more than one element" in audit_rows[0].err_text
```

---

## Test Case 3: Python Wrapper CLI & Environment Validation

### Purpose
Verify that the Python wrapper script `r_pfis_mps_vba_korrektur.py` correctly parses command-line arguments (`-v`, `-h`), resolves environment variables (`DW_DIR_ROOT`, `DW_EintragsNr`), and writes execution banners to the log file.

### Setup
Set up a clean local execution environment with mock environment variables.

### Action
Execute the Python wrapper script using `subprocess` under different flag configurations.

### Pass/Fail Criterion
* Running with `-h` must return exit code `0` and print the help usage text.
* Running with `-v` must output the log file contents to stdout.
* The generated log file must contain the correct `Jobkennung`, `Job-Nr`, and `Logdatei` path.

```python
import os
import subprocess
import tempfile
import pytest

def test_python_wrapper_cli():
    # Setup temporary environment
    temp_dir = tempfile.mkdtemp()
    log_file_path = os.path.join(temp_dir, "test_run.log")
    
    env = os.environ.copy()
    env["DW_DIR_ROOT"] = temp_dir
    env["DW_EintragsNr"] = "98765"
    env["LogDatei"] = log_file_path
    
    # Create a dummy SQL script to prevent execution failure
    sql_dir = os.path.join(temp_dir, "pruef/is/sql")
    os.makedirs(sql_dir, exist_ok=True)
    with open(os.path.join(sql_dir, "d_pfis_mps_vba_korrektur.sql"), "w") as f:
        f.write("SELECT 1;") # Dummy SQL

    # Action 1: Test Help Flag
    res_help = subprocess.run(
        ["python3", "r_pfis_mps_vba_korrektur.py", "-h"],
        capture_output=True, text=True, env=env
    )
    assert res_help.returncode == 0
    assert "Beschreibung:" in res_help.stdout

    # Action 2: Test Verbose Execution
    res_run = subprocess.run(
        ["python3", "r_pfis_mps_vba_korrektur.py", "-v"],
        capture_output=True, text=True, env=env
    )
    
    # Assertions
    assert res_run.returncode == 0
    assert "-- Logdatei --" in res_run.stdout
    assert "Jobkennung :  PFIS_MPS_VBA_KORR" in res_run.stdout
    assert "Job-Nr     :  98765" in res_run.stdout
    assert f"Logdatei   :  {log_file_path}" in res_run.stdout
```

---

## Test Case 4: Python Wrapper BigQuery Execution & Parameter Binding

### Purpose
Verify that `r_pfis_mps_vba_korrektur.py` correctly instantiates the Google Cloud BigQuery client, reads the SQL script, binds the `@p_eintrags_nr` parameter, and executes the query.

### Setup
Use `unittest.mock` to mock the `google.cloud.bigquery.Client` and verify the API calls made by the Python script.

### Action
Run the Python script's main execution block within a pytest harness.

### Pass/Fail Criterion
The test passes if the BigQuery client is called with:
1. The exact SQL content of the target script.
2. A `QueryJobConfig` containing a scalar parameter named `p_eintrags_nr` matching the resolved entry number.

```python
from unittest.mock import MagicMock, patch
import os
import sys

# Inject mock environment variables
os.environ["DW_EintragsNr"] = "55555"
os.environ["DW_DIR_ROOT"] = "/tmp"
os.environ["GCP_PROJECT"] = "mock-gcp-project"

@patch("google.cloud.bigquery.Client")
@patch("builtins.open")
@patch("os.path.exists")
def test_bigquery_client_binding(mock_exists, mock_open, mock_bq_client):
    # Mock file existence checks
    mock_exists.return_value = True
    
    # Mock SQL file content
    mock_file = MagicMock()
    mock_file.read.return_value = "UPDATE table SET col = @p_eintrags_nr;"
    mock_open.return_value.__enter__.return_value = mock_file
    
    # Mock BigQuery Client response
    mock_client_instance = MagicMock()
    mock_bq_client.return_value = mock_client_instance
    
    # Import and run main
    import r_pfis_mps_vba_korrektur
    
    with patch.object(sys, 'argv', ['r_pfis_mps_vba_korrektur.py']):
        try:
            r_pfis_mps_vba_korrektur.main()
        except SystemExit as e:
            assert e.code == 0

    # Verify BigQuery Client was instantiated with correct project
    mock_bq_client.assert_called_once_with(project="mock-gcp-project")
    
    # Verify query was called with correct parameters
    args, kwargs = mock_client_instance.query.call_args
    assert args[0] == "UPDATE table SET col = @p_eintrags_nr;"
    
    job_config = kwargs.get("job_config")
    assert job_config is not None
    assert len(job_config.query_parameters) == 1
    
    param = job_config.query_parameters[0]
    assert param.name == "p_eintrags_nr"
    assert param.value == "55555"
```

---

## Test Case 5: Airflow DAG Structural & Configuration Assertions

### Purpose
Verify that the migrated Airflow DAG `dw_dwh_pfis_mps_vba_korr` is correctly configured with the legacy scheduling constraints, concurrency limits, and environment variables.

### Setup
Load the DAG file `dw_dwh_pfis_mps_vba_korr.py` into an Airflow `DagBag` context.

### Action
Inspect the DAG properties and task attributes programmatically.

### Pass/Fail Criterion
The test passes if:
1. The DAG loads without import errors.
2. `schedule_interval` is `None` (externally triggered).
3. `max_active_runs` is `1` (prevents concurrent database write contention).
4. The task `r_pfis_mps_vba_korrektur` is a `BashOperator` executing the correct Python wrapper.
5. The environment variable `DWH_JOB_KENNUNG` is set to `'PFIS_MPS_VBA_KORR'`.

```python
from airflow.models import DagBag, Variable
from unittest.mock import patch

@patch("airflow.models.Variable.get")
def test_dag_structure(mock_variable_get):
    # Mock Airflow variables
    mock_variable_get.side_effect = lambda key: {
        "GCP_PROJECT": "test-project",
        "GCP_REGION": "europe-west3",
        "GCS_BUCKET": "test-bucket",
        "legacy_host": "test-host"
    }.get(key)

    dagbag = DagBag(dag_folder=".", include_examples=False)
    dag_id = "dw_dwh_pfis_mps_vba_korr"
    
    # 1. Assert no import errors
    assert dag_id in dagbag.dags, f"DAG {dag_id} failed to load. Errors: {dagbag.import_errors}"
    
    dag = dagbag.get_dag(dag_id)
    
    # 2. Assert scheduling and concurrency
    assert dag.schedule_interval is None
    assert dag.max_active_runs == 1
    
    # 3. Assert task configuration
    task_id = "r_pfis_mps_vba_korrektur"
    assert task_id in dag.task_ids
    
    task = dag.get_task(task_id)
    assert task.bash_command == "$HOME/aktuell/pruef/is/bin/r_pfis_mps_vba_korrektur"
    
    # 4. Assert environment variables
    assert task.env is not None
    assert task.env.get("DWH_JOB_KENNUNG") == "PFIS_MPS_VBA_KORR"
    assert task.env.get("GCP_PROJECT") == "test-project"
```