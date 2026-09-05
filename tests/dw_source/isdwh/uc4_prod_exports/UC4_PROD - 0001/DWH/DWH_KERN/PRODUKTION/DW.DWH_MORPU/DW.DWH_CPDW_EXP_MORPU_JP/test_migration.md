# Migration Validation Test Suite: DW.DWH_EXIS_CPDW_LOC

This document defines the migration-validation test suite to verify the behavioral equivalence of the migrated Airflow DAG `dw_dwh_exis_cpdw_loc` against the legacy UC4 job `DW.DWH_EXIS_CPDW_LOC`.

---

## Test Case 1: Airflow DAG Structure and Configuration Validation

### Purpose
Verify that the migrated Airflow DAG is correctly parsed by the Airflow engine, contains the correct task structure, uses the designated `SSHOperator`, and preserves the metadata and default arguments defined in the migration design.

### Setup
* Access to the Airflow environment or a local Python environment with `apache-airflow` and `pytest` installed.
* The migrated DAG file `dw_dwh_exis_cpdw_loc.py` placed in the DAGs folder or test path.

### Action
Run a unit test using `pytest` to parse the DAG and assert its structural properties.

```python
import pytest
from airflow.models import DagBag, Variable

def test_dag_structure_and_properties():
    # Setup mock Airflow Variable for SSH Connection
    Variable.set("SSH_CONN_DWHDWH1P", "ssh_dwhdwh1p_default")
    
    dagbag = DagBag(dag_folder=".", include_examples=False)
    dag_id = "dw_dwh_exis_cpdw_loc"
    
    # Assert DAG exists and loaded without errors
    dag = dagbag.get_dag(dag_id)
    assert dag is not None, f"DAG {dag_id} failed to load."
    assert len(dagbag.import_errors) == 0, f"Import errors detected: {dagbag.import_errors}"
    
    # Assert DAG properties
    assert dag.schedule_interval is None, "DAG schedule should be None (externally triggered)"
    assert dag.catchup is False, "Catchup should be disabled"
    assert dag.max_active_runs == 1, "Max active runs must be restricted to 1"
    
    # Assert Task properties
    task_id = "dwh_exis_cpdw_loc"
    assert task_id in dag.task_ids, f"Task {task_id} is missing from DAG"
    
    task = dag.get_task(task_id)
    from airflow.providers.ssh.operators.ssh import SSHOperator
    assert isinstance(task, SSHOperator), f"Task {task_id} must be an SSHOperator"
    
    # Assert command structure
    expected_command = (
        "source $HOME/.dw_init && "
        "export DWH_JOB_KENNUNG='EXIS_CPDW_LOC' && "
        "$HOME/aktuell/exporter/is/bin/r_exis -s cpdw -r loc -f sftp"
    )
    assert task.command == expected_command, "The executed SSH command does not match the legacy specification"
    assert task.environment.get("DWH_JOB_KENNUNG") == "EXIS_CPDW_LOC", "DWH_JOB_KENNUNG environment variable is missing or incorrect"
```

### Pass/Fail Criterion
* **Pass**: The DAG loads with zero import errors, contains exactly one task named `dwh_exis_cpdw_loc` of type `SSHOperator`, and matches all specified parameters.
* **Fail**: Any import errors occur, or the task properties/commands do not match the expected values.

---

## Test Case 2: SSH Connection and Authentication Verification

### Purpose
Verify that the Airflow connection configured via `SSH_CONN_DWHDWH1P` (defaulting to `ssh_dwhdwh1p_default`) successfully authenticates to the target host `dwhdwh1p` using the credentials corresponding to the legacy login object `DW.UNIX.ISTNS`.

### Setup
* The Airflow connection `ssh_dwhdwh1p_default` must be configured in the Airflow metadata database (or secrets manager) with the correct host, port, username (`istns` or equivalent), and SSH private key.
* Network connectivity must be open between the Airflow worker/triggerer and host `dwhdwh1p` on port 22.

### Action
Execute a test task that performs a simple connectivity and identity check on the remote host.

```python
import pytest
from airflow.providers.ssh.hooks.ssh import SSHHook

def test_ssh_connection_and_identity():
    # Initialize SSH Hook using the connection ID defined in the DAG
    # In a test environment, ensure the connection is mocked or points to a sandbox host
    ssh_hook = SSHHook(ssh_conn_id="ssh_dwhdwh1p_default")
    
    with ssh_hook.get_conn() as ssh_client:
        # Check connectivity and retrieve current user and hostname
        stdin, stdout, stderr = ssh_client.exec_command("whoami && hostname")
        output = stdout.read().decode().strip().split("\n")
        
        assert len(output) == 2, "Failed to retrieve user and hostname from remote host"
        remote_user, remote_host = output[0], output[1]
        
        # Assertions based on UC4 Login: DW.UNIX.ISTNS and Host: dwhdwh1p
        assert "istns" in remote_user.lower(), f"Expected remote user to be 'istns', got '{remote_user}'"
        assert "dwhdwh1p" in remote_host.lower(), f"Expected remote host to be 'dwhdwh1p', got '{remote_host}'"
```

### Pass/Fail Criterion
* **Pass**: The SSH connection is established successfully, and the remote execution returns the expected username and hostname.
* **Fail**: Connection timeout, authentication failure, or incorrect user/host returned.

---

## Test Case 3: Environment and Profile Initialization Parity

### Purpose
Verify that the remote shell environment is correctly initialized by sourcing `$HOME/.dw_init` and that the environment variable `DWH_JOB_KENNUNG` is correctly exported prior to executing the exporter utility.

### Setup
* Access to the target host `dwhdwh1p` via the `ssh_dwhdwh1p_default` connection.

### Action
Execute a test command via SSH that sources the profile, exports the variable, and prints the environment state to verify correct initialization.

```python
import pytest
from airflow.providers.ssh.hooks.ssh import SSHHook

def test_environment_initialization():
    ssh_hook = SSHHook(ssh_conn_id="ssh_dwhdwh1p_default")
    
    # Command to source the profile, export the variable, and print specific env vars
    test_command = (
        "source $HOME/.dw_init && "
        "export DWH_JOB_KENNUNG='EXIS_CPDW_LOC' && "
        "env | grep -E 'DWH_JOB_KENNUNG|PATH'"
    )
    
    with ssh_hook.get_conn() as ssh_client:
        stdin, stdout, stderr = ssh_client.exec_command(test_command)
        output = stdout.read().decode().strip()
        err_output = stderr.read().decode().strip()
        
        assert err_output == "", f"Errors encountered during environment initialization: {err_output}"
        assert "DWH_JOB_KENNUNG=EXIS_CPDW_LOC" in output, "DWH_JOB_KENNUNG was not correctly exported"
        # Verify that PATH or other critical variables initialized by .dw_init are present
        assert "PATH=" in output, "System PATH variable is missing; .dw_init may not have sourced correctly"
```

### Pass/Fail Criterion
* **Pass**: The environment variables are successfully set and printed without any shell errors (e.g., "file not found" for `.dw_init`).
* **Fail**: Sourcing `.dw_init` fails, or `DWH_JOB_KENNUNG` is not set to `EXIS_CPDW_LOC`.

---

## Test Case 4: End-to-End Output Parity & SFTP Delivery Validation

### Purpose
Verify that running the exporter utility `$HOME/aktuell/exporter/is/bin/r_exis -s cpdw -r loc -f sftp` via the Airflow DAG produces identical output files on the target CPDW SFTP server compared to the legacy UC4 execution.

### Setup
1. **Legacy Run**: Execute the legacy UC4 job `DW.DWH_EXIS_CPDW_LOC` in a controlled test environment. Capture the generated lookup files delivered to the CPDW SFTP target.
2. **Airflow Run**: Execute the migrated Airflow DAG `dw_dwh_exis_cpdw_loc`. Capture the generated lookup files delivered to the CPDW SFTP target.
3. **SFTP Access**: Credentials to connect to the CPDW SFTP target directory where lookup files are deposited.

### Action
Download the files generated by both runs and perform a binary and structural comparison.

```python
import pysftp
import hashlib
import pytest

def calculate_md5(file_path):
    hasher = hashlib.md5()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()

def test_sftp_output_parity():
    # SFTP Connection Details (Retrieve from Airflow Connection or Test Config)
    sftp_host = "cpdw-sftp-target.internal"
    sftp_user = "cpdw_user"
    sftp_pkey = "/path/to/sftp_private_key"
    remote_dir = "/target/lookup_data/loc"
    
    legacy_local_path = "/tmp/legacy_lookup_loc.csv"
    airflow_local_path = "/tmp/airflow_lookup_loc.csv"
    
    # Note: In a real test pipeline, these filenames would be dynamically resolved 
    # based on execution timestamps (e.g., lookup_loc_YYYYMMDD.csv)
    legacy_remote_file = "legacy_lookup_loc.csv" 
    airflow_remote_file = "airflow_lookup_loc.csv"

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None # For testing purposes only
    
    with pysftp.Connection(host=sftp_host, username=sftp_user, private_key=sftp_pkey, cnopts=cnopts) as sftp:
        sftp.cwd(remote_dir)
        
        # Download both files
        sftp.get(legacy_remote_file, legacy_local_path)
        sftp.get(airflow_remote_file, airflow_local_path)
        
    # 1. Compare File Sizes
    import os
    legacy_size = os.path.getsize(legacy_local_path)
    airflow_size = os.path.getsize(airflow_local_path)
    assert legacy_size == airflow_size, f"File size mismatch: Legacy ({legacy_size} bytes) vs Airflow ({airflow_size} bytes)"
    
    # 2. Compare MD5 Checksums (Ensures exact binary parity)
    legacy_md5 = calculate_md5(legacy_local_path)
    airflow_md5 = calculate_md5(airflow_local_path)
    assert legacy_md5 == airflow_md5, f"MD5 checksum mismatch! Legacy: {legacy_md5}, Airflow: {airflow_md5}"
```

### Pass/Fail Criterion
* **Pass**: The files delivered to the CPDW SFTP server by both the legacy job and the Airflow DAG are identical in size, structure, and MD5 checksum.
* **Fail**: File sizes or MD5 checksums do not match, indicating data loss, corruption, or formatting differences during the export process.

---

## Test Case 5: Data Quality and Schema Assertions (Source vs. Target)

### Purpose
Verify that the exported lookup data file contains the correct schema, row counts, and data types matching the source database tables (e.g., location/lookup tables in the DWH). This ensures the `r_exis` utility executed under the Airflow environment extracts data correctly without truncation or NULL handling errors.

### Setup
* Read access to the source DWH database (Oracle/Postgres/BigQuery depending on the DWH platform).
* Access to the exported lookup CSV file downloaded from the SFTP server in Test Case 4.

### Action
Query the source database for the expected lookup data and compare it with the parsed contents of the exported CSV file.

```python
import pandas as pd
import sqlalchemy as sa
import pytest

def test_source_vs_target_data_quality():
    # Database Connection
    db_uri = "oracle+cx_oracle://dwh_reader:password@dwh_host:1521/dwh_service"
    engine = sa.create_engine(db_uri)
    
    # Query to fetch source lookup data (adjust table name based on actual DWH schema)
    source_query = """
        SELECT LOC_ID, LOC_NAME, COUNTRY_CODE, IS_ACTIVE 
        FROM DWH_KERN.LOOKUP_LOCATION 
        ORDER BY LOC_ID
    """
    df_source = pd.read_sql(source_query, con=engine)
    
    # Load exported CSV file (adjust delimiter and column names based on actual export format)
    exported_file_path = "/tmp/airflow_lookup_loc.csv"
    df_exported = pd.read_csv(exported_file_path, delimiter=";", keep_default_na=True)
    
    # Sort exported data to match source ordering
    df_exported = df_exported.sort_values(by="LOC_ID").reset_index(drop=True)
    
    # 1. Row Count Assertion
    assert len(df_source) == len(df_exported), (
        f"Row count mismatch! Source DB: {len(df_source)} rows, Exported File: {len(df_exported)} rows"
    )
    
    # 2. Schema / Column Name Assertion
    expected_columns = ["LOC_ID", "LOC_NAME", "COUNTRY_CODE", "IS_ACTIVE"]
    assert list(df_exported.columns) == expected_columns, f"Column mismatch. Got: {list(df_exported.columns)}"
    
    # 3. NULL Handling Assertion
    # Ensure that NULLs in the database are correctly represented as empty fields or standard NULL indicators in the CSV
    db_null_count = df_source["COUNTRY_CODE"].isnull().sum()
    csv_null_count = df_exported["COUNTRY_CODE"].isnull().sum()
    assert db_null_count == csv_null_count, (
        f"NULL handling discrepancy in 'COUNTRY_CODE'. DB NULLs: {db_null_count}, CSV NULLs: {csv_null_count}"
    )
    
    # 4. Data Parity Assertion
    pd.testing.assert_frame_equal(df_source, df_exported, check_dtype=False, obj="Source vs Exported CSV")
```

### Pass/Fail Criterion
* **Pass**: The exported CSV file matches the source database lookup table exactly in terms of row count, column structure, NULL distribution, and record values.
* **Fail**: Row counts differ, columns are missing/misaligned, NULL values are incorrectly converted (e.g., string `"NULL"` or `"NaN"` instead of empty fields), or data values do not match.