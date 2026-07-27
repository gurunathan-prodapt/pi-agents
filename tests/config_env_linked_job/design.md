=== OBJECT: DW.CFG_LOAD_PARAMS (JOBS_UNIX) ===
active=1
title=Load DWH parameter file into staging
login=DW.UNIX.ISBERT
host=|DWHDWH1P|HOST
ert_seconds=6
launcher_type=unrecognized
launcher_details={'raw_command': '&HOME/cfg/bin/r_load_params.ksh'}
script_body:
:inc DW.HOLE_PFAD
:set &DWH_JOB_KENNUNG='AUSD_V_TA_PERIOD'
. $HOME/.dw_init
&HOME/cfg/bin/r_load_params.ksh
:inc DW.BERT_LESE_LOG
operational_notes=

=== UNRESOLVED REFERENCES (object named but not supplied in this bundle) ===
  (none — every referenced object was supplied in this bundle)


# UC4 to Apache Airflow Migration Design Document

---

## 1. Overview
The extracted UC4 object represents a standalone Unix job, `DW.CFG_LOAD_PARAMS`, which is responsible for loading Data Warehouse (DWH) parameter files into the staging environment. It initiates environment configuration, sets a specific job identifier metadata variable (`&DWH_JOB_KENNUNG` to `'AUSD_V_TA_PERIOD'`), and executes a custom Korn shell script (`r_load_params.ksh`). Because no wrapping workflow (`JOBP`) or schedule (`JSCH`) was provided in this extraction, this process is modeled as a standalone Airflow DAG that is externally triggered.

---

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.CFG_LOAD_PARAMS` | JOBS_UNIX | Active (1) | Load DWH parameter file into staging |

---

## 3. Scheduling
- **Trigger Source**: There are no `EVNT_TIME` objects, scheduled parents (`JSCH`), or native activation scripts (`SCRI`) present in this extraction bundle. The workflow is classified as externally triggered (source unknown from this extraction alone).
- **DAG Schedule**: `schedule=None` (no native calendar or interval schedule exists in the export).

---

## 4. Airflow DAG Properties
The following DAG properties are established for the single-job execution wrapper:

| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_cfg_load_params` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` (placeholder) |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` (Derived from Active=1) |
| **default_args** | `{'owner': 'dw', 'retries': 1, 'retry_delay': timedelta(minutes=5)}` |

---

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `cfg_load_params` | `DW.CFG_LOAD_PARAMS` | `EmptyOperator` | `N/A` | `N/A` | `1` | `5 min` | `None` | `None` | `N/A` | `None` | # REVIEW-STRUCT: launcher command `&HOME/cfg/bin/r_load_params.ksh` not recognised — confirm target operator/script manually. Environment variables: `&DWH_JOB_KENNUNG='AUSD_V_TA_PERIOD'`. |

---

## 6. Task Dependency Map
Since only one standalone job is defined within this extraction:
```python
cfg_load_params
```

---

## 7. Sync / Concurrency Analysis
No sync rows (`SYNC` objects or parallel limits) are declared in this extraction. Single concurrency is maintained by setting `max_active_runs=1` on the DAG level to prevent race conditions during parameter file staging operations.

---

## 8. Error Handling and Retry Strategy
- **Retries**: Standard retries are set to `1` with a `5-minute` retry delay, as specified in the default DAG arguments.
- **Callbacks**: No explicit global or task-specific error-handling notifications are declared in this extraction.
- **Preconditions**: No `earliest_start_time` or complex calendar dependencies exist.

---

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `&DWH_JOB_KENNUNG` | `'AUSD_V_TA_PERIOD'` | Map to an Airflow Variable or inject as an environment variable configuration context if migrated to a bash script step. |
| Host: `|DWHDWH1P|HOST` | Target Execution Machine | Create an Airflow connection representing this host (e.g., `ssh_dwhdwh1p`). |
| Login: `DW.UNIX.ISBERT` | OS Credentials | Store username and keys/passwords securely within the corresponding connection ID. |

---

## 10. Developer Notes
* **# REVIEW-STRUCT: Unrecognized Launcher**: The original execution relies on custom script wrappers (`&HOME/cfg/bin/r_load_params.ksh`). Because this launcher is unrecognized by the default migration rules, it has been mapped to an `EmptyOperator`. 
  * *Action required*: The developer should manually replace the `EmptyOperator` with either a `BashOperator` (if executing on a local Airflow worker configured with access to the source environment mount) or an `SSHOperator` (connecting to host `|DWHDWH1P|HOST` with connection credentials mapped from login `DW.UNIX.ISBERT`).
* **Includes/Dependencies inside Script Body**:
  * `:inc DW.HOLE_PFAD` (likely sets environmental paths such as `&HOME`).
  * `:inc DW.BERT_LESE_LOG` (likely handles post-execution log parsing or exit-code checks).
  * *Action required*: Verify whether the logic inside these includes should be integrated as native Airflow task wrapper methods or handled directly on the destination execution agent.
* **External Activation**: Since this script was extracted as a standalone object, ensure its upstream trigger (such as a File Sensor or message queue trigger that places the parameter file in the staging zone) is mapped correctly in Airflow.

---

# PSEUDOCODE OUTLINE

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# ── GCP Configuration ────────────────────────────────────
# No direct GCP services mapped for this unrecognized Unix script execution.
# (If migrated to GCS/BigQuery in the future, project_id and staging bucket definitions should be placed here).

# ── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'dw',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── on_failure_callback stubs ─────────────────────────────
# No callbacks or post-actions defined in the source export.

# ── DAG Definition ────────────────────────────────────────
with DAG(
    dag_id='dw_cfg_load_params',
    default_args=DEFAULT_ARGS,
    description='Load DWH parameter file into staging - Migrated from UC4',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['dwh', 'migration_uc4', 'parameter_load'],
) as dag:

    # ── Task: cfg_load_params ─────────────────────────────
    # # REVIEW-STRUCT: Original execution was a JOBS_UNIX object with launcher_type=unrecognized.
    # Raw Command: &HOME/cfg/bin/r_load_params.ksh
    # Host Target: |DWHDWH1P|HOST
    # Login Context: DW.UNIX.ISBERT
    # Target Parameter Set: &DWH_JOB_KENNUNG='AUSD_V_TA_PERIOD'
    # Includes evaluated: DW.HOLE_PFAD, DW.BERT_LESE_LOG
    #
    # Action Required: Update this EmptyOperator to SSHOperator or BashOperator once connectivity
    # details to the host '|DWHDWH1P|HOST' are resolved.
    cfg_load_params = EmptyOperator(
        task_id='cfg_load_params',
    )

    # ── Dependencies ─────────────────────────────────────────
    # Standalone task - no dependency routing required.
    cfg_load_params
```

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `config_env_linked_job/DWH_CFG_JOB/DW.CFG_LOAD_PARAMS.xml` | `config_env_linked_job/DWH_CFG_JOB/dw_cfg_load_params.py` | Converts the legacy UC4 Unix job definition into an Apache Airflow DAG to orchestrate the loading of parameters. |

---

### ADD CONTEXT THE MCP COULD NOT SEE

#### Execution order
The legacy execution order consists of three steps that must be preserved in the target orchestration sequence:
1. **Orchestrator Initiation**: `config_env_linked_job/DWH_CFG_JOB/DW.CFG_LOAD_PARAMS.xml` maps to the parent Airflow DAG: `config_env_linked_job/DWH_CFG_JOB/dw_cfg_load_params.py`.
2. **Execution of Load Script**: `config_env_linked_job/iscfg/bin/r_load_params.ksh` maps to the translated Python script/task `config_env_linked_job/iscfg/bin/r_load_params.py` (orchestrated inside the DAG as an operator/task).
3. **Execution of SQL Transformation**: `config_env_linked_job/iscfg/cfg/d_param_load.sql` maps to the Dataform transformation file `config_env_linked_job/iscfg/cfg/d_param_load.sqlx` (triggered directly within the DAG after the load script completes).

#### Lineage
- **Downstream Consumers**:
  - `config_env_linked_job/iscfg/bin/r_load_params.ksh`: Invoked directly by the UC4 job; translated into a task execution inside the Airflow DAG.
  - `DWHDWH1P` (External connection host): Mapped to a GCP SSH or local container connection configuration representing the environment host.
  - USES_PACKAGE `DW.UNIX.ISBERT`: Mapped to the credential context / connection ID used to authorize task execution.

#### External system replacements
- **Oracle to BigQuery Staging**: The legacy loading process involves loading parameters into the Oracle staging table `DWH_STG.PARAM_LOAD` via SQL\*Loader and then performing an upsert merge into `DWH_ADM.JOB_PARAMS`. On BigQuery:
  - Raw parameter files are placed in a Cloud Storage (GCS) bucket.
  - BigQuery native load commands or GCS-to-BigQuery staging tables are used to load the parameters into the BigQuery staging dataset.
  - BigQuery SQL / Dataform merges staging data into the final target dataset `BQ_DATASET`.
- **Target Host Execution**: The legacy physical execution host `|DWHDWH1P|HOST` and user login `DW.UNIX.ISBERT` are replaced with a Google Cloud IAM Service Account or native Airflow worker execution, removing the need for physical SSH loopbacks where possible.

#### Cross-file dependencies
- **Call Chain Dependency**: The Airflow DAG (`dw_cfg_load_params.py`) coordinates the sequential execution of the Python load task (the migrated `r_load_params.py`) followed by the Dataform staging merge model execution (the migrated `d_param_load.sqlx`).
- **Data Dependency**: The staging load script is dependent on parameter properties files residing in GCS. Once loaded, the resulting BigQuery staging table acts as a dependency for the post-load Dataform model.

#### Target file plan
- **Target File Path**: `config_env_linked_job/DWH_CFG_JOB/dw_cfg_load_params.py`
- **Language**: Python (Apache Airflow DAG)
- **Source File**: `config_env_linked_job/DWH_CFG_JOB/DW.CFG_LOAD_PARAMS.xml`

#### Environment-specific values
1. **GLOBAL (Environment-wide)**
   - `GCP_PROJECT`: The target GCP Project ID. Sourced at runtime via `Variable.get("GCP_PROJECT")` or `os.environ.get("GCP_PROJECT")`.
   - `GCS_BUCKET`: The GCS bucket containing incoming parameter files. Sourced at runtime via `Variable.get("GCS_BUCKET")`.
   - `BQ_DATASET`: The target BigQuery dataset representing staging/target environments. Sourced at runtime via `Variable.get("BQ_DATASET")`.
   - `CONN_DW_UNIX_ISBERT`: The Airflow connection ID representing the security and host context. Sourced at runtime via `Variable.get("CONN_DW_UNIX_ISBERT")`.

2. **JOB-SPECIFIC**
   - `DWH_JOB_KENNUNG` = `'AUSD_V_TA_PERIOD'`: Local job identifier metadata, hardcoded as a task parameter/environment variable inside the DAG task.

#### Risks and manual steps
- **Bypassed Legacy Dependencies**: The legacy includes `. $HOME/.dw_init`, `:inc DW.HOLE_PFAD`, and `:inc DW.BERT_LESE_LOG` were marked as `NO SOURCE NEEDED` after human review. Their environment setup and custom log reading features must be replaced natively using the Airflow worker environment variables and Cloud Composer's logging system.
- **Cross-Pass Integration Risk**: The Airflow DAG acts as the orchestration shell, but the actual execution relies on `r_load_params.py` (the migration of `r_load_params.ksh`) and `d_param_load.sqlx` (the migration of `d_param_load.sql`), which are designed and built under separate independent design passes. The integration must be manual and carefully tested to ensure tasks point to the correct file paths.

---

=== FILE: config_env_linked_job/iscfg/bin/r_load_params.ksh ===
#!/bin/ksh
# r_load_params.ksh — stage the DWH parameter file into DWH_STG.PARAM_LOAD.
# Environment comes from dwh.profile; connection settings from dwh_env.properties.

. ${DWH_HOME}/cfg/dwh.profile

PROPS=${DWH_HOME}/cfg/dwh_env.properties
if [[ ! -f ${PROPS} ]]; then
    print -u2 "FEHLER: Parameterdatei ${PROPS} nicht gefunden"
    exit 8
fi

DB_HOST=$(grep '^db.host=' ${PROPS} | cut -d'=' -f2)
DB_SID=$(grep '^db.sid=' ${PROPS} | cut -d'=' -f2)
STG_TABLE=$(grep '^stage.table=' ${PROPS} | cut -d'=' -f2)

print "Lade Parameter nach ${STG_TABLE} auf ${DB_HOST}/${DB_SID}"

sqlldr userid=dwh_stg@${DB_SID} control=${DWH_HOME}/cfg/param_load.ctl \
       data=${PROPS} log=${DWH_LOG_DIR}/param_load.log

rc=$?
if [[ ${rc} -ne 0 ]]; then
    print -u2 "FEHLER: sqlldr beendet mit RC=${rc}"
    exit ${rc}
fi

sqlplus -s dwh_adm@${DB_SID} @${DWH_HOME}/cfg/d_param_load.sql
rc=$?
if [[ ${rc} -ne 0 ]]; then
    print -u2 "FEHLER: d_param_load.sql beendet mit RC=${rc}"
    exit ${rc}
fi
print "Parameterladen erfolgreich abgeschlossen"
exit 0


# DESIGN DOCUMENT: r_load_params.ksh Conversion

## 1. SCRIPT OVERVIEW
* **Purpose**: This script loads environment and system parameters from a local configuration properties file (`dwh_env.properties`) into an Oracle Database staging table (nominally `DWH_STG.PARAM_LOAD`). 
* **Trigger**: This script is typically invoked as an orchestrator step during the initialization phase of a Data Warehouse (DWH) batch run.
* **Reads**: 
  * `${DWH_HOME}/cfg/dwh.profile` (environment shell profile)
  * `${DWH_HOME}/cfg/dwh_env.properties` (system configuration parameters)
  * `${DWH_HOME}/cfg/param_load.ctl` (Oracle SQL*Loader control file)
* **Writes**: 
  * Oracle staging table `DWH_STG.PARAM_LOAD` (via `sqlldr`)
  * `${DWH_LOG_DIR}/param_load.log` (loader execution log)
* **Business Process**: Prepares and synchronizes the dynamic execution context of the Data Warehouse in the database before the main processing ETL/ELT flows begin.

---

## 2. INVOCATION CONTEXT
* **Caller**: Typically invoked by a UC4/Automic Job (e.g., `JOBS_UNIX` object wrapper) or run manually during administrative tasks.
* **Command Line / Arguments**: No positional command line arguments are passed to this script.
* **UC4 Native Includes**:
  * None referenced in the source code extraction.
* **Environment Files Sourced**:
  * `. ${DWH_HOME}/cfg/dwh.profile`
    * *Assessment*: # REVIEW-STRUCT: environment file dwh.profile not supplied — variables it sets are unknown; do not guess their names or values.

---

## 3. PARAMETERS / INPUTS
* **DWH_HOME**:
  * *Type*: Environment Variable
  * *Source*: Inherited from caller or set by `dwh.profile`.
  * *Usage*: Used to resolve paths for properties files, SQL control files, and SQL script definitions.
  * *Python Representation*: `os.environ.get("DWH_HOME")`
* **DWH_LOG_DIR**:
  * *Type*: Environment Variable
  * *Source*: Inherited from caller or set by `dwh.profile`.
  * *Usage*: Destination directory for the Oracle SQL*Loader log output (`param_load.log`).
  * *Python Representation*: `os.environ.get("DWH_LOG_DIR")`
* **db.host** (extracted as `DB_HOST`):
  * *Type*: Property Value
  * *Source*: Parsed dynamically from `${DWH_HOME}/cfg/dwh_env.properties`.
  * *Usage*: Used only for output logging messages.
  * *Python Representation*: Local variable parsed from configuration.
* **db.sid** (extracted as `DB_SID`):
  * *Type*: Property Value
  * *Source*: Parsed dynamically from `${DWH_HOME}/cfg/dwh_env.properties`.
  * *Usage*: Used as the Net Service Name / Oracle SID connection target for `sqlldr` and `sqlplus`.
  * *Python Representation*: Local variable parsed from configuration.
* **stage.table** (extracted as `STG_TABLE`):
  * *Type*: Property Value
  * *Source*: Parsed dynamically from `${DWH_HOME}/cfg/dwh_env.properties`.
  * *Usage*: Used only for output logging messages.
  * *Python Representation*: Local variable parsed from configuration.

---

## 4. EXTERNAL COMMANDS / PROGRAMS INVOKED
### Command 1: `sqlldr` (Oracle SQL*Loader)
* **Verbatim Command Line**:
  ```bash
  sqlldr userid=dwh_stg@${DB_SID} control=${DWH_HOME}/cfg/param_load.ctl data=${PROPS} log=${DWH_LOG_DIR}/param_load.log
  ```
* **Purpose**: Bulk-loads configuration lines from the properties file (`dwh_env.properties`) into the database staging table using a pre-defined Oracle control file template.
* **Target Execution Style**: External process execution via Python's `subprocess` module.
* **Resolvable Launcher?**: No. SQL*Loader (`sqlldr`) is a specialized client utility that parses external files and loads them via proprietary protocols using a Control (`.ctl`) schema. Refactoring this directly to native Python DB-API logic would require reimplementing the control file parsing logic (`param_load.ctl`), which is not supplied. Thus, it must remain as a `subprocess` invocation.
* **Connection / Authentication Note**: 
  * # REVIEW: The connection string uses `userid=dwh_stg@${DB_SID}` without an explicit password. This indicates dependency on OS-native authentication (OPS$), external Oracle Wallet/autologin, or configured credentials inherited in the environment.

### Command 2: `sqlplus` (Oracle SQL*Plus)
* **Verbatim Command Line**:
  ```bash
  sqlplus -s dwh_adm@${DB_SID} @${DWH_HOME}/cfg/d_param_load.sql
  ```
* **Purpose**: Executes the administrative post-load processing script `d_param_load.sql` to apply or distribute the staged configuration records.
* **Target Execution Style**: Can remain a `subprocess` call OR be refactored into a native Python database execution (using `oracledb`) if the underlying database credentials are known and the SQL script contents do not rely heavily on custom SQL*Plus terminal features.
* **Resolvable Launcher?**: Partially, but since database credentials/passwords are not supplied in this extraction and the SQL file contents of `d_param_load.sql` are unknown, keeping it as an external `subprocess` or configuring `oracledb` with external credentials is required.

---

## 5. EMBEDDED SQL
* **Source File**: `${DWH_HOME}/cfg/d_param_load.sql` (referenced externally)
* **Full SQL Text**: Not supplied in the source extraction.
* **Statement Type**: Unknown (contained in external file).
* **Tables Touched**: Unknown (implied relationship to parameter staging/operational tables).
* **Dialect Identification**: Oracle SQL*Plus. The use of `@` file loader and `-s` (silent option) with an Oracle SID target (`DB_SID`) confirms Oracle.

---

## 6. CONTROL FLOW
1. **Environment Setup**: Sourced shell profile `dwh.profile` is run (equivalent to reading predefined system-level environments in Python).
2. **Path Setup**: Defines properties file path `${DWH_HOME}/cfg/dwh_env.properties`.
3. **Properties Existence Validation**: Checks if `dwh_env.properties` is a readable file. If missing, writes error "FEHLER: Parameterdatei ..." to standard error and terminates with exit code `8`.
4. **Configuration Extraction**: Parses database connection properties (`db.host`, `db.sid`, `stage.table`) from the properties file using standard string-matching techniques (KornShell implementation uses `grep` and `cut`).
5. **Console Logging**: Outputs targeted load metadata to standard output.
6. **Execute SQL\*Loader**: Initiates the `sqlldr` system utility to perform bulk loading.
7. **Validation of Load Success**: Captures the return code of `sqlldr`. If non-zero, logs the failure to standard error and terminates with the loader's exit code.
8. **Execute Administrative SQL**: Runs `sqlplus` executing `d_param_load.sql`.
9. **Validation of SQL Execution**: Captures the return code of `sqlplus`. If non-zero, logs the failure to standard error and terminates with the SQL*Plus exit code.
10. **Final Logging and Normal Exit**: Prints "Parameterladen erfolgreich abgeschlossen" to standard output and terminates with exit code `0`.

---

## 7. ERROR HANDLING & EXIT CODES
* **Detection**: Explicit check of shell pipeline return variables (`$?`) stored inside temporary loop contexts (`rc=$?`).
* **Response**: Intercepts error statuses, writes descriptive logs specifying the exact utility that failed to Standard Error (`sys.stderr`), and immediately bubbles up the failure code.
* **Exit Codes**:
  * `0`: Script completed successfully.
  * `8`: Properties configuration file not found.
  * `sqlldr` return code (dynamic non-zero): SQL*Loader execution failed.
  * `sqlplus` return code (dynamic non-zero): Post-load script execution failed.
* **Python Mapping**: Convert exit checks into a `try/except` block catching `subprocess.CalledProcessError`. Missing files can raise `FileNotFoundError` or exit with a specific error message and code `8` to preserve compatibility.

---

## 8. OUTPUTS / SIDE EFFECTS
* **Database Updates**: Populates the table specified in `${STG_TABLE}` (typically `DWH_STG.PARAM_LOAD`).
* **Logs**: Generates/overwrites SQL*Loader diagnostic run logs at `${DWH_LOG_DIR}/param_load.log`.
* **Database Actions**: Triggers unknown downstream procedural logic encapsulated within `d_param_load.sql`.

---

## 9. BUSINESS SUMMARY
* **Environment Provisioning**: Validates and reads local runtime settings dynamically configured in the environment.
* **Data Ingestion**: Standardizes external application-level property records into structural relational records inside an Oracle Staging environment.
* **Configuration State Control**: Enables dynamic runtime configurations without demanding hard-coded procedural database updates.

---

# PYTHON PSEUDOCODE OUTLINE

```python
# Modern Python 3 translation of r_load_params.ksh
import os
import sys
import subprocess
from pathlib import Path

# Step 1: Initialize Environment and Configuration Variables
# # REVIEW-STRUCT: environment file dwh.profile not supplied — variables it sets are unknown; do not guess their names or values
dwh_home = os.environ.get("DWH_HOME", "")
dwh_log_dir = os.environ.get("DWH_LOG_DIR", "")

# Step 2: Define and Validate Properties File Presence
props_path = Path(dwh_home) / "cfg" / "dwh_env.properties"

if not props_path.is_file():
    print(f"FEHLER: Parameterdatei {props_path} nicht gefunden", file=sys.stderr)
    sys.exit(8)

# Step 3: Parse Configuration Parameters dynamically
# Reads 'db.host', 'db.sid', and 'stage.table' directly from the configuration file without spawning grep processes
db_host = ""
db_sid = ""
stg_table = ""

with open(props_path, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if line.startswith("db.host="):
            db_host = line.split("=", 1)[1]
        elif line.startswith("db.sid="):
            db_sid = line.split("=", 1)[1]
        elif line.startswith("stage.table="):
            stg_table = line.split("=", 1)[1]

# Step 4: Print Operational Information
print(f"Lade Parameter nach {stg_table} auf {db_host}/{db_sid}")

# Step 5: Execute SQL*Loader
control_file = Path(dwh_home) / "cfg" / "param_load.ctl"
log_file = Path(dwh_log_dir) / "param_load.log"

# # REVIEW: The connection utilizes OS / Wallet Authentication. Ensure client wallet/environment is properly set up.
sqlldr_command = [
    "sqlldr",
    f"userid=dwh_stg@{db_sid}",
    f"control={control_file}",
    f"data={props_path}",
    f"log={log_file}"
]

try:
    # Step 6: Run SQL*Loader process and verify execution status
    subprocess.run(sqlldr_command, check=True)
except subprocess.CalledProcessError as e:
    print(f"FEHLER: sqlldr beendet mit RC={e.returncode}", file=sys.stderr)
    sys.exit(e.returncode)

# Step 7: Execute Post-Load Processing SQL script
sql_script = Path(dwh_home) / "cfg" / "d_param_load.sql"
sqlplus_command = [
    "sqlplus",
    "-s",
    f"dwh_adm@{db_sid}",
    f"@{sql_script}"
]

try:
    # Step 8: Run SQL*Plus process and verify execution status
    subprocess.run(sqlplus_command, check=True)
except subprocess.CalledProcessError as e:
    print(f"FEHLER: d_param_load.sql beendet mit RC={e.returncode}", file=sys.stderr)
    sys.exit(e.returncode)

# Step 9: Process success notification and clean termination
print("Parameterladen erfolgreich abgeschlossen")
sys.exit(0)
```

# File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `config_env_linked_job/iscfg/bin/r_load_params.ksh` | `config_env_linked_job/iscfg/bin/r_load_params.py` | Converts KornShell parameter parsing, logging, and load orchestration logic to native GCP Python execution. |

---

# Execution Order
The legacy job's execution sequence must be preserved in the Cloud Composer (Airflow) DAG orchestration:
1. **Legacy Step 1 (`config_env_linked_job/DWH_CFG_JOB/DW.CFG_LOAD_PARAMS.xml`)**: UC4 Job orchestration. 
   * *Target Mapping*: Migrated to an Airflow DAG file (handled in the orchestration design pass) that schedules and runs the tasks sequentially.
2. **Legacy Step 2 (`config_env_linked_job/iscfg/bin/r_load_params.ksh`)**: Main execution shell script.
   * *Target Mapping*: Migrated to `config_env_linked_job/iscfg/bin/r_load_params.py` and run as a `PythonOperator` or `GCSStartPipelineOperator` in the Airflow DAG.
3. **Legacy Step 3 (`config_env_linked_job/iscfg/cfg/d_param_load.sql`)**: Post-load SQL execution.
   * *Target Mapping*: Executed in the DAG as a Dataform compilation/run task (or a `BigQueryInsertJobOperator`) immediately succeeding the Python parameter-loading task.

---

# Lineage
* **Downstream Consumers**:
  * `config_env_linked_job/iscfg/cfg/d_param_load.sql` (File-level relationship: executed via SQL*Plus within the shell wrapper. In the target environment, this transitions to a task-level DAG dependency).

---

# External System Replacements
* **Oracle Database (staging & target schemas)** is replaced by **Google BigQuery**.
* **SQL\*Loader (`sqlldr`)** utility is retired and replaced by a Python-native implementation using the `google-cloud-bigquery` client library. The script will read parameters directly from `dwh_env.properties` stored in GCS and stream/load them into the target BigQuery table.
* **SQL\*Plus (`sqlplus`)** script execution is retired and replaced with native Airflow Dataform execution triggers or BigQuery SQL executions.

---

# Cross-File Dependencies
* **Configuration Properties (`dwh_env.properties`)**: Sourced dynamically by `r_load_params.ksh` to extract the staging table name and DB SID. In GCP, this properties file should reside in a Google Cloud Storage bucket and be loaded dynamically at runtime.
* **SQL Post-Processing Script (`d_param_load.sql`)**: Stored locally in `/cfg/` and invoked at the end of parameter staging. In GCP, this logic is managed as a Dataform model.

---

# Target File Plan
* **Target File**: `config_env_linked_job/iscfg/bin/r_load_params.py`
  * *Language*: Python
  * *Source File*: `config_env_linked_job/iscfg/bin/r_load_params.ksh`

---

# Environment-Specific Values

### 1. GLOBAL (Environment-Wide)
* **`GCP_PROJECT`**: The target Google Cloud project ID (replaces database connection settings/SIDs).
  * *Source (Python)*: `os.environ.get("GCP_PROJECT")`
* **`GCS_BUCKET`**: The target Cloud Storage bucket containing configuration files and logs (replaces `${DWH_HOME}` path prefixes).
  * *Source (Python)*: `os.environ.get("GCS_BUCKET")`
* **Composer Logs**: The legacy log path `${DWH_LOG_DIR}` is retired. Log capture is handled globally and natively by Google Cloud Composer's logging mechanism.

### 2. JOB-SPECIFIC
* **Properties File Path (`PROPS`)**: Points to the runtime properties file location.
  * *Value*: `gs://{GCS_BUCKET}/config_env_linked_job/iscfg/cfg/dwh_env.properties`
* **Staging Table Name (`STG_TABLE` / `stage.table`)**: The dynamic loading destination table.
  * *Value*: `<GCP_PROJECT>.<BQ_DATASET_STG>.PARAM_LOAD` (mapped directly in the job config or read from properties).

---

# Risks and Manual Steps

* **SOURCE: NOT FOUND — `param_load.ctl` — no candidate**
  * *Risk*: The SQL\*Loader control file `param_load.ctl` is missing from the codebase. It details how the fields are structured and delimited inside `dwh_env.properties` during Oracle loading.
  * *Manual Action*: A developer must locate `param_load.ctl` or inspect the structure of the legacy properties file to configure the parsing rules (delimiters, target column mappings) in the target Python loader script.

* **SOURCE: NOT FOUND — `dwh.profile` — no candidate**
  * *Risk*: The environment-level shell profile `dwh.profile` is missing from the context.
  * *Manual Action*: Ensure any global variables or paths defined in `dwh.profile` that affect this execution are identified and configured as Composer/Airflow Variables or secret environment variables.

* **Downstream SQL Migration Gap**:
  * *Risk*: The SQL file `d_param_load.sql` is not in this design pass's scope. If its migration to BigQuery/Dataform fails or is delayed in the sibling design pass, the parameter-loading workflow cannot be validated end-to-end.
  * *Manual Action*: Confirm the target Dataform model or BigQuery table structure for `d_param_load.sql` is deployed before testing this Python script.

---

# DESIGN DOCUMENT: HIVEQL TO BIGQUERY SQL MIGRATION

## 1. System Overview & Logic Description
The objective of this process is to migrate a HiveQL parameter-loading script (`d_param_load.sql`) to Google Cloud BigQuery. The script performs an upsert (MERGE) operation to synchronize staging parameters into the target Data Warehouse (DWH) administration parameter table.

### Key Operational Logic:
- **Source Data**: Extracted from the staging table `DWH_STG.PARAM_LOAD`.
- **Target Table**: Updated/Inserted into the master administration table `DWH_ADM.JOB_PARAMS`.
- **Merge Criteria**: Matches records using the unique identifier `param_key`.
- **Matched Execution**: Updates the target parameter value (`param_value`) and sets the update timestamp (`updated_at`) using the source load timestamp (`loaded_at`).
- **Not Matched Execution**: Appends new parameter records containing the key, value, and update timestamp.
- **Transaction Handling**: The explicit `COMMIT;` in HiveQL is deprecated in BigQuery since BigQuery runs single DML statements (like `MERGE`) as atomic transactions automatically.

---

## 2. Low-Level Pseudocode

```
START TRANSACTION (Implicit in BigQuery for single DML)

DECLARE source_dataset VIEW OR SUBQUERY AS:
    SELECT 
        CAST(param_key AS STRING) AS param_key, 
        CAST(param_value AS STRING) AS param_value, 
        CAST(loaded_at AS TIMESTAMP) AS loaded_at
    FROM DWH_STG.PARAM_LOAD

MERGE INTO DWH_ADM.JOB_PARAMS AS tgt
USING source_dataset AS src
ON tgt.param_key = src.param_key

WHEN MATCHED THEN
    UPDATE SET 
        tgt.param_value = src.param_value,
        tgt.updated_at  = src.loaded_at

WHEN NOT MATCHED THEN
    INSERT (param_key, param_value, updated_at)
    VALUES (src.param_key, src.param_value, src.loaded_at)

END TRANSACTION
```

---

## 3. Data Type Mapping & Conversion Strategy

| Hive Column Name | Hive Data Type (Inferred) | BigQuery Target Data Type | Conversion Rules & Rationale |
| :--- | :--- | :--- | :--- |
| `param_key` | `VARCHAR`/`STRING` | `STRING` | Standard string representation. No transformation required. |
| `param_value` | `VARCHAR`/`STRING` | `STRING` | Standard string representation. No transformation required. |
| `loaded_at` / `updated_at` | `TIMESTAMP`/`STRING` | `TIMESTAMP` | Ensure date-time values are handled as BigQuery UTC `TIMESTAMP` for consistent temporal queries. |

---

## 4. Equivalent BigQuery SQL Query

```sql
-- d_param_load.sql — merge staged parameters into the DWH parameter table
MERGE INTO DWH_ADM.JOB_PARAMS tgt
USING (
    SELECT 
        CAST(param_key AS STRING) AS param_key, 
        CAST(param_value AS STRING) AS param_value, 
        CAST(loaded_at AS TIMESTAMP) AS loaded_at
    FROM DWH_STG.PARAM_LOAD
) src
ON (tgt.param_key = src.param_key)
WHEN MATCHED THEN UPDATE SET
    tgt.param_value = src.param_value,
    tgt.updated_at  = src.loaded_at
WHEN NOT MATCHED THEN INSERT (param_key, param_value, updated_at)
VALUES (src.param_key, src.param_value, src.loaded_at);
```

---

## 5. Entities List

### Files Used:
- `config_env_linked_job/iscfg/cfg/d_param_load.sql`

### Tables:
- **Target Table**: `DWH_ADM.JOB_PARAMS`
- **Source Table**: `DWH_STG.PARAM_LOAD`

### Columns:
- `param_key` (Key Field)
- `param_value` (Attribute Field)
- `loaded_at` (Source Timestamp Field)
- `updated_at` (Target Timestamp Field)

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `config_env_linked_job/iscfg/cfg/d_param_load.sql` | `config_env_linked_job/iscfg/cfg/d_param_load.sqlx` | Converted to a Dataform SQLX script that executes a native BigQuery `MERGE` statement to upsert staged parameters into the main parameter table. |

---

### Execution Order
The execution order defined in the legacy metadata is:
1. `config_env_linked_job/DWH_CFG_JOB/DW.CFG_LOAD_PARAMS.xml` (Orchestration trigger)
2. `config_env_linked_job/iscfg/bin/r_load_params.ksh` (Wrapper script that loads parameter files to staging)
3. `config_env_linked_job/iscfg/cfg/d_param_load.sql` (Merges staged parameters — **this design scope**)

In the target Google Cloud environment, the sequence must be preserved using Cloud Composer (Airflow) and Dataform:
- **Task 1**: Python operator/Dataflow job (migrated from the KSH script) extracts parameter properties and loads them into the staging table `PARAM_LOAD`.
- **Task 2**: A Dataform run task executes `d_param_load.sqlx` to execute the table merge immediately following the staging load.

---

### Lineage
- **Upstream Producer**: Reads from staging table `DWH_STG.PARAM_LOAD`.
- **Downstream Consumer**: Writes to/updates the target master parameter table `DWH_ADM.JOB_PARAMS`.

---

### Cross-File Dependencies
- `d_param_load.sqlx` directly depends on the successful completion of the upstream parameter-staging process (previously implemented in `r_load_params.ksh`). The target orchestration in Cloud Composer must guarantee that the staging table `DWH_STG.PARAM_LOAD` is completely populated with the current run's data before this Dataform model executes.

---

### Target File Plan
- **Target File Path**: `config_env_linked_job/iscfg/cfg/d_param_load.sqlx`
- **Source File**: `config_env_linked_job/iscfg/cfg/d_param_load.sql`
- **Language**: SQLX (Dataform)
- **Purpose**: Orchestrates and executes the SCD Type 1 `MERGE` operation inside BigQuery to synchronize keys from the staging table to the master job parameters table.

---

### Environment-Specific Values

#### GLOBAL (Environment-Wide Variables)
- **GCP_PROJECT**: The Google Cloud Project ID where the datasets reside. Sourced from the Airflow/Dataform environment config.
- **BQ_LOCATION**: The geographic location of the BigQuery datasets (e.g., `US` or `EU`). Sourced from Dataform settings.

#### JOB-SPECIFIC Variables
- **DWH_STG.PARAM_LOAD**: The source staging table name in BigQuery.
- **DWH_ADM.JOB_PARAMS**: The target master parameter table name in BigQuery.

---

### Risks and Manual Steps
- **Commit Logic Retirement**: The legacy SQL script ends with an explicit `COMMIT;` statement. Since BigQuery runs single DML statements (like `MERGE`) as atomic transactions automatically, this statement has been retired. No manual intervention is needed, but testing should verify atomic upsert behavior.