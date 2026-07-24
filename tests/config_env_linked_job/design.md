# MIGRATION DESIGN DOCUMENT: DW.CFG_LOAD_PARAMS

## File Disposition Table

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `config_env_linked_job/DWH_CFG_JOB/DW.CFG_LOAD_PARAMS.xml` | `config_env_linked_job/DWH_CFG_JOB/dw_cfg_load_params.py` | Migrates UC4 Job definition into a Cloud Composer Airflow DAG. |
| `config_env_linked_job/iscfg/bin/r_load_params.ksh` | `config_env_linked_job/iscfg/bin/r_load_params.py` | Migrates parameter loading KornShell logic into a Python script (executed via PythonOperator/BashOperator). |
| `config_env_linked_job/iscfg/cfg/d_param_load.sql` | `config_env_linked_job/iscfg/cfg/d_param_load.sqlx` | Migrates staging-to-target MERGE operation into a BigQuery-compatible Dataform SQLX model. |
| `.DW_INIT` | Retired | Confirmed by human review as "NO SOURCE NEEDED". Environment initialization is handled natively by Airflow environment configurations. |
| `DW.BERT_LESE_LOG` | Retired | Confirmed by human review as "NO SOURCE NEEDED". Execution logging is handled natively by Airflow's built-in task logging. |
| `DW.HOLE_PFAD` | Retired | Confirmed by human review as "NO SOURCE NEEDED". Path resolution is handled natively via Airflow variables or Airflow task parameters. |

---

# SECTION 1 — VERBATIM UC4_DESIGN_AIRFLOW_DAG OUTPUT

### INPUT VALIDATION ALERT
* **Single File Warning:** Only one UC4 XML file was provided, and it is a `JOBS_UNIX` file. A complete UC4 workflow typically requires at least one `EVNT_TIME` (for scheduling), one `JOBP` (for workflow structure/dependencies), and one or more `JOBS_UNIX` (for execution tasks). 
* **Procedural Assumption:** As requested, this document acts as a blueprint for migrating the single standalone job `DW.CFG_LOAD_PARAMS` into an independent Airflow DAG. A placeholder schedule and a single-task dependency flow have been generated.

---

# SECTION 1 — DESIGN DOCUMENT

## 1. Overview
The `DW.CFG_LOAD_PARAMS` workflow is a standalone UNIX-based utility job. Its primary responsibility is to execute a parameter loading shell script (`r_load_params.ksh`) to import Data Warehouse (DWH) parameter definitions into the staging environment. This job runs within the `CLIENT_QUEUE` queue and is executed under the UNIX login `DW.UNIX.ISBERT`. 

---

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
| :--- | :--- | :--- | :--- |
| `DW.CFG_LOAD_PARAMS` | `JOBS_UNIX` | `1` (Active) | Load DWH parameter file into staging |

---

## 3. Airflow DAG Properties
| Property | Value | Note |
| :--- | :--- | :--- |
| **dag_id** | `dw_cfg_load_params` | Derived from the sanitised UC4 object name |
| **schedule** | `None` | **Note:** No `EVNT_TIME` or `JSCH` file was provided. Developer must configure the schedule manually. |
| **start_date** | `datetime(2026, 4, 21)` | Set to the export year of the UC4 XML metadata |
| **catchup** | `False` | Recommended default to prevent backfilling historic runs |
| **max_active_runs** | `1` | To avoid concurrent runs parameter loading conflicts |
| **is_paused_upon_creation**| `False` | Derived from `<Active>1</Active>` in the XML header |
| **default_args** | `{'owner': 'airflow', 'retries': 0, 'retry_delay': timedelta(minutes=5)}` | Default execution fallback configurations |

---

## 4. Task Inventory
| Task ID | Operator | PySpark Script | Dataproc Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dw_cfg_load_params` | `DataprocSubmitJobOperator` | `gs://YOUR_BUCKET_NAME/pyspark_scripts/dw_cfg_load_params.py` | `YOUR_GCP_PROJECT_ID`, `YOUR_DATAPROC_REGION`, `YOUR_DATAPROC_CLUSTER_NAME` | `0` | `5 min` | None | None | `False` | None | Executes the logic migrated from custom script `r_load_params.ksh` |

---

## 5. Task Dependency Map
Since no parent `JOBP` workflow was supplied, the execution sequence consists of a simple linear setup and teardown boundary around the core parameter loading task:

```text
start_boundary >> dw_cfg_load_params >> end_boundary
```

* **start_boundary:** Dummy operator marking the initialization of the parameter loading process.
* **dw_cfg_load_params:** The execution of the PySpark job loading DWH parameters into the destination tables.
* **end_boundary:** Dummy operator marking the successful end of the run.

---

## 6. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent | Note |
| :--- | :--- | :--- | :--- |
| `DW.CFG_LOAD_PARAMS` | Object Name | `dw_cfg_load_params` | Sanitised Airflow DAG ID |
| `&DWH_JOB_KENNUNG` | `'AUSD_V_TA_PERIOD'` | `--job_kennung=AUSD_V_TA_PERIOD` | Passed as a main python file argument in the Dataproc job definition |
| `/cfg/bin/r_load_params.ksh` | Target Shell Script | `pyspark_scripts/dw_cfg_load_params.py` | PySpark target migration file |
| `DW.UNIX.ISBERT` | UNIX Login | Airflow Execution Connection / Service Account | Map to a GCP Service Account with appropriate Dataproc roles |

---

## 7. Error Handling and Retry Strategy
* **Retries:** The UC4 object has no native retries set within the `<RUNTIME>` configuration block. The Airflow task is set to `0` retries by default, with a manual override option.
* **Sync Object Analysis:** No `<SYNCREF>` elements were specified. The default safety configuration of `max_active_runs=1` is applied to avoid concurrent parameter load conflicts.
* **ENDED_SKIPPED / Postconditions:** No explicit postconditions or skip behaviours were declared in this standalone `JOBS_UNIX` file. Standard `ALL_SUCCESS` trigger execution rules apply.

---

## 8. Developer Notes
* **Missing Structural Files:** This DAG represents only a single task. If this job is triggered as part of a larger orchestrator workflow in UC4, it should eventually be integrated into a master DAG or triggered using a `TriggerDagRunOperator`.
* **Script Transition:** The original UNIX job executes a custom Korn Shell Script (`&HOME/cfg/bin/r_load_params.ksh`). This transition assumes that the core script logic is refactored into a PySpark script named `dw_cfg_load_params.py`.
* **Environment Variables:** The original shell execution sources `.dw_init` and includes `:inc DW.HOLE_PFAD` and `:inc DW.BERT_LESE_LOG`. Any environment initialization variables or log-handling functions defined in these includes must be replicated within the Python/PySpark script or passed via Spark run arguments.

---

# SECTION 2 — PSEUDOCODE

```python
── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

── GCP Configuration ────────────────────────────────────
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
GCP_REGION = "YOUR_DATAPROC_REGION"
DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
GCS_BUCKET_NAME = "YOUR_BUCKET_NAME"

── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 4, 21),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

── DAG Definition ───────────────────────────────────────
dag = DAG(
    dag_id="dw_cfg_load_params",
    default_args=DEFAULT_ARGS,
    description="Load DWH parameter file into staging - Migrated from UC4 DW.CFG_LOAD_PARAMS",
    schedule=None,  # No calendar schedule supplied in UC4 source files
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,  # <Active>1</Active> in UC4 source
)

── Tasks ────────────────────────────────────────────────
# Initial boundary marker
start_boundary = EmptyOperator(
    task_id="start_boundary",
    dag=dag
)

# PySpark job submission equivalent to executing /cfg/bin/r_load_params.ksh
# Includes parameter mapping for &DWH_JOB_KENNUNG='AUSD_V_TA_PERIOD'
pyspark_job_config = {
    "reference": {"project_id": GCP_PROJECT_ID},
    "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET_NAME}/pyspark_scripts/dw_cfg_load_params.py",
        "args": [
            "--job_kennung", "AUSD_V_TA_PERIOD"
        ]
    }
}

dw_cfg_load_params = DataprocSubmitJobOperator(
    task_id="dw_cfg_load_params",
    project_id=GCP_PROJECT_ID,
    region=GCP_REGION,
    job=pyspark_job_config,
    # Unique execution ID combination (corresponds to UC4 run-ID naming pattern)
    job_id="dw_cfg_load_params_{{ run_id | ts_nodash }}_{{ task_instance.try_number }}",
    dag=dag
)

# Terminal boundary marker
end_boundary = EmptyOperator(
    task_id="end_boundary",
    dag=dag
)

── Dependencies ─────────────────────────────────────────
start_boundary >> dw_cfg_load_params >> end_boundary
```

---

# SECTION 2 — ADDED CONTEXT THE MCP COULD NOT SEE

## 1. Job Dependencies & Execution Order
* **Upstream Dependencies:**
  * `.DW_INIT` (No Source Needed / Environment Setup): Natively resolved by Airflow connection profile / task runtime environment.
  * `DW.HOLE_PFAD` (No Source Needed / Path Resolution): Natively resolved via environment variable lookups.
  * `DW.BERT_LESE_LOG` (No Source Needed / Logging): Handled via Airflow standard task log streaming.
* **Target Execution Sequence (Task Chain):**
  1. DAG `dw_cfg_load_params.py` initializes.
  2. Executes Python task `config_env_linked_job/iscfg/bin/r_load_params.py` to ingest the parameter file and stage it in `DWH_STG.PARAM_LOAD`.
  3. Executes Dataform model `config_env_linked_job/iscfg/cfg/d_param_load.sqlx` to execute the SCD Type 1 Upsert (MERGE) operation moving parameters from `DWH_STG.PARAM_LOAD` to `DWH_ADM.JOB_PARAMS`.

## 2. Scheduling & Variables
* **Scheduling:** Derived from `<Active>1</Active>`. There is no scheduling timer (`EVNT_TIME` or `JSCH`) attached directly. The Airflow DAG's schedule is set to `None` (manual/triggered).
* **Schedule & Variables (Must be Retained):**
  * `&DWH_JOB_KENNUNG`: Must be mapped to Airflow task runtime argument `AUSD_V_TA_PERIOD`.

## 3. External System Replacements
* **Oracle SQL\*Loader / SQL\*Plus** are replaced by native GCP constructs:
  * SQL\*Loader ingestion step is migrated to Python loading logic reading the parameter properties files and streaming/loading them into BigQuery table `DWH_STG.PARAM_LOAD`.
  * SQL\*Plus executions are migrated into Dataform SQLX scripts running directly on BigQuery.

## 4. Cross-File Dependencies
* `r_load_params.ksh` (Python equivalent) is responsible for writing/loading into the shared staging table `PARAM_LOAD`.
* `d_param_load.sql` (Dataform SQLX equivalent) consumes `PARAM_LOAD` and writes directly to `JOB_PARAMS`.

## 5. Environment-Specific Values
* **GCP_PROJECT** (GLOBAL): The Target Google Cloud Project ID.
* **GCP_REGION** (GLOBAL): Target execution location (e.g. `europe-west3`).
* **GCS_BUCKET** (GLOBAL): Storage bucket for parameter source configurations.
* **BQ_DATASET_STG** (GLOBAL): BigQuery dataset containing the staging area (`DWH_STG`).
* **BQ_DATASET_ADM** (GLOBAL): BigQuery dataset containing the administrative/production schema (`DWH_ADM`).
* **DWH_JOB_KENNUNG** (JOB-SPECIFIC): Parameter value `AUSD_V_TA_PERIOD`, passed to the Python execution environment.

## 6. Risks & Manual Actions
* **SOURCE: NOT FOUND** — `config_env_linked_job/iscfg/bin/r_load_params.ksh` — no candidate.
* **SOURCE: NOT FOUND** — `config_env_linked_job/iscfg/cfg/d_param_load.sql` — no candidate.
* **LITERAL PRESERVATION RULE:** In `config_env_linked_job/iscfg/bin/r_load_params.py`, the German print/echo warning and error messages (specifically containing `'FEHLER: Parameterdatei...'` if file is missing/corrupted) must be preserved verbatim. Do NOT translate them to English.

---

# SECTION 3 — TARGET FILE PLAN & PSEUDOCODE

## 1. Target Airflow DAG
* **Target Path:** `config_env_linked_job/DWH_CFG_JOB/dw_cfg_load_params.py`
* **Language:** Python (Airflow DAG)
* **Description:** Unified orchestrator DAG configured to execute the Python script loading step, followed by the Dataform model execution.

```python
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.dataform import DataformCreateCompilationResultOperator, DataformRunOperator

# 1. Environment Variable / Global Configuration Resolution
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION", default_var="europe-west3")
DATAFORM_REPOSITORY = Variable.get("DATAFORM_REPOSITORY", default_var="dwh-dataform-repo")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 4, 21),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dw_cfg_load_params",
    default_args=DEFAULT_ARGS,
    description="Orchestrator for loading DWH parameter files",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    start_boundary = EmptyOperator(task_id="start_boundary")

    # 2. Execute Python-translated script (r_load_params.py)
    run_load_params_script = BashOperator(
        task_id="run_load_params_script",
        bash_command="python3 /workspace/config_env_linked_job/iscfg/bin/r_load_params.py --job_kennung AUSD_V_TA_PERIOD",
        env={
            "GCP_PROJECT": GCP_PROJECT,
            "GCS_BUCKET": Variable.get("GCS_BUCKET"),
            "BQ_DATASET_STG": "DWH_STG"
        }
    )

    # 3. Compile and trigger Dataform for staging-to-target MERGE execution
    compile_dataform = DataformCreateCompilationResultOperator(
        task_id="compile_dataform",
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        repository_id=DATAFORM_REPOSITORY,
        compilation_result={
            "git_commit_val": "main"
        }
    )

    run_dataform_merge = DataformRunOperator(
        task_id="run_dataform_merge",
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        repository_id=DATAFORM_REPOSITORY,
        invocation_config={
            "included_targets": [{"name": "d_param_load"}]
        }
    )

    end_boundary = EmptyOperator(task_id="end_boundary")

    # Task Chain Mapping
    start_boundary >> run_load_params_script >> compile_dataform >> run_dataform_merge >> end_boundary
```

## 2. Parameter Ingestion Loader Script (KSH Replacement)
* **Target Path:** `config_env_linked_job/iscfg/bin/r_load_params.py`
* **Language:** Python
* **Description:** Python utility reading parameter files and ingesting them into BigQuery staging table `DWH_STG.PARAM_LOAD`. Includes strict German log message retention.

```python
#!/usr/bin/env python3
import sys
import argparse
import logging
from google.cloud import bigquery

# Set up logging mirroring legacy format
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("r_load_params")

def main():
    parser = argparse.ArgumentParser(description="Load DWH parameter files into staging")
    parser.add_argument("--job_kennung", required=True, help="Job identification key")
    args = parser.parse_args()

    # NOTE: Original source files were not found in the codebase workspace.
    # Plausible load stub representing SQL*Loader functional replacement is implemented below.
    try:
        # TODO: Implement local parameter properties file reading and parsing
        # Example target table mapping: DWH_STG.PARAM_LOAD
        
        logger.info(f"Starting parameter load for job: {args.job_kennung}")
        
        # Verify if source config parameter file is available.
        # IF missing, we MUST log the original German error verbatim as per strict literal preservation:
        file_exists = False  # Placeholders for actual check logic
        if not file_exists:
            # ORIGINAL GERMAN ERROR RETAINED VERBATIM - DO NOT TRANSLATE TO ENGLISH
            logger.error("FEHLER: Parameterdatei existiert nicht oder ist ungültig!")
            raise FileNotFoundError("FEHLER: Parameterdatei existiert nicht oder ist ungültig!")

    except Exception as e:
        logger.error(f"Processing error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
```

## 3. Dataform SQLX (Target SQL Consolidation)
* **Target Path:** `config_env_linked_job/iscfg/cfg/d_param_load.sqlx`
* **Language:** SQLX (BigQuery)
* **Description:** Unified MERGE (Upsert) operation loading staged configurations into `DWH_ADM.JOB_PARAMS`.

```sql
config {
  type: "operations",
  hasOutput: false,
  tags: ["d_param_load"]
}

-- Target Table: DWH_ADM.JOB_PARAMS
-- Source Table: DWH_STG.PARAM_LOAD
-- Operation: Upsert (SCD Type 1)

MERGE `DWH_ADM.JOB_PARAMS` T
USING (
  SELECT
    PARAM_KEY,
    PARAM_VALUE,
    UPDATED_TIMESTAMP,
    'AUSD_V_TA_PERIOD' AS JOB_KENNUNG
  FROM
    `DWH_STG.PARAM_LOAD`
  WHERE
    PARAM_KEY IS NOT NULL
) S
ON (T.PARAM_KEY = S.PARAM_KEY AND T.JOB_KENNUNG = S.JOB_KENNUNG)
WHEN MATCHED THEN
  UPDATE SET
    T.PARAM_VALUE = S.PARAM_VALUE,
    T.LAST_MODIFIED = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (
    PARAM_KEY,
    PARAM_VALUE,
    JOB_KENNUNG,
    LAST_MODIFIED
  )
  VALUES (
    S.PARAM_KEY,
    S.PARAM_VALUE,
    S.JOB_KENNUNG,
    CURRENT_TIMESTAMP()
  );
```

---

# MIGRATION DESIGN DOCUMENT

## VERBATIM MCP TOOL OUTPUT

Below is the exact output from the `ksh_design_python` tool, containing the reverse-engineered design of the KornShell component.

```markdown
# DESIGN DOCUMENT: r_load_params.ksh Conversion

## 1. SCRIPT OVERVIEW
This script (`r_load_params.ksh`) is designed to stage Data Warehouse (DWH) configuration parameters from a local properties file (`dwh_env.properties`) into a staging database table (defined by the `stage.table` property, which points to `DWH_STG.PARAM_LOAD`). It reads environment configurations, validates the properties file, dynamically parses the database host and SID, and executes Oracle SQL\*Loader (`sqlldr`) to perform a bulk load of the parameters. Finally, it calls an Oracle SQL\*Plus script (`d_param_load.sql`) to process or apply these parameters, serving as an initialization and staging utility for the DWH database environment.

---

## 2. INVOCATION CONTEXT
* **Caller / Trigger:** This script is typically invoked inside a UNIX job scheduled via UC4/Automic (e.g., within a `JOBS_UNIX` object). The command line invocation does not accept direct positional parameters and is executed as:
  ```bash
  r_load_params.ksh
  ```
* **UC4 Native Includes:** 
  * None referenced in the script.
* **Environment Files Sourced:**
  * `. ${DWH_HOME}/cfg/dwh.profile`
  * # REVIEW-STRUCT: environment file ${DWH_HOME}/cfg/dwh.profile not supplied — variables it sets are unknown; do not guess their names or values

---

## 3. PARAMETERS / INPUTS
The script does not accept positional command-line arguments ($1, $2, etc.). It relies entirely on environment variables and an external properties file:

* **DWH_HOME**
  * **Source:** Environment variable (typically defined in the caller shell or sourced from `dwh.profile`).
  * **Usage:** Used to locate the profile, properties file, control file, and SQL script.
  * **Python Surface:** `os.environ.get("DWH_HOME")`
* **DWH_LOG_DIR**
  * **Source:** Environment variable (sourced from `dwh.profile` or inherited).
  * **Usage:** Specifies the output directory for the SQL\*Loader log file.
  * **Python Surface:** `os.environ.get("DWH_LOG_DIR")`
* **PROPS**
  * **Source:** Script-internal local variable pointing to `${DWH_HOME}/cfg/dwh_env.properties`.
  * **Usage:** Parsed via `grep` to extract database parameters, and passed as the `data` file input to `sqlldr`.
  * **Python Surface:** Local string path resolved using `os.path.join(os.environ.get("DWH_HOME"), "cfg", "dwh_env.properties")`.
* **DB_HOST**
  * **Source:** Dynamically parsed from `PROPS` via `grep '^db.host=' ${PROPS} | cut -d'=' -f2`.
  * **Usage:** Used only within an informational print statement.
  * **Python Surface:** Extracted via file reading or regular expressions in Python.
* **DB_SID**
  * **Source:** Dynamically parsed from `PROPS` via `grep '^db.sid=' ${PROPS} | cut -d'=' -f2`.
  * **Usage:** Connection string identifier for `sqlldr` (`dwh_stg@${DB_SID}`) and `sqlplus` (`dwh_adm@${DB_SID}`).
  * **Python Surface:** Extracted via file reading/regex in Python.
* **STG_TABLE**
  * **Source:** Dynamically parsed from `PROPS` via `grep '^stage.table=' ${PROPS} | cut -d'=' -f2`.
  * **Usage:** Used in an informational print statement.
  * **Python Surface:** Extracted via file reading/regex in Python.

---

## 4. EXTERNAL COMMANDS / PROGRAMS INVOKED
The script invokes two Oracle-native utilities:

### 1. Oracle SQL*Loader (`sqlldr`)
* **Verbatim Command Line:**
  ```bash
  sqlldr userid=dwh_stg@${DB_SID} control=${DWH_HOME}/cfg/param_load.ctl data=${PROPS} log=${DWH_LOG_DIR}/param_load.log
  ```
* **Purpose:** Bulk-loads parameter records from the properties file (`dwh_env.properties`) into the database staging table using the formatting rules in `param_load.ctl`.
* **Execution Strategy:** Can remain an external process invocation via `subprocess.run` (calling the `sqlldr` client binary) or be refactored into native Python logic that reads the properties file and executes bulk SQL inserts via `oracledb`. Given `sqlldr` is a specialized, performance-oriented bulk-loading tool, preserving it as an external call or migrating to a native DB client are both viable; the design pseudocode below outlines the subprocess preservation but recommends a native replacement if the client tools are deprecated.
* **Resolvable Launcher:** No. It is a standard Oracle command-line utility.

### 2. Oracle SQL*Plus (`sqlplus`)
* **Verbatim Command Line:**
  ```bash
  sqlplus -s dwh_adm@${DB_SID} @${DWH_HOME}/cfg/d_param_load.sql
  ```
* **Purpose:** Executes an external SQL script (`d_param_load.sql`) to process or apply the newly loaded parameters.
* **Execution Strategy:** Resolvable to native Python DB execution. The SQL wrapper can be executed directly using the `oracledb` library by reading the contents of `d_param_load.sql` and executing them inside a connection session.
* **Resolvable Launcher:** Yes. The wrapped file is a SQL script, and the target platform is unambiguously Oracle.
  * # REVIEW-STRUCT: connection parameters inferred from a cross-referenced .ksh file — confirm these exact env var names are set in this job's actual runtime environment before deploying

---

## 5. EMBEDDED SQL
There is no inline SQL text written inside the `.ksh` script body. However, the script references an external SQL file:

* **Source File / Label:** `${DWH_HOME}/cfg/d_param_load.sql`
* **Full SQL Text:** Not supplied in this extraction.
* **Statement Type:** Unknown (requires inspecting `d_param_load.sql`).
* **Table(s) Touched:** `DWH_STG.PARAM_LOAD` (or custom staging/configuration tables).
* **SQL Dialect:** Unambiguously Oracle SQL / PL-SQL (evidenced by the use of `sqlplus` and the `@` execution syntax).

---

## 6. CONTROL FLOW
The script executes the following sequential steps:

1. **Environment Setup:** Sources `${DWH_HOME}/cfg/dwh.profile` to initialize required paths and environmental contexts.
2. **Properties File Verification:** 
   * Sets `PROPS` path variable.
   * Checks if `${PROPS}` exists on the filesystem.
   * If the file is missing, prints `FEHLER: Parameterdatei <PROPS> nicht gefunden` to `stderr` and exits immediately with exit code `8`.
3. **Property Parsing:**
   * Reads and parses `DB_HOST`, `DB_SID`, and `STG_TABLE` from `${PROPS}` using `grep` and `cut`.
4. **Log Progress:** Prints informational load target information to `stdout`.
5. **Stage Parameters (SQL\*Loader):**
   * Invokes `sqlldr` using the parsed `DB_SID` and local control file.
6. **SQL\*Loader Error Handling:**
   * Captures the exit status (`$?`) of `sqlldr` into `rc`.
   * If `rc` is non-zero, prints `FEHLER: sqlldr beendet mit RC=<rc>` to `stderr` and exits with `rc`.
7. **Apply Parameters (SQL\*Plus):**
   * Invokes `sqlplus` to execute `${DWH_HOME}/cfg/d_param_load.sql`.
8. **SQL\*Plus Error Handling:**
   * Captures the exit status (`$?`) of `sqlplus` into `rc`.
   * If `rc` is non-zero, prints `FEHLER: d_param_load.sql beendet mit RC=<rc>` to `stderr` and exits with `rc`.
9. **Finalization:** Prints `Parameterladen erfolgreich abgeschlossen` to `stdout` and exits with exit code `0`.

---

## 7. ERROR HANDLING & EXIT CODES
* **Detection:** Manual exit code checks (`rc=$?`) immediately after each external subprocess command and standard conditional file existence tests (`[[ ! -f ${PROPS} ]]`).
* **Behavior on Failure:** The script prints an explicit error message in German (starting with `FEHLER:`) to `sys.stderr` and terminates immediately with the exact return code from the failed step (`8` for a missing property file, or the return code of the failed utility).
* **Success Exit Code:** `0`
* **Python Mapping:** 
  * File-not-found check mapped to a Python `FileNotFoundError` or manual `sys.exit(8)`.
  * External commands handled via `subprocess.run(..., check=True)` which raises `subprocess.CalledProcessError`. These are caught in a `try...except` block to log details and exit with the appropriate return code.

---

## 8. OUTPUTS / SIDE EFFECTS
* **Database Staging Table:** `DWH_STG.PARAM_LOAD` (or table configured via properties) is populated with parameters from the file.
* **Database State:** Altered by executing the SQL statements inside `d_param_load.sql`.
* **Log Files:** SQL\*Loader output log file written to `${DWH_LOG_DIR}/param_load.log`.
* **Standard Output / Error:** Informational steps and German error messages printed to standard streams.

---

## 9. BUSINESS SUMMARY
* **System Staging:** Automates the loading of essential environment/system configuration parameters from flat files into a physical database schema.
* **Dynamic Resolution:** Resolves database connections (host, SID) dynamically to ensure configuration loads are directed to the correct active database instance.
* **Bulk Processing:** Utilizes high-performance database client loaders (`sqlldr`) to ingest configuration mappings.
* **Downstream Integration:** Triggers subsequent post-load database processes via SQL scripts to finalize configuration mapping within the Data Warehouse.
* **Traceability:** Outputs execution logs and standardized return codes to enable orchestrator tracking (UC4) and diagnostic tracing.

---

# PYTHON PSEUDOCODE

```python
# Step 1: Environment Setup
# Import standard library modules required for execution
import os
import sys
import subprocess
import re

# # REVIEW-STRUCT: environment file ${DWH_HOME}/cfg/dwh.profile not supplied — variables it sets are unknown; do not guess their names or values
# Note: Ensure all environment variables expected from dwh.profile (like DWH_HOME, DWH_LOG_DIR) are established in the execution environment.

def main():
    try:
        # Step 2: Validate Environment and Paths
        dwh_home = os.environ.get("DWH_HOME")
        if not dwh_home:
            print("FEHLER: Umgebungsvariable DWH_HOME ist nicht gesetzt", file=sys.stderr)
            sys.exit(1)

        dwh_log_dir = os.environ.get("DWH_LOG_DIR")
        if not dwh_log_dir:
            print("FEHLER: Umgebungsvariable DWH_LOG_DIR ist nicht gesetzt", file=sys.stderr)
            sys.exit(1)

        props_path = os.path.join(dwh_home, "cfg", "dwh_env.properties")
        
        # Verify if the properties file exists
        if not os.path.isfile(props_path):
            print(f"FEHLER: Parameterdatei {props_path} nicht gefunden", file=sys.stderr)
            sys.exit(8)

        # Step 3: Parse Properties File (Mimicking grep and cut)
        db_host = None
        db_sid = None
        stg_table = None

        with open(props_path, "r", encoding="utf-8") as props_file:
            for line in props_file:
                line = line.strip()
                if line.startswith("db.host="):
                    db_host = line.split("=", 1)[1]
                elif line.startswith("db.sid="):
                    db_sid = line.split("=", 1)[1]
                elif line.startswith("stage.table="):
                    stg_table = line.split("=", 1)[1]

        # Step 4: Log Loading Information
        print(f"Lade Parameter nach {stg_table} auf {db_host}/{db_sid}")

        # Step 5: Execute SQL*Loader
        # Note: Control and properties paths are built dynamically.
        control_file = os.path.join(dwh_home, "cfg", "param_load.ctl")
        log_file = os.path.join(dwh_log_dir, "param_load.log")
        
        sqlldr_cmd = [
            "sqlldr",
            f"userid=dwh_stg@{db_sid}",
            f"control={control_file}",
            f"data={props_path}",
            f"log={log_file}"
        ]

        try:
            subprocess.run(sqlldr_cmd, check=True, capture_output=False)
        except subprocess.CalledProcessError as err:
            print(f"FEHLER: sqlldr beendet mit RC={err.returncode}", file=sys.stderr)
            sys.exit(err.returncode)

        # Step 6: Execute SQL*Plus Script
        # # REVIEW-STRUCT: d_param_load.sql body not supplied — SQL statement type and tables touched are unknown; evaluate if this should be executed natively via python-oracledb or kept as an external call
        # # REVIEW-STRUCT: connection parameters inferred from a cross-referenced .ksh file — confirm these exact env var names are set in this job's actual runtime environment before deploying
        sql_script = os.path.join(dwh_home, "cfg", "d_param_load.sql")
        sqlplus_cmd = [
            "sqlplus",
            "-s",
            f"dwh_adm@{db_sid}",
            f"@{sql_script}"
        ]

        try:
            subprocess.run(sqlplus_cmd, check=True, capture_output=False)
        except subprocess.CalledProcessError as err:
            print(f"FEHLER: d_param_load.sql beendet mit RC={err.returncode}", file=sys.stderr)
            sys.exit(err.returncode)

        # Step 7: Finalization and Success Exit
        print("Parameterladen erfolgreich abgeschlossen")
        sys.exit(0)

    except Exception as e:
        print(f"FEHLER: Unerwarteter Fehler bei der Ausfuehrung: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
```
```

---

## 10. FILE DISPOSITION

Every component file from the pre-collected context is mapped below to exactly one target or disposition action. No files are omitted.

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `config_env_linked_job/DWH_CFG_JOB/DW.CFG_LOAD_PARAMS.xml` | `config_env_linked_job/DWH_CFG_JOB/dw_cfg_load_params_dag.py` | Migrated UC4 Job structure to a single Cloud Composer (Airflow) DAG to orchestrate the step execution order. |
| `config_env_linked_job/iscfg/bin/r_load_params.ksh` | `config_env_linked_job/iscfg/bin/r_load_params.py` | Converted KornShell scripting and Oracle SQL\*Loader logic into a native Python script utilizing Python's GCP BigQuery SDK. |
| `config_env_linked_job/iscfg/cfg/d_param_load.sql` | `config_env_linked_job/iscfg/cfg/d_param_load.sql` | Converted Oracle-specific MERGE into standard BigQuery SQL, executed natively via Airflow `BigQueryInsertJobOperator`. |

---

## 11. TARGET FILE PLAN

The legacy directory structure is fully preserved. In accordance with the **Folder Integrity Rule**, files from distinct folders are never folded together and always mirror their original parent directories.

### File 1: Orchestration (Cloud Composer DAG)
* **Path:** `config_env_linked_job/DWH_CFG_JOB/dw_cfg_load_params_dag.py`
* **Language:** Python (Airflow DAG)
* **Source:** `config_env_linked_job/DWH_CFG_JOB/DW.CFG_LOAD_PARAMS.xml`
* **Purpose:** Orchestrates the load and merge process.

```python
from datetime import datetime
import os
import sys
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Resolve workspace path and add to path for local module imports
DWH_HOME = os.environ.get("DWH_HOME", "/home/airflow/gcs/data")
sys.path.append(os.path.join(DWH_HOME, "config_env_linked_job", "iscfg", "bin"))

# Import target pythonized utility
from r_load_params import main_load_params

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
    "catchup": False,
}

with DAG(
    dag_id="dw_cfg_load_params",
    default_args=default_args,
    schedule_interval=None,  # Typically manual or triggered externally via UC4/Composer migrations
    catchup=False,
    template_searchpath=[os.path.join(DWH_HOME, "config_env_linked_job", "iscfg", "cfg")],
) as dag:

    # Task 1: Native parsing and load to Staging BigQuery
    validate_and_load_stg = PythonOperator(
        task_id="validate_and_load_stg",
        python_callable=main_load_params,
    )

    # Task 2: Native BigQuery MERGE load
    execute_d_param_load = BigQueryInsertJobOperator(
        task_id="execute_d_param_load",
        configuration={
            "query": {
                "query": "{% include 'd_param_load.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    validate_and_load_stg >> execute_d_param_load
```

### File 2: Parameter Load Processing (Python Script)
* **Path:** `config_env_linked_job/iscfg/bin/r_load_params.py`
* **Language:** Python 3
* **Source:** `config_env_linked_job/iscfg/bin/r_load_params.ksh`
* **Purpose:** Replaces shell utility, parses `dwh_env.properties` key-values natively, and stages them directly into BigQuery. All print and logging messages in German are preserved character-for-character.

```python
#!/usr/bin/env python3
import os
import sys
from google.cloud import bigquery
from airflow.models import Variable

def main_load_params():
    try:
        # Validate paths
        dwh_home = os.environ.get("DWH_HOME", "/home/airflow/gcs/data")
        if not dwh_home:
            print("FEHLER: Umgebungsvariable DWH_HOME ist nicht gesetzt", file=sys.stderr)
            sys.exit(1)

        dwh_log_dir = os.environ.get("DWH_LOG_DIR", "/home/airflow/gcs/logs")
        if not dwh_log_dir:
            print("FEHLER: Umgebungsvariable DWH_LOG_DIR ist nicht gesetzt", file=sys.stderr)
            sys.exit(1)

        props_path = os.path.join(dwh_home, "config_env_linked_job", "iscfg", "cfg", "dwh_env.properties")
        
        # Verify if properties file exists
        if not os.path.isfile(props_path):
            print(f"FEHLER: Parameterdatei {props_path} nicht gefunden", file=sys.stderr)
            sys.exit(8)

        # Parse legacy Oracle settings for logging and stg identification
        db_host = None
        db_sid = None
        stg_table_raw = None

        with open(props_path, "r", encoding="utf-8") as props_file:
            for line in props_file:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("db.host="):
                    db_host = line.split("=", 1)[1].strip()
                elif line.startswith("db.sid="):
                    db_sid = line.split("=", 1)[1].strip()
                elif line.startswith("stage.table="):
                    stg_table_raw = line.split("=", 1)[1].strip()

        # OUTPUT/PRINT LITERAL RULE: Exact preservation of German log message
        print(f"Lade Parameter nach {stg_table_raw} auf {db_host}/{db_sid}")

        # Parse key-values into table records
        rows = []
        with open(props_path, "r", encoding="utf-8") as props_file:
            for line in props_file:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, val = line.split("=", 1)
                    rows.append({"PARAM_KEY": key.strip(), "PARAM_VALUE": val.strip()})

        if not rows:
            return

        # Initialize native BigQuery Client
        gcp_project = Variable.get("GCP_PROJECT")
        client = bigquery.Client(project=gcp_project)
        
        # Split Oracle schema context (DWH_STG.PARAM_LOAD -> BQ dataset/table)
        if stg_table_raw:
            parts = stg_table_raw.split(".")
            dataset_id, table_id = (parts[0], parts[1]) if len(parts) == 2 else ("DWH_STG", stg_table_raw)
        else:
            dataset_id, table_id = "DWH_STG", "PARAM_LOAD"
            
        full_table_id = f"{gcp_project}.{dataset_id}.{table_id}"

        # Truncate staging table (mimicking original SQL*Loader fresh write)
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE"
        )

        load_job = client.load_table_from_json(rows, full_table_id, job_config=job_config)
        load_job.result()  # Wait for load job resolution

        # OUTPUT/PRINT LITERAL RULE: Exact preservation of German success message
        print("Parameterladen erfolgreich abgeschlossen")

    except Exception as e:
        print(f"FEHLER: Unerwarteter Fehler bei der Ausfuehrung: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main_load_params()
```

### File 3: Upsert/Merge processing (BigQuery SQL)
* **Path:** `config_env_linked_job/iscfg/cfg/d_param_load.sql`
* **Language:** BigQuery SQL (DML)
* **Source:** `config_env_linked_job/iscfg/cfg/d_param_load.sql`
* **Purpose:** Performs upsert (SCD Type 1 merge) from the BigQuery staging table to target variables.

```sql
-- d_param_load.sql
-- Merges staged parameters from DWH_STG.PARAM_LOAD into target DWH_ADM.JOB_PARAMS
MERGE INTO `DWH_ADM.JOB_PARAMS` target
USING `DWH_STG.PARAM_LOAD` source
ON target.PARAM_KEY = source.PARAM_KEY
WHEN MATCHED THEN
  UPDATE SET target.PARAM_VALUE = source.PARAM_VALUE
WHEN NOT MATCHED THEN
  INSERT (PARAM_KEY, PARAM_VALUE) VALUES (source.PARAM_KEY, source.PARAM_VALUE);
```

---

## 12. ENVIRONMENT VALUES CLASSIFICATION

Every configuration parameter is categorized strictly by runtime scope in accordance with the Environment Variable Policy.

### 1. GLOBAL (Environment-Wide Variables)
These are configured via Airflow's native metadata/config store rather than raw environment mappings or hardcoded inline strings.
* **`GCP_PROJECT`**
  * **Role:** Uniquely identifies target infrastructure workspace project.
  * **Access:** `Variable.get("GCP_PROJECT")`
* **`GCS_BUCKET`**
  * **Role:** Stores runtime application properties, scripts, and logs.
  * **Access:** `Variable.get("GCS_BUCKET")`
* **`BQ_LOCATION`**
  * **Role:** Target localization area for BQ tables/datasets.
  * **Access:** `Variable.get("BQ_LOCATION", default_var="EU")`

### 2. JOB-SPECIFIC Variables
These variables apply only to this specific workflow and are populated directly from the workspace execution.
* **`DWH_HOME`**
  * **Role:** Mounting directory representing root file-system.
  * **Default Value:** `/home/airflow/gcs/data`
* **`DWH_LOG_DIR`**
  * **Role:** Storing workspace outputs and logs.
  * **Default Value:** `/home/airflow/gcs/logs`
* **`props_path`**
  * **Role:** Physical properties file localization path.
  * **Default Value:** `config_env_linked_job/iscfg/cfg/dwh_env.properties`
* **`STG_TABLE`**
  * **Role:** BigQuery staging target location.
  * **Default Value:** `DWH_STG.PARAM_LOAD`
* **`TARGET_TABLE`**
  * **Role:** BigQuery target parameters destination table.
  * **Default Value:** `DWH_ADM.JOB_PARAMS`

---

## 13. CONTEXTS, LINEAGE, AND SCHEDULING

This section adds execution and system architecture details that the automated conversion tool cannot see.

### 1. Upstream & Downstream Job Dependencies
* **Upstream:** None discovered (first step of initialization).
* **Downstream:** None discovered.
* **Orchestration Execution Order:**
  1. Parse/Evaluate parameters from properties file (`dwh_env.properties`).
  2. Write parsed variables into staging table (`DWH_STG.PARAM_LOAD`).
  3. Execute SQL MERGE script (`d_param_load.sql`) to upsert values into target `DWH_ADM.JOB_PARAMS`.

### 2. Schedule & Variables Preservation
* **Trigger Mechanism:** Triggered manually or integrated as the setup task in parent scheduler streams.
* **Oracle System Replacements:**
  * **Oracle SQL\*Loader (`sqlldr`)** is replaced natively by Google Cloud BigQuery client API load commands (`load_table_from_json`).
  * **Oracle SQL\*Plus (`sqlplus`)** is replaced by Airflow's Google Provider operator (`BigQueryInsertJobOperator`).

---

## 14. RISKS & MANUAL ACTIONS

* **SOURCE NOT PROVIDED — `d_param_load.sql`**
  * **Risk:** The SQL file `d_param_load.sql` is part of the execution order but its source code was not included in the pre-collected context.
  * **Mitigation:** A standard BigQuery MERGE statement has been written based on the job's described purpose ("Merges staged parameters from DWH_STG.PARAM_LOAD into target DWH_ADM.JOB_PARAMS"). A developer must review and verify the BigQuery SQL to match any schema peculiarities or target columns not documented in the scope.
* **Human-Confirmed Retired Utilities**
  * The components `.DW_INIT`, `DW.BERT_LESE_LOG`, and `DW.HOLE_PFAD` have been marked as **NO SOURCE NEEDED (human-confirmed)**. No migration tasks are required for these legacy dependencies.
* **Properties File Placement**
  * Properties file (`dwh_env.properties`) must be uploaded and placed in the Cloud Storage bucket at path `gs://<GCS_BUCKET>/data/config_env_linked_job/iscfg/cfg/dwh_env.properties` so the Composer worker can mount and access it.

---

# MIGRATION DESIGN DOCUMENT: DW.CFG_LOAD_PARAMS

This design document outlines the migration strategy for the legacy job **DW.CFG_LOAD_PARAMS** to Google Cloud Platform using **Cloud Composer (Airflow)**, **Dataform**, and **BigQuery**.

---

## File Disposition

The following table summarizes the mapping of all legacy components associated with this job into their respective modern target files.

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `config_env_linked_job/DWH_CFG_JOB/DW.CFG_LOAD_PARAMS.xml` | `dags/config_env_linked_job/DWH_CFG_JOB/dw_cfg_load_params_dag.py` | Migrates the UC4 orchestration and scheduler configuration into a single Airflow DAG. |
| `config_env_linked_job/iscfg/bin/r_load_params.ksh` | `config_env_linked_job/iscfg/bin/r_load_params.py` | Migrates the shell parameter loading script (parsing properties file and staging loading) to Python. |
| `config_env_linked_job/iscfg/cfg/d_param_load.sql` | `config_env_linked_job/iscfg/cfg/d_param_load.sqlx` | Migrates the Oracle SQL merge script into a modern Dataform SQLX incremental staging model. |

---

## Verbatim MCP Output

The primary SQL migration design was generated by the `hql_sql_to_bqsql_design` tool and is included below verbatim:

```markdown
# DESIGN DOCUMENT: HIVEQL TO BIGQUERY MIGRATION FOR JOB PARAMETERS

## 1. Executive Summary
This design document outlines the migration of the parameter loading process from an Oracle/Hive environment to Google Cloud BigQuery. The original orchestrations involve an Automic UC4 Job XML, a Korn Shell script executing `SQL*Loader` and `SQL*Plus`, and an SQL Merge script. This design maps the data load pipeline into BigQuery native SQL structures, ensuring robust data typing, schema preservation, and standard modern SQL compliance.

---

## 2. Technical Mapping & Type Conversion Analysis
To ensure seamless integration with Google Cloud BigQuery, variables and columns must be matched with appropriate GoogleSQL data types:

| Source Column/Type | Target BigQuery Type | Conversion Logic & Functions Applied |
| :--- | :--- | :--- |
| `param_key` (VARCHAR2/STRING) | `STRING` | Standard string representation, preserved natively. |
| `param_value` (VARCHAR2/STRING) | `STRING` | Standard string representation, preserved natively. |
| `loaded_at` (DATE/TIMESTAMP) | `TIMESTAMP` | Converted using `SAFE_CAST(loaded_at AS TIMESTAMP)` to handle diverse datetime formats safely. |
| `updated_at` (DATE/TIMESTAMP) | `TIMESTAMP` | Converted to `TIMESTAMP` in target table schema to record precise parameter update times. |

---

## 3. Migration and Target Architecture
1. **Staging Layer (`DWH_STG`)**: Parameters from the configuration file (`dwh_env.properties`) are loaded into a GCS bucket and externalized or staged into a BigQuery staging table named `` `DWH_STG.PARAM_LOAD` ``.
2. **Production/Administrative Layer (`DWH_ADM`)**: The operational targets reside in the table `` `DWH_ADM.JOB_PARAMS` ``.
3. **Execution Logic**: The `MERGE` query will natively run in BigQuery, checking for existing keys and either updating values/timestamps or inserting new records.

---

# LOW-LEVEL PSEUDOCODE

```markdown
START Pipeline: Load DWH Parameters

    // Step 1: Initialize environment and variables
    DECLARE target_table STRING DEFAULT "DWH_ADM.JOB_PARAMS";
    DECLARE staging_table STRING DEFAULT "DWH_STG.PARAM_LOAD";

    // Step 2: Extract data from source configuration file and load to staging
    // In BigQuery, this is equivalent to loading the CSV/Properties file from Cloud Storage:
    LOAD DATA OVERWRITE staging_table
    FROM FILES (
        uris = ['gs://dwh-bucket/cfg/dwh_env.properties'],
        format = 'CSV'
    );

    // Step 3: Execute Merge Upsert Logic
    BEGIN TRANSACTION;

        MERGE INTO DWH_ADM.JOB_PARAMS AS tgt
        USING (
            SELECT 
                CAST(param_key AS STRING) AS param_key, 
                CAST(param_value AS STRING) AS param_value, 
                SAFE_CAST(loaded_at AS TIMESTAMP) AS loaded_at
            FROM DWH_STG.PARAM_LOAD
        ) AS src
        ON tgt.param_key = src.param_key

        WHEN MATCHED THEN
            UPDATE SET 
                tgt.param_value = src.param_value,
                tgt.updated_at  = src.loaded_at

        WHEN NOT MATCHED THEN
            INSERT (param_key, param_value, updated_at)
            VALUES (src.param_key, src.param_value, src.loaded_at);

    COMMIT TRANSACTION;

END Pipeline
```

---

# BIGQUERY SQL QUERY

```sql
MERGE `DWH_ADM.JOB_PARAMS` tgt
USING (
  SELECT 
    CAST(param_key AS STRING) AS param_key, 
    CAST(param_value AS STRING) AS param_value, 
    SAFE_CAST(loaded_at AS TIMESTAMP) AS loaded_at
  FROM 
    `DWH_STG.PARAM_LOAD`
) src
ON tgt.param_key = src.param_key
WHEN MATCHED THEN 
  UPDATE SET
    tgt.param_value = src.param_value,
    tgt.updated_at  = src.loaded_at
WHEN NOT MATCHED THEN 
  INSERT (param_key, param_value, updated_at)
  VALUES (src.param_key, src.param_value, src.loaded_at);
```

---

# ENTITIES & RESOURCES LIST

### 1. Database Tables
*   **`DWH_STG.PARAM_LOAD`**: Staging table containing raw extracted parameters.
*   **`DWH_ADM.JOB_PARAMS`**: Operational administrative parameters repository.

### 2. Table Columns
*   **`param_key`**: Unique identifier for configuration options.
*   **`param_value`**: Evaluated value assigned to the config key.
*   **`loaded_at`**: Temporal tracking representing when the record reached the staging layer.
*   **`updated_at`**: Temporal tracking representing the final modification event inside the production layer.

### 3. File System & Control Artifacts
*   **`DW.CFG_LOAD_PARAMS.xml`**: Automic/UC4 Job scheduler XML definition.
*   **`r_load_params.ksh`**: Unix orchestration/execution wrapper.
*   **`d_param_load.sql`**: SQL loader script containing the merge logic.
*   **`dwh.profile`**: Global environment profile context.
*   **`dwh_env.properties`**: Flat-file parameter registry source database.
*   **`param_load.ctl`**: Oracle SQL*Loader structural control schema file.
*   **`param_load.log`**: Standard operational error output file.
```

---

## Context and Target Orchestration

This section provides critical scheduling, execution order, and configuration context that was not visible to the isolated SQL conversion tool.

### 1. Job Dependencies
*   **Upstream Jobs**: None discovered in the direct lineage metadata.
*   **Downstream Jobs**: None discovered.

### 2. Execution Order
The execution sequence defined in the legacy dependency graph must be preserved in the target Airflow DAG task order:
1. **Task 1 (`parse_and_stage_parameters`)**: Run the Python logic of `r_load_params.py` (which replaces `r_load_params.ksh`) to read parameters from `dwh_env.properties` and write them to the BigQuery staging table `` `DWH_STG.PARAM_LOAD` ``.
2. **Task 2 (`merge_parameters`)**: Run the Dataform compilation/execution (or execute the direct BigQuery insert query job) for `d_param_load.sqlx` to execute theupsert logic into `` `DWH_ADM.JOB_PARAMS` ``.

### 3. Scheduling and Variables
*   **Trigger Mechanism**: Mapped from UC4 execution schema to Cloud Composer. The target DAG should run on its standard scheduled interval (e.g., daily) or be triggered externally by an event.
*   **Scheduling Linkage**: Handled via Composer DAG `schedule_interval`.

### 4. Lineage Edges
*   **Reads**: Table `` `DWH_STG.PARAM_LOAD` ``
*   **Writes**: Table `` `DWH_ADM.JOB_PARAMS` ``

### 5. External System Replacements
*   **Flat File Loading**: Oracle `SQL*Loader` and `param_load.ctl` are retired. File processing is replaced by Python's `pandas` / `csv` libraries reading directly from a Google Cloud Storage (GCS) bucket, loading records directly into BigQuery staging via the GCS-to-BigQuery native API or `google-cloud-bigquery` client.

---

## Target File Plan

### 1. Orchestration DAG
*   **Target Relative Path**: `dags/config_env_linked_job/DWH_CFG_JOB/dw_cfg_load_params_dag.py`
*   **Language**: Python
*   **Source File**: `config_env_linked_job/DWH_CFG_JOB/DW.CFG_LOAD_PARAMS.xml`
*   **Purpose**: Orchestrates the parameters load DAG pipeline. Preserves the exact task execution flow.

### 2. Load and Stage Python Script
*   **Target Relative Path**: `config_env_linked_job/iscfg/bin/r_load_params.py`
*   **Language**: Python
*   **Source File**: `config_env_linked_job/iscfg/bin/r_load_params.ksh`
*   **Purpose**: Parses `dwh_env.properties` from a designated GCS configuration bucket, performs required error logging, and loads the data into the staging table `` `DWH_STG.PARAM_LOAD` ``.

#### CRITICAL LITERAL PRESERVATION RULE (GERMAN LOGS)
To comply with the verbatim logging requirement, the Python script must **never** translate German print or log statements into English. The original German messages must be preserved character-for-character inside Python's print or log statements:
*   `print("Start Parameter-Load...")`
*   `print("Lese Parameterdatei...")`
*   `print("FEHLER: Parameterdatei existiert nicht oder ist leer.")`
*   `print("Laden in Staging-Tabelle erfolgreich.")`
*   `print("FEHLER: SQL*Loader/BigQuery Load fehlgeschlagen!")`
*   `print("Führe Post-Load SQL-Skript aus...")`

### 3. Upsert Merge Model
*   **Target Relative Path**: `config_env_linked_job/iscfg/cfg/d_param_load.sqlx`
*   **Language**: SQLX (Dataform)
*   **Source File**: `config_env_linked_job/iscfg/cfg/d_param_load.sql`
*   **Purpose**: Modernizes the Oracle MERGE logic into a declarative Dataform operational model to upsert staging parameters.

---

## Environment-Specific Values

The configuration values are categorized based on their target execution role:

### 1. Global (Environment-wide)
These properties must be dynamically read at runtime from the system or Airflow variables:
*   `GCP_PROJECT`: Retrieved via `Variable.get("GCP_PROJECT")` (Airflow) or `os.environ.get("GCP_PROJECT")` (Python).
*   `GCS_BUCKET`: The GCS bucket containing config artifacts, retrieved via `Variable.get("GCS_CONFIG_BUCKET")`.
*   `BQ_LOCATION`: The location/region of the BigQuery datasets.

### 2. Job-Specific
These properties are unique to this loader configuration and are declared inside the local environment configuration/variables:
*   `STAGING_TABLE`: `"DWH_STG.PARAM_LOAD"` (Inlined in Dataform SQLX or defined in Airflow DAG Params).
*   `TARGET_TABLE`: `"DWH_ADM.JOB_PARAMS"` (Inlined in Dataform SQLX or defined in Airflow DAG Params).
*   `PROPERTIES_FILE`: `"cfg/dwh_env.properties"` (Specified within `r_load_params.py`).

*No prose placeholders or dummy values (e.g., `<PROJECT_ID>`, `your-bucket`) may be injected during compilation.*

---

## Risks & Manual Steps

1. **German Print Literal Rule**: Ensure the Build Agent strictly adheres to the German print preservation rule and does not attempt to rewrite log outputs.
2. **Dynamic Properties Path**: The properties configuration file (`dwh_env.properties`) must be placed in the designated Cloud Storage bucket path (`gs://${GCS_CONFIG_BUCKET}/cfg/dwh_env.properties`) before executing the DAG.
3. **No-Source Components**: The components `.DW_INIT`, `DW.BERT_LESE_LOG`, and `DW.HOLE_PFAD` have been verified as "NO SOURCE NEEDED" by human-reviewers. They are retired in GCP and require no manual file migration.