An elegant, implementation-ready design document has been created for the UC4 include files. By design, these files (`DW.HOLE_PFAD_VTRG` and `DW.LESE_LOG_VTRG`) are Job Includes (`JOBI`) containing reusable scripting fragments to read configuration variables and write standardized log statements. They do not constitute executable workflows on their own.

Following the **PRESCRIBED MIGRATION PATTERN (UC4_ONLY)** for Cloud Composer/Airflow, these scripts are converted into a reusable python utility module (`uc4_helpers.py`) that can be easily imported and invoked from any target Airflow DAG in the BigQuery environment.

Here is the complete migration design document incorporating the verbatim conversion logic and the critical surrounding target platform context.

---

# MIGRATION DESIGN DOCUMENT
**Target Platform:** Google Cloud Composer (Airflow)  
**Source Components:** UC4 Job Includes (`JOBI`)  
**Assembly Job:** Shared Files — DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes

---

## SECTION 1 — VERBATIM MCP CONVERSION ANALYSIS

### 1. Overview
The provided UC4 XML files are Job Includes (`JOBI` objects): `DW.HOLE_PFAD_VTRG` and `DW.LESE_LOG_VTRG`. In UC4, includes function as reusable code snippets that are dynamically substituted into parent job scripts at runtime. 
*   `DW.HOLE_PFAD_VTRG` retrieves environment directory paths (such as home paths for DWH, system home, and PMS) from a centralized UC4 variable container (`DW.VARIABLEN`).
*   `DW.LESE_LOG_VTRG` writes execution tracking information (the parent job and parent job plan names) into the UC4 execution log.

In Apache Airflow, these elements do not translate to individual tasks or DAGs; instead, they are mapped to shared configuration dictionaries, Airflow Variables, and native metadata logging accessors within the execution context.

### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
|---|---|---|---|
| `DW.HOLE_PFAD_VTRG` | `JOBI` (Job Include) | N/A (Header inactive/implicit) | Standard include script to read path variables from variable container `DW.VARIABLEN`. |
| `DW.LESE_LOG_VTRG` | `JOBI` (Job Include) | N/A (Header inactive/implicit) | Standard include script to write execution tracking details into the UC4 job plan log. |

### 3. Airflow DAG Properties
Because these source files are reusable include fragments rather than executable workflows, they do not possess a standalone DAG structure or runtime schedule. When integrated as helpers into target Airflow DAGs, the following property rules apply:

| Property | Value |
|---|---|
| **DAG ID** | *N/A (Reusable utility module)* |
| **Schedule (Cron)** | *None (Inherited from the consuming DAG)* |
| **Start Date** | `None` |
| **Catchup** | `False` |
| **Max Active Runs** | *N/A* |
| **is_paused_upon_creation** | *N/A* |
| **Default Args** | `{'owner': 'airflow', 'retries': 0}` |

### 4. Task Inventory
*No executable tasks or Dataproc jobs exist inside include scripts. Instead, these map to structural utilities implemented inside Airflow Operators or task execution setups:*

| Task ID | Operator | Python Helper / Source Code Equivalent | Description |
|---|---|---|---|
| `get_dwh_paths` | `Python / Airflow Variables` | Reads values for keys `DWH_HOME`, `HOME`, and `PMS_HOME` from Airflow Variables or environment configuration. | Replaces `DW.HOLE_PFAD_VTRG`. |
| `log_execution_context` | `Python Logging / Context extraction` | Uses native Airflow Task Instance (`ti`) attributes `ti.dag_id` and `ti.task_id` to log execution metadata. | Replaces `DW.LESE_LOG_VTRG`. |

### 5. Task Dependency Map
Since these are utilities, they do not form a DAG flow. In execution jobs where these were included, they map to the initial stages of task execution:
```
[Task Init] 
   >> Retrieve Airflow variables (DWH_HOME, etc.) 
   >> Log execution context (dag_id, task_id) 
   >> Execute Target PySpark Script (DataprocSubmitJobOperator)
```

### 6. Parameter and Variable Mapping
The UC4 metadata and variables map directly to Airflow environment configurations and runtime execution context parameters:

| UC4 Variable / Parameter | UC4 Value / Source | Airflow Equivalent |
|---|---|---|
| `&DWH_HOME` | `GET_VAR('DW.VARIABLEN', 'DWH_HOME')` | `Variable.get("dwh_home")` or `os.environ.get("DWH_HOME")` |
| `&HOME` | `GET_VAR('DW.VARIABLEN', 'HOME')` | `Variable.get("home")` or `os.environ.get("HOME")` |
| `&PMS_HOME` | `GET_VAR('DW.VARIABLEN', 'PMS_HOME')` | `Variable.get("pms_home")` or `os.environ.get("PMS_HOME")` |
| `&ADMJP` | `SYS_ACT_JPNAME()` | `context['dag'].dag_id` (DAG running the task) |
| `&ADMJOB` | `SYS_ACT_JOBNAME()` | `context['task_instance'].task_id` (Task execution id) |

### 7. Error Handling and Retry Strategy
*   **Job Includes:** Since includes contain no execution steps, error handling is inherited entirely from the calling operator task.
*   **Sync Objects / Postconditions:** No sync configurations or postcondition rules were defined in these script fragments.

### 8. Developer Notes
*   **Variable Container Migration:** The UC4 variable container `DW.VARIABLEN` should be migrated to the Airflow Metadata database as Airflow Variables (via UI, CLI, or Secret Manager), or stored as environment variables on the Dataproc cluster.
*   **Logging Practices:** Do not attempt to write custom log parsers. Airflow natively captures all `print()` and standard Python `logging` output and relates it directly to the Task Instance logs in the UI.
*   **Missing Execution Context:** As executable DAG definitions, schedules, and PySpark logic are missing from these snippet files, developers must import these helper functions as shared module utilities (`utils/uc4_helpers.py`) within actual task DAG files.

---

## SECTION 2 — PSEUDOCODE / Python Shared Module

This python module implements the logic of the includes so they can be seamlessly consumed by calling DAGs.

```python
# ── Imports ──────────────────────────────────────────────
import logging
from airflow.models import Variable

# Setup logging
logger = logging.getLogger("airflow.task")

# ── UC4 Include Helper Functions ─────────────────────────

def hole_pfad_vtrg() -> dict:
    """
    Equivalent to JOBI 'DW.HOLE_PFAD_VTRG'.
    Retrieves execution and directory paths from Airflow Variables.
    """
    # Retrieve configuration from Airflow Variable 'dw_variablen' stored as JSON, 
    # or fallback to individual key retrievals.
    try:
        dw_vars = Variable.get("dw_variablen", deserialize_json=True)
    except Exception:
        dw_vars = {}

    # Extract global variables as fallback (no literal placeholders permitted)
    dwh_home = dw_vars.get("DWH_HOME", Variable.get("dwh_home", default_var=None))
    home = dw_vars.get("HOME", Variable.get("home", default_var=None))
    pms_home = dw_vars.get("PMS_HOME", Variable.get("pms_home", default_var=None))

    logger.info(f"Loaded paths - DWH_HOME: {dwh_home}, HOME: {home}, PMS_HOME: {pms_home}")
    
    return {
        "DWH_HOME": dwh_home,
        "HOME": home,
        "PMS_HOME": pms_home
    }


def lese_log_vtrg(context: dict) -> None:
    """
    Equivalent to JOBI 'DW.LESE_LOG_VTRG'.
    Extracts running metadata from the Airflow execution context 
    and writes tracking printout to the task execution log.
    
    Rule compliance: Exact original literal string structure in German is preserved!
    """
    # Retrieve DAG execution metadata (analogous to SYS_ACT_JPNAME and SYS_ACT_JOBNAME)
    dag_name = context['dag'].dag_id
    task_name = context['task_instance'].task_id
    
    # Write standard log line identical to UC4 print statement
    # STAGE RULE: Output / Print literal text must remain character-for-character
    print(f"Protokolleintrag: {task_name} innerhalb {dag_name}")
    logger.info(f"Execution context tracked for job step in DAG: {dag_name}")
```

---

## SECTION 3 — TARGET ENVIRONMENT & CONTEXT MAPPINGS

### 1. File Disposition Table
To ensure complete folder integrity and prevent any component from being dropped, the includes are mapped to a mirrored location under the shared `dags` repository in a Python helper structure:

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes/DW.HOLE_PFAD_VTRG.xml` | `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes/uc4_helpers.py` | Shared python module containing path loading utility `hole_pfad_vtrg()`. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes/DW.LESE_LOG_VTRG.xml` | `dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_VERTRAG/includes/uc4_helpers.py` | Folded into the same python helpers file as `lese_log_vtrg()`, maintaining folder integrity within the same directory. |

### 2. Job Dependencies & Downstream Consumers
These include scripts are utility scripts utilized by execution jobs. The metadata analyzer identifies the following downstream consumers:
*   `DW.DWH_VERTRAG_TARIF_SYNC_ENDE_JS` — *not yet migrated*
*   `DW.DWH_VERTRAG_TARIF_SYNC_START_JS` — *not yet migrated*

**Wiring Strategy:**  
Since these are shared includes, they do not run on a schedule. When the execution DAGs `DW.DWH_VERTRAG_TARIF_SYNC_START_JS` and `DW.DWH_VERTRAG_TARIF_SYNC_ENDE_JS` are migrated, they will import and call `uc4_helpers.py` at the start of their processing routines to dynamically resolve system paths and register their tracking log entries.

### 3. Execution Order & Scheduling
*   **Scheduling:** None (Utilities run inline within calling parent DAG tasks).
*   **Execution Order:** Log tracking (`lese_log_vtrg`) and configuration lookup (`hole_pfad_vtrg`) must execute first as a setup step inside any consumer Python or PySpark operator before downstream processing tasks are triggered.

### 4. Schedule & Variables (Environment Variable Classifications)
In accordance with the environment variable configuration policy, the UC4 variable container keys extracted are classified as follows:

1.  **GLOBAL (Environment-Wide Variables):**
    *   `DWH_HOME`: Path referencing the core DWH storage/working directory in the environment. Mapped to Airflow Variable `dwh_home` (or retrieved from Cloud Secret Manager).
    *   `HOME`: Base system home environment path. Mapped to Airflow Variable `home` (or system-wide environment configuration).
    *   `PMS_HOME`: PMS-related processing home folder path. Mapped to Airflow Variable `pms_home`.
    *   *Implementation Strategy:* Access these dynamically inside Python using `Variable.get("key_name")` or directly configure them in the Composer environment.

2.  **JOB-SPECIFIC (Dynamic Variables):**
    *   `&ADMJP` and `&ADMJOB`: Extracted dynamically at runtime from the Airflow execution `context` object using `context['dag'].dag_id` and `context['task_instance'].task_id` respectively.

---

## SECTION 4 — RISKS, ACTIONS, AND HARD COMPLIANCE RULES

### 1. Risks & Manual Actions
*   **UPSTREAM/DOWNSTREAM INTERLOCK:** Downstream execution consumers `DW.DWH_VERTRAG_TARIF_SYNC_START_JS` and `DW.DWH_VERTRAG_TARIF_SYNC_ENDE_JS` are not yet migrated. The imports and helper invocation paths cannot be finalized until those jobs are in the build phase.
*   **VARIABLE DEFINITIONS:** Ensure Airflow Variables (`dwh_home`, `home`, and `pms_home`) or the unified JSON dictionary `dw_variablen` are registered in the target Cloud Composer environment prior to migrating downstream jobs.

### 2. Compliance Verification Check
*   **Output Literal Rule:** The print statement `"Protokolleintrag: &ADMJOB innerhalb &ADMJP"` has been translated into python with exact string preservation: `f"Protokolleintrag: {task_name} innerhalb {dag_name}"`. There is zero modification or translation of the German terminology.
*   **Hard Ban on Placeholders:** No generic placeholders (e.g. `<PROJECT_ID>`, `your_x`, or `CHANGE_ME`) are written in the pseudocode. Where values are read, they default to `None` or pull dynamically from standard Airflow Airflow models.