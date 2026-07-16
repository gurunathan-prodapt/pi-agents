An analysis of the source UC4 Include objects (`DW.HOLE_PFAD_KNZB` and `DW.LESE_LOG_KNZB`) and the conversion mappings has been completed. Below is the comprehensive, implementation-ready Migration Design Document.

---

# MIGRATION DESIGN DOCUMENT

### File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW.HOLE_PFAD_KNZB.xml` | `dags/utils/dw_hole_pfad_knzb.py` | Migrated as a central configuration / Python utility module. Resolves paths via Airflow Variables. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_STAMM/includes/DW.LESE_LOG_KNZB.xml` | `dags/utils/dw_lese_log_knzb.py` | Migrated as a dynamic Python context logging helper module. Prints Task/DAG names into the Airflow task log. |

---

# SECTION 1 — DESIGN DOCUMENT

### 1. Overview
The files in this assembled job are UC4 Include (`JOBI`) objects. They are not executable standalone workflows or schedule-triggers; instead, they act as reusable scripts embedded in parent jobs:
- **`DW.HOLE_PFAD_KNZB`**: Standard include to dynamically fetch base paths (`DWH_HOME`, `HOME`, `ISTNS_HOME`) from a central variable container (`DW.VARIABLEN`).
- **`DW.LESE_LOG_KNZB`**: Standard logging helper that retrieves active runtime metadata—parent workflow name (`&ADMJP`) and active job name (`&ADMJOB`)—and prints a standardized protocol/log statement.

In the target BigQuery / Cloud Composer architecture, these reusable patterns are migrated to central Python utilities or Jinja variables:
- Paths are managed as environment configuration (Airflow Variables).
- Logging is handled via standard Python/Airflow Logging, using task instances (`ti`) and DAG metadata.

### 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Description |
| :--- | :--- | :--- | :--- |
| `DW.HOLE_PFAD_KNZB` | `JOBI` (Include Script) | N/A (Inherits) | Standard include to resolve paths from central container `DW.VARIABLEN`. |
| `DW.LESE_LOG_KNZB` | `JOBI` (Include Script) | N/A (Inherits) | Standard logging include to output execution parent and child metadata. |

### 3. Airflow DAG Properties
Since these are helpers rather than scheduled parent workflows, they do not own scheduling properties. The parent orchestrator DAGs must define execution schedules.
- **Folder Integrity:** The target utility files are placed in a mirrored helper/utility structure under `dags/utils/` to maintain modularity.

### 4. Task Inventory
These include scripts do not translate to independent tasks but are integrated directly as:
1. **Config parameters** passed to Google Cloud Composer / Dataproc execution tasks.
2. **Standardized helper calls** at the beginning of tasks.

### 5. Task Dependency Map
```
[Airflow Variables Config Store]
       │
       ▼
 [dags/utils/dw_hole_pfad_knzb.py] (Loads Env Paths)
       │
       ▼
[Parent Target DAG execution task] 
       │
       ▼
 [dags/utils/dw_lese_log_knzb.py] (Outputs runtime protocol: DAG ID and Task ID)
```

### 6. Parameter and Variable Mapping
The UC4 variables are mapped directly to runtime environments and Airflow variables:

| UC4 Parameter | Value / Source | Airflow Equivalent | Environment Type |
| :--- | :--- | :--- | :--- |
| `&DWH_HOME` | `GET_VAR('DW.VARIABLEN','DWH_HOME')` | `Variable.get("dw_variablen_dwh_home")` | GLOBAL |
| `&HOME` | `GET_VAR('DW.VARIABLEN','HOME')` | `Variable.get("dw_variablen_home")` | GLOBAL |
| `&ISTNS_HOME` | `GET_VAR('DW.VARIABLEN','ISTNS_HOME')` | `Variable.get("dw_variablen_istns_home")` | GLOBAL |
| `&ADMJP` | `SYS_ACT_JPNAME()` | `context['dag'].dag_id` | JOB-SPECIFIC |
| `&ADMJOB` | `SYS_ACT_JOBNAME()` | `context['task'].task_id` | JOB-SPECIFIC |

*Note on Global Variables:* If these environment-wide constants map to Cloud Storage buckets or global mount directories, they must be configured as Airflow Variables in Cloud Composer during environment setup:
- `dw_variablen_dwh_home`
- `dw_variablen_home`
- `dw_variablen_istns_home`

### 7. Error Handling and Retry Strategy
- **Path Resolution:** If an Airflow Variable is missing, the template resolution will fail at task compilation, preventing execution on an invalid path.
- **Log Collection:** Context logging helper exceptions are caught using standard `try-except` blocks to prevent logging infrastructure failures from interrupting core data pipeline execution.

### 8. Job Dependencies & Lineage
* **Downstream Consumers (Cross-Job Hand-offs):**
  - `DW.DWH_STAMM_KNZB_ABGL_ENDE_JS` (Not yet migrated)
  - `DW.DWH_STAMM_KNZB_ABGL_START_JS` (Not yet migrated)
* **Wiring on BigQuery/Composer:** Once the downstream parent jobs are migrated, they will import these shared scripts (`dags/utils/dw_hole_pfad_knzb.py` and `dags/utils/dw_lese_log_knzb.py`) or reference their logic within their Airflow operators to retrieve execution variables and log progress.

---

# SECTION 2 — PSEUDOCODE

### Target File 1: `dags/utils/dw_hole_pfad_knzb.py`
This module encapsulates the logic of `DW.HOLE_PFAD_KNZB.xml` and exports a utility to load path configuration variables.

```python
# -*- coding: utf-8 -*-
"""
Generated equivalent for JOBI DW.HOLE_PFAD_KNZB
Provides centralized environment path variable resolution using Airflow Variables.
"""

from airflow.models import Variable

def get_knzb_paths() -> dict:
    """
    Simulates retrieval of environment variables from central DW.VARIABLEN container.
    """
    # Environment-wide paths classified as GLOBAL variables.
    # No fallback prose placeholders are provided to guarantee failure if unconfigured.
    dwh_home = Variable.get("dw_variablen_dwh_home")
    home_dir = Variable.get("dw_variablen_home")
    istns_home = Variable.get("dw_variablen_istns_home")
    
    return {
        "DWH_HOME": dwh_home,
        "HOME": home_dir,
        "ISTNS_HOME": istns_home
    }
```

### Target File 2: `dags/utils/dw_lese_log_knzb.py`
This module encapsulates the logging logic of `DW.LESE_LOG_KNZB.xml` using native Airflow task context.

```python
# -*- coding: utf-8 -*-
"""
Generated equivalent for JOBI DW.LESE_LOG_KNZB
Emulates UC4 metadata context logging inside an Apache Airflow execution environment.
"""

import logging

# Ensure logs are routed to Airflow task execution log
logger = logging.getLogger("airflow.task")

def log_uc4_context_helper(**context) -> None:
    """
    Extracts runtime context parameters representing UC4 SYS_ACT variables.
    
    Equivalent UC4 Logic:
    :SET &ADMJP  = SYS_ACT_JPNAME()
    :SET &ADMJOB = SYS_ACT_JOBNAME()
    :PRINT "Protokolleintrag: &ADMJOB innerhalb &ADMJP"
    """
    try:
        # Resolve runtime job variables
        parent_plan_name = context['dag'].dag_id
        active_job_name = context['task'].task_id
        
        # Output rule: Character-for-character reproduction of the print statement text in German.
        logger.info(f"Protokolleintrag: {active_job_name} innerhalb {parent_plan_name}")
        
    except Exception as err:
        # Non-blocking infrastructure logging fallback
        logger.warning(f"Failed to log runtime context: {str(err)}")
```

### Example Usage: Reference DAG Template
This template shows how a migrated parent DAG imports and runs these shared utility structures.

```python
# -*- coding: utf-8 -*-
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from dags.utils.dw_hole_pfad_knzb import get_knzb_paths
from dags.utils.dw_lese_log_knzb import log_uc4_context_helper

default_args = {
    'owner': 'data_engineering',
    'start_date': days_ago(1),
    'catchup': False,
}

with DAG(
    dag_id='dw_dwh_stamm_example_parent_workflow',
    schedule_interval=None,
    default_args=default_args,
    tags=['dwh_kern', 'shared_files']
) as dag:

    # 1. Execute runtime context logging (replaces JOBI: DW.LESE_LOG_KNZB)
    log_context = PythonOperator(
        task_id='log_runtime_context',
        python_callable=log_uc4_context_helper,
        provide_context=True
    )

    # 2. Extract path variables (replaces JOBI: DW.HOLE_PFAD_KNZB)
    def task_execution_logic(**context):
        paths = get_knzb_paths()
        # Paths can now be used to run downstream shell, python, or SQL scripts
        print(f"Executing job inside HOME path: {paths['HOME']}")

    run_business_logic = PythonOperator(
        task_id='run_business_logic',
        python_callable=task_execution_logic,
        provide_context=True
    )

    log_context >> run_business_logic
```

---

# SECTION 3 — RISKS & MANUAL ACTIONS

1. **Unmigrated Downstream Dependencies**: 
   The downstream jobs `DW.DWH_STAMM_KNZB_ABGL_ENDE_JS` and `DW.DWH_STAMM_KNZB_ABGL_START_JS` are not yet migrated. The final integration and validation of these helper modules cannot be closed out until those target DAGs are migrated.
2. **Composer Variable Provisioning**:
   The variables `dw_variablen_dwh_home`, `dw_variablen_home`, and `dw_variablen_istns_home` must be explicitly provisioned in the Cloud Composer variable store before executing any parent DAGs. Otherwise, runtime `KeyError` failures will occur.