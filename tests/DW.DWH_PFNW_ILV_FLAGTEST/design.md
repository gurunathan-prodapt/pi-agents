=== OBJECT: DW.DWH_PFNW_ILV_FLAGTEST (JOBS_UNIX) ===
active=1
title=Ein Prüfskript testet, ob das Fill-Flag für Maximo gesetzt wurde, welches erforderlich ist, um den ILV-Jobplan weiterlaufen zu lassen
login=DW.UNIX.ISTNS
host=|DWHDWH4P|HOST
ert_seconds=15
launcher_type=unrecognized
launcher_details={'raw_command': '# [TRIMMED for the 3-file DE demo: ". $HOME/.dw_init" removed here --'}
script_body:
:inc DW.HOLE_PFAD
:set &DWH_JOB_KENNUNG='PFNW_ILV_FLAGTEST '
# [TRIMMED for the 3-file DE demo: ". $HOME/.dw_init" removed here --
#  framework env bootstrap, not this job's business logic; .dw_init is not
#  one of the 3 files in this demo. See the untrimmed chain under
#  isdwh/pruef/nw/... in ~/data for the real dot-source.]

$HOME/aktuell/pruef/nw/bin/k_pfnw_ilv_flagtest.ksh -q $HOME/aktuell/pruef/nw/sql/d_pfnw_ilv_flagtest.sql -j PFNW_ILV_FLAGTEST


:inc DW.LESE_LOG
operational_notes=kann ohne weitere Arbeiten erneut ausgeführt werden etc.

=== UNRESOLVED REFERENCES (object named but not supplied in this bundle) ===
  (none — every referenced object was supplied in this bundle)


# UC4 Migration Design Document: DW.DWH_PFNW_ILV_FLAGTEST

## 1. Overview
This migration design covers a single UC4 UNIX job object (`DW.DWH_PFNW_ILV_FLAGTEST`) that acts as a validation or gatekeeping step in the Maximo/DWH data loading workflow. Its primary function is to execute a shell script that checks if a specific "Fill-Flag" for Maximo has been set. This flag check is a mandatory prerequisite for the downstream Inter-Company Activity (ILV) job plan to proceed. Because no parent workflow (JOBP) or calendar schedule (EVNT_TIME) was supplied in this extraction, this process is modeled as an externally triggered, single-task DAG that must run prior to ILV processing.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_PFNW_ILV_FLAGTEST` | JOBS_UNIX | Active (`active=1`) | Ein Prüfskript testet, ob das Fill-Flag für Maximo gesetzt wurde, welches erforderlich ist, um den ILV-Jobplan weiterlaufen zu lassen |

## 3. Scheduling
- **Trigger Source**: This object does not contain an internal schedule or `EVNT_TIME` configuration. No SCRI triggers or parent JOBP definitions are present in this extraction.
- **Classification**: Externally triggered (source unknown from this extraction alone). 
- **Airflow Schedule**: `schedule=None` (no calendar or time-based triggers are defined).

## 4. Airflow DAG Properties
Since no parent `JOBP` wrapper was provided, a synthetic wrapper DAG is established to represent this job workflow.

| Property | Value |
| :--- | :--- |
| **DAG ID** | `dw_dwh_pfnw_ilv_flagtest` |
| **Schedule** | `None` |
| **Start Date** | `datetime(2023, 1, 1)` (Placeholder) |
| **Catchup** | `False` |
| **Max Active Runs** | `1` (Standard execution safety) |
| **Is Paused Upon Creation** | `False` (`active=1` in source) |
| **Default Args** | `{'owner': 'dw', 'retries': 1, 'retry_delay': timedelta(minutes=5)}` |

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dw_dwh_pfnw_ilv_flagtest_task` | `DW.DWH_PFNW_ILV_FLAGTEST` | `EmptyOperator` | N/A | N/A | 1 | 5 mins | None | None | N/A | None | # REVIEW-STRUCT: launcher command unrecognized. Executes `$HOME/aktuell/pruef/nw/bin/k_pfnw_ilv_flagtest.ksh` with SQL query `d_pfnw_ilv_flagtest.sql`. Needs manual conversion. |

## 6. Task Dependency Map
As this is a single-task DAG, the execution chain contains only one node:
```python
dw_dwh_pfnw_ilv_flagtest_task
```

## 7. Sync / Concurrency Analysis
No sync rows, self-locks, or cross-DAG locks were found in this extraction bundle. Standard DAG-level run concurrency safety is applied (`max_active_runs=1`).

## 8. Error Handling and Retry Strategy
- No complex postconditions, block structures, or execution triggers were defined in the source script.
- Standard Airflow execution constraints apply (failure of the task fails the run, prompting manual intervention to resolve the missing Maximo Fill-Flag downstream).

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| Sanitised DAG ID | `DW.DWH_PFNW_ILV_FLAGTEST` | `dw_dwh_pfnw_ilv_flagtest` |
| Login / Run-As | `DW.UNIX.ISTNS` | Handled via Airflow Connection / SSH configuration (if retained as a shell task) |

## 10. Developer Notes
*   **# REVIEW-STRUCT: Unrecognized Launcher Script Execution:** The command body runs a Korn Shell script wrapper (`k_pfnw_ilv_flagtest.ksh`) passing a SQL file (`d_pfnw_ilv_flagtest.sql`) as an argument:
    ```bash
    $HOME/aktuell/pruef/nw/bin/k_pfnw_ilv_flagtest.ksh -q $HOME/aktuell/pruef/nw/sql/d_pfnw_ilv_flagtest.sql -j PFNW_ILV_FLAGTEST
    ```
    This extraction cannot resolve the database connection or execution runtime. Confirm whether this should be:
    1.  Migrated into a `SqlSensor` or `BigQuerySensor` (recommended) to dynamically check for the Maximo Fill-Flag before starting the ILV pipeline.
    2.  An `SSHOperator` calling the legacy `.ksh` script.
    3.  A generic `PythonOperator` performing the database query using a native Airflow hook.
*   **Pipeline Placement:** Since this checks a flag required "to let the ILV jobplan continue running", this task should ideally be placed as an upstream sensor inside the target ILV DAG itself once that DAG's definition is available.

---

# Airflow DAG Pseudocode

```python
"""
DAG: dw_dwh_pfnw_ilv_flagtest
Source UC4 Object: DW.DWH_PFNW_ILV_FLAGTEST (JOBS_UNIX)
Description: Verifies whether the Maximo Fill-Flag has been set before downstream ILV runs.
"""

# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# ── GCP Configuration ────────────────────────────────────
# Placeholder variables for target environment migration
GCP_PROJECT_ID = "your-gcp-project-id"
GCP_REGION = "europe-west3"

# ── Default Args ─────────────────────────────────────────
default_args = {
    "owner": "dw",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id="dw_dwh_pfnw_ilv_flagtest",
    default_args=default_args,
    description="Tests whether the Maximo Fill-Flag is set to allow the ILV jobplan to proceed",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dwh", "maximo", "uc4_migration"],
) as dag:

    # ── Task: dw_dwh_pfnw_ilv_flagtest_task ──────────────
    # # REVIEW-STRUCT: The legacy UC4 task launcher is unrecognized.
    # Legacy Command:
    #   $HOME/aktuell/pruef/nw/bin/k_pfnw_ilv_flagtest.ksh -q $HOME/aktuell/pruef/nw/sql/d_pfnw_ilv_flagtest.sql -j PFNW_ILV_FLAGTEST
    # Target conversion path recommendation:
    #   Convert to an appropriate database sensor/operator checking `d_pfnw_ilv_flagtest.sql`.
    dw_dwh_pfnw_ilv_flagtest_task = EmptyOperator(
        task_id="dw_dwh_pfnw_ilv_flagtest_task",
    )

    # ── Dependencies ─────────────────────────────────────────
    # Single-task pipeline. No explicit dependencies needed.
    dw_dwh_pfnw_ilv_flagtest_task
```

### Execution order
Step-by-step mapping of the 3 execution steps from the legacy dependency graph:
- **Step 1: `DW.DWH_PFNW_ILV_FLAGTEST.xml`** maps to the target orchestration DAG file `dags/dw_dwh_pfnw_ilv_flagtest.py`.
- **Step 2: `d_pfnw_ilv_flagtest.sql`** is the query that checks the flag. Its target SQL schema/script and migration are handled in a separate design pass.
- **Step 3: `k_pfnw_ilv_flagtest.ksh`** is the execution script. Its target Python operator or script translation is handled in a separate design pass.

### Schedule & variables
- **Schedule**: The legacy job is active, but no schedule was defined in this object. It is designed as an externally triggered DAG (`schedule=None`).
- **Variables**:
  - `DWH_JOB_KENNUNG` (Value: `'PFNW_ILV_FLAGTEST '`): This scheduler-set variable must be preserved and passed into the target task/script as a parameter or environment variable.

### Lineage
Based on the pre-collected lineage edges:
- **Upstream Inclusions**:
  - `DW.HOLE_PFAD` (Included in the legacy job; human-confirmed as needing no target source code).
  - `DW.LESE_LOG` (Included in the legacy job; human-confirmed as needing no target source code).
- **Invocations**:
  - Invokes `FILE:k_pfnw_ilv_flagtest.ksh` (migrated in a separate design pass).
  - Invokes `FILE:d_pfnw_ilv_flagtest.sql` (migrated in a separate design pass).
- **Execution Target**:
  - Runs on Host: `dwhdwh4p` using package/login `DW.UNIX.ISTNS`.

### Cross-file dependencies
- The orchestration DAG `dags/dw_dwh_pfnw_ilv_flagtest.py` depends on:
  - The migrated Python logic of `k_pfnw_ilv_flagtest.ksh` to run the validation check.
  - The migrated SQL statement of `d_pfnw_ilv_flagtest.sql` to execute the query against BigQuery.
  - *Note*: Both files are outside the scope of this design pass and are converted in their own respective passes.

### Target file plan
- **Target File Path**: `dags/dw_dwh_pfnw_ilv_flagtest.py`
  - **Language**: Python (Airflow DAG)
  - **Source File**: `DW.DWH_PFNW_ILV_FLAGTEST.xml`

### Environment-specific values
Classified per target role:
- **GLOBAL**:
  - `GCP_PROJECT`: Sourced via Airflow Variable `Variable.get("GCP_PROJECT")` or environment variable.
  - `GCP_REGION`: Sourced via Airflow Variable `Variable.get("GCP_REGION")` or environment variable.
  - `HOST_CONNECTION` (corresponds to legacy host `dwhdwh4p` and login `DW.UNIX.ISTNS`): Airflow connection or SSH connection identifier to be set up globally.
- **JOB-SPECIFIC**:
  - `DWH_JOB_KENNUNG`: Set to `'PFNW_ILV_FLAGTEST '` inside the DAG context or task params.

### File Disposition Table
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DW.DWH_PFNW_ILV_FLAGTEST.xml` | `dags/dw_dwh_pfnw_ilv_flagtest.py` | Migrates the legacy UC4 job orchestration into an Airflow DAG file. |

### Risks & Manual Actions
- **Risk / Action**: Coordination is required with the independent design/migration passes for `d_pfnw_ilv_flagtest.sql` and `k_pfnw_ilv_flagtest.ksh`. The target Airflow DAG in `dags/dw_dwh_pfnw_ilv_flagtest.py` cannot execute correctly until those target artifacts are created and placed in their respective locations.
- **Risk / Action**: The login credentials/permissions represented by legacy `DW.UNIX.ISTNS` must be mapped to a GCP service account or IAM role with appropriate access to execute queries in BigQuery.

---

=== FILE: local/home/gurunathan_t/single_job_demo/d_pfnw_ilv_flagtest.sql ===
/*-- Falls das job_starten_flag gesetzt ist, 
--liefert die Abfrage einen Wert zurück, ansonsten nichts. 
--*/
select job_starten_flag 
from IS_MAINT_SCHEMA.DWH$TA_K_ILV_ABR_ILV 
where (quellsystem = 'MAXIMO')
AND (ilv_teilschritt = 'FILL')
AND (job_starten_flag = 0)
;



═══════════════════════════════════════════
SECTION 1 — DESIGN DOCUMENT
═══════════════════════════════════════════

Step 1: Understand the Script
1.1 Identify the type of Oracle SQL object being converted:
    - Standalone SELECT query.
1.2 Summarize the business logic and purpose of the script in plain English:
    - This query checks whether a specific job execution flag (`job_starten_flag`) is set to `0` for the source system 'MAXIMO' and sub-step 'FILL' in the control table. If a row exists matching these conditions, the query returns the value `0` (indicating the job should/can start), otherwise it returns no rows.
1.3 List all entities referenced:
    - Table: `IS_MAINT_SCHEMA.DWH$TA_K_ILV_ABR_ILV`
    - Columns:
      - `job_starten_flag` (inferred as Oracle `NUMBER` or `NUMBER(1)`)
      - `quellsystem` (inferred as `VARCHAR2`)
      - `ilv_teilschritt` (inferred as `VARCHAR2`)

Step 2: Oracle-Specific Construct Detection and Resolution

2.1 Data Type Conversions:
    - Oracle `VARCHAR2` (`quellsystem`, `ilv_teilschritt`) → BigQuery `STRING`
    - Oracle `NUMBER` (`job_starten_flag`) → BigQuery `INT64`

2.2 Implicit and Explicit Type Casting:
    - No implicit type casts detected in the source script.

2.3 NULL Handling and Conditional Functions:
    - None used.

2.4 String Functions:
    - None used.

2.5 Date and Timestamp Functions:
    - None used.

2.6 Numeric and Aggregate Functions:
    - None used.

2.7 Analytical and Window Functions:
    - None used.

2.8 Set and Join Operations:
    - None used.

2.9 Row Limiting and Sampling:
    - None used.

2.10 Sequences:
    - None used.

2.11 MERGE Statements:
    - None used.

2.12 INSERT / UPDATE / DELETE:
    - None used.

2.13 DDL Constructs (if present):
    - None used.

2.14 PL/SQL (if present):
    - None used.

2.15 Unresolvable or Advisory Items:
    - Special Character in Table Name: The Oracle table name contains a dollar sign (`DWH$TA_K_ILV_ABR_ILV`). BigQuery table identifiers cannot contain `$` characters. This must be resolved by replacing the dollar sign with an underscore (`_`), resulting in `dwh_ta_k_ilv_abr_ilv`.

2.16 MIGRATION DECISION MATRIX

| Oracle SQL Construct / Statement | Selected Target | Rejected Alternatives | Evidence / Reason |
| :--- | :--- | :--- | :--- |
| Standalone `SELECT` query | Direct BigQuery Standard SQL | 1. BigQuery SQL UDF<br>2. Python Wrapper | The statement is a simple, declarative read-only SQL query with standard relational filtering. It does not contain any complex procedural or external orchestration logic that would justify a UDF or Python wrapper. |
| Table Name with `$` Symbol (`DWH$TA_K_ILV_ABR_ILV`) | Table Name Mapping with replacement (`dwh_ta_k_ilv_abr_ilv`) | Retaining `$` in name | BigQuery does not allow standard table identifiers to contain the `$` symbol (except for partition decorators). A direct mapping to an underscore is the standard resolution. |

2.17 REQUIRED ARTIFACTS

| Generated Artifact | Type | Description / Invocation Contract |
| :--- | :--- | :--- |
| BigQuery SQL Query | BigQuery Standard SQL | A standalone `.sql` file executing the rewritten standard SELECT query targeting the migrated BigQuery table. |

2.18 DATA TYPE COMPATIBILITY TABLE

| Oracle Column | Oracle Type | BigQuery Type | Conversion Rule | Warnings / Implications |
| :--- | :--- | :--- | :--- | :--- |
| `quellsystem` | `VARCHAR2` | `STRING` | Direct mapping to variable-length character string. | None. |
| `ilv_teilschritt` | `VARCHAR2` | `STRING` | Direct mapping to variable-length character string. | None. |
| `job_starten_flag` | `NUMBER` | `INT64` | Convert numeric flag to 64-bit integer. | Verify if the source system uses decimal values (unlikely for a job flag). |

2.19 DESIGN REVIEW SUMMARY
- **Patterns/Objects Found**: Simple filtered query on a single table.
- **Unsupported Functions**: None.
- **UDF Required**: No.
- **Python Required**: No.
- **Direct Dependencies**: Table `IS_MAINT_SCHEMA.DWH$TA_K_ILV_ABR_ILV`.
- **Assumptions**: 
  - The source table `DWH$TA_K_ILV_ABR_ILV` will be migrated to the target dataset as `dwh_ta_k_ilv_abr_ilv`.
  - The schema name `IS_MAINT_SCHEMA` is mapped to a BigQuery dataset named `is_maint_schema`.
- **Warnings**: Ensure the ETL/ELT pipeline maps the table name containing `$` correctly to standard alphanumeric characters.
- **Manual-Intervention Items**: None, beyond confirming the target table name in the schema.

OVERALL MIGRATION STRATEGY: Direct BigQuery SQL

2.20 PACKAGE ANALYSIS
Not applicable; no PL/SQL PACKAGE or PACKAGE BODY construct was detected in the supplied source.

2.21 ORACLE FUNCTION ANALYSIS TABLE

| Oracle Function/Construct | Supported in BigQuery | BigQuery Equivalent / Alternative |
| :--- | :--- | :--- |
| `SELECT` / `WHERE` / `AND` | Direct | Standard SQL `SELECT` / `WHERE` / `AND` operators are fully compatible. |
| `$` in Table Identifier | Direct-with-rewrite | Map `DWH$TA_K_ILV_ABR_ILV` to `dwh_ta_k_ilv_abr_ilv`. |

Step 3: Conversion Strategy Summary
3.1 State the overall conversion approach:
    - Direct SQL translation of the SELECT query with standard projection and filter conditions.
3.2 List any assumptions made during conversion:
    - Schema mapping: `IS_MAINT_SCHEMA` → `is_maint_schema`
    - Table mapping: `DWH$TA_K_ILV_ABR_ILV` → `dwh_ta_k_ilv_abr_ilv`
3.3 List any items flagged for human review:
    - Validation of target table name identifier in BigQuery.

═══════════════════════════════════════════
SECTION 2 — PSEUDOCODE
═══════════════════════════════════════════

Step 4: Write Vendor-Neutral Pseudocode

```sql
/*-- If the job_starten_flag is set,
-- the query returns a value, otherwise nothing.
--*/
SELECT 
  job_starten_flag 
FROM 
  is_maint_schema.dwh_ta_k_ilv_abr_ilv  -- Resolved '$' character to '_' for BigQuery compatibility
WHERE 
  (quellsystem = 'MAXIMO')
  AND (ilv_teilschritt = 'FILL')
  AND (job_starten_flag = 0)
;
```

FLAGGED ITEMS FOR HUMAN REVIEW:
1. **Table Name Sanitization**: Confirm that the table `DWH$TA_K_ILV_ABR_ILV` was successfully created as `dwh_ta_k_ilv_abr_ilv` inside the BigQuery dataset `is_maint_schema` during the schema migration phase.

### Execution order
The target orchestration (e.g., Cloud Composer / Airflow DAG) must preserve the execution sequence:
1. `DW.DWH_PFNW_ILV_FLAGTEST.xml` -> Maps to the overall DAG orchestration definition (designed in its own dedicated design pass).
2. `d_pfnw_ilv_flagtest.sql` -> Maps to the BigQuery executing task / assertion check task (this design target).
3. `k_pfnw_ilv_flagtest.ksh` -> Maps to the post-check processing task (designed in its own dedicated design pass).

### Schedule & variables
- **Schedule**: The job is triggered in the source as part of the broader orchestration chain. Under Cloud Composer, this mapping will be preserved as a task transition / dependency within the master DAG.
- **Scheduler-Set Variables**:
  - `DWH_JOB_KENNUNG` = `'PFNW_ILV_FLAGTEST '` must be supplied to the execution environment at runtime via Airflow `params`, DAG configuration, or environment parameters.

### Lineage
- **Upstream Data Source**: TABLE:`DWH$TA_K_ILV_ABR_ILV` (migrated to `is_maint_schema.dwh_ta_k_ilv_abr_ilv` in BigQuery).

### Cross-file dependencies
- **Call Chain**: `k_pfnw_ilv_flagtest.ksh` (KSH wrapper script, out of scope for this pass) calls `d_pfnw_ilv_flagtest.sql` to check the flag status. If the query returns a row, the downstream flow is executed or allowed to start.

### Target file plan
- **Target File Path**: `d_pfnw_ilv_flagtest.sql`
- **Language**: BigQuery Standard SQL
- **Source File**: `d_pfnw_ilv_flagtest.sql`
- **Purpose**: Standard SQL assertion statement to fetch the step-starten flag for the MAXIMO FILL phase from the control table.

### Environment-specific values
- `IS_MAINT_SCHEMA`: **GLOBAL**
  - *Target Role*: The shared administrative/maintenance dataset where control tables reside.
  - *Sourcing Method*: Normalized to `BQ_DATASET` or schema name. Replaced in orchestration templating (e.g., via Airflow Jinja parameters like `{{ params.is_maint_schema }}`) or resolved during build/deployment, rather than inlining a hardcoded dataset name.
- `DWH_JOB_KENNUNG`: **JOB-SPECIFIC**
  - *Target Role*: Job-specific identifier used during execution.
  - *Sourcing Method*: Provided as an Airflow run param or local configuration variable.

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `d_pfnw_ilv_flagtest.sql` | `d_pfnw_ilv_flagtest.sql` | Translate Oracle syntax and table identifiers to BigQuery Standard SQL to perform the sentinel flag validation query. |

---

=== CONFIRMED TARGET PLATFORM ===
TARGET_PLATFORM: BIGQUERY
This is stated directly by the caller, not inferred from the extraction below. Treat it as ground truth everywhere the design/build instructions would otherwise infer the platform or fall back to a default -- it satisfies the "unless explicitly stated in the extraction" condition those rules already reference. Never emit "# REVIEW: target database platform not specified" while this section is present.

=== FILE: local/home/gurunathan_t/single_job_demo/k_pfnw_ilv_flagtest.ksh ===
#!/bin/ksh
#
#
#    Usage: Skript gibt exit 0 zurueck, falls das ausgefuehrte SQL keine Ausgabe erzeugt, exit 1 sonst
# History:
#    5.5.0; 11.11.02; Ingo Schwiters
#                     Initiale Version
#    5.5.1; 27.01.03; Dieter Fieber
#                     Kleine Fehlerbehebungen
#    6.1.1; 15.02.06; Matthias Engelbert ; Aenderungen:
#                     - Parameter "-u" auch in ParamList aufgenommen; Updateskript wurde bisher nicht akzeptiert.
#                     - Ergaenzung nawk-Aufruf zum Akzeptieren von '|' als COLumnSEParator im QueryResult.
#    6.1.2; 29.03.06; Matthias Engelbert ; Aenderungen:
#                     - Ab�nderung, kein Mailversand

# short name of the program
ProgName="Allgmeines Pruefskript ohne Mailversand"
ProgVersion="6.1.2"

# Usage:
#    usage - Print description
usage(){
cat <<EOF
   Programm: $ProgName
   Version:  $ProgVersion
   Aufruf:   $0 -q <Datei der Pruef-Query>
                [-j Jobkennung]
                [-h] [-v]

   Parameter:
       -q     <Datei die das Pruef-Query enth�lt>

       [-j]   <Job-Code>
              Verwendete Jobkennung. Voreinstellung ist PFNW

       [-h]   shows this page

       [-v]   	shows logfile in case if error

    Description:
        Allgemeines Pruefskript - Skript gibt exit 0 zurueck,
        falls das ausgefuehrte SQL keine Ausgabe erzeugt, exit 1 sonst

EOF
}

#####################
#    Definition of further variables

# [TRIMMED for the 3-file DE demo: ". $HOME/aktuell/.dw_init" removed here --
#  framework env bootstrap, not this job's business logic; .dw_init is not
#  one of the 3 files in this demo. See the untrimmed chain under
#  isdwh/pruef/nw/bin/... in ~/data for the real dot-source.]

# [TRIMMED: ". ${DW_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh"
#  removed -- error-framework helper (failure method), not included in this demo.]

# [TRIMMED: ". ${DW_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh"
#  removed -- parameter-parsing helper, not included in this demo.]

# [TRIMMED: ". ${DW_DIR_ROOT}/allgemein/is/util/bin/h_alis_verteiler.ksh"
#  removed -- mail-distributor helper, not included in this demo.]

set -e

ErrNr=0
ErrArg=""

DW_EintragsNr=0
export DW_EintragsNr

#####################
# read parameters
ParameterString="$*"

ParamList="q:j:i"
# initial values
typeset p_Verbose=0
p_QueryFile="not defined"
p_Datenbank="$DW_ORAUSER"
JobKennung="PFNW"

# use getopts for reading the parameters
while getopts ":hv$ParamList" param
do
    case $param in
        q)  p_QueryFile="$OPTARG";;
        j)  JobKennung="$OPTARG";;
        v)
            p_Verbose=1;;
        h)
            usage
            exit;;
        :)
            ErrNr=193  # Argument is missing
            ErrArg="$OPTARG";;
        ?)
            ErrNr=192  # Parameter unknown
            ErrArg="$OPTARG";;
    esac
done

# Are there any files that were not processed yet?
shift $(($OPTIND - 1))
if [ $# -ne 0 ]
then
    ErrNr=192  # Parameter unknown
    ErrArg="\"$*\""
fi

# Check, if all mandatory parameters are set

if [ "$p_QueryFile" != "not defined" ] && ! test -r "$p_QueryFile"
then
  echo "ERROR: Query-Datei $p_QueryFile not readable"
  exit 1
fi

# in case of error, abort script
if [ ! $ErrNr -eq 0 ]
then
    # output according to the failure concept
    DWMSG_MeldeFehler $DW_EintragsNr E $ErrNr $ErrArg
    usage

    exit $ErrNr
fi

#####################
#    Definition of further variables



# Set mini trap
trap "echo 'Without message Signal in $0' Zeile "\$LINENO" Return "\$?"" INT
trap "echo 'Without message Error in $0' Zeile "\$LINENO" Return "\$?" ; exit 1" ERR


  DWMSG_ErmittleNr DW_EintragsNr
  DWMSG_Logdateiname LogDatei $JobKennung $DW_EintragsNr
  DWMSG_ErzeugeEintrag $DW_EintragsNr $JobKennung $0 $LogDatei "$(echo "$ParameterString"|cut -c-235)" >> $LogDatei 2>&1
  # Set traps
  typeset aktion=""
  typeset trap="DWMSG_Fehlerbehandlung $DW_EintragsNr >> $LogDatei 2>&1"
  typeset trap_os="$trap ; echo '!OSERROR reported!'"
  typeset trap_err="$trap ;echo '!ERROR reported!'"

  if [ "$p_Verbose" != "0" ]
  then
      # Set DEBUG Traps
      aktion="; cat $LogDatei "
  fi
  trap "$trap_os  $aktion ; exit 1" INT  >> $LogDatei 2>&1
  trap "$trap_err $aktion" ERR >> $LogDatei 2>&1

echo "----------------- Allgemeines Pruefskript -----------------" | tee -a $LogDatei
echo "JobCode                : $JobKennung" | tee -a $LogDatei
echo "SQL-Pruef-Query           : $p_QueryFile" | tee -a $LogDatei
echo "-----------------------------------------------------------" | tee -a $LogDatei


#####################
# main Job

echo "Execute the following query:" >> $LogDatei
echo "Execute query..."

cat "$p_QueryFile"
cat "$p_QueryFile" >> $LogDatei

# execute query
QueryResult="$(cat "$p_QueryFile" |
                sqlplus -S $p_Datenbank)"
echo "Print query:" >> $LogDatei
echo "$QueryResult" >> $LogDatei
echo " " >> $LogDatei

if echo "$QueryResult" | grep "^ORA-[0-9][0-9]*:" > /dev/null   # Bei ORA-Felher direkt beenden
then
  echo "ERROR: Query reported an error" | tee -a $LogDatei
      DWMSG_MeldeFehler $DW_EintragsNr W $ErrNr $ErrArg >> $LogDatei 2>&1
  if test -r r_pfis*$$.tmp
  then
    rm r_pfis*$$.tmp
  fi
  exit 1
fi

# Header (alles bis zur ersten Zeile, die mit -- beginnt) und
# Footer (alles ab der ersten darauffolgenden leeren Zeile)
# aus der Ausgabe herauschneiden
export QueryResult="$(echo "$QueryResult"|
                      nawk '(o)
                            /^--[-| ]*$/ {o=1}
                            /^$/ {o=0}')"

#echo "Print query:" >> $LogDatei
#echo "$QueryResult" >> $LogDatei

if echo "$QueryResult" | grep "[A-Za-z0-9]" > /dev/null  # Did the query return any results?
then

    echo "Query reported an error - script aborts"
    DWMSG_SetzeStatusOK $DW_EintragsNr >> $LogDatei 2>&1
    Return_Code=1

else

   echo "Query reported no errors - script ends without any errors" | tee -a $LogDatei
   DWMSG_SetzeStatusOK $DW_EintragsNr >> $LogDatei 2>&1
   Return_Code=0
fi

   if test -r r_pfis*$$.tmp
   then
     rm r_pfis*$$.tmp
   fi

trap INT ERR

exit $Return_Code


=== CONVERSION VERDICT ===
VERDICT: PYTHON
REASON: The script contains command-line argument parsing, validation of query files, error traps, dynamic execution of SQL queries, text filtering with AWK, and conditional exit-code logic that requires a Python implementation.

EVIDENCE
- Business logic found: KSH custom logic. The script accepts a SQL file via `-q`, validates its accessibility, executes it, uses AWK to parse the stdout, and returns exit code 1 if the query returned any data rows (indicating validation failure) or 0 if it returned nothing.
- AWK: The program `nawk '(o) /^--[-| ]*$/ {o=1} /^$/ {o=0}'` is used to parse raw SQL*Plus text output, extracting rows between the column underline separator and the trailing summary. This is a text-formatting parser, not SQL-shaped.
- SQL-expressible: No, because the script functions as a generic execution runner for dynamic SQL files passed via CLI, relying on shell-level logging, text manipulation, and exit-code propagation.
- Non-SQL side effects: Writing log files, executing arbitrary dynamic SQL scripts from the local filesystem, and managing custom shell traps and exit codes.
- Against this verdict: None. It is a utility harness rather than a static query, which makes Python the only suitable target.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script acts as a generic validation test runner. It reads a dynamic SQL query from a specified file, executes it against the database, and processes the output. If the query returns any actual data records, the validation fails (returning exit code 1); if no records are found, the validation passes (returning exit code 0). This is used in data warehouse pipelines to perform automated quality/integrity checks.

2. INVOCATION CONTEXT
   - **Caller / UC4 Job**: Likely called by an Automic/UC4 Unix job object (e.g., `JOBS_UNIX`) passing the query file path via `-q`.
   - **UC4 Includes**: None referenced in the provided extraction.
   - **Environment Files Sourced**:
     - `. $HOME/aktuell/.dw_init` (Trimmed in source) — `# REVIEW-STRUCT: environment file .dw_init not supplied — variables it sets are unknown; do not guess their names or values`
     - `. ${DW_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (Trimmed helper) — `# REVIEW-STRUCT: helper script f_alis_msgerr.ksh not supplied — behaviour unknown`
     - `. ${DW_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (Trimmed helper) — `# REVIEW-STRUCT: helper script h_alis_parameter.ksh not supplied — behaviour unknown`
     - `. ${DW_DIR_ROOT}/allgemein/is/util/bin/h_alis_verteiler.ksh` (Trimmed helper) — `# REVIEW-STRUCT: helper script h_alis_verteiler.ksh not supplied — behaviour unknown`

3. PARAMETERS / INPUTS
   - `p_QueryFile` (parsed via `-q`): Positional parameter argument representing the path to the SQL query file. Used and critical. Surface in Python via `argparse`.
   - `JobKennung` (parsed via `-j`): Job identifier string, defaults to `PFNW`. Used in logfile naming and messaging. Surface in Python via `argparse`.
   - `p_Verbose` (parsed via `-v`): Verbose flag, sets `p_Verbose=1`. Used to control debugging output on errors. Surface in Python via `argparse`.
   - `p_Datenbank` (defaults to `$DW_ORAUSER`): Database connection string. Originally used for SQL*Plus. Since `TARGET_PLATFORM` is `BIGQUERY`, this parameter's Oracle-specific default is obsolete, but the variable should be retained or mapped to the BigQuery dataset/project configuration.
   - `DW_EintragsNr`: An environment variable tracking execution ID. Used in messaging. Surface in Python via `os.environ` or generate dynamically.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `sqlplus -S $p_Datenbank`: Oracle command-line utility used to execute the query. Under the confirmed `BIGQUERY` target platform, this must become a native Python DB-client call utilizing `google.cloud.bigquery.Client()`.
   - `nawk '(o) /^--[-| ]*$/ {o=1} /^$/ {o=0}'`: Used to extract structured query rows from raw SQL*Plus text output. Since the BigQuery Python client returns structured row objects directly, this text parsing logic is obsolete and will be handled natively by checking the row count of the query results.
   - `grep "^ORA-[0-9][0-9]*:"`: Used to detect Oracle execution errors in stdout. This is replaced by handling `google.cloud.exceptions.GoogleCloudError` in Python.
   - `grep "[A-Za-z0-9]"`: Used to check if the cleaned query result contains any alphanumeric characters. Replaced in Python by checking if the returned BigQuery RowIterator contains elements.
   - `rm r_pfis*$$.tmp`: Clean up temporary files. In Python, we do not need to create temporary files for SQL output processing.
   - `DWMSG_*` (e.g., `DWMSG_MeldeFehler`, `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_Fehlerbehandlung`, `DWMSG_SetzeStatusOK`): Custom shell framework functions for logging and error reporting. `# REVIEW-STRUCT: launcher commands DWMSG_* invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion`.

5. EMBEDDED SQL
   - **Source**: Dynamic SQL loaded from the path specified by the `-q` parameter.
   - **Dialect**: Originally Oracle SQL.
   - **Dialect Conversion**: Since the target platform is `BIGQUERY`, the SQL query contained in the input file must be written in BigQuery Standard SQL dialect. The Python execution harness will load the file contents and execute them directly on BigQuery.

6. CONTROL FLOW
   1. **Environment Setup**: Initialize variables `ErrNr = 0`, `ErrArg = ""`, `DW_EintragsNr = 0`.
   2. **Argument Parsing**: Parse `-q`, `-j`, `-v`, and `-h` using `getopts`. (Python: `argparse`).
   3. **Validate Parameters**: Check if `p_QueryFile` is specified and is a readable file. If not, print an error and exit with 1.
   4. **Initialization & Logging Setup**: Retrieve tracking number and logfile name via `DWMSG_*` utilities, write startup parameters to log.
   5. **Signal Traps**: Establish shell traps to handle interrupts and unexpected errors. (Python: standard `try-except-finally`).
   6. **Read and Execute Query**: Read the query text from the specified file. Run the query against BigQuery.
   7. **Error Detection**: Catch database/execution exceptions. If any error occurs, write to the log, execute failure messaging, and exit with 1.
   8. **Check Results**: Examine the returned dataset. If any rows are returned, set the exit code to 1 (validation failed). If no rows are returned, set the exit code to 0 (validation succeeded).
   9. **Cleanup**: Remove any temporary files (if created).
   10. **Exit**: Propagate the determined exit code.

7. ERROR HANDLING & EXIT CODES
   - If the query file cannot be read: exits with `1`.
   - If invalid arguments are passed: exits with custom code `192` or `193`.
   - If the query execution fails (database-side error): catches exception, logs it, and exits with `1`.
   - If the validation query returns records: validation fails, exits with `1`.
   - If the validation query returns zero records: validation succeeds, exits with `0`.
   - Python mapping: Wrap the BigQuery client call in a `try-except` block to catch database exceptions, logging the exception and raising `sys.exit(1)`. Standard `argparse` handles argument validation errors.

8. OUTPUTS / SIDE EFFECTS
   - **Log File**: Writes step-by-step progress, SQL queries, execution results, and errors to `LogDatei`.
   - **Stdout/Stderr**: Standard print statements indicating success or failure.

9. BUSINESS SUMMARY
   - Performs automated data warehouse quality control checks.
   - Executes validation rules formatted as SQL SELECT queries (where any returned row indicates an anomaly or constraint violation).
   - Generates operational tracking logs for execution audits.
   - Communicates validation status (0 for OK, 1 for failure) directly to the parent orchestrator (UC4) to halt downstream processing on failure.

=== PSEUDOCODE ===

```python
# Step 1: Import required modules
import os
import sys
import argparse
import logging
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

# Step 2: Define command-line interface
def parse_arguments():
    parser = argparse.ArgumentParser(description="Allgmeines Pruefskript ohne Mailversand")
    parser.add_argument("-q", dest="query_file", required=True, help="Path to the validation SQL query file")
    parser.add_argument("-j", dest="job_id", default="PFNW", help="Job identifier (default: PFNW)")
    parser.add_argument("-v", dest="verbose", action="store_true", help="Verbose log output on error")
    return parser.parse_args()

def main():
    args = parse_arguments()
    
    # Step 3: Validate query file existence and readability
    if not os.path.isfile(args.query_file) or not os.access(args.query_file, os.R_OK):
        print(f"ERROR: Query-Datei {args.query_file} not readable", file=sys.stderr)
        sys.exit(1)
        
    # Step 4: Environment bootstrap and framework logging initialization
    # # REVIEW-STRUCT: environment file .dw_init not supplied — variables it sets are unknown; do not guess their names or values
    # # REVIEW-STRUCT: launcher commands DWMSG_* invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
    dw_eintrags_nr = os.environ.get("DW_EintragsNr", "0")
    log_file_path = f"/tmp/validation_{args.job_id}_{dw_eintrags_nr}.log" # Placeholder log path fallback
    
    logging.basicConfig(
        filename=log_file_path,
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    
    print("----------------- Allgemeines Pruefskript -----------------")
    print(f"JobCode                : {args.job_id}")
    print(f"SQL-Pruef-Query           : {args.query_file}")
    print("-----------------------------------------------------------")
    
    logging.info("Execute the following query:")
    
    # Step 5: Read query from file
    try:
        with open(args.query_file, 'r', encoding='utf-8') as f:
            query_text = f.read()
    except Exception as e:
        logging.error(f"Failed to read query file: {str(e)}")
        print(f"ERROR: Failed to read query file: {str(e)}", file=sys.stderr)
        sys.exit(1)
        
    logging.info(query_text)
    
    # Step 6: Initialize BigQuery client and execute query
    # Note: Authentication and configuration are handled via standard Google Cloud environment variables.
    try:
        client = bigquery.Client()
        logging.info("Executing query on BigQuery...")
        query_job = client.query(query_text)
        results = query_job.result() # Waits for query to complete
    except GoogleCloudError as dberr:
        # Step 7: Handle query execution errors
        logging.error(f"ERROR: Query reported an error: {str(dberr)}")
        print("ERROR: Query reported an error", file=sys.stderr)
        # # REVIEW-STRUCT: DWMSG_MeldeFehler helper call replacement
        sys.exit(1)
        
    # Step 8: Analyze results
    # Checking if there are any returned rows containing non-null validation data.
    # If rows exist, validation fails.
    row_count = results.total_rows
    
    if row_count is not None and row_count > 0:
        logging.warning(f"Query returned {row_count} rows. Validation failed.")
        print("Query reported an error - script aborts", file=sys.stderr)
        # # REVIEW-STRUCT: DWMSG_SetzeStatusOK helper call replacement
        return_code = 1
    else:
        logging.info("Query returned 0 rows. Validation succeeded.")
        print("Query reported no errors - script ends without any errors")
        # # REVIEW-STRUCT: DWMSG_SetzeStatusOK helper call replacement
        return_code = 0
        
    # Step 9: Final cleanup and exit
    sys.exit(return_code)

if __name__ == "__main__":
    main()
```

### Execution order
The target orchestration (e.g., Cloud Composer DAG task chain) must preserve the execution order of the following step mapping:
* **Step 1**: `DW.DWH_PFNW_ILV_FLAGTEST.xml` (Legacy UC4 scheduling/orchestration block) $\rightarrow$ Maps to the Cloud Composer DAG metadata, structure, and DAG-level execution constraints.
* **Step 2**: `d_pfnw_ilv_flagtest.sql` (The validation query file) $\rightarrow$ This is the SQL resource file deployed alongside the Python task, which is executed against BigQuery.
* **Step 3**: `k_pfnw_ilv_flagtest.ksh` (The KSH execution wrapper) $\rightarrow$ Converted to a Python step (`k_pfnw_ilv_flagtest.py`) executed within Cloud Composer (e.g., via a `PythonOperator`).

### Schedule & variables
The migrated workflow must preserve the scheduler-configured variables:
* **Scheduler-Set Variables**:
  * `DWH_JOB_KENNUNG` (value: `'PFNW_ILV_FLAGTEST '`): This variable must be passed from Cloud Composer (e.g., via DAG parameters or Airflow task environment variables) to the Python script to maintain trace logging consistency.

### Lineage
* **Upstream Producers**: No cross-job lineage edges were found for the source file.
* **Downstream Consumers**: No cross-job lineage edges were found for the source file.

### Cross-file dependencies
* **SQL Query Dependency**: The validation runner requires the presence of `d_pfnw_ilv_flagtest.sql` to execute. The SQL file queries the maintenance table `DWH$TA_K_ILV_ABR_ILV` to check if `job_starten_flag` is set to `0` for the `MAXIMO FILL` step.
* **Orchestration / Sentinel Dependency**: Downstream jobs in the ILV job plan rely on this script completing with exit code `0` (indicating the sentinel flag check succeeded) before they can continue.

### Target file plan
* **Target File**: `k_pfnw_ilv_flagtest.py`
  * **Language**: Python (v3.8+)
  * **Source File**: `k_pfnw_ilv_flagtest.ksh`
  * **Purpose**: A Python-based validation harness that loads a SQL query file, executes it on BigQuery, and returns an exit code of `0` if zero records are returned (validation passed) or `1` if any record is found or a database error occurs (validation failed).

### Environment-specific values
The legacy script contains variables that represent environment configuration. These are classified below:

1. **GLOBAL (Environment-wide)**:
   * `DW_ORAUSER` (Legacy connection string variable): Represents the database endpoint. Under the confirmed BigQuery target platform, this is obsolete and maps to global infrastructure parameters.
     * *Normalized Target Names*: `GCP_PROJECT`, `BQ_DATASET`
     * *Resolution*: Accessed via the standard environment variables `os.environ.get("GCP_PROJECT")` or resolved implicitly through the Google Cloud application default credentials (ADC) inside the `google.cloud.bigquery.Client()`.
   * `DW_EintragsNr` (Legacy tracking number):
     * *Target Name*: `DW_EintragsNr`
     * *Resolution*: Read via `os.environ.get("DW_EintragsNr")` or generated dynamically in Cloud Composer as the Airflow task execution context ID.

2. **JOB-SPECIFIC**:
   * `JobKennung` (Defaults to `'PFNW'`): Identifies the job group.
     * *Target Name*: `args.job_id` / `JOB_ID`
     * *Resolution*: Configured as a script command-line parameter or DAG parameter.
   * `p_QueryFile` (Passed via `-q`): Path to the validation query SQL file.
     * *Target Name*: `args.query_file`
     * *Resolution*: Passed dynamically to the script at runtime in the orchestration task definition.

### File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `k_pfnw_ilv_flagtest.ksh` | `k_pfnw_ilv_flagtest.py` | Converted to a Python 3 script using the BigQuery Python Client to execute the SQL validation query and handle exit status codes. |

### Risks & Manual Actions
* **Framework Functions (`DWMSG_*`)**: The script invokes legacy DWMSG utilities (such as `DWMSG_MeldeFehler`, `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_Fehlerbehandlung`, and `DWMSG_SetzeStatusOK`) to write logs and register statuses. No direct native equivalent exists for these functions on the target platform. Standard Python `logging` or integration with the Cloud Composer/Airflow task runner's logging and status tracking must be manually configured to replace them.
* **Environment Initialization (`.dw_init`)**: The script originally sourced an environment initialization file `.dw_init`, which is not present in the workspace. Any hidden environment variables or configurations it set must be verified and provided through Composer's variable storage.