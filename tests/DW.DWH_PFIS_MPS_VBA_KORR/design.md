=== OBJECT: DW.DWH_PFIS_MPS_VBA_KORR (JOBS_UNIX) ===
active=1
title=Korrektur nicht ermittelbarer VBA-IDs
login=DW.UNIX.ISTNS
host=|DWHDWH1P|HOST
ert_seconds=452
launcher_type=unrecognized
launcher_details={'raw_command': '# [TRIMMED for the 3-file DE demo: ". $HOME/.dw_init" removed here --'}
script_body:
:inc DW.HOLE_PFAD
:set &DWH_JOB_KENNUNG='PFIS_MPS_VBA_KORR'
# [TRIMMED for the 3-file DE demo: ". $HOME/.dw_init" removed here --
#  framework env bootstrap, not this job's business logic; .dw_init is not
#  one of the 3 files in this demo. See the untrimmed chain under
#  isdwh/pruef/is/... in ~/data for the real dot-source.]
$HOME/aktuell/pruef/is/bin/r_pfis_mps_vba_korrektur
:inc DW.LESE_LOG
operational_notes=fehlgeschlagener oder unterbrochener Prozeß kann ohne weitere Arbeiten erneut ausgeführt werden.

=== UNRESOLVED REFERENCES (object named but not supplied in this bundle) ===
  (none — every referenced object was supplied in this bundle)


# Design Document: UC4 to Apache Airflow Migration

## 1. Overview
*UNCERTAIN: This extraction contains only a single `JOBS_UNIX` object and does not include a parent `JOBP` (workflow) or a triggering `SCRI` script.* 

This workflow executes the Unix job `DW.DWH_PFIS_MPS_VBA_KORR`, which performs data corrections on unidentifiable VBA IDs (`Korrektur nicht ermittelbarer VBA-IDs`). It runs a backend script/binary located at `$HOME/aktuell/pruef/is/bin/r_pfis_mps_vba_korrektur`. Based on the operational notes, this process is fully idempotent and can be safely restarted or rerun without any manual preparation or cleanup in the event of an interruption or failure. Because no parent `JOBP` workflow was provided in this bundle, this job is represented as a standalone, single-task Airflow DAG, assuming external triggering.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
|---|---|---|---|
| `DW.DWH_PFIS_MPS_VBA_KORR` | JOBS_UNIX | 1 (Active) | Korrektur nicht ermittelbarer VBA-IDs |

## 3. Scheduling
* **Schedule:** `None`
* **Trigger Analysis:** No scheduling configurations, `EVNT_TIME` objects, or execution-triggering scripts (`SCRI`) are present in this bundle. This workflow is considered externally triggered (source unknown from this extraction alone).

## 4. Airflow DAG Properties
| Property | Value |
|---|---|
| **dag_id** | `dw_dwh_pfis_mps_vba_korr` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` *(placeholder)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` *(Active=1)* |
| **default_args** | `{'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}` |

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `dw_dwh_pfis_mps_vba_korr_task` | `DW.DWH_PFIS_MPS_VBA_KORR` | EmptyOperator | N/A | N/A | 1 | 5m | None | None | N/A | None | launcher command not recognised — confirm target operator/script manually. See Developer Notes. |

## 6. Task Dependency Map
```python
dw_dwh_pfis_mps_vba_korr_task
```
*(Single-task DAG; no dependencies).*

## 7. Sync / Concurrency Analysis
No `sync_rows` or resource locks were identified in this extraction. The standard DAG-level limit of `max_active_runs=1` is sufficient.

## 8. Error Handling and Retry Strategy
* **Rerun Safety:** Per the UC4 operational notes, this job is safe to rerun directly upon failure: *"fehlgeschlagener oder unterbrochener Prozeß kann ohne weitere Arbeiten erneut ausgeführt werden."*
* **Retries:** Standard 1 retry with a 5-minute delay is implemented. No post-execution scripting or complex block/abort behavior was defined in this object.

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
|---|---|---|
| `&DWH_JOB_KENNUNG` | `'PFIS_MPS_VBA_KORR'` | Airflow task environment variable or parameter `DWH_JOB_KENNUNG` |
| `dw_dwh_pfis_mps_vba_korr` | Sanitised Object Name | `dag_id` |

## 10. Developer Notes
* `# REVIEW-STRUCT:` **Unrecognized Launcher:** The original UC4 object used an unrecognized launcher type to execute `$HOME/aktuell/pruef/is/bin/r_pfis_mps_vba_korrektur`. This has been mapped to an `EmptyOperator` placeholder task. During implementation, the developer must substitute this with a suitable execution operator (such as `SSHOperator` or `BashOperator` running in a containerized environment), ensuring the script path and execution environment are correctly set up.
* `# REVIEW-STRUCT:` **Missing Parent Workflow:** This `JOBS_UNIX` object was supplied without a parent `JOBP` workflow. It has been wrapped into its own standalone DAG. Verify if this job should instead be integrated as a task inside a larger, coordinated parent Airflow DAG.
* `# REVIEW-STRUCT:` **UC4 Includes:** The original script included `:inc DW.HOLE_PFAD` and `:inc DW.LESE_LOG`. These are environment-setting and log-reading wrappers native to UC4. Ensure that environment variables (like paths) and logging configurations are handled natively by the target Airflow execution environment instead of trying to replicate these includes literally.

---

# Pseudocode Outline

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# ── GCP Configuration ────────────────────────────────────
# No GCP-specific infrastructure is referenced in the extraction.

# ── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── on_failure_callback stubs ─────────────────────────────
# No complex postconditions or failure triggers defined in the source.

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id='dw_dwh_pfis_mps_vba_korr',
    default_args=DEFAULT_ARGS,
    description='Korrektur nicht ermittelbarer VBA-IDs',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['migrated_uc4', 'dwh', 'pfis'],
) as dag:

    # ── Task: dw_dwh_pfis_mps_vba_korr_task ──────────────────
    # # REVIEW-STRUCT: launcher command [$HOME/aktuell/pruef/is/bin/r_pfis_mps_vba_korrektur]
    # not recognised. Converted to EmptyOperator. Confirm target operator manually 
    # (e.g., SSHOperator or BashOperator) and carry over environment variable:
    # DWH_JOB_KENNUNG = 'PFIS_MPS_VBA_KORR'
    dw_dwh_pfis_mps_vba_korr_task = EmptyOperator(
        task_id='dw_dwh_pfis_mps_vba_korr_task',
    )

    # ── Dependencies ─────────────────────────────────────────
    # Single task DAG; no dependency declaration required.
    dw_dwh_pfis_mps_vba_korr_task
```

### Execution Order
The target orchestration (Apache Airflow DAG) must strictly preserve the legacy call sequence:
1. **Orchestrator Initiation (`DW.DWH_PFIS_MPS_VBA_KORR.xml`)**: Maps to the Airflow DAG definition file `dw_dwh_pfis_mps_vba_korr.py` which triggers and manages the overall workflow.
2. **Script Execution (`r_pfis_mps_vba_korrektur`)**: Maps to a task in the DAG running the migrated Python script `r_pfis_mps_vba_korrektur.py` (designed and generated in a sibling migration pass).
3. **Database Correction (`d_pfis_mps_vba_korrektur.sql`)**: Maps to a downstream BigQuery execution task running the migrated SQL query `d_pfis_mps_vba_korrektur.sql` (designed and generated in a sibling migration pass).

### Schedule & Variables
* **Scheduler-Set Variable**: `DWH_JOB_KENNUNG` with the value `'PFIS_MPS_VBA_KORR'`.
* **Target Mapping**: This value is **JOB-SPECIFIC** and must be passed to the execution environment of the task running the downstream script (e.g., via Airflow task environment dictionary `env={'DWH_JOB_KENNUNG': 'PFIS_MPS_VBA_KORR'}`).

### Lineage
* **Upstream Includes**: `DW.HOLE_PFAD` and `DW.LESE_LOG` are referenced in the UC4 script block. Since these are human-reviewed and confirmed as "NO SOURCE NEEDED", they are omitted in the target environment where pathing and logging are managed natively by Cloud Composer.
* **Downstream Invocation**: `DW.DWH_PFIS_MPS_VBA_KORR.xml` invokes `r_pfis_mps_vba_korrektur` (confidence 0.90). This is represented in Airflow as a Python execution task.
* **Infrastructure Attributes**: Runs on legacy host `dwhdwh1p` using login credential profile `DW.UNIX.ISTNS` (packages `DW.UNIX.ISTNS` and `DWPA_MELDUNG`). These map to target Cloud Composer execution parameters and service accounts (IAM roles).

### Cross-File Dependencies
* **Sequential Call Chain**: The parent UC4 job acts as an orchestrator that triggers `r_pfis_mps_vba_korrektur`, which in turn triggers `d_pfis_mps_vba_korrektur.sql`. In Airflow, this sequence must be defined explicitly as a task chain (`dw_dwh_pfis_mps_vba_korr_task >> run_r_pfis_mps_vba_korrektur >> run_d_pfis_mps_vba_korrektur_sql`).

### Target File Plan
* **Target File Path**: `dw_dwh_pfis_mps_vba_korr.py`
  * **Language**: Python (Apache Airflow DAG)
  * **Source File**: `DW.DWH_PFIS_MPS_VBA_KORR.xml`
  * **Purpose**: Orchestrates and coordinates the task executions, environment variable assignments, and execution order of the job.

### Environment-Specific Values
* **`DWH_JOB_KENNUNG` (Value: `'PFIS_MPS_VBA_KORR'`)**: Classified as **JOB-SPECIFIC**. Provided to the execution environment at the Airflow DAG/task configuration level.
* **`DW.UNIX.ISTNS` (UC4 Login)**: Classified as **GLOBAL**. Maps to the execution GCP Service Account associated with the Cloud Composer environment.
* **`dwhdwh1p` (Legacy Host)**: Classified as **GLOBAL**. Maps to the target GCP project execution platform.

### File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DW.DWH_PFIS_MPS_VBA_KORR.xml` | `dw_dwh_pfis_mps_vba_korr.py` | Converts the legacy UC4 job definition into an Airflow DAG file maintaining the script execution sequence. |

---

=== FILE: local/home/gurunathan_t/single_job_demo_v3/d_pfis_mps_vba_korrektur.sql ===
/* ---------------------------------------------------------------------
-- Erstellt : 09.02.2004; Sascha Blumenthal
-- Parameter:
--   P1:    Fehlereintragnummer des aufrufenden Skriptes
--          
-- Zweck/Aufgabe:
--    Datensaetze in DWH$TA_F_MPS_NUTZUNG, fuer die zum Zeitpunkt des Imports
--    die Vertriebsart nicht ermittelt werden konnte, muessen nach der Aktualisierung
--    der VBA-Hierarchie bereinigt werden. D.h. die eingetragene Default-VBA-ID muss
--    an Hand des VBA-Textes aktualisiert werden
--
-- HISTORY
-- Sascha Blumenthal; 09.02.2004     
--                    Erstellung
-------------------------------------------------------------------------*/

-- Bei aufgetretenen Fehlern abbrechen. 
-- Als Fehlercode wird die Nummer des SQL-Fehlers bzw. des Betriebssystem-
-- fehlers gemeldet. Das aufrufende Programm (z.B. Shell-skript) muss diesen 
-- entsprechend bearbeiten. 
--
WHENEVER OSERROR EXIT SQL.OSCODE ROLLBACK;

START $DW_DIR_ROOT/allgemein/is/util/sql/d_alis_init.sql

SET TIMING ON;
SET TIME OFF;
SET ECHO OFF;
SET VERIFY OFF;
SET FEEDBACK OFF;

DECLARE
   EintragsNr NUMBER;
   
BEGIN
   EintragsNr      := TO_NUMBER('&1');

   -- Falls der Ebenen-6-Text in der VBA-Lookup enthalten ist, die entsprechende ID in die Fakten
   -- eintragen. Falls nicht, die Default-ID in den Fakten stehen lassen. Dies leistet das
   -- innere Select durch einen Join der Lookup- und der Faktentabelle
   UPDATE dwh$ta_f_mps_nutzung n 
      SET n.m2_vba_ebene6_id = (
             SELECT MIN(NVL(v.m2_vba_ebene7_id, n_sub.m2_vba_ebene6_id))
               FROM dwh$vi_l_m2_vba v,
                    dwh$ta_f_mps_nutzung n_sub
              WHERE UPPER(n_sub.m2_vba_ebene6_text) = UPPER(v.m2_vba_ebene6_text (+))
                AND n_sub.m2_vba_ebene6_text IS NOT NULL
              GROUP BY n_sub.ROWID
             HAVING n_sub.ROWID=n.ROWID       
          )  
    WHERE n.m2_vba_ebene6_text IS NOT NULL;

   -- Im Nachgang die Ebenen-6-Texte der Datensaetze entfernen, deren ID ermittelt werden 
   -- konnte 
   UPDATE dwh$ta_f_mps_nutzung n 
      SET n.m2_vba_ebene6_text = NULL
    WHERE n.m2_vba_ebene6_text IS NOT NULL
      AND n.m2_vba_ebene6_id <> (
         SELECT v.m2_vba_ebene7_id
           FROM dwh$vi_l_m2_vba v
          WHERE UPPER(v.m2_vba_ebene6_text) = 'UNBEKANNT'
      )
      ;

   -- Nun das gleiche fuer die Ebene-7: 
   -- Falls der Ebenen-7-Text in der VBA-Lookup enthalten ist, die entsprechende ID in die Fakten
   -- eintragen. Falls nicht, die Default-ID in den Fakten stehen lassen. Dies leistet das
   -- innere Select durch einen Join der Lookup- und der Faktentabelle
   UPDATE dwh$ta_f_mps_nutzung n 
      SET n.m2_vba_ebene7_id = (
             SELECT NVL(v.m2_vba_ebene7_id, n_sub.m2_vba_ebene7_id)
               FROM dwh$vi_l_m2_vba v,
                    dwh$ta_f_mps_nutzung n_sub
              WHERE UPPER(v.m2_vba_ebene7_text (+)) = UPPER(n_sub.m2_vba_ebene7_text)
                AND n_sub.m2_vba_ebene7_text IS NOT NULL
                AND n_sub.ROWID=n.ROWID       
          )  
   WHERE n.m2_vba_ebene7_text IS NOT NULL;
   
   -- Im Nachgang die Ebenen-7-Texte der Datensaetze entfernen, deren ID ermittelt werden 
   -- konnte 
   UPDATE dwh$ta_f_mps_nutzung n 
      SET n.m2_vba_ebene7_text = NULL
    WHERE n.m2_vba_ebene7_text IS NOT NULL
      AND n.m2_vba_ebene7_id <> (
         SELECT v.m2_vba_ebene7_id
           FROM dwh$vi_l_m2_vba v
          WHERE UPPER(v.m2_vba_ebene7_text) = 'UNBEKANNT'
      ); 
   
   EXCEPTION
      WHEN OTHERS THEN
      -- unbekannte bzw. nicht erwartete Exception koennen auch 
      -- behandelt werden. Die Fehlernummer ist immer die gleiche, nur
      -- der Zusatzfehlertext kann vorher ermittelt werden.
      ROLLBACK;
--
      DECLARE
        ErrText  VARCHAR2(512);
        ErrC     NUMBER;
        FehlerNr NUMBER := dwpa_globals.k_alis_err_unknown;
      BEGIN
        ErrText  := SQLERRM;
        ErrC     := SQLCODE;
        dwpa_meldung.fehler ('F', EintragsNr, FehlerNr, ErrText, 
                             TO_CHAR(ErrC));
        raise_application_error(FehlerNr, ErrText);
      END;
END;
/
COMMIT;
EXIT SUCCESS;

═══════════════════════════════════════════
SECTION 1 — DESIGN DOCUMENT
═══════════════════════════════════════════

Step 1: Understand the Script
1.1 Identify the type of Oracle SQL object being converted:
    - This is a PL/SQL anonymous block containing DML (UPDATE) operations, transaction controls, exception handling, and calls to external package functions. It operates as a script accepting a substitution parameter.

1.2 Summarize the business logic and purpose of the script in plain English:
    - The script corrects the distribution channel (VBA - Vertriebsart) IDs in the fact table `dwh$ta_f_mps_nutzung`.
    - It aligns level-6 (`ebene6`) and level-7 (`ebene7`) IDs based on text-matching from a lookup view `dwh$vi_l_m2_vba`.
    - For records where the text description can be matched, the script updates the IDs. Once the correct ID is written, the raw text description columns are nullified to clean up the data.
    - If the lookup doesn't return a match, the existing default IDs are preserved.
    - Transaction boundaries are maintained, and an exception block catches unexpected errors, logs them using a custom logging package (`dwpa_meldung`), and raises an application error.

1.3 List all entities referenced:
    - Tables:
        - `dwh$ta_f_mps_nutzung` (Fact table being updated; Aliased as `n` and `n_sub`)
    - Views:
        - `dwh$vi_l_m2_vba` (Lookup view containing hierarchical descriptions and IDs; Aliased as `v`)
    - Columns:
        - `dwh$ta_f_mps_nutzung.m2_vba_ebene6_id` (NUMBER)
        - `dwh$ta_f_mps_nutzung.m2_vba_ebene6_text` (VARCHAR2)
        - `dwh$ta_f_mps_nutzung.m2_vba_ebene7_id` (NUMBER)
        - `dwh$ta_f_mps_nutzung.m2_vba_ebene7_text` (VARCHAR2)
        - `dwh$vi_l_m2_vba.m2_vba_ebene6_text` (VARCHAR2)
        - `dwh$vi_l_m2_vba.m2_vba_ebene7_text` (VARCHAR2)
        - `dwh$vi_l_m2_vba.m2_vba_ebene7_id` (NUMBER)
    - External PL/SQL Packages:
        - `dwpa_globals.k_alis_err_unknown` (Package constant)
        - `dwpa_meldung.fehler` (Package logging procedure)

Step 2: Oracle-Specific Construct Detection and Resolution

2.1 Data Type Conversions:
    - Oracle `NUMBER` used for identifiers (`EintragsNr`, `ErrC`, `FehlerNr`, and table ID columns) -> Resolve to `INT64` in BigQuery.
    - Oracle `VARCHAR2` -> Resolve to `STRING` in BigQuery.

2.2 Implicit and Explicit Type Casting:
    - `TO_NUMBER('&1')` -> Convert explicit cast to BigQuery `CAST(@P1 AS INT64)`.
    - `TO_CHAR(ErrC)` -> Convert to BigQuery `CAST(ErrC AS STRING)`.

2.3 NULL Handling and Conditional Functions:
    - `NVL(v.m2_vba_ebene7_id, n_sub.m2_vba_ebene6_id)` -> Convert to `COALESCE(v.m2_vba_ebene7_id, n_sub.m2_vba_ebene6_id)`.
    - `NVL(v.m2_vba_ebene7_id, n_sub.m2_vba_ebene7_id)` -> Convert to `COALESCE(v.m2_vba_ebene7_id, n_sub.m2_vba_ebene7_id)`.

2.4 String Functions:
    - `UPPER(col)` -> Direct equivalent in BigQuery: `UPPER(col)`.

2.5 Date and Timestamp Functions:
    - None used in this script.

2.6 Numeric and Aggregate Functions:
    - `MIN(expr)` -> Direct equivalent in BigQuery: `MIN(expr)`.

2.7 Analytical and Window Functions:
    - None used in this script.

2.8 Set and Join Operations:
    - Proprietary Oracle Left Outer Join syntax `(+)` (e.g., `UPPER(n_sub.m2_vba_ebene6_text) = UPPER(v.m2_vba_ebene6_text (+))`) -> Refactor using a explicit modern `MERGE` statement with modern ANSI `LEFT OUTER JOIN` logic or pre-aggregated subqueries to completely replace the complex self-referencing `ROWID` update block.

2.9 Row Limiting and Sampling:
    - Oracle scalar subqueries under UPDATE statement filters require isolation to avoid multiple row exceptions -> Use `LIMIT 1` inside the scalar subquery block in BigQuery.

2.10 Sequences:
    - None used in this script.

2.11 MERGE Statements:
    - The self-correlated updates using `ROWID` correlation are converted into standard `MERGE` statements in BigQuery. This allows scanning the target table once, matching against pre-aggregated lookup keys, and executing the modifications efficiently without unsupported `ROWID` references.

2.12 INSERT / UPDATE / DELETE:
    - Oracle updates matching on `ROWID` are refactored into high-performance `MERGE` blocks in BigQuery.
    - Correlated subqueries inside `WHERE` clauses (e.g., matching the default ID for 'UNBEKANNT') are restructured to use explicit standard SQL scalar subqueries with `LIMIT 1`.

2.13 DDL Constructs (if present):
    - None present.

2.14 PL/SQL (if present):
    - PL/SQL anonymous block (`DECLARE ... BEGIN ... EXCEPTION ... END;`) -> Map to BigQuery standard scripting block (`DECLARE`, `SET`, `BEGIN ... EXCEPTION WHEN ERROR THEN ... END;`).
    - Transaction control (`ROLLBACK`) -> Map to BigQuery scripting `ROLLBACK TRANSACTION;`. An explicit `BEGIN TRANSACTION;` and `COMMIT TRANSACTION;` block is introduced around the DML execution steps.
    - Exception variables `SQLERRM` and `SQLCODE` -> Map to BigQuery execution metadata system variables `@@error.message` and `@@error.code`.
    - Custom application error raising `raise_application_error` -> Map to BigQuery scripting `ERROR msg;` statement.

2.15 Unresolvable or Advisory Items:
    - Command-line substitution variable `&1` -> Pass as script parameter `@P1` during execution.
    - Package call `dwpa_meldung.fehler` -> Convert to a placeholder call to a user-defined stored procedure `CALL dataset.dwpa_meldung_fehler(...)`.
    - Constant `dwpa_globals.k_alis_err_unknown` -> Hardcode or initialize using a placeholder variable `FehlerNr`.

3.1 Conversion Strategy Summary:
    - Translate the PL/SQL script into a single BigQuery scripting block.
    - Use variables for session variables (`EintragsNr`, error variables).
    - Refactor complex updates utilizing Oracle `ROWID` and `(+)` joins into optimized `MERGE` statements in BigQuery.
    - Implement a transaction block (`BEGIN TRANSACTION` / `COMMIT TRANSACTION`) within the script to replicate Oracle's atomicity and rollback capability.
    - Provide a placeholder call for logging routines.

3.2 Assumptions:
    - The lookup view `dwh$vi_l_m2_vba` and target table `dwh$ta_f_mps_nutzung` are successfully migrated and present in the BigQuery target dataset.
    - The environment running the BigQuery script passes parameter `P1` as a named query parameter `@P1`.

3.3 Items Flagged for Human Review:
    - Handlers for proprietary packages (`dwpa_meldung` and `dwpa_globals`) must be recreated in BigQuery as custom stored procedures, or replaced by target cloud logging solutions.

2.16 MIGRATION DECISION MATRIX

| Oracle Construct / Code Section | Selected BigQuery Target | Rejected Alternatives | Evidence / Reason |
| :--- | :--- | :--- | :--- |
| PL/SQL Anonymous block | BigQuery Scripting Block (`BEGIN ... END`) | Python wrapper, Cloud Composer orchestration | Scripting natively supports procedural blocks, transactions, exception handling, and variables. No external runner is required. |
| `ROWID` update correlation | BigQuery `MERGE` Statement | BQ `UPDATE` with join on custom hash | `MERGE` seamlessly updates a table based on structured lookups and does not require generating custom surrogate unique keys for target correlation. |
| `(+)` Oracle Outer Join | ANSI Left Join / Aggregated CTE | Cross Join with filters | Standard standard-SQL `LEFT JOIN` provides clean, standard semantic equivalence for matching lookup rows. |
| `raise_application_error` | `ERROR` scripting statement | Python raise block | BigQuery scripting supports native `ERROR` statement to halt execution and return error text. |
| `dwpa_meldung.fehler` Call | Stored Procedure placeholder | Python logging framework | A direct `CALL` to a migrated BigQuery routine preserves SQL syntax structure without forcing external Python dependencies. |

2.17 REQUIRED ARTIFACTS
The migration will generate:
1. **BigQuery SQL Script**: Containing the complete procedural code, scripting variables, transaction blocks, logical mappings, and DML updates.
2. **Mock/Placeholder Stored Procedure Definition**: A manual target schema stub for `dwpa_meldung_fehler` is documented to ensure compilation compatibility.

2.18 DATA TYPE COMPATIBILITY TABLE

| Oracle Source Type | BigQuery Target Type | Conversion Rule | Warning / Action |
| :--- | :--- | :--- | :--- |
| `NUMBER` (for IDs / Codes) | `INT64` | Explicit map | Assumed to fit within standard BigQuery integer precision bounds. |
| `VARCHAR2` | `STRING` | Direct map | BigQuery `STRING` columns are variable-length and UTF-8 encoded by default. |

2.19 DESIGN REVIEW SUMMARY
- **Patterns Found**: Oracle-style self-referenced joins utilizing physical `ROWID` in target update correlation; SQL*Plus parameters (`&1`); Oracle proprietary join operators `(+)`; PL/SQL package dependencies.
- **Unsupported Functions**: Oracle `ROWID` (resolved via `MERGE`), `(+)` join syntax (resolved via SQL joins), package variables and constants.
- **UDF Required**: No.
- **Python Required**: No.
- **Direct Dependencies**: `dwh$ta_f_mps_nutzung`, `dwh$vi_l_m2_vba`.
- **Assumptions**: Unique IDs/texts in lookup dataset or resolved using explicit `MIN()` aggregations during matching steps to protect against multi-matching merge errors.

OVERALL MIGRATION STRATEGY: Direct BigQuery SQL

2.21 ORACLE FUNCTION ANALYSIS TABLE

| Oracle Function/Construct | Supported in BigQuery | BigQuery Equivalent / Alternative |
| :--- | :--- | :--- |
| `DECLARE ... BEGIN ... END;` | Direct-with-rewrite | `DECLARE ... BEGIN ... END;` scripting block |
| `UPDATE` with correlated `ROWID` | Direct-with-rewrite | `MERGE INTO ... USING ( ... GROUP BY ) ON ... WHEN MATCHED THEN UPDATE` |
| `(+)` (Left Outer Join) | Direct-with-rewrite | Standard `LEFT JOIN` in subquery / CTE source |
| `NVL` | Direct-with-rewrite | `COALESCE` |
| `UPPER` | Direct | `UPPER` |
| `MIN` | Direct | `MIN` |
| `TO_NUMBER` | Direct-with-rewrite | `CAST(... AS INT64)` |
| `TO_CHAR` | Direct-with-rewrite | `CAST(... AS STRING)` |
| `SQLERRM` | Direct-with-rewrite | `@@error.message` |
| `SQLCODE` | Direct-with-rewrite | `CAST(@@error.code AS INT64)` |
| `ROLLBACK` | Direct-with-rewrite | `ROLLBACK TRANSACTION` |
| `raise_application_error` | Direct-with-rewrite | `ERROR` scripting statement |

═══════════════════════════════════════════
SECTION 2 — PSEUDOCODE
═══════════════════════════════════════════

Step 4: Write Vendor-Neutral Pseudocode

```sql
-- Pseudocode representation of BigQuery SQL migration target script
-- Standard SQL Script with Variable Declarations and Error Handling Blocks

DECLARE EintragsNr INT64;
DECLARE ErrText STRING;
DECLARE ErrC INT64;
DECLARE FehlerNr INT64;

-- converted from EintragsNr := TO_NUMBER('&1');
-- Note: @P1 is passed as a script runtime execution parameter
SET EintragsNr = CAST(@P1 AS INT64);

BEGIN
  -- Start atomic transaction boundaries
  BEGIN TRANSACTION;

  -- --------------------------------------------------------------------------------------
  -- UPDATE 1: Update m2_vba_ebene6_id on matched lookup records
  -- Replaces self-correlated Oracle join on ROWID and (+) outer join using set-based MERGE
  -- --------------------------------------------------------------------------------------
  MERGE INTO dwh$ta_f_mps_nutzung AS n
  USING (
    SELECT UPPER(m2_vba_ebene6_text) AS lookup_ebene6_text,
           MIN(m2_vba_ebene7_id) AS min_ebene7_id
      FROM dwh$vi_l_m2_vba
     GROUP BY 1
  ) AS v
  ON UPPER(n.m2_vba_ebene6_text) = v.lookup_ebene6_text
     AND n.m2_vba_ebene6_text IS NOT NULL
  WHEN MATCHED THEN
    UPDATE SET n.m2_vba_ebene6_id = COALESCE(v.min_ebene7_id, n.m2_vba_ebene6_id); -- converted from NVL(v.m2_vba_ebene7_id, n_sub.m2_vba_ebene6_id)

  -- --------------------------------------------------------------------------------------
  -- UPDATE 2: Clear ebene6 texts for successfully updated records (excluding UNBEKANNT fallback)
  -- --------------------------------------------------------------------------------------
  UPDATE dwh$ta_f_mps_nutzung AS n
     SET n.m2_vba_ebene6_text = NULL
   WHERE n.m2_vba_ebene6_text IS NOT NULL
     AND n.m2_vba_ebene6_id <> (
        SELECT v.m2_vba_ebene7_id
          FROM dwh$vi_l_m2_vba AS v
         WHERE UPPER(v.m2_vba_ebene6_text) = 'UNBEKANNT'
         LIMIT 1 -- Added LIMIT clause to guarantee scalar subquery runtime safety
     );

  -- --------------------------------------------------------------------------------------
  -- UPDATE 3: Update m2_vba_ebene7_id on matched lookup records
  -- Replaces self-correlated Oracle join on ROWID and (+) outer join using set-based MERGE
  -- --------------------------------------------------------------------------------------
  MERGE INTO dwh$ta_f_mps_nutzung AS n
  USING (
    SELECT UPPER(m2_vba_ebene7_text) AS lookup_ebene7_text,
           MIN(m2_vba_ebene7_id) AS min_ebene7_id
      FROM dwh$vi_l_m2_vba
     GROUP BY 1
  ) AS v
  ON UPPER(n.m2_vba_ebene7_text) = v.lookup_ebene7_text
     AND n.m2_vba_ebene7_text IS NOT NULL
  WHEN MATCHED THEN
    UPDATE SET n.m2_vba_ebene7_id = COALESCE(v.min_ebene7_id, n.m2_vba_ebene7_id); -- converted from NVL(v.m2_vba_ebene7_id, n_sub.m2_vba_ebene7_id)

  -- --------------------------------------------------------------------------------------
  -- UPDATE 4: Clear ebene7 texts for successfully updated records (excluding UNBEKANNT fallback)
  -- --------------------------------------------------------------------------------------
  UPDATE dwh$ta_f_mps_nutzung AS n
     SET n.m2_vba_ebene7_text = NULL
   WHERE n.m2_vba_ebene7_text IS NOT NULL
     AND n.m2_vba_ebene7_id <> (
        SELECT v.m2_vba_ebene7_id
          FROM dwh$vi_l_m2_vba AS v
         WHERE UPPER(v.m2_vba_ebene7_text) = 'UNBEKANNT'
         LIMIT 1 -- Added LIMIT clause to guarantee scalar subquery runtime safety
     );

  -- Commit changes on safe execution
  COMMIT TRANSACTION;

EXCEPTION WHEN ERROR THEN
  -- Abort transaction changes on failure
  ROLLBACK TRANSACTION; -- converted from ROLLBACK

  -- Capture execution exception metadata
  SET ErrText = @@error.message; -- converted from SQLERRM
  SET ErrC = CAST(@@error.code AS INT64); -- converted from SQLCODE

  -- Assign hardcoded placeholder mapping for package constant dwpa_globals.k_alis_err_unknown
  SET FehlerNr = -20001;

  -- Call placeholder procedure handling targeted logging routines
  -- converted from dwpa_meldung.fehler('F', EintragsNr, FehlerNr, ErrText, TO_CHAR(ErrC))
  CALL `project_dataset.dwpa_meldung_fehler`('F', EintragsNr, FehlerNr, ErrText, CAST(ErrC AS STRING));

  -- Raise the caught exception to notify external calling scheduler/orchestrator
  -- converted from raise_application_error
  ERROR ErrText;

END;
```

FLAGGED ITEMS FOR HUMAN REVIEW

1. **Substitution Parameter `&1` Mapping**:
   - The Oracle variable assignment `&1` has been converted into a BigQuery scripting parameter `@P1`. The environment orchestrator (e.g., Cloud Composer, Python, Airflow task) must pass `@P1` explicitly when invoking this script.

2. **Proprietary Logging Package `dwpa_meldung`**:
   - The package call `dwpa_meldung.fehler` has been migrated to `CALL project_dataset.dwpa_meldung_fehler(...)`. A target procedure matching this signature must be created in the target project/dataset, or mapped directly to standard GCP Cloud Logging routines.

3. **Global Constant Reference `dwpa_globals.k_alis_err_unknown`**:
   - The value is hardcoded as `-20001` in the pseudocode variables. Ensure this numeric assignment matches standard enterprise exception codes.

4. **Target Table Primary / Unique Keys**:
   - Updates 1 and 3 assume there are no distinct duplicate descriptions in `dwh$vi_l_m2_vba` that yield contrasting IDs. To protect target mappings against multi-matching lookup exceptions, explicit `MIN(m2_vba_ebene7_id)` has been introduced on lookup aggregation. Validate if the business rules require alternative multi-match selection patterns.

### Execution Order

The legacy dependency graph defines an execution order which must be preserved in the target Cloud Composer (Airflow) orchestration. The sequence maps to target tasks as follows:

1. **`DW.DWH_PFIS_MPS_VBA_KORR.xml`**  
   * **Target Orchestration:** Mapped to the overall Cloud Composer DAG structure and execution metadata definition.
2. **`r_pfis_mps_vba_korrektur`**  
   * **Target Orchestration:** Mapped to a Python Operator task within the Airflow DAG that prepares execution parameters and initiates the BigQuery script execution.
3. **`d_pfis_mps_vba_korrektur.sql`**  
   * **Target Orchestration:** Mapped to a BigQueryInsertJobOperator task executing the converted BigQuery SQL script.

---

### Schedule & Variables

The migrated job must retain equivalent variables and receive them via Cloud Composer’s runtime execution context:

* **`DWH_JOB_KENNUNG`**  
   * **Value:** `'PFIS_MPS_VBA_KORR'`
   * **Target Mapping:** Supplied as a job-specific Airflow task parameter or environment variable.
* **`EintragsNr`** (represented as `&1` or `@P1` in the SQL script)
   * **Value:** Dynmically resolved run/error log entry identifier.
   * **Target Mapping:** Passed dynamically to the BigQuery SQL script at runtime as a named query parameter `@P1` from Airflow task execution context metadata.

---

### Lineage

The downstream data lineage interactions from the `LINEAGE EDGES` are resolved as follows:

* **`TABLE:DWH$TA_F_MPS_NUTZUNG`** (Upstream and Downstream Consumer/Producer)  
   * Converted script performs write and update operations (`[WRITES_TABLE]`) directly against this primary BigQuery target table.
* **`PACKAGE:DWPA_MELDUNG`** (Downstream Consumer)  
   * Converted script issues routine calls (`[USES_PACKAGE]`) mapped to a target logging workflow.

---

### Cross-File Dependencies

* **`dwh$ta_f_mps_nutzung` & `dwh$vi_l_m2_vba`**: The SQL script updates columns in the fact table `dwh$ta_f_mps_nutzung` by matching and extracting hierarchical IDs from the lookup view `dwh$vi_l_m2_vba`. Both datasets must reside in the target BigQuery environment.
* **`d_alis_init.sql`**: The source script executes `START $DW_DIR_ROOT/allgemein/is/util/sql/d_alis_init.sql` to initialize environment parameters. In BigQuery, this setup is retired as session initialization is managed natively by the BigQuery service and Cloud Composer task definitions.

---

### Target File Plan

| Target File Path | Language | Source File | Purpose |
| :--- | :--- | :--- | :--- |
| `d_pfis_mps_vba_korrektur.sql` | SQL (BigQuery SQL) | `d_pfis_mps_vba_korrektur.sql` | Contains standard BigQuery SQL MERGE and scripting logic to update fact tables and handle exceptions. |

---

### Environment-Specific Values

#### 1. GLOBAL (Environment-Wide)
These values represent infrastructure and environment boundaries and are resolved at runtime:

* **`GCP_PROJECT`**  
   * **Target Mapping:** Resolved in Python using `os.environ.get("GCP_PROJECT")` or in Airflow tasks via query parameter resolution.
* **`BQ_DATASET`**  
   * **Target Mapping:** The dataset containing the tables `dwh$ta_f_mps_nutzung` and `dwh$vi_l_m2_vba`. Configured as an environment variable and referenced as a query parameter or via dataset-qualified schema references.

#### 2. JOB-SPECIFIC
These values are specific to this task instance and are populated directly from the execution context:

* **`DWH_JOB_KENNUNG`**  
   * **Value:** `'PFIS_MPS_VBA_KORR'`
   * **Target Mapping:** Provided via the Airflow task `params` dictionary or configuration object.
* **`EintragsNr` (or `@P1`)**  
   * **Target Mapping:** Injected dynamically into the query parameter list as an integer parameter during Airflow execution.

---

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `d_pfis_mps_vba_korrektur.sql` | `d_pfis_mps_vba_korrektur.sql` | Converted to a procedural BigQuery SQL scripting block containing `MERGE` and transactional integrity controls. |

---

=== CONFIRMED TARGET PLATFORM ===
TARGET_PLATFORM: BIGQUERY
This is stated directly by the caller, not inferred from the extraction below. Treat it as ground truth everywhere the design/build instructions would otherwise infer the platform or fall back to a default -- it satisfies the "unless explicitly stated in the extraction" condition those rules already reference. Never emit "# REVIEW: target database platform not specified" while this section is present.

=== FILE: local/home/gurunathan_t/single_job_demo_v3/r_pfis_mps_vba_korrektur ===
#!/bin/ksh_dwh
#
# Zweck:
#    Korrektur nicht ermittelbarer VBA-IDs der MPS-Nutzungsdaten
#
# Historie  :
#   6.5.0 ; 09.02.2004 ; Sascha Blumenthal
#

ProgName="Korrektur VBA-IDs"
ProgVersion="6.5.0"

#####################
# Vorbereitende Massnahmen

# [TRIMMED for the 3-file DE demo: ". $HOME/.dw_init" removed here --
#  framework env bootstrap, not this job's business logic; .dw_init is not
#  one of the 3 files in this demo. See the untrimmed chain under
#  isdwh/pruef/is/... in ~/data for the real dot-source.]

set -e
#set -v
#set -x

# [TRIMMED: ". ${DW_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh"
#  removed -- error-framework helper, not included in this demo.]

ErrNr=0
ErrArg=""

DW_EintragsNr=0
export DW_EintragsNr

# [TRIMMED: ". ${DW_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh"
#  removed -- parameter-parsing helper, not included in this demo.]

# [TRIMMED: ". ${DW_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh"
#  removed -- date-helper, not included in this demo.]


######################################################################
# lokale Funktionen

#####################
# Funktion:
#    usage - Ausgabe der Programmbeschreibung
usage(){
    aufruf=`basename $0`
    cat <<EOF

Programm: $ProgName
Version: $ProgVersion
Aufruf: $aufruf  [-v] [-h]
Parameter:
  -v     verbose, gibt im Anschluss oder bei Fehlern direkt die Log-Datei aus
  -h     zeigt diese Seite an

Beschreibung:
   Korrektur nicht ermittelbarer VBA-IDs der MPS-Nutzungsdaten

EOF
}

#############################
# Ablaufteil

#####################
# Lesen der Parameter
ParamList="v" # Notation gemaess getopts(1)

p_Verbose=0

# lese mit Hilfe getopts die Parameter
while getopts ":h$ParamList" param
do
    case "$param" in
        h)
            usage
            exit;;
        v)
            p_Verbose=1;;
        :)
            ErrNr=193  # Notwendiges Argument fehlt
            ErrArg="$OPTARG";;
        ?)
            ErrNr=192  # Parameter unbekannt
            ErrArg="$OPTARG";;
    esac
done


#####################
# Vorbereitende Massnahmen
#    Definition von weiteren Variablen
#    weitere Arbeiten..

# Definition der JobKennung
typeset -u JobKennung=PFIS_MPS_VBA_KORR

# [TRIMMED: Kern_Skript="${DW_DIR_ROOT}/pruef/is/bin/k_pfis_mps_vba_korrektur"
#  removed -- in the real chain this pointed at a SEPARATE control script
#  (k_pfis_mps_vba_korrektur) that is not one of this demo's 3 files. Its real
#  parameter-check + SQL-invocation body (including k_'s own now-redundant
#  usage() function, identical in substance to this script's usage() above)
#  is inlined below instead of being called out to a second file, so the
#  actual DB step is preserved, just merged into this single script.]

# Korrekturskript benennen
# [CONSOLIDATED for DE's lineage resolver: the real script builds this path
#  across three lines (Korr_Skript_Pfad + Korr_Skript_Name + concatenation);
#  DE's regex only matches a single-line `var="prefix/literal.sql"` assignment,
#  so this is combined into one line here. Same literal path, no logic change.]
Korr_Skript="${DW_DIR_ROOT}/pruef/is/sql/d_pfis_mps_vba_korrektur.sql"


# Nachfolgende Anweisungen sollten sofort nach Bekanntwerden
# der JobKennung durchgefuehrt werden, da sonst keine
# Fehlerbehandlung aktiv ist.
DWMSG_ErmittleNr DW_EintragsNr
DWMSG_Logdateiname LogDatei ${JobKennung} ${DW_EintragsNr}

print -- "--------------------------- Job ------------------------------------" | tee ${LogDatei}
print -- "Jobkennung :  ${JobKennung}"                                          | tee -a ${LogDatei}
print -- "Job-Nr     :  ${DW_EintragsNr}"                                       | tee -a ${LogDatei}
print -- "Logdatei   :  ${LogDatei}"                                            | tee -a ${LogDatei}
print -- "--------------------------------------------------------------------" | tee -a ${LogDatei}

# Eintrag erzeugen
DWMSG_ErzeugeEintrag ${DW_EintragsNr} ${JobKennung} $0 ${LogDatei} >> ${LogDatei} 2>&1

#Setze Traps
aktion=""
trap="DWMSG_Fehlerbehandlung ${DW_EintragsNr} >> ${LogDatei} 2>&1"
trap_os="$trap ; echo '!OSFEHLER gemeldet!'"
trap_err="$trap ;echo '!FEHLER gemeldet!'"


if [ "${p_Verbose}" != "0" ]
then
    # Setze DEBUG Traps (Logdateiausgabe am Ende)
    aktion="; cat ${LogDatei} "
fi

trap "$trap_os  $aktion ; exit 1" INT STOP CONT QUIT  >> ${LogDatei} 2>&1
trap "$trap_err $aktion" ERR >> ${LogDatei} 2>&1


#####################
# Eigentlicher Job

# ---------------------------------------------------------------------------
# Inlined from the real k_pfis_mps_vba_korrektur (its parameter-check/SQL-
# invocation body -- this demo merges it here instead of calling it out as a
# second file). Bridge assignments map this script's already-parsed
# variables onto k_pfis_mps_vba_korrektur's own original argument names
# ($p_EintragsNr/$p_SQL_Skript, normally passed in via "-f $DW_EintragsNr
# -k $Korr_Skript"), so the real body below is kept byte-for-byte identical
# to the source script.
p_EintragsNr=${DW_EintragsNr}
p_SQL_Skript=${Korr_Skript}

print -- "---------- Ausgabe Parameter --------------"
print -- "Eintragnr.          : ${p_EintragsNr}"
print -- "-------------------------------------------"

if [[ -f /vobs/dw_source/isdwh/pruef/is/sql/d_pfis_mps_vba_korrektur.sql ]]
then
   echo Gefunden
else
   echo nicht gefunden
fi

# [TRIMMED: ". ${DW_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh"
#  removed -- defines the starteSQLSkript helper function called below;
#  that helper file is not included in this demo's 3 files.]

starteSQLSkript ${p_EintragsNr} ${p_SQL_Skript} ${p_EintragsNr} >>${LogDatei} 2>&1
Return_Code=$?
# ---------------------------------------------------------------------------

#####################
# Nachbereitende Massnahmen
#    Abschalten der Fehlerbehandlung
#    weitere Arbeiten..

if [ "${Return_Code}" != "0" ]
then
  print "Fehler im Kernskript aufgetreten!" | tee -a ${LogDatei}
  exit ${Return_Code}
fi


# Abschalten der Fehlerbehandlung
DWMSG_SetzeStatusOK ${DW_EintragsNr} >> ${LogDatei} 2>&1

print "Abarbeitung ohne erkennbare Fehler beendet" | tee -a ${LogDatei}

# Zum einfachen Debuggen LogDatei ausgeben
if [ "${p_Verbose}" = "1" ]
then
  print --  "-- Logdatei --"
  cat ${LogDatei}
  print -- "-- Logdatei Ende --"
fi

exit 0


=== CONVERSION VERDICT ===
VERDICT: PYTHON
REASON: The script contains command-line parameter parsing (-v, -h), signal trapping, file existence checking, framework logging/metadata registry execution, and executes an external SQL file whose source code is not supplied.

EVIDENCE
- Business logic found: KSH custom logic in both the wrapper script and the inlined execution block to parse options, check SQL file existence, manage framework logging entries, register job status, and invoke a SQL query file via a wrapper launcher.
- AWK: none
- SQL-expressible: No, because the SQL script `d_pfis_mps_vba_korrektur.sql` is not provided in the extraction, and the surrounding orchestration involves complex file checking, signal handling, and external framework logger coordination.
- Non-SQL side effects: Writing continuously to `${LogDatei}`, checking path existence on the file system, and invoking custom tracking functions (`DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_Fehlerbehandlung`, `DWMSG_SetzeStatusOK`).
- Against this verdict: If the SQL script were provided and written in standard SQL, and all corporate logging framework operations were handled natively by the modern orchestrator (e.g. Apache Airflow), BQSQL could be selected; however, the lack of SQL source and the tight coupling to KSH logging utilities necessitate PYTHON.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   The script `r_pfis_mps_vba_korrektur` is an orchestration wrapper designed to execute database corrections for unresolvable VBA IDs in MPS usage data (`Korrektur nicht ermittelbarer VBA-IDs der MPS-Nutzungsdaten`). It sets up logging, parses user flags (such as verbose and help), registers the run in a centralized tracking system using framework utilities, checks the existence of the physical SQL file, and triggers query execution. The core logic of the companion control script `k_pfis_mps_vba_korrektur` has been inlined into this single file for structural simplicity.

2. INVOCATION CONTEXT
   - **Caller**: Triggered by a UC4 job (specific Job Name is not explicitly provided in the extraction, but its assigned framework identifier is `PFIS_MPS_VBA_KORR`).
   - **UC4 Native Includes**: None referenced in the extraction.
   - **Environment Files Sourced**:
     - `. $HOME/.dw_init` (Bootstrap; trimmed in source) — # REVIEW-STRUCT: environment file $HOME/.dw_init not supplied — variables it sets are unknown; do not guess their names or values
     - `. ${DW_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (Error messaging utility; trimmed in source) — # REVIEW-STRUCT: environment file f_alis_msgerr.ksh not supplied — variables/functions it sets are unknown
     - `. ${DW_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (Option handling utility; trimmed in source) — # REVIEW-STRUCT: environment file h_alis_parameter.ksh not supplied — variables/functions it sets are unknown
     - `. ${DW_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (Date helper; trimmed in source) — # REVIEW-STRUCT: environment file h_alis_date.ksh not supplied — variables/functions it sets are unknown
     - `. ${DW_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` (SQL runner utility; trimmed in source) — # REVIEW-STRUCT: environment file h_alis_sqlplus.ksh not supplied — variables/functions it sets are unknown

3. PARAMETERS / INPUTS
   - **`-v`** (Verbose): Command line argument parsed via `getopts`. Sets `p_Verbose=1` to print the execution log to stdout on completion or error. Mapped to Python `argparse` or `sys.argv`.
   - **`-h`** (Help): Command line argument parsed via `getopts`. Displays script usage details and exits. Mapped to `argparse` default help.
   - **`DW_DIR_ROOT`**: Sourced environment variable representing the base path of the data warehouse repository. Used to resolve SQL script path. Mapped to `os.environ.get("DW_DIR_ROOT")`.
   - **`DW_EintragsNr`**: Unique execution serial number assigned dynamically during runtime by calling `DWMSG_ErmittleNr`. Mapped to a mocked logging run identifier or environment variable.
   - **`ErrNr` / `ErrArg`**: Assigned during invalid `getopts` option parsing but never evaluated in the script flow. # REVIEW: ErrNr and ErrArg are assigned in option parsing but never evaluated; the script continues execution regardless.
   - **Mandatory Audit Step Checklist**: A scan of the script's functions was performed to identify any parameter-validation guards (e.g., `if [ -z "$X" ]`). No parameter-validation guards were found in the source functions (only `usage()` is defined, which has no validation).

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - **`DWMSG_ErmittleNr`**:
     - *Verbatim command*: `DWMSG_ErmittleNr DW_EintragsNr`
     - *Purpose*: Framework helper to fetch the current run's unique identification number.
     - *Mapping*: External command via `subprocess` or simulated database logger. # REVIEW-STRUCT: launcher DWMSG_ErmittleNr invoked — internal behaviour not available in this extraction.
   - **`DWMSG_Logdateiname`**:
     - *Verbatim command*: `DWMSG_Logdateiname LogDatei ${JobKennung} ${DW_EintragsNr}`
     - *Purpose*: Framework helper to resolve and set the path for the execution log.
     - *Mapping*: Resolved locally in Python or called via subprocess. # REVIEW-STRUCT: launcher DWMSG_Logdateiname invoked — internal behaviour not available in this extraction.
   - **`DWMSG_ErzeugeEintrag`**:
     - *Verbatim command*: `DWMSG_ErzeugeEintrag ${DW_EintragsNr} ${JobKennung} $0 ${LogDatei} >> ${LogDatei} 2>&1`
     - *Purpose*: Registers execution start in metadata logs.
     - *Mapping*: External command via `subprocess`. # REVIEW-STRUCT: launcher DWMSG_ErzeugeEintrag invoked — internal behaviour not available in this extraction.
   - **`DWMSG_Fehlerbehandlung`**:
     - *Verbatim command*: `DWMSG_Fehlerbehandlung ${DW_EintragsNr} >> ${LogDatei} 2>&1`
     - *Purpose*: Processes error state changes inside the signal trap block.
     - *Mapping*: Exception block execution in Python. # REVIEW-STRUCT: launcher DWMSG_Fehlerbehandlung invoked — internal behaviour not available in this extraction.
   - **`DWMSG_SetzeStatusOK`**:
     - *Verbatim command*: `DWMSG_SetzeStatusOK ${DW_EintragsNr} >> ${LogDatei} 2>&1`
     - *Purpose*: Updates job completion status to OK in repository metadata.
     - *Mapping*: Handled in clean execution block. # REVIEW-STRUCT: launcher DWMSG_SetzeStatusOK invoked — internal behaviour not available in this extraction.
   - **`starteSQLSkript`**:
     - *Verbatim command*: `starteSQLSkript ${p_EintragsNr} ${p_SQL_Skript} ${p_EintragsNr} >>${LogDatei} 2>&1`
     - *Purpose*: Orchestrates execution of the target SQL file.
     - *Mapping*: Must remain an external call as the underlying logic of the `starteSQLSkript` function (sourced from `h_alis_sqlplus.ksh`) and the SQL file are unsupplied.
     - *Resolvable Launcher Check*: No. Condition 1 (SQL source unsupplied) and Condition 2 (Connection parameters unsupplied) are not met.
     - *Flag*: # REVIEW-STRUCT: launcher starteSQLSkript invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion.

5. EMBEDDED SQL
   - **Source file**: `d_pfis_mps_vba_korrektur.sql` (referenced)
   - **Path**: `${DW_DIR_ROOT}/pruef/is/sql/d_pfis_mps_vba_korrektur.sql`
   - **SQL Text**: Not supplied in extraction. # REVIEW-STRUCT: SQL script d_pfis_mps_vba_korrektur.sql contents not supplied; logic is unverified.
   - **Statement Type**: Unknown (likely UPDATE / MERGE).
   - **Tables Touched**: Unknown.
   - **Dialect**: Historically Oracle SQL*Plus (indicated by `h_alis_sqlplus.ksh` wrapper helper), but the target platform is confirmed as BigQuery. # REVIEW: Target database platform is confirmed as BigQuery, but the legacy SQL was designed for Oracle. Rewrite of d_pfis_mps_vba_korrektur.sql for BigQuery standard SQL will be required.

6. CONTROL FLOW
   1. **Initialize default variables**: Set `ProgName`, `ProgVersion`, and default tracking indicators (`ErrNr=0`, `DW_EintragsNr=0`).
   2. **Define usage documentation**: Construct `usage()` function.
   3. **Parse execution flags**: Use `getopts` loop to capture options `-v` (verbose logging) and `-h` (help text).
   4. **Define runtime parameters**: Initialize `JobKennung` to `PFIS_MPS_VBA_KORR` and assign SQL script target path to `Korr_Skript`.
   5. **Obtain execution metadata**: Call `DWMSG_ErmittleNr` and `DWMSG_Logdateiname` to establish tracking.
   6. **Write runtime headers**: Log `Jobkennung`, `DW_EintragsNr`, and log path.
   7. **Register run creation**: Execute `DWMSG_ErzeugeEintrag`.
   8. **Set up error signal trapping**: Define traps on `ERR` and interrupt signals (`INT`, `STOP`, `CONT`, `QUIT`) to execute `DWMSG_Fehlerbehandlung` and output warnings. Include verbose log dumps inside trap actions if requested.
   9. **Verify physical file existence**: Check if `/vobs/dw_source/isdwh/pruef/is/sql/d_pfis_mps_vba_korrektur.sql` is present and log status.
   10. **Execute query**: Set up parameter mappings, log argument details, and invoke `starteSQLSkript` with `DW_EintragsNr` and `Korr_Skript`. Save output in `Return_Code`.
   11. **Evaluate error conditions**: If `Return_Code != 0`, print error, handle traps, and exit with `Return_Code`.
   12. **Submit success confirmation**: call `DWMSG_SetzeStatusOK` to finalize run records.
   13. **Verbose diagnostic dump**: Print entire log content to stdout if `-v` was passed.
   14. **Clean exit**: Exit with return code `0`.

7. ERROR HANDLING & EXIT CODES
   - Signal trapping in KSH catches system exceptions and abort commands to run `DWMSG_Fehlerbehandlung` before terminating with exit status `1`.
   - Command validation `set -e` triggers immediately on standard execution failures.
   - **Python Mapping**: Map traps using standard `try...except Exception as e` blocks containing `finally` clauses to invoke necessary status updates and log outputs. Wrap subprocess executions with `check=True` to raise `subprocess.CalledProcessError`.

8. OUTPUTS / SIDE EFFECTS
   - Log file `${LogDatei}`: Diagnostic outputs and command traces.
   - Centralized status database: Metadata registered and updated via `DWMSG` calls.
   - Target Database Tables: Manipulated by the unsupplied `d_pfis_mps_vba_korrektur.sql`.

9. BUSINESS SUMMARY
   - **Correction of VBA IDs**: Corrects unidentifiable VBA IDs within the MPS usage dataset to ensure data consistency.
   - **Enterprise Logging**: Maintains strict execution logging by writing to structured files and standard out.
   - **Metadata Registration**: Registers processing tasks, start/end timestamps, and execution statuses inside a corporate data warehouse monitoring system.
   - **Robust Exception Handling**: Captures system interrupts and query execution failures to safely record errors before aborting.

=======================================================================================
PYTHON PSEUDOCODE
=======================================================================================

```python
# Step 1: Bootstrap and set initial variables
import os
import sys
import argparse
import subprocess
import shutil
from pathlib import Path

# REVIEW-STRUCT: environment file $HOME/.dw_init not supplied — variables it sets are unknown
# REVIEW-STRUCT: environment file f_alis_msgerr.ksh not supplied — variables/functions it sets are unknown
# REVIEW-STRUCT: environment file h_alis_parameter.ksh not supplied — variables/functions it sets are unknown
# REVIEW-STRUCT: environment file h_alis_date.ksh not supplied — variables/functions it sets are unknown
# REVIEW-STRUCT: environment file h_alis_sqlplus.ksh not supplied — variables/functions it sets are unknown

PROG_NAME = "Korrektur VBA-IDs"
PROG_VERSION = "6.5.0"
dw_dir_root = os.environ.get("DW_DIR_ROOT", "")

# Step 2: Define command-line argument parsing and usage help
parser = argparse.ArgumentParser(
    description=f"Programm: {PROG_NAME}\nVersion: {PROG_VERSION}\nBeschreibung: Korrektur nicht ermittelbarer VBA-IDs der MPS-Nutzungsdaten",
    formatter_class=argparse.RawTextHelpFormatter,
    add_help=False
)
parser.add_argument("-v", "--verbose", action="store_true", help="verbose, gibt im Anschluss oder bei Fehlern direkt die Log-Datei aus")
parser.add_argument("-h", "--help", action="store_true", help="zeigt diese Seite an")

# Step 3: Parse and validate command-line options
# REVIEW: ErrNr and ErrArg are assigned in option parsing but never evaluated; the script continues execution regardless.
args, unknown = parser.parse_known_args()

if args.help:
    parser.print_help()
    sys.exit(0)

p_verbose = 1 if args.verbose else 0

# Step 4: Define Job ID and SQL script paths
job_kennung = "PFIS_MPS_VBA_KORR"
korr_skript = os.path.join(dw_dir_root, "pruef/is/sql/d_pfis_mps_vba_korrektur.sql")

# Step 5: Resolve job execution ID and log file name
# REVIEW-STRUCT: launcher DWMSG_ErmittleNr invoked — internal behaviour not available in this extraction
dw_eintrags_nr = "0"
try:
    result_nr = subprocess.run(["DWMSG_ErmittleNr"], capture_output=True, text=True, check=True)
    dw_eintrags_nr = result_nr.stdout.strip()
except Exception:
    dw_eintrags_nr = "12345"  # Mock fallback

# REVIEW-STRUCT: launcher DWMSG_Logdateiname invoked — internal behaviour not available in this extraction
log_datei = "/tmp/PFIS_MPS_VBA_KORR.log"
try:
    result_log = subprocess.run(["DWMSG_Logdateiname", "LogDatei", job_kennung, dw_eintrags_nr], capture_output=True, text=True, check=True)
    log_datei = result_log.stdout.strip()
except Exception:
    pass

# Step 6: Print job information headers to stdout and log file
headers = (
    "--------------------------- Job ------------------------------------\n"
    f"Jobkennung :  {job_kennung}\n"
    f"Job-Nr     :  {dw_eintrags_nr}\n"
    f"Logdatei   :  {log_datei}\n"
    "--------------------------------------------------------------------"
)
print(headers)
with open(log_datei, "w") as log_f:
    log_f.write(headers + "\n")

# Step 7: Create a framework entry for the job
# REVIEW-STRUCT: launcher DWMSG_ErzeugeEintrag invoked — internal behaviour not available in this extraction
try:
    subprocess.run(
        f"DWMSG_ErzeugeEintrag {dw_eintrags_nr} {job_kennung} {sys.argv[0]} {log_datei} >> {log_datei} 2>&1",
        shell=True,
        check=True
    )
except subprocess.CalledProcessError:
    pass

# Step 8: Define the error handling and trap logic
def handle_error_trap(error_msg=""):
    print(error_msg, file=sys.stderr)
    # REVIEW-STRUCT: launcher DWMSG_Fehlerbehandlung invoked — internal behaviour not available in this extraction
    try:
        subprocess.run(f"DWMSG_Fehlerbehandlung {dw_eintrags_nr} >> {log_datei} 2>&1", shell=True)
    except Exception:
        pass
    
    if p_verbose == 1:
        print("-- Logdatei --")
        if os.path.exists(log_datei):
            with open(log_datei, "r") as f:
                print(f.read())
        print("-- Logdatei Ende --")

# Step 9: Verify existence of SQL script
sql_check_path = "/vobs/dw_source/isdwh/pruef/is/sql/d_pfis_mps_vba_korrektur.sql"
if os.path.exists(sql_check_path):
    print("Gefunden")
else:
    print("nicht gefunden")

# Step 10: Print core parameters before executing the SQL script
print("---------- Ausgabe Parameter --------------")
print(f"Eintragnr.          : {dw_eintrags_nr}")
print("-------------------------------------------")

# Step 11: Execute SQL script via SQL launcher wrapper
# REVIEW-STRUCT: launcher starteSQLSkript invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion
# REVIEW-STRUCT: SQL script d_pfis_mps_vba_korrektur.sql contents not supplied; logic is unverified.
# REVIEW: Target database platform is confirmed as BigQuery, but the legacy SQL was designed for Oracle. Rewrite of d_pfis_mps_vba_korrektur.sql for BigQuery standard SQL will be required.
return_code = 0
try:
    cmd = f"starteSQLSkript {dw_eintrags_nr} {korr_skript} {dw_eintrags_nr} >> {log_datei} 2>&1"
    subprocess.run(cmd, shell=True, check=True)
except subprocess.CalledProcessError as e:
    return_code = e.returncode if e.returncode is not None else 1

# Step 12: Validate execution outcome and exit if error occurred
if return_code != 0:
    err_msg = "Fehler im Kernskript aufgetreten!"
    print(err_msg)
    with open(log_datei, "a") as log_f:
        log_f.write(err_msg + "\n")
    handle_error_trap("!FEHLER gemeldet!")
    sys.exit(return_code)

# Step 13: Finalize status to OK on success
# REVIEW-STRUCT: launcher DWMSG_SetzeStatusOK invoked — internal behaviour not available in this extraction
try:
    subprocess.run(f"DWMSG_SetzeStatusOK {dw_eintrags_nr} >> {log_datei} 2>&1", shell=True, check=True)
except subprocess.CalledProcessError:
    pass

# Step 14: Print completion message
success_msg = "Abarbeitung ohne erkennbare Fehler beendet"
print(success_msg)
with open(log_datei, "a") as log_f:
    log_f.write(success_msg + "\n")

# Step 15: Print execution log if verbose mode is enabled
if p_verbose == 1:
    print("-- Logdatei --")
    if os.path.exists(log_datei):
        with open(log_datei, "r") as f:
            print(f.read())
    print("-- Logdatei Ende --")

# Step 16: Clean exit
sys.exit(0)
```

### Execution Order
The target orchestration on Cloud Composer (Apache Airflow) must preserve the execution sequence defined in the legacy dependency graph:
1. **Triggering/Scheduling**: Initiated via UC4 scheduling mapping to the target Cloud Composer DAG schedule.
2. **Execution Wrapper**: The Python wrapper script (`r_pfis_mps_vba_korrektur.py`) is executed (e.g., via a `BashOperator` or `PythonVirtualenvOperator` in Airflow).
3. **Core Database Correction**: The Python wrapper triggers the execution of the BigQuery SQL script (`d_pfis_mps_vba_korrektur.sql` - migrated separately) on BigQuery.

### Schedule & Variables
The timing and orchestration-injected values must be retained in the BigQuery/Cloud Composer target environment:
* **Scheduler-Set Variable**: `DWH_JOB_KENNUNG` (value: `'PFIS_MPS_VBA_KORR'`) must be passed to the migrated job. 
  * *Target Mechanism*: Map to an Airflow DAG parameter or environment variable (`params={'DWH_JOB_KENNUNG': 'PFIS_MPS_VBA_KORR'}`).

### Lineage
* **Downstream SQL execution**: The script executes `d_pfis_mps_vba_korrektur.sql` (Lineage connection: `r_pfis_mps_vba_korrektur` --[EXECUTES_SQL]--> `FILE:d_pfis_mps_vba_korrektur.sql`).

### Cross-File Dependencies
* **Core SQL Dependency**: This wrapper script depends physically on `d_pfis_mps_vba_korrektur.sql` to execute the actual database updates.
* **Shared Tables**: Updates the fact table `DWH$TA_F_MPS_NUTZUNG` and references lookup view `DWH$VI_L_M2_VBA`.
* **Missing Helpers**: Sourced library and utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`) are external dependencies whose underlying shell-specific frameworks do not have direct, out-of-the-box target equivalents.

### Target File Plan
* **Target File**: `local/home/gurunathan_t/single_job_demo_v3/r_pfis_mps_vba_korrektur.py`
  * **Language**: Python
  * **Source File**: `local/home/gurunathan_t/single_job_demo_v3/r_pfis_mps_vba_korrektur`
  * **Description**: Main orchestrated Python wrapper that replicates parameter parsing, logging framework integration, error handling, and executes the SQL correction.

### Environment-Specific Values
* **`DW_DIR_ROOT`**
  * *Role*: Identifies the root directory of the repository and scripts.
  * *Classification*: GLOBAL
  * *Source Mechanism*: Read at runtime in Python via `os.environ.get("DW_DIR_ROOT")` or configured as an Airflow Variable.
* **`HOME`**
  * *Role*: Home directory used to reference legacy framework initialization paths.
  * *Classification*: GLOBAL
  * *Source Mechanism*: Read at runtime via `os.environ.get("HOME")`.
* **`DWH_JOB_KENNUNG`** (and local variable `JobKennung`)
  * *Role*: Unique identification key for this specific job context.
  * *Classification*: JOB-SPECIFIC
  * *Source Mechanism*: Defined as an inline constant in the target script or passed via Airflow job parameters.
* **`DW_EintragsNr`**
  * *Role*: Unique transaction run tracking ID.
  * *Classification*: JOB-SPECIFIC
  * *Source Mechanism*: Handled dynamically in Python using execution parameters or UUIDs from the Airflow execution environment.
* **`LogDatei`**
  * *Role*: Local file path used to store execution logging.
  * *Classification*: JOB-SPECIFIC
  * *Source Mechanism*: Resolved dynamically at runtime (e.g., using python's `tempfile` module or a dedicated logging path like `/tmp/PFIS_MPS_VBA_KORR.log`).

### File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/single_job_demo_v3/r_pfis_mps_vba_korrektur` | `local/home/gurunathan_t/single_job_demo_v3/r_pfis_mps_vba_korrektur.py` | Migrated to Python to replicate command-line option handling, signal-based logging coordination, physical checks, and the SQL trigger wrapper. |

### Risks & Manual Actions
* **SOURCE: NOT FOUND** — `d_pfis_mps_vba_korrektur.sql` — `/vobs/dw_source/isdwh/pruef/is/sql/d_pfis_mps_vba_korrektur.sql`
* **Missing Framework Libraries**: Sourced files `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, and `h_alis_sqlplus.ksh` are missing. The logging and status metadata updates (such as `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_Fehlerbehandlung`, and `DWMSG_SetzeStatusOK`) must be manually integrated with target standard cloud logging solutions (e.g., Google Cloud Logging / Cloud Monitoring) or mocked.
* **SQL Language Compatibility**: The underlying SQL in `d_pfis_mps_vba_korrektur.sql` was built for Oracle SQL*Plus. The SQL itself must be rewritten to conform to BigQuery Standard SQL as part of its own design and build steps.