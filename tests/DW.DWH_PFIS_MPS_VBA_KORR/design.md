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


# UC4 WORKLOAD AUTOMATION MIGRATION DESIGN DOCUMENT
**Source Workflow:** Correction of non-determinable VBA IDs (`DW.DWH_PFIS_MPS_VBA_KORR`)
**Target Environment:** Apache Airflow (GCP Cloud Composer)

---

## 1. Overview
This design document covers the migration of a single standalone UC4 UNIX job, `DW.DWH_PFIS_MPS_VBA_KORR` ("Korrektur nicht ermittelbarer VBA-IDs"). This job is designed to perform data correction operations on non-determinable VBA IDs by executing a specialized Unix script (`r_pfis_mps_vba_korrektur`) on a target host. Based on the operational notes, this process is completely idempotent and safe to restart or retry automatically without any manual preparation or cleanup. Because no wrapping `JOBP` (workflow) or `SCRI` trigger is provided in this bundle, this job is classified as externally triggered.

---

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_PFIS_MPS_VBA_KORR` | JOBS_UNIX | Active (1) | Korrektur nicht ermittelbarer VBA-IDs (Correction of non-determinable VBA IDs) |

---

## 3. Scheduling
* **Schedule Policy:** No scheduling elements (`EVNT_TIME`, `JSCH`) are present in this extraction.
* **Trigger Source:** Externally triggered (the parent scheduler or calling workflow is outside the scope of this extraction bundle).
* **Airflow Schedule:** `schedule=None` (as per rules, no schedule is guessed or invented).

---

## 4. Airflow DAG Properties
Since this is a standalone JOBS object, it is wrapped in an individual Airflow DAG to manage its execution, logging, and retry logic.

| Property | Value |
| :--- | :--- |
| **DAG ID** | `dw_dwh_pfis_mps_vba_korr` |
| **Schedule** | `None` |
| **Start Date** | `datetime(2023, 1, 1)` *(Placeholder)* |
| **Catchup** | `False` |
| **Max Active Runs** | `1` |
| **Is Paused Upon Creation** | `False` *(Active flag is 1)* |
| **Default Args** | `{'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}` |

---

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `r_pfis_mps_vba_korrektur` | `DW.DWH_PFIS_MPS_VBA_KORR` | `EmptyOperator` | N/A | N/A | 1 | 5 Min | None | None | No | None | # REVIEW-STRUCT: launcher command [raw_command] not recognised — confirm target operator/script manually. Script executes a binary/script `$HOME/aktuell/pruef/is/bin/r_pfis_mps_vba_korrektur` on the local host. |

---

## 6. Task Dependency Map
As this DAG contains only a single task, there is no execution chain to represent.
```
r_pfis_mps_vba_korrektur
```

---

## 7. Sync / Concurrency Analysis
No sync rows or lock definitions were present in the source object. Standard DAG execution limits are applied.
* `max_active_runs=1` is applied to ensure that concurrent executions of this correction process do not overlap and cause database lock contention.

---

## 8. Error Handling and Retry Strategy
* **Retries:** Operational notes indicate that failed or interrupted processes can be executed again without any further manual work ("fehlgeschlagener oder unterbrochener Prozeß kann ohne weitere Arbeiten erneut ausgeführt werden"). Thus, automatic retries are safely configured to `1` with a standard delay.
* **Failure Handling:** Normal alert triggers (standard Airflow alerting) apply; no complex custom recovery triggers are specified.

---

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `&DWH_JOB_KENNUNG` | `'PFIS_MPS_VBA_KORR'` | `DWH_JOB_KENNUNG` as a task execution environment variable. |
| `DW.DWH_PFIS_MPS_VBA_KORR` | Object Name | `dw_dwh_pfis_mps_vba_korr` (Sanitized DAG ID) |

---

## 10. Developer Notes
* **# REVIEW-STRUCT: Unrecognized Launcher:** The job contains an unrecognized launcher command that points directly to a Unix script/binary: `$HOME/aktuell/pruef/is/bin/r_pfis_mps_vba_korrektur`. During implementation, the developers must decide whether this binary should be migrated to a containerized runner (e.g., Google Kubernetes Engine / GKEStartPodOperator), executed via SSH on a remaining persistent VM (`SSHOperator`), or rewritten entirely in Python.
* **Include Blocks:** The script contains references to `:inc DW.HOLE_PFAD` and `:inc DW.LESE_LOG`. These include files are not present in this extraction. They typically resolve directory paths and process logs. These functions should be natively replaced by Airflow's environment configuration/logging handlers.

---

# PSEUDOCODE OUTLINE

```python
── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
# Note: For deployment, replace EmptyOperator with the appropriate SSHOperator, 
# BashOperator, or GKEStartPodOperator depending on physical host migration strategy.

── GCP Configuration ────────────────────────────────────
# Placeholder for target script execution details (if run on GCP resources)
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
GCP_REGION = "YOUR_GCP_REGION"

── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

── on_failure_callback stubs ─────────────────────────────
# No custom failure callback objects required for this standalone job.

── DAG Definition (one per JOBP in the bundle) ──────────
dag_id = "dw_dwh_pfis_mps_vba_korr"

with DAG(
    dag_id=dag_id,
    default_args=DEFAULT_ARGS,
    description="Korrektur nicht ermittelbarer VBA-IDs",
    schedule_interval=None,  # No schedule present in UC4 extraction
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,  # Concurrency limit to prevent concurrent database writes
    tags=['migrated_uc4', 'jobs_unix', 'pfis'],
) as dag:

    ── Guard Task (if any self-lock Else=Skip sync detected) ─
    # None required

    ── Sensor Task (if any earliest_start_time constraint) ───
    # None required

    ── Calendar Check Task (if any CaleOn=1 detected) ────────
    # None required

    ── Task: r_pfis_mps_vba_korrektur (one block per Task Inventory row) ──
    # REVIEW-STRUCT: launcher command not recognised - currently mapped to EmptyOperator.
    # Replace with BashOperator or SSHOperator pointing to:
    # '$HOME/aktuell/pruef/is/bin/r_pfis_mps_vba_korrektur'
    # Environment variable needed: DWH_JOB_KENNUNG='PFIS_MPS_VBA_KORR'
    
    r_pfis_mps_vba_korrektur = EmptyOperator(
        task_id="r_pfis_mps_vba_korrektur",
    )

    ── Dependencies ─────────────────────────────────────────
    # Single-task workflow: no dependencies required.
    r_pfis_mps_vba_korrektur
```

# MIGRATION DESIGN DOCUMENT (ADDITIONAL CONTEXT)

This document provides the necessary scheduling, lineage, execution order, and environmental context for migrating `DW.DWH_PFIS_MPS_VBA_KORR` to Airflow (Cloud Composer), supplementing the automated conversion design.

---

## File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/data/source/single_job_demo_v3/DW.DWH_PFIS_MPS_VBA_KORR.xml` | `dags/dw_dwh_pfis_mps_vba_korr.py` | Converts the UC4 UNIX job definition into an Airflow DAG to orchestrate the execution sequence. |

---

## Execution order

The target Airflow DAG preserves the legacy step sequence as follows:
1. **Step 1:** `DW.DWH_PFIS_MPS_VBA_KORR.xml` maps directly to the wrapper DAG (`dags/dw_dwh_pfis_mps_vba_korr.py`).
2. **Step 2:** `r_pfis_mps_vba_korrektur` maps to the primary task inside the DAG (using an appropriate execution operator, e.g., `BashOperator`, `SSHOperator`, or `CloudRunStartJobOperator` to run the migrated shell-to-Python logic).
3. **Step 3:** `d_pfis_mps_vba_korrektur.sql` is invoked inside `r_pfis_mps_vba_korrektur` and therefore runs downstream as part of that script's database operations on BigQuery.

---

## Schedule & variables

* **Scheduler-Set Variables:**
  - `DWH_JOB_KENNUNG = 'PFIS_MPS_VBA_KORR'`: Passed to the execution task at runtime via Airflow's task environment config:
    ```python
    env={'DWH_JOB_KENNUNG': 'PFIS_MPS_VBA_KORR'}
    ```

---

## Lineage

* **Upstream Includes/Dependencies:**
  - `DW.HOLE_PFAD` (Unresolved in lineage; human-confirmed as not needed).
  - `DW.LESE_LOG` (Unresolved in lineage; human-confirmed as not needed).
* **Downstream Consumers / Call Chain:**
  - `r_pfis_mps_vba_korrektur` (FILE): A cross-group hand-off. The script is invoked by the UC4 job but is not listed in the `SOURCE FILES` section, meaning it belongs to a different migration pass.
* **Legacy Execution Host:**
  - `EXT:dwhdwh1p` (UNIX execution environment).
* **Legacy Package Dependency:**
  - `PACKAGE:DW.UNIX.ISTNS`.

---

## Target file plan

* **Target File:** `dags/dw_dwh_pfis_mps_vba_korr.py`
  - **Language:** Python
  - **Source File:** `local/data/source/single_job_demo_v3/DW.DWH_PFIS_MPS_VBA_KORR.xml`

---

## Environment-specific values

These values are classified by their role in the target GCP environment:

1. **GLOBAL (Environment-wide):**
   - `GCP_PROJECT`: Mapped to the target GCP project hosting Composer/BigQuery. Sourced at runtime via `Variable.get("GCP_PROJECT")` or `os.environ.get("GCP_PROJECT")`.
   - `GCP_REGION`: Mapped to the GCP region. Sourced at runtime via `Variable.get("GCP_REGION")` or `os.environ.get("GCP_REGION")`.
   - `GCS_BUCKET`: Mapped to the Composer/GCS DAGs or data bucket. Sourced at runtime via `Variable.get("GCS_BUCKET")` or `os.environ.get("GCS_BUCKET")`.
   - `legacy_host` (replaces legacy `dwhdwh1p`): Mapped to a GCP Connection ID or Airflow SSH Connection parameter.

2. **JOB-SPECIFIC:**
   - `DWH_JOB_KENNUNG`: Set directly as the literal string `'PFIS_MPS_VBA_KORR'`.

---

## Risks & Manual Actions

* SOURCE: NOT FOUND — DW.HOLE_PFAD — no candidate (Human-reviewed: not needed)
* SOURCE: NOT FOUND — DW.LESE_LOG — no candidate (Human-reviewed: not needed)
* **External Hand-off Migration:** The shell script `r_pfis_mps_vba_korrektur` and its SQL script `d_pfis_mps_vba_korrektur.sql` are outside the scope of this migration pass. The Airflow task pointing to this script must be treated as a placeholder until those files are migrated in their respective design passes.

---

=== FILE: local/data/source/single_job_demo_v3/d_pfis_mps_vba_korrektur.sql ===
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
    - PL/SQL Anonymous Block wrapped inside an SQL*Plus script.
1.2 Business Logic Summary:
    - This script cleans up and synchronizes missing sales channel/distribution IDs (`m2_vba_ebene6_id`, `m2_vba_ebene7_id`) in the facts table `dwh$ta_f_mps_nutzung` using description matches from the lookup view `dwh$vi_l_m2_vba`.
    - If a match is found on Level 6 description, it updates the Level 6 ID to Level 7 ID (or retains the current ID if null) and then clears the text description column.
    - It repeats the same lookup-and-update process for Level 7 description and ID.
    - Transaction control ensures that all four updates succeed together or roll back on failure. Runtime exceptions are caught, logged via a custom log library, and re-raised.
1.3 Entities Referenced:
    - `dwh$ta_f_mps_nutzung` (Fact Table)
        - `m2_vba_ebene6_id` (Inferred NUMBER/INT64)
        - `m2_vba_ebene7_id` (Inferred NUMBER/INT64)
        - `m2_vba_ebene6_text` (Inferred VARCHAR2/STRING)
        - `m2_vba_ebene7_text` (Inferred VARCHAR2/STRING)
    - `dwh$vi_l_m2_vba` (Lookup View)
        - `m2_vba_ebene6_text` (Inferred VARCHAR2/STRING)
        - `m2_vba_ebene7_text` (Inferred VARCHAR2/STRING)
        - `m2_vba_ebene7_id` (Inferred NUMBER/INT64)
    - `dwpa_globals.k_alis_err_unknown` (Oracle Package Constant)
    - `dwpa_meldung.fehler` (Oracle Package Stored Procedure)

Step 2: Oracle-Specific Construct Detection and Resolution

2.1 Data Type Conversions:
    - `NUMBER` (used for `EintragsNr` and `ErrC`) → `INT64`
    - `VARCHAR2(512)` (used for `ErrText`) → `STRING`
    - Table columns of type `VARCHAR2` → `STRING`
    - Table columns of type `NUMBER` → `INT64`

2.2 Implicit and Explicit Type Casting:
    - `TO_NUMBER('&1')` → `CAST(p_eintrags_nr AS INT64)`. In BigQuery Scripting, substitution parameters are declared as session variables or query parameters rather than SQL*Plus style `&1`.

2.3 NULL Handling and Conditional Functions:
    - `NVL(v.m2_vba_ebene7_id, n_sub.m2_vba_ebene6_id)` → `COALESCE(v.m2_vba_ebene7_id, n.m2_vba_ebene6_id)`

2.4 String Functions:
    - `UPPER(...)` → Fully compatible in BigQuery.

2.5 Date and Timestamp Functions:
    - None present.

2.6 Numeric and Aggregate Functions:
    - `MIN(col)` → Fully compatible in BigQuery.

2.7 Analytical and Window Functions:
    - None present.

2.8 Set and Join Operations:
    - `(+)` (Oracle proprietary outer join operator):
      In Update 1 and Update 3, Oracle joins `n_sub` (aliased fact table copy) and `v` (lookup view) with `ROWID` correlation to the target updated table `n`. This is rewritten in BigQuery by removing the redundant self-join and `ROWID` logic completely, replacing it with a clean, standard correlated scalar subquery wrapped in an outer `COALESCE`.

2.9 Row Limiting and Sampling:
    - `ROWID`: Oracle's physical row locator used in the correlated subquery `n_sub.ROWID = n.ROWID`. Since BigQuery does not expose physical row IDs, this correlation pattern is eliminated. The updates are refactored into direct correlated updates against single rows based on lookup parameters.

2.10 Sequences:
    - None present.

2.11 MERGE Statements:
    - None present.

2.12 INSERT / UPDATE / DELETE:
    - Correlation via `ROWID` in `UPDATE` is translated to BigQuery standard correlated subqueries.

2.13 DDL Constructs:
    - None present.

2.14 PL/SQL Scripting Constructs:
    - `DECLARE...BEGIN...END` block → BigQuery Scripting block `DECLARE...BEGIN...END`.
    - `EXCEPTION WHEN OTHERS THEN` → BigQuery Scripting `EXCEPTION WHEN ERROR THEN`.
    - `ROLLBACK` / `COMMIT` → BigQuery `ROLLBACK TRANSACTION` / `COMMIT TRANSACTION` wrapped around a transaction block.
    - `SQLERRM` → `@@error.message`
    - `SQLCODE` → `@@error.statement_text` (or custom code representation, as BigQuery does not provide numeric SQLCODE natively).
    - `dwpa_meldung.fehler` → Replaced with a placeholder procedure call `CALL dwh_utility.dwpa_meldung_fehler(...)`.
    - `raise_application_error` → `ERROR(@@error.message)`.

2.15 Unresolvable or Advisory Items:
    - SQL*Plus shell commands (`WHENEVER OSERROR EXIT`, `START`, `SET TIMING`, etc.) are stripped as they are tool-specific execution configurations.
    - External dependency packages `dwpa_globals` and `dwpa_meldung` must be deployed or stubbed in BigQuery as standalone schemas/procedures.

Step 3: Conversion Strategy Summary
3.1 Conversion Approach:
    - Translate the PL/SQL execution into a single, cohesive BigQuery Scripting block.
    - Encapsulate the updates inside a `BEGIN TRANSACTION` and `COMMIT TRANSACTION` block to preserve atomic business transactions.
    - Replace `ROWID` correlation logic with standard, optimized correlated scalar subqueries.
3.2 Assumptions:
    - The lookup view and target fact tables exist in the same dataset or have dataset routing configured.
    - Input parameter `&1` is passed as a session-defined scripting variable `p_eintrags_nr`.
3.3 Items Flagged for Human Review:
    - Deployment of auxiliary procedures/objects representing `dwpa_meldung.fehler` and package variables.

═══════════════════════════════════════════
2.16 MIGRATION DECISION MATRIX
═══════════════════════════════════════════

| Oracle Construct / Statement | BigQuery Target | Rejected Alternatives | Evidence / Reason |
| :--- | :--- | :--- | :--- |
| **SQL*Plus Commands** (`WHENEVER`, `SET`) | Stripped | Keep as metadata comments | Not supported or needed in BigQuery execution engine. |
| **PL/SQL Anonymous Block** | Scripting Block (`BEGIN...EXCEPTION...END`) | Python Orchestrator | Simple procedural logic fits standard BigQuery Scripting natively without overhead. |
| **ROWID Correlated Updates** | Correlated Scalar Subqueries with `COALESCE` | Temporary Table Join | Eliminating the self-join of the large fact table using simplified scalar subqueries maximizes performance. |
| **NVL** | `COALESCE` | `CASE WHEN` | `COALESCE` is standard, cleaner, and semantically equivalent. |
| **PL/SQL Exception Block** | `EXCEPTION WHEN ERROR THEN` | Python Exception Handling | BigQuery supports robust procedural error catching and transaction rollbacks natively. |

═══════════════════════════════════════════
2.17 REQUIRED ARTIFACTS
═══════════════════════════════════════════

The build will generate a single, deployable BigQuery Scripting execution file:
1. **BigQuery SQL Script**: `d_pfis_mps_vba_korrektur.sql`
   - Incorporates all data type alignments, rewritten scalar queries, transaction block wrappers, and exception mapping.
2. **Pre-Requisite (Manual Deployment)**:
   - Create a logging procedure in BigQuery (e.g., `dwh_utility.dwpa_meldung_fehler`) to receive log entries if audit tracking must be maintained.

═══════════════════════════════════════════
2.18 DATA TYPE COMPATIBILITY TABLE
═══════════════════════════════════════════

| Oracle Column / Variable | Oracle Type | BigQuery Type | Conversion Rule / Logic | Warning / Downstream Impact |
| :--- | :--- | :--- | :--- | :--- |
| `EintragsNr` | `NUMBER` | `INT64` | `CAST(p_eintrags_nr AS INT64)` | None. |
| `ErrC` | `NUMBER` | `INT64` | Default dummy/numeric representation | BQ does not have numeric SQL error codes natively. |
| `ErrText` | `VARCHAR2(512)` | `STRING` | `STRING` | Max length constraints are omitted in BQ. |
| `m2_vba_ebene6_id` | `NUMBER` | `INT64` | `INT64` | Standard numeric ID. |
| `m2_vba_ebene6_text` | `VARCHAR2` | `STRING` | `STRING` | Case-insensitive matching logic preserved via `UPPER()`. |

═══════════════════════════════════════════
2.19 DESIGN REVIEW SUMMARY
═══════════════════════════════════════════
- **Patterns/Objects Found**:
  - Double updates on level-based hierarchies with intermediate updates setting columns to `NULL`.
  - Heavy reliance on Oracle-specific physical `ROWID` join correlations.
- **Unsupported Functions**:
  - `ROWID`, `(+)` outer joins inside update expressions, SQL*Plus variables.
- **UDF Required**: No.
- **Python Required**: No.
- **Direct Dependencies**:
  - Target table: `dwh$ta_f_mps_nutzung`
  - Lookup View: `dwh$vi_l_m2_vba`
  - Logging procedure: `dwpa_meldung.fehler` (Placeholder deployed to BQ target).
- **Warnings**:
  - Oracle `SQLCODE` is dynamic; BigQuery script-level error capture uses `@@error.message` and `@@error.statement_text`. A hardcoded indicator or step context must be provided instead of numeric DB codes.

OVERALL MIGRATION STRATEGY: Direct BigQuery SQL

═══════════════════════════════════════════
2.21 ORACLE FUNCTION ANALYSIS TABLE
═══════════════════════════════════════════

| Oracle Function/Construct | Supported in BigQuery | BigQuery Equivalent / Alternative |
| :--- | :--- | :--- |
| `TO_NUMBER` | Direct-with-rewrite | `SAFE_CAST(val AS INT64)` |
| `NVL` | Direct-with-rewrite | `COALESCE` |
| `ROWID` | Direct-with-rewrite | None — Subqueries rewritten to execute correlations logically |
| `(+` (Outer Join) | Direct-with-rewrite | Standard correlated subquery logic with fallback default handles |
| `UPPER` | Direct | `UPPER` |
| `MIN` | Direct | `MIN` |
| `SQLERRM` | Direct-with-rewrite | `@@error.message` |
| `SQLCODE` | Direct-with-rewrite | Constant fallback / `@@error.statement_text` |
| `TO_CHAR` | Direct-with-rewrite | `CAST(val AS STRING)` |
| `raise_application_error` | Direct-with-rewrite | `ERROR(message)` |

<br>

═══════════════════════════════════════════
SECTION 2 — PSEUDOCODE
═══════════════════════════════════════════

```sql
-- Parameters must be declared at the entrypoint of the execution block.
-- Assuming p_eintrags_nr is passed as a script parameter.
DECLARE p_eintrags_nr STRING DEFAULT '12345'; 

DECLARE v_eintrags_nr INT64;
DECLARE v_err_text STRING;
DECLARE v_err_code STRING;
DECLARE v_fehler_nr INT64;

BEGIN
  -- Set parameter value converted to numeric format
  SET v_eintrags_nr = SAFE_CAST(p_eintrags_nr AS INT64);  -- converted from TO_NUMBER('&1')
  SET v_fehler_nr = -99999; -- placeholder representing dwpa_globals.k_alis_err_unknown

  -- Begin transaction block to ensure execution safety and rollback capabilities
  BEGIN TRANSACTION;

  -- ---------------------------------------------------------------------
  -- UPDATE 1: Update Level 6 VBA ID
  -- ---------------------------------------------------------------------
  UPDATE `dwh.dwh$ta_f_mps_nutzung` n 
     SET n.m2_vba_ebene6_id = COALESCE(
            (
               SELECT MIN(COALESCE(v.m2_vba_ebene7_id, n.m2_vba_ebene6_id)) -- converted from NVL(v.m2_vba_ebene7_id, n_sub.m2_vba_ebene6_id)
                 FROM `dwh.dwh$vi_l_m2_vba` v
                WHERE UPPER(n.m2_vba_ebene6_text) = UPPER(v.m2_vba_ebene6_text)
            ),
            n.m2_vba_ebene6_id
         ) -- ROWID self-join and outer join (+) rewritten to direct scalar correlated subquery wrapped in COALESCE
   WHERE n.m2_vba_ebene6_text IS NOT NULL;

  -- ---------------------------------------------------------------------
  -- UPDATE 2: Clear Level 6 texts for successfully mapped records
  -- ---------------------------------------------------------------------
  UPDATE `dwh.dwh$ta_f_mps_nutzung` n 
     SET n.m2_vba_ebene6_text = NULL
   WHERE n.m2_vba_ebene6_text IS NOT NULL
     AND n.m2_vba_ebene6_id <> (
        SELECT v.m2_vba_ebene7_id
          FROM `dwh.dwh$vi_l_m2_vba` v
         WHERE UPPER(v.m2_vba_ebene6_text) = 'UNBEKANNT'
     );

  -- ---------------------------------------------------------------------
  -- UPDATE 3: Update Level 7 VBA ID
  -- ---------------------------------------------------------------------
  UPDATE `dwh.dwh$ta_f_mps_nutzung` n 
     SET n.m2_vba_ebene7_id = COALESCE(
            (
               SELECT v.m2_vba_ebene7_id
                 FROM `dwh.dwh$vi_l_m2_vba` v
                WHERE UPPER(v.m2_vba_ebene7_text) = UPPER(n.m2_vba_ebene7_text)
            ),
            n.m2_vba_ebene7_id
         ) -- ROWID self-join and outer join (+) rewritten to direct scalar correlated subquery wrapped in COALESCE
   WHERE n.m2_vba_ebene7_text IS NOT NULL;
   
  -- ---------------------------------------------------------------------
  -- UPDATE 4: Clear Level 7 texts for successfully mapped records
  -- ---------------------------------------------------------------------
  UPDATE `dwh.dwh$ta_f_mps_nutzung` n 
     SET n.m2_vba_ebene7_text = NULL
   WHERE n.m2_vba_ebene7_text IS NOT NULL
     AND n.m2_vba_ebene7_id <> (
        SELECT v.m2_vba_ebene7_id
          FROM `dwh.dwh$vi_l_m2_vba` v
         WHERE UPPER(v.m2_vba_ebene7_text) = 'UNBEKANNT'
     );

  -- Commit changes only if all actions succeeded
  COMMIT TRANSACTION;

EXCEPTION WHEN ERROR THEN
  -- Rollback transaction block on any failures
  ROLLBACK TRANSACTION;

  -- Populate error details from context
  SET v_err_text = @@error.message;                  -- converted from SQLERRM
  SET v_err_code = @@error.statement_text;             -- converted from TO_CHAR(SQLCODE)

  -- Execute error logger call (Mocking external dependency procedure)
  CALL `dwh_utility.dwpa_meldung_fehler`('F', v_eintrags_nr, v_fehler_nr, v_err_text, v_err_code);

  -- Escalate and raise error context back to the orchestrator
  ERROR(v_err_text);  -- converted from raise_application_error
END;
```

═══════════════════════════════════════════
FLAGGED ITEMS FOR HUMAN REVIEW
═══════════════════════════════════════════
1. **Oracle Package Replacements**:
   - `dwpa_globals.k_alis_err_unknown`: The numeric value of this constant is currently hardcoded as `-99999` in the scripting block. The true numerical value needs to be verified and configured.
   - `dwpa_meldung.fehler`: This logging package procedure has been translated as a standard BigQuery procedure call `CALL dwh_utility.dwpa_meldung_fehler(...)`. A stub or wrapper procedure with this signature must be created in BigQuery to prevent execution compilation errors.
2. **Update Correlation Precision**:
   - The original queries leveraged Oracle's physical `ROWID` to handle duplicates when self-joining. The BigQuery refactoring replaces this logic with standard correlated subqueries assuming that matches on the level descriptions are functional and distinct. If duplicates exist in the lookup table `dwh$vi_l_m2_vba` for a single text value, the subquery in Update 3 may return more than one row and cause a runtime single-row subquery violation. If duplicates are expected, a `LIMIT 1` or further analytic ordering should be applied.

### EXECUTION ORDER
The legacy dependency graph lists 3 sequential steps for this job's group:
1. `DW.DWH_PFIS_MPS_VBA_KORR.xml` (UC4 orchestration, handled in a separate pass)
2. `r_pfis_mps_vba_korrektur` (KornShell wrapper, handled in a separate pass)
3. `d_pfis_mps_vba_korrektur.sql` (Core database updates, handled in this pass)

The target orchestration must preserve this order:
- **Step 1 (UC4 Orchestration)** maps to Cloud Composer DAG execution.
- **Step 2 (KSH Wrapper)** maps to a Python Operator executing wrapper/pre-checks.
- **Step 3 (SQL Script)** maps to a BigQueryInsertJobOperator executing the converted `d_pfis_mps_vba_korrektur.sql` file.

### SCHEDULE & VARIABLES — MUST BE RETAINED
The legacy job relies on the following scheduler-defined variable, which must be retained in the BigQuery/Composer environment:
* **DWH_JOB_KENNUNG** = `'PFIS_MPS_VBA_KORR'` (from `DW.DWH_PFIS_MPS_VBA_KORR`)
  - *Target mechanism:* Passed as an Airflow DAG-level parameter/Airflow Variable (`Variable.get("DWH_JOB_KENNUNG")` or defined as a runtime config parameter) and injected into the execution context.

### LINEAGE
Based on the lineage edges in the source context:
* **Upstream Producers / Referenced Objects:**
  - `dwh$vi_l_m2_vba` (Lookup view used to read updated level 6 and 7 sales channel/VBA hierarchies)
  - `dwpa_meldung` (Legacy package used for error logging/exception registration)
* **Downstream Consumers / Written Tables:**
  - `dwh$ta_f_mps_nutzung` (Fact table updated by the logic with corrected VBA IDs)

### ENVIRONMENT-SPECIFIC VALUES
The environment-specific values found in the source are classified by their role in the target platform:

1. **GLOBAL (Environment-Wide)**
   * **GCP_PROJECT**: The target Google Cloud Project ID where the BigQuery tables reside.
     - *Access Mechanism:* Referenced via a query parameter (`@gcp_project`) or dataset routing.
   * **BQ_DATASET**: The BigQuery dataset containing the DWH tables (`dwh$ta_f_mps_nutzung` and `dwh$vi_l_m2_vba`).
     - *Access Mechanism:* Referenced via a query parameter (`@bq_dataset`) or dataset routing.
   * **DW_DIR_ROOT**: The root directory for shared utilities in the legacy environment, referenced during initialization (`START $DW_DIR_ROOT/allgemein/is/util/sql/d_alis_init.sql`).
     - *Access Mechanism:* BigQuery has no direct target-platform equivalent for filesystem path-based script inclusion (`START`).

2. **JOB-SPECIFIC**
   * **p_eintrags_nr** (`&1` in the SQL script): The error entry number from the calling process.
     - *Access Mechanism:* Passed as a query parameter (e.g., `@p_eintrags_nr`) during script execution.

### TARGET FILE PLAN
* **Target File Path:** `d_pfis_mps_vba_korrektur.sql`
  - **Language:** SQL (BigQuery SQL Scripting)
  - **Source File:** `d_pfis_mps_vba_korrektur.sql`
  - **Purpose:** Execute the core hierarchy data corrections on table `dwh$ta_f_mps_nutzung` using BigQuery Scripting.

### FILE DISPOSITION
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `d_pfis_mps_vba_korrektur.sql` | `d_pfis_mps_vba_korrektur.sql` | Migrated directly to BigQuery SQL Scripting containing the transactional hierarchy updates and error logging calls. |

---

=== CONFIRMED TARGET PLATFORM ===
TARGET_PLATFORM: BIGQUERY
This is stated directly by the caller, not inferred from the extraction below. Treat it as ground truth everywhere the design/build instructions would otherwise infer the platform or fall back to a default -- it satisfies the "unless explicitly stated in the extraction" condition those rules already reference. Never emit "# REVIEW: target database platform not specified" while this section is present.

=== FILE: local/data/source/single_job_demo_v3/r_pfis_mps_vba_korrektur ===
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
REASON: The script orchestrates process execution using a complex database-backed logging framework, handles custom environment bootstrap traps, parses command-line flags, and executes an external SQL script whose body is not supplied.

EVIDENCE
- Business logic found: KSH custom logic orchestrates the execution of `d_pfis_mps_vba_korrektur.sql` via custom tracking and logging utilities.
- AWK: none
- SQL-expressible: No, because it manages process orchestration, custom logging (`DWMSG_*`), and log file generation using shell utilities.
- Non-SQL side effects: Local log files written via `tee`, process-level trap definitions, and status updates via external shell/DB utilities.
- Against this verdict: None. Converting this to pure BigQuery SQL is impossible due to the external process-orchestration wrappers, environment traps, and framework logging dependencies.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   The script `r_pfis_mps_vba_korrektur` performs corrective updates to unresolvable VBA-IDs (Vertriebsabwicklungs-IDs) within the MPS usage data. It serves to clean up and align downstream data warehouse tables. The script initiates a tracking session, configures log traps for process monitoring, checks for the presence of the required SQL correction script, and executes the SQL via an external launcher utility.

2. INVOCATION CONTEXT
   - Invocation: Typically invoked by a UC4/Automic UNIX job (such as a job utilizing job identifier `PFIS_MPS_VBA_KORR`).
   - Sourced Environment Files:
     * `. $HOME/.dw_init` (trimmed out from source) — `# REVIEW-STRUCT: environment file $HOME/.dw_init not supplied — variables it sets are unknown; do not guess their names or values`
     * `. ${DW_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (trimmed out) — `# REVIEW-STRUCT: environment file f_alis_msgerr.ksh not supplied — variables it sets are unknown; do not guess their names or values`
     * `. ${DW_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (trimmed out) — `# REVIEW-STRUCT: environment file h_alis_parameter.ksh not supplied — variables it sets are unknown; do not guess their names or values`
     * `. ${DW_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (trimmed out) — `# REVIEW-STRUCT: environment file h_alis_date.ksh not supplied — variables it sets are unknown; do not guess their names or values`
     * `. ${DW_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` (trimmed out, contains the definition for `starteSQLSkript`) — `# REVIEW-STRUCT: environment file h_alis_sqlplus.ksh not supplied — variables it sets are unknown; do not guess their names or values`

3. PARAMETERS / INPUTS
   - `-v` (verbose option): Parsed using `getopts`. Sets `p_Verbose=1` when present, which triggers printing the log file to stdout at exit. Surface in Python via `argparse.ArgumentParser`.
   - `-h` (help option): Triggers the `usage()` help screen and exits. Surface in Python via `argparse.ArgumentParser`.
   - `DW_DIR_ROOT`: Sourced framework environment variable. Used to resolve the absolute path to the SQL correction script. Surface in Python via `os.environ.get("DW_DIR_ROOT")`.
   - `DW_EintragsNr`: Sourced framework environment variable initialized to `0` and resolved dynamically via the framework command `DWMSG_ErmittleNr`.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `DWMSG_ErmittleNr DW_EintragsNr`: Generates or retrieves the tracking entry number for the job. Must remain an external process call or be refactored to interface with the modern equivalent of the metadata system.
     * `# REVIEW-STRUCT: launcher DWMSG_ErmittleNr invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion`
   - `DWMSG_Logdateiname LogDatei PFIS_MPS_VBA_KORR ${DW_EintragsNr}`: Resolves the log file path dynamically.
     * `# REVIEW-STRUCT: launcher DWMSG_Logdateiname invoked — internal behaviour not available in this extraction`
   - `DWMSG_ErzeugeEintrag ...`: Registers the job execution in the system log.
     * `# REVIEW-STRUCT: launcher DWMSG_ErzeugeEintrag invoked — internal behaviour not available in this extraction`
   - `DWMSG_Fehlerbehandlung ...`: Invoked during exit trap on errors to flag job failure in the system.
     * `# REVIEW-STRUCT: launcher DWMSG_Fehlerbehandlung invoked — internal behaviour not available in this extraction`
   - `DWMSG_SetzeStatusOK ...`: Invoked on success to register a successful completion in the tracking system.
     * `# REVIEW-STRUCT: launcher DWMSG_SetzeStatusOK invoked — internal behaviour not available in this extraction`
   - `starteSQLSkript ${p_EintragsNr} ${p_SQL_Skript} ${p_EintragsNr}`: Framework wrapper that executes the specified SQL script. Because the target platform is confirmed as BigQuery, executing SQL*Plus/Oracle scripts through shell utilities is no longer viable.
     * `# REVIEW: target database platform is confirmed as BIGQUERY. The starteSQLSkript utility represents a legacy SQL*Plus database launcher. The contents of 'd_pfis_mps_vba_korrektur.sql' must be migrated to BigQuery and executed via the python-bigquery client (google-cloud-bigquery) rather than through a wrapper script.`

5. EMBEDDED SQL
   - No inline SQL exists in this wrapper script.
   - Referenced SQL file: `${DW_DIR_ROOT}/pruef/is/sql/d_pfis_mps_vba_korrektur.sql`
   - Dialect / Implementation: Legacy Oracle SQL*Plus (indicated by `h_alis_sqlplus.ksh` dependency). Since the target platform is BigQuery, the SQL code within `d_pfis_mps_vba_korrektur.sql` must be translated into BigQuery Standard SQL and run natively.

6. CONTROL FLOW
   1. Initialize global flags and program metadata (`ProgName`, `ProgVersion`, `JobKennung`).
   2. Configure immediate shell termination on command failure (`set -e`).
   3. Parse command-line options `-v` and `-h` using standard argument parsing.
   4. Define path to SQL script (`Korr_Skript`).
   5. Resolve active job entry tracking number via `DWMSG_ErmittleNr`.
   6. Resolve log file name via `DWMSG_Logdateiname`.
   7. Log metadata headers to the designated log file.
   8. Register job entry via `DWMSG_ErzeugeEintrag`.
   9. Set up error traps to intercept process termination (`INT STOP CONT QUIT`) and run failures (`ERR`), binding them to dynamic error handling calls (`DWMSG_Fehlerbehandlung`).
   10. Perform a filesystem check on `/vobs/dw_source/isdwh/pruef/is/sql/d_pfis_mps_vba_korrektur.sql` and print its existence status.
   11. Call the database launcher `starteSQLSkript` with mapped parameters.
   12. Capture the return code of the database launcher.
   13. Evaluate return code; if non-zero, print failure warning and exit.
   14. Deactivate traps and register successful completion via `DWMSG_SetzeStatusOK`.
   15. If verbose execution is requested (`p_Verbose == 1`), print the entire log file to stdout.
   16. Terminate with exit code 0.

7. ERROR HANDLING & EXIT CODES
   - Errors are monitored at the process level via `set -e` and specific KSH traps on `ERR` and signals `INT STOP CONT QUIT`.
   - On error, the script calls `DWMSG_Fehlerbehandlung` to write error metadata to the tracking database.
   - If the core SQL script execution returns a non-zero exit code, the script exits immediately with that exact return code.
   - Python translation: Implement a `try...except Exception as e` block wrapping the process execution, and a `finally` block to mimic trap behavior, invoking equivalent logging/cleanup helpers. Raise `subprocess.CalledProcessError` or use `sys.exit()` for custom return codes.

8. OUTPUTS / SIDE EFFECTS
   - Writes log outputs dynamically to local disk path defined by `LogDatei`.
   - Mutates tracking system metadata tables via framework logging calls (`DWMSG_*`).
   - Alters tables in BigQuery via the execution of SQL contained in `d_pfis_mps_vba_korrektur.sql`.

9. BUSINESS SUMMARY
   - Coordinates the corrective database updates for unresolved VBA-IDs in MPS usage records.
   - Ensures strict audit trails by recording job status transitions (Start, Error, Success) in a centralized DWH tracking database.
   - Generates trace files to assist developers in debugging query execution steps when verbose mode is activated.

=== PSEUDOCODE STYLE ===

```python
# Step 1: Import required standard modules
import os
import sys
import argparse
import subprocess

# Step 2: Establish program metadata
PROG_NAME = "Korrektur VBA-IDs"
PROG_VERSION = "6.5.0"
JOB_KENNUNG = "PFIS_MPS_VBA_KORR"

def usage():
    print(f"""
Programm: {PROG_NAME}
Version: {PROG_VERSION}
Aufruf: {os.path.basename(__file__)} [-v] [-h]
Parameter:
  -v     verbose, gibt im Anschluss oder bei Fehlern direkt die Log-Datei aus
  -h     zeigt diese Seite an

Beschreibung:
   Korrektur nicht ermittelbarer VBA-IDs der MPS-Nutzungsdaten
""")

def main():
    # Step 3: Parse command-line parameters
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('-v', '--verbose', action='store_true', default=False)
    parser.add_argument('-h', '--help', action='store_true', default=False)
    
    args, unknown = parser.parse_known_args()
    
    if args.help:
        usage()
        sys.exit(0)
        
    p_verbose = 1 if args.verbose else 0
    
    # Step 4: Define path to SQL script
    dw_dir_root = os.environ.get("DW_DIR_ROOT", "")
    # REVIEW-STRUCT: environment file defining DW_DIR_ROOT was not supplied. Defaulting to empty string.
    korr_skript = os.path.join(dw_dir_root, "pruef/is/sql/d_pfis_mps_vba_korrektur.sql")
    
    # Step 5: Resolve job sequence/tracking ID
    # REVIEW-STRUCT: launcher DWMSG_ErmittleNr is an external binary/utility whose source code is unsupplied.
    # Representing as a subprocess execution.
    dw_eintrags_nr = "0"
    try:
        res = subprocess.run(["DWMSG_ErmittleNr"], capture_output=True, text=True, check=True)
        dw_eintrags_nr = res.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        # Fallback to default or raise depending on environmental configuration
        dw_eintrags_nr = "0"
        
    # Step 6: Determine log file location
    # REVIEW-STRUCT: launcher DWMSG_Logdateiname is an external utility.
    try:
        res_log = subprocess.run(["DWMSG_Logdateiname", "LogDatei", JOB_KENNUNG, dw_eintrags_nr], capture_output=True, text=True, check=True)
        log_datei = res_log.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        log_datei = f"/tmp/{JOB_KENNUNG}_{dw_eintrags_nr}.log"
        
    # Step 7: Write runtime banners to log file
    with open(log_datei, "w") as f_log:
        f_log.write("--------------------------- Job ------------------------------------\n")
        f_log.write(f"Jobkennung :  {JOB_KENNUNG}\n")
        f_log.write(f"Job-Nr     :  {dw_eintrags_nr}\n")
        f_log.write(f"Logdatei   :  {log_datei}\n")
        f_log.write("--------------------------------------------------------------------\n")
        
    # Step 8: Register execution start in tracking system
    # REVIEW-STRUCT: launcher DWMSG_ErzeugeEintrag is an external utility.
    try:
        with open(log_datei, "a") as f_log:
            subprocess.run(["DWMSG_ErzeugeEintrag", dw_eintrags_nr, JOB_KENNUNG, sys.argv[0], log_datei], stdout=f_log, stderr=subprocess.STDOUT, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        pass

    # Setup execution state tracking for cleanup traps
    execution_success = False

    try:
        # Step 9: Map execution bridge variables
        p_eintrags_nr = dw_eintrags_nr
        p_sql_skript = korr_skript
        
        print("---------- Ausgabe Parameter --------------")
        print(f"Eintragnr.          : {p_eintrags_nr}")
        print("-------------------------------------------")
        
        # Step 10: Check if physical SQL file exists on server filesystem path
        check_path = "/vobs/dw_source/isdwh/pruef/is/sql/d_pfis_mps_vba_korrektur.sql"
        if os.path.exists(check_path):
            print("Gefunden")
        else:
            print("nicht gefunden")
            
        # Step 11: Execute the SQL script (historically starteSQLSkript)
        # REVIEW: Target platform is BIGQUERY. starteSQLSkript executes Oracle SQL.
        # This python code mock executes the script but in practice must call BigQuery client.
        with open(log_datei, "a") as f_log:
            # Emulating legacy shell execution wrapper
            run_sql = subprocess.run(["starteSQLSkript", p_eintrags_nr, p_sql_skript, p_eintrags_nr], stdout=f_log, stderr=subprocess.STDOUT)
            return_code = run_sql.returncode

        # Step 12: Handle SQL execution failure
        if return_code != 0:
            with open(log_datei, "a") as f_log:
                f_log.write("Fehler im Kernskript aufgetreten!\n")
            print("Fehler im Kernskript aufgetreten!", file=sys.stderr)
            sys.exit(return_code)
            
        execution_success = True
        
    except Exception as exc:
        # Step 13: Exception handling block (analogous to KSH traps on error)
        # REVIEW-STRUCT: launcher DWMSG_Fehlerbehandlung is an external logging script.
        try:
            with open(log_datei, "a") as f_log:
                f_log.write(f"\n!FEHLER/OSFEHLER gemeldet! Exception occurred: {str(exc)}\n")
                subprocess.run(["DWMSG_Fehlerbehandlung", dw_eintrags_nr], stdout=f_log, stderr=subprocess.STDOUT)
        except Exception:
            pass
        
        if p_verbose == 1:
            if os.path.exists(log_datei):
                with open(log_datei, "r") as f_log:
                    print(f_log.read())
        sys.exit(1)
        
    finally:
        # Step 14: Finalize execution tracking and log verbose traces
        if execution_success:
            # REVIEW-STRUCT: launcher DWMSG_SetzeStatusOK is an external tracking wrapper.
            try:
                with open(log_datei, "a") as f_log:
                    subprocess.run(["DWMSG_SetzeStatusOK", dw_eintrags_nr], stdout=f_log, stderr=subprocess.STDOUT)
                    f_log.write("Abarbeitung ohne erkennbare Fehler beendet\n")
                print("Abarbeitung ohne erkennbare Fehler beendet")
            except Exception:
                pass
                
        if p_verbose == 1:
            print("-- Logdatei --")
            if os.path.exists(log_datei):
                with open(log_datei, "r") as f_log:
                    print(f_log.read())
            print("-- Logdatei Ende --")
            
        sys.exit(0)

if __name__ == "__main__":
    main()
```

### Execution Order
The legacy dependency graph lists a 3-step sequence that must be preserved in the target orchestration (e.g., within an Airflow DAG in Cloud Composer):
1. **`DW.DWH_PFIS_MPS_VBA_KORR.xml`** (UC4 Job Definition): This triggers the orchestration wrapper. In Cloud Composer, this corresponds to the DAG trigger or schedule.
2. **`r_pfis_mps_vba_korrektur`** (Orchestration Wrapper): Maps to the task running the converted Python script `r_pfis_mps_vba_korrektur.py`.
3. **`d_pfis_mps_vba_korrektur.sql`** (SQL script): Executed by the converted Python script. In BigQuery, this sequence is maintained by executing the translated BigQuery SQL code of `d_pfis_mps_vba_korrektur.sql` via the `google-cloud-bigquery` library.

### Schedule & Variables
The legacy scheduler-set environment variable must be retained and passed to the migrated job at runtime:
* **`DWH_JOB_KENNUNG`** (`'PFIS_MPS_VBA_KORR'`): Will reach the migrated Python script as an Airflow DAG parameter or task environment variable (e.g., `os.environ["DWH_JOB_KENNUNG"]`).

### Lineage
* **Downstream Consumers**: 
  * `r_pfis_mps_vba_korrektur` executes `d_pfis_mps_vba_korrektur.sql`. This relationship is mapped as a sequential task trigger or direct API invocation of the translated SQL on BigQuery.

### Cross-File Dependencies
* **Database Correction SQL (`d_pfis_mps_vba_korrektur.sql`)**: The Python wrapper relies on this script to perform the actual data correction on `DWH$TA_F_MPS_NUTZUNG`. Since this SQL file is outside the scope of this migration group, it is treated as an external dependency that must be migrated separately.
* **Trimmed Legacy Helpers**: The source refers to external utilities like `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, and `h_alis_sqlplus.ksh` (which defines `starteSQLSkript`). These are legacy system-wide utilities and are replaced by native Cloud logging and the Python BigQuery API client.

### Target File Plan
* **`r_pfis_mps_vba_korrektur.py`**
  * **Language**: Python (v3.11+)
  * **Source**: `r_pfis_mps_vba_korrektur`
  * **Purpose**: Orchestrates the execution of the SQL correction scripts, sets up process auditing logs, handles error signaling, and outputs verification logs.

### Environment-Specific Values
* **`DW_DIR_ROOT`** (GLOBAL): The root directory of the legacy DWH installation. In Cloud Composer, this is sourced via the environment variable `os.environ.get("DW_DIR_ROOT")` or mapped to the DAG/script storage bucket path.
* **`JobKennung`** (JOB-SPECIFIC): Set to `'PFIS_MPS_VBA_KORR'` within the script context.
* **`DW_EintragsNr`** (JOB-SPECIFIC): The sequence run ID from the legacy database tracker. In Cloud Composer, this is mapped to the Airflow task execution context run ID (`{{ run_id }}`).
* **`check_path`** (JOB-SPECIFIC): Legacy source control path `/vobs/dw_source/isdwh/pruef/is/sql/d_pfis_mps_vba_korrektur.sql`. This should be mapped to the actual Google Cloud Storage (GCS) URI or local DAG subfolder where the SQL script is staged in the target environment.

### File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/data/source/single_job_demo_v3/r_pfis_mps_vba_korrektur` | `r_pfis_mps_vba_korrektur.py` | Converts the shell-based database-tracking and execution wrapper into a python script that logs executions and coordinates the BigQuery task. |

### Risks & Manual Actions
* **Trimmed Legacy Framework Utilities**: Framework dependencies such as `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_Fehlerbehandlung`, and `DWMSG_SetzeStatusOK` are unsupplied because they are global infrastructure utilities. A cloud logging wrapper (e.g., using Google Cloud Logging or native Composer Airflow task state handling) must be implemented to replace these database-backed tracking calls.
* **Dependent SQL Migration**: The wrapper executes `d_pfis_mps_vba_korrektur.sql` which is outside the scope of this file's design pass. The SQL script must be separately migrated to BigQuery-compliant SQL syntax and stored in the target repository/bucket for this wrapper to execute.
* **Legacy Paths**: The physical check path `/vobs/dw_source/...` must be manually updated or removed to avoid false "nicht gefunden" logs in the target cloud environment.