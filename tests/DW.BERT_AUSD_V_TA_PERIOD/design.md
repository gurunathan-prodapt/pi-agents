=== OBJECT: DW.BERT_AUSD_V_TA_PERIOD (JOBS_UNIX) ===
active=1
title=Mirror Carmen period definitions
login=DW.UNIX.ISBERT
host=|DWHDWH1P|HOST
ert_seconds=6
launcher_type=unrecognized
launcher_details={'raw_command': '# [TRIMMED for the 3-file DE demo: ". $HOME/.dw_init" removed here --'}
script_body:
:inc DW.HOLE_PFAD
:set &DWH_JOB_KENNUNG='AUSD_V_TA_PERIOD'
# [TRIMMED for the 3-file DE demo: ". $HOME/.dw_init" removed here --
#  framework env bootstrap, not this job's business logic; .dw_init is not
#  one of the 3 files in this demo. See the untrimmed chain under
#  isrpt/isbert/SQL/aktuell/... in ~/data for the real dot-source.]
&HOME/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh
:inc DW.BERT_LESE_LOG
operational_notes=

=== UNRESOLVED REFERENCES (object named but not supplied in this bundle) ===
  (none — every referenced object was supplied in this bundle)


# UC4 to Apache Airflow Migration Design Document

## 1. Overview
This migration document details the transition of the UC4 job **DW.BERT_AUSD_V_TA_PERIOD** to an Apache Airflow environment. The original UC4 object is an active standalone UNIX job (`JOBS_UNIX`) designed to mirror Carmen period definitions by executing a local Korn Shell (KSH) script. The script relies on UC4-specific platform includes (`DW.HOLE_PFAD` and `DW.BERT_LESE_LOG`) for environment path setting and log scraping, and sets a context identifier variable `DWH_JOB_KENNUNG` to `'AUSD_V_TA_PERIOD'`. Because no parent workflow (JOBP) or execution schedule (JSCH/EVNT) is supplied in this extraction bundle, this job is represented as an independent Airflow DAG configured to run on-demand (externally triggered).

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.BERT_AUSD_V_TA_PERIOD` | JOBS_UNIX | 1 (Active) | Mirror Carmen period definitions |

## 3. Scheduling
- **Schedule**: `None`
- **Trigger Assessment**: No scheduling wrapper (`JOBP`, `JSCH`, or `EVNT_TIME`) was supplied in this extraction bundle. The job has no calendar-based schedule of its own and must be assumed to be externally triggered (source unknown from this extraction alone).

## 4. Airflow DAG Properties
Since this is an orphaned UNIX job without a parent workflow, a dedicated standalone DAG wrapper has been structured to orchestrate its execution.

| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_bert_ausd_v_ta_period` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` (placeholder) |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` (Derived from Active=1) |
| **default_args** | `{'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)}` |

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dw_bert_ausd_v_ta_period` | `DW.BERT_AUSD_V_TA_PERIOD` | `EmptyOperator` | N/A | N/A | Inherited (1) | Inherited (5m) | None | None | N/A | None | # REVIEW-STRUCT: launcher command `[# [TRIMMED for the 3-file DE demo: ...]]` not recognised — confirm target operator/script manually. Script body points to executing: `&HOME/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh` |

## 6. Task Dependency Map
As this DAG contains a single task representing the orphaned UC4 UNIX job, the execution chain is:

```
dw_bert_ausd_v_ta_period
```

## 7. Sync / Concurrency Analysis
No sync rows or cross-DAG locking exclusions were defined for this object.

## 8. Error Handling and Retry Strategy
No custom postcondition or execution actions were specified in the extraction. Default Airflow task retry parameters are assigned.

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `&DWH_JOB_KENNUNG` | `'AUSD_V_TA_PERIOD'` | Environment variable or runtime parameter `DWH_JOB_KENNUNG` |
| `&HOME` | Environment Home Path | To be defined via Airflow Environment Variables or task context configuration |
| Sanitised DAG ID | `dw_bert_ausd_v_ta_period` | `dw_bert_ausd_v_ta_period` |

## 10. Developer Notes
* # REVIEW-STRUCT: Unrecognized launcher type detected. The original UC4 script body references a local script execution: `&HOME/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh`. The developer must migrate this shell script to run under an appropriate target execution operator (e.g., `BashOperator`, `SSHOperator`, or `KubernetesPodOperator`) and replace the `EmptyOperator` placeholder when target execution topology is finalized.
* # REVIEW: This extraction is an orphaned `JOBS_UNIX` task with no parent `JOBP` workflow or scheduling definition. It has been wrapped in a standalone Airflow DAG with `schedule=None`. Identify how this script is triggered in the wider UC4 environment and integrate it into the parent DAG or schedule accordingly.
* The UC4 script includes helper utilities `:inc DW.HOLE_PFAD` (path resolution) and `:inc DW.BERT_LESE_LOG` (log processing). These are standard environment scaffolding routines and must be replaced by native Airflow logging and connection configurations.

---

# PSEUDOCODE OUTLINE

```python
── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

── GCP Configuration ────────────────────────────────────
# No explicit GCP configurations required for this EmptyOperator-based stub.

── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

── on_failure_callback stubs ─────────────────────────────
# No custom failure callback objects specified in source extraction.

── DAG Definition ───────────────────────────────────────
# Standalone wrapper DAG for orphaned UC4 JOBS_UNIX object
with DAG(
    dag_id='dw_bert_ausd_v_ta_period',
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['migrated_uc4', 'standalone_jobs_unix'],
) as dag:

    ── Task: dw_bert_ausd_v_ta_period ───────────────────
    # # REVIEW-STRUCT: Original launcher command was unrecognized. 
    # Original target command: &HOME/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh
    # Original variable context: DWH_JOB_KENNUNG='AUSD_V_TA_PERIOD'
    # Action Required: Replace EmptyOperator with appropriate execution operator (e.g., BashOperator/SSHOperator/KubernetesPodOperator)
    # and map environment parameters appropriately.
    dw_bert_ausd_v_ta_period_task = EmptyOperator(
        task_id='dw_bert_ausd_v_ta_period',
    )

    ── Dependencies ─────────────────────────────────────
    # Single-task DAG. No dependencies to define.
    dw_bert_ausd_v_ta_period_task
```

### Execution Order
The target Apache Airflow orchestration must preserve the sequential invocation order specified in the legacy dependency graph:
1. **Airflow DAG Orchestrator**: `dags/dw_bert_ausd_v_ta_period.py` (representing the entry-point UC4 job definition `DW.BERT_AUSD_V_TA_PERIOD.xml`).
2. **KornShell Wrapper Execution**: Triggers the execution task for `r_ausd_v_ta_period.ksh`.
3. **Database Processing**: Which eventually executes the SQL logic in `d_ausd_v_ta_period.sql`.

### Schedule & Variables
* **Schedule**: No cron or calendar trigger is defined in the source XML; the DAG must be configured with `schedule=None` (triggered externally or on-demand).
* **Scheduler-set Variables**:
  - `DWH_JOB_KENNUNG`: A job-specific runtime identifier with the value `'AUSD_V_TA_PERIOD'`. It must be passed to the task execution environment.

### Lineage
* **Upstream/Includes (Human-Resolved)**:
  - `DW.HOLE_PFAD` — Confirmed as **NO SOURCE NEEDED** (retired/not needed in the target environment).
  - `DW.BERT_LESE_LOG` — Confirmed as **NO SOURCE NEEDED** (retired/not needed in the target environment).
* **Downstream Invocation**:
  - `r_ausd_v_ta_period.ksh` (job-level wrapper) — Invoked directly by the UC4 script. This represents a cross-file execution step owned by a sibling migration pass. The DAG task will reference this script as its execution target.

### Target File Plan
* **Target File Path**: `dags/dw_bert_ausd_v_ta_period.py`
  * **Language**: Python (Apache Airflow DAG)
  * **Source File**: `local/home/gurunathan_t/single_job_demo/DW.BERT_AUSD_V_TA_PERIOD.xml`

### Environment-Specific Values
* `HOME` (or `&HOME`): **GLOBAL** (environment-wide). Identifies the root execution directory of the target platform. Sourced at runtime via `os.environ.get("HOME")`.
* `DWH_JOB_KENNUNG`: **JOB-SPECIFIC**. Value is `'AUSD_V_TA_PERIOD'`, which is injected directly into the DAG task's environment dictionary.

---

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/single_job_demo/DW.BERT_AUSD_V_TA_PERIOD.xml` | `dags/dw_bert_ausd_v_ta_period.py` | Migrates the legacy UC4 UNIX job definition into a native Airflow DAG that schedules and orchestrates the downstream execution tasks. |

---

=== FILE: local/home/gurunathan_t/single_job_demo/d_ausd_v_ta_period.sql ===
-- ===================================================================
-- Datei:  d_ausd_v_ta_period.sql
-- Datum:  16.10.2002
-- Autor:  Martin Buettner
-- ===================================================================
--
-- Modifikationen
----------------------------------------------------------------------
-- Version Datum    Autor  Dokumentation
-- 3.1.0   20030109 sj     Tabellennamenerweiterung um das tagesdatum
--         20030115 mb     Modifikationen und Erweiterung gemaess Protokoll:
--                         (u.a.: Ausgabe der Rabatthoehe, Sperrgruende, Sperrzeiten)
-- 7.5.0   20040831 Roh    Neues Attr. is_production in CDS$TA_DISCOUNT
--                         Umstellung auf parallel degree 4
-- 7.5.1   20041011 mb     Modifikationen fuer den neuen Rabattreport
-- ab 2005 neue Releasenummern
-- 5.1.1   20050204 Roh    neues Feld SEGMENT_ID auf Rechnugsdefiniton
-- 5.4.0   20090901 Roh    spool ins Unterverzeichnis ./tmp
-- 5.4.1   20051025 mb     Erweiterung fuer Pooling Report: Rechnungsinhaltskonfigurationstext
--                                                          (inv_cont_config_id)
-- 5.4.2   20051117 mb     Ersetzung der ANALYZE Kommandos durch GATHER_TABLE_STATS
-- 5.4.3   20051119 hs     Mergen auf dwh$ta_p_vertrag_<datum> zum Addiern von Stillegungstagen
-- 5.4.4   20051202 hs     Laufzeittuning zu Step 201b (merge via FTS)
-- 6.1.0   20051220 Roh    Neues Feld 0B-Nummer (CDS$TA_CNTRCT.ORDER_NUMBER)
-- 6.3.0   20060919 me/hs  Für Berechnung Bindefrist mit Sperren/Stillegungen: Erweiterung um Sperrklasse 7
-- 6.4.0   20061122 me     Umstellung wegen Datenmodell-Aenderung in Carmen:
--                         Tab. CDS$TA_BARRIER_KIND_CV -> CDS$TA_BARRIER_KIND,
--                         tab. CDS$TA_DESCRIPTION     -> CDS$TA_CARE_DESCRIPTION
-- 6.4.0   20061121 RR     Bestimmung Substitutions-Variable v_datum aus
--                         Meldungstabelle (Eintrag BERT_DROP_TEMP_TABLE)
-- 6.4.1   20061124 RR     Überflüssige ANALYZE/STATISTICS Kommandos entfernt
-- 6.4.2   20061129 RR     Restartfähigkeit unter Verwendung bereits erzeugter Tabellen
-- 7.2.0   20070511 ME     NVL bei Setzen Bindefrist eingebaut (MERGE, step201b)
-- 7.4.0   20070814 AR/ME  Nach Umstellung 9i -> 10g:
--                         Steps 13a/b und 106a/b umgestellt auf TABLE-Function, hierzu neues Package
--                         Step 201b, MERGE: VALUES bei INSERT WHEN NOT MATCHED ergaenzt
-- 8.1.0   20071219 FD     Ausgliederung aus d_ausd_vertrag.sql
-- 10.2.1  20100428  Alicja Kubicka     CREATE TABLE...AS -> INSERT by SELECT, DROP TABLE -> TRUNCATE TABLE, &v_datum aus den Tabellename entfernt
----------------------------------------------------------------------

--
--
prompt variablendefinitionen
--
--
-- DB-Link auf CARMEN DB: entweder leer oder mit "@"
DEFINE v_carmen       = "@pcrs1.de.tinternal.com"
-- Stichtag ermitteln
COLUMN s_datum new_value v_datum noprint
SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum
  FROM isbert_schema.dwtk_meldungen m
 WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
--
--
prompt tracing und settings
--
--
START ../trace.sql.cfg
SPOOL ./tmp/trace_d_ausd_v_ta_period

WHENEVER SQLERROR CONTINUE
  SET TIMING ON
  SET SERVEROUTPUT ON
WHENEVER SQLERROR EXIT FAILURE
--
--
prompt tabelle von vorherigem lauf loeschen
--
--
WHENEVER SQLERROR CONTINUE
begin 
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_period'); 
end;
/

WHENEVER SQLERROR EXIT FAILURE
--
--
prompt zieltabelle anlegen: carmen-period-tabelle
--
--
INSERT  INTO sof$ta_period(
        period_id,
        number_time_measurement,
        time_meas_cv,
        einheit,
        bfc_age
)
SELECT
        p.period_id,
        p.number_time_measurement,
        p.time_meas_cv,
        d.description,
        p.insert_at
FROM
        cds$ta_period           &v_carmen       p,
        CDS$TA_TIME_MEAS_CV     &v_carmen       tm,
        cds$ta_description      &v_carmen       d
WHERE
        tm.time_meas_cv   = p.time_meas_cv
AND     tm.DESCRIPTION_ID = d.DESCRIPTION_ID
AND
        p.insert_at <= TO_DATE('&v_datum','YYYYMMDD')
AND     (   p.modified_at IS NULL
         OR p.modified_at > TO_DATE('&v_datum','YYYYMMDD'));

commit;

prompt Verarbeitung fehlerfrei beendet.
spool off


═══════════════════════════════════════════
SECTION 1 — DESIGN DOCUMENT
═══════════════════════════════════════════
 
Step 1: Understand the Script
1.1 Identify the type of Oracle SQL object being converted:
    - This is a multi-statement Oracle SQL*Plus script. It contains dynamic variable initialization, dynamic PL/SQL execution (via a package call to truncate a table), and a cross-database link INSERT-SELECT statement.
 
1.2 Business Logic and Purpose:
    - The script identifies a business "reporting cut-off date" (`v_datum`) based on the latest execution timestamp of a drop job registered in the tracking table `isbert_schema.dwtk_meldungen`.
    - It then empties the target period table `sof$ta_period` (via a dynamic utility package `DWPA_UTIL_SKRIPT`).
    - Finally, it populates this target table with active historical period metadata pulled from a remote database instance (accessed via the `@pcrs1.de.tinternal.com` database link) where the records were inserted on or before the cut-off date and either have not been modified or were modified after the cut-off date.
 
1.3 Entities Referenced:
    - `isbert_schema.dwtk_meldungen` (Source table)
      - `timecreated`: DATE (Oracle Date containing time)
      - `job_kennung`: VARCHAR2/STRING
    - `isbert_schema.DWPA_UTIL_SKRIPT` (PL/SQL Utility Package)
    - `sof$ta_period` (Target table)
      - `period_id`: NUMBER / INT64
      - `number_time_measurement`: NUMBER / INT64
      - `time_meas_cv`: VARCHAR2 / STRING
      - `einheit`: VARCHAR2 / STRING
      - `bfc_age`: DATE / DATETIME
    - `cds$ta_period@pcrs1.de.tinternal.com` (Remote source table alias `p`)
      - `period_id`: NUMBER
      - `number_time_measurement`: NUMBER
      - `time_meas_cv`: VARCHAR2
      - `insert_at`: DATE
      - `modified_at`: DATE
    - `CDS$TA_TIME_MEAS_CV@pcrs1.de.tinternal.com` (Remote source lookup table alias `tm`)
      - `time_meas_cv`: VARCHAR2
      - `DESCRIPTION_ID`: NUMBER
    - `cds$ta_description@pcrs1.de.tinternal.com` (Remote source lookup table alias `d`)
      - `DESCRIPTION_ID`: NUMBER
      - `description`: VARCHAR2

Step 2: Oracle-Specific Construct Detection and Resolution
 
2.1 Data Type Conversions:
    - Oracle `DATE` (`timecreated`, `insert_at`, `modified_at`) maps to BigQuery `DATETIME` to preserve both the date and time components without time-zone overhead.
    - Oracle `NUMBER` maps to `INT64` for identifiers / measurements (`period_id`, `number_time_measurement`, `DESCRIPTION_ID`).
    - Oracle `VARCHAR2` maps to `STRING`.
 
2.2 Implicit and Explicit Type Casting:
    - Oracle's implicit conversion of the variable `v_datum` (string) to date during comparison is resolved to an explicit `PARSE_DATETIME` operation in BigQuery.
 
2.3 NULL Handling and Conditional Functions:
    - `NVL(TO_CHAR(...), '19000101')` is converted to `COALESCE(FORMAT_DATETIME('%Y%m%d', ...), '19000101')`.
 
2.4 String Functions:
    - `TO_CHAR(date, 'YYYYMMDD')` is resolved to `FORMAT_DATETIME('%Y%m%d', date)`.
 
2.5 Date and Timestamp Functions:
    - `TO_DATE('&v_datum', 'YYYYMMDD')` is resolved to `PARSE_DATETIME('%Y%m%d', v_datum)`.
 
2.8 Set and Join Operations:
    - The cross-database link syntax (`@pcrs1.de.tinternal.com`) is incompatible with BigQuery. This requires a dedicated data-ingestion step where the remote tables are staged into BigQuery beforehand (represented by a configured dataset prefix such as `carmen_stage`).
 
2.10 Sequences:
    - Not applicable.
 
2.14 PL/SQL Block:
    - The anonymous block calling `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` to execute a truncate is resolved to a direct, native BigQuery DML `TRUNCATE TABLE` statement.
 
2.15 Unresolvable or Advisory Items:
    - **Database Link (`@pcrs1.de.tinternal.com`)**: Cannot be natively resolved in a standard SQL statement query. The remote data must be replicated to BigQuery prior to the execution of this script.
    - **SQL*Plus Commands / Spooling**: `DEFINE`, `COLUMN`, `START`, `SPOOL`, and `SET` commands are client-side functions and are stripped. Variable declaration and assignment are converted into BigQuery Scripting syntax (`DECLARE`, `SET`).

Step 3: Conversion Strategy Summary
3.1 Overall Conversion Approach:
    - Translate the multi-statement SQL*Plus logic into a single BigQuery Scripting block (`BEGIN ... END;`).
    - Declare a local script variable `v_datum` of type `STRING`.
    - Retrieve and assign the cut-off date using a standard query.
    - Execute a direct `TRUNCATE TABLE` on the target BigQuery table.
    - Perform the final `INSERT INTO` select using staged versions of the remote tables.

3.2 Assumptions:
    - The remote Oracle tables accessed via the `@pcrs1.de.tinternal.com` database link are pre-replicated into BigQuery under a staging dataset named `carmen_stage`.
    - BigQuery target dataset is named `isbert_schema` (or customized based on target schema rules).

3.3 Items Flagged for Human Review:
    - The data replication pipeline of the remote CARMEN tables (`cds$ta_period`, `CDS$TA_TIME_MEAS_CV`, and `cds$ta_description`) must be established before running this script in BigQuery.

═══════════════════════════════════════════
MIGRATION DECISION AND REVIEW REPORTING
═══════════════════════════════════════════

2.16 MIGRATION DECISION MATRIX

| Source SQL Block/Construct | Selected Target | Rejected Alternatives | Evidence / Reason for Selection |
| :--- | :--- | :--- | :--- |
| **SQL\*Plus Variables & Spooling** | BigQuery Scripting (`DECLARE`, `SET`) | Python Wrapper | BigQuery Scripting native variables are clean, require less overhead, and maintain SQL-centric orchestration. |
| **DWPA_UTIL_SKRIPT.runstatement (Truncate)** | Native `TRUNCATE TABLE` | BigQuery UDF, Python Wrapper | A native `TRUNCATE TABLE` DML is the standard, safest, and most performant way to empty tables in BigQuery. |
| **Oracle Database Link (`@pcrs1...`)** | Pre-staged BigQuery datasets | Direct Federated Querying | DB Links are unsupported natively. Pre-staging ensures query performance and eliminates database-link latency during transformation. |

2.17 REQUIRED ARTIFACTS

| Generated Artifact | Type | Inputs / Dependency | Target Path / Destination |
| :--- | :--- | :--- | :--- |
| **BigQuery SQL Script** | BigQuery Standard SQL Script | Staged remote tables in `carmen_stage` dataset, source `dwtk_meldungen` table. | `isbert_schema.d_ausd_v_ta_period.sql` |

2.18 DATA TYPE COMPATIBILITY TABLE

| Oracle Source Type | BigQuery Target Type | Conversion Rule | Warnings / Implications |
| :--- | :--- | :--- | :--- |
| **DATE** (with time component) | **DATETIME** | `DATETIME` | Preserves both calendar date and precise time elements. |
| **NUMBER** (no scale specified) | **INT64** or **NUMERIC** | `CAST` / Direct mapping | Maps to `INT64` for identifiers and counts. |
| **VARCHAR2** | **STRING** | Direct mapping | Standard text conversion without maximum byte limitations. |

2.19 DESIGN REVIEW SUMMARY
- **Patterns/Objects Found**: SQL*Plus directives, Variable binding, Dynamic DB link references, PL/SQL Dynamic Truncate utility.
- **Unsupported Functions**: DB Link (`@dblink`), SQL*Plus commands (`SPOOL`, `SET`, `COLUMN`).
- **UDF Required**: No.
- **Python Required**: No (assuming replication pipeline is a decoupled prerequisites task, otherwise yes to fetch remote Oracle data).
- **Direct Dependencies**: `isbert_schema.dwtk_meldungen`, `carmen_stage.cds_ta_period`, `carmen_stage.cds_ta_time_meas_cv`, `carmen_stage.cds_ta_description`.
- **Assumptions**: Staged source tables have been fully replicated to BigQuery dataset `carmen_stage`.

OVERALL MIGRATION STRATEGY: Direct BigQuery SQL

2.21 ORACLE FUNCTION ANALYSIS TABLE

| Oracle Function/Construct | Supported in BigQuery | BigQuery Equivalent / Alternative |
| :--- | :--- | :--- |
| `NVL` | Direct-with-rewrite | `COALESCE` |
| `TO_CHAR(date, 'YYYYMMDD')` | Direct-with-rewrite | `FORMAT_DATETIME('%Y%m%d', ...)` |
| `TO_DATE(str, 'YYYYMMDD')` | Direct-with-rewrite | `PARSE_DATETIME('%Y%m%d', ...)` |
| `@pcrs1.de.tinternal.com` (DB Link) | Unsupported | Pre-stage external data to BQ dataset (e.g. `carmen_stage.table`) |
| `runstatement(...)` PL/SQL | Direct-with-rewrite | Native `TRUNCATE TABLE` statement |

═══════════════════════════════════════════
SECTION 2 — PSEUDOCODE
═══════════════════════════════════════════
 
Step 4: Write Vendor-Neutral Pseudocode

```sql
BEGIN
  -- Declare Script Variables (Converted from SQL*Plus DEFINE and COLUMN declarations)
  DECLARE v_datum STRING;

  -- Step 1: Resolve reporting cut-off date (Converted from TO_CHAR and NVL block)
  SET v_datum = (
    SELECT 
      COALESCE(
        FORMAT_DATETIME('%Y%m%d', MAX(m.timecreated)), -- converted from NVL and TO_CHAR
        '19000101'
      )
    FROM 
      isbert_schema.dwtk_meldungen m
    WHERE 
      m.job_kennung = 'BERT_DROP_TEMP_TABLE'
  );

  -- Step 2: Empty the target period table (Converted from DWPA_UTIL_SKRIPT.runstatement)
  TRUNCATE TABLE isbert_schema.sof_ta_period;

  -- Step 3: Populate Target Table (Converted from Insert Statement and DB Link calls)
  INSERT INTO isbert_schema.sof_ta_period (
    period_id,
    number_time_measurement,
    time_meas_cv,
    einheit,
    bfc_age
  )
  SELECT
    p.period_id,
    p.number_time_measurement,
    p.time_meas_cv,
    d.description,
    p.insert_at
  FROM
    -- Converted DB links to pre-staged BigQuery dataset tables
    carmen_stage.cds_ta_period p
  INNER JOIN
    carmen_stage.cds_ta_time_meas_cv tm
    ON tm.time_meas_cv = p.time_meas_cv
  INNER JOIN
    carmen_stage.cds_ta_description d
    ON tm.description_id = d.description_id
  WHERE
    -- Converted TO_DATE comparisons to explicit BQ DATETIME parsing
    p.insert_at <= PARSE_DATETIME('%Y%m%d', v_datum)  -- converted from TO_DATE('&v_datum','YYYYMMDD')
    AND (
      p.modified_at IS NULL
      OR p.modified_at > PARSE_DATETIME('%Y%m%d', v_datum)  -- converted from TO_DATE('&v_datum','YYYYMMDD')
    );

END;
```

═══════════════════════════════════════════
FLAGGED ITEMS FOR HUMAN REVIEW
═══════════════════════════════════════════
1. **DB Link Migration Dependency**: The tables prefixed with `carmen_stage.` (`cds_ta_period`, `cds_ta_time_meas_cv`, and `cds_ta_description`) represent the migrated counterparts of the Oracle `@pcrs1.de.tinternal.com` database-linked tables. The build and migration orchestrator must ensure these datasets are fully extracted and loaded into BigQuery before executing this script.
2. **Dynamic Truncate Utility**: The custom Oracle package `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_period')` was mapped directly to a native BigQuery `TRUNCATE TABLE` statement. This is functionally equivalent but bypasses any execution logging or transaction control built into that custom Oracle package.

### Execution Order
The target orchestration (Airflow/Cloud Composer DAG) must preserve the execution sequence from the legacy dependency graph:
* **Step 1:** Orchestration configuration and job definition metadata check (`DW.BERT_AUSD_V_TA_PERIOD.xml`).
* **Step 2:** KornShell wrapper execution (`r_ausd_v_ta_period.ksh`).
* **Step 3:** Main transformation execution (executes `d_ausd_v_ta_period.sql`, which is mapped to the BigQuery script).

### Schedule & Variables
The target environment must capture and pass the scheduler-set variable through its native orchestration parameters:
* **Scheduler-Set Variable:** `DWH_JOB_KENNUNG` = `'AUSD_V_TA_PERIOD'`
* **Target Mechanism:** Airflow DAG environment parameter or configuration block, accessible via `Variable.get("DWH_JOB_KENNUNG")` or task parameters.

### Lineage
* **Upstream Producers (Inputs):**
  * Table `isbert_schema.dwtk_meldungen` — read to retrieve the maximum timestamp (`timecreated`) for the watermark where `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
  * Table `cds$ta_period` (queried via Oracle DB Link `@pcrs1.de.tinternal.com`) — source period metadata.
  * Table `CDS$TA_TIME_MEAS_CV` (queried via Oracle DB Link `@pcrs1.de.tinternal.com`) — source time measurement lookup.
  * Table `cds$ta_description` (queried via Oracle DB Link `@pcrs1.de.tinternal.com`) — source description lookup.
* **Downstream Consumers (Outputs):**
  * Table `isbert_schema.sof$ta_period` — target operational table truncated and loaded by this script.
* **External Package Dependency:**
  * Package `DWPA_UTIL_SKRIPT` (specifically `runstatement`) — Oracle utility called to execute dynamic truncate statements on the target table.

### Cross-File Dependencies
* This script shares dependencies with the watermark tracking table `isbert_schema.dwtk_meldungen`, which is written/updated by other preceding modules in the `BERT` suite.
* It relies on an external ingestion pipeline to extract and pre-stage the remote tables (accessed via `@pcrs1.de.tinternal.com`) into BigQuery beforehand.

### Target File Plan
* **Target File Path:** `local/home/gurunathan_t/single_job_demo/d_ausd_v_ta_period.sql`
  * **Language:** BigQuery Standard SQL Scripting (SQL)
  * **Source File:** `local/home/gurunathan_t/single_job_demo/d_ausd_v_ta_period.sql`
  * **Purpose:** Implements the main SQL transformation block using scripting variables, a native `TRUNCATE` statement, and an `INSERT INTO ... SELECT` query joining the pre-staged BigQuery staging tables.

### Environment-Specific Values
The environment values are classified by role in the target architecture and must be resolved dynamically at runtime rather than hardcoded:

1. **GLOBAL (Environment-Wide):**
   * `GCP_PROJECT`: Identifies the target GCP Project ID. Sourced at runtime using query parameters or Airflow DAG parameters.
   * `BQ_DATASET` (replaces legacy schema `isbert_schema`): The operational BigQuery dataset where the output table resides. Normalized to `BQ_DATASET` and resolved at runtime.
   * `CARMEN_STAGE_DATASET` (replaces Oracle DB Link `@pcrs1.de.tinternal.com`): The staging dataset in BigQuery that houses the replicated source tables. Normalized to `CARMEN_STAGE_DATASET` and resolved at runtime.

2. **JOB-SPECIFIC:**
   * Target Table Name: `sof_ta_period` (formerly `sof$ta_period`). Real value populated inline.
   * Tracking Table Name: `dwtk_meldungen`. Real value populated inline.
   * Staged Source Tables: `cds_ta_period`, `cds_ta_time_meas_cv`, `cds_ta_description`. Real values populated inline within the query referencing `CARMEN_STAGE_DATASET`.

---

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/single_job_demo/d_ausd_v_ta_period.sql` | `local/home/gurunathan_t/single_job_demo/d_ausd_v_ta_period.sql` | Converted to BigQuery SQL scripting block containing local variable declaration, target truncation, and explicit date casting to load period metadata from pre-staged sources. |

---

### Risks & Manual Actions
* **Upstream Table Ingestion:** The source script queries active period records directly from a remote instance using an Oracle DB Link (`@pcrs1.de.tinternal.com`). Because database links are not natively supported in BigQuery, an ingestion pipeline must extract the remote tables (`cds$ta_period`, `CDS$TA_TIME_MEAS_CV`, and `cds$ta_description`) and pre-stage them into the target `CARMEN_STAGE_DATASET` prior to script execution.
* **Watermark Coordination:** The query relies on reading the reporting date (`v_datum`) from the tracking table `isbert_schema.dwtk_meldungen`. Upstream schedule orchestrations must guarantee that the job writing this watermark has successfully completed before running `d_ausd_v_ta_period.sql`.
* **Dynamic Truncate Utility:** The legacy PL/SQL utility package call `DWPA_UTIL_SKRIPT.runstatement` has been simplified to a native BigQuery `TRUNCATE TABLE` statement. Any framework logging, access controls, or transaction logic managed by the Oracle utility will be bypassed and must be handled at the orchestrator level.

---

=== CONFIRMED TARGET PLATFORM ===
TARGET_PLATFORM: BIGQUERY
This is stated directly by the caller, not inferred from the extraction below. Treat it as ground truth everywhere the design/build instructions would otherwise infer the platform or fall back to a default -- it satisfies the "unless explicitly stated in the extraction" condition those rules already reference. Never emit "# REVIEW: target database platform not specified" while this section is present.

=== FILE: local/home/gurunathan_t/single_job_demo/r_ausd_v_ta_period.ksh ===
#!/bin/ksh

# Zweck:
#    Rahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_period
#
# Erzeugt am: 20.12.2007
# Versions-Anmerkungen:
#    1.0.0;20.10.2007;Fabian Debus
#
ProgName="Vertragsdatenabgleich"
ProgVersion="V1.0.0"

#####################################
# Funktion:
#    usage - Ausgabe der Programmbeschreibung
usage(){
cat <<EOF
    Programm: $ProgName
    Version:  $ProgVersion
    Aufruf:   $0 Parameter
    Parameter:
	-h     zeigt diese Seite an

    Beschreibung:
        Rahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_period.
EOF
}


##########################
# Vorbereitende Massnahmen
#    Einlesen der Umgebung
# [TRIMMED for the 3-file DE demo: ". $HOME/.dw_init" removed here --
#  framework env bootstrap, not this job's business logic; .dw_init is not
#  one of the 3 files in this demo. See the untrimmed chain under
#  isrpt/isbert/SQL/aktuell/... in ~/data for the real dot-source.]


#    Fehlerkonzept einschalten
# [TRIMMED: ". ${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh"
#  removed -- error-framework helper, not included in this demo.]

set -eu

ErrNr=0
ErrArg=""

# Globale Fehlerbehandlung
ErrVal=0

DW_EintragsNr=0

# [TRIMMED: ". ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh"
#  removed -- parameter-parsing helper, not included in this demo.]
# [TRIMMED: ". ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh"
#  removed -- date-helper, not included in this demo.]

#####################
# Lesen der Parameter
ParamList="s:l:" # Notation gemaess getopts(1)

# lese mit Hilfe getopts die Parameter
while getopts ":h$ParamList" param
do
    case $param in
        h)
            usage
            exit;;
        :)
            ErrNr=193  # Notwendiges Argument fehlt
            ErrArg="$OPTARG";;
        ?)
            ErrNr=192  # Parameter unbekannt
            ErrArg="$OPTARG";;
    esac
done


# Falls Fehler aufgetreten, abbrechen
if [ ! $ErrNr -eq 0 ]
then
    #Ausgabe gemaess Fehlerkonzept
    DWMSG_MeldeFehler $DW_EintragsNr E $ErrNr $ErrArg
    usage
    #Austieg gemaess Nummernkreisen
    exit $ErrNr
fi

# [TRIMMED: Name_Kernskript="${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_period.ksh"
#  removed -- in the real chain this pointed at a SEPARATE control script
#  (k_ausd_v_ta_period.ksh) that is not one of this demo's 3 files. Its real
#  SQL-invocation business logic is inlined below instead of being called
#  out to a second file, so the actual DB step is preserved, just merged
#  into this single script.]

####################
# Fehlermeldekonzept
####################
typeset -u JobKennung="BERT_V_TA_PERIOD"
typeset -u v_sysdate=$(date +%d%m%Y)

DWMSG_ErmittleNr DW_EintragsNr
DWMSG_Logdateiname LogDatei $JobKennung $DW_EintragsNr
DWMSG_ErzeugeEintrag $DW_EintragsNr $JobKennung $0 \
                     $LogDatei >> $LogDatei 2>&1
DWMSG_SetzeStichtagInfo $DW_EintragsNr $v_sysdate 'DDMMYYYY'

# Setze traps#
trap "DWMSG_Fehlerbehandlung $DW_EintragsNr >> \$LogDatei 2>&1; echo 'OSError: Abbruch'; exit 1" INT
trap "DWMSG_Fehlerbehandlung $DW_EintragsNr >> \$LogDatei 2>&1; echo 'AppError: Abbruch'" ERR

print " ----------------- Job -----------------------"
print " Job-Nr    : '$DW_EintragsNr'"
print " JobKennung: '$JobKennung'"
print " Logdatei  : '$LogDatei'"
print " ---------------------------------------------"

# ---------------------------------------------------------------------------
# Inlined from the real k_ausd_v_ta_period.ksh (its Kontrollscript/SQL-invocation
# body -- this demo merges it here instead of calling it out as a second
# file). Bridge assignments map this script's already-parsed variables onto
# k_ausd_v_ta_period.ksh's own original argument names ($p_JobKennung/
# $p_EintragsNr, normally passed in via "-j $JobKennung -f $DW_EintragsNr"),
# so the real body below is kept byte-for-byte identical to the source script.
p_JobKennung=$JobKennung
p_EintragsNr=$DW_EintragsNr

# setze Tabellenname
v_TabName='ta_period'

# Pruefe, ob notwendige Parameter gesetzt worden sind
# Abruchskontrolle ausschalten
    set +e

    ErrNr=0
    ErrArg=""

    pruefeParameterGesetzt Jobkennung p_JobKennung
    pruefeParameterGesetzt EintragsNr p_EintragsNr

    # Falls Fehler aufgetreten, abbrechen
    if [ ! $ErrNr -eq 0 ]
    then
	#Ausgabe gemaess Fehlerkonzept
	DWMSG_MeldeFehler 0 E $ErrNr "$ErrArg"
	echo "FEHLER: 0 E $ErrNr $ErrArg"
        print "Bitte ueber Rahmenscript aufrufen";
	#Austieg gemaess Nummernkreisen
	exit $ErrNr
    fi

# Abruchskontrolle einschalten
set -eu

# [TRIMMED: ". ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh"
#  removed -- defines the starteSQLSkript helper function called below;
#  that helper file is not included in this demo's 3 files.]

# SQL-Skript
Name_SQLskript="${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_period.sql"

# Temporares File fuer die Zahl der Records
tmpFile="$DW_DIR_UTL/bert_k_ausd_v_ta_period_$$.tmp"

# *******************************************************

# DB-Script ausfuehren
# hierbei werden aktive Jobs ignoriert
starteSQLSkript $p_EintragsNr $Name_SQLskript $p_EintragsNr $p_JobKennung

print " ---------- ENDE Datenverarbeitung ----------"

# Hole Zahl der Bereitgestellten Records
eval "v_records=`cat $tmpFile`"
# ---------------------------------------------------------------------------

# hier kommt das Skript nur an, wenn alles OK war
print "Die Abarbeitung wurde ohne erkennbare Fehler beendet" | tee -a $LogDatei
DWMSG_SetzeStatusOK $DW_EintragsNr >> $LogDatei 2>&1

trap - INT ERR

exit 0


=== CONVERSION VERDICT ===
VERDICT: PYTHON
REASON: The script contains complex KornShell orchestration including command-line parameter parsing, custom error trapping, local temporary file reading, and invocation of an external SQL execution helper whose source is not supplied in this extraction.

EVIDENCE
- Business logic found: KSH custom logic performs getopts parameter parsing, system/job metadata initialization, environment verification, executes an external SQL script (`d_ausd_v_ta_period.sql`) via a launcher, and captures the resulting record count from a temporary file.
- AWK: none
- SQL-expressible: No. While the core database transformation resides in the unsupplied `d_ausd_v_ta_period.sql` script, the orchestrating wrapper logic requires a host environment with parameter parsing, error trapping, file I/O, and custom status-logging framework integrations (`DWMSG_*`).
- Non-SQL side effects: Capturing and reading a process-specific temporary file (`$tmpFile`) and calling external monitoring/logging framework commands (`DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, etc.).
- Against this verdict: One could attempt to convert the wrapper and SQL to a single BigQuery scripting block, but the external status-logging framework calls and dynamic file-based metric capture cannot be natively expressed inside BigQuery Standard SQL without a Python orchestrator.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

### 1. SCRIPT OVERVIEW
The script `r_ausd_v_ta_period.ksh` serves as an orchestration and monitoring wrapper ("Rahmenskript") for reconciling contract data within the database table `ta_period`. It validates input parameters, registers the execution status with a corporate monitoring framework (`DWMSG`), triggers the execution of an external SQL script (`d_ausd_v_ta_period.sql`), extracts execution metrics (processed record count) from a temporary file, and registers the final job status (success or failure).

### 2. INVOCATION CONTEXT
*   **Caller**: Typically invoked by a UC4/Automic job scheduler (e.g., using a `JOBS_UNIX` object).
*   **Command Line Arguments**: Positional parameters parsed via `getopts ":h$ParamList"`.
*   **UC4 Includes**: None referenced in this extraction.
*   **Environment Files Sourced**:
    *   `. $HOME/.dw_init` (Trimmed in extraction) — `# REVIEW-STRUCT: environment file [.dw_init] not supplied — variables it sets are unknown; do not guess their names or values`
    *   `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` (Trimmed in extraction) — `# REVIEW-STRUCT: environment file [f_alis_msgerr.ksh] not supplied — variables/functions it sets are unknown`
    *   `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` (Trimmed in extraction) — `# REVIEW-STRUCT: environment file [h_alis_parameter.ksh] not supplied — variables/functions it sets are unknown`
    *   `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` (Trimmed in extraction) — `# REVIEW-STRUCT: environment file [h_alis_date.ksh] not supplied — variables/functions it sets are unknown`

### 3. PARAMETERS / INPUTS
*   `ParamList="s:l:"`: Declared options for `getopts`.
    *   `-s`: Option requiring an argument. Used in getopts but not explicitly assigned to a variable in the visible script body. `# REVIEW: parameter -s is declared in getopts but not explicitly handled; confirm actual usage.`
    *   `-l`: Option requiring an argument. Used in getopts but not explicitly assigned to a variable in the visible script body. `# REVIEW: parameter -l is declared in getopts but not explicitly handled; confirm actual usage.`
*   `JobKennung`: Hardcoded as `"BERT_V_TA_PERIOD"`. Forced to uppercase using `typeset -u`.
*   `v_sysdate`: Formatted date string (`DDMMYYYY`) generated via `date +%d%m%Y`.
*   `LogDatei`: Variable populated by the legacy framework call `DWMSG_Logdateiname LogDatei $JobKennung $DW_EintragsNr`.
*   `tmpFile`: Path to temporal record-count file. Formatted as `"$DW_DIR_UTL/bert_k_ausd_v_ta_period_$$.tmp"`. Uses environment variable `DW_DIR_UTL` and process ID `$$`.
*   `Name_SQLskript`: Path to the SQL execution target, set to `"${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_period.sql"`.

### 4. EXTERNAL COMMANDS / PROGRAMS INVOKED
*   `DWMSG_ErmittleNr`: Status framework utility used to fetch a run ID into `DW_EintragsNr`.
*   `DWMSG_Logdateiname`: Status framework utility used to populate `LogDatei`.
*   `DWMSG_ErzeugeEintrag`: Registers the start of the job.
*   `DWMSG_SetzeStichtagInfo`: Registers the execution date.
*   `DWMSG_Fehlerbehandlung`: Triggered on error traps (`INT`/`ERR`) to log failure details.
*   `DWMSG_MeldeFehler`: Logs a validation error to the framework database/file.
*   `DWMSG_SetzeStatusOK`: Sets the final status to OK on successful completion.
*   `pruefeParameterGesetzt`: Standard parameter-validation utility.
*   `starteSQLSkript`: Helper function used to invoke the target SQL script against the database.
    *   *Verbatim Command Line*: `starteSQLSkript $p_EintragsNr $Name_SQLskript $p_EintragsNr $p_JobKennung`
    *   *Purpose*: Executes the specified SQL script using standard credentials and logs execution statistics.
    *   *Resolution*: This launcher is an opaque external framework command.
    *   `# REVIEW-STRUCT: launcher [starteSQLSkript] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion`
    *   *BigQuery Mapping*: Since the target platform is confirmed as BigQuery, this launcher should be refactored to read the contents of `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_period.sql` and execute them via `google.cloud.bigquery.Client().query()`.

### 5. EMBEDDED SQL
*   **Source File**: External script referenced at `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_period.sql`
*   **Full SQL Text**: Not supplied in extraction.
*   **Statement Type**: Expected to be DML/DDL modifying the `ta_period` table (e.g., `MERGE`, `INSERT`, `UPDATE`).
*   **Tables Touched**: `ta_period`.
*   **Dialect**: The target database platform is explicitly confirmed as **BIGQUERY**. Any SQL statements inside `d_ausd_v_ta_period.sql` must be written in BigQuery Standard SQL.

### 6. CONTROL FLOW
1.  **Environment Initialization**: Initialize `ErrNr`, `ErrArg`, `ErrVal`, and `DW_EintragsNr` to `0` or empty.
2.  **Command-Line Option Parsing**: Parse command line arguments via `getopts`. If invalid parameters are encountered, set `ErrNr` (192 or 193) and call `DWMSG_MeldeFehler` / exit.
3.  **Job Identity Definition**: Set `JobKennung` to `"BERT_V_TA_PERIOD"` and dynamic date `v_sysdate` to current date (`DDMMYYYY`).
4.  **Monitoring Framework Registration**:
    *   Fetch a unique tracking number (`DW_EintragsNr`).
    *   Generate log file path (`LogDatei`).
    *   Register the execution instance via `DWMSG_ErzeugeEintrag` and redirect standard streams to the log file.
    *   Assign the tracking execution key date via `DWMSG_SetzeStichtagInfo`.
5.  **Signal Traps Setup**: Establish `INT` and `ERR` traps to execute `DWMSG_Fehlerbehandlung` and exit with `1` on failure.
6.  **Parameter Verification**:
    *   Temporarily disable immediate shell termination (`set +e`).
    *   Verify `p_JobKennung` and `p_EintragsNr` are correctly populated using `pruefeParameterGesetzt`.
    *   If missing, call `DWMSG_MeldeFehler` and exit with failure code.
    *   Restore immediate shell termination (`set -eu`).
7.  **Paths and Targets Definition**: Set `Name_SQLskript` and `tmpFile` (incorporating process PID).
8.  **Database Script Execution**: Execute `starteSQLSkript` with tracking and script variables.
9.  **Record Count Parsing**: Read the content of the temporary metric file `tmpFile` into `v_records`.
10. **Success Registration**:
    *   Print success message to console and append to `LogDatei`.
    *   Register status OK via `DWMSG_SetzeStatusOK`.
    *   Disable traps and exit `0`.

### 7. ERROR HANDLING & EXIT CODES
*   **Detection**: Handled via `set -eu` and trap registrations on `INT` and `ERR` signals.
*   **Behavior**: When a step fails, the `ERR` trap executes `DWMSG_Fehlerbehandlung` to write context to the log file, prints `"AppError: Abbruch"`, and propagation occurs. If argument parsing fails, the script exits with code `192` or `193`.
*   **Success Code**: `0`.
*   **Python Translation**:
    *   Use a structured `try...except...finally` block to capture exceptions and emulate traps.
    *   Utilize standard Python logging or Cloud Logging instead of redirection blocks (`>> $LogDatei 2>&1`).
    *   Explicitly handle subprocess execution errors with `subprocess.CalledProcessError` or BigQuery client exceptions.

### 8. OUTPUTS / SIDE EFFECTS
*   **Database**: Modifications to BigQuery table `ta_period`.
*   **Logs**: Writes logs to `LogDatei` (dynamically determined).
*   **Temporary Files**: Reads metrics from `$tmpFile` (dynamically determined, expected to be generated during the execution of `starteSQLSkript` or the SQL file).

### 9. BUSINESS SUMMARY
*   Provides a standardized entry point for executing contract alignment routines against the `ta_period` table.
*   Integrates execution tracking with a centralized monitoring system (`DWMSG`), registering start times, key run dates, and completion status.
*   Triggers the core database processing sequence via an external SQL routine.
*   Captures and records performance metrics (number of processed records) from filesystem state files.
*   Secures execution stability through robust error traps and parameter validation guards.

=======================================================================================
PYTHON PSEUDOCODE
=======================================================================================

```python
# Step 1: Import required modules
import os
import sys
import datetime
import argparse
import subprocess
import shutil

# REVIEW-STRUCT: environment file [.dw_init / f_alis_msgerr.ksh / h_alis_parameter.ksh / h_alis_date.ksh] not supplied — variables/functions they set are unknown; do not guess their names or values
# Standard imports and setup logic must mimic the required framework behavior.

# Step 2: Initialize global tracking variables
err_nr = 0
err_arg = ""
dw_eintrags_nr = 0
job_kennung = "BERT_V_TA_PERIOD"
v_sysdate = datetime.datetime.now().strftime("%d%m%Y")

# Step 3: Parse command line arguments
# List of expected parameters derived from ParamList="s:l:"
parser = argparse.ArgumentParser(description="Rahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_period")
parser.add_init = False
parser.add_argument("-s", dest="param_s", help="Parameter S") # REVIEW: declared but usage not verified in script body
parser.add_argument("-l", dest="param_l", help="Parameter L") # REVIEW: declared but usage not verified in script body
# Note: original getopts allows -h which displays usage. argparse handles -h natively.

try:
    args = parser.parse_args()
except Exception as e:
    err_nr = 192 # Parameter unknown or missing
    err_arg = str(e)
    # Perform legacy error logging via subprocess wrapper or framework call
    # DWMSG_MeldeFehler(dw_eintrags_nr, "E", err_nr, err_arg)
    sys.exit(err_nr)

# Step 4: Metadata and tracking setup via legacy framework
# These represent legacy status tracking functions. Map to subprocess or Python equivalent if library is available.
# DWMSG_ErmittleNr(byref dw_eintrags_nr)
# DWMSG_Logdateiname(byref log_datei, job_kennung, dw_eintrags_nr)
# DWMSG_ErzeugeEintrag(dw_eintrags_nr, job_kennung, sys.argv[0], log_datei)
# DWMSG_SetzeStichtagInfo(dw_eintrags_nr, v_sysdate, 'DDMMYYYY')

# Mock paths for translation representation
log_datei = f"/tmp/{job_kennung}_{v_sysdate}.log" 
dw_eintrags_nr = 12345  # Inferred mock track ID

# Step 5: Define execution paths and variables
p_job_kennung = job_kennung
p_eintrags_nr = dw_eintrags_nr
v_tab_name = "ta_period"

# Step 6: Parameter Validation Guard Block
# Porting logic: 'pruefeParameterGesetzt Jobkennung p_JobKennung'
if not p_job_kennung:
    err_nr = 1  # Example error code
    # DWMSG_MeldeFehler(0, "E", err_nr, "Jobkennung nicht gesetzt")
    print("FEHLER: Jobkennung nicht gesetzt. Bitte ueber Rahmenscript aufrufen", file=sys.stderr)
    sys.exit(err_nr)

if not p_eintrags_nr:
    err_nr = 1  # Example error code
    # DWMSG_MeldeFehler(0, "E", err_nr, "EintragsNr nicht gesetzt")
    print("FEHLER: EintragsNr nicht gesetzt. Bitte ueber Rahmenscript aufrufen", file=sys.stderr)
    sys.exit(err_nr)

# Step 7: Define script and temporary record count target paths
bert_dir_root = os.environ.get("BERT_DIR_ROOT", "/opt/bert")
dw_dir_utl = os.environ.get("DW_DIR_UTL", "/tmp")
name_sql_skript = os.path.join(bert_dir_root, "aufbereitung/sql/d_ausd_v_ta_period.sql")
pid = os.getpid()
tmp_file = os.path.join(dw_dir_utl, f"bert_k_ausd_v_ta_period_{pid}.tmp")

# Step 8: DB Execution & Error Trapping block
try:
    print(" ----------------- Job -----------------------")
    print(f" Job-Nr    : '{dw_eintrags_nr}'")
    print(f" JobKennung: '{job_kennung}'")
    print(f" Logdatei  : '{log_datei}'")
    print(" ---------------------------------------------")

    # Execute SQL script using BigQuery Client
    # REVIEW-STRUCT: launcher starteSQLSkript is replaced here by BigQuery API execution logic
    # Confirm credentials and project ID before running.
    from google.cloud import bigquery
    client = bigquery.Client()
    
    with open(name_sql_skript, "r") as sql_file:
        query_text = sql_file.read()
    
    print(f"Starting BigQuery Execution: {name_sql_skript}")
    query_job = client.query(query_text)
    results = query_job.result()  # Wait for query to complete
    
    # Capture record metrics (simulating writing to tmp_file for downstream compatibility if required)
    num_rows = query_job.num_dml_affected_rows if query_job.num_dml_affected_rows is not None else results.total_rows
    with open(tmp_file, "w") as f:
        f.write(str(num_rows))

    print(" ---------- ENDE Datenverarbeitung ----------")

    # Step 9: Parse records processed
    if os.path.exists(tmp_file):
        with open(tmp_file, "r") as f:
            v_records = f.read().strip()
        print(f"Processed records: {v_records}")
    else:
        v_records = "0"
        print("Warning: Temporary metrics file not found.", file=sys.stderr)

    # Step 10: Register success and cleanup
    print("Die Abarbeitung wurde ohne erkennbare Fehler beendet")
    # DWMSG_SetzeStatusOK(dw_eintrags_nr)
    
    # Cleanup temporary metrics file
    if os.path.exists(tmp_file):
        os.remove(tmp_file)
        
    sys.exit(0)

except Exception as e:
    # Error Trap block: Mimics 'trap "DWMSG_Fehlerbehandlung ..."'
    print(f"AppError: Abbruch - {str(e)}", file=sys.stderr)
    # DWMSG_Fehlerbehandlung(dw_eintrags_nr)
    
    # Cleanup temporary metrics file if exists on failure
    if os.path.exists(tmp_file):
        try:
            os.remove(tmp_file)
        except OSError:
            pass
            
    sys.exit(1)
```

### Execution order
The target orchestration (e.g., Cloud Composer / Airflow DAG) must preserve the execution sequence defined in the legacy system:
1. **DW.BERT_AUSD_V_TA_PERIOD.xml**: UC4 orchestrator definition (triggers the flow).
2. **r_ausd_v_ta_period.ksh**: KornShell wrapper script that coordinates the process. In the target platform, this is represented by the Python script `r_ausd_v_ta_period.py` executed via a Python operator.
3. **d_ausd_v_ta_period.sql**: SQL script that performs the core database table alignment. In the target platform, this is executed against BigQuery (e.g., using BigQuery Client within `r_ausd_v_ta_period.py` or as a separate Dataform/BigQuery task).

---

### Schedule & variables
The migrated workflow must preserve the scheduling characteristics and receive scheduler-set variables through native BigQuery/Composer mechanisms:
* **Scheduler-Set Variable**: `DWH_JOB_KENNUNG = 'AUSD_V_TA_PERIOD'` (originally provided by the UC4 scheduler `DW.BERT_AUSD_V_TA_PERIOD`).
  * *Target Mechanism*: Map to an Airflow DAG parameter, Airflow Variable, or environment variable `DWH_JOB_KENNUNG` accessible at runtime.

---

### Lineage
* **Upstream**: The workflow execution is initiated by the scheduler defined in `DW.BERT_AUSD_V_TA_PERIOD.xml` (belongs to a different orchestration pass).
* **Downstream Consumer**: `d_ausd_v_ta_period.sql` (referenced via `FILE:d_ausd_v_ta_period.sql`), which is executed to perform the database transformations. This file belongs to a different migration pass and is a cross-job reference.

---

### Cross-file dependencies
* **d_ausd_v_ta_period.sql**: The legacy shell script `r_ausd_v_ta_period.ksh` directly references and executes this external SQL script using a database-launcher utility. This file's logic must be converted to BigQuery Standard SQL and be available for execution.
* **Corporate Framework Helpers**: Sourced files `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh` provide environment initialization, parameter parsing, and logging. Because these are omitted from the source files, they must be mocked or replaced with native Python libraries (e.g., `argparse`, `logging`, `datetime`).

---

### Target file plan
List of target files to be generated in this migration pass:

* **Target File Path**: `local/home/gurunathan_t/single_job_demo/r_ausd_v_ta_period.py`
  * **Language**: Python
  * **Source File**: `local/home/gurunathan_t/single_job_demo/r_ausd_v_ta_period.ksh`
  * **Purpose**: Replaces the KornShell orchestrator with a Python script. It parses arguments, configures environment-specific constants, handles signals/exceptions, executes the SQL transformation using the BigQuery Client library, and captures run metrics.

---

### Environment-specific values
Classified by their role in the target environment:

#### 1. GLOBAL (Environment-Wide)
These constants identify target cloud infrastructure and must be retrieved dynamically at runtime:
* **GCP_PROJECT**: The GCP project ID. Sourced via `os.environ.get("GCP_PROJECT")` or Airflow's `Variable.get("GCP_PROJECT")`.
* **BQ_DATASET**: The target BigQuery dataset. Sourced via environment variables or parameterization.
* **BQ_LOCATION**: Target BigQuery dataset location (e.g., `EU` or `US`).
* **BERT_DIR_ROOT**: Pointer to the root directory for scripts and resources. Sourced via `os.environ.get("BERT_DIR_ROOT")`.
* **DW_DIR_UTL**: Directory for temporary metric outputs and utility files. Sourced via `os.environ.get("DW_DIR_UTL")` or standard local `/tmp`.

#### 2. JOB-SPECIFIC
These parameters are specific to this particular job's context and logic:
* **DWH_JOB_KENNUNG**: Variable value `'AUSD_V_TA_PERIOD'`, to be supplied as a task param or environment variable.
* **JobKennung**: hardcoded value `"BERT_V_TA_PERIOD"`.
* **v_TabName**: Target table name `"ta_period"`.
* **LogDatei**: Dynamic runtime-generated log file path.
* **tmpFile**: Process-specific temporary file path used to parse row counts at runtime.

---

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/single_job_demo/r_ausd_v_ta_period.ksh` | `local/home/gurunathan_t/single_job_demo/r_ausd_v_ta_period.py` | KornShell orchestrator is converted to a Python script executing queries against BigQuery and managing status tracking. |

---

### Risks & Manual Actions
* **Unmigrated Execution Dependencies**: The core transformation script `d_ausd_v_ta_period.sql` and orchestrating definition `DW.BERT_AUSD_V_TA_PERIOD.xml` are not part of the source files in this pass. The system cannot be end-to-end verified until these sibling files are migrated and coordinated.
* **Opaque Framework Code (DWMSG)**: The logging, error logging, and execution-state updating functions (`DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_MeldeFehler`, `DWMSG_SetzeStatusOK`, `DWMSG_Fehlerbehandlung`) are external shell functions. These must be replaced with equivalent enterprise Python wrappers or standard GCP Cloud Logging calls.
* **Opaque SQL Launcher**: The custom launcher `starteSQLSkript` is not supplied. In the Python target, this execution is handled directly via `google.cloud.bigquery.Client().query()`. Authentication and client permissions must be verified on the runner.