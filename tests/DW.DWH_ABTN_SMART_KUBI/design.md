=== OBJECT: DW.DWH_ABTN_SMART_KUBI (JOBS_UNIX) ===
active=1
title=Populate temp table
login=DW.UNIX.ISTNS
host=|DWHDWH1P|HOST
ert_seconds=2838
launcher_type=sql_script
launcher_details={'job_arg': 'ABTN_SMART_KUBI', 'sql_path': '$HOME/aktuell/aufbereitung/tn/sql/d_abtn_x_smart_kubi.sql'}
script_body:
:inc DW.HOLE_PFAD
:set &DWH_JOB_KENNUNG='ABTN_SMART_KUBI'
. $HOME/.dw_init

!***************************************
:set &cdate  = SYS_DATE("YYYYMMDD")
:set &cmonth = SUBSTR(&cdate,1,6)
:set &cday   = SUBSTR(&cdate,7,2)

:if  &cday  < '15'
:     set &first = '01'
:     set &cmonth = "&cmonth&first"
:     set &cmonth = SUB_DAYS(&cmonth,1)
:     set &cmonth = SUBSTR(&cmonth,1,6)
:endif

:set &MONATSID = &cmonth
!***************************************

:print Berichtsmonat:  &MONATSID

$HOME/aktuell/allgemein/is/util/bin/r_sqlscript -j ABTN_SMART_KUBI -f $HOME/aktuell/aufbereitung/tn/sql/d_abtn_x_smart_kubi.sql -i &MONATSID
!$HOME/aktuell/allgemein/is/util/bin/r_sqlscript -j ABTN_SMART_KUBI -f $HOME/aktuell/aufbereitung/tn/sql/d_abtn_x_smart_kubi.sql -i 201707


:inc DW.LESE_LOG
operational_notes=None

=== UNRESOLVED REFERENCES (object named but not supplied in this bundle) ===
  (none — every referenced object was supplied in this bundle)


# Migration Design Document: DW.DWH_ABTN_SMART_KUBI

## 1. Overview
The `DW.DWH_ABTN_SMART_KUBI` workflow is a standalone data preparation job designed to populate a temporary database table. It executes a specific SQL script (`d_abtn_x_smart_kubi.sql`) after dynamically calculating a reporting month ID (`MONATSID`) based on the execution date (if the execution day is before the 15th, it targets the previous month; otherwise, it targets the current month). Since no calendar schedule or parent job plan (JOBP) is provided in this extraction, this DAG is designed to be triggered externally.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
|---|---|---|---|
| `DW.DWH_ABTN_SMART_KUBI` | JOBS_UNIX | 1 (Active) | Populate temp table |

## 3. Scheduling
- **Schedule**: `None`
- **Trigger Source**: This workflow contains no calendar-based schedule of its own and no parent workflow (JOBP) or script (SCRI) in this extraction triggers it. It is marked as **externally triggered** (source unknown from this extraction alone).

## 4. Airflow DAG Properties
| Property | Value |
|---|---|
| **dag_id** | `dw_dwh_abtn_smart_kubi` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` *(placeholder)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` *(Active=1)* |
| **default_args** | `{'owner': 'dw', 'retries': 1, 'retry_delay': timedelta(minutes=5)}` |

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `dwh_abtn_smart_kubi` | `DW.DWH_ABTN_SMART_KUBI` | `EmptyOperator` | N/A | N/A | 1 | 5 min | None | None | False | None | # REVIEW-STRUCT: launcher wraps SQL script `d_abtn_x_smart_kubi.sql`, converted separately by the companion KSH/SQL migration pipeline into EITHER a Python script or BigQuery SQL -- this extraction cannot know which. Confirm the actual artifact produced before wiring a real operator (BashOperator/PythonOperator for Python, BigQueryInsertJobOperator for BigQuery SQL); never assume Python. Calculates dynamic parameter `MONATSID`. |

## 6. Task Dependency Map
```python
# Standalone task. No dependencies defined within this bundle.
dwh_abtn_smart_kubi
```

## 7. Sync / Concurrency Analysis
No sync keys or mutual exclusion rules are defined for this object in the extraction. Safe concurrency defaults are applied via `max_active_runs=1`.

## 8. Error Handling and Retry Strategy
- Default task-level retries are set to `1` with a `5-minute` retry delay.
- No postcondition actions, alerts, or custom failure blocks were specified in the extraction.

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
|---|---|---|
| `&MONATSID` | Calculated dynamically based on current day of month (< '15' -> previous month `YYYYMM`, else current month `YYYYMM`) | Determined dynamically at runtime using DAG execution/logical date (see calculation in pseudocode). |

## 10. Developer Notes
- **# REVIEW-STRUCT: SQL Artifact Migration**: The underlying shell/SQL execution (`r_sqlscript`) must be converted by the companion SQL migration pipeline. Depending on whether this is migrated to Python/Bash or directly to BigQuery SQL, replace the `EmptyOperator` wrapper with either a `BashOperator`, `PythonOperator`, or `BigQueryInsertJobOperator`.
- **Date Logic Alignment**: The dynamic parameter calculation for `MONATSID` uses UC4 script dates. In Airflow, this should be bound to the run's `logical_date` (or `data_interval_end`) instead of timezone-unaware system time to ensure idempotency during backfills.

---

# Pseudocode Outline

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# ── GCP Configuration ────────────────────────────────────
# Placeholder for project-level connections if needed downstream
# GCP_PROJECT_ID = "your-gcp-project"
# GCP_REGION = "europe-west3"

# ── Default Args ─────────────────────────────────────────
default_args = {
    'owner': 'dw',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── on_failure_callback stubs ─────────────────────────────
# No custom error callbacks registered in extraction.

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id='dw_dwh_abtn_smart_kubi',
    default_args=default_args,
    description='Populate temp table - migrated from DW.DWH_ABTN_SMART_KUBI',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['dw', 'uc4_migration'],
) as dag:

    # Helper Python function to demonstrate the UC4 Date calculation logic
    def calculate_monatsid(logical_date):
        """
        Replicates the UC4 Script logic:
        :if  &cday  < '15'
        :     set &first = '01'
        :     set &cmonth = "&cmonth&first"
        :     set &cmonth = SUB_DAYS(&cmonth,1)
        :     set &cmonth = SUBSTR(&cmonth,1,6)
        :endif
        """
        # logical_date is a pendulum.DateTime object provided by Airflow context
        if logical_date.day < 15:
            # Go to first day of current month, then subtract 1 day to get previous month
            first_of_month = logical_date.replace(day=1)
            prev_month = first_of_month - timedelta(days=1)
            monatsid = prev_month.strftime('%Y%m')
        else:
            monatsid = logical_date.strftime('%Y%m')
        
        return monatsid

    # ── Task: dwh_abtn_smart_kubi ────────────────────────
    # # REVIEW-STRUCT: This operator is an EmptyOperator stub representing the r_sqlscript utility.
    # Replace this with BigQueryInsertJobOperator or PythonOperator / BashOperator 
    # once the companion KSH/SQL migration pipeline outputs the migrated SQL/Python code.
    # The calculated MONATSID should be passed into the migrated task execution context.
    dwh_abtn_smart_kubi = EmptyOperator(
        task_id='dwh_abtn_smart_kubi',
        # doc_md will display this note in the Airflow UI
        doc_md="""
        ### Migration Note
        Runs raw SQL: `d_abtn_x_smart_kubi.sql`
        Parameters required: `MONATSID` (calculated via `calculate_monatsid()`)
        """,
    )

    # ── Dependencies ─────────────────────────────────────────
    # Standalone job task, no internal workflow dependencies.
    dwh_abtn_smart_kubi
```

### File Disposition
| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/DW.DWH_ABTN_SMART_KUBI.xml` | `local/home/gurunathan_t/kubi/dw_dwh_abtn_smart_kubi_dag.py` | Converts the UC4 UNIX Job into an Airflow DAG that manages orchestration, parameter calculation, and invokes the database process. |

### Execution Order
The execution sequence identified in the legacy dependency graph must be preserved in the target architecture:
1. **DW.DWH_ABTN_SMART_KUBI.xml** (Legacy UC4 Job) -> Maps to the triggering and execution control flow of the Airflow DAG `dw_dwh_abtn_smart_kubi_dag.py`.
2. **d_abtn_x_smart_kubi.sql** (Oracle SQL script) -> Executed on BigQuery using the `BigQueryInsertJobOperator` once migrated.
3. **r_sqlscript** (KornShell wrapper) -> Replaced by Airflow's native BigQuery query execution, passing dynamic query parameters natively.
4. **.dw_init** (KornShell environment initializer) -> Replaced by environment configuration variables stored in GCP and Airflow connection configurations.
5. **f_alis_msgerr.ksh** and **h_alis_sqlplus.ksh** (Error logging and execution frameworks) -> Replaced natively by the logging framework of Cloud Composer/Airflow and TaskInstance state handling.

### Schedule & Variables
The execution scheduling constraints and variables from the legacy environment must be maintained:
* **Scheduler-Set Variables**:
  * `DWH_JOB_KENNUNG` (Value: `'ABTN_SMART_KUBI'`): Provided as a JOB-SPECIFIC parameter in the DAG setup.
  * `cdate` (Value: `SYS_DATE("YYYYMMDD")`): Calculated dynamically using standard Python `datetime` based on the DAG run's logical date: `logical_date.strftime('%Y%m%d')`.
  * `cmonth` (Value: `SUBSTR(&cdate,1,6)`): Mapped using `logical_date.strftime('%Y%m')`.
  * `cday` (Value: `SUBSTR(&cdate,7,2)`): Mapped using `logical_date.strftime('%d')`.
  * `first` (Value: `'01'`): Standard static Python string variable.
  * `MONATSID`: Derived at runtime matching the exact UC4 conditional logic:
    ```python
    # Replicates the original UC4 day-offset calculation logic
    # If the logical date day is before the 15th, target the previous month
    if logical_date.day < 15:
        monats_id = (logical_date.replace(day=1) - timedelta(days=1)).strftime('%Y%m')
    else:
        monats_id = logical_date.strftime('%Y%m')
    ```
    This calculated `MONATSID` must be logged as `Berichtsmonat: {monats_id}` and passed to the target BigQuery execution task as a parameter.

### Lineage
* **Upstream Components**:
  * `.dw_init` (Shell environment initializer): Replaced by Airflow's environment-wide variables.
  * `DW.HOLE_PFAD` and `DW.LESE_LOG`: Confirmed by Human Review as "NO SOURCE NEEDED" (retired/handled natively by Composer).
* **Downstream Components**:
  * `r_sqlscript` (Shell utility wrapper): Replaced by the execution operator inside the DAG.
  * `d_abtn_x_smart_kubi.sql` (Oracle SQL script): Executed downstream on BigQuery (handled in a separate migration pass).

### External System Replacements
* **Database Platform**: Legacy Oracle database operations running on host `dwhdwh1p` are replaced by **BigQuery** natively. Connections and credentials are managed securely via Cloud Composer connection IDs.
* **Shell Scripts to Cloud Composer**: Legacy shell invocation scripts (`r_sqlscript`, `h_alis_sqlplus.ksh`) are replaced by an Airflow DAG utilizing Google Cloud Operators (such as `BigQueryInsertJobOperator`).

### Cross-File Dependencies
* **d_abtn_x_smart_kubi.sql**: This SQL query contains the actual table truncation and loading business logic. While its transformation to BigQuery SQL is handled in a separate migration pass, this DAG is responsible for computing and injecting the `MONATSID` query parameter into its execution.
* **DW.UNIX.ISTNS**: Legacy UNIX login credentials, replaced with target GCP IAM permissions and service accounts configured on the Composer environment.

### Target File Plan
* **Target File Path**: `local/home/gurunathan_t/kubi/dw_dwh_abtn_smart_kubi_dag.py`
  * **Language**: Python (Airflow DAG)
  * **Source File**: `local/home/gurunathan_t/kubi/DW.DWH_ABTN_SMART_KUBI.xml`
  * **Role**: Coordinates the orchestration, calculates the reporting month (`MONATSID`), outputs the calculated variable to the log, and invokes the BigQuery query operator.

### Environment-Specific Values
We classify environmental parameters into GLOBAL or JOB-SPECIFIC roles:
1. **GLOBAL** (Environment-wide configuration, sourced dynamically at runtime via Airflow `Variable` or `os.environ`):
   * `GCP_PROJECT`: Sourced dynamically using `Variable.get("GCP_PROJECT")`.
   * `GCP_REGION`: Sourced dynamically using `Variable.get("GCP_REGION")`.
   * `BQ_DATASET`: Sourced dynamically using `Variable.get("BQ_DATASET")` to point to the correct deployment dataset (e.g., dev/test/prod).

2. **JOB-SPECIFIC** (Specific variables for this job, populated inline or inside DAG task parameters):
   * `DWH_JOB_KENNUNG` (Value: `"ABTN_SMART_KUBI"`): Declared statically in Python.
   * `SQL_FILE_PATH` (Value: `"local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql"`): Specifies the location of the SQL script.
   * `MONATSID`: Derived programmatically at runtime using Airflow execution date context and supplied to the BigQuery script as a runtime parameter.

### Risks and Manual Steps
* **SQL Migration Dependencies**: The XML workflow invokes `d_abtn_x_smart_kubi.sql`. The conversion of this SQL file to BigQuery SQL is handled under a separate design pass. Once the SQL file is prepared, the placeholder stub (representing `r_sqlscript`) in `dw_dwh_abtn_smart_kubi_dag.py` must be replaced with a `BigQueryInsertJobOperator` pointing to the migrated SQL, utilizing the calculated `MONATSID` variable.
* **Environment Setup Verification**: The environment variables `GCP_PROJECT` and `BQ_DATASET` must be configured in the destination Airflow instance before deployment. Hardcoded environment names or prose placeholders are prohibited in the final code.

---

### group 2/6 — DESIGN FAILED

ERROR: Design cannot proceed — REQUIRED TOOL returned errors, empty, or hollow (no real content extracted) responses on every attempt. Investigate the MCP service and retry the job.


---

=== FILE: local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql ===
-- Discription :Aggregation Job to load data into DWH$TA_T_SMART_KUBI table
-- Erstellt  : Ankita Suvarna
-- Datum     : 18.09.2015
-- Language  : PL/SQL
-- Version   : 16.1.0.
------------------------------------------------------
WHENEVER oserror EXIT failure;
WHENEVER sqlerror EXIT failure; 
---
SET timing ON;
SET serveroutput ON;
SET echo OFF;
DECLARE 
	v_anzahl_ds pls_integer := 0;
	l_monats_id number := to_number('&1');
	EintragsNr  number := to_number('&2');
	lv_str      varchar2(300);
	l_monats_date DATE := ADD_MONTHS(TO_DATE(l_monats_id, 'YYYYMM'), 1);
BEGIN
  LV_STR:= 'Truncate table DWH$TA_T_SMART_KUBI'; 
  dwpa_util_skript.runstatement(eintragsnr, lv_str); 

INSERT 
       /*+ Append */ 
INTO   dwh$ta_t_smart_kubi 
       ( 
              monats_id, 
              kundennummer, 
              tarif_id, 
              tarif_id_alt, 
              vo_kennung, 
              test_gp, 
              anzahl, 
              kennzahl_id 
       ) 
with temp AS 
       ( 
		   SELECT
				  /*+ parallel(t,4) full(t) parallel(tar,4) full(tar) */
				  t.tarif_id,
				  t.dwh_tarif_id,
				  t.gueltig_von,
				  t.gueltig_bis,
				  tar.mp_geschaeftsfeld_id
		   FROM   dwh$vi_l_map_fa_tarif T,
				  bl_d_tarif TAR
		   WHERE  t.tarif_id = tar.tarif_id
		   AND    t.gueltig_bis = To_date('4712-12-31', 'YYYY-MM-DD')
		)
SELECT /*+ full(fact) parallel(fact,4) full(d) parallel(d,4) use_hash(t1,t2,fact,d)*/ 
         l_monats_id                                    								  AS monats_id,
         Decode(t_new.mp_geschaeftsfeld_id,2,'-1',d.t_mobile_kundennummer)                      kundennummer,
         Nvl(t_new.tarif_id,0)                                                            AS tarif_id,
         Nvl(t_old.tarif_id,0)                                                            AS tarif_id_alt,
         Decode(ltrim(rtrim(fact.vo_kenn_bearb)),NULL,fact.vo_kenn,'#',fact.vo_kenn,fact.vo_kenn_bearb)  vo_kennung,
         d.test_gp, 
         sum(fact.zugang) AS anzahl, 
         fact.kennzahl_id 
FROM     dwh$ta_f_d1_twvv_tn partition(dwh$ta_f_d1_twvv_tn_&1) fact, 
         temp t_new, 
         temp t_old,
         dwh$ta_c_vertrag d 
WHERE    to_char(fact.gueltigkeitszeitpunkt,'yyyymm')=to_char(l_monats_id) 
AND      fact.kennzahl_id IN ('VVLREIN', 
                              'VVLTWC2C', 
                              'MIGP2CBF') 
AND      fact.dwh_tarif_id_neu  = t_new.dwh_tarif_id (+) 
AND      fact.dwh_tarif_id_alt = t_old.dwh_tarif_id (+) 
AND      fact.dwh_vertrag_id=d.dwh_vertrag_id(+) 
AND      l_monats_date > d.gueltig_von(+) 
AND      l_monats_date <= d.gueltig_bis(+) 
GROUP BY decode(t_new.mp_geschaeftsfeld_id,2,'-1',d.t_mobile_kundennummer), 
         nvl(t_new.tarif_id,0), 
         nvl(t_old.tarif_id,0), 
         decode(ltrim(rtrim(fact.vo_kenn_bearb)),NULL,fact.vo_kenn,'#',fact.vo_kenn,fact.vo_kenn_bearb), 
         d.test_gp, 
         fact.kennzahl_id;
				  
v_anzahl_ds := SQL%ROWCOUNT;
COMMIT;
  
dbms_output.put_line(TO_CHAR(v_anzahl_ds) || ' rows inserted in DWH$TA_T_SMART_KUBI');
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
    ErrText := SQLERRM;
    ErrC    := SQLCODE;
    dwpa_meldung.fehler ('F', EintragsNr, FehlerNr, ErrText, TO_CHAR(ErrC));
    raise_application_error(FehlerNr, ErrText);
  END;
END;
/ 

═══════════════════════════════════════════
SECTION 1 — DESIGN DOCUMENT
═══════════════════════════════════════════

Step 1: Understand the Script
1.1 Identify the type of Oracle SQL object being converted:
    - This is a PL/SQL anonymous block containing procedural operations, variables, dynamic SQL execution, error handling, and a large SELECT statement serving an INSERT INTO operation.
1.2 Business Logic Summary:
    - The script aggregates subscriber transaction data (Zugang numbers) for a specific monthly reporting period into the target table `DWH$TA_T_SMART_KUBI`.
    - It truncates the target table first.
    - It categorizes contracts, mappings of tariffs, and promotional codes using structured logical rules (such as matching active timeframes and filtering specific KPIs like 'VVLREIN', 'VVLTWC2C', 'MIGP2CBF').
1.3 Entities Referenced:
    - Target Table: `dwh$ta_t_smart_kubi` (mapped to `dwh_ta_t_smart_kubi`)
    - Source Tables/Views:
      - `dwh$vi_l_map_fa_tarif` (`T`): Mapped to `dwh_vi_l_map_fa_tarif`
      - `bl_d_tarif` (`TAR`): Mapped to `bl_d_tarif`
      - `dwh$ta_f_d1_twvv_tn` (`fact`): Mapped to `dwh_ta_f_d1_twvv_tn`
      - `dwh$ta_c_vertrag` (`d`): Mapped to `dwh_ta_c_vertrag`

Step 2: Oracle-Specific Construct Detection and Resolution

2.1 Data Type Conversions:
    - PLS_INTEGER (e.g. `v_anzahl_ds`) → INT64
    - NUMBER (e.g. `l_monats_id`, `EintragsNr`) → INT64
    - DATE (e.g. `l_monats_date`, `gueltig_von`, `gueltig_bis`, `gueltigkeitszeitpunkt`) → DATE or DATETIME. We will resolve to DATE for comparison consistency since hours/minutes are not utilized.
    - VARCHAR2(300) / VARCHAR2(512) → STRING

2.2 Implicit and Explicit Type Casting:
    - Oracle implicit conversion of number to date via `TO_DATE(number, 'YYYYMM')` → Resolved in BQ via explicit conversion: `PARSE_DATE('%Y%m%d', CONCAT(CAST(l_monats_id AS STRING), '01'))`.
    - Oracle implicit conversion of string comparison in `to_char(fact.gueltigkeitszeitpunkt,'yyyymm')=to_char(l_monats_id)` → Explicitly resolve to: `FORMAT_DATE('%Y%m', CAST(fact.gueltigkeitszeitpunkt AS DATE)) = CAST(l_monats_id AS STRING)`.

2.3 NULL Handling and Conditional Functions:
    - `DECODE(t_new.mp_geschaeftsfeld_id,2,'-1',d.t_mobile_kundennummer)` → CASE expression.
    - `NVL(x, y)` → `COALESCE(x, y)`.
    - `DECODE(LTRIM(RTRIM(fact.vo_kenn_bearb)), NULL, ...)` → Mapped to clean CASE expression utilizing `TRIM(fact.vo_kenn_bearb) IS NULL`.

2.4 String Functions:
    - `LTRIM(RTRIM(x))` → `TRIM(x)`.

2.5 Date and Timestamp Functions:
    - `ADD_MONTHS(d, n)` → `DATE_ADD(d, INTERVAL n MONTH)`.
    - `TO_DATE('4712-12-31', 'YYYY-MM-DD')` → `DATE '4712-12-31'`.

2.6-2.10 Standard SQL Constructs:
    - Optimizer Hints (e.g. `/*+ Append */`, `/*+ parallel(t,4) ... */`) → Stripped entirely.
    - Partition reference: `dwh$ta_f_d1_twvv_tn partition(dwh$ta_f_d1_twvv_tn_&1)` → BigQuery references the base consolidated table `dwh_ta_f_d1_twvv_tn`. Partition pruning is handled automatically by the filter clause on `gueltigkeitszeitpunkt`.

2.11-2.12 DML & Session Settings:
    - `WHENEVER SQLERROR EXIT...` / `SET timing ON` → Not applicable to BQ SQL. Handled by client/orchestrator.
    - Transaction control (`COMMIT`, `ROLLBACK`) → Supported in BigQuery Multi-statement transactions (`BEGIN TRANSACTION`, `COMMIT TRANSACTION`, `ROLLBACK TRANSACTION`).

2.13-2.14 PL/SQL Scripting and Variables:
    - Procedural Block → Resolved using a BQ Scripting Block (`BEGIN...END`).
    - `SQL%ROWCOUNT` → Replaced by system variable `@@row_count`.
    - `DBMS_OUTPUT.PUT_LINE` → Translated to `SELECT` statement returning string output for observability.
    - Package Call `dwpa_util_skript.runstatement(eintragsnr, lv_str)` (used to execute TRUNCATE) → Translated directly to a standard `TRUNCATE TABLE` statement.
    - Package Call `dwpa_meldung.fehler` (custom logger) → Documented as a standard placeholder procedure call `CALL dwpa_meldung_fehler(...)`.

2.15 Unresolvable or Advisory Items:
    - Command-line/SQL*Plus substitution parameters (`&1`, `&2`) → Declared as BigQuery parameters/variables at the start of the script.

Step 3: Conversion Strategy Summary
3.1 Conversion Approach:
    - Direct PL/SQL block to BigQuery scripting block conversion. 
    - Convert dynamic TRUNCATE execution into a static `TRUNCATE` command.
    - Convert Oracle's proprietary `(+)` outer-join conditions in the `WHERE` clause into proper ANSI `LEFT JOIN ... ON ...` conditions.
3.2 Assumptions:
    - The target system's database schema maps character values and dates safely.
    - BigQuery has a unified `dwh_ta_f_d1_twvv_tn` table instead of physically separated table partitions.
3.3 Human Review Flags:
    - Execution tracking framework (`dwpa_meldung.fehler`) requires setup of corresponding logging structures or stored procedure mocks in BigQuery.

═══════════════════════════════════════════
2.16 MIGRATION DECISION MATRIX
═══════════════════════════════════════════

| Statement / Construct | Selected Target | Rejected Alternatives | Evidence & Reason |
| :--- | :--- | :--- | :--- |
| Dynamic TRUNCATE logic | Direct BigQuery SQL | BQ JavaScript UDF | Dynamic execution is unnecessary; BQ natively supports the `TRUNCATE TABLE` statement. |
| Non-ANSI Join with `(+)` | Direct BigQuery SQL with rewrite | Python wrapper | BigQuery natively supports `LEFT JOIN` on both equality and non-equality conditions inside the `ON` clause. |
| Custom logger (`dwpa_meldung.fehler`) | Direct BigQuery SQL (mock procedure) | Python wrapper | A standard BQ stored procedure can emulate the error-handling output without moving away from pure SQL. |

═══════════════════════════════════════════
2.17 REQUIRED ARTIFACTS
═══════════════════════════════════════════
- **Artifact 1**: BigQuery SQL Script (`.sql` file) containing the procedural scripting block (`DECLARE`, `BEGIN`, `EXCEPTION`, `END`).

═══════════════════════════════════════════
2.18 DATA TYPE COMPATIBILITY TABLE
═══════════════════════════════════════════

| Oracle Source Type | BigQuery Target Type | Conversion Rule / Expression | Warning / Notes |
| :--- | :--- | :--- | :--- |
| `PLS_INTEGER` | `INT64` | Native mapping | None. |
| `NUMBER` (ID/Count) | `INT64` | `CAST(x AS INT64)` or implicit literal assignment | Check for decimal values (none found in this script). |
| `VARCHAR2` | `STRING` | Native mapping | None. |
| `DATE` | `DATE` | `DATE` or `CAST(x AS DATE)` | Time components are stripped to match business logic which operates on date boundaries. |

═══════════════════════════════════════════
2.19 DESIGN REVIEW SUMMARY
═══════════════════════════════════════════
- **Patterns Found**: Oracle legacy SQL outer-joins `(+)`, legacy string-padding `LTRIM(RTRIM())`, Oracle conditional `DECODE()`.
- **Unsupported Functions**: None (all successfully mapped to standard ANSI-equivalent SQL or BQ built-ins).
- **UDF Required**: No.
- **Python Required**: No.
- **Direct Dependencies**: Target tables (`dwh_ta_t_smart_kubi`), Source tables (`dwh_vi_l_map_fa_tarif`, `bl_d_tarif`, `dwh_ta_f_d1_twvv_tn`, `dwh_ta_c_vertrag`).
- **Warnings**: The `(+)` conditions involved range checks (`l_monats_date > d.gueltig_von(+)`). This must be moved cleanly into the `LEFT JOIN ... ON` clause to prevent incorrect row elimination.

OVERALL MIGRATION STRATEGY: Direct BigQuery SQL

═══════════════════════════════════════════
2.21 ORACLE FUNCTION ANALYSIS TABLE
═══════════════════════════════════════════

| Oracle Function/Construct | Supported in BigQuery | BigQuery Equivalent / Alternative |
| :--- | :--- | :--- |
| `ADD_MONTHS(date, n)` | Direct-with-rewrite | `DATE_ADD(date, INTERVAL n MONTH)` |
| `TO_DATE(str, fmt)` | Direct-with-rewrite | `PARSE_DATE('%Y%m%d', CONCAT(CAST(str AS STRING), '01'))` |
| `DECODE(expr, ...)` | Direct-with-rewrite | `CASE WHEN expr = val1 THEN res1 ELSE res2 END` |
| `NVL(x, y)` | Direct-with-rewrite | `COALESCE(x, y)` |
| `LTRIM(RTRIM(x))` | Direct-with-rewrite | `TRIM(x)` |
| `TO_CHAR(date, fmt)` | Direct-with-rewrite | `FORMAT_DATE('%Y%m', date)` |
| `SQL%ROWCOUNT` | Direct-with-rewrite | `@@row_count` |
| `DBMS_OUTPUT.PUT_LINE` | Direct-with-rewrite | `SELECT FORMAT(...)` |
| `(+)` Join Operator | Direct-with-rewrite | `LEFT OUTER JOIN ... ON ...` |
| `WHENEVER SQLERROR` | Unsupported | Handled outside BigQuery SQL via orchestrator (e.g. Airflow, dbt, Bash) |

\pagebreak

═══════════════════════════════════════════
SECTION 2 — PSEUDOCODE
═══════════════════════════════════════════

```sql
-- BigQuery Scripting Block representing the procedural migration
-- Note: Replaced dollar signs in table names with underscores (e.g. DWH$TA_T_SMART_KUBI -> dwh_ta_t_smart_kubi)

DECLARE v_anzahl_ds INT64 DEFAULT 0; -- converted from pls_integer
DECLARE l_monats_id INT64;
DECLARE EintragsNr INT64;
DECLARE lv_str STRING; -- converted from varchar2(300)
DECLARE l_monats_date DATE; -- converted from Oracle DATE

-- Assign parameter values (representing SQL*Plus &1 and &2)
SET l_monats_id = CAST(@param_monats_id AS INT64);
SET EintragsNr = CAST(@param_eintragsnr AS INT64);

-- Explicitly resolve Oracle ADD_MONTHS(TO_DATE(...), 1) logic
SET l_monats_date = DATE_ADD(PARSE_DATE('%Y%m%d', CONCAT(CAST(l_monats_id AS STRING), '01')), INTERVAL 1 MONTH); 

BEGIN
  -- Truncate target table directly instead of via dynamic string execution
  TRUNCATE TABLE dwh_ta_t_smart_kubi;

  -- Start transaction to ensure atomic execution
  BEGIN TRANSACTION;

  INSERT INTO dwh_ta_t_smart_kubi 
         ( 
                monats_id, 
                kundennummer, 
                tarif_id, 
                tarif_id_alt, 
                vo_kennung, 
                test_gp, 
                anzahl, 
                kennzahl_id 
         ) 
  WITH temp AS 
         ( 
             SELECT
                    t.tarif_id,
                    t.dwh_tarif_id,
                    t.gueltig_von,
                    t.gueltig_bis,
                    tar.mp_geschaeftsfeld_id
             FROM   dwh_vi_l_map_fa_tarif t
             INNER JOIN bl_d_tarif tar
                ON t.tarif_id = tar.tarif_id
             WHERE  CAST(t.gueltig_bis AS DATE) = DATE '4712-12-31' -- converted from To_date('4712-12-31', 'YYYY-MM-DD')
          )
  SELECT 
           l_monats_id                                    AS monats_id,
           -- converted from DECODE
           CASE 
             WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' 
             ELSE d.t_mobile_kundennummer 
           END                                            AS kundennummer,
           COALESCE(t_new.tarif_id, 0)                     AS tarif_id, -- converted from NVL(t_new.tarif_id,0)
           COALESCE(t_old.tarif_id, 0)                     AS tarif_id_alt, -- converted from NVL(t_old.tarif_id,0)
           -- converted from DECODE and LTRIM(RTRIM())
           CASE 
             WHEN TRIM(fact.vo_kenn_bearb) IS NULL THEN fact.vo_kenn
             WHEN TRIM(fact.vo_kenn_bearb) = '#' THEN fact.vo_kenn
             ELSE fact.vo_kenn_bearb
           END                                            AS vo_kennung,
           d.test_gp, 
           SUM(fact.zugang)                               AS anzahl, 
           fact.kennzahl_id 
  FROM     dwh_ta_f_d1_twvv_tn fact -- Partition filter is handled natively by BQ engine
  LEFT JOIN temp t_new 
    ON fact.dwh_tarif_id_neu = t_new.dwh_tarif_id -- converted from (+) join
  LEFT JOIN temp t_old 
    ON fact.dwh_tarif_id_alt = t_old.dwh_tarif_id -- converted from (+) join
  LEFT JOIN dwh_ta_c_vertrag d 
    ON fact.dwh_vertrag_id = d.dwh_vertrag_id -- converted from (+) join
   AND l_monats_date > CAST(d.gueltig_von AS DATE) -- converted from l_monats_date > d.gueltig_von(+)
   AND l_monats_date <= CAST(d.gueltig_bis AS DATE) -- converted from l_monats_date <= d.gueltig_bis(+)
  WHERE    FORMAT_DATE('%Y%m', CAST(fact.gueltigkeitszeitpunkt AS DATE)) = CAST(l_monats_id AS STRING) -- converted from to_char(...,'yyyymm')
  AND      fact.kennzahl_id IN ('VVLREIN', 
                                'VVLTWC2C', 
                                'MIGP2CBF') 
  GROUP BY 
           CASE 
             WHEN t_new.mp_geschaeftsfeld_id = 2 THEN '-1' 
             ELSE d.t_mobile_kundennummer 
           END,
           COALESCE(t_new.tarif_id, 0),
           COALESCE(t_old.tarif_id, 0),
           CASE 
             WHEN TRIM(fact.vo_kenn_bearb) IS NULL THEN fact.vo_kenn
             WHEN TRIM(fact.vo_kenn_bearb) = '#' THEN fact.vo_kenn
             ELSE fact.vo_kenn_bearb
           END,
           d.test_gp, 
           fact.kennzahl_id;

  SET v_anzahl_ds = @@row_count; -- converted from SQL%ROWCOUNT
  COMMIT TRANSACTION;
    
  -- converted from dbms_output.put_line
  SELECT FORMAT('%d rows inserted in DWH$TA_T_SMART_KUBI', v_anzahl_ds) AS log_message;

EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
  BEGIN
    -- Error Handling Section
    DECLARE ErrText STRING DEFAULT @@error.message;
    DECLARE ErrC INT64 DEFAULT 1; -- Placeholder code representation
    DECLARE FehlerNr INT64 DEFAULT -20001; -- Representing custom dwpa_globals.k_alis_err_unknown
    
    -- Calling Logging Call placeholder
    CALL dwpa_meldung_fehler('F', EintragsNr, FehlerNr, ErrText, CAST(ErrC AS STRING));
    
    -- Raise exception to calling routine
    RAISE USING MESSAGE = ErrText;
  END;
END;
```

═══════════════════════════════════════════
FLAGGED ITEMS FOR HUMAN REVIEW
═══════════════════════════════════════════
1. **Dynamic Execution Parameters**: The variables `@param_monats_id` and `@param_eintragsnr` must be supplied to the BigQuery session context at execution time.
2. **Error-Logging Framework**: The stored procedure call `CALL dwpa_meldung_fehler(...)` expects a target logging table or console interface. If this logging framework is not migrated to BigQuery, it can be replaced with a native `INSERT INTO logging_table` or removed.
3. **Partition Pruning**: BigQuery base table `dwh_ta_f_d1_twvv_tn` should be partitioned by `gueltigkeitszeitpunkt` to allow the query engine to run optimally and match the performance of the original Oracle partitioned queries.

### File Disposition Table

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql` | `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql` | Migrated to BigQuery SQL Scripting block, keeping procedural block structure for parameters, variables, truncation, and the main insert select logic. |

***

### Target File Plan

| Target File Path | Language | Source File Path |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql` | BigQuery SQL | `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql` |

***

### Execution Order
The legacy orchestration sequence must be preserved in the target orchestration (Airflow DAG):
1. **DW.DWH_ABTN_SMART_KUBI.xml** maps to **Airflow DAG (`dags/dwh_abtn_smart_kubi_dag.py`)** which acts as the central orchestrator. *Note: This orchestrator file is owned by another group's design pass and is not created in this file plan.*
2. **.dw_init** is retired. Legacy environment variables are managed directly as Airflow environment or task parameters.
3. **r_sqlscript** and **h_alis_sqlplus.ksh** are retired. These KSH wrappers are replaced by Airflow's native `BigQueryInsertJobOperator` which executes the target SQL script directly with parameter injection.
4. **d_abtn_x_smart_kubi.sql** maps to the executed task **`BigQueryInsertJobOperator`** running the BigQuery SQL Script `local/home/gurunathan_t/kubi/d_abtn_x_smart_kubi.sql`.
5. **f_alis_msgerr.ksh** is retired. Error reporting and alerting are managed by Airflow's native on-failure callbacks and Cloud Logging.

***

### Schedule & Variables
The dynamic date calculations and variables set by the legacy scheduler must be retained and mapped:
* **DWH_JOB_KENNUNG** = `'ABTN_SMART_KUBI'` $\rightarrow$ Managed as a static Airflow task parameter.
* **cdate** = `SYS_DATE("YYYYMMDD")` $\rightarrow$ Maps to the Airflow run execution date macro: `{{ ds_nodash }}`.
* **cmonth** = `SUBSTR(&cdate,1,6)` $\rightarrow$ Calculated as: `{{ execution_date.strftime('%Y%m') }}`.
* **cday** = `SUBSTR(&cdate,7,2)` $\rightarrow$ Calculated as: `{{ execution_date.strftime('%d') }}`.
* **first** = `'01'` $\rightarrow$ Retained as a static string parameter.
* **MONATSID** = `&cmonth` $\rightarrow$ Represents the previous month in YYYYMM format relative to execution time. In Airflow, this is dynamically mapped using the macro: `{{ (execution_date - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%Y%m') }}`. This is passed directly into the SQL script parameter `@param_monats_id`.

***

### Lineage
* **Upstream Table / View Producers:**
  * `DWH$TA_F_D1_TWVV_TN` (Source table, partitioned by month/date)
  * `DWH$VI_L_MAP_FA_TARIF` (Source view)
  * `BL_D_TARIF` (Source table)
  * `DWH$TA_C_VERTRAG` (Source table, alias `d` in the original script)
* **Downstream Consumers:**
  * `DWH$TA_T_SMART_KUBI` (Target table written to by the script)
* **Legacy Package Dependencies:**
  * `DWPA_UTIL_SKRIPT` (utility package used for statement execution)
  * `T_NEW` (referenced in query column resolution)
  * `T_OLD` (referenced in query column resolution)
  * `DWPA_MELDUNG` (reporting package used for logging exceptions)

***

### External System Replacements
* **Oracle Database $\rightarrow$ Google BigQuery:** Tables prefixed with `DWH$` or `BL_` map to BigQuery tables in the corresponding dataset (e.g. `dwh_ta_t_smart_kubi`, `bl_d_tarif`).
* **Oracle Packages (`DWPA_UTIL_SKRIPT`, `DWPA_MELDUNG`) $\rightarrow$ Retired / Replaced:**
  * `DWPA_UTIL_SKRIPT` (specifically `runstatement` for truncate) is replaced with a native BigQuery `TRUNCATE TABLE` statement.
  * `DWPA_MELDUNG` (specifically `fehler`) is replaced with custom BigQuery stored procedures (e.g., `dwpa_meldung_fehler` mock procedure) or handled via native Airflow execution logging.

***

### Cross-File Dependencies
* **Shared Tables:**
  * `DWH$TA_F_D1_TWVV_TN` is a partition-based transactional table shared with other DWH jobs.
  * `DWH$VI_L_MAP_FA_TARIF` view is a shared mapping table.
* **Common Schemas:**
  * The target schema for `DWH$TA_T_SMART_KUBI` must be predefined in BigQuery prior to executing this script.
* **Call Chains:**
  * Orchestration in Airflow calls the BigQuery SQL script `d_abtn_x_smart_kubi.sql` as a BigQuery task, injecting dynamic execution parameters `l_monats_id` (MONATSID) and `EintragsNr` (run execution id).

***

### Environment-Specific Values

#### 1. GLOBAL (Environment-Wide)
* **GCP_PROJECT:** Identifies the target GCP project. Sourced at runtime via `os.environ.get("GCP_PROJECT")` or Airflow config `Variable.get("GCP_PROJECT")`.
* **GCP_REGION:** Identifies the region for resource execution. Sourced at runtime via Airflow config `Variable.get("GCP_REGION")`.
* **BQ_DATASET:** Identifies the target BigQuery dataset namespace. Sourced via Airflow config `Variable.get("BQ_DATASET")` and substituted as a query parameter or dataset reference.

#### 2. JOB-SPECIFIC
* **l_monats_id / MONATSID:** Monthly reporting ID representing the previous month. Passed as a parameter from Airflow using `{{ (execution_date - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%Y%m') }}`.
* **EintragsNr:** Run execution ID or task instance try number. Passed from Airflow using `{{ task_instance.try_number }}`.
* **DWH_JOB_KENNUNG:** Constant value of `'ABTN_SMART_KUBI'` representing metadata of the job execution.

***

### Risks and Manual Steps

1. **Partition-based Join Logic:** The original query joins a specific partition of `dwh$ta_f_d1_twvv_tn` (`dwh$ta_f_d1_twvv_tn_&1`) but also includes a redundant filter on `fact.gueltigkeitszeitpunkt`. In BigQuery, partitions are managed automatically by filtering on the partition column. It is critical to verify that `dwh_ta_f_d1_twvv_tn` is correctly partitioned on `gueltigkeitszeitpunkt` in BigQuery to ensure query cost control and optimal performance.
2. **Oracle Outer Joins (`(+)`) with Range Checks:** The query contains non-ANSI outer joins:
   `AND fact.dwh_tarif_id_neu = t_new.dwh_tarif_id (+)`
   `AND fact.dwh_tarif_id_alt = t_old.dwh_tarif_id (+)`
   `AND fact.dwh_vertrag_id = d.dwh_vertrag_id (+)`
   `AND l_monats_date > d.gueltig_von (+)`
   `AND l_monats_date <= d.gueltig_bis (+)`
   These must be carefully translated into standard `LEFT JOIN` clauses with the range checks included in the `ON` condition (as done in the pseudocode). Human verification is recommended to ensure no rows are eliminated incorrectly.
3. **Package Dependency (`dwpa_meldung.fehler`):** The PL/SQL block calls `dwpa_meldung.fehler` in its exception handler. This framework must either be mocked with a BigQuery stored procedure or handled via Airflow alerting, as BigQuery does not support direct PL/SQL packages natively.
4. **Truncate Execution (`dwpa_util_skript.runstatement`):** The original code runs truncate via a utility script statement execution. This has been safely converted to a direct standard SQL `TRUNCATE TABLE` statement in BigQuery, which is more robust and native.

---

### group 4/6 — DESIGN FAILED

ERROR: Design cannot proceed — REQUIRED TOOL returned errors, empty, or hollow (no real content extracted) responses on every attempt. Investigate the MCP service and retry the job.


---

### group 5/6 — DESIGN FAILED

ERROR: Design cannot proceed — REQUIRED TOOL returned errors, empty, or hollow (no real content extracted) responses on every attempt. Investigate the MCP service and retry the job.


---

### group 6/6 — DESIGN FAILED

ERROR: Design cannot proceed — REQUIRED TOOL returned errors, empty, or hollow (no real content extracted) responses on every attempt. Investigate the MCP service and retry the job.
