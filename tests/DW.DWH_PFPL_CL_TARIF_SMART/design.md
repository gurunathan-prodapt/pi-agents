=== OBJECT: DW.DWH_PFPL_CL_TARIF_SMART (JOBS_UNIX) ===
active=1
title=Check the currentness of smart-tarif-mapping
login=DW.UNIX.ISTNS
host=|DWHDWH5P|HOST
ert_seconds=9
launcher_type=sql_script
launcher_details={'job_arg': 'PFPL_CL_TARIF_SMART', 'sql_path': '$HOME/aktuell/pruef/pl/sql/d_pfpl_classic_tarif_smart.sql'}
script_body:
:inc DW.HOLE_PFAD
:set &DWH_JOB_KENNUNG='PFPL_CL_TARIF_SMART'
# [TRIMMED for the 3-file DE demo: ". $HOME/.dw_init" removed here --
#  framework env bootstrap, not this job's business logic; .dw_init is not
#  one of the 3 files in this demo. See the untrimmed chain under
#  isdwh/pruef/pl/... in ~/data for the real dot-source.]
$HOME/aktuell/allgemein/is/util/bin/r_sqlscript -f $HOME/aktuell/pruef/pl/sql/d_pfpl_classic_tarif_smart.sql -j PFPL_CL_TARIF_SMART -m v2
:inc DW.LESE_LOG
operational_notes=In case of abort  	Restart the job without any previous actions (Default)
In case of failure	Analyse the problem and restart

=== UNRESOLVED REFERENCES (object named but not supplied in this bundle) ===
  (none — every referenced object was supplied in this bundle)


# Migration Design Document: UC4 to Apache Airflow

## 1. Overview
This migration design covers a single UC4 native UNIX job, `DW.DWH_PFPL_CL_TARIF_SMART`. This job is responsible for checking the currentness (freshness/integrity) of the smart-tarif-mapping schema or table by executing a specific SQL script (`d_pfpl_classic_tarif_smart.sql`). Based on this extraction, the job is not part of a larger workflow bundle and has no defined calendar trigger or parent Job Plan (JOBP); it is therefore assumed to be triggered externally or run as a standalone utility.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.DWH_PFPL_CL_TARIF_SMART` | JOBS_UNIX | 1 (Active) | Check the currentness of smart-tarif-mapping |

## 3. Scheduling
* **Calendar/Schedule Object**: None present in this bundle.
* **Trigger Mechanism**: Externally triggered (source unknown from this extraction alone, as no referencing `JOBP` or `SCRI` trigger object was supplied in this bundle).
* **Airflow Schedule**: `schedule=None`

## 4. Airflow DAG Properties
Since no parent `JOBP` workflow was supplied, a single-task DAG will be defined to represent the execution of this standalone job.

| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_dwh_pfpl_cl_tarif_smart` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` *(Placeholder)* |
| **catchup** | `False` |
| **max_active_runs** | `1` |
| **is_paused_upon_creation** | `False` *(Active=1)* |
| **default_args** | `{"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}` |

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `check_smart_tarif_mapping` | `DW.DWH_PFPL_CL_TARIF_SMART` | `EmptyOperator` | N/A | N/A | Default | Default | None | None | False | None | # REVIEW-STRUCT: launcher wraps SQL script [`$HOME/aktuell/pruef/pl/sql/d_pfpl_classic_tarif_smart.sql`], converted separately by the companion KSH/SQL migration pipeline into EITHER a Python script or BigQuery SQL -- this extraction cannot know which. Confirm the actual artifact produced before wiring a real operator (BashOperator/PythonOperator for Python, BigQueryInsertJobOperator for BigQuery SQL); never assume Python. |

## 6. Task Dependency Map
Because this extraction consists of a single standalone job, there are no upstream or downstream tasks.
```python
check_smart_tarif_mapping
```

## 7. Sync / Concurrency Analysis
No UC4 Sync or Queue objects were defined for this job in the extraction. To prevent concurrent runs of this verification step overlapping, `max_active_runs=1` is configured at the DAG level.

## 8. Error Handling and Retry Strategy
The operational notes indicate:
* *In case of abort*: Restart the job without any previous actions (Default).
* *In case of failure*: Analyze the problem and restart.

Standard Airflow retry behavior (1 retry with a 5-minute delay) is defined in `default_args` to allow transient issues to resolve. No custom `on_failure_callback` or postcondition handling is specified in the extraction.

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `&DWH_JOB_KENNUNG` | `'PFPL_CL_TARIF_SMART'` | Airflow task parameter / environment variable (if needed by the converted script) |
| SQL Path | `$HOME/aktuell/pruef/pl/sql/d_pfpl_classic_tarif_smart.sql` | Target SQL script to migrate to BigQuery/Cloud SQL |

## 10. Developer Notes
* # REVIEW-STRUCT: The launcher type is `sql_script`. The SQL logic in `d_pfpl_classic_tarif_smart.sql` must be migrated separately. Depending on the target environment (e.g., Google Cloud Platform), this task should be converted from `EmptyOperator` to a `BigQueryInsertJobOperator` (for BigQuery SQL) or a `CloudSQLExecuteQueryOperator` / `PythonOperator` / `BashOperator` (if wrapping a local Python script running SQL). Do not assume Python is the execution path.
* # REVIEW: This job was supplied without a parent workflow (`JOBP`) or calendar trigger (`EVNT_TIME`). Verify how this workflow is scheduled in the production environment (e.g., whether it is triggered by an external scheduler, file listener, or a parent process not included in this bundle).

---

# Pseudocode Outline

```python
# ── Imports ──────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# ── GCP Configuration ────────────────────────────────────
# # REVIEW-STRUCT: Define connections, datasets, and GCS buckets here once the
# SQL migration pipeline target environment (BigQuery vs Cloud SQL) is finalized.
# GCS_BUCKET_NAME = "your-gcs-bucket-name"

# ── Default Args ─────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── on_failure_callback stubs ─────────────────────────────
# No custom failure callbacks or alert objects were defined in this extraction.

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id="dw_dwh_pfpl_cl_tarif_smart",
    default_args=DEFAULT_ARGS,
    description="Check the currentness of smart-tarif-mapping",
    schedule_interval=None,  # Externally triggered or manually run
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["uc4_migration", "dwh", "validation"],
) as dag:

    # ── Task: check_smart_tarif_mapping ───────────────────
    # # REVIEW-STRUCT: This is currently set to an EmptyOperator as a placeholder.
    # The source UC4 job runs a SQL script: $HOME/aktuell/pruef/pl/sql/d_pfpl_classic_tarif_smart.sql
    #
    # Action required: Replace EmptyOperator with the appropriate runner once SQL migration is complete.
    # Examples:
    # 1. If migrated to BigQuery:
    #    check_smart_tarif_mapping = BigQueryInsertJobOperator(
    #        task_id="check_smart_tarif_mapping",
    #        configuration={"query": {"query": migrated_sql_string, "useLegacySql": False}}
    #    )
    # 2. If executed as a Python script:
    #    check_smart_tarif_mapping = PythonOperator(
    #        task_id="check_smart_tarif_mapping",
    #        python_callable=run_migrated_tarif_check_script
    #    )
    check_smart_tarif_mapping = EmptyOperator(
        task_id="check_smart_tarif_mapping",
    )

    # ── Dependencies ─────────────────────────────────────────
    # Single-task pipeline. No explicit dependencies needed.
    check_smart_tarif_mapping
```

# Migration Design Document

### Schedule & variables
* **Schedule**: The UC4 job `DW.DWH_PFPL_CL_TARIF_SMART` is active (`<Active>1</Active>`) but has no timer-based calendar or cron schedule specified in this bundle. In the target Airflow environment, it will be configured with `schedule=None` (triggered externally or run manually).
* **Variables**:
  * `DWH_JOB_KENNUNG` = `'PFPL_CL_TARIF_SMART'`: Passed as a job-specific parameter or task environment variable in the Airflow DAG.

### Execution order
The target Airflow DAG orchestration preserves the legacy sequential call chain:
1. **Environment Setup**: Handled via Composer environment variables and task execution context (replacing `DW.HOLE_PFAD`).
2. **SQL Execution**: Execution of the validation script `d_pfpl_classic_tarif_smart.sql` using the target Airflow operator (e.g., `BigQueryInsertJobOperator`).
3. **Execution Verification**: Checking task execution logs and status (replacing `DW.LESE_LOG`).

### Lineage
* **Upstream / Included Components (Implicit)**:
  * `DW.HOLE_PFAD` (human-resolved: NO SOURCE NEEDED) — setup utility.
  * `DW.LESE_LOG` (human-resolved: NO SOURCE NEEDED) — logging utility.
* **Invoked Components (External - Sibling Files)**:
  * `d_pfpl_classic_tarif_smart.sql` — the validation SQL query script invoked by the UC4 job.
  * `r_sqlscript` — the execution runner.
* **Target Host**:
  * Runs on host `dwhdwh5p` using login package `DW.UNIX.ISTNS`. Maps to Cloud Composer running on GKE with appropriate Service Account credentials.

### Cross-file dependencies
The validation execution relies on the existence and schema correctness of the following tables queried by the SQL logic:
* `TABLE:AKTUELL`
* `TABLE:CASE`
* `TABLE:DIFFERENZ_SMART_AKTUELL`
* `TABLE:DWH$TA_L_MAP_PLATO_PARAM`
* `TABLE:DWH$TA_L_MAP_PLATO_TARIF_SMART`
* `TABLE:SMART`
* `TABLE:SUMME_DIFFERENZ`
* `TABLE:V_VERSION_SMART`

### Target file plan
* **File Path**: `dags/DW_DWH_PFPL_CL_TARIF_SMART.py`
  * **Language**: Python (Apache Airflow DAG)
  * **Source File**: `DW.DWH_PFPL_CL_TARIF_SMART.xml`

### Environment-specific values
* **GLOBAL**:
  * `GCP_PROJECT`: Sourced at runtime via Airflow Variable `Variable.get("GCP_PROJECT")`.
  * `BQ_LOCATION`: Sourced at runtime via Airflow Variable `Variable.get("BQ_LOCATION")`.
  * `BQ_DATASET`: Sourced at runtime via Airflow Variable `Variable.get("BQ_DATASET")`.
* **JOB-SPECIFIC**:
  * `DWH_JOB_KENNUNG`: Hardcoded or passed via DAG task parameters as `'PFPL_CL_TARIF_SMART'`.
  * `SQL_SCRIPT_PATH`: Path to the migrated SQL script `d_pfpl_classic_tarif_smart.sql`.

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DW.DWH_PFPL_CL_TARIF_SMART.xml` | `dags/DW_DWH_PFPL_CL_TARIF_SMART.py` | Converts the UC4 UNIX Job scheduling and orchestration logic into a Cloud Composer Airflow DAG. |

### Risks & Manual Actions
* **Verification of Sibling SQL**: The SQL script `d_pfpl_classic_tarif_smart.sql` is invoked by this job but is outside the scope of this migration pass. The DAG operator's execution logic must be finalized once the target SQL code is migrated.
* **Bootstrap Environment Initialization**: The legacy dot-sourced script `.dw_init` was removed in the trimmed source. The environment configurations normally handled there must be pre-configured in the Cloud Composer environment.

---

=== FILE: local/home/gurunathan_t/single_job_demo_v2/d_pfpl_classic_tarif_smart.sql ===
-- DWH Business Hub, Release 15.3
--
-- Datei       :
-- Version     : 15.3.0
-- Zweck       :
--
-- Thema       : Plato-Classic
-- Aufruf:
-- Autor       : Dorothea Horst
-- erstellt am : 20.07.2015
-- geaendert   : 16.12.2015 F. Hoefner: Erweiterung v_target
-- geandert    : 01:08.2016 R.Schmied : Erweiterung MP_PLATO_ID auf 5er Tupel
--
-- Aufruf:
-- $HOME/aktuell/allgemein/is/util/bin/r_sqlscript
--                  -f $HOME/aktuell/pruef/pl/sql/d_pfpl_classic_tarif_smart.sql
--                  -j PFPL_CL_TARIF_SMART
--                  -m v2
--

WHENEVER OSERROR EXIT SQL.OSCODE ROLLBACK;
WHENEVER SQLERROR EXIT FAILURE;

set serveroutput on
START $DW_DIR_ROOT/allgemein/is/util/sql/d_alis_init.sql

column col_anz_differenzen	new_value c_anz_differenzen	format 9999 noprint
column col_exit			new_value c_exit		format 9999 noprint


	WITH v_target AS
		(
		SELECT	parameter_text			target_database
		FROM	dwh$ta_l_map_plato_param
		WHERE	parameter_name = 'TARGET_DATABASE_SMART'
		AND     gueltig_bis    = to_date('31.12.4712', 'DD.MM.YYYY')
		),
	v_version_smart AS
		(
		SELECT	max(version_smart) max_version_smart
		FROM    DWH$TA_L_MAP_PLATO_TARIF_SMART
		),
	aktuell AS
		(
		SELECT	dwh_m.tarif_id,
			dwh_m.tarif_bez,
			dwh_m.mp_marktprodukt_id,
			dwh_m.mp_marktprodukt_bez,
			dwh_m.mp_familie_id,
			dwh_m.mp_familie_bez,
			dwh_m.mp_typ_id,
			dwh_m.mp_typ_bez,
			dwh_m.mp_tarifart_id,
			dwh_m.mp_tarifart_bez,
			dwh_m.mp_laufzeit_id,
			dwh_m.mp_laufzeit_bez,
			dwh_m.mp_bestandsrelevanz_id,
			dwh_m.mp_bestandsrelevanz_bez,
			dwh_m.mp_tarifsparte_id,
			dwh_m.mp_tarifsparte_bez,
			dwh_m.mp_vertragsbesonderheit_id,
			dwh_m.mp_vertragsbesonderheit_bez,
			dwh_m.mp_taktung_id,
			dwh_m.mp_taktung_bez,
			dwh_m.mp_sonderkartenart_id,
			dwh_m.mp_sonderkartenart_bez,
			dwh_m.mp_sonderkartentyp_id,
			dwh_m.mp_sonderkartentyp_bez,
			dwh_m.mp_eg_jn_id,
			dwh_m.mp_eg_jn_bez,
			dwh_m.mp_geschaeftsfeld_id,
			dwh_m.mp_geschaeftsfeld_bez,
			dwh_m.mp_provider_id,
			dwh_m.mp_provider_bez,
			dwh_m.mp_generation_id,
			dwh_m.mp_generation_bez,
			dwh_m.mp_standard_grundpreis_id,
			dwh_m.mp_standard_grundpreis_bez,
			dwh_m.mp_startguthaben_id,
			dwh_m.mp_startguthaben_bez,
			plato.plato_sparte_id,
			plato.plato_sparte_text,
			plato.plato_tarifart_id,
			plato.plato_tarifart_text,
			plato.plato_geschaeftsfeld_id,
			plato.plato_geschaeftsfeld_text,
			plato.plato_eg_jn_id,
			plato.plato_eg_jn_text,
			plato.mp_plato_id,
			plato.mp_plato_text,
			plato.plato_tarif_id,
			plato.plato_tarif_text,
			plato.plato_plan_tarif_vertrieb_id,
			plato.plato_plan_tarif_vertrieb_text,
			plato.plato_plan_tarif_market_id,
			plato.plato_plan_tarif_market_text,
			plato.plato_tariffamilie_id,
			plato.plato_tariffamilie_text,
			plato.plato_tariftyp_id,
			plato.plato_tariftyp_text,
			plato.gueltig_von,
			plato.gueltig_bis,
			plato.target_database
		FROM	d_tarif				dwh_m,
			dwh$ta_l_map_plato_mp_tarif	plato,
			v_target
		WHERE	plato.mp_plato_id	=	to_char (dwh_m.mp_marktprodukt_id)	|| '-' ||
							to_char (dwh_m.mp_tarifsparte_id)	|| '-' ||
                                                        to_char (dwh_m.mp_geschaeftsfeld_id)    || '-' ||
                                                        to_char (dwh_m.mp_eg_jn_id)             || '-' ||
                                                        to_char (dwh_m.mp_generation_id)
                AND     plato.gueltig_bis       =       to_date ('31.12.4712', 'dd.mm.yyyy')
                AND     plato.target_database   =       v_target.target_database
		),
		smart AS
		(
		SELECT  tarif_id,
			tarif_bez,
			mp_marktprodukt_id,
			mp_marktprodukt_bez,
			mp_familie_id,
			mp_familie_bez,
			mp_typ_id,
			mp_typ_bez,
			mp_tarifart_id,
			mp_tarifart_bez,
			mp_laufzeit_id,
			mp_laufzeit_bez,
			mp_bestandsrelevanz_id,
			mp_bestandsrelevanz_bez,
			mp_tarifsparte_id,
			mp_tarifsparte_bez,
			mp_vertragsbesonderheit_id,
			mp_vertragsbesonderheit_bez,
			mp_taktung_id,
			mp_taktung_bez,
			mp_sonderkartenart_id,
			mp_sonderkartenart_bez,
			mp_sonderkartentyp_id,
			mp_sonderkartentyp_bez,
			mp_eg_jn_id,
			mp_eg_jn_bez,
			mp_geschaeftsfeld_id,
			mp_geschaeftsfeld_bez,
			mp_provider_id,
			mp_provider_bez,
			mp_generation_id,
			mp_generation_bez,
			mp_standard_grundpreis_id,
			mp_standard_grundpreis_bez,
			mp_startguthaben_id,
			mp_startguthaben_bez,
			plato_sparte_id,
			plato_sparte_text,
			plato_tarifart_id,
			plato_tarifart_text,
			plato_geschaeftsfeld_id,
			plato_geschaeftsfeld_text,
			plato_eg_jn_id,
			plato_eg_jn_text,
			mp_plato_id,
			mp_plato_text,
			plato_tarif_id,
			plato_tarif_text,
			plato_plan_tarif_vertrieb_id,
			plato_plan_tarif_vertrieb_text,
			plato_plan_tarif_market_id,
			plato_plan_tarif_market_text,
			plato_tariffamilie_id,
			plato_tariffamilie_text,
			plato_tariftyp_id,
			plato_tariftyp_text,
			gueltig_von,
			gueltig_bis,
			target_database
		FROM    dwh$ta_l_map_plato_tarif_smart,
			v_version_smart
		WHERE	dwh$ta_l_map_plato_tarif_smart.version_smart = v_version_smart.max_version_smart
		),
	differenz_aktuell_smart AS
		(
		SELECT	*
		FROM	aktuell
		--
		MINUS
		SELECT	*
		FROM	smart
		),
	differenz_smart_aktuell AS
		(
		SELECT	*
		FROM	smart
		--
		MINUS
		SELECT	*
		FROM	aktuell
		),
	summe_differenz AS
		(
		SELECT	(
				(SELECT count(*)
			 	FROM	differenz_aktuell_smart)
				+
				(SELECT	count(*)
			 	FROM	differenz_smart_aktuell)
				) summe_differenz
		FROM	dual
		)
	SELECT	summe_differenz col_anz_differenzen,
		CASE WHEN summe_differenz = 0
			THEN 0
			ELSE 100
		END	col_exit
	FROM	summe_differenz
	;
	
	exec dbms_output.put_line ('Anzahl Differenzen : ' || &c_anz_differenzen);

exit &c_exit;


═══════════════════════════════════════════
SECTION 1 — DESIGN DOCUMENT
═══════════════════════════════════════════
 
Step 1: Understand the Script
1.1 Object Type: 
    Multi-statement migration script executing analytical validation checks, utilizing SQL*Plus variable substitutions, explicit exit statuses, and procedural diagnostic printing.

1.2 Business Logic Summary:
    The script validates data consistency between target and source datasets for Plato-Classic tariffs. It performs a mutual set-difference evaluation (A MINUS B and B MINUS A) between:
    - Current state data (`aktuell` CTE): A composite of `d_tarif` and `dwh$ta_l_map_plato_mp_tarif` (filtered on active status `31.12.4712` and linked to target database parameter profiles).
    - Aggregated target data (`smart` CTE): Extracted from `dwh$ta_l_map_plato_tarif_smart` for the latest historical snapshot version.
    
    If any structural or value-based discrepancies exist between these datasets, the script logs the count of differences and exits with system return code 100. If datasets match, it exits cleanly with code 0.

1.3 Entities Referenced:
    - `dwh$ta_l_map_plato_param` (table)
    - `DWH$TA_L_MAP_PLATO_TARIF_SMART` / `dwh$ta_l_map_plato_tarif_smart` (table)
    - `d_tarif` (table / view)
    - `dwh$ta_l_map_plato_mp_tarif` (table)
    - Column data types include: `VARCHAR2`/`STRING` (for IDs, texts, names), `NUMBER` (for IDs and metrics), and `DATE` (for temporal ranges).
 
Step 2: Oracle-Specific Construct Detection and Resolution
 
2.1 Data Type Conversions:
    - Oracle `DATE` (includes time component) → Resolved to BigQuery `DATETIME` to prevent time-truncation during comparison.
    - Oracle `NUMBER` / `NUMBER(p, s)` → Resolved to `INT64` (for identifiers/counts) or `NUMERIC` (for decimal precision fields).
    - Oracle `VARCHAR2` → Resolved to BigQuery `STRING`.
 
2.2 Implicit and Explicit Type Casting:
    - Oracle explicit `to_char` operations are resolved to explicit BigQuery `CAST(expression AS STRING)` operations.
    - Oracle explicit `to_date` conversions are mapped to explicit BigQuery `DATETIME` or `PARSE_DATETIME` literals.
 
2.3 NULL Handling and Conditional Functions:
    - No direct NVL/DECODE patterns exist in the core processing logic. A safe standard `CASE WHEN` statement is utilized for exit evaluation.
 
2.4 String Functions:
    - Oracle string concatenation operator `||` is mapped to the standard BigQuery `CONCAT` function for deterministic string resolution.
 
2.5 Date and Timestamp Functions:
    - `to_date('31.12.4712', 'dd.mm.yyyy')` → Resolved to BQ `DATETIME '4712-12-31 00:00:00'` to ensure matching temporal representations.
 
2.8 Set and Join Operations:
    - Oracle `MINUS` set operations are mapped to BigQuery `EXCEPT DISTINCT` to maintain duplicate elimination semantics.
 
2.9 Row Limiting and Sampling:
    - `FROM DUAL` construct used to compile scalar counts → Removed entirely, as BigQuery supports selecting expressions without a source table.
 
2.14 PL/SQL / Scripting Constructs:
    - SQL*Plus execution control (`WHENEVER SQLERROR`, `exit &c_exit`) → Replaced by BigQuery Scripting block with variable declarations (`DECLARE`, `SET`) and conditional standard error raising (`ERROR()`).
    - `dbms_output.put_line` → Converted to standard BQ Scripting `SELECT` log statement.
 
2.15 Unresolvable or Advisory Items:
    - External SQL initialization file (`START $DW_DIR_ROOT/allgemein/is/util/sql/d_alis_init.sql`) → Flagged as external dependency; its settings must be parsed separately or handled via a calling orchestrator (e.g., Python wrapper).
 
Step 3: Conversion Strategy Summary
3.1 Overall Conversion Approach:
    Direct translation to a declarative BigQuery Scripting Block. Scripting variables are declared at the head of the scope, set via a structured transaction query, logged to standard output, and evaluated to trigger a script-level runtime error (`ERROR()`) if execution logic dictates a failure exit status (100).
 
3.2 Assumptions:
    - The underlying environment execution system is capable of catching runtime BigQuery query failure states (which occur when the `ERROR()` function is executed) and interpreting them as non-zero exit codes.
    - External initialization file `d_alis_init.sql` does not perform DML/DDL that changes target query results.
 
3.3 Flagged Items:
    - External initialization dependency (`d_alis_init.sql`).
    - Standard OS environment variable capturing for exit status (delegated to Python orchestrator or wrapper code).

═══════════════════════════════════════════
SECTION 2.16 — MIGRATION DECISION MATRIX
═══════════════════════════════════════════

| Oracle Statement/Construct | Selected Target | Rejected Alternatives | Evidence / Reason |
| :--- | :--- | :--- | :--- |
| Set Operations (`MINUS`) | `EXCEPT DISTINCT` | Direct `LEFT JOIN` / `NOT EXISTS` | `EXCEPT DISTINCT` is the direct ANSI SQL standard replacement for `MINUS`, matching its distinct-row subtraction behavior exactly. |
| Type Casting (`to_char`, `to_date`) | Standard `CAST` and `DATETIME` Literals | `PARSE_DATE` / `FORMAT_DATE` | Oracle DATE has a time component. Using native `DATETIME` prevents data truncation issues during set difference. |
| Dual Query (`FROM dual`) | Omit `FROM` | `SELECT ... FROM (SELECT 1)` | BigQuery allows execution of scalar operations directly in `SELECT` without referencing any table. |
| SQL*Plus Logging (`dbms_output`) | Scripting `SELECT` | Standard Python Print | Logging can be natively returned as a single-row string result from BigQuery scripting blocks. |
| OS Exit Handling (`exit &c_exit`) | Python Wrapper + BQ `ERROR()` | Direct SQL Client Exit | BigQuery engine has no concept of terminal OS exit codes. A Python execution script must intercept failure queries. |

═══════════════════════════════════════════
SECTION 2.17 — REQUIRED ARTIFACTS
═══════════════════════════════════════════

The migration build must generate two key artifacts:
1. **BigQuery SQL Scripting Block**: Containing the variable declarations, fully mapped CTE blocks, validation logic, and runtime error generation structure.
2. **Python Orchestration Wrapper (`run_tarif_validation.py`)**:
   - **Inputs**: GCP Project ID, Target Dataset Names.
   - **Invocation Contract**: Executed via standard shell command.
   - **Libraries**: `google-cloud-bigquery`
   - **Coordination**: Establishes API connection, sends the BigQuery SQL script, captures the log message output from execution, intercepts any scripted engine failure exceptions (`ERROR()`), and returns OS exit status `100` on failure or `0` on clean matches.

═══════════════════════════════════════════
SECTION 2.18 — DATA TYPE COMPATIBILITY TABLE
═══════════════════════════════════════════

| Oracle Column/Type | BigQuery Target Type | Conversion / Mapping Rule | Warnings & Risks |
| :--- | :--- | :--- | :--- |
| `DATE` (temporal fields) | `DATETIME` | Map to `DATETIME` | Ensures time-components are not truncated (as they would be with `DATE`). |
| `NUMBER` (IDs) | `INT64` | Explicit precision casting | Assumed non-fractional identifiers fit safely in 64-bit integers. |
| `VARCHAR2` | `STRING` | Direct mapping | BigQuery strings are UTF-8 compliant and have no length limitations. |

═══════════════════════════════════════════
SECTION 2.19 — DESIGN REVIEW SUMMARY
═══════════════════════════════════════════
- **Patterns/Objects Found**: SQL*Plus environment configurations, relational CTEs with explicit joins, mutual structural difference operations via `MINUS`, scalar block wrapping, and procedural system exit routines.
- **Unsupported Functions**: Oracle SQL*Plus commands (`set`, `column`, `exit`, `START`), DBMS Output library.
- **UDF Required**: No.
- **Python Required**: Yes (required to execute the pipeline and translate BigQuery script runtime failures into system OS exit codes).
- **Direct Dependencies**: Table datasets `d_tarif`, `dwh$ta_l_map_plato_mp_tarif`, `dwh$ta_l_map_plato_param`, and `dwh$ta_l_map_plato_tarif_smart`.
- **Manual Intervention Items**: SQL*Plus init file `d_alis_init.sql` contents must be evaluated manually if they set runtime session properties.

OVERALL MIGRATION STRATEGY: Python Wrapper Required

═══════════════════════════════════════════
SECTION 2.21 — ORACLE FUNCTION ANALYSIS TABLE
═══════════════════════════════════════════

| Oracle Function/Construct | Supported in BigQuery | BigQuery Equivalent / Alternative |
| :--- | :--- | :--- |
| `to_date('31.12.4712', 'dd.mm.yyyy')` | Direct-with-rewrite | `DATETIME '4712-12-31 00:00:00'` |
| `to_char(numeric_expression)` | Direct-with-rewrite | `CAST(numeric_expression AS STRING)` |
| `||` (String Concatenation) | Direct-with-rewrite | `CONCAT(a, b, ...)` |
| `MINUS` | Direct-with-rewrite | `EXCEPT DISTINCT` |
| `FROM dual` | Direct-with-rewrite | Omit `FROM` statement entirely |
| `dbms_output.put_line` | Direct-with-rewrite | BigQuery Scripting `SELECT` expression |
| `exit [code]` | Direct-with-rewrite | Scripting `ERROR()` control check inside standard Python runner wrapper |

 
═══════════════════════════════════════════
SECTION 2 — PSEUDOCODE
═══════════════════════════════════════════
 
Step 4: Write Vendor-Neutral Pseudocode

```sql
-- DECLARE scripting variables to replace SQL*Plus column allocations
DECLARE c_anz_differenzen INT64;
DECLARE c_exit INT64;

-- Execute the comparison calculation and capture results in defined script state
SET (c_anz_differenzen, c_exit) = (
  WITH v_target AS
    (
    SELECT parameter_text AS target_database
    FROM dwh_dataset.dwh$ta_l_map_plato_param
    WHERE parameter_name = 'TARGET_DATABASE_SMART'
      AND guelig_bis = DATETIME '4712-12-31 00:00:00' -- converted from to_date('31.12.4712', 'DD.MM.YYYY')
    ),
  v_version_smart AS
    (
    SELECT MAX(version_smart) AS max_version_smart
    FROM dwh_dataset.DWH$TA_L_MAP_PLATO_TARIF_SMART
    ),
  aktuell AS
    (
    SELECT 
      dwh_m.tarif_id,
      dwh_m.tarif_bez,
      dwh_m.mp_marktprodukt_id,
      dwh_m.mp_marktprodukt_bez,
      dwh_m.mp_familie_id,
      dwh_m.mp_familie_bez,
      dwh_m.mp_typ_id,
      dwh_m.mp_typ_bez,
      dwh_m.mp_tarifart_id,
      dwh_m.mp_tarifart_bez,
      dwh_m.mp_laufzeit_id,
      dwh_m.mp_laufzeit_bez,
      dwh_m.mp_bestandsrelevanz_id,
      dwh_m.mp_bestandsrelevanz_bez,
      dwh_m.mp_tarifsparte_id,
      dwh_m.mp_tarifsparte_bez,
      dwh_m.mp_vertragsbesonderheit_id,
      dwh_m.mp_vertragsbesonderheit_bez,
      dwh_m.mp_taktung_id,
      dwh_m.mp_taktung_bez,
      dwh_m.mp_sonderkartenart_id,
      dwh_m.mp_sonderkartenart_bez,
      dwh_m.mp_sonderkartentyp_id,
      dwh_m.mp_sonderkartentyp_bez,
      dwh_m.mp_eg_jn_id,
      dwh_m.mp_eg_jn_bez,
      dwh_m.mp_geschaeftsfeld_id,
      dwh_m.mp_geschaeftsfeld_bez,
      dwh_m.mp_provider_id,
      dwh_m.mp_provider_bez,
      dwh_m.mp_generation_id,
      dwh_m.mp_generation_bez,
      dwh_m.mp_standard_grundpreis_id,
      dwh_m.mp_standard_grundpreis_bez,
      dwh_m.mp_startguthaben_id,
      dwh_m.mp_startguthaben_bez,
      plato.plato_sparte_id,
      plato.plato_sparte_text,
      plato.plato_tarifart_id,
      plato.plato_tarifart_text,
      plato.plato_geschaeftsfeld_id,
      plato.plato_geschaeftsfeld_text,
      plato.plato_eg_jn_id,
      plato.plato_eg_jn_text,
      plato.mp_plato_id,
      plato.mp_plato_text,
      plato.plato_tarif_id,
      plato.plato_tarif_text,
      plato.plato_plan_tarif_vertrieb_id,
      plato.plato_plan_tarif_vertrieb_text,
      plato.plato_plan_tarif_market_id,
      plato.plato_plan_tarif_market_text,
      plato.plato_tariffamilie_id,
      plato.plato_tariffamilie_text,
      plato.plato_tariftyp_id,
      plato.plato_tariftyp_text,
      plato.gueltig_von,
      plato.gueltig_bis,
      plato.target_database
    FROM dwh_dataset.d_tarif AS dwh_m
    CROSS JOIN v_target
    INNER JOIN dwh_dataset.dwh$ta_l_map_plato_mp_tarif AS plato
      ON plato.mp_plato_id = CONCAT(
        CAST(dwh_m.mp_marktprodukt_id AS STRING), '-',         -- converted from to_char()
        CAST(dwh_m.mp_tarifsparte_id AS STRING), '-',          -- converted from to_char()
        CAST(dwh_m.mp_geschaeftsfeld_id AS STRING), '-',       -- converted from to_char()
        CAST(dwh_m.mp_eg_jn_id AS STRING), '-',                -- converted from to_char()
        CAST(dwh_m.mp_generation_id AS STRING)                 -- converted from to_char()
      )
      AND plato.gueltig_bis = DATETIME '4712-12-31 00:00:00'    -- converted from to_date('31.12.4712', 'dd.mm.yyyy')
      AND plato.target_database = v_target.target_database
    ),
  smart AS
    (
    SELECT 
      tarif_id,
      tarif_bez,
      mp_marktprodukt_id,
      mp_marktprodukt_bez,
      mp_familie_id,
      mp_familie_bez,
      mp_typ_id,
      mp_typ_bez,
      mp_tarifart_id,
      mp_tarifart_bez,
      mp_laufzeit_id,
      mp_laufzeit_bez,
      mp_bestandsrelevanz_id,
      mp_bestandsrelevanz_bez,
      mp_tarifsparte_id,
      mp_tarifsparte_bez,
      mp_vertragsbesonderheit_id,
      mp_vertragsbesonderheit_bez,
      mp_taktung_id,
      mp_taktung_bez,
      mp_sonderkartenart_id,
      mp_sonderkartenart_bez,
      mp_sonderkartentyp_id,
      mp_sonderkartentyp_bez,
      mp_eg_jn_id,
      mp_eg_jn_bez,
      mp_geschaeftsfeld_id,
      mp_geschaeftsfeld_bez,
      mp_provider_id,
      mp_provider_bez,
      mp_generation_id,
      mp_generation_bez,
      mp_standard_grundpreis_id,
      mp_standard_grundpreis_bez,
      mp_startguthaben_id,
      mp_startguthaben_bez,
      plato_sparte_id,
      plato_sparte_text,
      plato_tarifart_id,
      plato_tarifart_text,
      plato_geschaeftsfeld_id,
      plato_geschaeftsfeld_text,
      plato_eg_jn_id,
      plato_eg_jn_text,
      mp_plato_id,
      mp_plato_text,
      plato_tarif_id,
      plato_tarif_text,
      plato_plan_tarif_vertrieb_id,
      plato_plan_tarif_vertrieb_text,
      plato_plan_tarif_market_id,
      plato_plan_tarif_market_text,
      plato_tariffamilie_id,
      plato_tariffamilie_text,
      plato_tariftyp_id,
      plato_tariftyp_text,
      gueltig_von,
      gueltig_bis,
      target_database
    FROM dwh_dataset.dwh$ta_l_map_plato_tarif_smart
    CROSS JOIN v_version_smart
    WHERE dwh$ta_l_map_plato_tarif_smart.version_smart = v_version_smart.max_version_smart
    ),
  differenz_aktuell_smart AS
    (
    SELECT * FROM aktuell
    EXCEPT DISTINCT -- converted from MINUS
    SELECT * FROM smart
    ),
  differenz_smart_aktuell AS
    (
    SELECT * FROM smart
    EXCEPT DISTINCT -- converted from MINUS
    SELECT * FROM aktuell
    ),
  summe_differenz AS
    (
    SELECT (
        (SELECT COUNT(*) FROM differenz_aktuell_smart)
        +
        (SELECT COUNT(*) FROM differenz_smart_aktuell)
      ) AS val_differenz
    -- Removed FROM DUAL
    )
  SELECT AS STRUCT 
    val_differenz AS col_anz_differenzen,
    CASE 
      WHEN val_differenz = 0 THEN 0
      ELSE 100
    END AS col_exit
  FROM summe_differenz
);

-- Output validation result (converted from exec dbms_output.put_line)
SELECT CONCAT('Anzahl Differenzen : ', CAST(c_anz_differenzen AS STRING)) AS execution_log;

-- Process exit assertions mapping SQL*Plus dynamic exit commands
IF c_exit != 0 THEN
  ERROR(
    CONCAT('Data validation check failed. Validation code: ', CAST(c_exit AS STRING), '. Differences detected: ', CAST(c_anz_differenzen AS STRING))
  );
END IF;
```

═══════════════════════════════════════════
FLAGGED ITEMS FOR HUMAN REVIEW
═══════════════════════════════════════════
1. **Dynamic Client Exit Validation**: Native BigQuery SQL does not directly support standard exit-code propagation back to command-line shells. Ensure the Python calling routine implements try/except handling around the Google Cloud client library requests. It must capture the generated `ERROR` message context and map it to standard system terminal code `100` on validation failures.
2. **External Script Path Execution**: The start initialization file dependency (`$DW_DIR_ROOT/allgemein/is/util/sql/d_alis_init.sql`) cannot be parsed inside BigQuery engine routines. If configuration data or schema validation rules are dynamically established by this dependency, these variables/settings must be extracted manually and defined at the head of the deployment environment or within the wrapper Python script execution scope.

### Execution order
The target orchestration must preserve the following legacy execution sequence of components:
1. **DW.DWH_PFPL_CL_TARIF_SMART.xml** (Orchestration entrypoint) -> Handled by the Cloud Composer DAG orchestration layer (out of scope for this file group).
2. **d_pfpl_classic_tarif_smart.sql** -> Executed on BigQuery as a BigQuery SQL Scripting Block.
3. **r_sqlscript** -> Replaced by BigQuery's native execution engine called via the Composer `BigQueryInsertJobOperator`.

---

### Schedule & variables
The timing and parameters from the source scheduler must be retained using native target mechanisms:
- **Schedule**: This validation job is triggered as part of the wider Plato-Classic pipeline orchestration. Its scheduling and execution trigger must be managed by the parent Cloud Composer DAG.
- **Variables**:
  - `DWH_JOB_KENNUNG` = `'PFPL_CL_TARIF_SMART'` -> Must be passed to the BigQuery scripting block or logging tasks using Airflow DAG `params` or environment variables, maintaining metadata consistency.

---

### Lineage
The lineage connections for the source SQL script contain both actual table reads and internal CTE query references:
- **Upstream Tables / Producers**:
  - `dwh$ta_l_map_plato_param` (table)
  - `DWH$TA_L_MAP_PLATO_TARIF_SMART` (table)
  - `d_tarif` (table/view referenced inside the SQL code)
  - `dwh$ta_l_map_plato_mp_tarif` (table referenced inside the SQL code)
- **Inline Components (Detected in Lineage as Tables)**:
  - `AKTUELL`, `V_VERSION_SMART`, `SMART`, `DIFFERENZ_SMART_AKTUELL`, `SUMME_DIFFERENZ`, and `CASE` are internal query CTEs or syntax constructs within the script, not physical database tables.
- **External/Procedure Calls**:
  - `PROCEDURE:PUT_LINE` -> Represents the legacy PL/SQL Oracle `dbms_output.put_line` operation. Converted to a standard BigQuery Scripting logging SELECT statement.

---

### Target file plan
The following target file will be generated for this specific component:
- **Target File Path**: `d_pfpl_classic_tarif_smart.sql`
- **Language**: `SQL` (BigQuery SQL Scripting Block)
- **Source File**: `local/home/gurunathan_t/single_job_demo_v2/d_pfpl_classic_tarif_smart.sql`

---

### Environment-specific values
Every environment-dependent or platform-sourced variable is categorized below to guide the Build Agent:

1. **GLOBAL (Environment-wide)**:
   - `GCP_PROJECT`: Sourced at runtime via query parameters (`@gcp_project`) or configured via the Airflow connection.
   - `BQ_DATASET`: The target BigQuery dataset containing the source and target tables, sourced using query parameters or Airflow DAG variables.
   - `DW_DIR_ROOT`: Replaced by a shared Google Cloud Storage (GCS) path (e.g., `gs://<shared_bucket>/allgemein/is/util/sql/`) or retired entirely since the initialization script `d_alis_init.sql` is folded into the global environment configuration.

2. **JOB-SPECIFIC**:
   - `d_tarif`, `dwh$ta_l_map_plato_mp_tarif`, `dwh$ta_l_map_plato_param`, `dwh$ta_l_map_plato_tarif_smart`: Inline database tables. They will be prefixed with the dynamic environment-wide `BQ_DATASET` variable during execution (e.g., `` `gcp_project.bq_dataset.d_tarif` ``).

---

### File Disposition
The following table lists the disposition of the file within our assigned scope:

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/single_job_demo_v2/d_pfpl_classic_tarif_smart.sql` | `d_pfpl_classic_tarif_smart.sql` | Converted to a native BigQuery SQL scripting block containing validation logic, minus set operations (`EXCEPT DISTINCT`), variable declarations, and exit condition raises. |

---

=== CONFIRMED TARGET PLATFORM ===
TARGET_PLATFORM: BIGQUERY
This is stated directly by the caller, not inferred from the extraction below. Treat it as ground truth everywhere the design/build instructions would otherwise infer the platform or fall back to a default -- it satisfies the "unless explicitly stated in the extraction" condition those rules already reference. Never emit "# REVIEW: target database platform not specified" while this section is present.

=== FILE: local/home/gurunathan_t/single_job_demo_v2/r_sqlscript ===
#!/bin/ksh_dwh
#
# Zweck:
#      siehe usage
# Aenderung : 27.05.2002; Stefan Kurz
#
# Historie  :
#   3.0.1; 05.07.2000;  Stephan Kriwet
#       - initiale Version
#   3.0.2; 21.09.2000; Marcus Blaha
#       - Jobkennung Parameter -j
#       - in DWTK_Meldungen wird PROGRAMM mit r_sqlscript_namedessqlscripts gefuellt
#   3.5.1  03.11.2000 Stephan Kriwet
#       - Parameter -i Inputstring.
#         Der Inputstring wird als Parameter an das SQL-Script weitergegeben
#   5.0.1  29.05.2002 Claudia Toussaint
#       - $DW_EintragsNr wird als letzter Parameterwert an sql-Script weitergereicht
#   9.1.1 24.03.2009 Arthur Feljauer
#	- neuer optionaler Parameter "-m v2" (nur Modus v2 moeglich) eingefuegt, wodurch diverse Aenderungen greifen
#	  sollte der neue Modus nicht angegeben werden, dann greift die alte Variante
#	- Aenderungen gegenueber der alten Variante
#		- Alle Parameter werden vollstaendig in die Meldungstabelle geschrieben (Attribut Parameter)
#		- Spalte "Programm" der Meldungstabelle enthaelt nur noch den Programmnamen
#		- Entrynr wird immer als erster Parameter an das SQL Skript uebergeben
#  20.3.1 26.10.2020 Markus Simon
#       - Option, im "v2"-Modus einen zweiten Kommandozeilenparameter mit -k zu �bergeben


ProgName="Ausf�hrung Script $0"
ProgVersion="5.0.0"

# Funktion:
#    usage - Ausgabe der Programmbeschreibung
usage(){
cat <<EOF
   Programm: $ProgName
   Version: $ProgVersion
   Aufruf: $0 Parameter

   Das als Parameter -f  �bergebene SQL-Script wird ausgef�hrt.
   Es mu� die Zeile "whenever sqlerror exit failure" enthalten,
   damit das Rahmenscript bei Fehlern abbricht.
   Der mit dem Parameter -i �bergebene String wird an das SQL-Script
   weitergereicht.
   Im Modus "-m v2" kann mit "-k" optional ein zweiter Parameter �bergeben werden.
   Wenn das SQL-Script keinen Pfad hat, wird es  erst in  ../sql
   parallel zum Ablageverzeichnis dieses Rahmenscripts vermutet,
   dann direkt im Ablageverzeichnis dieses Rahmenscripts.
   Dies Rahmenscript mu� deswegen immer mit Komplettpfad aufgerufen werden
   oder direkt aus  dem  Verzeichnis, in dem es gespeichert ist.

   -m v2 (Modus v2, optional)
   Aenderungen gegenueber der alten Variante
   	- Alle Parameter werden vollstaendig in die Meldungstabelle geschrieben (Attribut Parameter)
   	- Spalte "Programm" der Meldungstabelle enthaelt nur noch den Programmnamen
   	- Entrynr wird immer als erster Parameter an das SQL Skript uebergeben



   Parameter:
       -f     hier wird der Name des SQL-Scripts angegeben

       -i     m�gliche Parameter f�r das SQL-Script

       -k     m�glicher zweiter Parameter f�r das SQL-Script (nur in Kombination mit "-m v2")

       -j     Jobkennung (default DWH_KORR)

       -m     Modusangabe (nur v2 -> neue Variante , Parameter nicht angegeben oder falsch -> alte Variante)

       -h     zeigt diese Seite an

       -v     verbose (zeigt bei Fehler sofort die Logdatei an)
EOF
}

#####################
# Vorbereitende Massnahmen

# [TRIMMED for the 3-file DE demo: ". $HOME/aktuell/.dw_init" removed here --
#  framework env bootstrap, not this job's business logic; .dw_init is not
#  one of the 3 files in this demo. See the untrimmed chain under
#  isdwh/allgemein/is/util/bin/... in ~/data for the real dot-source.]

# [TRIMMED: ". ${DW_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh"
#  removed -- error-framework helper (failure method), not included in this demo.]

# [TRIMMED: ". ${DW_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh"
#  removed -- defines the starteSQLSkript helper function called below;
#  that helper file is not included in this demo's 3 files.]


set -e

ErrNr=0
ErrArg=""

DW_EintragsNr=0
export DW_EintragsNr

typeset l_DBskript

#####################
# Lesen der Parameter
ParamList="f:j:i:k:m:" # Notation gemaess getopts(1)
#Initialwerte der Parameter
typeset p_Verbose=0
typeset -l p_sqlscript

# lese mit Hilfe getopts die Parameter
while getopts ":hv$ParamList" param
do
    case $param in
        f)
            p_sqlscript=$OPTARG;;
        i)
            p_sqlpar=$OPTARG;;
        k)
            p_sqlpar2=$OPTARG;;
        v)
            p_Verbose=1;;
        j)
            p_Job=$OPTARG;;
        m)
            p_Modus=$OPTARG;;
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

cd `dirname $0`
case `dirname ${p_sqlscript}` in
'.') l_DBskript=../sql/${p_sqlscript};
     if [ ! -f "$l_DBskript" ]
     then
         l_DBskript=${p_sqlscript}
     fi;;
*) l_DBskript=${p_sqlscript};;              #mit  Pfad
esac


if [  -f "$l_DBskript" ]
then

    ErrNr=198 # Parameterwert unbekannt
    ErrArg="$p_Kuerzel"
fi


#####################
# Vorbereitende Massnahmen
#    Definition von weiteren Variablen
#    weitere Arbeiten..


typeset -u JobKennung  # Kennung in Grossbuchstaben
if [[ $p_Job = "" ]]
then
   JobKennung="DWH_KORR" # JobKennung eintragen gemaess Namenskonvention
else
   JobKennung=$p_Job
fi

echo "----------------- Parameter -----------------"
echo "Jobkennung     : $JobKennung"
echo "DB-Skript      : $l_DBskript"
echo "---------------------------------------------"


# Nachfolgende Anweisungen sollten sofort nach bekanntwerden
# der JobKennung durchgefuehrt werden, da sonst keine
# Fehlerbehandlung aktiv ist.
DWMSG_ErmittleNr DW_EintragsNr
DWMSG_Logdateiname LogDatei $JobKennung $DW_EintragsNr

# wenn der Modus v2 verwendet wird, werden alle Parameter ins Attribut Parameter der Meldungstabelle geschrieben
# und Attribut Programm enthaelt nur den eigentlichen Programmnamen inkl. Pfad
if [ "$p_Modus" = "v2" ]
then
    DWMSG_ErzeugeEintrag $DW_EintragsNr $JobKennung $0 $LogDatei "$*" >> $LogDatei 2>&1
else
  DWMSG_ErzeugeEintrag $DW_EintragsNr $JobKennung $0_$l_DBskript $LogDatei >> $LogDatei 2>&1
fi

# Setze traps
typeset aktion=""
typeset trap="DWMSG_Fehlerbehandlung $DW_EintragsNr >> $LogDatei 2>&1"
typeset trap_os="$trap ; echo '!OSFEHLER gemeldet!'"
typeset trap_err="$trap ;echo '!FEHLER gemeldet!'"

if [ "$p_Verbose" != "0" ]
then
    # Setze DEBUG Traps (Logdateiausgabe am Ende)
    aktion="; cat $LogDatei "
fi

trap "$trap_os  $aktion ; exit 1" INT  >> $LogDatei 2>&1
trap "$trap_err $aktion" ERR >> $LogDatei 2>&1

#####################
# Eigentlicher Job

    # Aufrufe des Kernskriptes etc. mit Umleitung >>$Logdatei
    echo "----------------- Job -----------------------"
    echo "Job-Nr    : '$DW_EintragsNr'"
    echo "Logdatei  : '$LogDatei'"
    echo "---------------------------------------------"

# Fuehre Skript zum Abgleich aus
# Wenn Modus v2, dann wird immer die Eintragsnummer als erstes uebergeben
if [ "$p_Modus" = "v2" ]
then
   starteSQLSkript $DW_EintragsNr $l_DBskript $DW_EintragsNr $p_sqlpar $p_sqlpar2 >> $LogDatei 2>&1
else
   starteSQLSkript $DW_EintragsNr $l_DBskript $p_sqlpar $DW_EintragsNr >> $LogDatei 2>&1
fi

#####################
# Nachbereitende Massnahmen
#    Abschalten der Fehlerbehandlung
#    weitere Arbeiten..

# Abschalten der Fehlerbehandlung
DWMSG_SetzeStatusOK $DW_EintragsNr >> $LogDatei 2>&1

trap INT ERR

echo "Die Abarbeitung des Rahmenskriptes ist ohne erkennbare Fehler beendet"


=== CONVERSION VERDICT ===
VERDICT: PYTHON
REASON: This script is an orchestration and logging wrapper that handles command-line arguments, dynamic file paths, shell traps, and framework-specific metadata logging, which cannot be expressed natively in BigQuery SQL.

EVIDENCE
- Business logic found: KSH custom logic parses options (-f, -i, -k, -j, -m), resolves SQL file paths, registers execution status in database tables via custom framework helpers (DWMSG_*), and orchestrates SQL execution based on version mode.
- AWK: none
- SQL-expressible: No, the wrapper manages OS-level activities (traps, logging, path construction) and framework state.
- Non-SQL side effects: Creates and appends to local log files, manages process interrupt traps (INT, ERR), and executes external framework logging binaries/functions.
- Against this verdict: If the underlying SQL scripts called by this wrapper were merged and analyzed, they might be SQL-expressible; however, this script itself is purely a generic Python orchestration candidate.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script (`r_sqlscript`) is a generic KornShell framework utility used to run SQL scripts. It acts as an orchestrator that sets up logging, registers jobs and execution metadata into centralized tracking tables, captures options, resolves the dynamic paths of target SQL scripts, and executes them via `starteSQLSkript`. It supports a legacy execution signature as well as a newer "v2" mode supporting multiple input arguments.

2. INVOCATION CONTEXT
   - Invoker: Typically called by UC4/Automic jobs (using standard UNIX JOBS objects) or parent shell orchestrators.
   - Arguments passed: Custom command-line flags (`-f`, `-i`, `-k`, `-j`, `-m`, `-v`, `-h`).
   - Environment files sourced:
     - `. $HOME/aktuell/.dw_init` — # REVIEW-STRUCT: environment file [.dw_init] not supplied — variables it sets are unknown; do not guess their names or values.
     - `. ${DW_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` — # REVIEW-STRUCT: environment file [f_alis_msgerr.ksh] not supplied — variables/functions it defines are unknown.
     - `. ${DW_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` — # REVIEW-STRUCT: environment file [h_alis_sqlplus.ksh] not supplied — defines starteSQLSkript; internal behaviour unknown.

3. PARAMETERS / INPUTS
   - `p_sqlscript` (`-f` flag): Sourced via command-line arguments. Defines the SQL script to be run. Always converted to lowercase due to `typeset -l`. Used in script body.
   - `p_sqlpar` (`-i` flag): Sourced via command-line arguments. First parameter passed to the SQL script. Used in script body.
   - `p_sqlpar2` (`-k` flag): Sourced via command-line arguments. Second parameter passed to the SQL script (used in "v2" mode). Used in script body.
   - `p_Verbose` (`-v` flag): Sourced via command-line arguments. Controls verbose error/logging behavior. Used in script body.
   - `p_Job` / `JobKennung` (`-j` flag): Sourced via command-line arguments. Name of the target job/framework entry (defaults to "DWH_KORR"). Used in script body.
   - `p_Modus` (`-m` flag): Sourced via command-line arguments. Execution mode (expects "v2" or defaults to legacy). Used in script body.
   - `DW_EintragsNr` (Environment variable): Generated dynamically via the framework helper `DWMSG_ErmittleNr`. Used as state parameter and registered with SQL execution.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `starteSQLSkript`:
     - Exact command lines:
       - Mode v2: `starteSQLSkript $DW_EintragsNr $l_DBskript $DW_EintragsNr $p_sqlpar $p_sqlpar2 >> $LogDatei 2>&1`
       - Legacy mode: `starteSQLSkript $DW_EintragsNr $l_DBskript $p_sqlpar $DW_EintragsNr >> $LogDatei 2>&1`
     - Purpose: Executes the target SQL script using the database client. Since the target platform is confirmed as BigQuery, this would typically map to running a `.sql` file using the Google Cloud BigQuery Python client.
     - Resolver Status: # REVIEW-STRUCT: launcher [starteSQLSkript] invoked — internal behaviour not available in this extraction; confirm logging, error propagation, and credential handling before finalizing the conversion.
   - `DWMSG_MeldeFehler`, `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStatusOK`, `DWMSG_Fehlerbehandlung`:
     - Purpose: Internal framework tools/functions for metadata management, error reporting, and log management.
     - Integration: These must remain external calls or be replaced with equivalent enterprise Python logging and metadata management APIs.

5. EMBEDDED SQL
   - No direct embedded SQL exists within this wrapper script. It dynamically executes external SQL files targeted by `-f`.

6. CONTROL FLOW
   1. **Environment Initialization**: Source the framework configuration files (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_sqlplus.ksh`).
   2. **Option Parsing**: Parse `-f`, `-i`, `-k`, `-j`, `-m`, `-v`, and `-h` using `getopts` loop. Convert the SQL filename (`p_sqlscript`) to lowercase.
   3. **Input Validation**: Check if parsing errors occurred. If `ErrNr != 0`, call `DWMSG_MeldeFehler` and exit with the error code.
   4. **SQL Path Resolution**: Locate the target SQL file. If the path is relative (`.`), check if the script exists in `../sql/` relative to the wrapper execution directory; otherwise fall back to direct relative/absolute pathing.
   5. **Job Identification**: Extract `JobKennung` and set default to `"DWH_KORR"` if not specified. Ensure it is uppercase.
   6. **Logging & Job Entry Allocation**:
      - Obtain a unique log/entry number via `DWMSG_ErmittleNr`.
      - Generate a standardized log file name via `DWMSG_Logdateiname`.
      - Create a database status entry via `DWMSG_ErzeugeEintrag`.
   7. **Trap Registration**: Set up shell traps for process interrupts (`INT`, `ERR`) to capture failures, run `DWMSG_Fehlerbehandlung`, and optionally dump log content to stdout if verbose mode is enabled.
   8. **SQL Execution**:
      - If `p_Modus` equals `"v2"`, run `starteSQLSkript` with arguments structured as: `[DW_EintragsNr, l_DBskript, DW_EintragsNr, p_sqlpar, p_sqlpar2]`.
      - Otherwise, run in legacy mode with arguments structured as: `[DW_EintragsNr, l_DBskript, p_sqlpar, DW_EintragsNr]`.
   9. **Framework Status Update**: Mark execution status as successful via `DWMSG_SetzeStatusOK`.
   10. **Cleanup**: Clear traps and output a final completion message.

7. ERROR HANDLING & EXIT CODES
   - Exit status tracking via `ErrNr` (e.g., exit codes 192, 193) for option parsing failures.
   - Sourced framework trap systems trigger on signal interrupts (`INT`) and execution step failures (`ERR`/non-zero exits when `set -e` is active).
   - Python translation: Wrap step-by-step logic in `try-except` blocks. Use `subprocess.CalledProcessError` to capture failure from external utilities and invoke equivalent metadata reporting methods in Python's `except` blocks.

8. OUTPUTS / SIDE EFFECTS
   - Log files: Generated and appended to at paths resolved by `DWMSG_Logdateiname`.
   - Metadata Database: Row additions and status updates are executed using the `DWMSG_*` family of methods.

9. BUSINESS SUMMARY
   - Acts as a unified execution facade for database transformations.
   - Enforces database status logging and auditing configurations on all executing scripts.
   - Handles variable inputs safely, standardizing execution modes across legacy and modern components.
   - Redirects stdout/stderr streams to trace log files for debugging and monitoring.

=== PSEUDOCODE ===

```python
# Step 1: Environment Initialization
# # REVIEW-STRUCT: Sourced files [.dw_init, f_alis_msgerr.ksh, h_alis_sqlplus.ksh] are not supplied.
# In target implementation, import equivalent Python modules for database logging and framework initiation.
import sys
import os
import argparse
import subprocess
import shutil

# Step 2: Option Parsing
parser = argparse.ArgumentParser(description="Ausführung Script r_sqlscript", add_help=False)
parser.add_argument("-f", dest="p_sqlscript", required=False, default="")
parser.add_argument("-i", dest="p_sqlpar", required=False, default="")
parser.add_argument("-k", dest="p_sqlpar2", required=False, default="")
parser.add_argument("-v", dest="p_Verbose", action="store_true", default=False)
parser.add_argument("-j", dest="p_Job", required=False, default="")
parser.add_argument("-m", dest="p_Modus", required=False, default="")
parser.add_argument("-h", action="help", help="Zeigt diese Hilfeseite an")

try:
    args = parser.parse_args()
except Exception as e:
    # Step 3: Input Validation & Error handling for parsing failure
    # Map ErrNr=192/193 behavior
    # # REVIEW-STRUCT: DWMSG_MeldeFehler helper implementation not supplied
    subprocess.run(["DWMSG_MeldeFehler", "0", "E", "192", str(e)], check=False)
    sys.exit(192)

# Standardize script casing (typeset -l)
p_sqlscript = args.p_sqlscript.lower()
p_sqlpar = args.p_sqlpar
p_sqlpar2 = args.p_sqlpar2
p_Verbose = args.p_Verbose
p_Job = args.p_Job
p_Modus = args.p_Modus

# Step 4: SQL Path Resolution
script_dir = os.path.dirname(os.path.realpath(__file__))
os.chdir(script_dir)

l_DBskript = ""
if os.path.dirname(p_sqlscript) in ["", "."]:
    potential_path = os.path.join("../sql", p_sqlscript)
    if os.path.isfile(potential_path):
        l_DBskript = potential_path
    else:
        l_DBskript = p_sqlscript
else:
    l_DBskript = p_sqlscript

# Step 5: Job Identification & Normalization (typeset -u)
JobKennung = p_Job.upper() if p_Job else "DWH_KORR"

print("----------------- Parameter -----------------")
print(f"Jobkennung     : {JobKennung}")
print(f"DB-Skript      : {l_DBskript}")
print("---------------------------------------------")

# Step 6: Logging & Job Entry Allocation
# # REVIEW-STRUCT: External framework utility binaries are simulated or executed as subprocesses
DW_EintragsNr = subprocess.run(["DWMSG_ErmittleNr"], capture_output=True, text=True, check=True).stdout.strip()
os.environ["DW_EintragsNr"] = DW_EintragsNr

LogDatei = subprocess.run(["DWMSG_Logdateiname", JobKennung, DW_EintragsNr], capture_output=True, text=True, check=True).stdout.strip()

# Create initial log entry
# Redirecting append logic
with open(LogDatei, "a") as log_f:
    if p_Modus == "v2":
        # Pass all parsed command line args to simulate "$*"
        all_args = " ".join(sys.argv[1:])
        subprocess.run(["DWMSG_ErzeugeEintrag", DW_EintragsNr, JobKennung, sys.argv[0], LogDatei, all_args], stdout=log_f, stderr=log_f, check=True)
    else:
        program_identifier = f"{sys.argv[0]}_{l_DBskript}"
        subprocess.run(["DWMSG_ErzeugeEintrag", DW_EintragsNr, JobKennung, program_identifier, LogDatei], stdout=log_f, stderr=log_f, check=True)

# Step 7: Trap Registration (via Try/Except blocks)
try:
    print("----------------- Job -----------------------")
    print(f"Job-Nr    : '{DW_EintragsNr}'")
    print(f"Logdatei  : '{LogDatei}'")
    print("---------------------------------------------")

    # Step 8: SQL Execution (starteSQLSkript)
    # Target Platform: BIGQUERY (standard client should be integrated inside SQL runner script)
    with open(LogDatei, "a") as log_f:
        if p_Modus == "v2":
            # starteSQLSkript $DW_EintragsNr $l_DBskript $DW_EintragsNr $p_sqlpar $p_sqlpar2
            # # REVIEW-STRUCT: launcher starteSQLSkript logic must be confirmed in target Python environment
            subprocess.run(["starteSQLSkript", DW_EintragsNr, l_DBskript, DW_EintragsNr, p_sqlpar, p_sqlpar2], stdout=log_f, stderr=log_f, check=True)
        else:
            # starteSQLSkript $DW_EintragsNr $l_DBskript $p_sqlpar $DW_EintragsNr
            subprocess.run(["starteSQLSkript", DW_EintragsNr, l_DBskript, p_sqlpar, DW_EintragsNr], stdout=log_f, stderr=log_f, check=True)

    # Step 9: Framework Status Update (OK)
    with open(LogDatei, "a") as log_f:
        subprocess.run(["DWMSG_SetzeStatusOK", DW_EintragsNr], stdout=log_f, stderr=log_f, check=True)

except Exception as e:
    # Error Trap Handler Logic
    # # REVIEW-STRUCT: DWMSG_Fehlerbehandlung framework utility is not supplied
    with open(LogDatei, "a") as log_f:
        log_f.write(f"!FEHLER/OSFEHLER gemeldet! Details: {str(e)}\n")
        subprocess.run(["DWMSG_Fehlerbehandlung", DW_EintragsNr], stdout=log_f, stderr=log_f, check=False)
    
    if p_Verbose:
        # If verbose, print log contents immediately to stderr
        if os.path.isfile(LogDatei):
            with open(LogDatei, "r") as log_f:
                print(log_f.read(), file=sys.stderr)
                
    sys.exit(1)

# Step 10: Cleanup & normal completion message
print("Die Abarbeitung des Rahmenskriptes ist ohne erkennbare Fehler beendet")
```

### Execution Order
The target orchestration (managed via Cloud Composer / Apache Airflow) must preserve the execution and calling relationships of the legacy sequence:
1. **DW.DWH_PFPL_CL_TARIF_SMART.xml**: Sourced as the primary UC4 orchestration trigger. In the target environment, this maps to the Airflow DAG definition that schedules and triggers the task execution.
2. **r_sqlscript** (wrapped task execution): The Airflow DAG will instantiate a Python operator or Bash operator to execute the migrated wrapper script `r_sqlscript.py` with appropriate parameters.
3. **d_pfpl_classic_tarif_smart.sql** (the business SQL script): Passed as an argument (`-f d_pfpl_classic_tarif_smart.sql`) to the wrapper execution, which is then executed on BigQuery.

---

### Schedule & Variables — Must Be Retained
* **Scheduler-Set Variables**:
  * `DWH_JOB_KENNUNG` (Value: `'PFPL_CL_TARIF_SMART'`): This identifier is set by the scheduler for this job run. 
* **Target Delivery**:
  * In Cloud Composer (Airflow), this variable must be passed either as a DAG param (`params={"DWH_JOB_KENNUNG": "PFPL_CL_TARIF_SMART"}`) or injected as an environment variable (`os.environ["DWH_JOB_KENNUNG"] = "PFPL_CL_TARIF_SMART"`) into the executing task. The migrated `r_sqlscript.py` will read this variable dynamically via `os.environ.get("DWH_JOB_KENNUNG")` to initialize proper logging, metadata tracking, and auditing.

---

### Lineage
* **Lineage Edges**: No lineage edges or direct upstream/downstream file dependencies were found in the legacy metadata scan for this wrapper script.

---

### Cross-File Dependencies
* **Sourced Files / Shared Utilities**:
  * `.dw_init` (Legacy path: `$HOME/aktuell/.dw_init`): A bootstrap environment script that sets general database connections and environment paths.
  * `f_alis_msgerr.ksh` (Legacy path: `${DW_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`): Framework utility for error signaling and failure logging.
  * `h_alis_sqlplus.ksh` (Legacy path: `${DW_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh`): Library containing the `starteSQLSkript` function that executes external SQL files on the target database.
* **Metadata & Logging Utilities**:
  * This script invokes external utilities `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStatusOK`, and `DWMSG_Fehlerbehandlung` to register execution progress and results. These are shared framework-wide and must be replaced in the target environment with unified Python logging modules or service hooks that write to Google Cloud Logging / BigQuery auditing tables.

---

### Target File Plan
* **Target File**: `r_sqlscript.py`
  * **Language**: Python
  * **Source File**: `r_sqlscript` (located at `/home/gurunathan_t/single_job_demo_v2/r_sqlscript`)
  * **Relative Target Path**: `r_sqlscript.py` (mirrors the root directory structure relative to the source repository folder, as per the Folder Integrity Rule).

---

### Environment-Specific Values
The following parameters and environment paths must be resolved dynamically in the target environment:

#### 1. GLOBAL (Environment-Wide Configuration)
* **`DW_DIR_ROOT`**: Sourced dynamically at runtime using `os.environ.get("DW_DIR_ROOT")`. It points to the top-level directory where common utilities and configurations are located.
* **`GCP_PROJECT`**: The GCP Project ID where BigQuery operations are performed. Sourced via `os.environ.get("GCP_PROJECT")` or Airflow's environment configuration.
* **`BQ_DATASET`**: The target BigQuery dataset for logging or metadata updates. Substituted dynamically during runtime execution.

#### 2. JOB-SPECIFIC (Task-Level Parameters)
* **`JobKennung`** (or `p_Job` / `DWH_JOB_KENNUNG`): Set dynamically via the `-j` command-line flag during task invocation (defaulting to `"DWH_KORR"` if not provided, or overridden by the scheduler-set value `"PFPL_CL_TARIF_SMART"`).
* **`DW_EintragsNr`**: The specific execution ID for the current job run. In Cloud Composer, this is sourced from the task instance run ID (`{{ run_id }}`) and passed as an environment variable or command-line argument.
* **`LogDatei`**: The path to the task execution log. In Cloud Composer, this is handled natively by GCS logging; however, for local file appending, this is generated dynamically at runtime using job parameters.

---

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `r_sqlscript` | `r_sqlscript.py` | Converted to a Python orchestration utility to parse options, handle BigQuery script routing, capture execution status, and integrate with Cloud Composer/BigQuery. |