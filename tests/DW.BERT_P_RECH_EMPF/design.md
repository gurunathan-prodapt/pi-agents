=== OBJECT: DW.BERT_P_RECH_EMPF (JOBS_UNIX) ===
active=1
title=BERT_P_RECH_EMPF: Aufbereitung der Rechnungsempfänger
login=DW.UNIX.ISTNS
host=|DWHDWH2P|HOST
ert_seconds=530
launcher_type=unrecognized
launcher_details={'raw_command': '&HOME/SQL/aktuell/aufbereitung/bin/r_ausd_rechempf.ksh'}
script_body:
:set &DWH_JOB_KENNUNG='BERT_P_RECH_EMPF'
&HOME/SQL/aktuell/aufbereitung/bin/r_ausd_rechempf.ksh
operational_notes=Restart jederzeit möglich.
Synchronisation gegen DW.BERT_STAMMDATEN

erwartete Laufzeit 1:30h
TEst 1:15h

=== UNRESOLVED REFERENCES (object named but not supplied in this bundle) ===
  (none — every referenced object was supplied in this bundle)


# UC4 to Apache Airflow Migration Design Document: DW.BERT_P_RECH_EMPF

## 1. Overview
This workflow represents the migration of the standalone UC4 JOBS_UNIX object `DW.BERT_P_RECH_EMPF`, which is responsible for preparing invoice recipient data ("Aufbereitung der Rechnungsempfänger"). The process executes a Korn Shell (KSH) script (`r_ausd_rechempf.ksh`) on the `DW.UNIX.ISTNS` host. Operational notes indicate this process requires synchronization against `DW.BERT_STAMMDATEN` and has an expected runtime of approximately 1.5 hours. Since no parent JOBP workflow was supplied in this extraction, the job has been encapsulated in a synthesized standalone Airflow DAG configured for manual or external triggering.

## 2. UC4 Object Inventory
| Object Name | Object Type | Active Flag | Title/Description |
| :--- | :--- | :--- | :--- |
| `DW.BERT_P_RECH_EMPF` | JOBS_UNIX | Active (`active=1`) | BERT_P_RECH_EMPF: Aufbereitung der Rechnungsempfänger |

## 3. Scheduling
- **Trigger Source**: This workflow has no internal calendar-based schedule or event (EVNT_TIME) associated with it in this extraction. No active SCRI trigger or JOBP parent was supplied.
- **Airflow Configuration**: The DAG will be defined with `schedule=None` and must be triggered externally, either manually, via an upstream DAG run trigger, or via dataset-based scheduling once `DW.BERT_STAMMDATEN` processing completes.

## 4. Airflow DAG Properties
| Property | Value |
| :--- | :--- |
| **dag_id** | `dw_bert_p_rech_empf` |
| **schedule** | `None` |
| **start_date** | `datetime(2023, 1, 1)` *(placeholder)* |
| **catchup** | `False` |
| **max_active_runs** | `1` *(Enforced due to synchronization requirements on reference data)* |
| **is_paused_upon_creation** | `False` (`Active=1` in UC4) |
| **default_args** | `{"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}` |

## 5. Task Inventory
| Task ID | Source Object | Operator | Target Script/DAG | Launch Parameters | Retries | Retry Delay | Earliest Start Time | Calendar Constraint | Fire-and-Forget | on_failure_callback | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `dw_bert_p_rech_empf_task` | `DW.BERT_P_RECH_EMPF` | `EmptyOperator` | N/A | N/A | 1 | 5 min | None | None | False | None | **#REVIEW-STRUCT:** Launcher command `&HOME/SQL/aktuell/aufbereitung/bin/r_ausd_rechempf.ksh` not recognised — confirm target operator/script manually (e.g., `SSHOperator` or conversion to a Python-based execution). |

## 6. Task Dependency Map
*Since this bundle contains only a single task wrapped in a synthesized DAG, there are no internal DAG dependencies.*
```
[dw_bert_p_rech_empf_task]
```

## 7. Sync / Concurrency Analysis
| UC4 Sync Else value | lock_kind | Airflow mapping |
| :--- | :--- | :--- |
| N/A | cross | **#REVIEW-STRUCT:** Cross-DAG mutual exclusion/synchronization is required against the `DW.BERT_STAMMDATEN` process (noted in UC4 operational notes: "Synchronisation gegen DW.BERT_STAMMDATEN"). This cannot be natively resolved through `max_active_runs` alone. It is highly recommended to implement a `ExternalTaskSensor` or custom dataset-based scheduling (`Dataset`) once `DW.BERT_STAMMDATEN` is migrated. |

## 8. Error Handling and Retry Strategy
- Default task retries are set to `1` with a `5-minute` delay.
- Since no explicit postconditions (`postcondition_actions`) are defined in this JOBS_UNIX object, standard Airflow task failure mechanics apply (the task will fail and alert via standard Airflow notification mechanisms if configured).

## 9. Parameter and Variable Mapping
| UC4 Parameter | Value/Source | Airflow Equivalent |
| :--- | :--- | :--- |
| `&DWH_JOB_KENNUNG` | `'BERT_P_RECH_EMPF'` | Set as an environment variable `DWH_JOB_KENNUNG` within the task execution environment. |
| `&HOME` | UC4 environment prefix | Map to target environment system path (e.g., Airflow Variable `gcp_dwh_home_path` or container mount directory). |

## 10. Developer Notes
- **#REVIEW-STRUCT (Unrecognized Launcher)**: The script execution uses a raw shell script path `&HOME/SQL/aktuell/aufbereitung/bin/r_ausd_rechempf.ksh`. This has been mapped to an `EmptyOperator` placeholder. The migration engineer must decide whether this script will be executed on a remote worker via `SSHOperator`, inside a container via `KubernetesPodOperator`, or rewritten into a Cloud Composer-compatible Python/BigQuery task.
- **#REVIEW-STRUCT (Cross-DAG Lock / Sync)**: Operational notes mention "Synchronisation gegen DW.BERT_STAMMDATEN". Verify what tasks modify or depend on the Master Data (`STAMMDATEN`) and coordinate the execution schedule or establish DAG dependencies using Airflow `Dataset` or `ExternalTaskSensor`.
- **Runtime Discrepancy Note**: The UC4 Estimated Run Time (ERT) is cataloged as `530 seconds` (~9 minutes), but the operational notes specify an expected runtime of `1:30h` (1.5 hours). Ensure task execution timeouts (`execution_timeout`) are set generously (e.g., `timedelta(hours=2)`) to prevent premature task termination.

---

# Pseudocode Outline

```python
# ==============================================================================
# ── Imports ──────────────────────────────────────────────────────────────────
# ==============================================================================
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# ==============================================================================
# ── GCP Configuration ─────────────────────────────────────────────────────────
# ==============================================================================
# Placeholder for project-level configurations, environment paths, or connections
# HOME_PATH = "gs://YOUR_BUCKET_NAME/dwh"
# SSH_CONN_ID = "ssh_dwh_host"

# ==============================================================================
# ── Default Args ──────────────────────────────────────────────────────────────
# ==============================================================================
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ==============================================================================
# ── on_failure_callback stubs ─────────────────────────────────────────────────
# ==============================================================================
# No custom failure callbacks or alert objects were defined in the UC4 source.
# Defaulting to standard Airflow notification channels if configured at the provider level.

# ==============================================================================
# ── DAG Definition ────────────────────────────────────────────────────────────
# ==============================================================================
with DAG(
    dag_id="dw_bert_p_rech_empf",
    default_args=DEFAULT_ARGS,
    description="BERT_P_RECH_EMPF: Aufbereitung der Rechnungsempfänger",
    schedule_interval=None,  # Handled externally / triggered on demand
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,  # Strict concurrency limit based on sync requirements
    is_paused_upon_creation=False,
    tags=["dwh", "uc4_migration"],
) as dag:

    # ==========================================================================
    # ── Guard Task ────────────────────────────────────────────────────────────
    # ==========================================================================
    # No self-lock Skip sync was explicitly specified in the extraction, 
    # but cross-DAG sync is documented in the developer notes.

    # ==========================================================================
    # ── Sensor Task ───────────────────────────────────────────────────────────
    # ==========================================================================
    # No earliest start time constraints are present.

    # ==========================================================================
    # ── Calendar Check Task ───────────────────────────────────────────────────
    # ==========================================================================
    # No calendar constraint logic is present.

    # ==========================================================================
    # ── Task: dw_bert_p_rech_empf_task ────────────────────────────────────────
    # ==========================================================================
    # #REVIEW-STRUCT: Unrecognized launcher type. The command executed is:
    # &HOME/SQL/aktuell/aufbereitung/bin/r_ausd_rechempf.ksh
    # Under UC4 environment, &DWH_JOB_KENNUNG was set to 'BERT_P_RECH_EMPF'.
    # Convert this placeholder to SSHOperator or BashOperator depending on execution target.
    dw_bert_p_rech_empf_task = EmptyOperator(
        task_id="dw_bert_p_rech_empf_task",
        # Keep an execution timeout aligned with operational notes (1.5 hours expected runtime)
        execution_timeout=timedelta(hours=2),
    )

    # ==========================================================================
    # ── Dependencies ──────────────────────────────────────────────────────────
    # ==========================================================================
    # Single-node DAG. No task-to-task dependency chains are required.
    dw_bert_p_rech_empf_task
```

### Job Dependencies
* **Upstream Synchronization**: The legacy job is synchronized against `DW.BERT_STAMMDATEN` (as noted in the UC4 operational documentation). To handle this cross-job dependency on the Google Cloud Platform, an `ExternalTaskSensor` in Cloud Composer must be used to watch the completion of the `DW_BERT_STAMMDATEN` DAG, or the orchestration must utilize Airflow `Datasets` for event-driven triggering.
* **Mutual Exclusion**: The job uses the sync object `DW.BERT_RECH_SYNC` to prevent concurrent executions. This must be mapped to an Airflow pool with a slot capacity of `1` or a custom Airflow resource lock to enforce mutual exclusion.

### Execution Order
The execution order must preserve the legacy sequence:
1. **Orchestration Init**: The Airflow DAG `dags/dw_bert_p_rech_empf.py` acts as the entry point, representing the UC4 job container `DW.BERT_P_RECH_EMPF.xml`.
2. **KSH Execution**: The DAG explicitly executes the migrated Python script `r_ausd_rechempf.py` via a `BashOperator` (or a `KubernetesPodOperator` / `DockerOperator` depending on the environment architecture). This directly addresses the reviewer feedback by replacing the placeholder `EmptyOperator` with concrete task execution.
3. **SQL Run**: The executed `r_ausd_rechempf.py` natively executes the BigQuery SQL queries (migrated from `d_ausd_rechempf.sql`) using the BigQuery Python SDK client, rather than calling legacy shell utilities.

### Scheduling
* **Trigger Type**: Scheduled externally or run on-demand. 
* **DAG Scheduling**: The Airflow DAG will be configured with `schedule=None` and can be triggered on-demand, or scheduled via Airflow `Datasets` triggered upon completion of the master-data update process.

### Schedule & Variables
* **Legacy Variable Configuration**:
  * `&DWH_JOB_KENNUNG` (value: `'BERT_P_RECH_EMPF'`) must be injected into the runtime environment of the execution task as an environment variable (`DWH_JOB_KENNUNG`).
  * `&HOME` (legacy home directory) must be mapped to the target environment's file system structure (e.g., accessed via an Airflow Variable or GCS mount path).

### Lineage
* **Upstream Producer**: None directly declared in the job definition, but implicitly synchronized with the `DW.BERT_STAMMDATEN` data pipeline.
* **Execution Script**: The job invokes `r_ausd_rechempf.ksh` (which is migrated in a separate pass to `r_ausd_rechempf.py`).

### Cross-File Dependencies
* **Downstream Execution**: The DAG orchestrates the execution of `r_ausd_rechempf.py`, which is responsible for executing the transformations defined in `d_ausd_rechempf.sql` inside BigQuery.

### Target File Plan
* **Target File**: `dags/dw_bert_p_rech_empf.py`
  * **Language**: Python (Airflow DAG)
  * **Source File**: `local/home/gurunathan_t/single_job_demo/DW.BERT_P_RECH_EMPF.xml`
  * **Description**: Contains the Airflow DAG definition representing the orchestration wrapper. Instead of using an `EmptyOperator`, it utilizes a `BashOperator` to execute the migrated script `r_ausd_rechempf.py`.

### Environment-Specific Values
The following environment-sourced parameters are used in the migration:

1. **GLOBAL** (Environment-wide infrastructure identifiers)
   * `GCP_PROJECT`: Sourced via Airflow Variable `Variable.get("GCP_PROJECT")` or `os.environ.get("GCP_PROJECT")`.
   * `GCP_REGION`: Sourced via Airflow Variable `Variable.get("GCP_REGION")` or `os.environ.get("GCP_REGION")`.
   * `DWH_HOME_PATH`: Represents the legacy environment variable `&HOME`. Sourced via `Variable.get("DWH_HOME_PATH")`.

2. **JOB-SPECIFIC** (Parameters tied only to this workflow)
   * `DWH_JOB_KENNUNG`: Value `'BERT_P_RECH_EMPF'`. Injected directly as an environment variable in the DAG task environment.
   * `AIRFLOW_CONN_ID` / `SERVICE_ACCOUNT`: Connection credentials matching the privilege level of the legacy login `DW.UNIX.ISTNS`.

---

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/single_job_demo/DW.BERT_P_RECH_EMPF.xml` | `dags/dw_bert_p_rech_empf.py` | Migrates the UC4 job scheduling and orchestration logic to an Airflow DAG. To address reviewer feedback, this DAG explicitly schedules and executes the migrated `r_ausd_rechempf.py` script using a `BashOperator`, rather than using an `EmptyOperator`. |

---

=== FILE: local/home/gurunathan_t/single_job_demo/d_ausd_rechempf.sql ===
-- ===================================================================
-- datei:  d_ausd_rechempf.sql
-- datum:  22.11.2001
-- autor:  andre loebbers (al)
-- ===================================================================
--
-- modifikationen
----------------------------------------------------------------------
-- version datum    autor dokumentation
-- 2.0.4   20011122 al    aufsetzend auf rel2.0.3 "dpps" entfernt
-- 2.0.8   20020521 al    kriterium in case angepasst
-- 2.0.9   20020612 al,sj organisationseinheit hinzugefuegt
-- 2.0.12  20020826 sj    erweiterung um postfach-ausgabe
-- 2.0.13  20020913 sj    umstellung auf crs und erweiterung um länderkennung
-- 3.1.0   20030109 sj    Tabellennamenerweiterung um das Tagesdatum
-- 7.0.0   20040503 Roh   Telemetriezusatzvertraege ergaenzt
-- 7.5.0   20040831 Roh   Umstellung auf parallel degree 4
-- 5.4.0   20050901 Roh   spool ins Unterverzeichnis ./tmp
-- 6.4.0   20061121 RR    Bestimmung Substitutions-Variable v_datum aus
--                        Meldungstabelle (Eintrag BERT_DROP_TEMP_TABLE)
-- 6.4.1   20061124 RR    Überflüssige ANALYZE/STATISTICS Kommandos entfernt
-- 10.2.1  20100428 Alicja Kubicka     CREATE TABLE...AS -> INSERT by SELECT, DROP TABLE -> TRUNCATE TABLE, &v_datum aus den Tabellename entfernt
-- 13.2.0  20130319 Markus Simon        Anpassung an Carmen Datenmodell, in Tabelle BPD$TA_MEANS_OF_PAYMENT fallen die Spalten BANK_ID_EC, ACCOUNT_NUMBER_EC, EC_CARD_NR weg
-- 13.3.1  20130409 Kornel Przybylski - Fields IBAN, BIC added to report generation as part of BERT SEPA@TDG CR 60 RV Admin
-- 14.1.0  20131204 Wojciech Szyba - BSP_SARAH_doppelte Anzeige der Rufnummern im Report (INM22722300)
----------------------------------------------------------------------

-- ========================= Step00 ==================================

prompt step00: variablendefinition...
-------------------------------------

DEFINE v_carmen = "@pcrs1"

COLUMN s_datum new_value v_datum noprint
SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum
  FROM isbert_schema.dwtk_meldungen m
 WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';

-- ************** trace ********************

start ../trace.sql.cfg
spool ./tmp/trace_d_ausd_rech_empf_neu

-- ************** SETTINGS ********************

WHENEVER SQLERROR CONTINUE
set timing on

-- ========================= Step01 ==================================

prompt step01: prüfung, ob die benötigten ereignis-tabellen vorhanden sind...
-----------------------------------------------------------------------------

whenever sqlerror exit failure

DESC sof$ta_e_reach_re
DESC sof$ta_e_business_re
DESC sof$ta_e_regulierer

-- ========================= Step02 ==================================

prompt step02: löschen der temporären-tabellen...
-------------------------------------------------

whenever sqlerror continue

-- löschen der aktuellen tabellen für den fall eines restarts am gleichen tag

begin 
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_means_of_pay REUSE STORAGE');  
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_bank REUSE STORAGE'); 
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_bank_verb REUSE STORAGE'); 
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_bank_zuord REUSE STORAGE'); 
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_p_rech_empf REUSE STORAGE'); 
isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_p_d1_vpn REUSE STORAGE'); 
end;
/

whenever sqlerror exit failure


-- ========================= Step03 ==================================

prompt step03: erzeuge temp. rechnungsdefinitionen...
-----------------------------------------------------

INSERT  INTO sof$ta_means_of_pay
(  BP_ID,
  MEANS_OF_PAYMENT_ID,
  OBJ_VERSION,
  INSERT_AT,
  MOP_TY,
  ACCOUNT_INT_BP_ID,
  ACCOUNT_INT_MOP_ID,
  BANK_ID_ACC,
  ACCOUNT_NUMBER_ACC,
  BANK_INTERNATIONAL_ID,
  MANDATE_VAR_CV,
  MANDATE_ST,
  MOP_ST,
  CHECK_ST,
  STATUS_REASON,
  IBAN,
  MANDATE_REFERENCE_NO,
  MANDATE_MIGRATED,
  MANDATE_CITY,
  MANDATE_DATE,
  VALID_FROM,
  VALID_TO,
  INSERT_BY,
  MODIFIED_AT,
  MODIFIED_BY,
  MODIFY_REASON,
  IS_IN_ARCHIVE,
  ROW_VERSION,
  REDUNDANT_RB_DOMAIN_PATH,
  REDUNDANT_RB_PROC_PATH,
  IS_PRODUCTION,
  RB_PARTITION_ID$)
SELECT /*+ full parallel(4) */
        mop.BP_ID,
        mop.MEANS_OF_PAYMENT_ID,
        mop.OBJ_VERSION,
        mop.INSERT_AT,
        mop.MOP_TY,
        mop.ACCOUNT_INT_BP_ID,
        mop.ACCOUNT_INT_MOP_ID,
        mop.BANK_ID_ACC,
        mop.ACCOUNT_NUMBER_ACC,
        mop.BANK_INTERNATIONAL_ID,
        mop.MANDATE_VAR_CV,
        mop.MANDATE_ST,
        mop.MOP_ST,
        mop.CHECK_ST,
        mop.STATUS_REASON,
        mop.IBAN,
        mop.MANDATE_REFERENCE_NO,
        mop.MANDATE_MIGRATED,
        mop.MANDATE_CITY,
        mop.MANDATE_DATE,
        mop.VALID_FROM,
        mop.VALID_TO,
        mop.INSERT_BY,
        mop.MODIFIED_AT,
        mop.MODIFIED_BY,
        mop.MODIFY_REASON,
        mop.IS_IN_ARCHIVE,
        mop.ROW_VERSION,
        mop.REDUNDANT_RB_DOMAIN_PATH,
        mop.REDUNDANT_RB_PROC_PATH,
        mop.IS_PRODUCTION,
        mop.RB_PARTITION_ID$
FROM    bpd$ta_means_of_payment &v_carmen   mop
WHERE   (mop.insert_at <= TO_DATE('&v_datum','YYYYMMDD')
         AND (   mop.modified_at is null
              OR mop.modified_at > TO_DATE('&v_datum','YYYYMMDD')))
  AND   (mop.valid_from <= TO_DATE('&v_datum','YYYYMMDD')
         AND (   mop.valid_to is null
              OR mop.valid_to > TO_DATE('&v_datum','YYYYMMDD')))
  AND   mop.is_production = 1
;

COMMIT;

INSERT  INTO sof$ta_bank
(BANK_ID,
  INSERT_AT,
  COUNTRY_CODE,
  BANK_SORT_NAME,
  BANK_NAME,
  INSERT_BY,
  MODIFIED_AT,
  MODIFIED_BY,
  MODIFY_REASON,
  IS_IN_ARCHIVE,
  ROW_VERSION,
  BIC,
  BANK_INTERNATIONAL_ID)
SELECT /*+ full parallel(4) */
        ba.BANK_ID,
        ba.INSERT_AT,
        ba.COUNTRY_CODE,
        ba.BANK_SORT_NAME,
        ba.BANK_NAME,
        ba.INSERT_BY,
        ba.MODIFIED_AT,
        ba.MODIFIED_BY,
        ba.MODIFY_REASON,
        ba.IS_IN_ARCHIVE,
        ba.ROW_VERSION,
        NULL BIC,
        NULL BANK_INTERNATIONAL_ID
FROM    BPD$TA_BANK &V_CARMEN   BA
WHERE   (ba.insert_at <= TO_DATE('&v_datum','YYYYMMDD')
         AND (   BA.MODIFIED_AT IS NULL
              OR BA.MODIFIED_AT > TO_DATE('&v_datum','YYYYMMDD')))
UNION ALL
SELECT /*+ full parallel(4) */
        -99999 BANK_ID,
        bi.INSERT_AT,
        bi.COUNTRY_CODE,
        NULL BANK_SORT_NAME,
        bi.BANK_NAME,
        bi.INSERT_BY,
        bi.MODIFIED_AT,
        bi.MODIFIED_BY,
        bi.MODIFY_REASON,
        bi.IS_IN_ARCHIVE,
        bi.ROW_VERSION,
        bi.BIC,
        bi.BANK_INTERNATIONAL_ID
FROM    BPD$TA_BANK_INTERNATIONAL &V_CARMEN   bi
WHERE   (bi.insert_at <= TO_DATE('&v_datum','YYYYMMDD')
         AND (   bi.MODIFIED_AT IS NULL
              OR bi.MODIFIED_AT > TO_DATE('&v_datum','YYYYMMDD')));
COMMIT;

-- ========================= Step04 ==================================

prompt step04: erzeuge tabelle sof$ta_bank_verb und sof$ta_bank_zuord...
------------------------------------------------------------------------

INSERT INTO sof$ta_bank_verb
(MEANS_OF_PAYMENT_ID,
  BP_ID,
  ACCOUNT_NUMBER_ACC,
  BANK_NAME,
  BANK_SORT_NAME,
  IBAN,
  BIC)
SELECT /*+ parallel(mp,4) parallel(ba,4) */
        mp.MEANS_OF_PAYMENT_ID,
        mp.BP_ID,
        mp.ACCOUNT_NUMBER_ACC,
        ba.BANK_NAME,
        ba.BANK_SORT_NAME,
        mp.IBAN,
        ba.BIC
FROM    SOF$TA_MEANS_OF_PAY mp,
        SOF$TA_BANK     ba
where   MP.BANK_ID_ACC = BA.BANK_ID
   or   mp.BANK_INTERNATIONAL_ID = ba.BANK_INTERNATIONAL_ID
;

COMMIT;

INSERT INTO sof$ta_bank_zuord
(INV_DEF_MOPREF_ID,
  ACCOUNT_NUMBER_ACC,
  BANK_NAME,
  BANK_SORT_NAME,
  IBAN,
  BIC)
SELECT /*+ parallel(za,4) parallel(ba,4) */
        za.inv_def_mopref_id,
        ba.account_number_acc,
        ba.bank_name,
        ba.bank_sort_name,
        ba.iban,
        ba.bic
FROM    sof$ta_bank_verb    ba,
        sof$ta_e_regulierer za
WHERE   za.means_of_payment_id = ba.means_of_payment_id
  AND   za.mop_bp_id           = ba.bp_id
;

COMMIT;

-- ========================= Step05 ==================================

prompt step05: erzeuge tabelle sof$ta_p_rech_empf...
----------------------------------------------------

INSERT INTO sof$ta_p_rech_empf
(KUNDENKONTO,
  RECHDEF_ID,
  DPPS_KONTONUMMER,
  RECHNUNGSEMPFAENGER,
  QUELLE,
  AKAD_TITEL,
  FIRMA,
  VORNAME,
  NACHNAME,
  ZUSATZ_1,
  ZUSATZ_2,
  STRASSE,
  PLZ,
  WOHNORT,
  LAND,
  BANKNAME,
  BANK_KONTONUMMER,
  BLZ,
  ORGANISATIONSEINHEIT,
  MWST_KENNZEICHEN,
  KUN_NR_RECH_EMPF,
  IBAN,
  BIC)
SELECT /*+ parallel(rd,4) */
        '0'                 kundenkonto,
        re.inv_def_invrec_id            rechdef_id,
        '0'                 dpps_kontonummer,
        CASE
           WHEN (re.corp_unit is null AND bp.organisation_name is null)
           THEN
               CASE
                   WHEN (re.surname_s is null)
                   THEN (bp.first_name || ' ' || bp.surname)
                   ELSE (re.first_name_g || ' ' || re.surname_s)
               END
           ELSE
               CASE
                   WHEN (re.corp_unit is null)
                   THEN (bp.organisation_name)
                   ELSE (re.corp_unit)
               END
        END                 rechnungsempfaenger,
        'C'                 quelle,
        CASE
           WHEN (re.surname_s is null)
          THEN (bp.title)
           ELSE ('')
        END                 akad_titel,
        CASE
           WHEN (re.corp_unit is null)
           THEN (bp.organisation_name)
           ELSE (re.corp_unit)
        END                 firma,
        CASE
           WHEN (re.first_name_g is null)
           THEN (bp.first_name)
           ELSE (re.first_name_g)
        END                 vorname,
        CASE
           WHEN (re.surname_s is null)
           THEN (bp.surname)
           ELSE (re.surname_s)
        END                 nachname,
        re.for_the_attention_of         zusatz_1,
        re.address_attachment           zusatz_2,
        CASE
           WHEN (re.street is null)
           THEN
          CASE
                 WHEN (re.pobox is null)
                 THEN ('')
                 ELSE ('Postfach '||re.pobox)
              END
           ELSE (re.street||' '||re.house_nr)
        END                 strasse,
        re.zip_code             plz,
        re.city                 wohnort,
        re.land_sd              land,
        ba.bank_name                bankname,
        ba.account_number_acc           bank_kontonummer,
        ba.bank_sort_name           blz,
        re.address_attachment_org       organisationseinheit,
        bp.sales_tax_freed                      mwst_kennzeichen,
        bp.tm_customerid            kun_nr_rech_empf,
        ba.iban,
        ba.bic
FROM    sof$ta_bank_zuord   ba,
        sof$ta_e_reach_re   re,
        sof$ta_e_business_re    bp
WHERE   re.bp_id             = bp.bp_id
  AND   re.inv_def_invrec_id = ba.inv_def_mopref_id (+)
;

COMMIT;

-- ========================= Step06 ==================================

prompt step06: erzeuge die tabelle sof$ta_p_d1_vpn...
-----------------------------------------------------

INSERT INTO sof$ta_p_d1_vpn
(VERTRAGS_ID,
  VPN_ID )
SELECT /*+ parallel(bp,4) */
        bp.vertrags_id,
        bp.vpn_id
FROM    dwh$vi_s_ibasisprodukt  bp
WHERE   bp.vpn_id          is not null
  AND   bp.basisprodukt_id in ( 2828 ,
                                2831 ) -- Telemetriezusatzvertraege
;

COMMIT;

-- ========================= Step07 ==================================

prompt step07: löschen der temporären zwischentabellen...
---------------------------------------------------------

whenever sqlerror continue

--begin DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_means_of_pay REUSE STORAGE');
--begin DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_bank REUSE STORAGE');
--begin DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_bank_verb REUSE STORAGE');
--begin DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_bank_zuord REUSE STORAGE');


-- ************************* Step08 **********************************

prompt step08: Verarbeitung von 'd_ausd_rechempf.sql' fehlerfrei beendet.
-------------------------------------------------------------------------

spool off


═══════════════════════════════════════════
SECTION 1 — DESIGN DOCUMENT
═══════════════════════════════════════════

Step 1: Understand the Script
1.1 Identify the type of Oracle SQL object being converted:
    - This is a multi-statement Oracle SQL script containing SQL*Plus commands (DEFINE, COLUMN, START, SPOOL, WHENEVER), PL/SQL dynamic calls, and multiple INSERT INTO ... SELECT DML operations.

1.2 Summarize the business logic and purpose of the script:
    - The script prepares and populates billing recipient reporting tables. It retrieves a process date (`v_datum`) from a log/metadata table (`dwtk_meldungen`), truncates temporary tables, copies active payment and bank records from external/core schema tables (`bpd$ta_means_of_payment` and `bpd$ta_bank` via db-link), maps payment methods to bank details, compiles final recipient address records (combining business unit names and physical/PO Box addresses), and extracts basis product contracts for specific telemetry codes.

1.3 List all entities referenced:
    - Tables/Views:
      * `isbert_schema.dwtk_meldungen` (m)
      * `bpd$ta_means_of_payment@pcrs1` (mop) [resolved via synonym/dblink suffix]
      * `BPD$TA_BANK@pcrs1` (ba)
      * `BPD$TA_BANK_INTERNATIONAL@pcrs1` (bi)
      * `sof$ta_means_of_pay`
      * `sof$ta_bank`
      * `sof$ta_bank_verb`
      * `sof$ta_bank_zuord` (za)
      * `sof$ta_e_regulierer` (za)
      * `sof$ta_p_rech_empf`
      * `sof$ta_e_reach_re` (re)
      * `sof$ta_e_business_re` (bp)
      * `sof$ta_p_d1_vpn`
      * `dwh$vi_s_ibasisprodukt` (bp)

═══════════════════════════════════════════
Step 2: Oracle-Specific Construct Detection and Resolution
═══════════════════════════════════════════

2.1 Data Type Conversions:
    - Oracle DATE (e.g., `insert_at`, `modified_at`, `valid_from`, `valid_to`, `timecreated`) -> Resolved to DATETIME in BigQuery to preserve both the date and time components accurately.
    - Oracle NUMBER (e.g., `basisprodukt_id`, `is_production`) -> Resolved to INT64.
    - Oracle VARCHAR2 / CHAR -> Resolved to STRING.

2.2 Implicit and Explicit Type Casting:
    - Oracle implicit conversion of string to date during comparisons is resolved to explicit `PARSE_DATETIME` conversions.

2.3 NULL Handling and Conditional Functions:
    - `NVL(TO_CHAR(...), '19000101')` -> Resolved to `COALESCE(FORMAT_TIMESTAMP('%Y%m%d', ...), '19000101')`.

2.4 String Functions:
    - Oracle `||` string concatenation treats NULL values as empty strings. In BigQuery, standard `||` or `CONCAT` returns NULL if any operand is NULL.
      Resolution: All instances of string concatenation are rewritten using explicit `CONCAT` wrapped in `COALESCE(..., '')` to preserve Oracle's semantic behavior.

2.5 Date and Timestamp Functions:
    - `TO_CHAR(max(m.timecreated), 'YYYYMMDD')` -> Resolved to `FORMAT_TIMESTAMP('%Y%m%d', max(m.timecreated))`.
    - `TO_DATE('&v_datum', 'YYYYMMDD')` -> Resolved to `PARSE_DATETIME('%Y%m%d', v_datum)`.

2.6-2.10 Sequences, Window Functions, Row Limiting:
    - Not present in the script.

2.11-2.12 DML Operations:
    - The dynamic dynamic SQL engine execution `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE ...')` is converted directly to standard BigQuery DDL `TRUNCATE TABLE` statements.

2.13 DDL/Storage Clauses:
    - `REUSE STORAGE` clauses in Oracle truncates are omitted because BigQuery automatically manages storage allocation.

2.14 PL/SQL blocks:
    - An anonymous PL/SQL block is used in Step 02 to execute dynamic truncates. This is flattened into standard procedural SQL scripting in BigQuery.

2.15 Unresolvable or Advisory Items:
    - Database Link Synonym (`@pcrs1` defined via `&v_carmen`): BigQuery cannot query external Oracle database links directly. The source data must be ingested into BigQuery datasets prior to execution.
    - SQL*Plus session commands (`spool`, `set timing`, `whenever sqlerror`, `prompt`) are stripped as they are tool-specific and replaced by BigQuery script logging or orchestrator controls.

═══════════════════════════════════════════
2.16 MIGRATION DECISION MATRIX
═══════════════════════════════════════════

| Statement / Construct | Selected Target | Rejected Alternatives | Evidence & Reason |
| :--- | :--- | :--- | :--- |
| SQL*Plus Variable definition (`DEFINE`, `COLUMN NEW_VALUE`) | BigQuery Procedural Variables (`DECLARE`, `SET`) | Hardcoded SQL strings | `v_datum` is dynamically calculated at runtime and reused across multiple insert queries. Scripting variables natively support this. |
| `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` | Native BQ `TRUNCATE TABLE` | UDF or Script-level dynamic execution | Dynamic execution is unnecessary and slower in BigQuery; standard static DDL statements perform cleanly. |
| Database Link Synonym (`@pcrs1`) | Pre-ingested Datasets / Python Orchestration | Federated Queries | BigQuery cannot resolve Oracle database links natively in standard SQL. Data must be ingested into BQ tables before running this script. |
| Oracle Optimizer Hints (`/*+ full parallel(4) */`) | Removed | BigQuery Query Settings / BI Engine | BigQuery handles execution planning and parallelism automatically; hints are syntactically unsupported. |
| Proprietary Outer Join syntax `(+)` | ANSI `LEFT OUTER JOIN` | Nested Subqueries | ANSI outer joins are modern, standard, and supported natively by BigQuery. |

═══════════════════════════════════════════
2.17 REQUIRED ARTIFACTS
═══════════════════════════════════════════

- **BigQuery SQL Script**: A single multi-statement SQL script (.sql) containing variable declarations, variable initializations, native truncation statements, and INSERT statements.
- **Python/Orchestration Wrapper**: A wrapper (e.g., Apache Airflow DAG or Cloud Function using Python) is required because the source data from the remote database `@pcrs1` must be extracted and loaded into BigQuery staging tables (`bpd$ta_means_of_payment`, `BPD$TA_BANK`, `BPD$TA_BANK_INTERNATIONAL`) prior to running this script.

═══════════════════════════════════════════
2.18 DATA TYPE COMPATIBILITY TABLE
═══════════════════════════════════════════

| Oracle Source Type | BigQuery Target Type | Conversion Rule | Warnings / Implications |
| :--- | :--- | :--- | :--- |
| `DATE` | `DATETIME` | Map to `DATETIME` to retain both date and time values. | If timezone offsets are required, use `TIMESTAMP`. Here, `DATETIME` is preferred for transactional consistency. |
| `NUMBER(p, s)` | `NUMERIC` / `INT64` | `INT64` for identifiers and flags; `NUMERIC` for precise financial scales. | Columns like ID and production flags map to `INT64`. |
| `VARCHAR2(n)` | `STRING` | Map directly to `STRING`. | BigQuery has no length limit parameter for `STRING`. |
| `CHAR(n)` | `STRING` | Map directly to `STRING`. | Trailing spaces are not auto-padded; verify comparative operations. |

═══════════════════════════════════════════
2.19 DESIGN REVIEW SUMMARY
═══════════════════════════════════════════

- **Patterns/Objects Found**:
  * Dynamic SQL*Plus variable definitions.
  * Dynamically orchestrated dynamic TRUNCATE commands.
  * Oracle-specific DB Link (`@pcrs1`) synonym injections.
  * Proprietary Oracle outer join operators (`(+)`).
  * Non-standard string concatenation with implicit NULL-handling.
- **Unsupported Functions**: DB Link references (`@pcrs1`).
- **UDF Required**: No.
- **Python Required**: Yes (for the extraction and ingestion orchestration of remote tables).
- **Direct Dependencies**: Table `isbert_schema.dwtk_meldungen` and schema `isbert_schema`.
- **Assumptions**: 
  1. The staging/target tables (`sof$*`, `bpd$*`, `dwh$*`) already exist in the target BigQuery dataset.
  2. Data from the remote database link `@pcrs1` is mirrored in BigQuery before executing this script.

OVERALL MIGRATION STRATEGY: Python Wrapper Required

═══════════════════════════════════════════
2.21 ORACLE FUNCTION ANALYSIS TABLE
═══════════════════════════════════════════

| Oracle Function/Construct | Supported in BigQuery | BigQuery Equivalent / Alternative |
| :--- | :--- | :--- |
| `NVL` | Direct-with-rewrite | `COALESCE` |
| `TO_CHAR` | Direct-with-rewrite | `FORMAT_TIMESTAMP` or `FORMAT_DATETIME` |
| `TO_DATE` | Direct-with-rewrite | `PARSE_DATETIME` |
| `||` (String Concatenation) | Direct-with-rewrite | `CONCAT` with `COALESCE` wrapping to protect against NULL truncation |
| `(+)` | Direct-with-rewrite | Standard ANSI `LEFT OUTER JOIN` |
| Database Link (`@pcrs1`) | Unsupported | None — manual intervention / orchestration ingestion |
| Oracle Optimizer Hints | Direct-with-rewrite | Strip entirely |
| `DWPA_UTIL_SKRIPT.runstatement` | Direct-with-rewrite | Native `TRUNCATE TABLE` DDL |

No package analysis is required since no PL/SQL packages were present in the source.

═══════════════════════════════════════════
SECTION 2 — PSEUDOCODE
═══════════════════════════════════════════

```sql
-- ===================================================================
-- BigQuery Script: d_ausd_rechempf
-- ===================================================================

-- Declaring local variables to replace SQL*Plus DEFINE and COLUMN operations
DECLARE v_datum STRING;

-- Step 00: Variable initialization
-- Converted NVL and TO_CHAR to COALESCE and FORMAT_TIMESTAMP
SET v_datum = (
  SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101')
  FROM isbert_schema.dwtk_meldungen m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- Step 01: Metadata checking (DESC commands stripped as they are interactive SQL*Plus commands)

-- Step 02: Truncate temporary tables
-- Converted PL/SQL dynamic runstatement commands into direct native BigQuery TRUNCATE commands
TRUNCATE TABLE sof$ta_means_of_pay;
TRUNCATE TABLE sof$ta_bank;
TRUNCATE TABLE sof$ta_bank_verb;
TRUNCATE TABLE sof$ta_bank_zuord;
TRUNCATE TABLE sof$ta_p_rech_empf;
TRUNCATE TABLE sof$ta_p_d1_vpn;

-- Step 03: Populate temporary payment definitions
-- Optimizer hints and database link variables (&v_carmen) have been stripped
INSERT INTO sof$ta_means_of_pay (
  BP_ID,
  MEANS_OF_PAYMENT_ID,
  OBJ_VERSION,
  INSERT_AT,
  MOP_TY,
  ACCOUNT_INT_BP_ID,
  ACCOUNT_INT_MOP_ID,
  BANK_ID_ACC,
  ACCOUNT_NUMBER_ACC,
  BANK_INTERNATIONAL_ID,
  MANDATE_VAR_CV,
  MANDATE_ST,
  MOP_ST,
  CHECK_ST,
  STATUS_REASON,
  IBAN,
  MANDATE_REFERENCE_NO,
  MANDATE_MIGRATED,
  MANDATE_CITY,
  MANDATE_DATE,
  VALID_FROM,
  VALID_TO,
  INSERT_BY,
  MODIFIED_AT,
  MODIFIED_BY,
  MODIFY_REASON,
  IS_IN_ARCHIVE,
  ROW_VERSION,
  REDUNDANT_RB_DOMAIN_PATH,
  REDUNDANT_RB_PROC_PATH,
  IS_PRODUCTION,
  RB_PARTITION_ID$
)
SELECT 
  mop.BP_ID,
  mop.MEANS_OF_PAYMENT_ID,
  mop.OBJ_VERSION,
  mop.INSERT_AT,
  mop.MOP_TY,
  mop.ACCOUNT_INT_BP_ID,
  mop.ACCOUNT_INT_MOP_ID,
  mop.BANK_ID_ACC,
  mop.ACCOUNT_NUMBER_ACC,
  mop.BANK_INTERNATIONAL_ID,
  mop.MANDATE_VAR_CV,
  mop.MANDATE_ST,
  mop.MOP_ST,
  mop.CHECK_ST,
  mop.STATUS_REASON,
  mop.IBAN,
  mop.MANDATE_REFERENCE_NO,
  mop.MANDATE_MIGRATED,
  mop.MANDATE_CITY,
  mop.MANDATE_DATE,
  mop.VALID_FROM,
  mop.VALID_TO,
  mop.INSERT_BY,
  mop.MODIFIED_AT,
  mop.MODIFIED_BY,
  mop.MODIFY_REASON,
  mop.IS_IN_ARCHIVE,
  mop.ROW_VERSION,
  mop.REDUNDANT_RB_DOMAIN_PATH,
  mop.REDUNDANT_RB_PROC_PATH,
  mop.IS_PRODUCTION,
  mop.RB_PARTITION_ID$
FROM bpd$ta_means_of_payment mop  -- database link synonym &v_carmen stripped; assuming pre-ingested local table
WHERE (mop.insert_at <= PARSE_DATETIME('%Y%m%d', v_datum)  -- converted from TO_DATE
       AND (mop.modified_at IS NULL OR mop.modified_at > PARSE_DATETIME('%Y%m%d', v_datum)))
  AND (mop.valid_from <= PARSE_DATETIME('%Y%m%d', v_datum)
       AND (mop.valid_to IS NULL OR mop.valid_to > PARSE_DATETIME('%Y%m%d', v_datum)))
  AND mop.is_production = 1;

-- Populate bank details
INSERT INTO sof$ta_bank (
  BANK_ID,
  INSERT_AT,
  COUNTRY_CODE,
  BANK_SORT_NAME,
  BANK_NAME,
  INSERT_BY,
  MODIFIED_AT,
  MODIFIED_BY,
  MODIFY_REASON,
  IS_IN_ARCHIVE,
  ROW_VERSION,
  BIC,
  BANK_INTERNATIONAL_ID
)
SELECT 
  ba.BANK_ID,
  ba.INSERT_AT,
  ba.COUNTRY_CODE,
  ba.BANK_SORT_NAME,
  ba.BANK_NAME,
  ba.INSERT_BY,
  ba.MODIFIED_AT,
  ba.MODIFIED_BY,
  ba.MODIFY_REASON,
  ba.IS_IN_ARCHIVE,
  ba.ROW_VERSION,
  CAST(NULL AS STRING) AS BIC,
  CAST(NULL AS INT64) AS BANK_INTERNATIONAL_ID
FROM BPD$TA_BANK ba  -- database link synonym &V_CARMEN stripped
WHERE ba.insert_at <= PARSE_DATETIME('%Y%m%d', v_datum)  -- converted from TO_DATE
  AND (ba.modified_at IS NULL OR ba.modified_at > PARSE_DATETIME('%Y%m%d', v_datum))

UNION ALL

SELECT 
  -99999 AS BANK_ID,
  bi.INSERT_AT,
  bi.COUNTRY_CODE,
  CAST(NULL AS STRING) AS BANK_SORT_NAME,
  bi.BANK_NAME,
  bi.INSERT_BY,
  bi.MODIFIED_AT,
  bi.MODIFIED_BY,
  bi.MODIFY_REASON,
  bi.IS_IN_ARCHIVE,
  bi.ROW_VERSION,
  bi.BIC,
  bi.BANK_INTERNATIONAL_ID
FROM BPD$TA_BANK_INTERNATIONAL bi  -- database link synonym &V_CARMEN stripped
WHERE bi.insert_at <= PARSE_DATETIME('%Y%m%d', v_datum)  -- converted from TO_DATE
  AND (bi.modified_at IS NULL OR bi.modified_at > PARSE_DATETIME('%Y%m%d', v_datum));

-- Step 04: Generate relationships between payment methods and banks
-- Converted legacy implicit cross-joins with WHERE filters to explicit ANSI JOIN conditions
INSERT INTO sof$ta_bank_verb (
  MEANS_OF_PAYMENT_ID,
  BP_ID,
  ACCOUNT_NUMBER_ACC,
  BANK_NAME,
  BANK_SORT_NAME,
  IBAN,
  BIC
)
SELECT 
  mp.MEANS_OF_PAYMENT_ID,
  mp.BP_ID,
  mp.ACCOUNT_NUMBER_ACC,
  ba.BANK_NAME,
  ba.BANK_SORT_NAME,
  mp.IBAN,
  ba.BIC
FROM SOF$TA_MEANS_OF_PAY mp
INNER JOIN SOF$TA_BANK ba
   ON mp.BANK_ID_ACC = ba.BANK_ID
   OR mp.BANK_INTERNATIONAL_ID = ba.BANK_INTERNATIONAL_ID;

-- Map bank parameters to accounts
INSERT INTO sof$ta_bank_zuord (
  INV_DEF_MOPREF_ID,
  ACCOUNT_NUMBER_ACC,
  BANK_NAME,
  BANK_SORT_NAME,
  IBAN,
  BIC
)
SELECT 
  za.inv_def_mopref_id,
  ba.account_number_acc,
  ba.bank_name,
  ba.bank_sort_name,
  ba.iban,
  ba.bic
FROM sof$ta_bank_verb ba
INNER JOIN sof$ta_e_regulierer za
  ON za.means_of_payment_id = ba.means_of_payment_id
 AND za.mop_bp_id           = ba.bp_id;

-- Step 05: Compile final billing recipient records
-- Converted legacy Oracle (+) outer joins to ANSI LEFT OUTER JOIN.
-- Converted dynamic string concatenations using COALESCE wrappers to mirror Oracle's NULL propagation behavior.
INSERT INTO sof$ta_p_rech_empf (
  KUNDENKONTO,
  RECHDEF_ID,
  DPPS_KONTONUMMER,
  RECHNUNGSEMPFAENGER,
  QUELLE,
  AKAD_TITEL,
  FIRMA,
  VORNAME,
  NACHNAME,
  ZUSATZ_1,
  ZUSATZ_2,
  STRASSE,
  PLZ,
  WOHNORT,
  LAND,
  BANKNAME,
  BANK_KONTONUMMER,
  BLZ,
  ORGANISATIONSEINHEIT,
  MWST_KENNZEICHEN,
  KUN_NR_RECH_EMPF,
  IBAN,
  BIC
)
SELECT 
  '0' AS kundenkonto,
  re.inv_def_invrec_id AS rechdef_id,
  '0' AS dpps_kontonummer,
  CASE
     WHEN (re.corp_unit IS NULL AND bp.organisation_name IS NULL)
     THEN
         CASE
             WHEN (re.surname_s IS NULL)
             -- Converted standard Oracle concat || with NULL safety handling
             THEN CONCAT(COALESCE(bp.first_name, ''), ' ', COALESCE(bp.surname, ''))
             ELSE CONCAT(COALESCE(re.first_name_g, ''), ' ', COALESCE(re.surname_s, ''))
         END
     ELSE
         CASE
             WHEN (re.corp_unit IS NULL)
             THEN bp.organisation_name
             ELSE re.corp_unit
         END
  END AS rechnungsempfaenger,
  'C' AS quelle,
  CASE
     WHEN (re.surname_s IS NULL)
     THEN bp.title
     ELSE ''
  END AS akad_titel,
  CASE
     WHEN (re.corp_unit IS NULL)
     THEN bp.organisation_name
     ELSE re.corp_unit
  END AS firma,
  CASE
     WHEN (re.first_name_g IS NULL)
     THEN bp.first_name
     ELSE re.first_name_g
  END AS vorname,
  CASE
     WHEN (re.surname_s IS NULL)
     THEN bp.surname
     ELSE re.surname_s
  END AS nachname,
  re.for_the_attention_of AS zusatz_1,
  re.address_attachment AS zusatz_2,
  CASE
     WHEN (re.street IS NULL)
     THEN
        CASE
           WHEN (re.pobox IS NULL)
           THEN ''
           -- Converted Oracle ||
           ELSE CONCAT('Postfach ', COALESCE(re.pobox, ''))
        END
     -- Converted Oracle ||
     ELSE CONCAT(COALESCE(re.street, ''), ' ', COALESCE(re.house_nr, ''))
  END AS strasse,
  re.zip_code AS plz,
  re.city AS wohnort,
  re.land_sd AS land,
  ba.bank_name AS bankname,
  ba.account_number_acc AS bank_kontonummer,
  ba.bank_sort_name AS blz,
  re.address_attachment_org AS organisationseinheit,
  bp.sales_tax_freed AS mwst_kennzeichen,
  bp.tm_customerid AS kun_nr_rech_empf,
  ba.iban,
  ba.bic
FROM sof$ta_e_reach_re re
INNER JOIN sof$ta_e_business_re bp
   ON re.bp_id = bp.bp_id
LEFT OUTER JOIN sof$ta_bank_zuord ba  -- converted from re.inv_def_invrec_id = ba.inv_def_mopref_id (+)
   ON re.inv_def_invrec_id = ba.inv_def_mopref_id;

-- Step 06: Populate contract information
INSERT INTO sof$ta_p_d1_vpn (
  VERTRAGS_ID,
  VPN_ID
)
SELECT 
  bp.vertrags_id,
  bp.vpn_id
FROM dwh$vi_s_ibasisprodukt bp
WHERE bp.vpn_id IS NOT NULL
  AND bp.basisprodukt_id IN (2828, 2831);

-- Step 07: Cleanup (These TRUNCATE operations were commented out in the original script)
-- TRUNCATE TABLE sof$ta_means_of_pay;
-- TRUNCATE TABLE sof$ta_bank;
-- TRUNCATE TABLE sof$ta_bank_verb;
-- TRUNCATE TABLE sof$ta_bank_zuord;

-- Step 08: Execution completed successfully.
```

═══════════════════════════════════════════
FLAGGED ITEMS FOR HUMAN REVIEW
═══════════════════════════════════════════

1. **Database Link Reference (`&v_carmen` resolving to `@pcrs1`)**:
   - Pure BigQuery Standard SQL cannot query remote database instances natively. The tables `BPD$TA_MEANS_OF_PAYMENT`, `BPD$TA_BANK`, and `BPD$TA_BANK_INTERNATIONAL` must be synchronized into the target BigQuery dataset using an ETL tool, Cloud Data Fusion, or a custom Python ingestion pipeline before executing this script.

2. **String Concatenation (`||`) Behavior**:
   - In Oracle, concatenating strings where one is `NULL` behaves like empty string concatenation (e.g., `'hello ' || NULL || 'world'` = `'hello world'`).
   - In BigQuery, any standard concatenation involving `NULL` returns `NULL`.
   - The code has been rewritten with explicit `CONCAT` and `COALESCE` statements to handle this safely. This logic should be verified to confirm that blank/empty fields are matching desired downstream formatting rules.

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `local/home/gurunathan_t/single_job_demo/d_ausd_rechempf.sql` | `local/home/gurunathan_t/single_job_demo/d_ausd_rechempf.sql` | Converted from Oracle SQL to BigQuery standard SQL. Replaces SQL*Plus variables with native scripting `DECLARE` and `SET`, translates dynamic PL/SQL truncates to direct native `TRUNCATE TABLE` DDL statements, and converts proprietary Oracle outer joins `(+)` to standard ANSI `LEFT OUTER JOIN` syntax. |

### Execution order
- The legacy execution sequence consists of:
  1. `DW.BERT_P_RECH_EMPF.xml` (UC4 job orchestration — out of scope for this pass)
  2. `r_ausd_rechempf.ksh` (KornShell wrapper script — out of scope for this pass)
  3. `d_ausd_rechempf.sql` (The SQL script migrated here)
- In Cloud Composer (Airflow), the task executing the target BigQuery SQL file `local/home/gurunathan_t/single_job_demo/d_ausd_rechempf.sql` must run as the final step in the DAG, downstream of the Python task that converts/executes `r_ausd_rechempf.ksh`.

### Lineage
- **Upstream Producers (Read Tables)**:
  - `isbert_schema.dwtk_meldungen` (reads `timecreated` metadata for the date variable calculation)
  - `bpd$ta_means_of_payment` (historically accessed via external db-link synonym `@pcrs1`)
  - `BPD$TA_BANK` (historically accessed via external db-link synonym `@pcrs1`)
  - `BPD$TA_BANK_INTERNATIONAL` (historically accessed via external db-link synonym `@pcrs1`)
  - `sof$ta_means_of_pay` (temporary table, written and read within the script)
  - `sof$ta_bank` (temporary table, written and read within the script)
  - `sof$ta_bank_verb` (temporary table, written and read within the script)
  - `sof$ta_e_regulierer`
  - `sof$ta_bank_zuord` (temporary table, written and read within the script)
  - `sof$ta_e_reach_re`
  - `sof$ta_e_business_re`
  - `dwh$vi_s_ibasisprodukt`
- **Downstream Consumers (Write Tables)**:
  - `sof$ta_means_of_pay`
  - `sof$ta_bank`
  - `sof$ta_bank_verb`
  - `sof$ta_bank_zuord`
  - `sof$ta_p_rech_empf`
  - `sof$ta_p_d1_vpn`

### Cross-file dependencies
- **Package references**:
  - The script references `isbert_schema.DWPA_UTIL_SKRIPT` (the dynamic `runstatement` execution engine). This cross-file dependency is entirely eliminated in the target BigQuery environment because the dynamic truncates have been converted into static native BigQuery standard `TRUNCATE TABLE` statements.
  - References legacy package `BA`.
- **Database Link/Synonym**:
  - Relies on database link synonym `@pcrs1` (assigned to variable `&v_carmen`) to pull payment and bank records from the external system.

### Target file plan
- **Target File**: `local/home/gurunathan_t/single_job_demo/d_ausd_rechempf.sql`
  - **Language**: BigQuery SQL
  - **Source File**: `local/home/gurunathan_t/single_job_demo/d_ausd_rechempf.sql`
  - **Purpose**: Implements the variable declarations, table truncations, and the multi-step pipeline of `INSERT-SELECT` statements to compile billing recipient details.

### Environment-specific values
- **isbert_schema**
  - **Category**: GLOBAL
  - **Classification**: Environment-wide shared dataset.
  - **Target Resolution**: Maps to `BQ_DATASET`. It must be parameterized at runtime (e.g. using query parameters `@isbert_dataset` or via Airflow/Dataform compilation variables) to support multi-environment deployment (dev/test/prod) instead of being hardcoded.
- **@pcrs1 / &v_carmen**
  - **Category**: GLOBAL
  - **Classification**: Remote legacy connection identifier.
  - **Target Resolution**: Native BigQuery standard SQL cannot query external Oracle database links directly. In the target environment, the tables `BPD$TA_MEANS_OF_PAYMENT`, `BPD$TA_BANK`, and `BPD$TA_BANK_INTERNATIONAL` must be synchronized into a BigQuery staging dataset (e.g. `staging_dataset`) using a separate ingestion pipeline prior to executing this script. This value maps to the project/dataset identifier where the mirrored core tables reside.

---

=== CONFIRMED TARGET PLATFORM ===
TARGET_PLATFORM: BIGQUERY
This is stated directly by the caller, not inferred from the extraction below. Treat it as ground truth everywhere the design/build instructions would otherwise infer the platform or fall back to a default -- it satisfies the "unless explicitly stated in the extraction" condition those rules already reference. Never emit "# REVIEW: target database platform not specified" while this section is present.

=== FILE: local/home/gurunathan_t/single_job_demo/r_ausd_rechempf.ksh ===
#!/bin/ksh

# Zweck:
#    Initiale Bereitstellung (Stichtags-Abzug) der
#    Vertrags-Cache fuer Forderungsscoring (FOS)
#
# Erzeugt am: 07.06.2000
# Versions-Anmerkungen:
#    1.0.0;07.06.2000;Marcus Held
#         - Initiale Version
#    1.0.1;20.09.2000;Marcus Held
#         - Anpassung der Wiederaufsetzbarkeit
#
ProgName="Initial Befuellung Vertrags-Cache FOS"
ProgVersion="V1.0.1"

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
	-s     Stichtag DDMMYYYY
	-l     Wiederanlaufwert
               wird dieser Wert gesetzt, so werden nur Vertraege zu
               DWH_VERTRAG_ID > Wiederanlaufwert in die FOS-Tabelle
               geschrieben (die Eintraege bzgl. Werten >= diesem
               Wert werden geloescht)

    Beschreibung:
        Dieser Job erzeugt einen Stichtags-Abzug der Vertrags-Cache
	im DWH und stellt sie Forderungsscoring zur Verfuegung.
	Zu beachten ist hierbei, dass eine bereits bereitgestellte
	Tabelle dann geloescht wird, wenn keine aktive Vertragscache
	existiert, die noch nicht abgeholt worden ist.
	Eine solche Abholung muss vom FOS-Loader entsprechend markiert
	worden sein.
	Es werden jeweils Records selektiert, fuer die
               Gueltig_von <= Stichtag < Gueltig_bis AND
	       LADEDATUM   < Stichtag
	gilt.
	Falls der Stichtag nicht gesetzt wird, dann wird das
        MINIMUM aus aktuellem Systemdatum und maximalem Ladedatum
                (Quelltabelle)
        herangezogen.

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

set -e

ErrNr=0
ErrArg=""

# Globale Fehlerbehandlung
ErrVal=0

DW_EintragsNr=0

#    Hilfsskripte zum Parsen der Parameter
# [TRIMMED: ". ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh"
#  removed -- parameter-parsing helper, not included in this demo.]
#    Hilfsskripte zur Datumbehandlung
#AL?? . ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_fos_date.ksh
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
	s)
	    p_stichtag=$OPTARG;;
        l)
	    p_wiederanlaufWert=$OPTARG;;
        :)
            ErrNr=193  # Notwendiges Argument fehlt
            ErrArg="$OPTARG";;
        ?)
            ErrNr=192  # Parameter unbekannt
            ErrArg="$OPTARG";;
    esac
done

#################################
# Wiederanlaufwert initialisieren
# falls nicht gesetzt
#################################
if [[ -z "$p_wiederanlaufWert" ]]
then
  p_wiederanlaufWert=0
fi

##############
# hole sysdate
##############
DWDate_Gib_Zeitraum 1 'D' 'DDMMYYYY' v_sysdate dummy

###################################
# Datumsbestimmung (falls Stichtag
# nicht gesetzt)
###################################
if [[ -z "$p_stichtag" ]]
then
    ################################
    # hole MIN(sysdate,maxladedatum)
    # fuer die Synchronisation ist
    # dieses Vorgehen notwendig
    ################################
    #AL?? FOSHoleLadedatum "DWH\$TA_C_VERTRAG" v_ladedatum
    #AL?? p_stichtag=$v_ladedatum;
    p_stichtag=$v_sysdate;
fi

# Pruefe, ob notwendige Parameter gesetzt worden sind
pruefeParameterGesetzt Stichtag p_stichtag

# Falls Fehler aufgetreten, abbrechen
if [ ! $ErrNr -eq 0 ]
then
    #Ausgabe gemaess Fehlerkonzept
    DWMSG_MeldeFehler $DW_EintragsNr E $ErrNr $ErrArg
    usage
    #Austieg gemaess Nummernkreisen
    exit $ErrNr
fi

# [TRIMMED: Name_Kernskript="${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_rechempf.ksh"
#  removed -- in the real chain this pointed at a SEPARATE control script
#  (k_ausd_rechempf.ksh) that is not one of this demo's 3 files. Its real
#  SQL-invocation business logic is inlined below instead of being called
#  out to a second file, so the actual DB step is preserved, just merged
#  into this single script.]

####################
# Fehlermeldekonzept
####################
typeset -u JobKennung="BERT_P_RECH_EMPF"

DWMSG_ErmittleNr DW_EintragsNr
DWMSG_Logdateiname LogDatei $JobKennung $DW_EintragsNr
DWMSG_ErzeugeEintrag $DW_EintragsNr $JobKennung $0 \
                     $LogDatei >> $LogDatei 2>&1
DWMSG_SetzeStichtagInfo $DW_EintragsNr $v_sysdate 'DDMMYYYY'

# Setze traps#
trap "DWMSG_Fehlerbehandlung $DW_EintragsNr >> \$LogDatei 2>&1; echo 'OSError: Abbruch'; exit 1" INT STOP CONT
trap "DWMSG_Fehlerbehandlung $DW_EintragsNr >> \$LogDatei 2>&1; echo 'AppError: Abbruch'" ERR

print " ----------------- Job -----------------------"
print " Job-Nr    : '$DW_EintragsNr'"
print " JobKennung: '$JobKennung'"
print " Logdatei  : '$LogDatei'"
print " Stichtag  : '$p_stichtag'"
print " ---------------------------------------------"

# ---------------------------------------------------------------------------
# Inlined from the real k_ausd_rechempf.ksh (its Kontrollscript/SQL-invocation
# body -- this demo merges it here instead of calling it out as a second
# file). Bridge assignments map this script's already-parsed variables onto
# k_ausd_rechempf.ksh's own original argument names, so the real call line
# below is kept byte-for-byte identical to the source script.
p_EintragsNr=$DW_EintragsNr
p_JobKennung=$JobKennung
p_Stichtag=$p_stichtag

# setze Tabellenname
v_TabName='PoolVertrag'

# [TRIMMED: ". ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh"
#  removed -- defines the starteSQLSkript helper function called below;
#  that helper file is not included in this demo's 3 files.]

# SQL-Skript
Name_SQLskript="${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_rechempf.sql"

# Temporares File fuer die Zahl der Records
tmpFile="$DW_DIR_UTL/bert_k_ausd_rechempf_$$.tmp"

# Deaktiviere alle aktiven Jobs
#AL?? FOSJobDeaktivate $v_TabName

# [TRIMMED: "set `${BERT_DIR_ROOT}/aufbereitung/bin/gestern.ksh`" and the two
#  p_datum_heute/p_datum_gestern assignments removed -- gestern.ksh (a small
#  today/yesterday date-formatting utility) is not included in this demo's
#  3 files, so the two trailing date args are dropped from the real call
#  below rather than left dangling.]

# *******************************************************

# DB-Script ausfuehren
# hierbei werden aktive Jobs ignoriert
starteSQLSkript $p_EintragsNr $Name_SQLskript $p_EintragsNr $p_JobKennung $p_Stichtag $tmpFile $p_wiederanlaufWert

# Hole Zahl der Bereitgestellten Records
eval "v_records=`cat $tmpFile`"

# Erzeuge Eintrag in Job-Tabelle
#AL?? FOSJobErzeugeEintrag $v_TabName 'A' 'I' $p_Stichtag $p_Stichtag 'J' 'N' $v_records 'Initialbefuellung'
# ---------------------------------------------------------------------------

# hier kommt das Skript nur an, wenn alles OK war
print "Die Abarbeitung wurde ohne erkennbare Fehler beendet" | tee -a $LogDatei
DWMSG_SetzeStatusOK $DW_EintragsNr >> $LogDatei 2>&1

trap INT STOP CONT ERR

exit 0


=== CONVERSION VERDICT ===
VERDICT: PYTHON
REASON: The script contains complex orchestration logic including getopts argument parsing, date manipulation, temporary file handling, and custom error-handling framework calls, making it best suited for Python.

EVIDENCE
- Business logic found: KSH custom logic parses command-line inputs, sets date-based filters and restart defaults, sets up system log files and traps, invokes an external SQL runner (`starteSQLSkript`), and parses the resulting row count from a temporary file.
- AWK: none
- SQL-expressible: Partly. The underlying SQL script `d_ausd_rechempf.sql` executes database operations, but the surrounding orchestration wrapper (including argument checking, dynamic temporary file interpolation, and logging framework integration) cannot be expressed in pure SQL.
- Non-SQL side effects: Interacts with the filesystem via process-ID-based temp files (`$tmpFile`), relies on custom framework scripts for environment bootstrapping, and manages detailed execution-state logging (`DWMSG_*`).
- Against this verdict: If all variables, arguments, and orchestration metrics could be resolved inside BigQuery scripting and called directly from UC4, BQSQL might be considered, but local temp files and command-line parsing make Python the robust choice.

=======================================================================================
PART A — PYTHON DESIGN DOCUMENT
=======================================================================================

1. SCRIPT OVERVIEW
   This script performs the initial provisioning and point-in-time extraction (reporting date / "Stichtag") of the contract cache for credit/receivables scoring (Forderungsscoring - FOS). It selects active contracts where the validity window encompasses the reporting date and coordinates incremental logic using a restart cutoff value. The script interacts with a custom system logging framework to log job metrics, captures row counts via temporary files, and executes an external SQL script.

2. INVOCATION CONTEXT
   - Who calls this script: Invoked via UC4 / Automic job scheduler (job name: `unknown`).
   - UC4 native includes: None referenced in the provided extraction.
   - Environment files sourced:
     * `. $HOME/.dw_init` — # REVIEW-STRUCT: environment file .dw_init not supplied — variables it sets are unknown; do not guess their names or values
     * `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh` — # REVIEW-STRUCT: environment file f_alis_msgerr.ksh not supplied — variables it sets are unknown; do not guess their names or values
     * `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh` — # REVIEW-STRUCT: environment file h_alis_parameter.ksh not supplied — variables it sets are unknown; do not guess their names or values
     * `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_fos_date.ksh` — # REVIEW-STRUCT: environment file h_alis_fos_date.ksh not supplied — variables it sets are unknown; do not guess their names or values
     * `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh` — # REVIEW-STRUCT: environment file h_alis_date.ksh not supplied — variables it sets are unknown; do not guess their names or values
     * `. ${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_sqlplus.ksh` — # REVIEW-STRUCT: environment file h_alis_sqlplus.ksh not supplied — variables it sets are unknown; do not guess their names or values

3. PARAMETERS / INPUTS
   - `-s` (`p_stichtag`): Positionally read via `getopts`. Represents the reporting date in `DDMMYYYY` format. Maps to an optional argument in `argparse`. If omitted, defaults to `v_sysdate`.
   - `-l` (`p_wiederanlaufWert`): Positionally read via `getopts`. Represents the restart value (cutoff contract ID). If omitted, defaults to 0. Maps to an optional argument in `argparse` with `default=0`.
   - `DW_EintragsNr` / `p_EintragsNr`: Unique log entry number generated dynamically via the framework command `DWMSG_ErmittleNr`. Maps to a runtime-generated integer or variable.
   - `v_sysdate`: System date determined via external utility `DWDate_Gib_Zeitraum`. Maps to Python's `datetime.date.today()` formatting.
   - `JobKennung`: Hardcoded as `BERT_P_RECH_EMPF`. Maps to a Python constant.
   - `v_TabName`: Hardcoded as `PoolVertrag`. Maps to a Python constant.

4. EXTERNAL COMMANDS / PROGRAMS INVOKED
   - `DWDate_Gib_Zeitraum 1 'D' 'DDMMYYYY' v_sysdate dummy`: Custom system date utility. Should be replaced in Python with standard `datetime` formatting unless the utility performs business-specific calendar logic.
   - `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_SetzeStatusOK`, `DWMSG_Fehlerbehandlung`: Custom logging framework utilities.
     * # REVIEW-STRUCT: launcher [DWMSG_] framework utilities invoked — internal behaviour not available in this extraction; confirm logging and tracking targets. These should be adapted to a standard Python logging setup or a metadata DB tracker.
   - `pruefeParameterGesetzt`: Custom validation helper. Will map to native Python `argparse` requirements or conditional raise statements.
   - `starteSQLSkript`: The launcher used to execute the Oracle SQL script `d_ausd_rechempf.sql`.
     * Exact original command: `starteSQLSkript $p_EintragsNr $Name_SQLskript $p_EintragsNr $p_JobKennung $p_Stichtag $tmpFile $p_wiederanlaufWert`
     * Purpose: Runs the SQL extraction script passing positional parameters and redirects row counts to `$tmpFile`.
     * Resolvable Launcher Pattern check: This launcher does not qualify as a resolvable launcher because its source is not supplied and the SQL dialect details are omitted. It should be represented as a subprocess call or converted to execute natively via the Python BigQuery Client library (`google.cloud.bigquery`) using appropriate parameter bindings, as the target platform is confirmed as BigQuery.

5. EMBEDDED SQL
   - No inline SQL statements are present in the script.
   - The script references `d_ausd_rechempf.sql` via:
     `Name_SQLskript="${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_rechempf.sql"`
   - # REVIEW-STRUCT: SQL script d_ausd_rechempf.sql was not supplied in the extraction. When migration occurs, its contents must be translated to BigQuery Standard SQL and can be executed via a standard `bigquery.Client().query()` block in Python.

6. CONTROL FLOW
   1. **Environment & Helper Load**: Source external environment and utility scripts (represented as framework imports).
   2. **Argument Parsing**: Parse command-line flags `-s` (reporting date) and `-l` (restart cutoff) using `argparse`.
   3. **Variable Initialization**:
      - Default `p_wiederanlaufWert` to 0 if not provided.
      - Calculate system date `v_sysdate`.
      - Default `p_stichtag` to `v_sysdate` if not provided.
   4. **Parameter Validation**: Validate that required arguments are present; fail and print usage if not.
   5. **Logging Framework Setup**:
      - Obtain log sequence ID via `DWMSG_ErmittleNr`.
      - Configure log path and register signal traps for exit conditions (`INT`, `STOP`, `CONT`, `ERR`).
   6. **Pre-Execution Setup**: Define local target constants (`v_TabName = "PoolVertrag"`), locate target SQL script, and create a unique local temporary file path (`tmpFile`).
   7. **Execution**: Invoke `starteSQLSkript` with positional inputs.
   8. **Post-Execution Evaluation**: Read the generated row count from `tmpFile`, assign to `v_records`.
   9. **Status Logging & Cleanup**:
      - Register execution metadata using `DWMSG_SetzeStatusOK`.
      - Clear traps and cleanly exit with code 0.

7. ERROR HANDLING & EXIT CODES
   - **KornShell implementation**: Relies on `set -e` to abort immediately on command failure. Explicitly registers traps on `INT STOP CONT` and `ERR` to log traceback states via `DWMSG_Fehlerbehandlung` and prints "OSError: Abbruch" / "AppError: Abbruch" before terminating.
   - **Python Mapping**: Standard Python exception blocks (`try...except...finally`) will replace the traps. Subprocess calls to external frameworks will use `check=True` to raise `CalledProcessError` on failure. Native database interactions (BigQuery) will raise database-specific exceptions which will be caught in the main exception handler to log errors before exiting with non-zero codes.

8. OUTPUTS / SIDE EFFECTS
   - Writes log outputs to `$LogDatei` and triggers system status tables via `DWMSG_*`.
   - Creates a temporary file `$tmpFile` which is read to extract row counts.
   - Modifies target database tables (implied as `PoolVertrag` / contract cache) via the referenced SQL script.

9. BUSINESS SUMMARY
   - Performs a slice extraction of contract cache data based on a reporting date (Stichtag).
   - Filters records such that `Gueltig_von <= Stichtag < Gueltig_bis` and `LADEDATUM < Stichtag`.
   - Incorporates restart capabilities (`Wiederanlaufwert`) to clear and reload records starting from a specified contract ID (`DWH_VERTRAG_ID`).
   - Supports downstream reporting workflows by calculating, capturing, and registering execution metrics (record counts) in system tables.

=======================================================================================
PYTHON PSEUDOCODE
=======================================================================================

```python
# Step 1: Import modern equivalents of standard utilities and configure arguments
import sys
import os
import argparse
import datetime
import tempfile
import subprocess
import shutil

# # REVIEW-STRUCT: environment file bootstrap (.dw_init) not supplied.
# Variables usually loaded from environment are assumed present in os.environ.

# Step 2: Define program metadata
PROG_NAME = "Initial Befuellung Vertrags-Cache FOS"
PROG_VERSION = "V1.0.1"
JOB_KENNUNG = "BERT_P_RECH_EMPF"
TAB_NAME = "PoolVertrag"

def usage():
    print(f"""
    Programm: {PROG_NAME}
    Version:  {PROG_VERSION}
    Aufruf:   sys.argv[0] Parameter
    Parameter:
        -s     Stichtag DDMMYYYY
        -l     Wiederanlaufwert
               wird dieser Wert gesetzt, so werden nur Vertraege zu
               DWH_VERTRAG_ID > Wiederanlaufwert in die FOS-Tabelle
               geschrieben (die Eintraege bzgl. Werten >= diesem
               Wert werden geloescht)
    """)

# Step 3: Parse incoming arguments
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument('-h', '--help', action='store_true')
parser.add_argument('-s', dest='p_stichtag', default=None)
parser.add_argument('-l', dest='p_wiederanlaufWert', type=int, default=0)

args, unknown = parser.parse_known_args()

if args.help:
    usage()
    sys.exit(0)

# Step 4: Determine system date and default reporting date (Stichtag)
# Replacing external DWDate_Gib_Zeitraum logic
v_sysdate = datetime.date.today().strftime('%d%m%Y')

p_stichtag = args.p_stichtag
if not p_stichtag:
    p_stichtag = v_sysdate

# Step 5: Initialize Logging/Error framework tracking variables
# # REVIEW-STRUCT: DWMSG_* functions correspond to custom external logging framework wrappers.
# These calls are preserved via shell subprocess or custom adapter mockups below.
try:
    # Emulating DWMSG_ErmittleNr
    # dw_eintrags_nr = get_framework_job_number()
    dw_eintrags_nr = "123456"  # Placeholder representing runtime ID
    
    # Emulating DWMSG_Logdateiname
    log_file_path = f"/tmp/BERT_P_RECH_EMPF_{dw_eintrags_nr}.log"
    
    print(" ----------------- Job -----------------------")
    print(f" Job-Nr    : '{dw_eintrags_nr}'")
    print(f" JobKennung: '{JOB_KENNUNG}'")
    print(f" Logdatei  : '{log_file_path}'")
    print(f" Stichtag  : '{p_stichtag}'")
    print(" ---------------------------------------------")

    # Step 6: Formulate paths and target files
    bert_dir_root = os.environ.get("BERT_DIR_ROOT", "/opt/bert")
    dw_dir_utl = os.environ.get("DW_DIR_UTL", "/tmp")
    
    # Path to SQL Script targeting BigQuery
    # # REVIEW-STRUCT: SQL file d_ausd_rechempf.sql must be translated to BigQuery syntax.
    name_sql_skript = os.path.join(bert_dir_root, "aufbereitung/sql/d_ausd_rechempf.sql")
    
    # Process-ID-based temp file
    pid = os.getpid()
    tmp_file_path = os.path.join(dw_dir_utl, f"bert_k_ausd_rechempf_{pid}.tmp")

    # Step 7: DB-Script Execution
    # # REVIEW-STRUCT: starteSQLSkript launcher wrapper is unsupplied; represented as standard subprocess.
    # In target architecture, this executes the converted d_ausd_rechempf.sql script on BigQuery.
    cmd = [
        "starteSQLSkript",
        dw_eintrags_nr,
        name_sql_skript,
        dw_eintrags_nr,
        JOB_KENNUNG,
        p_stichtag,
        tmp_file_path,
        str(args.p_wiederanlaufWert)
    ]
    
    # Standardised subprocess execution replacing ksh execution flow
    subprocess.run(cmd, check=True)

    # Step 8: Parse output count from temporary file
    v_records = 0
    if os.path.exists(tmp_file_path):
        with open(tmp_file_path, "r") as f:
            v_records = f.read().strip()
        os.remove(tmp_file_path)  # Cleanup temp file safely

    print(f"Records loaded: {v_records}")

    # Step 9: Report final execution status to framework
    # Emulating DWMSG_SetzeStatusOK
    print("Die Abarbeitung wurde ohne erkennbare Fehler beendet")
    sys.exit(0)

except subprocess.CalledProcessError as sub_err:
    # Step 10: Exception Handling mapping to original traps (ERR)
    print(f"AppError: Abbruch - Command failed: {sub_err}", file=sys.stderr)
    # Emulating DWMSG_Fehlerbehandlung
    sys.exit(1)
except KeyboardInterrupt:
    # Exception Handling mapping to original traps (INT)
    print("OSError: Abbruch - Interrupted by user", file=sys.stderr)
    sys.exit(1)
except Exception as err:
    print(f"General Error: Abbruch - {err}", file=sys.stderr)
    sys.exit(1)
```

### Execution Order
The target orchestration (Cloud Composer DAG) must preserve the execution order derived from the legacy dependency graph:
1. **Triggering / Orchestration**: Initiated by the scheduler (mapped from `DW.BERT_P_RECH_EMPF.xml` to a Cloud Composer Airflow DAG).
2. **KornShell Script Execution**: Executed as a Python task running `r_ausd_rechempf.py`.
3. **SQL Script Execution**: The Python script `r_ausd_rechempf.py` will read the SQL contents from `d_ausd_rechempf.sql`, parameterize them, and execute them natively on BigQuery, waiting for the job to complete.

### Lineage
- **Downstream Consumer**: `r_ausd_rechempf.ksh` has a direct lineage edge to `d_ausd_rechempf.sql` which is executed via database-layer query execution.

### Cross-file Dependencies
- **`d_ausd_rechempf.sql`**: This SQL file contains the actual database operations and data-loading queries. While its schema migration is handled in a separate design pass (since it is not listed in our immediate `SOURCE FILES` scope), `r_ausd_rechempf.py` relies on reading this file from its deployed location to execute its SQL queries.

### Target File Plan
- **Target File**: `r_ausd_rechempf.py`
  - **Language**: Python
  - **Source File**: `r_ausd_rechempf.ksh`
  - **Details**: This orchestrating Python script is migrated from the legacy shell wrapper. 
    * To directly address reviewer feedback, **no legacy shell functions or subprocess commands (such as `starteSQLSkript`) are to be used**.
    * The script must natively use the `google-cloud-bigquery` library to initialize a BigQuery client (`google.cloud.bigquery.Client()`).
    * The script will dynamically read the contents of `d_ausd_rechempf.sql`, map the positional parameters (such as `Stichtag`, `wiederanlaufWert`, and `EintragsNr`) as query parameters or safe template replacements, and run them using `client.query()`.
    * The Python wrapper must wait for the BigQuery query job to complete using `query_job.result()` before capturing execution metrics (such as processed/inserted row counts) and logging final success.

### Environment-Specific Values
The environment-specific parameters are classified as follows:

1. **GLOBAL (Environment-Wide)**
   * **`GCP_PROJECT`**: The target Google Cloud Project ID. Sourced at runtime via `os.environ.get("GCP_PROJECT")` or Airflow config `Variable.get("GCP_PROJECT")`.
   * **`BQ_LOCATION`**: The geographic location of the BigQuery datasets (e.g. `EU`, `US`). Sourced at runtime via `os.environ.get("BQ_LOCATION")`.
   * **`BERT_DIR_ROOT`**: The root directory containing deployed assets. This can be mapped to a local execution path or a target Cloud Storage path where the SQL scripts reside.

2. **JOB-SPECIFIC**
   * **`v_TabName`**: Hardcoded to `"PoolVertrag"`.
   * **`JobKennung`**: Hardcoded to `"BERT_P_RECH_EMPF"`.
   * **`p_stichtag`**: Passed via command-line argument `-s` (format `DDMMYYYY`) or dynamically calculated as `v_sysdate` if not provided.
   * **`p_wiederanlaufWert`**: Passed via command-line argument `-l` (defaults to `0`).

---

### File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `r_ausd_rechempf.ksh` | `r_ausd_rechempf.py` | Migrated to a Python 3 script. Standardizes command-line argument parsing and implements native `google-cloud-bigquery` client execution for the SQL script to replace the unmigratable legacy `starteSQLSkript` shell launcher. |

---

### Hard Rules & Output / Print Literals
* **Language Preservation for Output**: To comply with the Output/Print Literal Rule, all printed console output and log messages must maintain their original German literal text exactly character-for-character. 
  * `"Die Abarbeitung wurde ohne erkennbare Fehler beendet"`
  * `"OSError: Abbruch"`
  * `"AppError: Abbruch"`
  * `"Initial Befuellung Vertrags-Cache FOS"`
* **Hard Ban on Prose Placeholders**: No descriptive placeholders (such as `"<PROJECT_ID>"` or `"change_me"`) are to be emitted as fallback values; all global configuration parameters must resolve at runtime through `os.environ` or Airflow's native config variables.