# MIGRATION DESIGN DOCUMENT: `ausd_bp_ta_bpr_opt_text`

This document outlines the migration design for the assembled job `ausd_bp_ta_bpr_opt_text` from the legacy environment (comprising UC4 scheduler, KornShell scripts, and Oracle SQL) to Google Cloud BigQuery and Apache Airflow.

---

## 1. VERBATIM MCP TOOL OUTPUT

Below is the complete, unaltered output of the migration design tool, containing the target Airflow DAGs, BigQuery transformation logic, and pseudo-code structures generated for each of the source component files.

=== Result for vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_BPR_OPT_TEXT.xml ===
```python
from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# ------------------------------------------------------------------------------
# DAG default arguments
# ------------------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ------------------------------------------------------------------------------
# Single SQL builder function
# Encapsulates the full BigQuery logic in one place.
# Replace the SQL body below with the complete transformation logic required
# for DW.BERT_AUSD_BP_TA_BPR_OPT_TEXT.
# ------------------------------------------------------------------------------
def build_ausd_bp_ta_bpr_opt_text_sql():
    sql = """
    -- =========================================================================
    -- Purpose: Prepare instantiated base products text data for BERT processing
    -- Target: Replace with the full BigQuery transformation logic
    -- =========================================================================

    CREATE TABLE IF NOT EXISTS `your_project.your_dataset.DW_BERT_AUSD_BP_TA_BPR_OPT_TEXT` AS
    SELECT
        *
    FROM
        `your_project.your_dataset.source_table`
    WHERE
        1 = 0;

    INSERT INTO `your_project.your_dataset.DW_BERT_AUSD_BP_TA_BPR_OPT_TEXT`
    SELECT
        *
    FROM
        `your_project.your_dataset.source_table`;
    """
    return sql

# ------------------------------------------------------------------------------
# DAG definition
# ------------------------------------------------------------------------------
with DAG(
    dag_id="DW_BERT_AUSD_BP_TA_BPR_OPT_TEXT",
    default_args=default_args,
    description="BERT_P_BASISPRODUKT: Aufbereitung der instantiierten Basisprodukte",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["bigquery", "bert", "basisprodukt"],
) as dag:

    # --------------------------------------------------------------------------
    # Single BigQuery task:
    # Executes the complete SQL logic in one operator.
    # create_disposition='CREATE_IF_NEEDED' ensures the target table is created
    # if it does not already exist.
    # --------------------------------------------------------------------------
    process_ausd_bp_ta_bpr_opt_text = BigQueryExecuteQueryOperator(
        task_id="process_ausd_bp_ta_bpr_opt_text",
        sql=build_ausd_bp_ta_bpr_opt_text_sql(),
        use_legacy_sql=False,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        location="EU",
    )

    # --------------------------------------------------------------------------
    # Task dependencies
    # --------------------------------------------------------------------------
    process_ausd_bp_ta_bpr_opt_text

```

=== Result for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_opt_text.ksh ===
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    dag_id="bereitstellung_basisprodukte_bert",
    default_args=default_args,
    description="Initial provisioning of selected base products for BERT using BigQuery",
    schedule_interval=None,
    catchup=False,
    tags=["bigquery", "bert", "basisprodukte"],
)

def build_bq_sql(stichtag: str = None, wiederanlaufwert: int = 0) -> str:
    """
    Build the full BigQuery SQL statement for the provisioning logic.
    The SQL is kept in a single statement and can create the target table if needed.
    """
    # NOTE:
    # Replace the source/target table names below with your actual BigQuery tables.
    # The SQL below preserves the original business logic:
    # - Use provided stichtag if present, otherwise derive current system date
    # - Filter records by validity and load date
    # - Apply restart value to only process records with DWH_VERTRAG_ID > wiederanlaufwert
    # - Create target table if it does not exist

    stichtag_expr = f"DATE(PARSE_DATE('%d%m%Y', '{stichtag}'))" if stichtag else "CURRENT_DATE()"
    wiederanlaufwert_expr = int(wiederanlaufwert) if wiederanlaufwert is not None else 0

    sql = f"""
    CREATE TABLE IF NOT EXISTS `project.dataset.target_table` AS
    SELECT
      *
    FROM (
      SELECT
        src.*,
        {stichtag_expr} AS stichtag
      FROM `project.dataset.source_table` AS src
      WHERE
        src.GUELTIG_VON <= {stichtag_expr}
        AND {stichtag_expr} < src.GUELTIG_BIS
        AND src.LADEDATUM < {stichtag_expr}
        AND src.DWH_VERTRAG_ID > {wiederanlaufwert_expr}
    );
    """
    return sql

def create_bigquery_task(**context):
    """
    Create a single BigQuery operator that executes the complete SQL logic.
    """
    stichtag = context["dag_run"].conf.get("stichtag") if context.get("dag_run") and context["dag_run"].conf else None
    wiederanlaufwert = context["dag_run"].conf.get("wiederanlaufwert", 0) if context.get("dag_run") and context["dag_run"].conf else 0

    query = build_bq_sql(stichtag=stichtag, wiederanlaufwert=wiederanlaufwert)

    return BigQueryExecuteQueryOperator(
        task_id="bereitstellung_basisprodukte_bert_bq",
        sql=query,
        use_legacy_sql=False,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        bigquery_conn_id="google_cloud_default",
        dag=dag,
    )

# Single task encapsulating the full BigQuery processing
process_task = create_bigquery_task()

# Optional wrapper to ensure task is part of DAG context
start = PythonOperator(
    task_id="start",
    python_callable=lambda: None,
    dag=dag,
)

end = PythonOperator(
    task_id="end",
    python_callable=lambda: None,
    dag=dag,
)

# Task dependencies
start >> process_task >> end

=== Result for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_opt_text.ksh ===
```python
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def build_bigquery_sql(stichtag: str, job_kennung: str, eintrags_nr: str, wiederanlauf_wert: int = 0) -> str:
    # SQL logic encapsulated in a single function to keep the DAG modular and maintainable.
    # The target table is created automatically if it does not exist via CREATE TABLE IF NOT EXISTS.
    sql = f"""
    CREATE TABLE IF NOT EXISTS `your_project.your_dataset.PoolBasisprodukt` (
      job_kennung STRING,
      eintrags_nr STRING,
      stichtag DATE,
      wiederanlauf_wert INT64,
      created_at TIMESTAMP,
      source_record_count INT64,
      status STRING,
      note STRING
    );

    INSERT INTO `your_project.your_dataset.PoolBasisprodukt` (
      job_kennung,
      eintrags_nr,
      stichtag,
      wiederanlauf_wert,
      created_at,
      source_record_count,
      status,
      note
    )
    SELECT
      '{job_kennung}' AS job_kennung,
      '{eintrags_nr}' AS eintrags_nr,
      PARSE_DATE('%d%m%Y', '{stichtag}') AS stichtag,
      {int(wiederanlauf_wert)} AS wiederanlauf_wert,
      CURRENT_TIMESTAMP() AS created_at,
      COUNT(1) AS source_record_count,
      'A' AS status,
      'Initialbefuellung' AS note
    FROM `your_project.your_dataset.source_table`
    WHERE DATE(event_date) = PARSE_DATE('%d%m%Y', '{stichtag}');
    """
    return sql


def create_bigquery_task(**context):
    # Build the complete SQL statement in one place and execute it with a single BigQuery operator.
    dag_run_conf = context.get("dag_run").conf if context.get("dag_run") else {}
    stichtag = dag_run_conf.get("stichtag", "01012024")
    job_kennung = dag_run_conf.get("job_kennung", "DEFAULT_JOB")
    eintrags_nr = dag_run_conf.get("eintrags_nr", "0")
    wiederanlauf_wert = dag_run_conf.get("wiederanlauf_wert", 0)

    sql = build_bigquery_sql(
        stichtag=stichtag,
        job_kennung=job_kennung,
        eintrags_nr=eintrags_nr,
        wiederanlauf_wert=wiederanlauf_wert,
    )

    return BigQueryExecuteQueryOperator(
        task_id="process_poolbasisprodukt",
        sql=sql,
        use_legacy_sql=False,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        location="EU",
        gcp_conn_id="google_cloud_default",
    )


with DAG(
    dag_id="d_ausd_bp_ta_bpr_opt_text_bigquery",
    default_args=default_args,
    description="BigQuery processing DAG for PoolBasisprodukt",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["bigquery", "etl", "poolbasisprodukt"],
) as dag:

    # Single Python task that prepares the SQL and returns the BigQuery operator.
    process_task = PythonOperator(
        task_id="prepare_and_execute_bigquery",
        python_callable=create_bigquery_task,
        provide_context=True,
    )

    # Task dependency definition.
    process_task
```

=== Result for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_bpr_opt_text.sql ===
```python
from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# SQL logic encapsulated in a single function
def build_bq_sql():
    # Step 1: Clear target table data for restart safety
    # Step 2: Insert joined tariff option text data into the target table
    # Step 3: Use CREATE_IF_NEEDED so the target table is created if missing
    sql = """
    -- ===================================================================
    -- datei:  d_ausd_basisprodukt.sql
    -- datum:  22.11.2001
    -- autor:  andre loebbers (al)
    -- ===================================================================
    --
    -- modifikationen
    ----------------------------------------------------------------------
    -- version datum    autor dokumentation
    -- 2.0.4   20011122 al    aufsetzend auf rel2.0.3
    -- 2.0.5   20020222 al    drop table nach oben gesetzt, damit kein
    --                        abbruch proviziert wird.
    -- 2.0.7   20020502 al    twinmsisdn hinzugefuegt
    -- 3.0.0   20021031 sj    umstellung auf den crs
    -- 3.1.0   20030109 sj    tabellennamenerweiterung um das tagesdatum
    --                        und Bercksichtigung der terminierten
    --                        MSISDN's bzw. ICC_ID's
    -- 6.5.0   20031010 sj    Umstellung auf 6.5 und Aufnahme weiter BP's
    -- 7.0.0   20040408 Roh   Tabelle PDS$TA_BPR_INSTANCE wird zu 7.0
    --                        in  PDS$TA_BPRI_COM (bpr_ty <> 1)
    --                        und PDS$TA_BPRI_NET (bpr_ty =  1) geteilt
    -- 7.0.3   20040629 Roh   neue Basisprodukt-IDs
    -- 7.0.5   20040720 Roh   MSISDN des BCP-Vertrages (nur voice)
    -- 7.5.0   20040831 Roh   Umstellung auf parallel degree 4
    --         20040917 Roh   Ausweisung Bevorrechtigung gem. TKSiV (2917)
    -- ab 2005 neue Releasenummern
    -- 5.1.0   20050110 Roh   4 neue Basisprodukte (3450,3528,3529,3530)
    -- 5.1.1   20050209 Roh   4 neue Basisprodukte (3519,3520,3521,3522)
    --         20050615 Roh   9 neue Basisprodukte
    -- 6.1.0   20060131 Roh   weitere BP
    -- 6.2.0   20060306 Roh   weitere BP zu 6.2
    -- 6.2.0   20060505 YP    einbau nologging
    -- 6.4.0   20061121 RR    Bestimmung Substitutions-Variable v_datum aus
    --                        Meldungstabelle (Eintrag BERT_DROP_TEMP_TABLE)
    -- 6.4.1   20061124 RR    berflssige ANALYZE/STATISTICS Kommandos entfernt
    -- 7.1.0   20070309 ME    - parallel hint korrigiert (bp, bs statt 2x bp),
    --                        - parallel (degree 4) hinzugefgt,
    --                        - order by entfernt, stattdessen Sortierung
    --                          im nchsten Script.
    -- 10.2.1  20100428  Alicja Kubicka     CREATE TABLE...AS -> INSERT by SELECT, DROP TABLE -> TRUNCATE TABLE, &v_datum aus den Tabellename entfernt
    ----------------------------------------------------------------------

    -- ========================= Step00 ==================================

    -- ========================= Step01 ==================================
    TRUNCATE TABLE `sof_ta_bpr_opt_text`;

    -- ========================= Step09 ==================================
    INSERT INTO `sof_ta_bpr_opt_text`
    (CNTRCT_ID, BPR_ID, PDS_DESCRIPTION)
    SELECT
      bp.cntrct_id,
      bp.bpr_id,
      bs.pds_description
    FROM `sof_ta_bpr_optionen` AS bp
    JOIN `sof_ta_bpr_beschr` AS bs
      ON bp.bpr_id = bs.bpr_id;

    -- ========================= Step16 ==================================
    """
    return sql

with DAG(
    dag_id="d_ausd_basisprodukt_bigquery",
    default_args=default_args,
    description="BigQuery DAG for d_ausd_basisprodukt.sql",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["bigquery", "basisprodukt"],
) as dag:

    # Single BigQuery operator executing the full SQL logic
    process_basisprodukt = BigQueryExecuteQueryOperator(
        task_id="process_basisprodukt",
        sql=build_bq_sql(),
        use_legacy_sql=False,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
    )

    # Task dependency placeholder for modular extensibility
    process_basisprodukt

```

---

## 2. INTEGRATED TARGET ARCHITECTURE & ADDITIONAL CONTEXT

The section below supplements the verbatim output with contextual details and dependencies that the automated tools cannot fully capture.

### 2.1 Cross-File Dependencies & Call Chain
The legacy execution structure follows a nesting wrapper-logic:
1. **UC4 Job (`DW.BERT_AUSD_BP_TA_BPR_OPT_TEXT`)** initiates the process by logging into the Unix environment and calling the outer-shell wrapper script:
   `$HOME/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_opt_text.ksh`
2. **Outer Shell Wrapper (`r_ausd_bp_ta_bpr_opt_text.ksh`)**:
   - Initializes environment variables (`. $HOME/.dw_init`).
   - Standardizes parameters (`p_stichtag`, `p_wiederanlaufWert`).
   - If `p_stichtag` (reference date) is empty, it queries the system date (`v_sysdate`).
   - Logs the job execution ID inside the table `dwtk_meldungen` using legacy logging procedures (`DWMSG_ErzeugeEintrag`).
   - Invokes the core controller script `k_ausd_bp_ta_bpr_opt_text.ksh` with parsed variables.
3. **Core Controller Script (`k_ausd_bp_ta_bpr_opt_text.ksh`)**:
   - Validates arguments (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`).
   - Sets the target job entity/table log registration to `PoolBasisprodukt`.
   - Calls the Oracle SQL script `d_ausd_bp_ta_bpr_opt_text.sql` via SQL*Plus.
4. **Oracle SQL Script (`d_ausd_bp_ta_bpr_opt_text.sql`)**:
   - Truncates the target table `sof$ta_bpr_opt_text`.
   - Populates `sof$ta_bpr_opt_text` with base product descriptions (`pds_description`) by performing an inner join between options (`sof$ta_bpr_optionen`) and descriptions (`sof$ta_bpr_beschr`).

---

### 2.2 Lineage & Data Flow
```
[sof$ta_bpr_optionen] ---\
                         +---> [INNER JOIN (bpr_id)] ---> [sof$ta_bpr_opt_text] (Truncate & Insert)
[sof$ta_bpr_beschr]   ---/
```
* **Upstream Producers (Inputs):**
  - `sof$ta_bpr_optionen` (Alias `bp`)
  - `sof$ta_bpr_beschr` (Alias `bs`)
  - `isbert_schema.dwtk_meldungen` (Used to extract status variables, e.g., the last execution run time for `BERT_DROP_TEMP_TABLE`)
* **Downstream Consumers (Outputs):**
  - `sof$ta_bpr_opt_text` (Populated table containing mapped options and texts)
  - `PoolBasisprodukt` (Legacy logging and orchestration metadata)

---

### 2.3 External System Replacements
* **Scheduler Migration:** UC4/Automic structures are fully decommissioned. Job dependencies and timing controls are converted to **Cloud Composer (Apache Airflow)**.
* **Script Wrapper Consolidation:** The KornShell wrappers (`r_*.ksh`, `k_*.ksh`) used for argument parsing and logging are replaced by the Python-based Airflow DAG orchestration wrapper. Parameter parsing (`stichtag`, `wiederanlauf_wert`) is handled natively via the DAG runtime configurations.
* **Database Engine:** Oracle Database is replaced by **Google Cloud BigQuery**.
* **Utility Libraries:** Legacy procedures (`DWPA_UTIL_SKRIPT.runstatement` or the `DWMSG_*` Unix logging suite) are migrated to native BigQuery statement executions (`TRUNCATE TABLE`) and standard Airflow metadata/Cloud Logging.

---

### 2.4 Environment-Specific Values
The Build Agent and the deployment pipelines must replace standard placeholders with environment configurations:
* **Project ID:** GCP target project name (e.g., `prj-dwh-prod-1234`).
* **Dataset ID:** Target BigQuery dataset (e.g., `isbert_schema` or `isbert_stage`).
* **Connection ID:** `google_cloud_default` (or custom GCP connection profile configured in Airflow).
* **Location:** BigQuery data processing location (e.g., `EU` or `US`).

---

### 2.5 Target File Plan
To build this job, the Build Agent will compile two core target assets in the target repository:

| Target File Path | Target Language | Source File(s) | Target Description |
| :--- | :--- | :--- | :--- |
| `dags/bereitstellung_basisprodukte_bert_dag.py` | Python (Airflow DAG) | `DW.BERT_AUSD_BP_TA_BPR_OPT_TEXT.xml`, `r_ausd_bp_ta_bpr_opt_text.ksh`, `k_ausd_bp_ta_bpr_opt_text.ksh` | Airflow DAG orchestrating parameter initialization, execution tracking, and dependency steps. |
| `gcp/bigquery/sql/d_ausd_bp_ta_bpr_opt_text.sql` | BigQuery SQL (DML) | `d_ausd_bp_ta_bpr_opt_text.sql` | Standard SQL script performing the TRUNCATE and INSERT-SELECT JOIN logic. |

---

### 2.6 Risks and Manual Steps
1. **Identifier Character Replacement:** Special characters in Oracle table names (specifically `$`) are incompatible with standard BigQuery identifiers. All references to `sof$ta_bpr_opt_text`, `sof$ta_bpr_optionen`, and `sof$ta_bpr_beschr` must be translated to `sof_ta_bpr_opt_text`, `sof_ta_bpr_optionen`, and `sof_ta_bpr_beschr` respectively.
2. **Oracle Dynamic SQL Procedure Truncation:** In Oracle, the table truncation was wrapped inside `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_bpr_opt_text REUSE STORAGE')` to bypass DDL locking issues. In BigQuery, this is replaced by standard DDL/DML execution: `TRUNCATE TABLE your_project.your_dataset.sof_ta_bpr_opt_text;` or handled implicitly with a `WRITE_TRUNCATE` configuration.
3. **Date Formats:** The parameter `stichtag` uses German system date format `DDMMYYYY`. When parsing, `PARSE_DATE('%d%m%Y', '{stichtag}')` must be used to cast to standard ISO-8601 BigQuery `DATE` format. Ensure parameter validation is properly implemented inside the Airflow template context.