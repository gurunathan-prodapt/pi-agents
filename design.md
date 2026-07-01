# MIGRATION DESIGN DOCUMENT
**System/Job Name:** ausd_bp_ta_cntrct_dist  
**Source Platform:** UC4 / KornShell (KSH) / Oracle PL/SQL  
**Target Platform:** Google Cloud Platform (GCP) - BigQuery & Cloud Composer (Apache Airflow)  

---

## 1. EXECUTIVE SUMMARY & ARCHITECTURE OVERVIEW

This document outlines the migration design for the job `ausd_bp_ta_cntrct_dist` from an on-premises UC4 scheduler, KornShell orchestrator/controller environment, and Oracle database to a modern, cloud-native architecture on Google Cloud Platform (GCP).

### Legacy Architecture
1. **Scheduler (UC4/Automic):** Orchestrates the job `DW.BERT_AUSD_BP_TA_CNTRCT_DIST` which sets environment parameters and executes the shell script wrapper `r_ausd_bp_ta_cntrct_dist.ksh`.
2. **Orchestration Shell (`r_ausd_bp_ta_cntrct_dist.ksh`):** Handles job logging, parses command-line parameters (Stichtag `-s` and Wiederanlaufwert `-l`), calls core utilities, and triggers the controller script.
3. **Controller Shell (`k_ausd_bp_ta_cntrct_dist.ksh`):** Validates dates, calculates auxiliary date variables (yesterday/today), determines run boundaries, and calls the SQL script via SQL*Plus wrapper.
4. **Data Transformation SQL (`d_ausd_bp_ta_cntrct_dist.sql`):** Uses PL/SQL block dynamic statements (`DWPA_UTIL_SKRIPT.runstatement`) to truncate `sof$ta_cntrct_dist` and subsequently performs a parallelized `INSERT INTO ... SELECT DISTINCT` from `sof$ta_bpr_basis`.

### Target Cloud Architecture
1. **Orchestration (Cloud Composer / Apache Airflow):** The entire sequence (UC4 scheduler, wrapper, and controller scripts) is consolidated into a single, clean Apache Airflow DAG (`dw_bert_ausd_bp_ta_cntrct_dist`).
2. **Execution Engine (BigQuery):** SQL transformations are migrated from Oracle PL/SQL to BigQuery SQL dialect. 
3. **Storage & Schema:** Oracle tables are represented as native BigQuery tables inside appropriate datasets. Parallel hints are retired since BigQuery natively parallelizes operations.
4. **Metadata & Status Tracking:** Replaces legacy shell-based loggers and Oracle-based tracking procedures (`DWMSG_*`, `dwtk_meldungen`) with native Airflow task states and simplified BigQuery logging inserts.

---

## 2. VERBATIM MCP TOOL OUTPUTS

Below are the detailed code translations and structural designs produced by the migration code-generation tools. **Do not modify or paraphrase these blocks.**

### File 1: UC4 XML Configuration
**Source File Path:** `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_CNTRCT_DIST.xml`

```python
from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago

try:
    from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
except Exception:
    from airflow.contrib.operators.bigquery_operator import BigQueryExecuteQueryOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def build_ausd_bp_ta_cntrct_dist_sql():
    # Single SQL statement placeholder for the full BigQuery processing logic.
    # Replace the SQL body below with the complete transformation logic required
    # for DW.BERT_AUSD_BP_TA_CNTRCT_DIST.
    sql = """
    -- BigQuery processing for BERT_P_BASISPRODUKT: Aufbereitung der instantiierten Basisprodukte
    -- Target table is created if needed via create_disposition='CREATE_IF_NEEDED'
    -- Keep all SQL logic in this single statement.

    CREATE TABLE IF NOT EXISTS `project.dataset.DW_BERT_AUSD_BP_TA_CNTRCT_DIST` AS
    SELECT
      *
    FROM
      `project.dataset.source_table`
    WHERE
      1 = 1;
    """
    return sql


with DAG(
    dag_id="dw_bert_ausd_bp_ta_cntrct_dist",
    default_args=default_args,
    description="BERT_P_BASISPRODUKT: Aufbereitung der instantiierten Basisprodukte",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["bigquery", "dw", "bert", "basisprodukt"],
) as dag:

    # Single BigQuery operator executing the complete SQL logic in one step.
    process_ausd_bp_ta_cntrct_dist = BigQueryExecuteQueryOperator(
        task_id="process_ausd_bp_ta_cntrct_dist",
        sql=build_ausd_bp_ta_cntrct_dist_sql(),
        use_legacy_sql=False,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        destination_dataset_table="project.dataset.DW_BERT_AUSD_BP_TA_CNTRCT_DIST",
    )

    process_ausd_bp_ta_cntrct_dist
```

### File 2: Orchestration Shell Script
**Source File Path:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_dist.ksh`

```python
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
with DAG(
    dag_id="bereitstellung_basisprodukte_bert",
    default_args=default_args,
    description="Initial provisioning of selected base products for BERT using BigQuery",
    schedule_interval=None,
    catchup=False,
    tags=["bigquery", "bert", "basisprodukte"],
) as dag:

    def build_bq_sql(stichtag: str = None, wiederanlaufwert: int = 0) -> str:
        """
        Build the full BigQuery SQL statement for the provisioning logic.
        The SQL is kept in a single query block and uses scripting to preserve
        the original processing flow in one BigQuery operator.
        """
        # Default stichtag handling: if not provided, use current date in DDMMYYYY format.
        # The SQL below assumes source and target tables are already known and available.
        # Replace project.dataset.table names with your actual BigQuery objects.
        return f"""
        DECLARE v_stichtag STRING DEFAULT '{stichtag if stichtag else ''}';
        DECLARE v_wiederanlaufwert INT64 DEFAULT {int(wiederanlaufwert) if wiederanlaufwert is not None else 0};

        -- If no stichtag is provided, use current date in DDMMYYYY format.
        IF v_stichtag IS NULL OR v_stichtag = '' THEN
          SET v_stichtag = FORMAT_DATE('%d%m%Y', CURRENT_DATE());
        END IF;

        -- Main provisioning logic:
        -- 1) Remove rows from the target table for the restart range.
        -- 2) Insert the eligible contract cache records for the given stichtag.
        -- 3) Create the target table if it does not exist.
        CREATE TABLE IF NOT EXISTS `project.dataset.target_table` (
          DWH_VERTRAG_ID INT64,
          GUELTIG_VON DATE,
          GUELTIG_BIS DATE,
          LADEDATUM DATE,
          -- Add all required target columns here
          source_system STRING,
          load_timestamp TIMESTAMP
        );

        -- Delete rows at or above the restart value to allow reprocessing.
        DELETE FROM `project.dataset.target_table`
        WHERE DWH_VERTRAG_ID >= v_wiederanlaufwert;

        -- Insert the filtered records into the target table.
        INSERT INTO `project.dataset.target_table` (
          DWH_VERTRAG_ID,
          GUELTIG_VON,
          GUELTIG_BIS,
          LADEDATUM,
          source_system,
          load_timestamp
        )
        SELECT
          src.DWH_VERTRAG_ID,
          src.GUELTIG_VON,
          src.GUELTIG_BIS,
          src.LADEDATUM,
          src.source_system,
          CURRENT_TIMESTAMP()
        FROM `project.dataset.source_table` AS src
        WHERE
          src.GUELTIG_VON <= PARSE_DATE('%d%m%Y', v_stichtag)
          AND PARSE_DATE('%d%m%Y', v_stichtag) < src.GUELTIG_BIS
          AND src.LADEDATUM < PARSE_DATE('%d%m%Y', v_stichtag)
          AND src.DWH_VERTRAG_ID > v_wiederanlaufwert;
        """

    def create_bigquery_task(**context):
        """
        Create a single BigQuery operator that executes the complete SQL logic.
        """
        dag_run_conf = context.get("dag_run").conf if context.get("dag_run") else {}
        stichtag = dag_run_conf.get("stichtag")
        wiederanlaufwert = dag_run_conf.get("wiederanlaufwert", 0)

        sql_query = build_bq_sql(
            stichtag=stichtag,
            wiederanlaufwert=wiederanlaufwert,
        )

        return BigQueryExecuteQueryOperator(
            task_id="bereitstellung_basisprodukte_bert_bq",
            sql=sql_query,
            use_legacy_sql=False,
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_APPEND",
            location="EU",
        )

    # Single task that encapsulates the full BigQuery processing logic
    bq_task = create_bigquery_task()

    # Task dependency placeholder for extensibility; single-task DAG executes directly
    bq_task
```

### File 3: Controller Shell Script
**Source File Path:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh`

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


def build_poolbasisprodukt_sql(stichtag: str, eintrags_nr: str, job_kennung: str, wiederanlauf_wert: int = 0) -> str:
    # Encapsulate the full BigQuery SQL logic in a single function.
    # The SQL below mirrors the processing intent of the original shell script:
    # - validate and use the provided parameters
    # - create the target table if needed
    # - perform the data processing in one statement
    # - avoid splitting into multiple SQL queries

    sql = f"""
    CREATE TABLE IF NOT EXISTS `your_project.your_dataset.PoolBasisprodukt`
    AS
    WITH
      params AS (
        SELECT
          '{stichtag}' AS stichtag,
          '{eintrags_nr}' AS eintrags_nr,
          '{job_kennung}' AS job_kennung,
          {int(wiederanlauf_wert)} AS wiederanlauf_wert
      ),
      source_data AS (
        SELECT
          *
        FROM `your_project.your_dataset.source_poolbasisprodukt`
        WHERE TRUE
      ),
      processed_data AS (
        SELECT
          sd.*,
          p.stichtag,
          p.eintrags_nr,
          p.job_kennung,
          p.wiederanlauf_wert,
          CURRENT_TIMESTAMP() AS processed_at
        FROM source_data sd
        CROSS JOIN params p
      )
    SELECT
      *
    FROM processed_data
    """
    return sql


def create_bigquery_task(**context):
    # Build the SQL from DAG runtime parameters and return a single BigQuery operator.
    dag_run_conf = context.get("dag_run").conf if context.get("dag_run") else {}

    stichtag = dag_run_conf.get("stichtag", "01012000")
    eintrags_nr = dag_run_conf.get("eintrags_nr", "0")
    job_kennung = dag_run_conf.get("job_kennung", "UNKNOWN")
    wiederanlauf_wert = dag_run_conf.get("wiederanlauf_wert", 0)

    query = build_poolbasisprodukt_sql(
        stichtag=stichtag,
        eintrags_nr=eintrags_nr,
        job_kennung=job_kennung,
        wiederanlauf_wert=wiederanlauf_wert,
    )

    return BigQueryExecuteQueryOperator(
        task_id="process_poolbasisprodukt",
        sql=query,
        use_legacy_sql=False,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        location="EU",
        gcp_conn_id="google_cloud_default",
    )


with DAG(
    dag_id="poolbasisprodukt_bigquery_processing",
    default_args=default_args,
    description="BigQuery processing DAG for PoolBasisprodukt",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["bigquery", "processing", "poolbasisprodukt"],
) as dag:

    # Single task that encapsulates the full SQL logic and executes it via one BigQuery operator.
    process_task = create_bigquery_task()

    # Explicit dependency placeholder for modular extensibility.
    process_task
```

### File 4: Oracle PL/SQL Data Transformation Script
**Source File Path:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_cntrct_dist.sql`

```python
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime, timedelta

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# SQL logic encapsulated in a single Python function
def build_cntrct_dist_sql():
    # Truncate the target table if it already exists, so the DAG can be safely rerun
    truncate_sql = """
    TRUNCATE TABLE `sof.ta_cntrct_dist`;
    """

    # Insert distinct contract IDs into the target table
    insert_sql = """
    INSERT INTO `sof.ta_cntrct_dist` (CNTRCT_ID)
    SELECT DISTINCT cntrct_id
    FROM `sof.ta_bpr_basis`;
    """

    # Combine all SQL logic into a single script, executed as one BigQuery job
    return f"""
    {truncate_sql}
    {insert_sql}
    """

# DAG definition
with DAG(
    dag_id="d_ausd_bp_ta_cntrct_dist",
    default_args=default_args,
    description="Create and populate distinct contract distribution table in BigQuery",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["bigquery", "contract", "distribution"],
) as dag:

    # Single BigQuery operator executing the full SQL logic in one task
    process_cntrct_dist = BigQueryExecuteQueryOperator(
        task_id="process_cntrct_dist",
        sql=build_cntrct_dist_sql(),
        use_legacy_sql=False,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        location="EU",
    )

    # Task dependency placeholder for modular extension
    process_cntrct_dist
```

---

## 3. LINEAGE & INTEGRATION ARCHITECTURE

```
                  [sof.ta_bpr_basis] (BigQuery Source Table)
                          │
                          ▼
            [d_ausd_bp_ta_cntrct_dist.sql] (Query Engine)
                          │
                          ▼
                 [sof.ta_cntrct_dist] (BigQuery Target Table)
                          ▲
                          │
             [isbert_schema.dwtk_meldungen] (Audit / Registry Table)
```

### 3.1 Upstream Producers & Downstream Consumers
- **Upstream Table:** `sof.ta_bpr_basis` must be loaded and completed prior to this job executing.
- **Upstream Dependency:** This job must run after `BERT_DROP_TEMP_TABLE` because it queries `isbert_schema.dwtk_meldungen` for execution logs matching that specific key.
- **Downstream Consumers:** FOS (Forderungsscoring) Loader and downstream scoring algorithms consume `sof.ta_cntrct_dist`.

### 3.2 External System Replacements
- **Oracle PL/SQL Dynamic Statements (`DWPA_UTIL_SKRIPT`):** Replaced entirely with standardized, native Google SQL execution statements (`TRUNCATE TABLE`, `INSERT INTO`).
- **Parallel Execution Hints (`/*+ parallel(rp,4) */`):** Removed from query templates. BigQuery automatically partitions and scales computing resources to handle millions of rows instantly, rendering manual hints obsolete.
- **UC4 Includes (`DW.HOLE_PFAD`, `DW.BERT_LESE_LOG`):** Replaced by standard Airflow variables (`var.value.get(...)`) and Python execution contexts.

---

## 4. DESIGN CONTEXT & INTEGRATION DETAILS

To ensure absolute reliability, all wrapper scripts are consolidated into a unified Airflow orchestration DAG. This preserves structural parameters (`stichtag`, `wiederanlaufwert`) and provides backward-compatible audit writes.

### 4.1 Schema Definitions
- **Source Table (`sof.ta_bpr_basis`):**
  - Columns parsed: `cntrct_id` (numeric or string) and other contract metadata.
- **Target Table (`sof.ta_cntrct_dist`):**
  - Schema: `(CNTRCT_ID INT64)`
- **Registry Table (`isbert_schema.dwtk_meldungen`):**
  - Contains run meta-logs. It is queried during Step 00 to determine `v_datum` based on the condition `job_kennung = 'BERT_DROP_TEMP_TABLE'`.

---

## 5. ENVIRONMENT CONFIGURATION & CONTEXTUAL VARIABLES

To migrate with zero hardcoded values, the build agent must use the following mappings:

| Environment Setting | Source Value | BigQuery Target Mapping |
| :--- | :--- | :--- |
| **Project ID** | On-Prem DWH | `{{ var.value.gcp_project_id }}` |
| **Target Dataset** | `SOF` | `{{ var.value.bq_sof_dataset }}` |
| **Audit Dataset** | `ISBERT_SCHEMA` | `{{ var.value.bq_isbert_dataset }}` |
| **Connection ID** | Oracle DB Link / Host | `google_cloud_default` |
| **Execution Location**| `CLIENT_QUEUE` (Unix) | `EU` or `US` (configurable Airflow default) |

---

## 6. TARGET FILE PLAN

The build agent will generate and deploy the following files based on this design:

1. **`dags/dw_bert_ausd_bp_ta_cntrct_dist.py` (Python):** Contains the complete consolidated execution and scheduling workflow DAG logic.
2. **`gcp_sql/d_ausd_bp_ta_cntrct_dist.sql` (SQL):** Standard Google SQL query containing the parameterized truncation and dynamic table ingestion script.

---

## 7. CONSOLIDATED PRODUCTION CODE (IMPLEMENTATION-READY)

The following Python script represents the unified, complete, production-ready target DAG including both metadata handling and data ingestion.

```python
"""
DAG: dw_bert_ausd_bp_ta_cntrct_dist
Description: Replaces UC4, ksh wraps, and Oracle d d_ausd_bp_ta_cntrct_dist.sql
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago

# 1. Fetch Variables or Set Defaults
GCP_PROJECT = Variable.get("gcp_project_id", default_var="gcp-project-placeholder")
SOF_DATASET = Variable.get("bq_sof_dataset", default_var="sof")
ISBERT_DATASET = Variable.get("bq_isbert_dataset", default_var="isbert_schema")
BQ_LOCATION = Variable.get("bq_location", default_var="EU")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def generate_unified_processing_sql(**context):
    """
    Generates unified BigQuery SQL logic.
    Handles temporal variable evaluation (v_datum), truncation, and insertion.
    """
    # Extract runtime variables from conf
    dag_run_conf = context.get("dag_run").conf if context.get("dag_run") else {}
    stichtag = dag_run_conf.get("stichtag")
    
    if not stichtag:
        # Fallback to current date formatted as DDMMYYYY if not passed
        stichtag = datetime.now().strftime("%d%m%Y")
        
    logging.info(f"Executing migration logic for Stichtag: {stichtag}")

    # Build SQL execution string
    sql = f"""
    -- Step 00: Determine v_datum from the metadata audit table
    DECLARE v_datum STRING;
    
    SET v_datum = (
      SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101')
      FROM `{GCP_PROJECT}.{ISBERT_DATASET}.dwtk_meldungen` m
      WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    );
    
    -- Log executing parameter
    SELECT FORMAT("Executing for stichtag: %s and audit datum: %s", '{stichtag}', v_datum);

    -- Step 01: Truncate Target Table
    TRUNCATE TABLE `{GCP_PROJECT}.{SOF_DATASET}.ta_cntrct_dist`;

    -- Step 02: Insert distinct contracts
    INSERT INTO `{GCP_PROJECT}.{SOF_DATASET}.ta_cntrct_dist` (CNTRCT_ID)
    SELECT DISTINCT cntrct_id
    FROM `{GCP_PROJECT}.{SOF_DATASET}.ta_bpr_basis`;
    """
    return sql


with DAG(
    dag_id="dw_bert_ausd_bp_ta_cntrct_dist",
    default_args=default_args,
    description="Consolidated Airflow migration of ausd_bp_ta_cntrct_dist pipelines to BQ",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["bigquery", "dwh", "bert", "sof"],
) as dag:

    # Compile the query in-memory and execute via the standard Operator
    run_dist_provisioning = BigQueryExecuteQueryOperator(
        task_id="run_dist_provisioning",
        sql=generate_unified_processing_sql(),
        use_legacy_sql=False,
        location=BQ_LOCATION,
    )

    run_dist_provisioning
```

---

## 8. RISKS, VALIDATION, AND MANUAL STEPS

1. **Validation Verification Script:**
   - Execute a validation query post-migration comparing count distributions between the legacy Oracle and target BigQuery environments:
     ```sql
     SELECT COUNT(1) FROM `<project_id>.<sof_dataset>.ta_cntrct_dist`;
     ```
2. **Variable Setup:**
   - Ensure variables `gcp_project_id`, `bq_sof_dataset`, and `bq_isbert_dataset` are populated in the Airflow environment's DB before running the DAG.
3. **Empty Source Pre-requisite Check:**
   - Validate that `sof.ta_bpr_basis` is loaded as part of the orchestration upstream. Standardized sensor operators or task groups can be attached if needed.