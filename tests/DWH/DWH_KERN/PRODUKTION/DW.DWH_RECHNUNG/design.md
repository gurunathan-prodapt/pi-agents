# MIGRATION DESIGN DOCUMENT: DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS

---

## MIGRATION DESIGN & TRANSLATION PATTERN

*   **Prescribed Migration Pattern:** `UC4+KSH+SQL_MEDIUM` (High Confidence)
*   **Target Architecture:** Cloud Composer (Airflow) + Dataform + BigQuery + Google Cloud Storage (GCS)
*   **Migration Approach:**
    *   **Orchestration & File Export (Cloud Composer):** Apache Airflow coordinates parameters (`stichtag`), triggers Dataform executions, handles conditional workflow logic, performs row-count checks, prints strict German console statements, and extracts BigQuery table data to GCS.
    *   **Data Modeling & Processing (Dataform):** Replaces legacy Oracle SQL script transformations by compiling and executing a Dataform model in BigQuery.
    *   **Data Destination (GCS):** Receives the final flat, pipe-separated export file using native BigQuery extraction features.

---

## FILE DISPOSITION

| Source File Path | Target File / Target Task | Disposition | Description / Rationale |
| :--- | :--- | :--- | :--- |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS.xml` | `dags/dwh_rechnung_export_taeglich_dag.py` | **Target File** | Translated to Airflow DAG code containing Python operators for orchestration and validation logging. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/bin/r_exp_rechnung_taeglich.ksh` | `dags/dwh_rechnung_export_taeglich_dag.py` | **Merged into Orchestrator** | Shell validation logic, date fallback computations, and native console prints are merged directly into the Composer DAG Python script. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/sql/d_exp_rechnung_taeglich.sql` | `definitions/d_exp_rechnung_taeglich.sqlx` | **Target File** | Ported to a Google Dataform SQLX model inside BigQuery with parameterized filters. |

---

## COMPONENT DETAILS & TRANSLATION

### 1. Source: `DW_RECHNUNG_EXPORT_TAEGLICH_JS.xml` & `r_exp_rechnung_taeglich.ksh`
*   **Role:** Job scheduler triggers script execution, calculates date parameters, executes SQL\*Plus extraction, checks row lengths, prints custom German diagnostic logs, and asserts execution exit status.
*   **Target Translation:** Converted to a single, consolidated Cloud Composer DAG file (`dags/dwh_rechnung_export_taeglich_dag.py`). Native Python methods compute the date fallback, make Dataform API compilation requests, read result counts, write output flat files to GCS using `google-cloud-bigquery`, and guarantee the preservation of original logging strings verbatim.

### 2. Source: `d_exp_rechnung_taeglich.sql`
*   **Role:** Extracts active billing data from Oracle table `DWH_KERN.T_RECHNUNG` matching a given date (`&p_Stichtag`) and formats the records as pipe-separated values.
*   **Target Translation:** Converted to a Dataform incremental model (`definitions/d_exp_rechnung_taeglich.sqlx`) targeting BigQuery. The query utilizes partition filtering on `rechnungsdatum` matching the dynamic Dataform compilation variable `stichtag`.

---

## BUSINESS RULES & LITERAL LOG OUTPUT PRESERVATION

To prevent breaking existing downstream log parsers and legacy operational auditing tools, **no English logging statements have been fabricated**. All literal print outputs have been preserved character-for-character from the original `.xml` and `.ksh` source files:

1.  **Job Triggering:**
    *   *Source (XML):* `PRINT "Rechnungsexport fuer Stichtag &EXPORT_STICHTAG angestossen"`
    *   *Target (DAG):* `print(f"Rechnungsexport fuer Stichtag {stichtag} angestossen")`
2.  **Export Startup:**
    *   *Source (KSH):* `echo "Starte Export Rechnungsdaten fuer Stichtag $l_Stichtag"`
    *   *Target (DAG):* `print(f"Starte Export Rechnungsdaten fuer Stichtag {stichtag}")`
3.  **Result Record Metrics:**
    *   *Source (KSH):* `echo "Anzahl exportierter Rechnungssaetze: $l_Anzahl"`
    *   *Target (DAG):* `print(f"Anzahl exportierter Rechnungssaetze: {row_count}")`
4.  **Zero-Row Verification:**
    *   *Source (KSH):* `f_alis_msgerr "W" "Keine Rechnungsdaten fuer Stichtag $l_Stichtag exportiert"`
    *   *Target (DAG):* `print(f"[W] Keine Rechnungsdaten fuer Stichtag {stichtag} exportiert")`
5.  **Clean Completion:**
    *   *Source (KSH):* `echo "Export Rechnungsdaten ohne erkennbare Fehler beendet"`
    *   *Target (DAG):* `print("Export Rechnungsdaten ohne erkennbare Fehler beendet")`

---

## ENVIRONMENT VALUES & VARIABLES CLASSIFICATION

Per the environment variable management strategy, variables are classified by their deployment role:

### 1. GLOBAL (Environment-Wide Infrastructure Configurations)
These refer to environment configurations shared across all pipelines in a given deployment stage. In the Airflow DAG environment, these are fetched dynamically from Airflow Variables rather than hardcoded string literals:
*   `GCP_PROJECT`: Sourced via `Variable.get("GCP_PROJECT")`
*   `GCP_LOCATION`: Sourced via `Variable.get("GCP_LOCATION", default_var="europe-west3")`
*   `GCS_BUCKET`: Sourced via `Variable.get("GCS_BUCKET")` (corresponds to the export target bucket path)
*   `DATAFORM_REPOSITORY_ID`: Sourced via `Variable.get("DATAFORM_REPOSITORY_ID")`

### 2. JOB-SPECIFIC (Pipeline Configurations)
These settings are specific strictly to this daily invoice extraction module:
*   `DATASET_ID`: `"dw_staging"` (target staging dataset for Dataform execution)
*   `TABLE_ID`: `"tmp_export_rechnung_taeglich"` (target model table processed by the query execution)

---

## VERBATIM MCP DESIGN OUTPUTS

### Target Staging Model: `definitions/d_exp_rechnung_taeglich.sqlx`
```sql
config {
  type: "incremental",
  schema: "dw_staging",
  name: "tmp_export_rechnung_taeglich",
  uniqueKey: ["rechnungs_id"],
  bigquery: {
    partitionBy: "rechnungs_datum"
  }
}

SELECT
  r.RECHNUNGSNUMMER AS rechnungs_id,
  r.VERTRAG AS vertrag,
  r.KUNDE AS kunden_nr,
  r.TARIF AS tarif,
  r.ABRECHNUNGSZEITRAUM AS abrechnungszeitraum,
  ROUND(CAST(r.RECHNUNGSBETRAG AS NUMERIC), 2) AS brutto_betrag,
  r.WAEHRUNG AS waehrung,
  r.RECHNUNGSDATUM AS rechnungs_datum
FROM
  ${ref("dw_source", "tb_rechnungen")} AS r
WHERE
  r.RECHNUNGSDATUM = DATE('${dataform.projectConfig.vars.stichtag}')
ORDER BY r.RECHNUNGSNUMMER
```

### Orchestration Pipeline DAG: `dags/dwh_rechnung_export_taeglich_dag.py`
```python
from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from google.cloud import dataform_v1beta1 as dataform

# -------------------------------------------------------------
# ENV VARIABLE CLASSIFICATION & INGESTION
# -------------------------------------------------------------
# GLOBAL (Environment-Wide Constants)
PROJECT_ID = Variable.get("GCP_PROJECT")
LOCATION = Variable.get("GCP_LOCATION", default_var="europe-west3")
REPOSITORY_ID = Variable.get("DATAFORM_REPOSITORY_ID")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# JOB-SPECIFIC (Pipeline Specific Constants)
DATASET_ID = "dw_staging"
TABLE_ID = "tmp_export_rechnung_taeglich"

DEFAULT_ARGS = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def resolve_stichtag(**kwargs):
    """Calculates extraction target execution date parameter ('stichtag')."""
    dag_run_conf = kwargs.get('dag_run').conf if kwargs.get('dag_run') else None
    if dag_run_conf and 'stichtag' in dag_run_conf:
        return dag_run_conf['stichtag']
    else:
        # Default to T-1 execution logic when triggered on a schedule
        yesterday = datetime.utcnow() - timedelta(days=1)
        return yesterday.strftime('%Y-%m-%d')

def log_initialization(**kwargs):
    ti = kwargs['ti']
    stichtag = ti.xcom_pull(task_ids='resolve_stichtag_task')
    # Exact original XML log output
    print(f"Rechnungsexport fuer Stichtag {stichtag} angestossen")

def log_start_export(**kwargs):
    ti = kwargs['ti']
    stichtag = ti.xcom_pull(task_ids='resolve_stichtag_task')
    # Exact original KSH log output
    print(f"Starte Export Rechnungsdaten fuer Stichtag {stichtag}")

def trigger_dataform_compilation_and_execution(**kwargs):
    ti = kwargs['ti']
    stichtag = ti.xcom_pull(task_ids='resolve_stichtag_task')
    
    # Initialize Google Cloud Dataform Client
    client = dataform.DataformClient()
    repo_path = client.repository_path(PROJECT_ID, LOCATION, REPOSITORY_ID)
    
    # Compile Dataform setting compilation variables dynamically
    compilation_result = client.create_compilation_result(
        parent=repo_path,
        compilation_result=dataform.CompilationResult(
            git_commitish="main",
            code_compilation_config=dataform.CodeCompilationConfig(
                vars={"stichtag": stichtag}
            )
        )
    )
    
    # Trigger Dataform execution sequence for the staging table
    invocation = client.create_workflow_invocation(
        parent=repo_path,
        workflow_invocation=dataform.WorkflowInvocation(
            compilation_result=compilation_result.name,
            invocation_config=dataform.InvocationConfig(
                included_targets=[
                    dataform.Target(
                        database=PROJECT_ID,
                        schema=DATASET_ID,
                        name=TABLE_ID
                    )
                ]
            )
        )
    )
    return invocation.name

def verify_and_extract_results(**kwargs):
    ti = kwargs['ti']
    stichtag = ti.xcom_pull(task_ids='resolve_stichtag_task')
    
    bq_client = bigquery.Client(project=PROJECT_ID)
    
    # Perform Count Query Check
    count_query = f"""
        SELECT COUNT(1) as total_rows 
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE rechnungs_datum = DATE('{stichtag}')
    """
    query_job = bq_client.query(count_query)
    results = query_job.result()
    row_count = next(results).total_rows
    
    # Conditional Logs & Extract Execution
    if row_count == 0:
        # Exact original KSH Warning log output
        print(f"[W] Keine Rechnungsdaten fuer Stichtag {stichtag} exportiert")
        return
    
    # Exact original KSH metrics log output
    print(f"Anzahl exportierter Rechnungssaetze: {row_count}")
    
    # Export execution to GCS as a Pipe-separated Flat File
    stichtag_clean = stichtag.replace('-', '')
    destination_uri = f"gs://{GCS_BUCKET}/rechnung_export_{stichtag_clean}.dat"
    dataset_ref = bigquery.DatasetReference(PROJECT_ID, DATASET_ID)
    table_ref = dataset_ref.table(TABLE_ID)
    
    job_config = bigquery.ExtractJobConfig()
    job_config.field_delimiter = "|"
    job_config.print_header = False  # Matches SQL*Plus query spool format
    
    extract_job = bq_client.extract_table(
        table_ref,
        destination_uri,
        job_config=job_config
    )
    extract_job.result()  # Wait for the GCP Job to finish processing
    
    # Exact original KSH completion log output
    print("Export Rechnungsdaten ohne erkennbare Fehler beendet")

with DAG(
    'dw_dwh_rechnung_export_taeglich_js',
    default_args=DEFAULT_ARGS,
    schedule_interval='0 6 * * *',  # Daily 06:00 UTC
    catchup=False
) as dag:

    resolve_stichtag_task = PythonOperator(
        task_id='resolve_stichtag_task',
        python_callable=resolve_stichtag,
        provide_context=True
    )

    log_init = PythonOperator(
        task_id='log_initialization',
        python_callable=log_initialization,
        provide_context=True
    )

    log_start = PythonOperator(
        task_id='log_start_export',
        python_callable=log_start_export,
        provide_context=True
    )

    dataform_execution_task = PythonOperator(
        task_id='dataform_execution_task',
        python_callable=trigger_dataform_compilation_and_execution,
        provide_context=True
    )

    verify_and_extract = PythonOperator(
        task_id='verify_and_extract',
        python_callable=verify_and_extract_results,
        provide_context=True
    )

    resolve_stichtag_task >> log_init >> log_start >> dataform_execution_task >> verify_and_extract
```

---

## ADDITIONAL CONTEXT & RUNTIME ARCHITECTURE

### Job Dependencies & Flow Coordination
*   **Upstream Dependencies:** This daily export depends on the upstream billing loading process (`DW.DWH_RECHNUNG_LOAD_JS` / `DW.DWH_RECHNUNG_LOAD_JP` or equivalent) that updates the BigQuery base table `tb_rechnungen`.
*   **Scheduling Execution:** Scheduled to trigger daily at `06:00 UTC` via standard Airflow cron expression.
*   **Downstream Consumers:** Downstream systems that process `rechnung_export_YYYYMMDD.dat` from Cloud Storage should be reconfigured to read from the target GCS bucket rather than the legacy local path (`$HOME/aktuell/export/rechnung/ausgang`).

---

## RISKS & MANUAL ACTIONS

1.  **Downstream File Fetching Adjustment:** Downstream consumption processes currently pointing to local Unix system mount paths must adapt to fetch the file from the target Google Cloud Storage bucket (`GCS_BUCKET`).
2.  **Date Validation Testing:** Ensure that manual Airflow DAG executions passing custom JSON configurations (e.g., `{"stichtag": "2023-11-20"}`) compile and override standard schedule defaults seamlessly.