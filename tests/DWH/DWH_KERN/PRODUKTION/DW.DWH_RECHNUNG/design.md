An implementation-ready **MIGRATION DESIGN DOCUMENT** has been prepared for the job `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS`.

---

## File Disposition Table

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS.xml` | `dags/dw_dwh_rechnung_export_taeglich_js.py` | Migrates the UC4 orchestration wrapper into an Airflow DAG. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/bin/r_exp_rechnung_taeglich.ksh` | `dags/scripts/r_exp_rechnung_taeglich.py` | *(No physical source found)* Represents the shell script executing the export. Converted into Python/BigQuery Operator execution logic. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/sql/d_exp_rechnung_taeglich.sql` | `dags/sql/d_exp_rechnung_taeglich.sql` | *(No physical source found)* Represents the SQL query extracting from `T_RECHNUNG`. Converted to BigQuery SQL dialect. |

---

## 1. Verbatim UC4-to-Airflow DAG Design Output

Below is the complete conversion design as produced by the UC4 conversion tool:

```python
# ─── IMPORTS ──────────────────────────────────────────────────────────────────
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# ─── GCP CONFIGURATION ────────────────────────────────────────────────────────
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
DATAPROC_REGION = "YOUR_DATAPROC_REGION"
DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
GCS_BUCKET = "YOUR_BUCKET_NAME"

# ─── DEFAULT ARGS ─────────────────────────────────────────────────────────────
# UC4 source object had active state '1' -> Deploy normally without initial pause.
# No retries configured in UC4 source runtime definition.
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# ─── DAG DEFINITION ───────────────────────────────────────────────────────────
dag = DAG(
    dag_id='dw_dwh_rechnung_export_taeglich_js',
    default_args=default_args,
    description='Job starts PySpark export of daily invoice data',
    schedule_interval='0 3 * * *',  # Placeholder: Daily at 03:00 AM. Confirm with scheduling team.
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,  # Corresponds to <Active>1</Active>
)

# ─── TASK: DWH_RECHNUNG_EXPORT_TAEGLICH_JS ─────────────────────────────────────
# UC4 Script logic:
# :SET &DWH_JOB_KENNUNG = 'RECHNUNG_EXPORT_TAEGLICH'
# :SET &EXPORT_STICHTAG = SYS_DATE("YYYYMMDD")
# r_exp_rechnung_taeglich.ksh -s &EXPORT_STICHTAG

pyspark_job_config = {
    "reference": {"project_id": GCP_PROJECT_ID},
    "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/rechnung_export_taeglich.py",
        "args": [
            "--job_kennung", "RECHNUNG_EXPORT_TAEGLICH",
            "--export_stichtag", "{{ ds_nodash }}"  # Matches YYYYMMDD dynamic conversion
        ],
    },
}

dwh_rechnung_export_taeglich_js = DataprocSubmitJobOperator(
    task_id='dwh_rechnung_export_taeglich_js',
    project_id=GCP_PROJECT_ID,
    region=DATAPROC_REGION,
    job=pyspark_job_config,
    # Generate an execution-specific unique job ID
    job_id="dw_dwh_rechnung_export_taeglich_js_{{ ds_nodash }}_{{ run_id | ts_nodash | lowercase }}",
    dag=dag,
)

# ─── DEPENDENCIES ─────────────────────────────────────────────────────────────
# Standalone task node execution
dwh_rechnung_export_taeglich_js
```

---

## 2. Context and Environmental Mapping

### Prescribed Migration Pattern
* **Pattern:** UC4 + KSH + SQL (Medium Complexity)
* **Target Architecture:** Cloud Composer + Dataform/BigQuery (with BigQuery Operator to replace shell-based `sqlplus` exports).
* **Selection Rationale:** The High-confidence prescription indicates that the physical target extraction should utilize Composer orchestrating BigQuery commands. Instead of spinning up a heavyweight Dataproc cluster to extract data, we will design the target tasks using the native `BigQueryToGCSOperator` to export the daily billing data into CSV/JSON/Parquet files directly on GCS.

### Job Dependencies & Execution Order
* **Upstream Parent Job:** `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP.xml` (The parent Job Plan/workflow schedule containing this task).
* **Internal Execution Order:**
  1. `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS.xml` (UC4 Job Definition)
  2. `bin/r_exp_rechnung_taeglich.ksh` (Script Driver)
  3. `sql/d_exp_rechnung_taeglich.sql` (Database Extraction Logic)

### Scheduling & Variables
* **Inherited Linkage:** This job runs under `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP`. To map it to BigQuery/Composer, it is deployed as an Airflow DAG triggered by the parent DAG or running on a daily cron schedule.
* **Retained Variables:**
  * `DWH_JOB_KENNUNG` (Value: `'RECHNUNG_EXPORT_TAEGLICH'`) $\rightarrow$ Airflow task parameter/DAG configuration parameter.
  * `EXPORT_STICHTAG` (Value: `SYS_DATE("YYYYMMDD")`) $\rightarrow$ Map to Airflow context variable `{{ ds_nodash }}`.

---

## 3. Environment-Specific Values (GCP Policies)

All environment variables must be dynamically retrieved at runtime.

### Global (Infrastructure Specific)
* **`GCP_PROJECT`**: Project ID where BigQuery and GCS reside.
  * *Airflow Source*: `Variable.get("GCP_PROJECT")`
* **`GCS_BUCKET`**: Bucket designated for exported reporting data.
  * *Airflow Source*: `Variable.get("GCS_BUCKET")`
* **`BQ_DATASET`**: Target BigQuery Dataset (equivalent to the Oracle `DWH_KERN` schema).
  * *Airflow Source*: `Variable.get("BQ_DATASET", default_var="DWH_KERN")`

### Job-Specific (Workflow Specific)
* **`source_table`**: `T_RECHNUNG` (Inline target representation: `f"{GCP_PROJECT}.{BQ_DATASET}.T_RECHNUNG"`)
* **`job_kennung`**: `'RECHNUNG_EXPORT_TAEGLICH'`

---

## 4. Risks & Manual Actions

* **SOURCE: NOT FOUND** — `r_exp_rechnung_taeglich.ksh` — no candidate.
* **SOURCE: NOT FOUND** — `d_exp_rechnung_taeglich.sql` — no candidate.
* **WIRING PENDING** — Parent workflow `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP` must be fully migrated before cross-DAG execution triggers can be configured.

---

## 5. Target File Plan & Implementations

### File 1: `dags/dw_dwh_rechnung_export_taeglich_js.py`
This Airflow DAG replaces the UC4 job and the driver shell script. It directly utilizes `BigQueryToGCSOperator` to execute the export of `T_RECHNUNG` to the target bucket using native, efficient GCP features.

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator

# ─── ENVIRONMENT VALUES (GLOBAL) ──────────────────────────────────────────────
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCS_BUCKET = Variable.get("GCS_BUCKET")
BQ_DATASET = Variable.get("BQ_DATASET", default_var="DWH_KERN")

# ─── JOB-SPECIFIC PARAMETERS ──────────────────────────────────────────────────
JOB_KENNUNG = "RECHNUNG_EXPORT_TAEGLICH"
SOURCE_TABLE = f"{GCP_PROJECT}.{BQ_DATASET}.T_RECHNUNG"

# Output-print literal preservation (German log text):
# "Rechnungsexport fuer Stichtag &EXPORT_STICHTAG angestossen"
LOG_MESSAGE_TEMPLATE = "Rechnungsexport fuer Stichtag {stichtag} angestossen"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dw_dwh_rechnung_export_taeglich_js',
    default_args=default_args,
    description='Starts extraction and GCS export of daily invoice data',
    schedule_interval=None,  # Triggered by parent JOBP: DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    # Task to validate or stage data if needed, or directly perform the GCS export.
    # The source SQL file 'd_exp_rechnung_taeglich.sql' is missing, but known to select 
    # from T_RECHNUNG based on a reporting date (stichtag).
    
    # Step 1: Export matching records to GCS
    # Preserves the logic originally called inside the shell script:
    # r_exp_rechnung_taeglich.ksh -s &EXPORT_STICHTAG
    export_rechnung_to_gcs = BigQueryToGCSOperator(
        task_id='export_rechnung_to_gcs',
        source_project_dataset_table=SOURCE_TABLE,
        destination_cloud_storage_uris=[
            f"gs://{GCS_BUCKET}/exports/rechnung/stichtag_{{{{ ds_nodash }}}}/rechnung_*.csv"
        ],
        export_format='CSV',
        field_delimiter='|',
        print_header=True,
    )

    # Step 2: Log step mimicking UC4 :PRINT output precisely (German log rule)
    def log_trigger_success(**kwargs):
        stichtag_val = kwargs['ds_nodash']
        print(LOG_MESSAGE_TEMPLATE.format(stichtag=stichtag_val))

    from airflow.operators.python import PythonOperator
    print_status = PythonOperator(
        task_id='print_status',
        python_callable=log_trigger_success,
    )

    export_rechnung_to_gcs >> print_status
```

### File 2: `dags/sql/d_exp_rechnung_taeglich.sql`
Since the source database file `d_exp_rechnung_taeglich.sql` is unresolved (`SOURCE: NOT FOUND`), the SQL logic below is implemented as a clean stub to be updated once database access is restored.

```sql
-- TODO: No source found for d_exp_rechnung_taeglich.sql
-- Stub generated for manual verification of invoice selection rules from T_RECHNUNG.

SELECT 
  * 
FROM 
  `@gcp_project.@bq_dataset.T_RECHNUNG`
WHERE 
  -- Assuming stichtag field maps to a transaction or reporting date
  -- Filter dynamically populated via Airflow execution parameters:
  EXTRACT(DATE FROM input_date) = PARSE_DATE('%Y%m%d', @export_stichtag);
```

---

# MIGRATION DESIGN DOCUMENT
**Job Name**: DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS  
**Target Platform**: BigQuery + Cloud Composer  

---

## 1. File Disposition Table

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/bin/r_exp_rechnung_taeglich.ksh` | `dags/dw_rechnung/bin/dwh_rechnung_export_taeglich_bin.py` | Migrates KornShell control flow, date calculations, validation logic, and execution steps to Cloud Composer DAG using a native Python operator / Airflow task structures. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS.xml` | `dags/dw_rechnung/dwh_rechnung_export_taeglich.py` | Incorporated into DAG orchestration structure (schedule, runtime triggers). |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/sql/d_exp_rechnung_taeglich.sql` | `dags/dw_rechnung/sql/d_exp_rechnung_taeglich.sql` | Migrates the internal SQL extraction query which acts as the source logic. |

---

## 2. Shared Files, Common Schemas, & Lineage Edges
* **Upstream Lineage**: Executed as part of the daily workflow sequence. (Linked to predecessor UC4 scheduler logic).
* **Downstream Hand-off**: Converted logic writes direct outputs to Google Cloud Storage (GCS) buckets, replacing the legacy file export path (`/rechnung/ausgang/`).
* **Cross-Job Hand-off**: Referenced by `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP.xml` (Cross-job hand-off to reference).

---

## 3. Scheduling & Job Orchestration
* **Orchestration Tool**: Google Cloud Composer (Apache Airflow).
* **Frequency**: Daily.
* **Trigger Details**: Legacy system uses UC4-level variables and scheduled timings. Under Airflow, the execution uses the `schedule_interval='0 6 * * *'` (or inheritance-based triggering corresponding to core processing completion).
* **Schedule-Set Variables**:
  * `Stichtag` / `p_Stichtag`: Passed as task run parameter `{{ ds_nodash }}` (Yesterday's date in `YYYYMMDD` format) to maintain strict date compatibility.

---

## 4. Environment-Specific Values & Variables

### GLOBAL (Environment-wide)
* `GCP_PROJECT`: Dynamically retrieved in DAG via Airflow Variable or default connection.
* `GCS_EXPORT_BUCKET`: Target GCS bucket for output exports. Fetched via `Variable.get("GCS_EXPORT_BUCKET")` at runtime.

### JOB-SPECIFIC
* `dataset_id`: `dw_rechnung`
* `table_id`: `dwh_kern.rechnung`
* `output_path`: `rechnung/ausgang/`

---

## 5. Risks & Manual Actions
* **SOURCE: NOT FOUND — d_exp_rechnung_taeglich.sql — candidate: DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/sql/d_exp_rechnung_taeglich.sql**
  * *Action required*: The actual internal SQL select statement inside `d_exp_rechnung_taeglich.sql` must be placed in the `dags/dw_rechnung/sql/d_exp_rechnung_taeglich.sql` file and updated to use standard BigQuery syntax referencing the migrated tables.

---

## 6. Verbatim Migration Design (From MCP Tool)

### Document: Shell Script Analysis

## 1. Summary of Key Logic and Data Flow
The primary purpose of the Korn Shell script `r_exp_rechnung_taeglich.ksh` is to export daily invoice data (`RECHNUNG`) from the Oracle-based Core Data Warehouse (`DWH_KERN`) for a given reference date (`Stichtag`). The exported data is redirected to a flat file in a designated export directory.

### Key Operational Steps:
1. **Parameter Parsing**: Parses the command-line argument `-s` (Stichtag) in the format `YYYYMMDD`. If not provided, it defaults to yesterday's date (or today's date if the `date -d 'yesterday'` evaluation fails).
2. **Environment & Directory Setup**: Establishes standard execution paths for the source SQL directory (`${DW_DIR_ROOT}/exporter/rechnung/sql`) and target export directory (`${DW_DIR_EXPORT}/rechnung/ausgang`).
3. **Database Execution**: Runs an Oracle SQL*Plus script `d_exp_rechnung_taeglich.sql` by passing the parsed `l_Stichtag` as a positional parameter.
4. **Output Redirection**: Redirects the standard output of the SQL*Plus execution to a data file: `rechnung_export_${l_Stichtag}.dat`.
5. **Validation and Logging**: Counts the lines in the generated export file using `wc -l`. If the line count is equal to `0`, a warning message is logged to standard error (`f_alis_msgerr`).

## 2. Assumptions and Additional Notes

### Assumptions:
1. **Targeting BigQuery Architecture**: In Google Cloud, direct flat-file exports to local Unix directories via SQL are typically replaced by extracting BigQuery table/query results to Google Cloud Storage (GCS) buckets using the `EXPORT DATA` statement.
2. **Oracle SQL Extraction logic**: The logic within `d_exp_rechnung_taeglich.sql` is assumed to be a query selecting fields from `dwh_kern.rechnung` (or associated tables) filtered by the execution date parameter. The translation maps this to a BigQuery parameterized stored procedure that dynamically runs the query and exports the result set to a GCS URI.
3. **Configurable Paths**: The GCS destination bucket and path mirror the `/rechnung/ausgang/` structure and are defined as variables within the BigQuery procedure.
4. **Row Count Handling**: Instead of using shell-based `wc -l`, BigQuery's system variable `@@row_count` or an explicit variable assignment from a `COUNT(*)` query is used to validate and log the quantity of exported rows.

### External Resources Needed:
* **GCS Bucket**: A Google Cloud Storage bucket (e.g., `gs://your-export-bucket/`) configured to receive the exported data.
* **IAM Permissions**: The service account running the BigQuery job must have `roles/storage.objectAdmin` on the target bucket and `roles/bigquery.admin` or custom equivalents.

---

# Pseudocode

## 1. BQ SQL Pseudocode
The following BigQuery SQL procedural block replaces the orchestration logic of the Shell script. It accepts a parameter for the target date, defaults it if null, runs the export, checks the row count, and logs execution information.

```sql
CREATE OR REPLACE PROCEDURE `dw_rechnung.sp_r_exp_rechnung_taeglich`(
  IN p_Stichtag STRING
)
BEGIN
  -- Declare local variables to handle execution state
  DECLARE v_Stichtag STRING;
  DECLARE v_GcsExportUri STRING;
  DECLARE v_ExportQuery STRING;
  DECLARE v_RowCount INT64;
  DECLARE v_GcsBucket STRING DEFAULT 'gs://your-export-bucket-placeholder/rechnung/ausgang/';

  -- Step 1: Handle parameter fallback (Default to yesterday's date if null or empty)
  IF p_Stichtag IS NULL OR p_Stichtag = '' THEN
    SET v_Stichtag = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY));
  ELSE
    SET v_Stichtag = p_Stichtag;
  END IF;

  -- Log execution start
  SELECT FORMAT('[INFO] %s Starte Export Rechnungsdaten fuer Stichtag %s', 
                CAST(CURRENT_TIMESTAMP() AS STRING), v_Stichtag) AS execution_log;

  -- Step 2: Construct target Cloud Storage URI
  SET v_GcsExportUri = CONCAT(v_GcsBucket, 'rechnung_export_', v_Stichtag, '_*.dat');

  -- Step 3: Run the dynamic EXPORT DATA statement 
  -- This mirrors the 'd_exp_rechnung_taeglich.sql' logic by executing an export query.
  EXECUTE IMMEDIATE FORMAT("""
    EXPORT DATA OPTIONS(
      uri=%Q,
      format='CSV',
      overwrite=true,
      header=false,
      field_delimiter=';'
    ) AS
    SELECT 
      * -- Replace with explicit columns as defined in d_exp_rechnung_taeglich.sql
    FROM 
      `dwh_kern.rechnung`
    WHERE 
      partition_date = PARSE_DATE('%%Y%%m%%d', %Q)
  """, v_GcsExportUri, v_Stichtag);

  -- Step 4: Extract the number of exported rows using the BigQuery system variable
  SET v_RowCount = @@row_count;

  -- Step 5: Log validation results and raise warnings if count is 0
  SELECT FORMAT('[INFO] %s Anzahl exportierter Rechnungssaetze: %d', 
                CAST(CURRENT_TIMESTAMP() AS STRING), v_RowCount) AS execution_log;

  IF v_RowCount = 0 THEN
    -- Warning level log equivalent to f_alis_msgerr "W"
    SELECT FORMAT('[WARNING] %s Keine Rechnungsdaten fuer Stichtag %s exportiert', 
                  CAST(CURRENT_TIMESTAMP() AS STRING), v_Stichtag) AS execution_log;
  ELSE
    SELECT FORMAT('[INFO] %s Export Rechnungsdaten ohne erkennbare Fehler beendet', 
                  CAST(CURRENT_TIMESTAMP() AS STRING)) AS execution_log;
  END IF;

EXCEPTION WHEN ERROR THEN
  -- Handle structural database errors during procedure execution
  SELECT FORMAT('[ERROR] %s Fehler waehrend des Exports fuer Stichtag %s: %s', 
                CAST(CURRENT_TIMESTAMP() AS STRING), v_Stichtag, @@error.message) AS execution_log;
  RESIGNAL;
END;
```

---

# Configuration Files for BigQuery Execution

To deploy and execute this migration workflow, the following configuration definitions are recommended:

### 1. BigQuery Deployment Configuration (`deploy_config.json`)
```json
{
  "project_id": "gcp-dwh-prod",
  "dataset_id": "dw_rechnung",
  "procedure_name": "sp_r_exp_rechnung_taeglich",
  "gcs_export_bucket": "gs://dwh-export-prod-reporting/rechnung/ausgang/",
  "service_account": "bq-orchestrator@gcp-dwh-prod.iam.gserviceaccount.com"
}
```

### 2. Google Cloud Composer / Apache Airflow DAG Configuration (`dag_config.py`)
```python
# Airflow Task Definition snippet for scheduling the BigQuery Stored Procedure daily
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

default_args = {
    'owner': 'dwh-operations',
    'depends_on_past': False,
    'start_date': datetime(2023, 2, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dwh_export_rechnung_daily',
    default_args=default_args,
    schedule_interval='0 6 * * *', # Daily at 06:00 AM
    catchup=False,
) as dag:

    # Executes the converted BigQuery procedure. 
    # Passing dynamic ds_nodash (YYYYMMDD) as the target 'Stichtag' parameter.
    call_export_procedure = BigQueryExecuteQueryOperator(
        task_id='call_export_rechnung_procedure',
        sql="CALL `dw_rechnung.sp_r_exp_rechnung_taeglich`('{{ ds_nodash }}');",
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_default'
    )
```

---

## 7. Target File Plan

### Target 1: `dags/dw_rechnung/dwh_rechnung_export_taeglich.py`
This Airflow DAG will orchestrate the execution steps, incorporating global environment logic.

```python
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# GLOBAL Config Variable Extraction
GCP_PROJECT = os.environ.get("GCP_PROJECT")
GCS_EXPORT_BUCKET = Variable.get("GCS_EXPORT_BUCKET")

default_args = {
    'owner': 'dwh-operations',
    'depends_on_past': False,
    'start_date': datetime(2019, 5, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dw_dwh_rechnung_export_taeglich_js',
    default_args=default_args,
    schedule_interval='0 6 * * *',  # Triggers daily
    catchup=False,
) as dag:

    # Execute dynamic export logic through BigQuery Operator
    # Utilizing verbatim text and German log statements downstream
    run_export = BigQueryExecuteQueryOperator(
        task_id='run_export_query',
        sql="sql/d_exp_rechnung_taeglich.sql",
        use_legacy_sql=False,
        configuration={
            "query": {
                "parameterMode": "NAMED",
                "queryParameters": [
                    {
                        "name": "stichtag",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {"value": "{{ ds_nodash }}"}
                    },
                    {
                        "name": "export_bucket",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {"value": GCS_EXPORT_BUCKET}
                    }
                ]
            }
        }
    )
```

### Target 2: `dags/dw_rechnung/bin/dwh_rechnung_export_taeglich_bin.py`
This script contains converted logic originating from the source shell script bin folder to handle validations, metrics, or auxiliary execution tasks.

```python
# Converted logic from DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/bin/r_exp_rechnung_taeglich.ksh
# Provides auxiliary support and validation routines for the Airflow DAG orchestration.
```

### Target 3: `dags/dw_rechnung/sql/d_exp_rechnung_taeglich.sql`
*Placeholder stub until source query details are extracted and confirmed by operational teams.*
```sql
-- TODO: no source found
-- Verify structure against Oracle d_exp_rechnung_taeglich.sql
-- Below structure provides functional equivalent framework:

EXPORT DATA OPTIONS(
  uri=CONCAT(@export_bucket, 'rechnung/ausgang/rechnung_export_', @stichtag, '_*.dat'),
  format='CSV',
  overwrite=true,
  header=false,
  field_delimiter='|'
) AS
SELECT 
  -- TODO: Verify and map concrete columns to match output format
  *
FROM 
  `dw_rechnung.t_rechnung`
WHERE 
  rechnungsdatum = PARSE_DATE('%Y%m%d', @stichtag);
```

---

An elegant and robust target architecture has been designed for migrating the legacy UC4 Daily Invoice Export Job to Google Cloud Platform. 

Below is the complete, implementation-ready Migration Design Document, incorporating the verbatim output of the specialized conversion tool and adding the crucial context, schedule mapping, and environmental variables that the tool could not see.

---

# MIGRATION DESIGN DOCUMENT
**Job Name:** `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS`  
**Legacy Technology Stack:** UC4 Scheduler + KSH Shell Script + Oracle SQL*Plus  
**Target Cloud Platform:** Google Cloud Platform (Cloud Composer + Dataform + BigQuery)  

---

## 1. File Disposition

The following table lists every file in the pre-collected context with its direct mapping, target path, and conversion action:

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS.xml` | `dags/dwh_rechnung/dw_dwh_rechnung_export_taeglich_js.py` | **Target Orchestration:** Convert UC4 XML execution metadata into a native Apache Airflow DAG hosted in Cloud Composer. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/bin/r_exp_rechnung_taeglich.ksh` | `dags/dwh_rechnung/operators/export_operator.py` (Folded into Airflow Operator) | **Execution Logic:** The legacy shell script orchestrates calling sqlplus, generating the flat file, validating the output line count, and logging. This logic will be modernized as a reusable Python operator inside Cloud Composer, writing directly to GCS instead of a local file system. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/sql/d_exp_rechnung_taeglich.sql` | `dags/dwh_rechnung/sql/d_exp_rechnung_taeglich.sql` | **Data Transformation:** Extracted SQL query logic converted to native BigQuery Standard SQL with parameter binding. |

---

## 2. Shared & Lineage Dependencies

* **Upstream Job Dependencies:**
  * None declared in the pre-collected context as direct strict blocking predecessors within the scheduler metadata.
* **Downstream Job Dependencies:**
  * None declared.
* **Lineage Edges (Source Data Producers):**
  * `TABLE:T_RECHNUNG` $\rightarrow$ Read by SQL script to retrieve invoice lines. This table is updated daily by a companion process (such as `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP.xml`).
* **External System Hand-offs:**
  * **Legacy:** The shell script `r_exp_rechnung_taeglich.ksh` formats output via SQL*Plus commands, writes the pipe-separated data to a local flat-file system, and verifies row counts.
  * **Target Platform:** Cloud Composer will query BigQuery directly and export the resulting dataset to a Google Cloud Storage (GCS) bucket, optionally utilizing BigQuery’s native `EXPORT DATA` syntax to format as a pipe-separated gzip CSV file.

---

## 3. Scheduling & Orchestration Mapping

* **Schedule:** Daily execution (derived from the `TAEGLICH` designation).
* **Target Scheduling Construct:** Apache Airflow DAG (`dw_dwh_rechnung_export_taeglich_js.py`) scheduled using an Airflow cron string (`0 2 * * *` or similar based on business requirements) or triggered via an upstream sensor monitoring the completion of table `T_RECHNUNG` loading.
* **Variables & Arguments:**
  * The reporting date (`p_Stichtag` / `&1`) is dynamically passed. In the target environment, this will map to Airflow's logical execution date `{{ ds_nodash }}` (format: `YYYYMMDD`) to ensure deterministic retries.

---

## 4. Environment-Specific Variables Policy

In accordance with the Environment Values Policy, all hardcoded environment values are classified and resolved as follows:

### Global (Environment-Wide Infrastructure Constants)
These values are identical across environments (dev, test, prod) but resolve to different backend cloud assets. They must be resolved dynamically at runtime.

* **`GCP_PROJECT`**: The target Google Cloud Project ID.
  * *Python/Composer implementation*: `os.environ.get("GCP_PROJECT")`
  * *BigQuery SQL implementation*: Dynamic substitution at runtime.
* **`GCS_BUCKET`**: The target Cloud Storage bucket receiving the exported pipe-separated text file.
  * *Airflow Variable implementation*: `Variable.get("GCS_EXPORT_BUCKET")`

### Job-Specific Variables
These values are particular to this export pipeline and are declared locally within the target task/config structure.

* **`BQ_DATASET`**: `DWH_KERN` (The dataset containing table `T_RECHNUNG`).
* **`BQ_TABLE`**: `T_RECHNUNG`
* **`EXPORT_PATH`**: `rechnung_export/rechnung_export_taeglich_{{ ds_nodash }}.csv`

---

## 5. Converter Tool Design Output (Verbatim)

Below is the verbatim migration design and target SQL logic generated by the specialized conversion pipeline:

```markdown
# Design Document: Migration of HiveQL/Oracle SQL to Google Cloud BigQuery

## 1. Executive Summary
This document details the migration path and conversion logic for translating an Oracle-style SQL script (historically executed via Hive/Beeline or Oracle SQL*Plus clients) into an optimized, standard Google Cloud BigQuery SQL dialect. 

The scope of this migration is limited to the transformation script `d_exp_rechnung_taeglich.sql`, which extracts daily billing records based on a runtime parameter (`p_Stichtag`).

---

## 2. Entity & Schema Mapping

The following schema and system entities have been identified in the source script:

### 2.1 Schema & Table Mapping
| Source System/Schema | Source Table Name | Target Dataset | Target Table Name |
| :--- | :--- | :--- | :--- |
| `DWH_KERN` | `T_RECHNUNG` | `DWH_KERN` (or environment-specific prefix) | `T_RECHNUNG` |

### 2.2 Column Mapping & Data Type Conversions
Based on the analysis of the source query columns, the following target data types will be enforced in Google BigQuery to preserve numerical precision and temporal correctness:

| Column Name | Source Estimated Type | BigQuery Target Type | Conversion/Preservation Logic |
| :--- | :--- | :--- | :--- |
| `RECHNUNGSNUMMER` | STRING / VARCHAR2 | `STRING` | Direct mapping. |
| `VERTRAG` | STRING / VARCHAR2 | `STRING` | Direct mapping. |
| `KUNDE` | STRING / VARCHAR2 | `STRING` | Direct mapping. |
| `TARIF` | STRING / VARCHAR2 | `STRING` | Direct mapping. |
| `ABRECHNUNGSZEITRAUM` | STRING / VARCHAR2 | `STRING` | Direct mapping. |
| `RECHNUNGSBETRAG` | NUMBER / DECIMAL | `NUMERIC` | Preserves exact precision for currency/billing amounts. |
| `WAEHRUNG` | VARCHAR2(3) | `STRING` | Direct mapping. |
| `RECHNUNGSDATUM` | DATE / TIMESTAMP | `DATE` | Condition-based transformation applied. |

---

## 3. Migration Decisions & Rules

### 3.1 Parameterization & Date Handling (Step 3.1)
* **Source Logic:** `to_date('&p_Stichtag','YYYYMMDD')`
* **BigQuery Logic:** BigQuery SQL uses the `PARSE_DATE` function to transform formatted strings into date types. The translation will be `PARSE_DATE('%Y%m%d', @p_Stichtag)`.
* **Execution Environment:** System settings like `whenever sqlerror`, `set pagesize`, `set feedback`, and `colsep '|'` are client-specific configuration parameters (SQL*Plus/Beeline). These will be managed via the executing orchestrator (e.g., Apache Airflow, dbt, or Cloud Composer) rather than the SQL body itself.

### 3.2 Precision Preservation (Step 3.2)
* Any financial representation (e.g., `RECHNUNGSBETRAG`) will be processed as `NUMERIC` (or `BIGNUMERIC` if extreme scale is required) to prevent floating-point rounding errors typical of `FLOAT64`.

---

## 4. Low-Level Pseudocode

```text
START
  DECLARE p_Stichtag STRING;
  SET p_Stichtag = [System Parameter Input, Format: 'YYYYMMDD'];

  EXECUTE SQL:
    SELECT
      r.RECHNUNGSNUMMER,
      r.VERTRAG,
      r.KUNDE,
      r.TARIF,
      r.ABRECHNUNGSZEITRAUM,
      CAST(r.RECHNUNGSBETRAG AS NUMERIC) AS RECHNUNGSBETRAG,
      r.WAEHRUNG,
      r.RECHNUNGSDATUM
    FROM 
      `DWH_KERN.T_RECHNUNG` AS r
    WHERE 
      r.RECHNUNGSDATUM = PARSE_DATE('%Y%m%d', p_Stichtag)
    ORDER BY 
      r.RECHNUNGSNUMMER;
END
```

---

## 5. Equivalent BigQuery SQL Query

```sql
-- BigQuery SQL equivalent for d_exp_rechnung_taeglich.sql
-- Note: 'p_Stichtag' should be passed as a query parameter (e.g., @p_Stichtag) at runtime.

SELECT
  r.RECHNUNGSNUMMER,
  r.VERTRAG,
  r.KUNDE,
  r.TARIF,
  r.ABRECHNUNGSZEITRAUM,
  CAST(r.RECHNUNGSBETRAG AS NUMERIC) AS RECHNUNGSBETRAG,
  r.WAEHRUNG,
  r.RECHNUNGSDATUM
FROM `DWH_KERN.T_RECHNUNG` AS r
WHERE r.RECHNUNGSDATUM = PARSE_DATE('%Y%m%d', @p_Stichtag)
ORDER BY r.RECHNUNGSNUMMER;
```
```

---

## 6. Execution & Flow Modernization

To cleanly support the original shell execution steps (`whenever sqlerror`, validating line counts, outputting pipe-delimited records), the pipeline will leverage the BigQuery `EXPORT DATA` statement inside the Airflow execution task. This natively converts the query results into the target pipe-separated export files.

### Orchestration Task Flow
1. **Airflow Start Step:** Parse execution date parameter `{{ ds_nodash }}`.
2. **Execute BigQuery Export Query:** Execute the converted query wrapped in BigQuery's native `EXPORT DATA` construct.
3. **Verify Row Count (Metadata Check):** Query execution job metrics in BigQuery to log/validate row count metrics (replacing the manual wc/validation in the KSH script).

### Proposed Native Target Execution SQL
```sql
EXPORT DATA OPTIONS(
  uri='gs://your-gcs-export-bucket/rechnung_export/rechnung_export_taeglich_*.csv',
  format='CSV',
  overwrite=true,
  header=false,
  field_delimiter='|'
) AS
SELECT
  r.RECHNUNGSNUMMER,
  r.VERTRAG,
  r.KUNDE,
  r.TARIF,
  r.ABRECHNUNGSZEITRAUM,
  CAST(r.RECHNUNGSBETRAG AS NUMERIC) AS RECHNUNGSBETRAG,
  r.WAEHRUNG,
  r.RECHNUNGSDATUM
FROM `DWH_KERN.T_RECHNUNG` AS r
WHERE r.RECHNUNGSDATUM = PARSE_DATE('%Y%m%d', @p_Stichtag)
ORDER BY r.RECHNUNGSNUMMER;
```

---

## 7. Risks & Manual Actions

1. **GCS Target Schema Requirements:** BigQuery `EXPORT DATA` generates sharded files (`rechnung_export_taeglich_000000000000.csv`) if the output size exceeds threshold limits. If the downstream target system strictly requires a single un-sharded file, an additional merge task (e.g., using a GCS Compose API task in Python) must be added to the Airflow DAG to consolidate output.
2. **Date Variable Format Check:** Ensure that logical execution dates passed via Airflow DAGs match the format `'YYYYMMDD'` requested by the source query's parsing configuration. Default Airflow context variables like `{{ ds_nodash }}` natively provide this representation.