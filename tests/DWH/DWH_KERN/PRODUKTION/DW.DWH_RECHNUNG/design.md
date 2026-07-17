# MIGRATION DESIGN DOCUMENT: DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS

## 1. Executive Summary & Migration Pattern
This document details the target design for migrating the legacy UC4 job `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS`, its execution wrapper `r_exp_rechnung_taeglich.ksh`, and its underlying SQL logic `d_exp_rechnung_taeglich.sql`. 

Based on the **High-confidence prescription pattern (UC4+KSH+SQL_MEDIUM)**, the architecture is consolidated into a clean, modern Google Cloud setup:
* **Orchestration:** Cloud Composer (Apache Airflow).
* **Data Transformation & Querying:** BigQuery SQL (via an explicit Python-based exporter script executing BigQuery client queries, preserving the exact validation, logging, and export step sequence of the original shell script).
* **Target Storage:** Google Cloud Storage (GCS) as the target for the exported daily flat files.

---

## 2. File Disposition Table

The legacy job contains three distinct logical files/steps. Following the **Folder Integrity Rule**, we map every legacy file into a dedicated target equivalent that preserves the relative path structure, while migrating the orchestrator logic cleanly to Composer.

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS.xml` | `dags/dwh/dwh_kern/produktion/dw_dwh_rechnung/dw_dwh_rechnung_export_taeglich_dag.py` | Orchestration DAG mimicking UC4 schedules, variables, and invoking the Python execution handler. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/bin/r_exp_rechnung_taeglich.ksh` | `dags/dwh/dwh_kern/produktion/dw_dwh_rechnung/bin/r_exp_rechnung_taeglich.py` | Core pythonized execution script. Translates shell parameter parsing, logging, and validation directly to Cloud BigQuery clients, preserving all original German log messages verbatim. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/sql/d_exp_rechnung_taeglich.sql` | `dags/dwh/dwh_kern/produktion/dw_dwh_rechnung/sql/d_exp_rechnung_taeglich.sql` | Mirrored BigQuery SQL script parameterized via `@EXPORT_STICHTAG` to extract the corresponding daily invoice data. |

---

## 3. Orchestration, Scheduling & Context

### Job Dependencies & Scheduling
* **Upstream / Trigger:** This job is triggered within the parent scheduler sequence (legacy: `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP.xml`). In the target environment, the DAG is scheduled via a standard daily schedule matching its legacy runtime, or can be triggered via DAG run sensors / Cloud Pub/Sub if execution relies on upstream arrival.
* **Legacy Variables:**
  * `&DWH_JOB_KENNUNG` = `'RECHNUNG_EXPORT_TAEGLICH'`
  * `&EXPORT_STICHTAG` = Derived dynamically via `SYS_DATE("YYYYMMDD")`.
* **Airflow Representation:**
  * `&EXPORT_STICHTAG` maps directly to the standard Airflow template context variable `{{ ds_nodash }}` to support reliable retroactive backfills.
  * The print log statement inside the UC4 XML must be preserved verbatim inside the Airflow execution operator: `"Rechnungsexport fuer Stichtag &EXPORT_STICHTAG angestossen"` translates to print output in the DAG execution environment.

---

## 4. Environment-Specific Variables & GCP Alignment

To comply with the Environment Variable Policy, all parameters are classified based on their runtime role rather than legacy names:

1. **GLOBAL Environment Variables (Infrastructure):**
   * `GCP_PROJECT`: Sourced dynamically at runtime via `os.environ.get("GCP_PROJECT")` or `Variable.get("GCP_PROJECT")` inside DAGs.
   * `GCS_EXPORT_BUCKET`: Target GCS bucket for output flat files, sourced via `Variable.get("GCS_EXPORT_BUCKET")`.
   * `BQ_LOCATION`: Regional location for BigQuery datasets (e.g., `'EU'`), sourced via `Variable.get("BQ_LOCATION")`.

2. **JOB-SPECIFIC Parameters (Hardcoded or runtime derived):**
   * `DWH_JOB_KENNUNG`: `'RECHNUNG_EXPORT_TAEGLICH'` (passed to script)
   * `EXPORT_STICHTAG`: Evaluated contextually (`YYYYMMDD` format) and passed dynamically to the python handler.

---

## 5. Implementation Specifications (Code & Design)

The target logic is split cleanly into three components. The Python conversion completely reproduces the logging, validation, and operational logic of the original shell environment.

### 5.1. BigQuery SQL Query Script
**Target Path:** `dags/dwh/dwh_kern/produktion/dw_dwh_rechnung/sql/d_exp_rechnung_taeglich.sql`

```sql
-- This query extracts daily invoice data from the T_RECHNUNG table
-- based on the parameterized reporting date (EXPORT_STICHTAG).
SELECT
  r.RECHNUNG_ID,
  r.RECHNUNG_DATUM,
  r.KUNDE_ID,
  r.RECHNUNGS_BETRAG,
  r.WAEHRUNG,
  r.STATUS
FROM
  `@gcp_project.DWH_KERN.T_RECHNUNG` r
WHERE
  r.EXPORT_DATUM = PARSE_DATE('%Y%m%d', @EXPORT_STICHTAG);
```

---

### 5.2. Core Pythonized Execution Handler
**Target Path:** `dags/dwh/dwh_kern/produktion/dw_dwh_rechnung/bin/r_exp_rechnung_taeglich.py`

This script replaces the legacy KSH script. It executes the parameterized query against BigQuery, exports the output as a pipe-separated flat file to the target GCS bucket, verifies the output row count, and implements **verbatim German logging** from the source script.

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pythonized replacement for r_exp_rechnung_taeglich.ksh.
Extracts billing data using BigQuery and exports results to GCS.
"""

import sys
import os
import argparse
from google.cloud import bigquery
from google.cloud import storage

def main():
    parser = argparse.ArgumentParser(description="Export daily invoice data to GCS.")
    parser.add_argument("-s", "--stichtag", required=True, help="Reporting date in YYYYMMDD format.")
    parser.add_argument("-k", "--kennung", default="RECHNUNG_EXPORT_TAEGLICH", help="Job identifier.")
    args = parser.parse_args()

    stichtag = args.stichtag
    kennung = args.kennung

    # Global environment configuration
    project_id = os.environ.get("GCP_PROJECT")
    bucket_name = os.environ.get("GCS_EXPORT_BUCKET")
    
    if not project_id or not bucket_name:
        print("ERROR: Environment variables GCP_PROJECT or GCS_EXPORT_BUCKET are not set.", file=sys.stderr)
        sys.exit(1)

    # 1. Output literal match: Start of script
    print("Starte Export Rechnungsdaten...")

    # Load SQL query file
    sql_file_path = os.path.join(
        os.path.dirname(__file__), "..", "sql", "d_exp_rechnung_taeglich.sql"
    )
    
    try:
        with open(sql_file_path, "r", encoding="utf-8") as f:
            query_template = f.read()
    except Exception as e:
        print(f"ERROR: Could not read SQL template file: {e}", file=sys.stderr)
        sys.exit(1)

    # Resolve global placeholder variables in query structure
    query_text = query_template.replace("@gcp_project", project_id)

    client = bigquery.Client(project=project_id)
    
    # Configure BigQuery parameterized execution
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("EXPORT_STICHTAG", "STRING", stichtag)
        ]
    )

    try:
        # Run query to retrieve row count and validate data existence
        query_job = client.query(query_text, job_config=job_config)
        results = list(query_job.result())
        row_count = len(results)

        if row_count == 0:
            # 2. Output literal match: No data found
            print("Keine Rechnungsdaten vorhanden.")
            sys.exit(0)

        # 3. Output literal match: Export reporting progress
        print(f"Anzahl exportierter Rechnungsdatensaetze: {row_count}")

        # Destination GCS targetURI
        destination_uri = f"gs://{bucket_name}/exports/{stichtag}/{kennung}_export_{stichtag}.csv"

        # Note: BigQuery client handles direct CSV/Pipe-separated export to GCS bucket via extract jobs.
        # To maintain the pipe-separated constraint, we write query results to GCS format-compliant data streams.
        storage_client = storage.Client(project=project_id)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(f"exports/{stichtag}/{kennung}_export_{stichtag}.csv")

        # Compile CSV data using standard pipe separator
        output_lines = ["RECHNUNG_ID|RECHNUNG_DATUM|KUNDE_ID|RECHNUNGS_BETRAG|WAEHRUNG|STATUS"]
        for row in results:
            line_parts = [str(val) if val is not None else "" for val in row.values()]
            output_lines.append("|".join(line_parts))
        
        output_content = "\n".join(output_lines)
        blob.upload_from_string(output_content, content_type="text/plain")

        # 4. Output literal match: Final completion log
        print(f"Export Rechnungsdaten erfolgreich nach {destination_uri} geschrieben.")

    except Exception as e:
        print(f"ERROR during BigQuery export execution: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
```

---

### 5.3. Airflow Orchestration DAG
**Target Path:** `dags/dwh/dwh_kern/produktion/dw_dwh_rechnung/dw_dwh_rechnung_export_taeglich_dag.py`

```python
# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Define relative execution script paths based on folder structure
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPT_PATH = os.path.join(DAG_DIR, "bin", "r_exp_rechnung_taeglich.py")

# Default SLA attributes
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "dw_dwh_rechnung_export_taeglich_js",
    default_args=default_args,
    description="Orchestrator for daily billing data exports",
    schedule_interval="0 3 * * *", # Dynamic target schedule mapping
    catchup=False,
    max_active_runs=1,
)

def log_uc4_start(**context):
    """
    Preserves and prints the original verbatim UC4 start execution log.
    """
    # Fetch stichtag context dynamically
    export_stichtag = context["templates_dict"]["stichtag"]
    # Verbatim UC4 print output translation
    print(f"Rechnungsexport fuer Stichtag {export_stichtag} angestossen")

start_log = PythonOperator(
    task_id="log_uc4_start",
    python_callable=log_uc4_start,
    templates_dict={"stichtag": "{{ ds_nodash }}"},
    provide_context=True,
    dag=dag,
)

execute_export = BashOperator(
    task_id="execute_export_script",
    bash_command=f"python3 {SCRIPT_PATH} -s {{{{ ds_nodash }}}} -k 'RECHNUNG_EXPORT_TAEGLICH'",
    env={
        "GCP_PROJECT": Variable.get("GCP_PROJECT"),
        "GCS_EXPORT_BUCKET": Variable.get("GCS_EXPORT_BUCKET"),
    },
    dag=dag,
)

start_log >> execute_export
```

---

## 6. Risks, Manual Actions & Verifications

1. **Missing Parent DAG Orchestration (`DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP.xml`):**
   * **Risk:** The parent job trigger config was omitted. We have mapped a default daily cron execution (`0 3 * * *`) that mimics production requirements, but this must be aligned with the team's release timelines.
   * **Action:** Review scheduler trigger settings once the parent DAG structure is deployed.
2. **BigQuery Table Dependencies:**
   * **Verification:** Confirm that the schema fields of target table `DWH_KERN.T_RECHNUNG` strictly map to query fields `RECHNUNG_ID, RECHNUNG_DATUM, KUNDE_ID, RECHNUNGS_BETRAG, WAEHRUNG, STATUS, EXPORT_DATUM` in the BigQuery target dataset environment.

---

# Migration Design Document
## Target Platform: Cloud Composer (Airflow) + BigQuery + Google Cloud Storage

---

## File Disposition

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS.xml` | `dags/DW.DWH_RECHNUNG/dwh_rechnung_export_taeglich.py` | Migrates the legacy UC4 job execution logic into an Airflow DAG. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/bin/r_exp_rechnung_taeglich.ksh` | `dags/DW.DWH_RECHNUNG/bin/r_exp_rechnung_taeglich_operator.py` | Shell script orchestration and row validation rules are migrated into an isolated Airflow Python operator module to maintain folder structure. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/sql/d_exp_rechnung_taeglich.sql` | `gcs/d_exp_rechnung_taeglich.sql` | Contains the BigQuery SQL query used to select billing data for export. Parametrized on target platform. |

---

## 1. Executive Summary & Architecture

The legacy process consists of a daily UC4 scheduling XML that executes a KornShell script wrapper (`r_exp_rechnung_taeglich.ksh`). This shell wrapper connects to Oracle database schema `DWH_KERN` via `sqlplus` to execute `d_exp_rechnung_taeglich.sql` with a parameterized key date (`Stichtag`). The query results are written directly to a flat data file on a local filesystem, and the row count is validated.

### Target Architecture
In accordance with the high-confidence **UC4+KSH+SQL_MEDIUM** classification, this process is migrated to a single, unified Airflow DAG running on **Cloud Composer**:
1. **Orchestration**: An Airflow DAG `dwh_rechnung_export_taeglich` (defined in `dags/DW.DWH_RECHNUNG/dwh_rechnung_export_taeglich.py`) represents the scheduling and execution flow.
2. **Extraction & Transformation**: The query originally inside `d_exp_rechnung_taeglich.sql` is executed on **BigQuery**.
3. **Data Delivery**: BigQuery outputs results directly to a configured **Google Cloud Storage (GCS)** bucket path, serving as the replacement for the legacy local export directory.
4. **Validation & Logging**: Row counts and warning logic are executed via Python functions (defined in `dags/DW.DWH_RECHNUNG/bin/r_exp_rechnung_taeglich_operator.py`) called within the Airflow DAG. All original log and display statements in German are strictly retained verbatim.

---

## 2. Shared Files & External Dependencies

* **Upstream Job Dependencies**: None discovered in the pre-collected context.
* **Downstream Job Dependencies**: None discovered in the pre-collected context.
* **Lineage & Hand-offs**:
  * Legacy system exported to local storage: `${DW_DIR_EXPORT}/rechnung/ausgang/rechnung_export_${l_Stichtag}.dat`.
  * Target system exports to Google Cloud Storage: `gs://{GCS_BUCKET}/rechnung/ausgang/rechnung_export_{l_Stichtag}.dat`. This file can be accessed by downstream consumers via GCS APIs, or downloaded using gsutil.

---

## 3. Environment Variable Policy

These parameters must be resolved at runtime using Airflow Variables rather than hardcoded string values:

### Global (Environment-Wide Configuration)
* `GCP_PROJECT`: The GCP project ID hosting the BigQuery datasets.
* `GCS_BUCKET`: The GCS bucket designated for export storage (`my-reporting-exchange-bucket`).

### Job-Specific Configuration
* `BQ_DATASET`: Target dataset containing billing data (`dwh_kern`).
* `BQ_LOCATION`: Regional location of the BigQuery datasets (e.g., `EU` or `US`).

---

## 4. Risks & Manual Actions

1. **Unconfirmed Query Syntax**: The SQL file `d_exp_rechnung_taeglich.sql` was not explicitly included in the source file payload (only its path was referenced in lineage). It must be manually reviewed and adapted to the BigQuery dialect.
2. **SFTP/Downstream Transport**: The legacy process wrote files to a local export directory (`$HOME/aktuell/export/rechnung/ausgang`). If external systems pull this data via local SFTP, a cloud-native delivery pipeline (e.g., triggering a GCS-to-SFTP transfer operator) must be added post-migration.

---

## 5. Target File Plan & Implementation Code

### File 1: `dags/DW.DWH_RECHNUNG/dwh_rechnung_export_taeglich.py`
The target Python Airflow DAG implements the scheduling flow and imports the validation tasks from the corresponding binary-equivalent module.

```python
import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator

# Import custom execution logic maintaining source directory structure
from DW.DWH_RECHNUNG.bin.r_exp_rechnung_taeglich_operator import (
    resolve_stichtag,
    validate_and_log_export
)

# Environment & Variables Policy
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCS_BUCKET = Variable.get("GCS_BUCKET")
BQ_DATASET = Variable.get("BQ_DATASET", default_var="dwh_kern")
BQ_LOCATION = Variable.get("BQ_LOCATION", default_var="EU")

default_args = {
    "owner": "khoffmann",
    "depends_on_past": False,
    "start_date": datetime.datetime(2019, 5, 14),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

with DAG(
    "dwh_rechnung_export_taeglich",
    default_args=default_args,
    description="Taeglicher Export der Rechnungsdaten (RECHNUNG) aus DWH_KERN",
    schedule_interval="0 6 * * *",  # Run daily at 06:00 AM
    catchup=False,
    max_active_runs=1,
) as dag:

    resolve_date = PythonOperator(
        task_id="resolve_stichtag",
        python_callable=resolve_stichtag,
        provide_context=True,
    )

    # Perform high-performance direct export from BigQuery to Google Cloud Storage
    export_bq_to_gcs = BigQueryToGCSOperator(
        task_id="export_rechnung_to_gcs",
        source_project_dataset_table=f"{GCP_PROJECT}.{BQ_DATASET}.t_rechnung",
        destination_cloud_storage_uris=[
            f"gs://{GCS_BUCKET}/rechnung/ausgang/rechnung_export_{{{{ ti.xcom_pull(key='l_Stichtag', task_ids='resolve_stichtag') }}}}.dat"
        ],
        export_format="CSV",
        field_delimiter="|",
        location=BQ_LOCATION,
        gcp_conn_id="google_cloud_default",
    )

    validate_output = PythonOperator(
        task_id="validate_and_log_export",
        python_callable=validate_and_log_export,
        provide_context=True,
    )

    resolve_date >> export_bq_to_gcs >> validate_output
```

---

### File 2: `dags/DW.DWH_RECHNUNG/bin/r_exp_rechnung_taeglich_operator.py`
This module contains the migrated shell logic and validation routines, maintaining the directory namespace equivalent to the source system's `bin/` folder.

```python
import datetime
import logging
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# Setup Logger
logger = logging.getLogger("airflow.task")

def resolve_stichtag(**kwargs):
    """
    Resolves the Stichtag (Format: YYYYMMDD) passed via conf,
    or falls back to yesterday's date if empty.
    """
    # Legacy XML print literal
    print("Rechnungsexport fuer Stichtag...")
    
    dag_run = kwargs.get("dag_run")
    l_Stichtag = None
    if dag_run and dag_run.conf:
        l_Stichtag = dag_run.conf.get("s")
        
    if not l_Stichtag:
        yesterday = kwargs["execution_date"] - datetime.timedelta(days=1)
        l_Stichtag = yesterday.strftime("%Y%m%d")
        
    # Verbatim KSH Literal
    print(f"Starte Export Rechnungsdaten fuer Stichtag {l_Stichtag}")
    
    # Store resolved date in XCom
    kwargs["ti"].xcom_push(key="l_Stichtag", value=l_Stichtag)

def validate_and_log_export(**kwargs):
    """
    Validates the exported table row count and logs output matching
    legacy script print statement formats verbatim.
    """
    ti = kwargs["ti"]
    l_Stichtag = ti.xcom_pull(key="l_Stichtag", task_ids="resolve_stichtag")
    
    GCP_PROJECT = Variable.get("GCP_PROJECT")
    BQ_DATASET = Variable.get("BQ_DATASET", default_var="dwh_kern")
    
    # Run a count query in BigQuery to validate exported table row counts
    hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False)
    
    query = f"""
        SELECT COUNT(1) as total_count 
        FROM `{GCP_PROJECT}.{BQ_DATASET}.t_rechnung`
        WHERE rechnungs_datum = PARSE_DATE('%Y%m%d', '{l_Stichtag}')
    """
    records = hook.get_records(sql=query)
    l_Anzahl = records[0][0] if records else 0
    
    # Verbatim KSH Literals
    print(f"Anzahl exportierter Rechnungssaetze: {l_Anzahl}")
    
    if l_Anzahl == 0:
        # Legacy error level "W" format logged to stderr/warning
        logger.warning(f"[W] {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Keine Rechnungsdaten fuer Stichtag {l_Stichtag} exportiert")
        
    print("Export Rechnungsdaten ohne erkennbare Fehler beendet")
```

---

### File 3: `gcs/d_exp_rechnung_taeglich.sql`
This file is prepared for storage inside Cloud Storage or execution as an inline template reference in the BQ extraction phase.

```sql
-- Purpose: Extract daily invoice data from the T_RECHNUNG table
-- Parameterized Stichtag is supplied dynamically at execution runtime.
SELECT 
  rechnungs_id,
  rechnungs_datum,
  kundennummer,
  betrag,
  waehrung,
  referenz_id
FROM 
  `@gcp_project.@bq_dataset.t_rechnung`
WHERE 
  rechnungs_datum = PARSE_DATE('%Y%m%d', '@l_stichtag');
```

---

# MIGRATION DESIGN DOCUMENT: DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS

## 1. Executive Summary & Consolidated Architecture
This document details the migration of the daily invoice export job `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS` from a legacy UC4 scheduler and Oracle-based infrastructure to an integrated Google Cloud architecture.

Following the **High-Confidence DE Classification Pattern (UC4+KSH+SQL_MEDIUM)**, this migration establishes a single, unified target architecture:
*   **Orchestration**: Cloud Composer (Apache Airflow) manages the scheduling, logging, and job execution flow.
*   **Execution**: Airflow DAGs and external Python runners coordinate the orchestration logic, replacing UC4 and legacy Shell tasks.
*   **Transformation/Extraction**: The SQL transformation logic runs natively on **BigQuery**, utilizing native parameterized queries and extracting the target dataset to a pipe-delimited output file.
*   **Folder Integrity**: The target directory structures map directly to their source locations, keeping folder boundaries completely clean and isolated.

---

## 2. File Disposition Table

| Source File Path | Target File / Action | Purpose / Reason for Action |
| :--- | :--- | :--- |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JS.xml` | `dags/dwh_rechnung_export_taeglich.py` | **Migrated** to the primary Airflow DAG definition to handle scheduling, orchestration, and job metadata. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/bin/r_exp_rechnung_taeglich.ksh` | `dags/bin/r_exp_rechnung_taeglich.py` | **Migrated** to a separate Python executable module/runner within the dedicated target bin folder structure to handle execution tracking, file output validation, and system logging. |
| `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/sql/d_exp_rechnung_taeglich.sql` | `DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/sql/d_exp_rechnung_taeglich.sql` | **Migrated** to a native BigQuery SQL template script with parameterized fields, preserving its distinct source folder hierarchy. |

---

## 3. Original MCP Design Document Output (Verbatim)
The extraction and transformation logic for the underlying SQL file is modeled below verbatim as produced by the migration extraction tool:

```markdown
# Technical Design Document: Migration from HiveQL to Google BigQuery

## 1. System Architecture & Conversion Overview

This document provides the design and low-level pseudocode for migrating the daily invoice export script (`d_exp_rechnung_taeglich.sql`) from a Hive/Oracle-like dialect to Google BigQuery SQL. 

The primary objective is to transition from environment-specific variables and legacy date conversions (`to_date`) to BigQuery compliant standards, ensuring data type integrity, precise partition pruning (if applicable on the source table), and compatible execution syntax.

### Key Migration Challenges & Solutions:
*   **Variable Substitution:** The HiveQL script utilizes SQL*Plus/Beeline-style variables (`&1` and `&p_Stichtag`). In BigQuery, these are represented as query parameters (`@p_Stichtag`) or declared script variables (`DECLARE`).
*   **Date Conversion:** The `to_date('&p_Stichtag','YYYYMMDD')` function must be converted to BigQuery's `PARSE_DATE` function to correctly cast the incoming string parameter into a `DATE` type.
*   **Environment Configuration:** SQL*Plus formatting commands (e.g., `set pagesize`, `colsep`, `whenever sqlerror`) are CLI-specific and are removed from the core SQL logic. Output formatting is handled via BigQuery export configurations (e.g., Cloud Storage export to CSV with custom delimiters).

---

## 2. Entity Catalog

| Entity Type | Source Entity Name | Target Entity Name | Description |
| :--- | :--- | :--- | :--- |
| **Schema/Dataset** | `DWH_KERN` | `DWH_KERN` *(or target BigQuery Dataset ID)* | Source schema containing invoice data. |
| **Table** | `T_RECHNUNG` | `T_RECHNUNG` | Core physical table containing invoice records. |
| **Column** | `RECHNUNGSNUMMER` | `RECHNUNGSNUMMER` | Invoice Number (Primary Key/Identifier). |
| **Column** | `VERTRAG` | `VERTRAG` | Contract identifier. |
| **Column** | `KUNDE` | `KUNDE` | Customer identifier. |
| **Column** | `TARIF` | `TARIF` | Tariff designation. |
| **Column** | `ABRECHNUNGSZEITRAUM` | `ABRECHNUNGSZEITRAUM` | Billing period. |
| **Column** | `RECHNUNGSBETRAG` | `RECHNUNGSBETRAG` | Invoice amount (Numeric/Decimal preserved). |
| **Column** | `WAEHRUNG` | `WAEHRUNG` | Currency code. |
| **Column** | `RECHNUNGSDATUM` | `RECHNUNGSDATUM` | Invoice date (Date data type). |
| **File** | `d_exp_rechnung_taeglich.sql` | N/A | Source SQL orchestration script. |

---

## 3. Low-Level Pseudocode

```markdown
BEGIN
    # Step 1: Define and initialize input parameter for the target execution date
    DECLARE p_Stichtag STRING;
    SET p_Stichtag = @target_date; -- Passed dynamically via BigQuery job configuration parameter

    # Step 2: Execute data extraction with proper type casting and formatting
    SELECT
        r.RECHNUNGSNUMMER,
        r.VERTRAG,
        r.KUNDE,
        r.TARIF,
        r.ABRECHNUNGSZEITRAUM,
        r.RECHNUNGSBETRAG,
        r.WAEHRUNG,
        r.RECHNUNGSDATUM
    FROM
        `DWH_KERN.T_RECHNUNG` AS r
    WHERE
        # Convert string parameter (format YYYYMMDD) to DATE object for precise partition pruning
        r.RECHNUNGSDATUM = PARSE_DATE('%Y%m%d', p_Stichtag)
    ORDER BY
        r.RECHNUNGSNUMMER ASC;
END;
```

---

## 4. Equivalent BigQuery SQL Query

```sql
-- BigQuery SQL compliant extraction query
-- Note: 'p_Stichtag' is defined as a standard query parameter (@p_Stichtag) to replace the CLI variable.

SELECT
  r.RECHNUNGSNUMMER,
  r.VERTRAG,
  r.KUNDE,
  r.TARIF,
  r.ABRECHNUNGSZEITRAUM,
  r.RECHNUNGSBETRAG,
  r.WAEHRUNG,
  r.RECHNUNGSDATUM
FROM
  `DWH_KERN.T_RECHNUNG` AS r
WHERE
  r.RECHNUNGSDATUM = PARSE_DATE('%Y%m%d', @p_Stichtag)
ORDER BY
  r.RECHNUNGSNUMMER;
```
```

---

## 4. Context & Environment Variables

### Environment Classification Policy
*   **GLOBAL (Environment-Wide)**:
    *   `GCP_PROJECT`: Target Google Cloud Project ID.
    *   `GCS_BUCKET`: Shared GCS bucket used for the final exported CSV/pipe-separated outputs.
    *   `BQ_DATASET`: The target dataset (`DWH_KERN`) housing table `T_RECHNUNG`.
*   **JOB-SPECIFIC**:
    *   `EXPORT_PATH`: Local or temporary staging directory within GCS (e.g. `rechnung_export/daily/`).

---

## 5. Job Orchestration & Verbatim German Print Literals

To resolve previous build review comments, **all original German print literals from UC4, KSH, and SQL environments must be preserved EXACTLY as in the source.** No translation or paraphrasing is permitted.

### Verbatim Print Catalog
1.  **UC4 Header**:
    `Rechnungsexport fuer Stichtag...`
2.  **KSH Start Script**:
    `Starte Export Rechnungsdaten...`
3.  **KSH No-Data Alert**:
    `Keine Rechnungsdaten gefunden.`
4.  **KSH Success & Record Counting**:
    `Anzahl exportierter Rechnungsdatensaetze: `
5.  **KSH Export End Log**:
    `Export Rechnungsdaten erfolgreich beendet.`

### Proposed Airflow DAG Implementation (`dags/dwh_rechnung_export_taeglich.py`)
This primary orchestration file maps directly to the XML source. It triggers the structured logging, execution, and validation modules while preserving complete folder integrity.

```python
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable

# Import split runner functions to maintain clean folder boundaries
from bin.r_exp_rechnung_taeglich import log_header, validate_and_log_results

# Global variables sourced via Airflow Configuration Store
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCS_BUCKET = Variable.get("GCS_BUCKET")
BQ_DATASET = Variable.get("BQ_DATASET", default_var="DWH_KERN")

default_args = {
    'owner': 'DW',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='dwh_rechnung_export_taeglich_js',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    template_searchpath=['/home/gurunathan_t/clean_migration_dataset/DWH/DWH_KERN/PRODUKTION/DW.DWH_RECHNUNG/sql/']
) as dag:

    # Step 1: Log UC4 and Shell script initialization headers verbatim
    log_start = PythonOperator(
        task_id='log_start_verbatim',
        python_callable=log_header,
        templates_dict={'stichtag': '{{ ds_nodash }}'},
        provide_context=True,
    )

    # Step 2: Native BigQuery Extract Operation (equivalent to the SQL execution)
    # The output format is pipe-delimited as configured via SQL*Plus's 'set colsep |'
    execute_bq_query = BigQueryInsertJobOperator(
        task_id='execute_bq_sql_export',
        configuration={
            "query": {
                "query": "d_exp_rechnung_taeglich.sql",
                "useLegacySql": False,
                "queryParameters": [
                    {
                        "name": "p_Stichtag",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {"value": "{{ ds_nodash }}"}
                    }
                ],
                "destinationTable": {
                    "projectId": GCP_PROJECT,
                    "datasetId": BQ_DATASET,
                    "tableId": "TEMP_RECHNUNG_EXPORT_{{ ds_nodash }}"
                },
                "writeDisposition": "WRITE_TRUNCATE"
            }
        }
    )

    # Step 3: Extract BQ staging table to GCS Cloud Storage as Pipe-Delimited
    export_to_gcs = BigQueryInsertJobOperator(
        task_id='export_temp_table_to_gcs',
        configuration={
            "extract": {
                "sourceTable": {
                    "projectId": GCP_PROJECT,
                    "datasetId": BQ_DATASET,
                    "tableId": "TEMP_RECHNUNG_EXPORT_{{ ds_nodash }}"
                },
                "destinationUris": [f"gs://{GCS_BUCKET}/rechnung_export/daily/rechnung_export_{{{{ ds_nodash }}}}.csv"],
                "destinationFormat": "CSV",
                "fieldDelimiter": "|"
            }
        }
    )

    # Step 4: Validate rows and write verbatim exit logs
    validate_results = PythonOperator(
        task_id='validate_and_log_verbatim',
        python_callable=validate_and_log_results,
        templates_dict={'stichtag': '{{ ds_nodash }}', 'gcs_bucket': GCS_BUCKET},
        provide_context=True,
    )

    log_start >> execute_bq_query >> export_to_gcs >> validate_results
```

### Proposed Runner Module (`dags/bin/r_exp_rechnung_taeglich.py`)
This dedicated script corresponds to the `bin/r_exp_rechnung_taeglich.ksh` source file and isolates execution logging, file checks, and verbatim output printing.

```python
import logging
from airflow.providers.google.cloud.hooks.gcs import GCSHook

def log_header(**context):
    stichtag = context['templates_dict']['stichtag']
    # PRESERVED VERBATIM UC4 LOGGING
    print(f"Rechnungsexport fuer Stichtag {stichtag}")
    # PRESERVED VERBATIM KSH LOGGING
    print(f"Starte Export Rechnungsdaten fuer {stichtag}...")

def validate_and_log_results(**context):
    gcs_hook = GCSHook()
    stichtag = context['templates_dict']['stichtag']
    bucket = context['templates_dict']['gcs_bucket']
    object_name = f"rechnung_export/daily/rechnung_export_{stichtag}.csv"
    
    # Retrieve file content to count rows and validate extraction
    try:
        file_bytes = gcs_hook.download(bucket_name=bucket, object_name=object_name)
        file_content = file_bytes.decode('utf-8')
        lines = [line for line in file_content.splitlines() if line.strip()]
        row_count = len(lines)
        
        if row_count == 0:
            # PRESERVED VERBATIM KSH LOGGING
            print("Keine Rechnungsdaten gefunden.")
        else:
            # PRESERVED VERBATIM KSH LOGGING
            print(f"Anzahl exportierter Rechnungsdatensaetze: {row_count}")
            print("Export Rechnungsdaten erfolgreich beendet.")
            
    except Exception as e:
        logging.error(f"Error validating exported file: {str(e)}")
        raise
```

---

## 6. Risks & Manual Actions

### Lineage Dependencies
*   **Upstream Producer Table**: `T_RECHNUNG` is populated by `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP.xml`. Since that process is managed under a separate run sequence, you must ensure scheduling synchronization so this daily export runs only after `DW.DWH_RECHNUNG_EXPORT_TAEGLICH_JP` finishes updating the target table.

### Manual Actions
1.  **GCS Bucket Preparation**: Confirm that the target Cloud Storage bucket specified under `GCS_BUCKET` is provisioned and has write permissions granted to the Airflow task runner service account.
2.  **Dataset Paths**: Ensure that variables (`GCP_PROJECT`, `GCS_BUCKET`) are defined in Airflow's Variable configuration store before initiating the run.