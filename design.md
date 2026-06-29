# Migration Design — ausd_bp_ta_bpr_bcp

This document outlines the migration design for migrating the legacy UC4 and KornShell-based job **ausd_bp_ta_bpr_bcp** to Google Cloud Platform (GCP) with **BigQuery** and **Cloud Composer (Apache Airflow)**.

---

## 1. Purpose & Scope
The job `ausd_bp_ta_bpr_bcp` is responsible for provisioning the "BusinessCardPackage" (BCP) instantiated base products for BERT (Forderungsscoring/Collection Management). 
Specifically, it filters the base product instances (`sof$ta_bpr_instance`) to extract contracts associated with a specific base product ID (`3142` - "Zusatzkarte im BusinessCardPackage"), deduplicates them, and loads them into a target table (`sof$ta_bpr_bcp`).

This processing ensures that active credit scoring and contract structures are correctly prepared and available for downstream scoring processes.

---

## 2. Source Inventory
The legacy job consists of four primary components:

| File Path | Technology | Complexity Tier | Automation Bucket | Summary / Function |
| :--- | :--- | :--- | :--- | :--- |
| `DW.BERT_AUSD_BP_TA_BPR_BCP.xml` | UC4 / Automic | Medium | Semi-Automated | Scheduler job that initializes the environment and calls the shell script wrapper `r_ausd_bp_ta_bpr_bcp.ksh`. |
| `r_ausd_bp_ta_bpr_bcp.ksh` | KornShell (ksh) | Medium | Semi-Automated | Orchestration wrapper script that handles parameters (`-s` for cutoff date, `-l` for restart tracking value), log initialisation, and trap execution errors. |
| `k_ausd_bp_ta_bpr_bcp.ksh` | KornShell (ksh) | Medium | Semi-Automated | Control script that performs date checks, establishes runtime variables (`p_datum_heute`, `p_datum_gestern`), and executes the underlying SQL script using `starteSQLSkript`. |
| `d_ausd_bp_ta_bpr_bcp.sql` | Oracle SQL | Medium | Semi-Automated | Oracle SQL script that queries metadata, truncates the target table `sof$ta_bpr_bcp`, and bulk-inserts records from `sof$ta_bpr_instance` filtered on `bpr_id = '3142'`. |

---

## 3. Target Architecture
The proposed architecture maps legacy scheduling, scripting, and database components directly to GCP native solutions:

*   **Orchestration & Workflow**: **Google Cloud Composer (Apache Airflow)** replaces UC4 and KornShell orchestration wrappers.
*   **Data Warehouse & Processing Engine**: **Google BigQuery** replaces the Oracle database instance.
*   **Target Schema Mapping**:
    *   Legacy Oracle tables are mapped to BigQuery under the dataset `isbert_schema` (or another designated BQ dataset).
    *   Table names containing special characters such as `$` (e.g., `sof$ta_bpr_bcp`) are converted to standard BQ identifiers by substituting `$` with `_` (e.g., `sof_ta_bpr_bcp`).
*   **Error Handling & Alerting**: Cloud Composer leverages standard Airflow logging and native alerting (via email, Slack, or Google Cloud Monitoring) to replace the legacy custom shell logging tool (`f_alis_msgerr.ksh`).

### Target Layout (BigQuery Objects)
1.  **Source Tables**:
    *   `isbert_schema.dwtk_meldungen` (Tracks execution status/job events)
    *   `isbert_schema.sof_ta_bpr_instance` (Instantiated base products)
2.  **Target Table**:
    *   `isbert_schema.sof_ta_bpr_bcp` (Deduplicated BCP contracts table)

---

## 4. Data Flow & Lineage

The end-to-end data processing lineage is shown below:

```
[dwtk_meldungen] ------------> (Query v_datum)
                                    |
                                    v
[sof_ta_bpr_instance] --------> [BigQuery DML Statement] -------> [sof_ta_bpr_bcp]
(Source Table)             (Filter: bpr_id = '3142')           (Target Table)
```

### Execution Steps
1.  **Airflow Trigger**: The Cloud Composer DAG starts either on a predefined cron schedule or via manual trigger.
2.  **Date Resolution**: The DAG resolves the effective execution cutoff date (`Stichtag`).
3.  **SQL Block Execution**: Airflow runs a single BigQuery transaction script that:
    *   Queries `isbert_schema.dwtk_meldungen` to identify the most recent runtime timestamp for `BERT_DROP_TEMP_TABLE`.
    *   Truncates `isbert_schema.sof_ta_bpr_bcp` to clear prior daily or partial run data.
    *   Executes the bulk distinct insertion into `sof_ta_bpr_bcp` from the source `sof_ta_bpr_instance`.

---

## 5. Transformation Logic

### SQL Dialect Conversion (Oracle to BigQuery Standard SQL)
1.  **Table Metadata Drop Date Lookup**:
    *   *Oracle*: `SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum ...`
    *   *BigQuery*: `SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101') AS s_datum ...`
2.  **Truncate Strategy**:
    *   *Oracle*: `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_bpr_bcp REUSE STORAGE');`
    *   *BigQuery*: Standard `TRUNCATE TABLE `isbert_schema.sof_ta_bpr_bcp`;`
3.  **Bulk Selection**:
    *   Oracle optimizer hints `/*+ full(bp) parallel(bp,4) */` are removed as BigQuery automatically handles scale and parallelism.
    *   The `DISTINCT` operator is retained to guarantee contract deduplication.

### Final Target BigQuery SQL Script
```sql
-- ===================================================================
-- Target BigQuery Script: d_ausd_bp_ta_bpr_bcp
-- Replaces d_ausd_bp_ta_bpr_bcp.sql
-- ===================================================================

-- Step 1: Query legacy metadata variable v_datum (retained for backward compatibility and audits)
DECLARE v_datum STRING;

SET v_datum = (
  SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101')
  FROM `isbert_schema.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- Step 2: Clear the target table
TRUNCATE TABLE `isbert_schema.sof_ta_bpr_bcp`;

-- Step 3: Insert valid distinct Business Card Package (BCP) contracts
INSERT INTO `isbert_schema.sof_ta_bpr_bcp` (
  CNTRCT_ID,
  BPR_ID,
  CNTRCT_ID_REF
)
SELECT DISTINCT
  bp.cntrct_id,
  bp.bpr_id,
  bp.cntrct_id_ref
FROM `isbert_schema.sof_ta_bpr_instance` bp
WHERE bp.bpr_id = '3142';
```

---

## 6. External Dependencies

1.  **DWH Shell Infrastructure (`.dw_init`, `gestern.ksh`)**: 
    *   *Replacement*: Replaced by Airflow standard context variables. Execution/today's date maps to `{{ ds }}` and yesterday's date maps to `{{ yesterday_ds }}`.
2.  **Oracle System Table / Custom Schema Procedures (`DWPA_UTIL_SKRIPT`)**:
    *   *Replacement*: Replaced entirely by standard native BigQuery DDL/DML, avoiding the necessity of calling specialized dynamic SQL runtime procedures.
3.  **FOS Loader and Job Entry Logging (`FOSJobErzeugeEintrag`)**:
    *   The original control script had commented-out calls to registration APIs. If downstream pipelines rely on SQL-level status logs, these can be appended as simple SQL log inserts to a central execution audit table in BigQuery.

---

## 7. Unresolved / Risks

*   **Unused Variables**: The SQL script queries the date `v_datum` from `dwtk_meldungen` but does not use it inside the `INSERT` block (it was removed in an earlier legacy version `10.2.1`). We have preserved the query logic inside a SQL local variable `v_datum` to keep schema reads identical. If this table (`dwtk_meldungen`) is no longer being populated in GCP, this lookup should be simplified to a default constant value.
*   **Special Character Mapping**: Table names containing `$` (Oracle syntax) have been mapped to `_`. Any downstream analytical tools or other workloads querying `sof$ta_bpr_bcp` must be updated to refer to `sof_ta_bpr_bcp`.

---

## 8. Build Plan

The migration implementation requires the delivery of two main objects:

### 1. BigQuery SQL Procedure File (`d_ausd_bp_ta_bpr_bcp.sql`)
This file contains the complete processing logic, conforming to Google Standard SQL, as designed in **Section 5**.

### 2. Airflow DAG Python Script (`dw_bert_ausd_bp_ta_bpr_bcp.py`)
This Airflow pipeline orchestrates the workflow. It uses a single `BigQueryExecuteQueryOperator` to process the entire DML statement safely within a single transaction step.

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# Default arguments config
default_args = {
    "owner": "dwh_migration_team",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dw_bert_ausd_bp_ta_bpr_bcp",
    default_args=default_args,
    description="BERT_P_BASISPRODUKT: Provisioning of instantiated BCP base products in BigQuery",
    schedule_interval="0 4 * * *",  # Runs daily at 04:00 AM
    catchup=False,
    max_active_runs=1,
    tags=["dwh", "bert", "bigquery"],
) as dag:

    # Execute conversion SQL script using BigQuery Operator
    process_bpr_bcp = BigQueryExecuteQueryOperator(
        task_id="execute_bpr_bcp_processing",
        sql="""
            DECLARE v_datum STRING;

            SET v_datum = (
              SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101')
              FROM `isbert_schema.dwtk_meldungen` m
              WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
            );

            TRUNCATE TABLE `isbert_schema.sof_ta_bpr_bcp`;

            INSERT INTO `isbert_schema.sof_ta_bpr_bcp` (
              CNTRCT_ID,
              BPR_ID,
              CNTRCT_ID_REF
            )
            SELECT DISTINCT
              bp.cntrct_id,
              bp.bpr_id,
              bp.cntrct_id_ref
            FROM `isbert_schema.sof_ta_bpr_instance` bp
            WHERE bp.bpr_id = '3142';
        """,
        use_legacy_sql=False,
        location="EU",  # Set to the appropriate BigQuery dataset location (US/EU)
    )

    process_bpr_bcp
```