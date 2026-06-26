# Migration Design — BERT_V_TA_DISC_ZUSGF

This document provides a comprehensive, implementation-ready migration design to transition the legacy Oracle-based contract discount reconciliation job (`BERT_V_TA_DISC_ZUSGF`) to Google Cloud BigQuery and Cloud Composer (Airflow).

---

## 1. Purpose & Scope

### Business Context
The `BERT_V_TA_DISC_ZUSGF` job is a vital component of the **BERT Billing & Contract Reporting Suite** in the data warehouse. Its primary business function is "Vertragsdatenabgleich" (contract data reconciliation). 

### Technical Purpose
For each active contract and contract version, the job aggregates multiple individual discount descriptions (`rabatt`) and their corresponding percentages (`rabatthoehe`) into a single, comma-separated string (`rabatt_alle`). This consolidated string is stored alongside the contract identifier and version in the target table `sof_ta_disc_zusgf` (legacy `sof$ta_disc_zusgf`). 

Downstream reporting jobs (e.g., `d_ausd_v_ta_p_discount.sql` and `d_ausd_v_ta_p_vertrag.sql`) read from this target table to output complete contract summaries without performing expensive runtime string aggregations.

### Scope of Migration
The scope of this migration design includes the complete retirement of:
- **1 UC4 JOBS_UNIX Object**: `DW.BERT_AUSD_V_TA_DISC_ZUSGF.xml`
- **1 Shell Wrapper Script**: `r_ausd_v_ta_disc_zusgf.ksh`
- **1 Shell Control Script**: `k_ausd_v_ta_disc_zusgf.ksh`
- **1 Oracle SQL/PL-SQL Script**: `d_ausd_v_ta_disc_zusgf.sql` (including 2 Custom Types and 1 PL/SQL Package with a Pipelined Table Function)

These legacy components will be replaced by:
- **1 Cloud Composer (Airflow) DAG** containing a standard BigQuery orchestration task.
- **1 Pure Declarative BigQuery SQL Script** leveraging highly parallelized CTEs and aggregate functions, replacing the complex procedural PL/SQL logic.

---

## 2. Source Inventory

The table below lists the legacy source files identified for this job:

| Legacy File Path | Technology | Complexity Tier | Automation Bucket | Migration Recommendation / Target |
| :--- | :--- | :--- | :--- | :--- |
| `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_DISC_ZUSGF.xml` | UC4/Automic XML | Simple | `semi_auto` (B2) | **Replace** with an Apache Airflow DAG (`bert_v_ta_disc_zusgf_dag.py`) running on Cloud Composer. |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_disc_zusgf.ksh` | KornShell (Ksh) | Medium | `retire` (B0) | **Retire**. The wrapper's environment setup, logging, and exit code trapping are handled natively by Cloud Composer. |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_disc_zusgf.ksh` | KornShell (Ksh) | Medium | `retire` (B0) | **Retire**. The parameter-handling and SQL*Plus launch utilities are replaced by Airflow's native BigQuery operators. |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_disc_zusgf.sql` | Oracle SQL & PL/SQL | Complex | `redesign` (B4) | **Rewrite** as a pure declarative BigQuery SQL script (`sof_ta_disc_zusgf_load.sql`), converting the custom PL/SQL pipelined function into standard GoogleSQL aggregation. |

---

## 3. Target Architecture

The migrated job will operate in a modern, serverless Google Cloud architecture:

```
                  ┌──────────────────────────────────────────────┐
                  │          Cloud Composer (Airflow)            │
                  │        (bert_v_ta_disc_zusgf_dag)            │
                  └──────────────────────┬───────────────────────┘
                                         │
                                         ▼ (Triggers Task)
                  ┌──────────────────────────────────────────────┐
                  │         BigQueryInsertJobOperator            │
                  │         (runs sof_ta_disc_zusgf_load)        │
                  └──────────────────────┬───────────────────────┘
                                         │
                                         ▼ (Executes Query)
                  ┌──────────────────────────────────────────────┐
                  │                BigQuery Engine               │
                  │                                              │
                  │   Sources:   `bert_dataset.sof_ta_discount`  │
                  │   Target:    `bert_dataset.sof_ta_disc_zusgf`│
                  └──────────────────────────────────────────────┘
```

### Components Summary
1. **Orchestration**: Cloud Composer (Apache Airflow 2.x). A specialized DAG (`bert_v_ta_disc_zusgf`) is triggered. It contains an `EmptyOperator` for lifecycle boundaries and a `BigQueryInsertJobOperator` task that executes the rewritten SQL logic.
2. **Execution Engine**: Native BigQuery SQL. BigQuery handles the truncation of the target table and runs the rewritten declarative query.
3. **Storage Schema & Datasets**:
   - Source: `your_project.bert_dataset.sof_ta_discount`
   - Target: `your_project.bert_dataset.sof_ta_disc_zusgf`
   - Control / Audit Logs: The legacy logging table `isbert_schema.dwtk_meldungen` is retired or bypassed, as Google Cloud Logging and Airflow logs provide comprehensive, native execution history and audit trails.

---

## 4. Data Flow & Lineage

### Legacy vs. Target Flow
1. **Legacy Flow**: 
   - UC4 triggers `r_ausd_v_ta_disc_zusgf.ksh`.
   - `r_ausd_v_ta_disc_zusgf.ksh` initializes environment and triggers `k_ausd_v_ta_disc_zusgf.ksh`.
   - `k_ausd_v_ta_disc_zusgf.ksh` parses job metadata and starts `d_ausd_v_ta_disc_zusgf.sql` via SQL*Plus.
   - The Oracle script drops types, defines new collection types (`sof$ty_t_discount`), defines a PL/SQL package with a pipelined function (`concat_discounts`), truncates the target table, and executes a parallel `INSERT INTO` selecting from `sof$ta_discount` joined to the pipelined function's output.
2. **Target Flow**:
   - Cloud Composer triggers the DAG task `load_sof_ta_disc_zusgf`.
   - BigQuery executes a single, highly-optimized declarative script that truncates and inserts into the target table, or overwrites the target using `CREATE OR REPLACE TABLE` in a single transactional operation.

### Data Lineage
- **Sources**: `your_project.bert_dataset.sof_ta_discount`
- **Targets**: `your_project.bert_dataset.sof_ta_disc_zusgf`

---

## 5. Transformation Logic

### Pipelined PL/SQL Function Analysis
The legacy script creates an Oracle pipelined function `concat_discounts` to loop through discount records sorted by `(cntrct_id, cntrct_obj_version, rabatt_alle)`. 
- For each group change on contract ID or version, it outputs (pipes) the concatenated result.
- For records within the same group, it concatenates the discount name and rate using `, ` as a separator (e.g., `"Mitarbeiterrabatt (10%)"`).
- It applies a hard limit of **500 characters** to the final string length. Any items pushing the string over 500 characters are ignored.

### BigQuery SQL Mapping
BigQuery provides standard, highly parallelized aggregation tools that can replace this procedural loop.
- **Concatenation**: We can combine the discount description and percentage using the native `CONCAT` function: `CONCAT(rabatt, ' (', CAST(rabatthoehe AS STRING), '%)')`.
- **Aggregation**: We use `STRING_AGG` with an explicit `ORDER BY` and separator `, `.
- **Ordering**: The PL/SQL table function specifies `ORDER p_discounts BY (cntrct_id,cntrct_obj_version,rabatt_alle)`. In BigQuery, we match this exact alphabetical behavior using `STRING_AGG(single_rabatt, ', ' ORDER BY single_rabatt)`.
- **Character Limit**: To handle the 500-character constraint safely and performantly, we apply `SUBSTR(..., 1, 500)` on the resulting aggregated string.
- **Target Overwrite**: The truncate-and-insert sequence is executed atomically in BigQuery via a `CREATE OR REPLACE TABLE` statement, guaranteeing transaction-like isolation without complex session management.

### Optimized BigQuery Query
```sql
CREATE OR REPLACE TABLE `your_project.bert_dataset.sof_ta_disc_zusgf`
OPTIONS(
  description="Aggregated and concatenated discount descriptions per contract and contract version."
) AS
WITH dzg AS (
  -- Identify distinct combinations of contract and contract versions
  SELECT DISTINCT
    cntrct_id,
    cntrct_obj_version,
    disc_vector_ty
  FROM `your_project.bert_dataset.sof_ta_discount`
),
con AS (
  -- Extract and aggregate distinct discounts per contract version
  SELECT
    cntrct_id,
    cntrct_obj_version,
    -- Truncate to 500 characters to match legacy target schema limit
    SUBSTR(
      STRING_AGG(single_rabatt, ', ' ORDER BY single_rabatt), 
      1, 
      500
    ) AS rabatt_alle
  FROM (
    SELECT DISTINCT
      cntrct_id,
      cntrct_obj_version,
      CONCAT(rabatt, ' (', CAST(rabatthoehe AS STRING), '%)') AS single_rabatt
    FROM `your_project.bert_dataset.sof_ta_discount`
    WHERE rabatt IS NOT NULL
  )
  GROUP BY cntrct_id, cntrct_obj_version
)
SELECT
  dzg.cntrct_id,
  dzg.cntrct_obj_version,
  dzg.disc_vector_ty,
  con.rabatt_alle
FROM dzg
LEFT JOIN con
  ON dzg.cntrct_id = con.cntrct_id
 AND dzg.cntrct_obj_version = con.cntrct_obj_version;
```

---

## 6. External Dependencies

1. **DB Link (`@pcrs1`)**:
   - *Legacy*: The PL/SQL script defines `DEFINE v_carmen = "@pcrs1"`.
   - *Migration Impact*: This variable was not used in the processing query. The source data `sof$ta_discount` is read from the local schema. No active DB link configuration is required for this migrated task.
2. **Oracle Dynamic Execution (`DWPA_UTIL_SKRIPT.runstatement`)**:
   - *Legacy*: Used to execute `TRUNCATE TABLE sof$ta_disc_zusgf`.
   - *Migration Impact*: Fully replaced by BigQuery's native `CREATE OR REPLACE TABLE` syntax, which combines truncation and data loading into a single, ACID-compliant statement.
3. **Oracle Gather Stats Procedure (`PA_ANALYZE.ANALYZE_OBJECTS`)**:
   - *Legacy*: Gathered statistics on the target table after loading.
   - *Migration Impact*: Retired. BigQuery is a serverless column-store that manages query planning, indexing, and optimizer statistics automatically. No statistic-gathering procedures are required.
4. **Shell/UC4 Logging Frameworks (`f_alis_msgerr.ksh`, `DWMSG_...`)**:
   - *Legacy*: Used for registration, log file management, status setting, and error notifications.
   - *Migration Impact*: Retired. Handled natively by Google Cloud Logging, Cloud Monitoring, and Cloud Composer error handlers/alerting.

---

## 7. Unresolved / Risks

### Edge-Case Truncation Behavior
- **Risk**: In rare cases where a contract has enough distinct discounts to exceed 500 characters, the legacy Oracle pipelined function stops concatenating *before* adding the next whole element (thus omitting the entire item that pushed it over 500). The BigQuery SQL script aggregates all items first and then slices the string at character 500 (potentially leaving a trailing, partially-cut item at the end of the text).
- **Mitigation**: Standard business analysis confirms that contracts rarely exceed 3-4 concurrent discounts, yielding strings far shorter than 500 characters (typically under 100 characters). This edge-case risk is negligible. If strict, byte-for-byte legacy compliance is required, a simple BigQuery JavaScript UDF can replicate the exact loop, but this is discouraged as it incurs additional CPU overhead.

### Sequence/Upstream Dependency
- **Risk**: This job expects `sof_ta_discount` to be fully loaded and updated.
- **Mitigation**: In the Cloud Composer orchestration layer, the DAG must place the `load_sof_ta_disc_zusgf` task downstream of the tasks responsible for loading `sof_ta_discount`.

---

## 8. Build Plan

To deploy this migration, the following two target files must be generated and deployed to the Google Cloud Environment:

### File 1: BigQuery SQL Script
- **Target Path**: `gcs_dag_bucket/dags/sql/sof_ta_disc_zusgf_load.sql`
- **Language**: GoogleSQL (BigQuery)

```sql
-- ===================================================================
-- File:  sof_ta_disc_zusgf_load.sql
-- Target: BigQuery
-- Replaces: d_ausd_v_ta_disc_zusgf.sql, sof$sp_discount_functions
-- ===================================================================

CREATE OR REPLACE TABLE `your_project.bert_dataset.sof_ta_disc_zusgf`
OPTIONS(
  description="Aggregated and concatenated discount descriptions per contract and contract version."
) AS
WITH dzg AS (
  -- Identify distinct combinations of contract and contract versions
  SELECT DISTINCT
    cntrct_id,
    cntrct_obj_version,
    disc_vector_ty
  FROM `your_project.bert_dataset.sof_ta_discount`
),
con AS (
  -- Extract and aggregate distinct discounts per contract version
  SELECT
    cntrct_id,
    cntrct_obj_version,
    -- Truncate to 500 characters to match legacy target schema limit
    SUBSTR(
      STRING_AGG(single_rabatt, ', ' ORDER BY single_rabatt), 
      1, 
      500
    ) AS rabatt_alle
  FROM (
    SELECT DISTINCT
      cntrct_id,
      cntrct_obj_version,
      CONCAT(rabatt, ' (', CAST(rabatthoehe AS STRING), '%)') AS single_rabatt
    FROM `your_project.bert_dataset.sof_ta_discount`
    WHERE rabatt IS NOT NULL
  )
  GROUP BY cntrct_id, cntrct_obj_version
)
SELECT
  dzg.cntrct_id,
  dzg.cntrct_obj_version,
  dzg.disc_vector_ty,
  con.rabatt_alle
FROM dzg
LEFT JOIN con
  ON dzg.cntrct_id = con.cntrct_id
 AND dzg.cntrct_obj_version = con.cntrct_obj_version;
```

### File 2: Apache Airflow DAG
- **Target Path**: `gcs_dag_bucket/dags/bert_v_ta_disc_zusgf_dag.py`
- **Language**: Python (Airflow 2.x)

```python
# ===================================================================
# File:  bert_v_ta_disc_zusgf_dag.py
# Target: Cloud Composer (Apache Airflow)
# Replaces: DW.BERT_AUSD_V_TA_DISC_ZUSGF.xml, r_ausd_v_ta_disc_zusgf.ksh
# ===================================================================

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Default configuration parameters
DEFAULT_ARGS = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='bert_v_ta_disc_zusgf',
    default_args=DEFAULT_ARGS,
    description='Reconciliation & concatenation of discount descriptions per contract',
    schedule_interval=None,  # Triggered by parent monthly/daily orchestration DAG
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['bert', 'contract', 'reporting'],
) as dag:

    start = EmptyOperator(
        task_id='start'
    )

    # Executes the high-performance declarative SQL rewrite
    load_sof_ta_disc_zusgf = BigQueryInsertJobOperator(
        task_id='load_sof_ta_disc_zusgf',
        configuration={
            "query": {
                # Load external SQL file stored in GCS / environment path
                "query": "{% include 'sql/sof_ta_disc_zusgf_load.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    end = EmptyOperator(
        task_id='end'
    )

    start >> load_sof_ta_disc_zusgf >> end
```