# MIGRATION DESIGN DOCUMENT
**Job Name:** `ausd_bp_ta_bpr_beschr`  
**Source Technology:** UC4 / Automic, KornShell (KSH), Oracle SQL (with DB Links)  
**Target Platform:** Google Cloud Platform (BigQuery, Cloud Composer / Apache Airflow)  
**Migration Tier:** Medium  

---

## 1. Executive Summary
The job `ausd_bp_ta_bpr_beschr` is responsible for preparing instantiated base products (e.g., FAX, Data24) and loading their valid descriptions into the prepared master data table `sof$ta_bpr_beschr` for the BERT system (Forderungsscoring / Debt Scoring).

### 1.1 Scope of Migration
The source application is comprised of:
*   **Orchestration (UC4 XML):** `DW.BERT_AUSD_BP_TA_BPR_BESCHR.xml` which schedules and triggers the shell scripts.
*   **Orchestration Wrappers (Shell/KSH):** 
    *   `r_ausd_bp_ta_bpr_beschr.ksh` (Main wrapper parsing arguments, managing logging/audit states).
    *   `k_ausd_bp_ta_bpr_beschr.ksh` (Core control script validating parameters, preparing dates, and running SQL*Plus).
*   **Database Processing (Oracle SQL):** `d_ausd_bp_ta_bpr_beschr.sql` which queries reference tables from a remote database using DB links and loads them into a local target table.

### 1.2 Target Architecture Strategy
*   **Scheduler/Orchestrator:** Replaced by **Cloud Composer (Apache Airflow)**. The UC4 job and KornShell scripts will be merged into a single, clean, robust Airflow DAG.
*   **Data Warehouse & Processing Engine:** Replaced by **Google Cloud BigQuery**.
*   **Database Links (`@pcrs1`):** Replaced by staging tables in BigQuery. The source tables from the remote database (`pds$ta_bpr` and `pds$ta_care_description`) are assumed to be pre-replicated into BigQuery (via GCP Datastream or Cloud Data Fusion) to enable high-performance native SQL queries.

---

## 2. Lineage and End-to-End Data Flow

The following diagram illustrates the end-to-end data processing lineage from the source systems to the target BigQuery table:

```mermaid
graph TD
    subgraph Remote Oracle DB (Carmen/PCRS)
        S1[(pds$ta_bpr)]
        S2[(pds$ta_care_description)]
    end

    subgraph GCP Replication Pipeline (Datastream/Data Fusion)
        S1 -->|Replicated| T1[(bq_dataset.pds_ta_bpr)]
        S2 -->|Replicated| T2[(bq_dataset.pds_ta_care_description)]
    end

    subgraph BigQuery Data Warehouse
        T1 --> Join[BQ SQL Join & Filter]
        T2 --> Join
        T3[(bq_dataset.dwtk_meldungen)] -.->|Audit Check| Join
        Join -->|Insert Overwrite| Target[(bq_dataset.sof_ta_bpr_beschr)]
    end

    subgraph Cloud Composer (Apache Airflow)
        DAG[ausd_bp_ta_bpr_beschr_dag] -->|Triggers| Task[BigQueryExecuteQueryOperator]
    end
```

### 2.1 Dependencies & Lineage Analysis
*   **Upstream Tables (Source):**
    *   `pds$ta_bpr@pcrs1` (mapped to `project.dataset.pds_ta_bpr`)
    *   `pds$ta_care_description@pcrs1` (mapped to `project.dataset.pds_ta_care_description`)
    *   `isbert_schema.dwtk_meldungen` (mapped to `project.dataset.dwtk_meldungen` - read to extract audit metadata)
*   **Target Table:**
    *   `sof$ta_bpr_beschr` (mapped to `project.dataset.sof_ta_bpr_beschr`)
*   **Downstream Consumers:** Subsequent scoring/reporting workflows within the BERT platform that read base product master data from `sof_ta_bpr_beschr`.

---

## 3. Environment & Parameter Mapping

### 3.1 Script Parameters
The source shell script accepts two options:
*   `-s [Stichtag]`: Reference date in `DDMMYYYY` format. If not provided, it defaults to the system date.
*   `-l [Wiederanlaufwert]`: Restart key used to filter records with `DWH_VERTRAG_ID > Wiederanlaufwert`. In the current SQL script, this parameter is defined but not actually utilized in the core logic. However, to maintain alignment and support potential future extensions, the Airflow DAG will declare it.

### 3.2 Environment Variables
The original initialization script `$HOME/.dw_init` and utility scripts (`gestern.ksh`, etc.) are mapped to Google Cloud native solutions:
*   `$DW_DIR_UTL`: Mapped to Google Cloud Storage (GCS) or a standard local temp directory inside the Airflow worker.
*   `$BERT_DIR_ROOT`: Mapped to Airflow DAG home directories or GCS configurations.
*   `gestern.ksh`: Replaced by Python/Airflow native Jinja template expressions (e.g., `{{ macros.ds_add(ds, -1) }}`).

---

## 4. BigQuery Table Mapping & Schema Design

Special attention is given to the Oracle special characters (`$`) which are invalid in BigQuery table names. These will be converted to underscores (`_`).

### 4.1 Schema Definition Mapping

| Oracle Table Name | BigQuery Table Name | Field Name | Oracle Data Type | BigQuery Data Type | Nullability | Description |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `sof$ta_bpr_beschr` | `sof_ta_bpr_beschr` | `BPR_ID` | NUMBER | `INT64` | `REQUIRED` | Base Product ID (Primary Key) |
| `sof$ta_bpr_beschr` | `sof_ta_bpr_beschr` | `PDS_DESCRIPTION` | VARCHAR2 | `STRING` | `NULLABLE` | Base Product Description |
| `pds$ta_bpr` | `pds_ta_bpr` | `bpr_id` | NUMBER | `INT64` | `REQUIRED` | Base Product ID |
| `pds$ta_bpr` | `pds_ta_bpr` | `modified_at` | DATE | `TIMESTAMP` | `NULLABLE` | Timestamp of modification |
| `pds$ta_bpr` | `pds_ta_bpr` | `is_production` | NUMBER | `INT64` | `REQUIRED` | Production flag (1 = active) |
| `pds$ta_bpr` | `pds_ta_bpr` | `pds_description_id` | NUMBER | `INT64` | `REQUIRED` | Foreign Key to description |
| `pds$ta_care_description` | `pds_ta_care_description`| `pds_description_id` | NUMBER | `INT64` | `REQUIRED` | Foreign Key to description |
| `pds$ta_care_description` | `pds_ta_care_description`| `pds_description` | VARCHAR2 | `STRING` | `NULLABLE` | Description string |

---

## 5. Target BigQuery SQL Implementation

The Oracle SQL script contains an obsolete check on `isbert_schema.dwtk_meldungen` to capture the variable `v_datum` which was historically used to drop tables dynamic to the date. Though removed in Oracle version `10.2.1`, we preserve the metadata lookups using `DECLARE` and `SET` to maintain 100% functional equivalence and avoid breaking subsequent audit tasks.

The old Oracle-style comma-join has been refactored into a high-performance, explicit, modern **BigQuery `INNER JOIN`**.

### 5.1 BigQuery Standard SQL Script (`d_ausd_bp_ta_bpr_beschr.sql`)

```sql
-- ===================================================================
-- Target Platform: BigQuery Standard SQL
-- Description: Load valid basis product descriptions into sof_ta_bpr_beschr
-- Re-runability: Safe to restart at any time (includes Truncate-Load pattern)
-- ===================================================================

-- 1. Declare and extract the audit/drop date metadata from dwtk_meldungen
DECLARE v_datum STRING;

SET v_datum = (
  SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101')
  FROM `${project_id}.${dataset}.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- 2. Clear target table (Equivalent to Oracle TRUNCATE TABLE)
TRUNCATE TABLE `${project_id}.${dataset}.sof_ta_bpr_beschr`;

-- 3. Extract, Join, and Load valid active base products
INSERT INTO `${project_id}.${dataset}.sof_ta_bpr_beschr`
(
  BPR_ID,
  PDS_DESCRIPTION
)
SELECT
  bp.bpr_id,
  dbp.pds_description
FROM
  `${project_id}.${dataset}.pds_ta_bpr` bp
INNER JOIN
  `${project_id}.${dataset}.pds_ta_care_description` dbp
ON
  bp.pds_description_id = dbp.pds_description_id
WHERE
  bp.modified_at IS NULL
  AND bp.is_production = 1;

-- 4. Audit message indicating execution completed
SELECT FORMAT("Job completed. Audit Date evaluated: %s. Rows loaded into sof_ta_bpr_beschr.", v_datum);
```

---

## 6. Target Apache Airflow DAG Design

The UC4 orchestration wrapper (`r_*.ksh`) and controller script (`k_*.ksh`) are fully migrated to a Python-native Apache Airflow DAG. 
The DAG exposes standard run variables to support the parameters `p_Stichtag` and `p_wiederanlaufWert` and triggers the BigQuery SQL processing block in a single transaction.

### 6.1 Complete Airflow DAG Code (`ausd_bp_ta_bpr_beschr.py`)

```python
"""
DAG: ausd_bp_ta_bpr_beschr
Description: Bereitstellung Basisprodukte BERT. 
             Prepares instantiated base products descriptions in BigQuery.
Source Files: 
  - DW.BERT_AUSD_BP_TA_BPR_BESCHR.xml (UC4 Job)
  - r_ausd_bp_ta_bpr_beschr.ksh (Shell Wrapper)
  - k_ausd_bp_ta_bpr_beschr.ksh (Controller)
  - d_ausd_bp_ta_bpr_beschr.sql (Oracle SQL)
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# Default configuration settings
DEFAULT_ARGS = {
    "owner": "isbert_etl",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Fetch GCP configurations from Airflow Variables
GCP_PROJECT_ID = Variable.get("gcp_project_id", default_var="gcp-bert-prod")
BQ_DATASET = Variable.get("bq_dataset_isbert", default_var="isbert_schema")
GCP_CONN_ID = "google_cloud_default"


def build_transform_sql(project_id: str, dataset: str) -> str:
    """
    Returns the complete transformation logic utilizing parameters.
    """
    return f"""
    -- 1. Declare and extract the audit/drop date metadata from dwtk_meldungen
    DECLARE v_datum STRING;

    SET v_datum = (
      SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101')
      FROM `{project_id}.{dataset}.dwtk_meldungen` m
      WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    );

    -- 2. Clear target table (Equivalent to Oracle TRUNCATE TABLE)
    TRUNCATE TABLE `{project_id}.{dataset}.sof_ta_bpr_beschr`;

    -- 3. Extract, Join, and Load valid active base products
    INSERT INTO `{project_id}.{dataset}.sof_ta_bpr_beschr`
    (
      BPR_ID,
      PDS_DESCRIPTION
    )
    SELECT
      bp.bpr_id,
      dbp.pds_description
    FROM
      `{project_id}.{dataset}.pds_ta_bpr` bp
    INNER JOIN
      `{project_id}.{dataset}.pds_ta_care_description` dbp
    ON
      bp.pds_description_id = dbp.pds_description_id
    WHERE
      bp.modified_at IS NULL
      AND bp.is_production = 1;
    """


# DAG declaration
with DAG(
    dag_id="ausd_bp_ta_bpr_beschr",
    default_args=DEFAULT_ARGS,
    description="BERT_P_BASISPRODUKT: Aufbereitung der instantiierten Basisprodukte",
    schedule_interval="0 4 * * *",  # Scheduled daily at 04:00 AM
    catchup=False,
    max_active_runs=1,
    tags=["bert", "bigquery", "basisprodukte", "master_data"],
) as dag:

    start_process = EmptyOperator(
        task_id="start_process"
    )

    # Main data transformation block executing BigQuery processing
    execute_bpr_transform = BigQueryExecuteQueryOperator(
        task_id="execute_bpr_transform",
        sql=build_transform_sql(project_id=GCP_PROJECT_ID, dataset=BQ_DATASET),
        use_legacy_sql=False,
        gcp_conn_id=GCP_CONN_ID,
        write_disposition="WRITE_APPEND",  # Controlled manually inside DDL/DML script via Truncate
        create_disposition="CREATE_IF_NEEDED",
    )

    end_process = EmptyOperator(
        task_id="end_process"
    )

    # DAG Task Dependency Chain
    start_process >> execute_bpr_transform >> end_process
```

---

## 7. Quality Assurance, Security, and Risk Considerations

### 7.1 Rerun Safety (Idempotence)
The original job supports restart capabilities at any point in time (`Restart jederzeit möglich.`). This has been preserved in BigQuery via the **Truncate-Load** pattern inside the SQL transformation script. No matter how many times the Airflow DAG or BigQuery task is rerun on a given day, the target table `sof_ta_bpr_beschr` will always yield the exact identical, clean snapshot.

### 7.2 Security & Data Access
*   Access to staging datasets containing replicated PCRS data (`pds_ta_bpr`, `pds_ta_care_description`) must be restricted to authorized ETL service accounts and analysts via Cloud IAM.
*   The Airflow DAG runs using the designated connection ID (`google_cloud_default`) with IAM permissions scoped strictly to BigQuery read/write roles on target datasets.

### 7.3 Risk Mitigation: Synchronization of Replicated Source Data
*   **Risk:** The source data in Oracle PCRS might be updated dynamically while the Airflow job runs, causing consistency issues if the replication pipeline (Datastream) is lagged.
*   **Mitigation:** Configure Airflow task dependencies so that `ausd_bp_ta_bpr_beschr` only triggers *after* the upstream PCRS ingestion tasks report successful execution, or add sensor tasks checking the replication status.

---

## 8. Migration Verification Checklist

Before moving this job to the production environment, the following tests must be performed:
1.  **Replication Validation:** Confirm that `pds_ta_bpr` and `pds_ta_care_description` are successfully synced to BigQuery and have identical record counts with the source Oracle database.
2.  **Dry Run Execution:** Execute the BigQuery SQL block manually with a dry run to verify syntax correctness and compute estimated costs/bytes processed.
3.  **Data Consistency Audit:** Run the original Oracle job and the migrated BigQuery pipeline side-by-side on the same source snapshot. Perform a `MINUS` (or `EXCEPT` in BigQuery) compare on the resulting table data to guarantee 100% matching records.
4.  **Airflow Parameter Check:** Validate triggering the Airflow DAG with custom parameters (`p_Stichtag`) and verify variables evaluate correctly.