# Migration Notes: DW.BERT_AUSD_V_TA_PERIOD

This document details the migration of the legacy UC4 job **DW.BERT_AUSD_V_TA_PERIOD** to Google Cloud Platform (GCP). It serves as an operational guide for deploying, validating, and maintaining the migrated artifacts.

---

## 1. Summary

The legacy UC4 UNIX job `DW.BERT_AUSD_V_TA_PERIOD` has been migrated from an on-premises Automic/UC4 scheduler and Oracle database environment to **Apache Airflow (Cloud Composer)** and **Google Cloud BigQuery**.

### Scope of Migration
* **Legacy Orchestration**: UC4 standalone UNIX job (`JOBS_UNIX`) executing a KornShell wrapper.
* **Legacy Wrapper**: `r_ausd_v_ta_period.ksh` (handles environment setup, parameter validation, and execution logging).
* **Legacy Database Logic**: `d_ausd_v_ta_period.sql` (Oracle SQL*Plus script that truncates and repopulates the `sof$ta_period` table with active period definitions from a remote Carmen database via DB Link).
* **Target Orchestration**: Apache Airflow DAG (`dw_bert_ausd_v_ta_period`).
* **Target Wrapper**: Python 3 script (`r_ausd_v_ta_period.py`) utilizing the Google Cloud BigQuery Client SDK.
* **Target Database Logic**: BigQuery Standard SQL Scripting (`d_ausd_v_ta_period.sql`).

---

## 2. Generated Artifacts

The migration process generated three primary files, each playing a specific role in the target architecture:

| File Name | Target Path | Language / Format | Role |
| :--- | :--- | :--- | :--- |
| **`dw_bert_ausd_v_ta_period.py`** | `dags/` | Python (Airflow DAG) | Orchestrates the execution of the job. Defines a standalone, on-demand DAG that triggers the Python wrapper script via a `BashOperator`. |
| **`r_ausd_v_ta_period.py`** | `bin/` | Python 3 | Replaces the legacy KornShell wrapper. Handles command-line arguments, configures logging, initializes the BigQuery client, executes the SQL script, and captures row-count metrics. |
| **`d_ausd_v_ta_period.sql`** | `sql/` | BigQuery Standard SQL | Replaces the Oracle SQL*Plus script. Uses BigQuery Scripting to dynamically resolve datasets, truncate the target table, and perform the `INSERT INTO ... SELECT` transformation. |

---

## 3. Key Design Decisions

### 3.1 KornShell to Python 3 Translation
* **Decision**: The legacy shell script `r_ausd_v_ta_period.ksh` was rewritten as a native Python 3 script (`r_ausd_v_ta_period.py`) rather than being executed directly in a shell environment.
* **Reasoning**: Python provides robust, cross-platform exception handling, native integration with the Google Cloud SDK, and cleaner string manipulation. It also eliminates the need to maintain legacy Unix-specific shell utilities.
* **Trade-off**: Requires a Python runtime environment on the Airflow worker, which is standard in Cloud Composer.

### 3.2 Elimination of Oracle Database Links
* **Decision**: The legacy script queried remote Carmen tables using an Oracle DB Link (`@pcrs1.de.tinternal.com`). In BigQuery, these references have been replaced by a staging dataset pattern (`CARMEN_STAGE_DATASET`).
* **Reasoning**: BigQuery does not support native database links to external transactional databases. 
* **Trade-off**: Introduces an external dependency on an upstream ingestion pipeline that must replicate the Carmen tables (`cds_ta_period`, `cds_ta_time_meas_cv`, and `cds_ta_description`) into BigQuery prior to running this job.

### 3.3 BigQuery Scripting and Dynamic SQL
* **Decision**: The SQL script uses `EXECUTE IMMEDIATE FORMAT` to dynamically inject the GCP Project ID, target dataset, and staging dataset names at runtime.
* **Reasoning**: This avoids hardcoding environment-specific schema names (e.g., Dev vs. Prod) and allows the same SQL file to be deployed across multiple environments without modification.
* **Trade-off**: Slightly increases the complexity of the SQL script, but significantly improves CI/CD pipeline compatibility.

### 3.4 Native Truncate Strategy
* **Decision**: Replaced the legacy PL/SQL dynamic truncate utility (`isbert_schema.DWPA_UTIL_SKRIPT.runstatement`) with a native BigQuery `TRUNCATE TABLE` statement.
* **Reasoning**: Native DML statements in BigQuery are highly performant, secure, and do not require maintaining custom database utility packages.

---

## 4. Manual Steps Before Go-Live

The following setup steps must be completed in the target GCP environment before triggering the workflow:

### 4.1 Schema and Table Creation
Ensure the following tables exist in your target BigQuery datasets:

1. **Target Dataset (`BQ_DATASET`)**:
   * `sof_ta_period`: Target table containing mirrored period definitions.
   * `dwtk_meldungen`: Watermark tracking table.
2. **Staging Dataset (`CARMEN_STAGE_DATASET`)**:
   * `cds_ta_period`: Replicated source period table.
   * `cds_ta_time_meas_cv`: Replicated source time measurement lookup table.
   * `cds_ta_description`: Replicated source description lookup table.

### 4.2 IAM and Permissions
The Service Account running the Cloud Composer workers (or Airflow tasks) must be granted the following IAM roles:
* `roles/bigquery.jobUser` on the GCP Project.
* `roles/bigquery.dataEditor` on the target operational dataset (`BQ_DATASET`).
* `roles/bigquery.dataViewer` on the staging dataset (`CARMEN_STAGE_DATASET`).

### 4.3 Airflow Variables Configuration
Configure the following Airflow Variables in the Airflow UI (**Admin -> Variables**):

| Variable Key | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `my-gcp-project-id` | The target Google Cloud Project ID. |
| `BQ_DATASET` | `isbert_schema` | The operational dataset containing target tables. |
| `CARMEN_STAGE_DATASET` | `carmen_stage` | The staging dataset containing replicated Carmen tables. |

### 4.4 Environment Variables
Ensure the following environment variables are accessible to the Airflow worker (configured via Composer Environment Variables or task context):
* `HOME`: Path to the execution home directory (defaults to `/home/airflow`).
* `BERT_DIR_ROOT`: Path to the root directory of the migrated scripts (defaults to `$HOME/SQL/aktuell`).
* `DW_DIR_UTL`: Path to write temporary execution metrics (defaults to `/tmp`).

---

## 5. Known Gaps & Unresolved References

### 5.1 Upstream Ingestion Pipeline (External Dependency)
* **Gap**: The replication of the Carmen source tables (`cds_ta_period`, `cds_ta_time_meas_cv`, and `cds_ta_description`) from the source Oracle database to the BigQuery `CARMEN_STAGE_DATASET` is **not** handled by this job.
* **Action Required**: Ensure that the external ingestion pipeline (e.g., via Cloud Data Fusion, Qlik Replicate, or a custom Spark job) is scheduled and completes successfully before this job runs.

### 5.2 Watermark Coordination
* **Gap**: This script retrieves its execution watermark (`v_datum`) from `dwtk_meldungen` where `job_kennung = 'BERT_DROP_TEMP_TABLE'`.
* **Action Required**: The upstream job responsible for writing this watermark entry must be migrated and scheduled to run prior to `dw_bert_ausd_v_ta_period`.

### 5.3 Legacy Monitoring Framework (`DWMSG`)
* **Gap**: The legacy shell script relied heavily on an on-premises monitoring framework (`DWMSG_*` commands) to log job states and register execution IDs. These have been replaced with standard Python `logging` statements.
* **Action Required**: If centralized enterprise monitoring is required, the logging statements in `r_ausd_v_ta_period.py` must be integrated with Google Cloud Logging sinks or an equivalent enterprise monitoring API.

---

## 6. Validation

To validate the migration, perform the following testing steps:

### 6.1 Local/Development Execution
Run the Python wrapper script manually in a development environment where the GCP SDK is authenticated:

```bash
export GCP_PROJECT="your-dev-project"
export BQ_DATASET="your_dev_dataset"
export CARMEN_STAGE_DATASET="your_stage_dataset"
export BERT_DIR_ROOT="/path/to/migrated/code"
export DW_DIR_UTL="/tmp"

python r_ausd_v_ta_period.py -s "test" -l "test"
```

### 6.2 Airflow DAG Validation
1. Upload `dw_bert_ausd_v_ta_period.py` to the Airflow `dags/` folder.
2. Verify that the DAG parses successfully without syntax errors in the Airflow UI.
3. Trigger the DAG manually by clicking **Trigger DAG**.

### 6.3 Definition of "Passing"
The validation is considered successful if:
1. The Airflow task execution status is `SUCCESS`.
2. The Airflow task log displays:
   * `Query executed successfully. Affected/Total rows: <count>`
   * `Die Abarbeitung wurde ohne erkennbare Fehler beendet`
3. The target BigQuery table `sof_ta_period` contains the expected mirrored records matching the source system for the corresponding watermark date.
4. The temporary file in `DW_DIR_UTL` is successfully cleaned up after execution.

---

## 7. Rollback Procedure

In the event of a critical failure or data corruption during deployment:

1. **Pause the Airflow DAG**:
   Immediately toggle the DAG to **Off** (Paused) in the Airflow UI to prevent further automated executions.
   ```bash
   airflow dags pause dw_bert_ausd_v_ta_period
   ```

2. **Restore Target Table Data**:
   If the target table `sof_ta_period` was corrupted or loaded with invalid data, restore it to its pre-execution state using BigQuery's time-travel feature:
   ```sql
   -- Overwrite the corrupted table with data from 1 hour ago
   CREATE OR REPLACE TABLE `your_project.isbert_schema.sof_ta_period` AS
   SELECT * FROM `your_project.isbert_schema.sof_ta_period`
   FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
   ```

3. **Re-enable Legacy Execution**:
   If necessary, un-pause/re-enable the legacy UC4 job `DW.BERT_AUSD_V_TA_PERIOD` in the legacy Automic environment to resume on-premises processing.