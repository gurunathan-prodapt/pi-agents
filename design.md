# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_apn_ve.ksh
## 1. Purpose & Scope

### 1.1 Purpose of the job
This ETL job extracts data from Oracle source tables, applies date-based filtering and join logic, and loads the result into a target table `sof$ta_apn_ve`.

### 1.2 Problem it addresses
The job appears to maintain a refreshed dataset of access-point-related records, likely for downstream reporting or operational use. It truncates the target table and reloads it fully each run, ensuring the target reflects the latest eligible source data.

### 1.3 Context in the larger system
The current implementation is a classic Oracle batch ETL pattern:
- A shell orchestrator starts the job
- A second shell script prepares SQL execution
- An Oracle SQL*Plus script performs the transformation and load

The migration goal is to replace this with:
- **Cloud Composer DAG** for orchestration
- **BigQuery SQL** for transformation and loading
- **Cloud Logging / Monitoring** for observability
- Optional **Cloud Storage** staging if source extraction is required outside BigQuery

## 2. Source Inventory

The job `5af228f1-3847-4cc6-9310-ed82ed19407c` consists of the following components:

1.  **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_apn_ve.ksh`**
    *   **Technology:** KornShell Script
    *   **Category:** shell
    *   **Tool:** KornShell
    *   **Purpose:** Orchestrator (primary_bucket: pipeline_orchestrator)
    *   **Complexity Tier:** medium
    *   **Migration Flags:** []
    *   **Automation Bucket:** semi_auto
    *   **Description:** Main entry point, handles environment setup, parameter parsing, logging, error trapping, and orchestrates the execution of `k_ausd_v_ta_apn_ve.ksh`.

2.  **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_apn_ve.ksh`**
    *   **Technology:** KornShell Script
    *   **Purpose:** Control script, invoked by `r_ausd_v_ta_apn_ve.ksh`. Prepares the environment for SQL execution and runs the core SQL script `d_ausd_v_ta_apn_ve.sql`.

3.  **`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_apn_ve.sql`**
    *   **Technology:** Oracle SQL*Plus Script
    *   **Purpose:** Performs the actual data transformation and loading. Reads from Oracle sources and writes to an Oracle target table.

## 3. Target Architecture

### Proposed GCP architecture
The recommended approach is to use **staging tables in BigQuery** and perform the transformation in BigQuery SQL. This is the cleanest replacement for the Oracle SQL*Plus script.

### Recommended design
-   **Cloud Composer** orchestrates the workflow
-   **BigQuery** stores both staged source data and final target data
-   **Cloud Logging/Monitoring** replaces shell-based logging
-   **Secret Manager** stores credentials and connection details
-   **Airflow tasks** replace shell wrappers and SQL*Plus execution
-   **BigQuery SQL** replaces Oracle SQL*Plus transformation logic

## 4. Data Flow & Lineage

### Current flow
Shell (`r_ausd_v_ta_apn_ve.ksh`) → Shell (`k_ausd_v_ta_apn_ve.ksh`) → SQL*Plus (`d_ausd_v_ta_apn_ve.sql`) → Oracle source tables (`isbert_schema.dwtk_meldungen`, `pds$ta_pdp_context_assoc`, `pds$ta_pdp_context`, `pds$ta_access_point`) → Oracle target table (`sof$ta_apn_ve`)

### Target flow
Airflow DAG → Source ingestion/staging → BigQuery transformation → BigQuery target table

### Detailed flow:
1.  An Airflow DAG will trigger the overall job.
2.  A task within the DAG will handle the ingestion of source data from Oracle into BigQuery staging tables. This can be via Datastream, Data Fusion, or a custom JDBC-based ingestion tool.
3.  Another task will derive the `v_datum` (watermark date) from the `dwtk_meldungen` BigQuery table.
4.  A BigQuery SQL task will execute the transformation logic:
    *   Truncate the target BigQuery table `project.dataset.sof_ta_apn_ve`.
    *   Insert data by joining the staged source tables (`project.dataset.pds_ta_pdp_context_assoc`, `project.dataset.pds_ta_pdp_context`, `project.dataset.pds_ta_access_point`) and applying the date filters based on `v_datum`.
5.  Logging will be handled by Cloud Logging, and monitoring by Cloud Monitoring, integrated with Airflow.

## 5. Transformation Logic

### Oracle construct replacements

| Oracle Feature | GCP/BigQuery Replacement |
| :------------------------------- | :---------------------------------------------------- |
| SQL*Plus `DEFINE` | BigQuery scripting variables or Airflow parameters |
| `COLUMN ... NEW_VALUE` | BigQuery `DECLARE` / `SET` or Airflow XCom |
| `WHENEVER SQLERROR` | Airflow task failure handling |
| DB-link (`v_carmen`) | Replicated source tables in BigQuery or external ingestion |
| `DWPA_UTIL_SKRIPT.runstatement` | BigQuery `EXECUTE IMMEDIATE` or separate DDL task |
| `TRUNCATE TABLE` | BigQuery `TRUNCATE TABLE` or `CREATE OR REPLACE TABLE` |
| `COMMIT` | Not needed in BigQuery for standard DML/DDL job boundaries |

### Example BigQuery SQL pattern (rewritten `d_ausd_v_ta_apn_ve.sql`)

```sql
DECLARE v_datum DATE;

-- Derive watermark date
SET v_datum = (
  SELECT MAX(DATE(timecreated))
  FROM `project.dataset.dwtk_meldungen`
  WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- Truncate target table
TRUNCATE TABLE `project.dataset.sof_ta_apn_ve`;

-- Insert transformed data
INSERT INTO `project.dataset.sof_ta_apn_ve` (
        cntrct_id,
        access_point_name
)
SELECT
        pca.cntrct_id,
        ap.access_point_name
FROM
        `project.dataset.pds_ta_pdp_context_assoc`        pca
JOIN    `project.dataset.pds_ta_pdp_context`              pc
  ON    pca.pdp_context_id      = pc.pdp_context_id
JOIN    `project.dataset.pds_ta_access_point`             ap
  ON    pc.access_point_id      = ap.access_point_id
WHERE
        pca.insert_at <= v_datum
AND     (   pca.modified_at IS NULL
         OR pca.modified_at > v_datum )
AND     pca.valid_from <= v_datum
AND     (   pca.valid_to IS NULL
         OR pca.valid_to > v_datum )
AND
        pc.insert_at <= v_datum
AND     (   pc.modified_at IS NULL
         OR pc.modified_at > v_datum )
AND
        pc.is_production = 1
AND
        ap.insert_at <= v_datum
AND     (   ap.modified_at IS NULL
         OR ap.modified_at > v_datum )
AND
        pca.cntrct_id IS NOT NULL;
```

## 6. External Dependencies

### Current dependencies
-   KornShell (`ksh`)
-   Oracle SQL*Plus
-   Oracle database
-   Oracle DB-link (`v_carmen`)
-   Shell utilities: `date`, `print`, `tee`
-   Internal utility scripts: `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`
-   Oracle package: `isbert_schema.DWPA_UTIL_SKRIPT`

### Target GCP dependencies (Replacements)
-   **Oracle DB-link `v_carmen`:** Replaced by replicated source tables in BigQuery. Data will be ingested from the source Oracle system into BigQuery staging tables using appropriate data transfer services (e.g., Datastream, Data Fusion, or custom ingestion).
-   **Oracle `DWPA_UTIL_SKRIPT`:** The specific `runstatement` function used for `TRUNCATE TABLE` will be replaced by direct BigQuery DDL operations. Any other logic within this package not identified needs further analysis.
-   **KornShell scripts (`r_ausd_v_ta_apn_ve.ksh`, `k_ausd_v_ta_apn_ve.ksh`):** Replaced by an Airflow DAG. The orchestration logic, parameter passing, and error handling will be implemented using Airflow operators and Python.
-   **Oracle SQL*Plus:** Replaced by BigQuery SQL.
-   **Logging (`DWMSG_` functions, `LogDatei`, `tee`):** Replaced by Cloud Logging for structured logging of Airflow tasks and BigQuery job logs.
-   **Error Handling (`trap`, `DWMSG_MeldeFehler`):** Replaced by Airflow's native error handling, retries, and failure callbacks, integrated with Cloud Monitoring for alerts.
-   **Environment setup (`. $HOME/.dw_init`):** Replaced by Airflow environment variables, connections, and service accounts.

## 7. Unresolved / Risks

### Technical risks
-   **Oracle DB-link behavior:** The exact functionality of `v_carmen` and its performance characteristics need to be thoroughly understood to ensure accurate replication in BigQuery via data ingestion.
-   **`DWPA_UTIL_SKRIPT`:** The full functionality of this Oracle package is not known from the provided snippet. If it contains complex business logic beyond `runstatement`, additional reverse engineering and migration effort will be required.
-   **Oracle Date Semantics:** Differences in date/timestamp handling between Oracle and BigQuery, especially related to time zones and implicit conversions, need careful validation.
-   **Data Type Mismatches:** Potential for data type incompatibilities between Oracle and BigQuery during ingestion and transformation.

### Operational risks
-   **Full refresh performance:** Truncating and re-inserting large tables in BigQuery might be resource-intensive or impact downstream consumers if not managed carefully.
-   **Source ingestion latency:** The latency of getting fresh Oracle data into BigQuery staging tables must meet the job's requirements.
-   **Reconciliation:** Thorough data reconciliation between the Oracle source and BigQuery target will be crucial during the cutover phase.

### Migration risks
-   **Hidden dependencies:** Other undocumented shell utilities or environment variables used by the ksh scripts might exist.
-   **Logging semantics:** Exact replication of `DWMSG_` logging messages and their downstream consumption may be challenging.
-   **Downstream impacts:** Changes in refresh timing or data availability might affect other systems relying on `sof$ta_apn_ve`.

## 8. Build Plan

### Phase 1: Discovery
-   Capture full Oracle DDL for `sof$ta_apn_ve`, `dwtk_meldungen`, `pds$ta_pdp_context_assoc`, `pds$ta_pdp_context`, `pds$ta_access_point`.
-   Extract exact SQL for `d_ausd_v_ta_apn_ve.sql`.
-   Thoroughly document all columns, datatypes, and join conditions.
-   Analyze `isbert_schema.DWPA_UTIL_SKRIPT` for any additional hidden logic.
-   Identify and document all shell environment variables and utility scripts referenced.

### Phase 2: Landing Zone Setup
-   Create a dedicated GCP project or folder structure.
-   Set up BigQuery datasets (e.g., `raw`, `staging`, `data_warehouse`).
-   Provision a Cloud Composer environment.
-   Configure Secret Manager for Oracle credentials and other sensitive information.
-   Establish IAM roles and permissions for Composer service account with least privilege.

### Phase 3: Source Ingestion
-   Select and implement a data ingestion method (e.g., Datastream for CDC, Data Fusion, or custom batch load) to bring Oracle source tables (`dwtk_meldungen`, `pds$ta_pdp_context_assoc`, `pds$ta_pdp_context`, `pds$ta_access_point`) into BigQuery staging datasets.
-   Validate ingested data for row counts, data types, and data quality.

### Phase 4: Transformation Build (BigQuery SQL)
-   Rewrite `d_ausd_v_ta_apn_ve.sql` into BigQuery Standard SQL, incorporating BigQuery-specific functions and syntax.
-   Replace the `DEFINE` and `COLUMN ... NEW_VALUE` logic with `DECLARE`/`SET` statements in BigQuery scripting or use Airflow XComs for dynamic values.
-   Implement the `TRUNCATE TABLE` and `INSERT INTO ... SELECT ...` logic.
-   Add BigQuery audit logging as needed.

### Phase 5: Orchestration Build (Cloud Composer / Airflow DAG)
-   Develop an Airflow DAG for `r_ausd_v_ta_apn_ve.ksh`.
-   Define tasks for:
    *   **Data Ingestion:** Triggering/monitoring source data ingestion into BigQuery staging.
    *   **Get Watermark:** A PythonOperator or BigQueryOperator to derive `v_datum`.
    *   **Transform & Load:** A BigQueryOperator to execute the BigQuery SQL transformation.
    *   **Monitoring/Alerting:** Tasks for sending notifications on success/failure (e.g., using SlackOperator, EmailOperator).
-   Configure task dependencies, retries, and SLAs.
-   Replace shell-based logging with Airflow's built-in logging to Cloud Logging.

### Phase 6: Testing
-   **Unit Testing:** Test the BigQuery SQL transformation logic independently with sample data.
-   **Integration Testing:** Test the Airflow DAG with the ingested staging data.
-   **Data Validation:** Compare row counts and a sample of transformed data between the original Oracle output and the BigQuery output.
-   **Performance Testing:** Assess the runtime of the BigQuery job and optimize where necessary.
-   **Error Handling Testing:** Verify that error conditions are correctly caught and handled by Airflow, generating appropriate alerts.

### Phase 7: Cutover
-   Execute parallel runs of the legacy Oracle job and the new GCP job.
-   Rigorously reconcile data outputs and job statuses for a defined period.
-   Once confidence is high, switch downstream consumers to the BigQuery target table.
-   Decommission the legacy Oracle batch job.